//! Connecteur WebSocket Binance (J1).
//!
//! Consomme le stream *partial book depth* `btcusdt@depth20@100ms` : chaque message
//! est un snapshot complet des 20 meilleurs niveaux bid/ask, ce qui évite la
//! resynchronisation par `lastUpdateId` du stream différentiel et rend la
//! reconnexion triviale. Reconstruit la `BinanceOrderBook` et publie chaque mise à
//! jour sur un canal `watch` consommé par le moteur Radar.

use std::cmp::Reverse;
use std::time::Duration;

use chrono::Utc;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::types::{BinanceOrderBook, BookUpdate, OrderedFloat};

/// Message du stream partial book depth (`@depthN@100ms`).
#[derive(Debug, Deserialize)]
struct PartialDepth {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

impl PartialDepth {
    fn into_book(self) -> BinanceOrderBook {
        let mut book = BinanceOrderBook::new();
        book.last_update_id = self.last_update_id;
        for [p, q] in &self.bids {
            if let (Ok(price), Ok(qty)) = (p.parse::<f64>(), q.parse::<f64>()) {
                if qty > 0.0 {
                    book.bids.insert(Reverse(OrderedFloat(price)), qty);
                }
            }
        }
        for [p, q] in &self.asks {
            if let (Ok(price), Ok(qty)) = (p.parse::<f64>(), q.parse::<f64>()) {
                if qty > 0.0 {
                    book.asks.insert(OrderedFloat(price), qty);
                }
            }
        }
        book
    }
}

/// Prix d'ouverture BTC à `window_ts` (kline 1m) — proxy du strike de référence
/// (la résolution officielle est Chainlink, mais l'open Binance en est très proche).
pub async fn price_at_window_open(window_ts: i64) -> anyhow::Result<f64> {
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={}&limit=1",
        window_ts * 1000
    );
    let arr: Vec<Vec<serde_json::Value>> = reqwest::Client::new()
        .get(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let open = arr
        .first()
        .and_then(|k| k.get(1))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow::anyhow!("open price introuvable pour window {window_ts}"))?;
    Ok(open)
}

/// Boucle de connexion résiliente. Ne retourne jamais en fonctionnement normal :
/// en cas de coupure, reconnecte avec backoff exponentiel borné.
pub async fn run(url: String, tx: watch::Sender<Option<BookUpdate>>) -> anyhow::Result<()> {
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(30);

    loop {
        match connect_and_stream(&url, &tx).await {
            Ok(()) => {
                tracing::warn!("Flux Binance terminé proprement, reconnexion…");
                backoff = Duration::from_millis(500);
            }
            Err(e) => {
                tracing::error!(error = %e, backoff_ms = backoff.as_millis(), "Erreur Binance, reconnexion");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn connect_and_stream(
    url: &str,
    tx: &watch::Sender<Option<BookUpdate>>,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(url).await?;
    tracing::info!(%url, "Binance WS connecté");
    let (_write, mut read) = ws.split();

    // depth20@100ms envoie ~10 msg/s : 15 s de silence = connexion zombie
    // (morte sans FIN TCP — cause du gel du 6 juil. : spot figé 1h15, fair morte,
    // résolutions fausses). On coupe et on laisse la boucle externe reconnecter.
    while let Some(msg) = tokio::time::timeout(Duration::from_secs(15), read.next())
        .await
        .map_err(|_| anyhow::anyhow!("flux Binance silencieux >15 s (connexion zombie)"))?
    {
        match msg? {
            Message::Text(txt) => {
                match serde_json::from_str::<PartialDepth>(&txt) {
                    Ok(depth) => {
                        let book = depth.into_book();
                        let update = BookUpdate {
                            book,
                            ts_ms: Utc::now().timestamp_millis() as u64,
                        };
                        // Ignorer l'erreur si plus aucun récepteur (arrêt en cours).
                        let _ = tx.send(Some(update));
                    }
                    Err(e) => tracing::debug!(error = %e, "Message Binance non parsé"),
                }
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(c) => {
                tracing::warn!(?c, "Binance a fermé la connexion");
                return Ok(());
            }
            _ => {}
        }
    }
    Ok(())
}
