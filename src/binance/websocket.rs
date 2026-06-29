//! Connecteur WebSocket Binance — flux **diff-depth complet** `@depth`
//! (P2b). Maintient un `OrderBookL2` complet (snapshot REST + diffs séquencés),
//! seule façon de couvrir la bande OBI 0.15 % (±~$90 sur BTC). Partagé via
//! `Arc<Mutex<OrderBookL2>>` ; le hot-loop lit sans bloquer le réseau.
//!
//! **Event-driven (Bloc L)** : chaque update livre `(obi_b, spot, microprice)` via
//! `watch::Sender` → le signal task évalue immédiatement (zéro attente tick).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use super::local_book::OrderBookL2;

#[derive(Deserialize)]
struct DepthEvent {
    #[serde(rename = "U")] first_id: u64,
    #[serde(rename = "u")] final_id: u64,
    #[serde(rename = "b")] bids: Vec<[String; 2]>,
    #[serde(rename = "a")] asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct Snapshot {
    #[serde(rename = "lastUpdateId")] last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

async fn fetch_snapshot() -> anyhow::Result<Snapshot> {
    let url = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000";
    Ok(reqwest::Client::new().get(url).timeout(Duration::from_secs(10))
        .send().await?.error_for_status()?.json().await?)
}

fn apply_levels(book: &mut OrderBookL2, is_bid: bool, levels: &[[String; 2]]) {
    for [p, q] in levels {
        if let (Ok(price), Ok(qty)) = (p.parse::<f64>(), q.parse::<f64>()) {
            book.update_level(is_bid, price, qty);
        }
    }
}

/// Canal : `(obi_b, spot_opt, microprice_opt)`.
/// - `obi_b`       : OBI multi-niveaux ∈ [-1, 1]
/// - `spot_opt`    : mid-price (None avant la 1re sync)
/// - `microprice_opt`: microprice top-of-book (None si livre vide)
pub async fn run(
    url: String,
    shared: Arc<Mutex<OrderBookL2>>,
    obi_tx: watch::Sender<(f64, Option<f64>, Option<f64>)>,
    obi_n: usize,
    obi_lambda: f64,
) -> anyhow::Result<()> {
    let mut backoff = Duration::from_millis(500);
    loop {
        match connect_and_stream(&url, &shared, &obi_tx, obi_n, obi_lambda).await {
            Ok(()) => backoff = Duration::from_millis(500),
            Err(e) => tracing::error!(error = %e, "Binance WS, reconnexion"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn connect_and_stream(
    url: &str,
    shared: &Arc<Mutex<OrderBookL2>>,
    obi_tx: &watch::Sender<(f64, Option<f64>, Option<f64>)>,
    obi_n: usize,
    obi_lambda: f64,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(url).await?;
    tracing::info!(%url, "Binance WS connecté");
    let (_w, mut read) = ws.split();

    let mut buffer: Vec<DepthEvent> = Vec::new();
    let snapshot = fetch_snapshot().await?;
    let mut last_id = snapshot.last_update_id;

    {
        let mut book = shared.lock().unwrap();
        book.bids.clear();
        book.asks.clear();
        apply_levels(&mut book, true, &snapshot.bids);
        apply_levels(&mut book, false, &snapshot.asks);
    }

    let mut synced = false;
    while let Some(msg) = read.next().await {
        let txt = match msg? {
            Message::Text(t) => t,
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(_) => return Ok(()),
            _ => continue,
        };
        let ev: DepthEvent = match serde_json::from_str(&txt) {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !synced {
            if ev.final_id <= last_id { continue; }
            buffer.push(ev);
            if buffer.first().map_or(false, |e| e.first_id <= last_id + 1) {
                let snap = {
                    let mut book = shared.lock().unwrap();
                    for e in buffer.drain(..) {
                        apply_levels(&mut book, true, &e.bids);
                        apply_levels(&mut book, false, &e.asks);
                        last_id = e.final_id;
                    }
                    (book.calculate_obi_multilevel(obi_n, obi_lambda), book.mid_price(), book.microprice())
                };
                synced = true;
                let _ = obi_tx.send(snap);
            }
            continue;
        }

        if ev.final_id <= last_id { continue; }
        let snap = {
            let mut book = shared.lock().unwrap();
            apply_levels(&mut book, true, &ev.bids);
            apply_levels(&mut book, false, &ev.asks);
            last_id = ev.final_id;
            (book.calculate_obi_multilevel(obi_n, obi_lambda), book.mid_price(), book.microprice())
        };
        let _ = obi_tx.send(snap);
    }
    Ok(())
}
