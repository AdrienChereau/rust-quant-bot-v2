//! WebSocket Polymarket — carnets temps réel (porté du legacy `pm_websocket.rs`,
//! tag `legacy-sniper-v2`, adapté au PolyBook du monolith).
//!
//! Endpoint : `wss://ws-subscriptions-clob.polymarket.com/ws/market`
//! Subscribe : `{"assets_ids": [...], "type": "market"}`
//!
//! Events gérés :
//! - `book`         → snapshot complet
//! - `price_change` → deltas de niveaux (price_changes[] avec asset_id par entrée)
//! - `best_bid_ask` → ignoré (on maintient le carnet complet, le top en découle)
//!
//! Lifecycle : une task au boot via `spawn()` ; au rollover marché l'exécuteur
//! envoie les nouveaux token_ids dans le `watch::Sender` → resouscription
//! in-session. L'exécuteur lit les carnets via le `PmWsShared` (RwLock std,
//! sections critiques courtes) et vérifie la fraîcheur (`last_ts_ms`).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::polymarket::{Level, PolyBook};

const PM_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL_S: u64 = 10;
const MAX_BACKOFF_S: u64 = 30;

#[derive(Default)]
pub struct PmWsState {
    pub books: HashMap<String, PolyBook>, // asset_id → carnet maintenu
    pub last_ts_ms: u64,                  // dernier event reçu (fraîcheur)
}

pub type PmWsShared = Arc<RwLock<PmWsState>>;

/// Carnet d'un token si le flux est frais (< `max_age_ms`), sinon None
/// (l'appelant retombe sur le REST — le bot ne dépend jamais du WS seul).
pub fn fresh_book(state: &PmWsShared, token: &str, now_ms: u64, max_age_ms: u64) -> Option<PolyBook> {
    let g = state.read().ok()?;
    if g.last_ts_ms == 0 || now_ms.saturating_sub(g.last_ts_ms) > max_age_ms {
        return None;
    }
    g.books.get(token).cloned()
}

/// Lance la task WS market une fois au boot. Retourne le canal d'envoi des
/// token_ids du marché courant (rollover → resouscription sans reconnexion).
pub fn spawn(state: PmWsShared) -> watch::Sender<Vec<String>> {
    let (tx, mut rx) = watch::channel(Vec::<String>::new());
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            let tokens = loop {
                let t = rx.borrow().clone();
                if !t.is_empty() {
                    break t;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            };
            match run_session(&state, &tokens, &mut rx).await {
                Ok(()) => tracing::info!("pm_ws: session terminée, reconnexion dans {backoff}s"),
                Err(e) => tracing::warn!(error = %e, backoff, "pm_ws: erreur, reconnexion"),
            }
            // Session morte → carnets invalides : on force le fallback REST.
            if let Ok(mut g) = state.write() {
                g.last_ts_ms = 0;
            }
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_S);
        }
    });
    tx
}

async fn run_session(
    state: &PmWsShared,
    initial_tokens: &[String],
    rx: &mut watch::Receiver<Vec<String>>,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(PM_WS_URL).await?;
    tracing::info!("pm_ws: WS market connecté");
    let (mut sink, mut stream) = ws.split();
    subscribe(&mut sink, initial_tokens).await?;

    let mut ping = tokio::time::interval(Duration::from_secs(PING_INTERVAL_S));
    ping.tick().await;
    // Silence > 30 s = connexion zombie (même leçon que le WS Binance du 6 juil.).
    let mut last_msg = tokio::time::Instant::now();

    loop {
        tokio::select! {
            msg = stream.next() => {
                last_msg = tokio::time::Instant::now();
                match msg {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(txt))) => process_message(&txt, state),
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => {}
                }
            }
            _ = ping.tick() => {
                if last_msg.elapsed() > Duration::from_secs(30) {
                    return Err(anyhow::anyhow!("pm_ws silencieux >30 s (zombie)"));
                }
                sink.send(Message::Ping(vec![].into())).await?;
            }
            Ok(()) = rx.changed() => {
                let new_tokens = rx.borrow().clone();
                if !new_tokens.is_empty() {
                    tracing::info!("pm_ws: resouscription rollover marché");
                    // On purge les carnets des anciens tokens (mémoire bornée).
                    if let Ok(mut g) = state.write() {
                        g.books.retain(|k, _| new_tokens.contains(k));
                    }
                    subscribe(&mut sink, &new_tokens).await?;
                }
            }
        }
    }
}

async fn subscribe(
    sink: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    tokens: &[String],
) -> anyhow::Result<()> {
    let sub = serde_json::json!({ "assets_ids": tokens, "type": "market" });
    sink.send(Message::Text(sub.to_string().into()))
        .await
        .map_err(|e| anyhow::anyhow!("pm_ws send: {e}"))
}

fn process_message(txt: &str, state: &PmWsShared) {
    let events = parse_events::<WsEvent>(txt);
    if events.is_empty() {
        return;
    }
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    let Ok(mut g) = state.write() else { return };
    for ev in events {
        match ev.event_type.as_deref() {
            Some("book") => {
                let Some(asset) = ev.asset_id.clone() else { continue };
                g.books.insert(
                    asset,
                    PolyBook {
                        bids: ev.bids.iter().filter_map(parse_level).collect(),
                        asks: ev.asks.iter().filter_map(parse_level).collect(),
                    },
                );
                g.last_ts_ms = now_ms;
            }
            Some("price_change") => {
                for c in &ev.price_changes {
                    let Some(book) = g.books.get_mut(&c.asset_id) else { continue };
                    let (Ok(price), Ok(size)) = (c.price.parse::<f64>(), c.size.parse::<f64>())
                    else {
                        continue;
                    };
                    let levels = if c.side.as_deref() == Some("BUY") {
                        &mut book.bids
                    } else {
                        &mut book.asks
                    };
                    if size == 0.0 {
                        levels.retain(|l| (l.price - price).abs() > 1e-9);
                    } else if let Some(l) =
                        levels.iter_mut().find(|l| (l.price - price).abs() < 1e-9)
                    {
                        l.size = size;
                    } else {
                        levels.push(Level { price, size });
                    }
                }
                if !ev.price_changes.is_empty() {
                    g.last_ts_ms = now_ms;
                }
            }
            _ => {}
        }
    }
}

fn parse_level(l: &RawLevel) -> Option<Level> {
    Some(Level { price: l.price.parse().ok()?, size: l.size.parse().ok()? })
}

/// Parse un texte JSON en `Vec<T>` — tente tableau puis objet unique.
fn parse_events<T: serde::de::DeserializeOwned>(txt: &str) -> Vec<T> {
    if let Ok(v) = serde_json::from_str::<Vec<T>>(txt) {
        return v;
    }
    if let Ok(v) = serde_json::from_str::<T>(txt) {
        return vec![v];
    }
    vec![]
}

#[derive(Deserialize)]
struct WsEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    asset_id: Option<String>,
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
    #[serde(default)]
    price_changes: Vec<PriceChangeEntry>,
}

#[derive(Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct PriceChangeEntry {
    asset_id: String,
    price: String,
    size: String,
    side: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> PmWsShared {
        Arc::new(RwLock::new(PmWsState::default()))
    }

    #[test]
    fn book_snapshot_then_price_change() {
        let s = st();
        let snap = serde_json::json!([{
            "type": "book", "asset_id": "UP",
            "bids": [{"price": "0.50", "size": "10"}, {"price": "0.49", "size": "20"}],
            "asks": [{"price": "0.52", "size": "15"}],
            "price_changes": []
        }])
        .to_string();
        process_message(&snap, &s);
        {
            let g = s.read().unwrap();
            assert_eq!(g.books["UP"].bids.len(), 2);
            assert!((g.books["UP"].best_bid().unwrap() - 0.50).abs() < 1e-9);
            assert!(g.last_ts_ms > 0);
        }
        // delta : retire 0.49, modifie 0.50
        let delta = serde_json::json!([{
            "type": "price_change",
            "price_changes": [
                {"asset_id": "UP", "price": "0.49", "size": "0", "side": "BUY"},
                {"asset_id": "UP", "price": "0.50", "size": "25", "side": "BUY"}
            ]
        }])
        .to_string();
        process_message(&delta, &s);
        let g = s.read().unwrap();
        assert_eq!(g.books["UP"].bids.len(), 1);
        assert!((g.books["UP"].bids[0].size - 25.0).abs() < 1e-9);
    }

    #[test]
    fn fresh_book_respects_age() {
        let s = st();
        let snap = serde_json::json!({
            "type": "book", "asset_id": "UP",
            "bids": [{"price": "0.50", "size": "10"}], "asks": [], "price_changes": []
        })
        .to_string();
        process_message(&snap, &s);
        let ts = s.read().unwrap().last_ts_ms;
        assert!(fresh_book(&s, "UP", ts + 1000, 5000).is_some());
        assert!(fresh_book(&s, "UP", ts + 6000, 5000).is_none(), "périmé");
        assert!(fresh_book(&s, "AUTRE", ts + 1000, 5000).is_none());
    }

    #[test]
    fn parse_events_array_and_object() {
        assert_eq!(parse_events::<serde_json::Value>(r#"[{"a":1},{"b":2}]"#).len(), 2);
        assert_eq!(parse_events::<serde_json::Value>(r#"{"a":1}"#).len(), 1);
    }
}
