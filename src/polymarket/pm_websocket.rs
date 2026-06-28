//! WebSocket Polymarket — carnets temps réel (Phase 2).
//!
//! Endpoint : `wss://ws-subscriptions-clob.polymarket.com/ws/market`
//! Subscribe : `{"assets_ids": [...], "type": "market"}`
//!
//! Events gérés :
//! - `book`           → snapshot complet (initial)
//! - `price_change`   → delta niveaux (price_changes[] avec asset_id par entrée)
//! - `best_bid_ask`   → top-of-book direct (zéro clone carnet)
//! - `tick_size_change` → invalide TOKEN_META
//!
//! Lifecycle : une seule task lancée au boot via `init_market_ws()` ; au rollover
//! marché, `pm_poller` envoie les nouveaux tokens dans le `watch::Sender<Vec<String>>`
//! → la task resouscrit in-session sans se fermer.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::relayer::{Level, PolyBook, PmShared};

const PM_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL_S: u64 = 10;
const MAX_BACKOFF_S: u64 = 60;

/// Lance la task WS market **une fois** au boot.
/// Retourne un `watch::Sender` que le poller utilise pour envoyer les tokens du marché courant.
/// À chaque envoi, la task resouscrit au nouveau set de tokens sans se reconnecter.
pub fn init_market_ws(pm: Arc<Mutex<PmShared>>) -> watch::Sender<Vec<String>> {
    let (tx, mut rx) = watch::channel(Vec::<String>::new());
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            // Attend qu'il y ait des tokens à souscrire.
            let tokens = loop {
                let t = rx.borrow().clone();
                if !t.is_empty() { break t; }
                if rx.changed().await.is_err() { return; }
            };

            match run_ws_session(&pm, &tokens, &mut rx).await {
                Ok(()) => tracing::info!("pm_ws: session terminée, reconnexion dans {backoff}s"),
                Err(e) => tracing::warn!(error = %e, backoff, "pm_ws: erreur, reconnexion dans {backoff}s"),
            }
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_S);
        }
    });
    tx
}

async fn run_ws_session(
    pm: &Arc<Mutex<PmShared>>,
    initial_tokens: &[String],
    rx: &mut watch::Receiver<Vec<String>>,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(PM_WS_URL).await?;
    let (mut sink, mut stream) = ws.split();

    subscribe(&mut sink, initial_tokens).await?;

    let mut ping_interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_S));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(txt))) => {
                        if let Err(e) = process_message(&txt, pm) {
                            tracing::debug!(error = %e, "pm_ws: message ignoré");
                        }
                    }
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => {}
                }
            }
            _ = ping_interval.tick() => {
                sink.send(Message::Ping(vec![])).await?;
            }
            // Rollover marché : nouveaux tokens → resouscrit in-session sans se reconnecter.
            Ok(()) = rx.changed() => {
                let new_tokens = rx.borrow().clone();
                if !new_tokens.is_empty() {
                    tracing::info!(tokens = ?new_tokens, "pm_ws: resouscription rollover marché");
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
    let sub = serde_json::json!({
        "assets_ids": tokens,
        "type": "market",
    });
    sink.send(Message::Text(sub.to_string())).await
        .map_err(|e| anyhow::anyhow!("pm_ws send: {e}"))
}

fn process_message(txt: &str, pm: &Arc<Mutex<PmShared>>) -> anyhow::Result<()> {
    let events = parse_events::<WsEvent>(txt);
    if events.is_empty() {
        return Err(anyhow::anyhow!("aucun event parseable"));
    }
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    let mut g = pm.lock().unwrap();

    for ev in events {
        let up_tok = g.market.as_ref().map(|m| m.up_token_id.clone());
        let dn_tok = g.market.as_ref().map(|m| m.down_token_id.clone());
        match ev.event_type.as_deref() {
            Some("book") => {
                let book = build_book(&ev);
                let asset = ev.asset_id.as_deref().unwrap_or("");
                if up_tok.as_deref() == Some(asset) {
                    g.real_up = book.mid().unwrap_or(g.real_up);
                    g.up_best_bid = book.best_bid().unwrap_or(0.0);
                    g.up_best_ask = book.best_ask().unwrap_or(1.0);
                    g.up_book = Arc::new(book);
                } else if dn_tok.as_deref() == Some(asset) {
                    g.down_best_bid = book.best_bid().unwrap_or(0.0);
                    g.down_best_ask = book.best_ask().unwrap_or(1.0);
                    g.down_book = Arc::new(book);
                }
                g.last_ws_ts_ms = now_ms;
            }
            Some("price_change") => {
                apply_price_changes(&ev, &up_tok, &dn_tok, &mut g, now_ms);
            }
            Some("best_bid_ask") => {
                // Mise à jour top-of-book directe — zéro clone Vec<Level>.
                let asset = ev.asset_id.as_deref().unwrap_or("");
                if up_tok.as_deref() == Some(asset) {
                    if let Some(b) = ev.best_bid.as_ref().and_then(|s| s.parse::<f64>().ok()) {
                        g.up_best_bid = b;
                    }
                    if let Some(a) = ev.best_ask.as_ref().and_then(|s| s.parse::<f64>().ok()) {
                        g.up_best_ask = a;
                    }
                    g.real_up = (g.up_best_bid + g.up_best_ask) / 2.0;
                } else if dn_tok.as_deref() == Some(asset) {
                    if let Some(b) = ev.best_bid.as_ref().and_then(|s| s.parse::<f64>().ok()) {
                        g.down_best_bid = b;
                    }
                    if let Some(a) = ev.best_ask.as_ref().and_then(|s| s.parse::<f64>().ok()) {
                        g.down_best_ask = a;
                    }
                }
                g.last_ws_ts_ms = now_ms;
            }
            Some("tick_size_change") => {
                #[cfg(feature = "live")]
                if let Some(asset) = &ev.asset_id {
                    crate::polymarket::poly1271::invalidate_token_meta(asset);
                }
                g.last_ws_ts_ms = now_ms;
            }
            _ => {}
        }
    }
    Ok(())
}

fn build_book(ev: &WsEvent) -> PolyBook {
    PolyBook {
        bids: ev.bids.iter().filter_map(parse_level).collect(),
        asks: ev.asks.iter().filter_map(parse_level).collect(),
    }
}

fn apply_price_changes(
    ev: &WsEvent,
    up_tok: &Option<String>,
    dn_tok: &Option<String>,
    g: &mut std::sync::MutexGuard<PmShared>,
    now_ms: u64,
) {
    for change in &ev.price_changes {
        let asset = change.asset_id.as_str();
        let is_up = up_tok.as_deref() == Some(asset);
        let is_dn = !is_up && dn_tok.as_deref() == Some(asset);
        if !is_up && !is_dn { continue; }

        let Some(price) = change.price.parse::<f64>().ok() else { continue };
        let Some(size) = change.size.parse::<f64>().ok() else { continue };

        let book = if is_up { Arc::make_mut(&mut g.up_book) } else { Arc::make_mut(&mut g.down_book) };
        let levels = if change.side.as_deref() == Some("BUY") { &mut book.bids } else { &mut book.asks };

        if size == 0.0 {
            levels.retain(|l| (l.price - price).abs() > 1e-9);
        } else if let Some(l) = levels.iter_mut().find(|l| (l.price - price).abs() < 1e-9) {
            l.size = size;
        } else {
            levels.push(Level { price, size });
        }

        if is_up {
            g.real_up = g.up_book.mid().unwrap_or(g.real_up);
            g.up_best_bid = g.up_book.best_bid().unwrap_or(0.0);
            g.up_best_ask = g.up_book.best_ask().unwrap_or(1.0);
        } else {
            g.down_best_bid = g.down_book.best_bid().unwrap_or(0.0);
            g.down_best_ask = g.down_book.best_ask().unwrap_or(1.0);
        }
    }
    if !ev.price_changes.is_empty() {
        g.last_ws_ts_ms = now_ms;
    }
}

fn parse_level(l: &RawLevel) -> Option<Level> {
    Some(Level { price: l.price.parse().ok()?, size: l.size.parse().ok()? })
}

/// Parse un texte JSON en `Vec<T>` — tente tableau puis objet unique.
pub fn parse_events<T: serde::de::DeserializeOwned>(txt: &str) -> Vec<T> {
    if let Ok(v) = serde_json::from_str::<Vec<T>>(txt) { return v; }
    if let Ok(v) = serde_json::from_str::<T>(txt) { return vec![v]; }
    vec![]
}

#[derive(Deserialize)]
struct WsEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    asset_id: Option<String>,
    best_bid: Option<String>,
    best_ask: Option<String>,
    #[serde(default)] bids: Vec<RawLevel>,
    #[serde(default)] asks: Vec<RawLevel>,
    #[serde(default)] price_changes: Vec<PriceChangeEntry>,
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
    use std::sync::{Arc, Mutex};
    use crate::polymarket::relayer::PmShared;

    fn pm_with_tokens(up: &str, dn: &str) -> Arc<Mutex<PmShared>> {
        use crate::polymarket::relayer::Market;
        use chrono::Utc;
        let mut pm = PmShared::default();
        pm.market = Some(Market {
            slug: "test".into(),
            condition_id: "cond".into(),
            up_token_id: up.into(),
            down_token_id: dn.into(),
            end_time: Utc::now() + chrono::Duration::seconds(300),
            window_ts: 0,
            tick_size: 0.01,
            min_order_size: 5.0,
            neg_risk: false,
        });
        Arc::new(Mutex::new(pm))
    }

    #[test]
    fn book_snapshot_updates_best_bid_ask_and_ts() {
        let pm = pm_with_tokens("UP_TOK", "DN_TOK");
        let txt = serde_json::json!([{
            "type": "book",
            "asset_id": "UP_TOK",
            "bids": [{"price": "0.52", "size": "100"}],
            "asks": [{"price": "0.54", "size": "80"}],
            "price_changes": []
        }]).to_string();
        process_message(&txt, &pm).unwrap();
        let g = pm.lock().unwrap();
        assert!(g.last_ws_ts_ms > 0, "last_ws_ts_ms doit être mis à jour");
        assert!((g.up_best_bid - 0.52).abs() < 1e-9);
        assert!((g.up_best_ask - 0.54).abs() < 1e-9);
        assert!((g.real_up - 0.53).abs() < 1e-6);
    }

    #[test]
    fn book_object_single_parses_ok() {
        // Cas objet unique (non-tableau).
        let pm = pm_with_tokens("UP_TOK", "DN_TOK");
        let txt = serde_json::json!({
            "type": "book",
            "asset_id": "DN_TOK",
            "bids": [{"price": "0.45", "size": "50"}],
            "asks": [{"price": "0.47", "size": "30"}],
            "price_changes": []
        }).to_string();
        process_message(&txt, &pm).unwrap();
        let g = pm.lock().unwrap();
        assert!((g.down_best_bid - 0.45).abs() < 1e-9);
        assert!((g.down_best_ask - 0.47).abs() < 1e-9);
    }

    #[test]
    fn best_bid_ask_updates_top_of_book_only() {
        let pm = pm_with_tokens("UP_TOK", "DN_TOK");
        // D'abord un snapshot book pour initialiser le carnet.
        let init = serde_json::json!([{
            "type": "book", "asset_id": "UP_TOK",
            "bids": [{"price": "0.50", "size": "10"}],
            "asks": [{"price": "0.52", "size": "10"}],
            "price_changes": []
        }]).to_string();
        process_message(&init, &pm).unwrap();
        let bids_len_before = pm.lock().unwrap().up_book.bids.len();

        // Puis best_bid_ask — ne doit pas toucher Vec<Level>.
        let txt = serde_json::json!([{
            "type": "best_bid_ask",
            "asset_id": "UP_TOK",
            "best_bid": "0.51",
            "best_ask": "0.53"
        }]).to_string();
        process_message(&txt, &pm).unwrap();
        let g = pm.lock().unwrap();
        assert!((g.up_best_bid - 0.51).abs() < 1e-9);
        assert!((g.up_best_ask - 0.53).abs() < 1e-9);
        // Vec<Level> du carnet inchangé.
        assert_eq!(g.up_book.bids.len(), bids_len_before);
    }

    #[test]
    fn price_change_delta_applies_correctly() {
        let pm = pm_with_tokens("UP_TOK", "DN_TOK");
        // Snapshot initial.
        let init = serde_json::json!([{
            "type": "book", "asset_id": "UP_TOK",
            "bids": [{"price": "0.50", "size": "10"}, {"price": "0.49", "size": "20"}],
            "asks": [{"price": "0.52", "size": "15"}],
            "price_changes": []
        }]).to_string();
        process_message(&init, &pm).unwrap();

        // price_change : retire le niveau 0.49, modifie 0.50.
        let txt = serde_json::json!([{
            "type": "price_change",
            "price_changes": [
                {"asset_id": "UP_TOK", "price": "0.49", "size": "0", "side": "BUY"},
                {"asset_id": "UP_TOK", "price": "0.50", "size": "25", "side": "BUY"}
            ]
        }]).to_string();
        process_message(&txt, &pm).unwrap();
        let g = pm.lock().unwrap();
        assert_eq!(g.up_book.bids.len(), 1, "niveau 0.49 supprimé");
        assert!((g.up_book.bids[0].size - 25.0).abs() < 1e-9, "niveau 0.50 mis à jour");
        assert!(g.last_ws_ts_ms > 0);
    }

    #[test]
    fn parse_events_handles_array_and_object() {
        let arr = r#"[{"type":"book"},{"type":"price_change"}]"#;
        let obj = r#"{"type":"book"}"#;
        let arr_parsed = parse_events::<serde_json::Value>(arr);
        let obj_parsed = parse_events::<serde_json::Value>(obj);
        assert_eq!(arr_parsed.len(), 2);
        assert_eq!(obj_parsed.len(), 1);
    }
}
