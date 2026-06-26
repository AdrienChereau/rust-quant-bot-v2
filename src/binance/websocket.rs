//! Connecteur WebSocket Binance — flux **diff-depth complet** `@depth@100ms`
//! (P2b). Maintient un `OrderBookL2` complet (snapshot REST + diffs séquencés),
//! seule façon de couvrir la bande OBI 0.15 % (±~$90 sur BTC). Partagé via
//! `Arc<Mutex<OrderBookL2>>` ; le hot-loop lit sans bloquer le réseau.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
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

pub async fn run(url: String, shared: Arc<Mutex<OrderBookL2>>) -> anyhow::Result<()> {
    let mut backoff = Duration::from_millis(500);
    loop {
        match connect_and_stream(&url, &shared).await {
            Ok(()) => backoff = Duration::from_millis(500),
            Err(e) => tracing::error!(error = %e, "Binance WS, reconnexion"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn connect_and_stream(url: &str, shared: &Arc<Mutex<OrderBookL2>>) -> anyhow::Result<()> {
    let (ws, _) = connect_async(url).await?;
    tracing::info!(%url, "Binance WS connecté");
    let (_w, mut read) = ws.split();

    // Tampon des events le temps de récupérer le snapshot REST.
    let mut buffer: Vec<DepthEvent> = Vec::new();
    let snapshot = fetch_snapshot().await?;
    let mut last_id = snapshot.last_update_id;

    // Seed le carnet partagé.
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
            // On ignore les events entièrement antérieurs au snapshot.
            if ev.final_id <= last_id {
                continue;
            }
            buffer.push(ev);
            // Le premier event valide doit chevaucher last_id+1.
            if buffer.first().map_or(false, |e| e.first_id <= last_id + 1) {
                let mut book = shared.lock().unwrap();
                for e in buffer.drain(..) {
                    apply_levels(&mut book, true, &e.bids);
                    apply_levels(&mut book, false, &e.asks);
                    last_id = e.final_id;
                }
                synced = true;
            }
            continue;
        }

        // Flux synchronisé : appliquer en continu (tolérant aux petits trous → OBI statistique).
        if ev.final_id <= last_id {
            continue;
        }
        let mut book = shared.lock().unwrap();
        apply_levels(&mut book, true, &ev.bids);
        apply_levels(&mut book, false, &ev.asks);
        last_id = ev.final_id;
    }
    Ok(())
}
