//! Connecteur WebSocket OKX (`books`, 400 niveaux) — confirmation cross-exchange.
//! Même `OrderBookL2` (bande OBI 0.15 %). Partagé via `Arc<Mutex<OrderBookL2>>`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::binance::local_book::OrderBookL2;

#[derive(Deserialize)]
struct OkxMsg {
    #[serde(default)] action: String,
    #[serde(default)] data: Vec<OkxBook>,
}
#[derive(Deserialize)]
struct OkxBook {
    #[serde(default)] bids: Vec<Vec<String>>, // [price, size, _, _]
    #[serde(default)] asks: Vec<Vec<String>>,
}

fn apply(book: &mut OrderBookL2, is_bid: bool, levels: &[Vec<String>]) {
    for lvl in levels {
        if lvl.len() < 2 {
            continue;
        }
        if let (Ok(price), Ok(qty)) = (lvl[0].parse::<f64>(), lvl[1].parse::<f64>()) {
            book.update_level(is_bid, price, qty);
        }
    }
}

pub async fn run(url: String, shared: Arc<Mutex<OrderBookL2>>) -> anyhow::Result<()> {
    let mut backoff = Duration::from_millis(500);
    loop {
        match connect_and_stream(&url, &shared).await {
            Ok(()) => backoff = Duration::from_millis(500),
            Err(e) => tracing::error!(error = %e, "OKX WS, reconnexion"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn connect_and_stream(url: &str, shared: &Arc<Mutex<OrderBookL2>>) -> anyhow::Result<()> {
    let (ws, _) = connect_async(url).await?;
    let (mut write, mut read) = ws.split();
    let sub = r#"{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}"#;
    write.send(Message::Text(sub.to_string())).await?;
    tracing::info!(%url, "OKX WS connecté + abonné books BTC-USDT");

    while let Some(msg) = read.next().await {
        let txt = match msg? {
            Message::Text(t) => t,
            Message::Ping(p) => { let _ = write.send(Message::Pong(p)).await; continue; }
            Message::Close(_) => return Ok(()),
            _ => continue,
        };
        let m: OkxMsg = match serde_json::from_str(&txt) {
            Ok(m) => m,
            Err(_) => continue, // events de souscription/erreur
        };
        if m.data.is_empty() {
            continue;
        }
        let mut book = shared.lock().unwrap();
        if m.action == "snapshot" {
            book.bids.clear();
            book.asks.clear();
        }
        for d in &m.data {
            apply(&mut book, true, &d.bids);
            apply(&mut book, false, &d.asks);
        }
    }
    Ok(())
}
