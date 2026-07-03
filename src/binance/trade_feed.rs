//! Task B : flux aggTrade Binance → TFI (Trade Flow Imbalance) O(1).
//! Écrit dans un Arc<AtomicU64> — zéro verrou partagé avec le hot loop.
//!
//! URL : `wss://stream.binance.com:9443/ws/btcusdt@aggTrade`
//! JSON : `{ "E": ts_ms, "q": qty_str, "m": is_buyer_maker }`

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize)]
struct AggTrade {
    #[serde(rename = "E")] ts_ms: u64,
    #[serde(rename = "q")] qty: String,
    #[serde(rename = "m")] is_buyer_maker: bool,
}

/// TFI O(1) avec running sums — `window_ms` de mémoire glissante.
pub struct TFITracker {
    window_ms: u64,
    history: VecDeque<(u64, f64, bool)>,
    running_buy: f64,
    running_sell: f64,
}

impl TFITracker {
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            history: VecDeque::with_capacity(4096),
            running_buy: 0.0,
            running_sell: 0.0,
        }
    }

    pub fn update(&mut self, now_ms: u64, qty: f64, is_buy: bool) {
        if is_buy { self.running_buy += qty } else { self.running_sell += qty }
        self.history.push_back((now_ms, qty, is_buy));
        let cutoff = now_ms.saturating_sub(self.window_ms);
        while let Some(&(ts, q, b)) = self.history.front() {
            if ts < cutoff {
                if b { self.running_buy -= q } else { self.running_sell -= q }
                self.history.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn tfi(&self) -> f64 {
        let total = self.running_buy + self.running_sell;
        if total < 1e-9 { return 0.0; }
        (self.running_buy - self.running_sell) / total
    }
}

/// Lance la Task B aggTrade. À exécuter dans `tokio::spawn`.
pub async fn run_agg_trade(ws_url: String, tfi_atomic: Arc<AtomicU64>, window_ms: u64) {
    let mut backoff = Duration::from_millis(500);
    loop {
        match stream_once(&ws_url, &tfi_atomic, window_ms).await {
            Ok(()) => backoff = Duration::from_millis(500),
            Err(e) => tracing::error!(error = %e, "aggTrade WS, reconnexion"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

async fn stream_once(
    url: &str,
    tfi_atomic: &Arc<AtomicU64>,
    window_ms: u64,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(url).await?;
    let (_, mut read) = ws.split();
    tracing::info!(%url, "aggTrade WS connecté (Task B)");
    let mut tracker = TFITracker::new(window_ms);
    loop {
        // Watchdog anti connexion à moitié morte : 60 s sans trade BTC = jamais → reconnexion.
        let msg = match tokio::time::timeout(Duration::from_secs(60), read.next()).await {
            Err(_) => {
                tracing::warn!("aggTrade WS silencieux 60 s — reconnexion forcée");
                return Ok(());
            }
            Ok(None) => return Ok(()),
            Ok(Some(m)) => m,
        };
        let txt = match msg? {
            Message::Text(t) => t,
            Message::Close(_) => return Ok(()),
            _ => continue,
        };
        let t: AggTrade = match serde_json::from_str(&txt) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let qty: f64 = match t.qty.parse() {
            Ok(q) => q,
            Err(_) => continue,
        };
        tracker.update(t.ts_ms, qty, !t.is_buyer_maker);
        tfi_atomic.store(tracker.tfi().to_bits(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_tfi_is_zero() {
        let mut t = TFITracker::new(5000);
        t.update(1000, 10.0, true);
        t.update(1000, 10.0, false);
        assert!(t.tfi().abs() < 1e-9);
    }

    #[test]
    fn all_buy_gives_one() {
        let mut t = TFITracker::new(5000);
        t.update(1000, 10.0, true);
        t.update(2000, 5.0, true);
        assert!((t.tfi() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn old_trades_expire() {
        let mut t = TFITracker::new(5000);
        t.update(0, 100.0, false);     // vieux sell expiré à t=7000
        t.update(7000, 10.0, true);    // seul buy récent
        assert!(t.tfi() > 0.9, "tfi={}", t.tfi());
    }
}
