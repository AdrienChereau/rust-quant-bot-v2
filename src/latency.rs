//! Sonde TCP périodique vers les exchanges — mesure la latence one-way aller.
//! Tourne dans une tâche séparée toutes les PROBE_INTERVAL_S secondes.
//! Zéro TLS : le TCP connect seul donne le RTT réseau utile.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::time::timeout;

const PROBE_INTERVAL_S: u64 = 5;
const PROBE_TIMEOUT_MS: u64 = 3000;

const BINANCE_ADDR: &str = "stream.binance.com:9443";
const OKX_ADDR: &str = "ws.okx.com:8443";
const PM_ADDR: &str = "ws-subscriptions-clob.polymarket.com:443";

/// Quelles cibles sonder selon le rôle du nœud.
#[derive(Debug, Clone, Copy)]
pub enum Probes {
    /// Mono / local : Binance + OKX + Polymarket.
    All,
    /// Radar (Tokyo) : seulement les flux CEX en amont.
    CexOnly,
    /// Exécuteur (Dublin) : seulement le flux Polymarket.
    PmOnly,
}

#[derive(Debug, Clone, Default)]
pub struct LatencySnapshot {
    pub binance_ms:     Option<f64>,
    pub okx_ms:         Option<f64>,
    pub polymarket_ms:  Option<f64>,
}

pub type SharedLatency = Arc<Mutex<LatencySnapshot>>;

pub fn shared() -> SharedLatency {
    Arc::new(Mutex::new(LatencySnapshot::default()))
}

async fn probe(addr: &str) -> Option<f64> {
    let t0 = Instant::now();
    let ok = timeout(
        Duration::from_millis(PROBE_TIMEOUT_MS),
        TcpStream::connect(addr),
    )
    .await
    .ok()?
    .ok()?;
    drop(ok);
    Some(t0.elapsed().as_secs_f64() * 1000.0)
}

pub async fn run(shared: SharedLatency, probes: Probes) {
    let mut interval = tokio::time::interval(Duration::from_secs(PROBE_INTERVAL_S));
    loop {
        interval.tick().await;
        // CEX en parallèle, ou PM en parallèle
        let (b, o) = match probes {
            Probes::All | Probes::CexOnly => tokio::join!(probe(BINANCE_ADDR), probe(OKX_ADDR)),
            Probes::PmOnly => (None, None),
        };
        let p = match probes {
            Probes::All | Probes::PmOnly => probe(PM_ADDR).await,
            Probes::CexOnly => None,
        };
        {
            let mut snap = shared.lock().unwrap();
            snap.binance_ms    = b;
            snap.okx_ms        = o;
            snap.polymarket_ms = p;
        }
        tracing::debug!(
            binance  = b.map(|v| format!("{v:.0}ms")).as_deref().unwrap_or("—"),
            okx      = o.map(|v| format!("{v:.0}ms")).as_deref().unwrap_or("—"),
            pm       = p.map(|v| format!("{v:.0}ms")).as_deref().unwrap_or("—"),
            "latence"
        );
    }
}
