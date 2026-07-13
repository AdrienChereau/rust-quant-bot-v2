//! Nœud Radar (Tokyo).
//! J1 : connecteur Binance + carnet L2.
//! J2 : boucle 10 Hz `RadarEngine` (OBI + vélocité) → émission `Signal::Kill`.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::watch;

use crate::config::Config;
use crate::connectors::binance;
use crate::dashboard::Shared;
use crate::engines::radar::RadarEngine;
use crate::engines::{drift::DriftEngine, ofi::OfiEngine, volatility::VolatilityEngine};
use crate::signal::SignalTransport;
use crate::types::{BookUpdate, Signal, WireTick};

pub async fn run(cfg: Config, transport: Arc<dyn SignalTransport>, dash: Shared) -> anyhow::Result<()> {
    tracing::info!(
        role = "radar",
        binance_ws = %cfg.binance_ws_url,
        obi_threshold = cfg.obi_threshold,
        velocity_threshold = cfg.velocity_threshold,
        "Nœud Radar démarré"
    );

    let (tx, rx) = watch::channel::<Option<BookUpdate>>(None);

    let url = cfg.binance_ws_url.clone();
    tokio::spawn(async move {
        if let Err(e) = binance::run(url, tx).await {
            tracing::error!(error = %e, "connecteur Binance arrêté");
        }
    });

    let mut engine = RadarEngine::new(
        cfg.obi_depth_levels,
        cfg.obi_threshold,
        cfg.velocity_threshold,
    );
    // v9 : le signal COMPLET est calculé ICI, au plus près de Binance —
    // drift (tendance), sigma (pricing), OFI — puis émis à 10 Hz vers Dublin.
    let mut drift_eng = DriftEngine::new(cfg.drift_halflife_secs);
    let mut vol_eng = VolatilityEngine::new(2000, cfg.volatility_floor);
    let mut ofi_eng = OfiEngine::new(5000);
    let mut seq: u64 = 0;

    // Boucle stricte à 10 Hz : échantillonne le dernier carnet et analyse.
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Anti-spam : on ne ré-émet pas un KILL plus d'une fois par seconde.
    let mut last_kill_ms: u64 = 0;
    let mut log_throttle: u8 = 0;
    let mut kills_emitted: u64 = 0;
    // Journal d'événements pour l'UI Tokyo (connexions, kills, silences).
    {
        let mut d = dash.write().await;
        crate::dashboard::radar_log(&mut d, "◉ nœud radar démarré — feed Binance en cours de connexion");
    }
    let mut was_connected = false;
    let mut last_micro = 0.0f64;
    let mut micro_still_since: u64 = 0;
    // CHEMIN CHAUD (passe 3) : ring ~800 ms du micro-price — l'impulsion est
    // le déplacement relatif sur ~500 ms. Une explosion de fenêtre (5 s) se
    // voit ici en 300-500 ms, là où l'EMA drift (halflife 25 s) met ~2 s.
    let mut imp_ring: std::collections::VecDeque<(u64, f64)> = std::collections::VecDeque::new();
    let mut stale_warned = false;

    loop {
        tick.tick().await;

        let snapshot = rx.borrow().clone();
        let Some(update) = snapshot else { continue };

        let now_ms = Utc::now().timestamp_millis() as u64;
        let obi = engine.calculate_obi(&update.book);
        let micro = update.book.calculate_micro_price().unwrap_or(0.0);
        let maybe_kill = engine.tick(update.ts_ms, &update.book);

        // Tick signal complet → Dublin (drift/σ/OFI made in Tokyo).
        if micro > 0.0 {
            drift_eng.update(update.ts_ms, micro);
            vol_eng.update(update.ts_ms, micro);
            let bid_sz = update.book.bids.values().next().copied().unwrap_or(0.0);
            let ask_sz = update.book.asks.values().next().copied().unwrap_or(0.0);
            if let (Some(bb), Some(ba)) = (update.book.best_bid(), update.book.best_ask()) {
                ofi_eng.update(update.ts_ms, bb, bid_sz, ba, ask_sz);
            }
            imp_ring.push_back((update.ts_ms, micro));
            while imp_ring
                .front()
                .is_some_and(|(t0, _)| update.ts_ms.saturating_sub(*t0) > 800)
            {
                imp_ring.pop_front();
            }
            // Base : l'échantillon le plus récent d'âge ≥ 400 ms (fenêtre réelle
            // 400-800 ms selon la cadence du feed).
            let impulse = imp_ring
                .iter()
                .filter(|(t0, _)| update.ts_ms.saturating_sub(*t0) >= 400)
                .next_back()
                .map(|(_, p0)| if *p0 > 0.0 { (micro - p0) / p0 } else { 0.0 })
                .unwrap_or(0.0);
            seq += 1;
            let t = WireTick {
                seq,
                ts_ms: now_ms,
                spot: micro,
                sigma: vol_eng.annualized_sigma(),
                drift: drift_eng.per_sec(),
                ofi: ofi_eng.value_norm(),
                obi,
                velocity: maybe_kill.is_some() as u8 as f64, // 1.0 = KILL armé ce tick (la vélocité brute reste interne au RadarEngine)
                impulse,
            };
            if let Err(e) = transport.send_signal(Signal::Tick(t)).await {
                tracing::error!(error = %e, "échec d'émission du tick signal");
            }
        }

        if maybe_kill.is_some() && now_ms.saturating_sub(last_kill_ms) >= 1000 {
            last_kill_ms = now_ms;
            kills_emitted += 1;
            tracing::warn!(obi = format!("{:+.3}", obi), "⚡ KILL détecté — émission du signal");
            if let Err(e) = transport.send_signal(crate::types::Signal::Kill).await {
                tracing::error!(error = %e, "échec d'émission du signal KILL");
            } else {
                let mut d = dash.write().await;
                crate::dashboard::radar_log(&mut d, format!("⚡ KILL émis — OBI {obi:+.2}, carnet en emballement"));
            }
        }

        // Événements de santé du feed (pour le journal de l'UI).
        let connected_now = micro > 0.0;
        if connected_now != was_connected {
            let mut d = dash.write().await;
            crate::dashboard::radar_log(&mut d, if connected_now {
                "✓ feed Binance connecté — émission du signal vers Dublin"
            } else {
                "✗ feed Binance perdu — reconnexion en cours"
            });
            was_connected = connected_now;
        }
        if (micro - last_micro).abs() > f64::EPSILON {
            last_micro = micro;
            micro_still_since = now_ms;
            if stale_warned {
                stale_warned = false;
                let mut d = dash.write().await;
                crate::dashboard::radar_log(&mut d, "✓ spot de nouveau vivant");
            }
        } else if !stale_warned && micro > 0.0 && now_ms.saturating_sub(micro_still_since) > 8_000 {
            stale_warned = true;
            let mut d = dash.write().await;
            crate::dashboard::radar_log(&mut d, "⚠ spot figé depuis 8 s — surveillance zombie");
        }

        // Mise à jour du dashboard (état radar).
        {
            let mut d = dash.write().await;
            d.binance_connected = micro > 0.0;
            d.btc_micro = micro;
            d.obi = obi;
            d.kills_emitted = kills_emitted;
            d.seq = seq;
            d.drift = drift_eng.per_sec();
            d.sigma = vol_eng.annualized_sigma();
            d.ofi = ofi_eng.value_norm();
        }

        log_throttle = log_throttle.wrapping_add(1);
        if log_throttle % 10 == 0 && micro > 0.0 {
            tracing::info!(obi = format!("{:+.3}", obi), micro = format!("{:.2}", micro), "radar");
        }
    }
}
