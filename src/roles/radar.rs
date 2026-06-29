//! Nœud **Radar (Tokyo)** — émetteur.
//!
//! Possède toute la chaîne 100 % CEX : WS Binance + OKX, carnets L2, OBI multilevel,
//! microprice, TFI aggTrade, Kalman, basis, score composite, B&S d2-shift.
//! Quand la FSM sort d'ARMING avec score confirmé, on **tire un paquet UDP** vers les exécuteurs.
//!
//! **Event-driven (Bloc L)** : l'OBI est calculé dans les tasks WS via watch channels.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use crate::binance::math_engine::VelocityTracker;
use crate::binance::trade_feed::run_agg_trade;
use crate::config::Config;
use crate::net::udp::UdpSender;
use crate::net::wire::WireSignal;
use crate::polymarket::relayer::btc_price_at_window_open;
use crate::pricing::black_scholes::{d2 as bs_d2, fair_up_with_d2_shift, years_from_secs};
use crate::pricing::volatility::{EwmaVolatility, VolatilityTracker};
use crate::signal::basis::BasisSignal;
use crate::signal::composite::{self, CompositeWeights};
use crate::signal::kalman::KalmanFilter;
use crate::strategy::bankroll::EmaScoreStat;
use crate::strategy::sniper::{Action, Sniper, TickInput};
use crate::state::RuntimeControls;
use crate::{binance, dashboard, latency, okx};
use binance::local_book::OrderBookL2;

const WINDOW_SEC: i64 = 300;

#[derive(Default)]
struct StrikeState {
    window_ts: i64,
    strike: Option<f64>,
}

pub async fn run(cfg: Config, target_ip: String, target_port: u16) -> anyhow::Result<()> {
    tracing::info!(%target_ip, target_port, "🛰️  RADAR (Tokyo) démarré");

    let controls = Arc::new(RuntimeControls::new());
    let dash = dashboard::shared(cfg.dry_run, "radar");
    {
        let (port, st, ct) = (cfg.dashboard_port, dash.clone(), controls.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct, None).await; });
    }

    let lat = latency::shared();
    { let l = lat.clone(); tokio::spawn(async move { latency::run(l, latency::Probes::CexOnly).await; }); }

    let binance_book = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));
    let okx_book    = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));

    let (bin_tx, bin_rx) = watch::channel::<(f64, Option<f64>, Option<f64>)>((0.0, None, None));
    let (okx_tx, okx_rx) = watch::channel::<(f64, f64)>((0.0, 0.0));

    let okx_ts_atomic = Arc::new(AtomicU64::new(0u64));
    let tfi_atomic    = Arc::new(AtomicU64::new(0u64));

    let obi_n = cfg.obi_top_n;
    tokio::spawn(binance::websocket::run(
        cfg.binance_ws_url.clone(), binance_book.clone(),
        bin_tx, obi_n, cfg.obi_multilevel_lambda,
    ));
    tokio::spawn(okx::websocket::run(
        cfg.okx_ws_url.clone(), okx_book.clone(),
        okx_tx, obi_n, Arc::clone(&okx_ts_atomic),
    ));
    tokio::spawn(run_agg_trade(
        cfg.agg_trade_ws_url.clone(), Arc::clone(&tfi_atomic), cfg.tfi_window_ms,
    ));

    let strike = Arc::new(Mutex::new(StrikeState::default()));
    spawn_strike_task(strike.clone());

    let live_sender = UdpSender::new(&target_ip, target_port).await?;
    let paper_sender = match (std::env::var("TARGET_PAPER_IP"), std::env::var("TARGET_PAPER_PORT")) {
        (Ok(ip), port) if !ip.is_empty() => {
            let p = port.ok().and_then(|s| s.parse().ok()).unwrap_or(8081u16);
            match UdpSender::new(&ip, p).await {
                Ok(s) => { tracing::info!(%ip, port = p, "📝 cible paper activée"); Some(s) }
                Err(e) => { tracing::warn!(error = %e, "cible paper injoignable — live seul"); None }
            }
        }
        _ => { tracing::info!("pas de TARGET_PAPER_IP — tir live uniquement"); None }
    };

    let sniper = Sniper::new(cfg.obi_dwell_ms, cfg.cooldown_ms, 0.0, cfg.score_fire_threshold);

    spawn_signal_task(
        bin_rx, okx_rx, tfi_atomic, okx_ts_atomic,
        sniper, live_sender, paper_sender, strike, cfg, dash, lat,
    );

    tokio::signal::ctrl_c().await?;
    tracing::info!("SIGINT reçu — arrêt propre (radar)");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_signal_task(
    mut bin_rx: watch::Receiver<(f64, Option<f64>, Option<f64>)>,
    mut okx_rx: watch::Receiver<(f64, f64)>,
    tfi_atomic: Arc<AtomicU64>,
    okx_ts_atomic: Arc<AtomicU64>,
    mut sniper: Sniper,
    live_sender: UdpSender,
    paper_sender: Option<UdpSender>,
    strike: Arc<Mutex<StrikeState>>,
    cfg: Config,
    dash: dashboard::Shared,
    lat: Arc<Mutex<latency::LatencySnapshot>>,
) {
    tokio::spawn(async move {
        let weights = CompositeWeights {
            w_obi: cfg.composite_w_obi, w_tfi: cfg.composite_w_tfi,
            w_kalman: cfg.composite_w_kalman, w_basis: cfg.composite_w_basis,
        };
        let mut kalman = KalmanFilter::new(
            cfg.kalman_q00, cfg.kalman_q11, cfg.kalman_r,
            cfg.kalman_spike_sigma, cfg.kalman_reset_after_n,
        );
        let mut vol   = VolatilityTracker::new(2000, 0.80);
        let mut ewma  = EwmaVolatility::new(cfg.ewma_lambda, 0.80);
        let mut vel   = VelocityTracker::new(1000);
        let mut basis = BasisSignal::new(cfg.basis_stale_ms, cfg.basis_threshold_usd, cfg.basis_lambda);
        let mut ema_stat = EmaScoreStat::new(cfg.ewma_score_lambda);
        let mut prev_spot: Option<f64> = None;
        let mut log_throttle: u32 = 0;

        loop {
            tokio::select! {
                result = bin_rx.changed() => { if result.is_err() { break; } }
                result = okx_rx.changed() => { if result.is_err() { break; } }
            }

            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
            let (obi_b, spot_opt, microprice_opt) = *bin_rx.borrow();
            let (obi_o, mid_okx) = *okx_rx.borrow();
            let Some(spot) = spot_opt else { continue };

            vel.update(now_ms, spot);
            vol.update(now_ms, spot);
            let velocity = vel.velocity(); // pour vacuum + dashboard

            let micro = microprice_opt.unwrap_or(spot);
            kalman.update(now_ms, micro);

            if let Some(prev) = prev_spot {
                if prev > 0.0 { ewma.update(now_ms, (spot / prev).ln()); }
            }
            prev_spot = Some(spot);

            let tfi    = f64::from_bits(tfi_atomic.load(Ordering::Relaxed));
            let okx_ts = okx_ts_atomic.load(Ordering::Relaxed);

            let (basis_norm, basis_unc) = basis.evaluate(micro, mid_okx, okx_ts, now_ms);
            let vel_norm = (kalman.velocity() / cfg.vel_norm_factor).clamp(-1.0, 1.0);
            let score = composite::score(obi_b, tfi, vel_norm, basis_norm, basis_unc, &weights);
            ema_stat.update(score);
            let score_sigma = ema_stat.std_dev();

            // Fenêtre 5 min déterministe + strike → fair_up B&S avec d2 shift.
            let now_s = (now_ms / 1000) as i64;
            let window_ts = (now_s / WINDOW_SEC) * WINDOW_SEC;
            let remaining_s = window_ts + WINDOW_SEC - now_s;
            let strike_opt = { let s = strike.lock().unwrap(); if s.window_ts == window_ts { s.strike } else { None } };

            let sigma_realized = vol.annualized_sigma();
            let sigma_ewma = ewma.annualized_sigma();
            let sigma_blended = 0.5 * sigma_realized + 0.5 * sigma_ewma;
            let (mut d2_base, mut d2_adj, mut strike_val) = (0.0, 0.0, 0.0);
            let fair_up = if let Some(strk) = strike_opt {
                let t_years = years_from_secs(remaining_s.max(0) as f64);
                strike_val = strk;
                d2_base = bs_d2(spot, strk, sigma_blended, t_years);
                d2_adj = d2_base + cfg.d2_gamma * score;
                fair_up_with_d2_shift(spot, strk, sigma_blended, t_years, score, cfg.d2_gamma)
            } else { 0.5 };

            let liquidity_vacuum = velocity <= cfg.vacuum_velocity && obi_b <= cfg.vacuum_obi;
            let blocked = remaining_s <= cfg.end_window_block_secs;

            let input = TickInput {
                now_ms, score, score_sigma, basis_unc,
                fair_up, real_up: fair_up, // radar : pas de prix PM → gap nul (gap_min=0)
                kalman_velocity: kalman.velocity(),
                liquidity_vacuum, blocked,
            };
            match sniper.step(&input) {
                Action::Fire { side, strength } => {
                    let sig = WireSignal::Attack {
                        side, size: strength_to_size(strength), price: fair_up as f32, sent_ms: now_ms
                    };
                    live_sender.send(sig).await;
                    if let Some(p) = &paper_sender { p.send(sig).await; }
                }
                Action::Kill => {
                    let sig = WireSignal::Kill { sent_ms: now_ms };
                    live_sender.send(sig).await;
                    if let Some(p) = &paper_sender { p.send(sig).await; }
                }
                Action::None => {}
            }

            let fsm = if sniper.in_cooldown() { "COOLDOWN" } else if sniper.is_armed() { "ARMING" } else { "IDLE" };
            let lat_snap = lat.lock().unwrap().clone();
            {
                let mut d = dash.write().await;
                d.binance_connected = spot > 0.0;
                d.okx_connected = obi_o != 0.0;
                d.btc_spot = spot;
                d.obi_binance = obi_b; d.obi_okx = obi_o; d.obi_consolidated = score;
                d.microprice = micro; d.tfi = tfi;
                d.kalman_velocity = kalman.velocity(); d.vel_norm = vel_norm;
                d.basis_norm = basis_norm; d.basis_unc = basis_unc;
                d.score = score; d.score_sigma = score_sigma;
                d.sigma_realized = sigma_realized; d.sigma_ewma = sigma_ewma; d.sigma_blended = sigma_blended;
                d.d2_base = d2_base; d.d2_adj = d2_adj; d.strike = strike_val;
                d.agreement = score.abs() >= cfg.score_fire_threshold;
                d.velocity = velocity;
                d.fsm_state = fsm.into();
                d.remaining_s = remaining_s;
                d.fair_up = fair_up;
                d.liquidity_vacuum = liquidity_vacuum;
                d.cond_agreement = score.abs() >= cfg.score_fire_threshold;
                d.cond_persist = sniper.is_armed();
                d.cond_velocity = vel_norm.abs() > 0.1;
                d.cond_ready = !blocked && !liquidity_vacuum && !sniper.in_cooldown();
                d.lat_binance_ms = lat_snap.binance_ms;
                d.lat_okx_ms = lat_snap.okx_ms;
            }

            log_throttle += 1;
            if log_throttle % 500 == 0 {
                tracing::info!(
                    score = format!("{:+.3}", score),
                    obi_b = format!("{:+.2}", obi_b),
                    obi_o = format!("{:+.2}", obi_o),
                    tfi = format!("{:+.2}", tfi),
                    fair = format!("{:.3}", fair_up),
                    fsm, "radar"
                );
            }
        }
    });
}

fn spawn_strike_task(strike: Arc<Mutex<StrikeState>>) {
    tokio::spawn(async move {
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        loop {
            poll.tick().await;
            let now_s = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            let window_ts = (now_s / WINDOW_SEC) * WINDOW_SEC;
            let need = { let s = strike.lock().unwrap(); s.window_ts != window_ts || s.strike.is_none() };
            if need {
                if let Ok(px) = btc_price_at_window_open(window_ts).await {
                    let mut s = strike.lock().unwrap();
                    s.window_ts = window_ts; s.strike = Some(px);
                    tracing::info!(window_ts, strike = px, "=== strike fenêtre (radar) ===");
                }
            }
        }
    });
}

fn strength_to_size(strength: f64) -> u8 {
    (strength.abs() * 50.0).round().clamp(1.0, 255.0) as u8
}
