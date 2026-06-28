//! Nœud **Radar (Tokyo)** — émetteur.
//!
//! Possède toute la chaîne 100 % CEX : WS Binance + OKX, carnets L2, OBI consolidé, vélocité,
//! FSM de persistance OBI, et le `fair_up` B&S (strike CEX-dérivé via klines). Quand la FSM sort
//! d'ARMING avec vélocité confirmée, on **tire un paquet UDP de 6 octets** vers l'exécuteur ;
//! sur vide de liquidité, on émet un KILL.
//!
//! **Event-driven (Bloc L)** : l'OBI est calculé directement dans les tasks WS via watch channels.
//! La signal task se déclenche à chaque update Binance/OKX (0 attente tick).

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

use crate::concurrency::bus::Side;
use crate::config::Config;
use crate::net::udp::UdpSender;
use crate::net::wire::WireSignal;
use crate::polymarket::relayer::btc_price_at_window_open;
use crate::pricing::black_scholes::{fair_up_probability, years_from_secs};
use crate::pricing::volatility::VolatilityTracker;
use crate::signal::consolidated_obi::ConsolidatedObi;
use crate::strategy::sniper::{Action, Sniper, TickInput};
use crate::state::RuntimeControls;
use crate::{binance, dashboard, latency, okx};
use binance::local_book::OrderBookL2;
use binance::math_engine::VelocityTracker;

const WINDOW_SEC: i64 = 300;

#[derive(Default)]
struct StrikeState {
    window_ts: i64,
    strike: Option<f64>,
}

pub async fn run(cfg: Config, target_ip: String, target_port: u16) -> anyhow::Result<()> {
    tracing::info!(%target_ip, target_port, "🛰️  RADAR (Tokyo) démarré");

    // Le radar n'exécute pas : contrôles présents seulement pour servir le dashboard.
    let controls = Arc::new(RuntimeControls::new());
    let dash = dashboard::shared(cfg.dry_run, "radar");
    {
        let (port, st, ct) = (cfg.dashboard_port, dash.clone(), controls.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct).await; });
    }

    // Sonde latence CEX uniquement (Binance/OKX).
    let lat = latency::shared();
    { let l = lat.clone(); tokio::spawn(async move { latency::run(l, latency::Probes::CexOnly).await; }); }

    let binance_book = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));
    let okx_book = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));

    // Watch channels OBI — les WS tasks envoient après chaque apply_levels().
    let (bin_tx, bin_rx) = watch::channel::<(f64, Option<f64>)>((0.0, None));
    let (okx_tx, okx_rx) = watch::channel::<f64>(0.0);

    let obi_n = cfg.obi_top_n;
    tokio::spawn(binance::websocket::run(cfg.binance_ws_url.clone(), binance_book.clone(), bin_tx, obi_n));
    tokio::spawn(okx::websocket::run(cfg.okx_ws_url.clone(), okx_book.clone(), okx_tx, obi_n));

    // Strike (prix BTC à l'ouverture de la fenêtre) — via klines Binance, dérivable côté CEX.
    let strike = Arc::new(Mutex::new(StrikeState::default()));
    spawn_strike_task(strike.clone());

    // Cible live (primaire) — toujours servie en premier.
    let live_sender = UdpSender::new(&target_ip, target_port).await?;
    // Cible paper (secondaire, optionnelle) — le radar tire AUSSI au paper, mais APRÈS le live.
    let paper_sender = match (std::env::var("TARGET_PAPER_IP"), std::env::var("TARGET_PAPER_PORT")) {
        (Ok(ip), port) if !ip.is_empty() => {
            let p = port.ok().and_then(|s| s.parse().ok()).unwrap_or(8081u16);
            match UdpSender::new(&ip, p).await {
                Ok(s) => { tracing::info!(%ip, port = p, "📝 cible paper activée (tir secondaire)"); Some(s) }
                Err(e) => { tracing::warn!(error = %e, "cible paper injoignable — live seul"); None }
            }
        }
        _ => { tracing::info!("pas de TARGET_PAPER_IP — tir live uniquement"); None }
    };

    let consolidated = ConsolidatedObi::new(
        cfg.obi_floor_per_exchange, cfg.obi_fire_threshold, cfg.weight_binance, cfg.weight_okx);
    let sniper = Sniper::new(cfg.obi_dwell_ms, cfg.cooldown_ms, 0.0, cfg.velocity_confirm);
    let vel = VelocityTracker::new(1000);
    let vol = VolatilityTracker::new(2000, 0.80);

    // Signal task event-driven — se déclenche à chaque update WS (pas de tick).
    spawn_signal_task(bin_rx, okx_rx, vel, vol, sniper, consolidated, live_sender, paper_sender, strike, cfg, dash, lat);

    tokio::signal::ctrl_c().await?;
    tracing::info!("SIGINT reçu — arrêt propre (radar)");
    Ok(())
}

fn spawn_signal_task(
    mut bin_rx: watch::Receiver<(f64, Option<f64>)>,
    mut okx_rx: watch::Receiver<f64>,
    mut vel: VelocityTracker,
    mut vol: VolatilityTracker,
    mut sniper: Sniper,
    consolidated: ConsolidatedObi,
    live_sender: UdpSender,
    paper_sender: Option<UdpSender>,
    strike: Arc<Mutex<StrikeState>>,
    cfg: Config,
    dash: dashboard::Shared,
    lat: Arc<Mutex<latency::LatencySnapshot>>,
) {
    tokio::spawn(async move {
        let mut log_throttle: u32 = 0;
        loop {
            // Se réveille dès qu'un WS envoie un nouvel OBI.
            tokio::select! {
                result = bin_rx.changed() => { if result.is_err() { break; } }
                result = okx_rx.changed() => { if result.is_err() { break; } }
            }

            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
            let (obi_b, spot_opt) = *bin_rx.borrow();
            let obi_o = *okx_rx.borrow();
            let Some(spot) = spot_opt else { continue };

            vel.update(now_ms, spot);
            vol.update(now_ms, spot);
            let velocity = vel.velocity();
            let decision = consolidated.evaluate(obi_b, obi_o);

            // Fenêtre 5 min déterministe + strike → fair_up B&S.
            let now_s = (now_ms / 1000) as i64;
            let window_ts = (now_s / WINDOW_SEC) * WINDOW_SEC;
            let remaining_s = window_ts + WINDOW_SEC - now_s;
            let strike_opt = { let s = strike.lock().unwrap(); if s.window_ts == window_ts { s.strike } else { None } };
            let mut fair_up = 0.5;
            if let Some(strk) = strike_opt {
                let t_years = years_from_secs(remaining_s.max(0) as f64);
                fair_up = fair_up_probability(spot, strk, vol.annualized_sigma(), t_years);
            }

            let liquidity_vacuum = velocity <= cfg.vacuum_velocity && obi_b <= cfg.vacuum_obi;
            let blocked = remaining_s <= cfg.end_window_block_secs;

            // FSM : real_up = fair_up → gap nul, gap_min=0 ⇒ test neutralisé (jugé à l'exécuteur).
            let input = TickInput { now_ms, decision, fair_up, real_up: fair_up, velocity, liquidity_vacuum, blocked };
            // Tir aux deux cibles : LIVE d'abord (priorité absolue à la vitesse d'exécution),
            // PAPER ensuite. Même `sent_ms` pour les deux → latence transport comparable.
            match sniper.step(&input) {
                Action::Fire { side, strength } => {
                    let sig = WireSignal::Attack { side, size: strength_to_size(strength), price: fair_up as f32, sent_ms: now_ms };
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

            // Dashboard radar (champs CEX uniquement).
            let fsm = if sniper.in_cooldown() { "COOLDOWN" } else if sniper.is_armed() { "ARMING" } else { "IDLE" };
            let lat_snap = lat.lock().unwrap().clone();
            {
                let mut d = dash.write().await;
                d.binance_connected = spot > 0.0;
                d.okx_connected = obi_o != 0.0 || { okx_rx.borrow().abs() > 0.0 };
                d.btc_spot = spot;
                d.obi_binance = obi_b; d.obi_okx = obi_o; d.obi_consolidated = decision.strength;
                d.agreement = decision.fire; d.velocity = velocity;
                d.fsm_state = fsm.into();
                d.remaining_s = remaining_s;
                d.fair_up = fair_up;
                d.liquidity_vacuum = liquidity_vacuum;
                d.cond_agreement = decision.fire;
                d.cond_persist = sniper.is_armed();
                d.cond_velocity = match decision.side {
                    Some(Side::Up) => velocity >= cfg.velocity_confirm,
                    Some(Side::Down) => velocity <= -cfg.velocity_confirm,
                    None => false,
                };
                d.cond_ready = !blocked && !liquidity_vacuum && !sniper.in_cooldown();
                d.lat_binance_ms = lat_snap.binance_ms;
                d.lat_okx_ms = lat_snap.okx_ms;
            }

            log_throttle += 1;
            if log_throttle % 500 == 0 {
                tracing::info!(obi_b = format!("{:+.2}", obi_b), obi_o = format!("{:+.2}", obi_o),
                    fair = format!("{:.3}", fair_up), fsm, "radar");
            }
        }
    });
}

/// Met à jour le strike au rollover de fenêtre (kline 1m Binance).
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

/// Taille indicative (u8) dérivée de la force du signal ∈ [0,1] → 1..=255.
/// L'exécuteur recalcule le sizing Kelly autoritaire ; ce champ n'est qu'un repère.
fn strength_to_size(strength: f64) -> u8 {
    (strength.abs() * 50.0).round().clamp(1.0, 255.0) as u8
}
