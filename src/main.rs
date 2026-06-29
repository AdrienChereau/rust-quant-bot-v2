//! `rust-quant-bot` — sniper front-running Binance+OKX → Polymarket. Paper-first.
//!
//! Trois modes (CLI `clap`) :
//!   - `radar`    : nœud Tokyo, émet les signaux OBI en UDP (`roles::radar`).
//!   - `executor` : nœud Dublin, écoute l'UDP + exécute en paper (`roles::executor`).
//!   - `mono`     : radar+exécuteur dans le même processus (défaut, run local/cloudy) — ci-dessous.
//!   - `poly`     : outils Polymarket (verify, derive-creds, dry-order) — ops Rust sans Python.

mod binance;
mod concurrency;
mod config;
mod dashboard;
mod latency;
mod logbuffer;
mod net;
mod okx;
mod polymarket;
mod pricing;
mod roles;
mod series;
mod signal;
mod state;
mod strategy;
mod tuning;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::sync::watch;

use binance::local_book::OrderBookL2;
use binance::math_engine::VelocityTracker;
use binance::trade_feed::run_agg_trade;
use config::Config;
use pricing::black_scholes::{d2 as bs_d2, fair_up_with_d2_shift, years_from_secs};
use pricing::volatility::{EwmaVolatility, VolatilityTracker};
use polymarket::cli::{self, PolyCmd};
use polymarket::live_executor::{self, LiveCredentials};
use polymarket::pm_poller::{spawn_pm_poller, PmShared};
use polymarket::pm_websocket;
use signal::basis::BasisSignal;
use signal::composite::{self, CompositeWeights};
use signal::kalman::KalmanFilter;
use state::RuntimeControls;
use strategy::bankroll::{self, EmaScoreStat, IcTracker, KellyParams, PaperEngine};
use strategy::live_position::LivePositionManager;
use strategy::sniper::{Action, Sniper, TickInput};

#[derive(Parser)]
#[command(author, version, about = "HFT Polymarket Bot — Radar (Tokyo) / Executor (Dublin)")]
struct Cli {
    #[command(subcommand)]
    mode: Option<Mode>,
}

#[derive(Subcommand)]
enum Mode {
    /// Nœud Radar (Tokyo) : écoute Binance/OKX, calcule l'OBI, tire en UDP vers live (+ paper).
    Radar {
        /// IP du nœud live (Dublin). Fallback env `TARGET_LIVE_IP` (ou legacy `TARGET_EXECUTOR_IP`).
        #[arg(short, long, env = "TARGET_LIVE_IP")]
        target_ip: String,
        /// Port UDP du nœud live. Fallback env `TARGET_LIVE_PORT` (ou legacy `TARGET_PORT`).
        #[arg(long, env = "TARGET_LIVE_PORT", default_value = "8080")]
        target_port: u16,
    },
    /// Nœud Live (Dublin) : écoute l'UDP et exécute RÉELLEMENT sur Polymarket (zéro code paper).
    Live {
        /// Port UDP d'écoute. Fallback env `LISTEN_PORT`.
        #[arg(long, env = "LISTEN_PORT", default_value = "8080")]
        listen_port: u16,
    },
    /// Nœud Paper (machine séparée) : écoute l'UDP et simule (zéro code live).
    Paper {
        /// Port UDP d'écoute. Fallback env `PAPER_LISTEN_PORT`.
        #[arg(long, env = "PAPER_LISTEN_PORT", default_value = "8081")]
        listen_port: u16,
    },
    /// Alias rétro-compatible de `live` (ancien nom).
    Executor {
        #[arg(long, env = "LISTEN_PORT", default_value = "8080")]
        listen_port: u16,
    },
    /// Mono : radar + exécuteur in-process (loopback). Mode par défaut (run local / cloudy).
    Mono,
    /// Outils Polymarket (setup + preflight) — remplace scripts Python sur AWS.
    Poly {
        #[command(subcommand)]
        cmd: PolyCmd,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // .env puis .env.local (local écrase) — sur AWS, un seul `.env` suffit.
    dotenvy::dotenv().ok();
    dotenvy::from_filename_override(".env.local").ok();
    // Logs : tee stdout/journald + ring buffer en mémoire (endpoint /logs sur tous les nœuds).
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_ansi(false)
        .with_writer(logbuffer::maker())
        .init();
    let cfg = Config::from_env();

    match Cli::parse().mode.unwrap_or(Mode::Mono) {
        Mode::Radar { target_ip, target_port } => roles::radar::run(cfg, target_ip, target_port).await,
        Mode::Live { listen_port } => roles::live::run(cfg, listen_port).await,
        Mode::Paper { listen_port } => roles::paper::run(cfg, listen_port).await,
        Mode::Executor { listen_port } => roles::live::run(cfg, listen_port).await,
        Mode::Mono => run_mono(cfg).await,
        Mode::Poly { cmd } => cli::run(cmd, cfg).await,
    }
}

/// Mode mono-processus historique : radar + exécuteur event-driven.
/// Le FSM sniper se déclenche sur chaque update WS Binance/OKX (Bloc L).
/// La gestion de position (TP/SL/max-hold) et le dashboard tournent sur un tick 50ms séparé.
async fn run_mono(cfg: Config) -> anyhow::Result<()> {
    tracing::info!(dry_run = cfg.dry_run, "🎯 rust-quant-bot (sniper, mono) démarré");

    // Contrôle d'exécution lock-free (pause/live/breaker), partagé avec le dashboard.
    let controls = Arc::new(RuntimeControls::new());
    let live_creds = LiveCredentials::from_env();
    if let Some(ref c) = live_creds {
        if let Err(e) = live_executor::startup_poly(c).await {
            tracing::error!(error = %e, "🛑 startup Polymarket échoué — arrêt");
            return Err(e);
        }
    }
    if cfg.live_armed {
        tracing::warn!(creds = live_creds.is_some(), "⚠️  LIVE_ARMED=true — envoi réel possible (si signature vérifiée)");
    }
    if cfg.live_force_min_size {
        tracing::warn!("⚠️  LIVE_FORCE_MIN_SIZE=true — taille minimale forcée (Kelly ignoré, agressif)");
    }

    // Vraie collatéral USDC (CLOB) — bankroll pour le sizing LIVE (jamais le cash paper).
    let live_bankroll = Arc::new(Mutex::new(None::<f64>));
    if let Some(creds) = live_creds.clone() {
        let bk = live_bankroll.clone();
        let poll_secs = cfg.bankroll_poll_secs;
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(Duration::from_secs(poll_secs));
            loop {
                poll.tick().await;
                match live_executor::get_collateral_balance(&creds).await {
                    Ok(usdc) => { *bk.lock().unwrap() = Some(usdc);
                        tracing::info!(usdc = format!("{usdc:.2}"), "💰 bankroll réelle CLOB"); }
                    Err(e) => tracing::warn!(error = %e, "lecture bankroll CLOB échouée"),
                }
            }
        });
    }

    // Console de tuning à chaud (snapshot lock-free partagé avec le dashboard).
    let tuning = tuning::Tuning::load(&cfg);

    let dash = dashboard::shared(cfg.dry_run, "mono");
    dash.write().await.trades_path =
        std::env::var("TRADES_PATH").unwrap_or_else(|_| "data/sniper_trades.jsonl".into());
    {
        let (port, st, ct, tn) = (cfg.dashboard_port, dash.clone(), controls.clone(), tuning.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct, Some(tn)).await; });
    }

    // Sonde de latence TCP (toutes les 5 s, hors hot-loop) — mono = les trois cibles.
    let lat = latency::shared();
    { let l = lat.clone(); tokio::spawn(async move { latency::run(l, latency::Probes::All).await; }); }

    // Watch channels OBI — les WS tasks envoient après chaque apply_levels().
    let (bin_tx, mut bin_rx) = watch::channel::<(f64, Option<f64>, Option<f64>)>((0.0, None, None));
    let (okx_tx, mut okx_rx) = watch::channel::<(f64, f64)>((0.0, 0.0));

    // AtomicU64 lock-free : Task B (aggTrade) → tfi_atomic ; Task C (OKX) → okx_ts_atomic.
    let tfi_atomic    = Arc::new(AtomicU64::new(0u64));
    let okx_ts_atomic = Arc::new(AtomicU64::new(0u64));

    let obi_n = cfg.obi_top_n;
    let binance_book = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));
    let okx_book = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));
    tokio::spawn(binance::websocket::run(
        cfg.binance_ws_url.clone(), binance_book.clone(),
        bin_tx, obi_n, cfg.obi_multilevel_lambda,
    ));
    tokio::spawn(okx::websocket::run(
        cfg.okx_ws_url.clone(), okx_book.clone(),
        okx_tx, obi_n, Arc::clone(&okx_ts_atomic),
    ));
    // Task B : aggTrade → TFI O(1) → AtomicU64 (zéro verrou avec le hot loop)
    tokio::spawn(run_agg_trade(
        cfg.agg_trade_ws_url.clone(), Arc::clone(&tfi_atomic), cfg.tfi_window_ms,
    ));

    // État Polymarket, rafraîchi toutes les 1 s (hors hot-loop). `true` = on a besoin du strike.
    let pm = Arc::new(Mutex::new(PmShared::default()));
    let ws_market_tx = pm_websocket::init_market_ws(pm.clone());
    spawn_pm_poller(pm.clone(), true, Some(ws_market_tx), live_creds.clone(), cfg.pm_ws_stale_threshold_ms);

    // Moteurs signal stack v2.
    let mut sniper = Sniper::new(cfg.obi_dwell_ms, cfg.cooldown_ms, cfg.gap_min, cfg.score_fire_threshold);
    let mut vel   = VelocityTracker::new(1000); // pour liquidity_vacuum (unités relatives)
    let mut vol   = VolatilityTracker::new(2000, 0.80);
    let mut ewma  = EwmaVolatility::new(cfg.ewma_lambda, 0.80);
    let mut kalman = KalmanFilter::new(
        cfg.kalman_q00, cfg.kalman_q11, cfg.kalman_r,
        cfg.kalman_spike_sigma, cfg.kalman_reset_after_n,
    );
    let mut basis = BasisSignal::new(cfg.basis_stale_ms, cfg.basis_threshold_usd, cfg.basis_lambda);
    // `weights` et les seuils du sniper/Kelly sont reconstruits à chaque tick depuis le snapshot
    // de tuning (console à chaud) — voir le hot loop plus bas.
    let mut ema_stat = EmaScoreStat::new(cfg.ewma_score_lambda);
    let mut ic_tracker = IcTracker::new(200);
    let mut prev_spot: Option<f64> = None;
    let mut last_fire_score: f64 = 0.0;

    let kelly = KellyParams {
        kelly_fraction: cfg.kelly_fraction, max_size_pct: cfg.max_kelly_size_pct,
        tp_cents: cfg.take_profit_cents, sl_cents: cfg.stop_loss_cents,
        max_hold_secs: cfg.max_hold_secs, kelly_price_max: cfg.kelly_price_max,
    };
    let mut paper = PaperEngine::load_or_init(
        cfg.start_cash, kelly,
        std::env::var("STATE_PATH").unwrap_or_else(|_| "data/sniper_state.json".into()),
        std::env::var("TRADES_PATH").unwrap_or_else(|_| "data/sniper_trades.jsonl".into()),
    );
    paper.fixed_order_usd = cfg.fixed_order_usd;
    // Manager LIVE — symétrique au PaperEngine, persistance séparée.
    let mut live_mgr = LivePositionManager::load_or_init(
        kelly,
        std::env::var("LIVE_STATE_PATH").unwrap_or_else(|_| "data/live_state.json".into()),
        std::env::var("LIVE_TRADES_PATH").unwrap_or_else(|_| "data/live_trades.jsonl".into()),
    );

    // ── EVENT LOOP (Bloc L) ──
    // - Bras OBI (bin_rx / okx_rx) : signal immédiat à chaque update WS → FSM sniper + fire
    // - Bras tick 50ms             : gestion position, dashboard, circuit breaker
    enum LoopEvent { Obi, Tick }
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut log_throttle: u32 = 0;
    let mut live_dd = bankroll::LiveDrawdown::default();
    let mut live_pnl = bankroll::LivePnl::default();
    let mut was_live = false;
    let mut live_shots: u64 = 0;
    // État de gestion gardé entre les OBI events pour que le bras Tick y accède.
    let mut last_mark_bid: Option<f64> = None;
    let mut last_live_pnl_val: Option<f64> = None;
    let mut last_series_ms: u64 = 0; // échantillonnage série graphe (1/s)

    loop {
        let event = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT reçu — arrêt propre (mono)");
                break Ok(());
            }
            Ok(()) = bin_rx.changed() => LoopEvent::Obi,
            Ok(()) = okx_rx.changed() => LoopEvent::Obi,
            _ = tick.tick() => LoopEvent::Tick,
        };

        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        // ── Snapshot des réglages à chaud (lock-free) — un Arc cohérent par tick.
        //    snapshot() = load_full() : Send-safe pour traverser les .await (try_open/manage). ──
        let tp = tuning.snapshot();
        sniper.apply_tunables(tp.gap_min, tp.cooldown_ms as u64, tp.score_fire_threshold);
        basis.set_threshold(tp.basis_threshold_usd);
        paper.update_sizing(tp.kelly_fraction, tp.max_kelly_size_pct, tp.kelly_price_max);
        let weights = CompositeWeights {
            w_obi: tp.w_obi, w_tfi: tp.w_tfi, w_kalman: tp.w_kalman, w_basis: tp.w_basis,
        };
        let kelly = KellyParams {
            kelly_fraction: tp.kelly_fraction, max_size_pct: tp.max_kelly_size_pct,
            tp_cents: cfg.take_profit_cents, sl_cents: cfg.stop_loss_cents,
            max_hold_secs: cfg.max_hold_secs, kelly_price_max: tp.kelly_price_max,
        };

        // ── Lectures lock-free ──
        let (obi_b, spot_opt, microprice_opt) = *bin_rx.borrow();
        let (obi_o, mid_okx) = *okx_rx.borrow();
        let Some(spot) = spot_opt else { continue };

        // Vélocité relative (pour liquidity_vacuum — unités relatives, seuil inchangé).
        vel.update(now_ms, spot);
        vol.update(now_ms, spot);
        let velocity = vel.velocity();

        // Vol EWMA sur log-return (variance par seconde — dt géré en interne).
        if let Some(prev) = prev_spot {
            if prev > 0.0 { ewma.update(now_ms, (spot / prev).ln()); }
        }
        prev_spot = Some(spot);

        // Kalman sur microprice (USD/s).
        let micro = microprice_opt.unwrap_or(spot);
        kalman.update(now_ms, micro);

        // TFI et timestamp OKX (lock-free).
        let tfi    = f64::from_bits(tfi_atomic.load(Ordering::Relaxed));
        let okx_ts = okx_ts_atomic.load(Ordering::Relaxed);

        // Basis + score composite.
        let (basis_norm, basis_unc) = basis.evaluate(micro, mid_okx, okx_ts, now_ms);
        let vel_norm = (kalman.velocity() / tp.vel_norm_factor).clamp(-1.0, 1.0);
        let score = composite::score(obi_b, tfi, vel_norm, basis_norm, basis_unc, &weights);
        ema_stat.update(score);
        let score_sigma = ema_stat.std_dev();

        // Snapshot Polymarket.
        let (market, strike, real_up, up_book, down_book, remaining_s) = {
            let g = pm.lock().unwrap();
            (g.market.clone(), g.strike, g.real_up, g.up_book.clone(), g.down_book.clone(), g.remaining_s)
        };

        // fair_up B&S avec décalage d2 par le score composite.
        let mut fair_up = 0.5;
        let mut gap = 0.0;
        let sigma_realized = vol.annualized_sigma();
        let sigma_ewma = ewma.annualized_sigma();
        let sigma_blended = 0.5 * sigma_realized + 0.5 * sigma_ewma;
        let mut d2_base = 0.0;
        let mut d2_adj = 0.0;
        let mut strike_val = 0.0;
        if let (Some(_m), Some(strk)) = (&market, strike) {
            let t_years = years_from_secs(remaining_s.max(0) as f64);
            strike_val = strk;
            d2_base = bs_d2(spot, strk, sigma_blended, t_years);
            d2_adj = d2_base + tp.d2_gamma * score;
            fair_up = fair_up_with_d2_shift(spot, strk, sigma_blended, t_years, score, tp.d2_gamma);
            gap = fair_up - real_up;
        }

        let liquidity_vacuum = velocity <= tp.vacuum_velocity && obi_b <= tp.vacuum_obi;
        let blocked = market.is_none() || strike.is_none() || remaining_s <= cfg.end_window_block_secs;

        // ── FSM sniper : se déclenche sur chaque event OBI et Tick ──
        let is_live = controls.live_active();
        let input = TickInput { now_ms, score, score_sigma, basis_unc, fair_up, real_up,
            kalman_velocity: kalman.velocity(), liquidity_vacuum, blocked };
        match sniper.step(&input) {
            Action::Fire { side, .. } => {
                last_fire_score = score.abs();
                if let Some(m) = &market {
                    let (book, token) = if side == concurrency::bus::Side::Up {
                        (&up_book, &m.up_token_id)
                    } else { (&down_book, &m.down_token_id) };
                    if controls.is_breaker_tripped() {
                        // exécution coupée
                    } else if is_live {
                        if live_mgr.position().is_some() {
                            tracing::info!(reason = "position live déjà ouverte", "✗ ordre live ignoré");
                        } else {
                            match (*live_bankroll.lock().unwrap(), live_creds.as_ref()) {
                                (None, _) => tracing::warn!("LIVE actif mais bankroll réelle pas encore lue — tir ignoré"),
                                (_, None) => tracing::warn!("LIVE actif mais POLY_* credentials absents — tir ignoré"),
                                (Some(bk), Some(creds)) => {
                                    let price = book.best_ask().unwrap_or(real_up);
                                    let sized = if cfg.live_force_min_size {
                                        Some(m.min_order_size)
                                    } else {
                                        bankroll::adjust_size_to_min(
                                            kelly.robust_kelly_size_for(gap.abs(), price, bk, score.abs(), score_sigma, basis_unc),
                                            m.min_order_size,
                                        )
                                    };
                                    match sized {
                                        None => tracing::info!(reason = "taille sous le minimum",
                                            min = m.min_order_size, "✗ ordre live ignoré"),
                                        Some(size) if size * price > bk => tracing::warn!(
                                            cost = format!("{:.2}", size * price), bankroll = format!("{bk:.2}"),
                                            "✗ ordre live ignoré — bankroll insuffisante"),
                                        Some(size) => {
                                            if cfg.live_force_min_size {
                                                tracing::warn!(size, "⚠️ taille FORCÉE au minimum (LIVE_FORCE_MIN_SIZE)");
                                            }
                                            live_mgr.try_open(
                                                creds, cfg.live_armed, side, token, m.neg_risk,
                                                price, size, m.tick_size, m.min_order_size, now_ms,
                                            ).await;
                                        }
                                    }
                                }
                            }
                        }
                    } else if !controls.is_paper_paused() {
                        paper.fire(side, token, gap.abs(), book, m.tick_size, m.min_order_size, now_ms);
                    }
                }
            }
            Action::Kill => tracing::warn!("⚡ KILL (liquidity vacuum) — abstention"),
            Action::None => {}
        }

        // ── Gestion position + dashboard : seulement sur tick 50ms ──
        if matches!(event, LoopEvent::Tick) {
            // Échantillon série graphe (1/s, borné) — pour le chart entrées/sorties.
            if now_ms.saturating_sub(last_series_ms) >= 1000 {
                series::push(now_ms, fair_up, real_up, spot);
                last_series_ms = now_ms;
            }

            // Gestion paper + IC Tracker.
            let mark_bid = if let Some(p) = &paper.position {
                let bk = if p.side == concurrency::bus::Side::Up { &up_book } else { &down_book };
                bk.best_bid()
            } else { None };
            let wins_before = paper.state.wins;
            let had_position = paper.position.is_some();
            paper.manage(mark_bid, now_ms, remaining_s);
            if had_position && paper.position.is_none() {
                ic_tracker.record(last_fire_score, paper.state.wins > wins_before);
            }
            last_mark_bid = mark_bid;

            // Gestion position LIVE (TP/SL/max-hold).
            if let (Some(p), Some(creds), Some(m)) =
                (live_mgr.position(), live_creds.as_ref(), market.as_ref())
            {
                let live_book = if p.side == concurrency::bus::Side::Up { &up_book } else { &down_book };
                let live_mark = live_book.best_bid();
                live_mgr.manage(
                    creds, cfg.live_armed, live_mark, live_book,
                    m.min_order_size, m.tick_size, now_ms, remaining_s,
                ).await;
            }

            // Circuit breaker (drawdown).
            let breaker_hit = if is_live {
                match *live_bankroll.lock().unwrap() {
                    Some(real) => live_dd.breached(real, cfg.max_drawdown),
                    None => false,
                }
            } else {
                bankroll::check_drawdown_breaker(paper.equity(last_mark_bid), cfg.start_cash, cfg.max_drawdown)
            };
            if !controls.is_breaker_tripped() && breaker_hit && controls.trip_breaker() {
                tracing::error!(mode = controls.mode_label(), max_dd = cfg.max_drawdown,
                    "🛑 CIRCUIT BREAKER — drawdown atteint, exécution coupée");
            }

            // PnL live.
            if is_live && !was_live { live_pnl.reset(); live_shots = 0; }
            was_live = is_live;
            last_live_pnl_val = if is_live {
                live_bankroll.lock().unwrap().map(|bk| live_pnl.update(bk))
            } else { None };

            // Conditions de tir (dashboard).
            let score_ok = score.abs() >= tp.score_fire_threshold;
            let cond_gap = (fair_up - real_up).abs() >= tp.gap_min;
            let cond_velocity = vel_norm.abs() > 0.1;
            let cond_ready = !blocked && !liquidity_vacuum && !sniper.in_cooldown();
            let cond_persist = sniper.is_armed();
            let all_conditions = score_ok && cond_gap && cond_ready;

            let kelly_size = market.as_ref().map(|_|
                kelly.robust_kelly_size_for(gap.abs(), real_up.max(0.01), paper.state.cash, score.abs(), score_sigma, basis_unc)
            ).unwrap_or(0.0);
            let fsm = if sniper.in_cooldown() { "COOLDOWN" } else if sniper.is_armed() { "ARMING" } else { "IDLE" };
            let lat_snap = lat.lock().unwrap().clone();
            {
                let mut d = dash.write().await;
                d.binance_connected = spot > 0.0;
                d.okx_connected = obi_o != 0.0;
                d.btc_spot = spot;
                d.obi_binance = obi_b; d.obi_okx = obi_o; d.obi_consolidated = score;
                d.agreement = score_ok; d.velocity = velocity;
                // Compartiment Maths — valeurs vivantes du signal stack.
                d.microprice = micro; d.tfi = tfi;
                d.kalman_velocity = kalman.velocity(); d.vel_norm = vel_norm;
                d.basis_norm = basis_norm; d.basis_unc = basis_unc;
                d.score = score; d.score_sigma = score_sigma;
                d.sigma_realized = sigma_realized; d.sigma_ewma = sigma_ewma; d.sigma_blended = sigma_blended;
                d.d2_base = d2_base; d.d2_adj = d2_adj; d.strike = strike_val;
                d.ic = ic_tracker.ic();
                d.fsm_state = fsm.into();
                d.market_slug = market.as_ref().map(|m| m.slug.clone()).unwrap_or_default();
                d.remaining_s = remaining_s;
                d.fair_up = fair_up; d.real_up = real_up; d.gap = gap;
                d.liquidity_vacuum = liquidity_vacuum; d.kelly_size = kelly_size;
                d.cond_agreement = score_ok; d.cond_gap = cond_gap; d.cond_velocity = cond_velocity;
                d.cond_ready = cond_ready; d.cond_persist = cond_persist; d.all_conditions = all_conditions;
                if let Some(p) = live_mgr.position() {
                    d.in_position = true;
                    d.pos_side = p.side.as_str().into(); d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
                } else if let Some(p) = &paper.position {
                    d.in_position = true;
                    d.pos_side = p.side.as_str().into(); d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
                } else {
                    d.in_position = false;
                }
                d.cash = paper.state.cash; d.equity = paper.equity(last_mark_bid);
                d.realized_pnl = paper.state.realized_pnl; d.drawdown = paper.drawdown();
                d.shots = paper.state.shots; d.wins = paper.state.wins; d.losses = paper.state.losses;
                d.hit_rate = paper.hit_rate();
                d.lat_binance_ms    = lat_snap.binance_ms;
                d.lat_okx_ms        = lat_snap.okx_ms;
                d.lat_polymarket_ms = lat_snap.polymarket_ms;
                d.mode = controls.mode_label().into();
                d.paper_paused = controls.is_paper_paused();
                d.live_enabled = controls.is_live_enabled();
                d.live_paused = controls.is_live_paused();
                d.live_armed = cfg.live_armed;
                d.breaker_tripped = controls.is_breaker_tripped();
                d.initial_capital = cfg.start_cash;
                d.max_drawdown = cfg.max_drawdown;
                d.live_bankroll = *live_bankroll.lock().unwrap();
                d.live_pnl = if is_live {
                    if live_mgr.state.shots > 0 { Some(live_mgr.state.realized_pnl) } else { last_live_pnl_val }
                } else { None };
                d.live_shots = live_mgr.state.shots.max(live_shots);
                d.live_force_min = cfg.live_force_min_size;
                d.lat_last_buy_ms = live_mgr.last_buy_ms;
                d.lat_last_sell_ms = live_mgr.last_sell_ms;
            }

            log_throttle += 1;
            if log_throttle % 100 == 0 { // ~5 s
                let fsm_str = if sniper.in_cooldown() { "COOLDOWN" } else if sniper.is_armed() { "ARMING" } else { "IDLE" };
                tracing::info!(
                    score = format!("{:+.3}", score), obi_b = format!("{:+.2}", obi_b),
                    obi_o = format!("{:+.2}", obi_o), tfi = format!("{:+.2}", tfi),
                    fair = format!("{:.3}", fair_up), real = format!("{:.3}", real_up),
                    gap = format!("{:+.3}", gap), fsm = fsm_str, shots = paper.state.shots,
                    ic = format!("{:.3}", ic_tracker.ic()), "mono"
                );
            }
        }
    }
}
