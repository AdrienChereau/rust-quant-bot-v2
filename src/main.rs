//! `rust-quant-bot` — sniper front-running Binance+OKX → Polymarket. Paper-first.
//!
//! Trois modes (CLI `clap`) :
//!   - `radar`    : nœud Tokyo, émet les signaux OBI en UDP (`roles::radar`).
//!   - `executor` : nœud Dublin, écoute l'UDP + exécute en paper (`roles::executor`).
//!   - `mono`     : radar+exécuteur dans le même processus (défaut, run local/cloudy) — ci-dessous.

mod binance;
mod concurrency;
mod config;
mod dashboard;
mod error;
mod latency;
mod net;
mod okx;
mod polymarket;
mod pricing;
mod roles;
mod signal;
mod state;
mod strategy;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{Parser, Subcommand};

use binance::local_book::OrderBookL2;
use binance::math_engine::VelocityTracker;
use config::Config;
use polymarket::relayer::{btc_price_at_window_open, Market, PolyBook, PolymarketClient};
use pricing::black_scholes::{fair_up_probability, years_from_secs};
use pricing::volatility::VolatilityTracker;
use polymarket::live_executor::{self, LiveCredentials, OrderArgs};
use signal::consolidated_obi::ConsolidatedObi;
use state::RuntimeControls;
use strategy::bankroll::{self, KellyParams, PaperEngine};
use strategy::sniper::{Action, Sniper, TickInput};

#[derive(Parser)]
#[command(author, version, about = "HFT Polymarket Bot — Radar (Tokyo) / Executor (Dublin)")]
struct Cli {
    #[command(subcommand)]
    mode: Option<Mode>,
}

#[derive(Subcommand)]
enum Mode {
    /// Nœud Radar (Tokyo) : écoute Binance/OKX, calcule l'OBI, tire en UDP vers l'exécuteur.
    Radar {
        /// IP de l'exécuteur (Dublin). Fallback env `TARGET_EXECUTOR_IP`.
        #[arg(short, long, env = "TARGET_EXECUTOR_IP")]
        target_ip: String,
        /// Port UDP de l'exécuteur. Fallback env `TARGET_PORT`.
        #[arg(long, env = "TARGET_PORT", default_value = "8080")]
        target_port: u16,
    },
    /// Nœud Exécuteur (Dublin) : écoute l'UDP et exécute (paper) sur Polymarket.
    Executor {
        /// Port UDP d'écoute. Fallback env `LISTEN_PORT`.
        #[arg(long, env = "LISTEN_PORT", default_value = "8080")]
        listen_port: u16,
    },
    /// Mono : radar + exécuteur in-process (loopback). Mode par défaut (run local / cloudy).
    Mono,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cfg = Config::from_env();

    match Cli::parse().mode.unwrap_or(Mode::Mono) {
        Mode::Radar { target_ip, target_port } => roles::radar::run(cfg, target_ip, target_port).await,
        Mode::Executor { listen_port } => roles::executor::run(cfg, listen_port).await,
        Mode::Mono => run_mono(cfg).await,
    }
}

#[derive(Default)]
struct PmShared {
    market: Option<Market>,
    strike: Option<f64>,
    real_up: f64,
    up_book: PolyBook,
    down_book: PolyBook,
    remaining_s: i64,
}

/// Mode mono-processus historique : radar + exécuteur dans la même boucle 50 ms.
async fn run_mono(cfg: Config) -> anyhow::Result<()> {
    tracing::info!(dry_run = cfg.dry_run, "🎯 rust-quant-bot (sniper, mono) démarré");

    // Contrôle d'exécution lock-free (pause/live/breaker), partagé avec le dashboard.
    let controls = Arc::new(RuntimeControls::new());
    let live_creds = LiveCredentials::from_env();
    if cfg.live_armed {
        tracing::warn!(creds = live_creds.is_some(), "⚠️  LIVE_ARMED=true — envoi réel possible (si signature vérifiée)");
    }

    // Vraie collatéral USDC (CLOB) — bankroll pour le sizing LIVE (jamais le cash paper).
    let live_bankroll = Arc::new(Mutex::new(None::<f64>));
    if let Some(creds) = live_creds.clone() {
        let bk = live_bankroll.clone();
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(Duration::from_secs(30));
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

    let dash = dashboard::shared(cfg.dry_run);
    {
        let (port, st, ct) = (cfg.dashboard_port, dash.clone(), controls.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct).await; });
    }

    // Sonde de latence TCP (toutes les 5 s, hors hot-loop) — mono = les trois cibles.
    let lat = latency::shared();
    { let l = lat.clone(); tokio::spawn(async move { latency::run(l, latency::Probes::All).await; }); }

    // Carnets CEX partagés, alimentés par les tâches WS.
    let binance_book = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));
    let okx_book = Arc::new(Mutex::new(OrderBookL2::new(cfg.obi_band_pct)));
    tokio::spawn(binance::websocket::run(cfg.binance_ws_url.clone(), binance_book.clone()));
    tokio::spawn(okx::websocket::run(cfg.okx_ws_url.clone(), okx_book.clone()));

    // État Polymarket, rafraîchi toutes les 1 s (hors hot-loop).
    let pm = Arc::new(Mutex::new(PmShared::default()));
    spawn_pm_task(pm.clone());

    // Moteurs.
    let consolidated = ConsolidatedObi::new(cfg.obi_floor_per_exchange, cfg.obi_fire_threshold, cfg.weight_binance, cfg.weight_okx);
    let mut sniper = Sniper::new(cfg.obi_dwell_ms, cfg.cooldown_ms, cfg.gap_min, cfg.velocity_confirm);
    let mut vel = VelocityTracker::new(1000);
    let mut vol = VolatilityTracker::new(2000, 0.80);
    let mut paper = PaperEngine::load_or_init(
        cfg.start_cash,
        KellyParams { kelly_fraction: cfg.kelly_fraction, max_size_pct: cfg.max_kelly_size_pct,
            tp_cents: cfg.take_profit_cents, sl_cents: cfg.stop_loss_cents, max_hold_secs: cfg.max_hold_secs },
        std::env::var("STATE_PATH").unwrap_or_else(|_| "data/sniper_state.json".into()),
        std::env::var("TRADES_PATH").unwrap_or_else(|_| "data/sniper_trades.jsonl".into()),
    );

    // ── HOT LOOP : 50 ms (20 Hz) ── (lectures de carnets = locks brefs, aucun await réseau)
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut log_throttle: u32 = 0;
    loop {
        tick.tick().await;
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        let n = cfg.obi_top_n;
        let (obi_b, spot) = { let b = binance_book.lock().unwrap(); (b.calculate_obi_topn(n), b.mid_price()) };
        let obi_o = { okx_book.lock().unwrap().calculate_obi_topn(n) };
        let Some(spot) = spot else { continue };
        vel.update(now_ms, spot);
        vol.update(now_ms, spot);
        let velocity = vel.velocity();
        let decision = consolidated.evaluate(obi_b, obi_o);

        // Snapshot Polymarket.
        let (market, strike, real_up, up_book, down_book, remaining_s) = {
            let g = pm.lock().unwrap();
            (g.market.clone(), g.strike, g.real_up, g.up_book.clone(), g.down_book.clone(), g.remaining_s)
        };

        let mut fair_up = 0.5;
        let mut gap = 0.0;
        if let (Some(m), Some(strike)) = (&market, strike) {
            let _ = m;
            let t_years = years_from_secs(remaining_s.max(0) as f64);
            fair_up = fair_up_probability(spot, strike, vol.annualized_sigma(), t_years);
            gap = fair_up - real_up;
        }

        // Vide de liquidité (règle PDF) + blocage fin de fenêtre.
        let liquidity_vacuum = velocity <= cfg.vacuum_velocity && obi_b <= cfg.vacuum_obi;
        let blocked = market.is_none() || strike.is_none() || remaining_s <= cfg.end_window_block_secs;

        // Gestion de la position ouverte (TP/SL/max-hold) à chaque tick.
        let mark_bid = if let Some(p) = &paper.position {
            let bk = if p.side == concurrency::bus::Side::Up { &up_book } else { &down_book };
            bk.best_bid()
        } else { None };
        paper.manage(mark_bid, now_ms, remaining_s);

        // Circuit breaker (drawdown sur l'equity) — déclenché une fois, coupe toute exécution.
        let equity_now = paper.equity(mark_bid);
        if !controls.is_breaker_tripped()
            && bankroll::check_drawdown_breaker(equity_now, cfg.start_cash, cfg.max_drawdown)
            && controls.trip_breaker()
        {
            tracing::error!(equity = format!("{:.2}", equity_now), capital = cfg.start_cash,
                max_dd = cfg.max_drawdown, "🛑 CIRCUIT BREAKER — drawdown atteint, exécution coupée");
        }

        // FSM sniper.
        let input = TickInput { now_ms, decision, fair_up, real_up, velocity, liquidity_vacuum, blocked };
        match sniper.step(&input) {
            Action::Fire { side, .. } => {
                if let Some(m) = &market {
                    let (book, token) = if side == concurrency::bus::Side::Up {
                        (&up_book, &m.up_token_id)
                    } else { (&down_book, &m.down_token_id) };
                    // Aiguillage breaker → live → paper.
                    if controls.is_breaker_tripped() {
                        // exécution coupée
                    } else if controls.live_active() {
                        // Sizing sur la VRAIE collatéral CLOB (jamais le cash paper). Pas encore lue → abstention.
                        match *live_bankroll.lock().unwrap() {
                            None => tracing::warn!("LIVE actif mais bankroll réelle pas encore lue — tir ignoré"),
                            Some(bk) => {
                                let price = book.best_ask().unwrap_or(real_up);
                                let size_k = paper.kelly_size_for(gap.abs(), price, bk);
                                if let Some(size) = bankroll::adjust_size_to_min(size_k, m.min_order_size) {
                                    let args = OrderArgs { side, price, size };
                                    let _ = live_executor::place_order(cfg.live_armed, live_creds.as_ref(), token, args).await;
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

        // Conditions de tir (checklist AND) — pour le dashboard.
        use concurrency::bus::Side;
        let cand_side = decision.side.or(if gap > 0.0 { Some(Side::Up) } else if gap < 0.0 { Some(Side::Down) } else { None });
        let cond_agreement = decision.fire;
        let cond_gap = match cand_side {
            Some(Side::Up) => fair_up - real_up >= cfg.gap_min,
            Some(Side::Down) => real_up - fair_up >= cfg.gap_min,
            None => false,
        };
        let cond_velocity = match cand_side {
            Some(Side::Up) => velocity >= cfg.velocity_confirm,
            Some(Side::Down) => velocity <= -cfg.velocity_confirm,
            None => false,
        };
        let cond_ready = !blocked && !liquidity_vacuum && !sniper.in_cooldown();
        let cond_persist = sniper.is_armed();
        let all_conditions = cond_agreement && cond_gap && cond_velocity && cond_ready;

        // Dashboard (écriture brève hors chemin critique).
        let kelly = market.as_ref().map(|_| paper.kelly_size(gap.abs(), real_up.max(0.01))).unwrap_or(0.0);
        let fsm = if sniper.in_cooldown() { "COOLDOWN" } else if sniper.is_armed() { "ARMING" } else { "IDLE" };
        let lat_snap = lat.lock().unwrap().clone();
        {
            let mut d = dash.write().await;
            d.binance_connected = spot > 0.0;
            d.okx_connected = obi_o != 0.0 || { okx_book.lock().unwrap().mid_price().is_some() };
            d.btc_spot = spot;
            d.obi_binance = obi_b; d.obi_okx = obi_o; d.obi_consolidated = decision.strength;
            d.agreement = decision.fire; d.velocity = velocity;
            d.fsm_state = fsm.into();
            d.market_slug = market.as_ref().map(|m| m.slug.clone()).unwrap_or_default();
            d.remaining_s = remaining_s;
            d.fair_up = fair_up; d.real_up = real_up; d.gap = gap;
            d.liquidity_vacuum = liquidity_vacuum; d.kelly_size = kelly;
            d.cond_agreement = cond_agreement; d.cond_gap = cond_gap; d.cond_velocity = cond_velocity;
            d.cond_ready = cond_ready; d.cond_persist = cond_persist; d.all_conditions = all_conditions;
            d.in_position = paper.position.is_some();
            if let Some(p) = &paper.position {
                d.pos_side = p.side.as_str().into(); d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
            }
            d.cash = paper.state.cash; d.equity = paper.equity(mark_bid);
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
        }

        log_throttle += 1;
        if log_throttle % 100 == 0 { // ~5 s
            tracing::info!(obi_b = format!("{:+.2}", obi_b), obi_o = format!("{:+.2}", obi_o),
                fair = format!("{:.3}", fair_up), real = format!("{:.3}", real_up),
                gap = format!("{:+.3}", gap), fsm, shots = paper.state.shots, "radar");
        }
    }
}

/// Tâche Polymarket : (re)résout le marché 5 min, capture le strike, polle les carnets Up/Down.
fn spawn_pm_task(pm: Arc<Mutex<PmShared>>) {
    tokio::spawn(async move {
        let client = PolymarketClient::new();
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        loop {
            poll.tick().await;
            let need = { let g = pm.lock().unwrap(); g.market.as_ref().map_or(true, |m| m.time_remaining_sec() <= 0) };
            if need {
                if let Ok(Some(m)) = client.get_current_btc_5m_market().await {
                    let strike = btc_price_at_window_open(m.window_ts).await.ok();
                    tracing::info!(slug = %m.slug, strike = ?strike, "=== nouveau marché ===");
                    let mut g = pm.lock().unwrap();
                    g.market = Some(m); g.strike = strike;
                }
            }
            let (up_tok, dn_tok, win) = { let g = pm.lock().unwrap();
                match &g.market { Some(m) => (m.up_token_id.clone(), m.down_token_id.clone(), m.window_ts), None => continue } };
            // Retry strike si manquant.
            if pm.lock().unwrap().strike.is_none() {
                if let Ok(s) = btc_price_at_window_open(win).await { pm.lock().unwrap().strike = Some(s); }
            }
            let up = client.get_book(&up_tok).await.ok();
            let dn = client.get_book(&dn_tok).await.ok();
            let mut g = pm.lock().unwrap();
            if let Some(up) = up { g.real_up = up.mid().unwrap_or(g.real_up); g.up_book = up; }
            if let Some(dn) = dn { g.down_book = dn; }
            g.remaining_s = g.market.as_ref().map(|m| m.time_remaining_sec()).unwrap_or(0);
        }
    });
}
