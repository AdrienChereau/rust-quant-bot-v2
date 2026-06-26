//! Nœud **Exécuteur (Dublin)** — récepteur.
//!
//! Possède le carnet Polymarket, la bankroll et le moteur paper. Une tâche UDP dédiée décode les
//! paquets 6 octets du radar et les pousse dans un `mpsc` ; la boucle timer (50 ms) :
//!   1. gère la position ouverte (TP/SL/max-hold) à chaque tick ;
//!   2. draine les signaux reçus → calcule le **gap = fair(paquet) − real(local)**, applique le
//!      filtre `gap_min` + fin-de-fenêtre + cooldown, **dimensionne via Kelly** (autoritaire),
//!      puis `paper.fire` (DRY_RUN : fill simulé, aucun ordre réel).
//!
//! Sonde de latence côté Dublin : Polymarket uniquement (`Probes::PmOnly`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::concurrency::bus::Side;
use crate::config::Config;
use crate::dashboard;
use crate::net::udp;
use crate::net::wire::WireSignal;
use crate::polymarket::live_executor::{self, LiveCredentials, OrderArgs};
use crate::polymarket::relayer::{Market, PolyBook, PolymarketClient};
use crate::state::RuntimeControls;
use crate::strategy::bankroll::{self, KellyParams, PaperEngine};

#[derive(Default)]
struct PmShared {
    market: Option<Market>,
    real_up: f64,
    up_book: PolyBook,
    down_book: PolyBook,
    remaining_s: i64,
}

pub async fn run(cfg: Config, listen_port: u16) -> anyhow::Result<()> {
    tracing::info!(listen_port, dry_run = cfg.dry_run, "🎯 EXÉCUTEUR (Dublin) démarré");

    let controls = Arc::new(RuntimeControls::new());
    let live_creds = LiveCredentials::from_env();
    if cfg.live_armed {
        tracing::warn!(creds = live_creds.is_some(), "⚠️  LIVE_ARMED=true — envoi réel possible (si signature vérifiée)");
    }

    // Vraie collatéral USDC (CLOB) — lue toutes les 30 s ; sert de bankroll pour le sizing LIVE.
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

    let pm = Arc::new(Mutex::new(PmShared::default()));
    spawn_pm_task(pm.clone());

    let lat = crate::latency::shared();
    {
        let l = lat.clone();
        tokio::spawn(async move { crate::latency::run(l, crate::latency::Probes::PmOnly).await; });
    }

    let mut paper = PaperEngine::load_or_init(
        cfg.start_cash,
        KellyParams {
            kelly_fraction: cfg.kelly_fraction, max_size_pct: cfg.max_kelly_size_pct,
            tp_cents: cfg.take_profit_cents, sl_cents: cfg.stop_loss_cents, max_hold_secs: cfg.max_hold_secs,
        },
        std::env::var("STATE_PATH").unwrap_or_else(|_| "data/sniper_state.json".into()),
        std::env::var("TRADES_PATH").unwrap_or_else(|_| "data/sniper_trades.jsonl".into()),
    );

    let mut rx = udp::listen(listen_port).await?;

    let mut last_fire_ms: u64 = 0;
    let mut last_fair: f64 = 0.5; // dernier fair reçu (affichage gap)
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut log_throttle: u32 = 0;
    loop {
        tick.tick().await;
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        let (market, real_up, up_book, down_book, remaining_s) = {
            let g = pm.lock().unwrap();
            (g.market.clone(), g.real_up, g.up_book.clone(), g.down_book.clone(), g.remaining_s)
        };

        // 1. Gestion de la position ouverte (TP/SL/max-hold).
        let mark_bid = if let Some(p) = &paper.position {
            let bk = if p.side == Side::Up { &up_book } else { &down_book };
            bk.best_bid()
        } else { None };
        paper.manage(mark_bid, now_ms, remaining_s);

        // Circuit breaker (drawdown equity).
        let equity_now = paper.equity(mark_bid);
        if !controls.is_breaker_tripped()
            && bankroll::check_drawdown_breaker(equity_now, cfg.start_cash, cfg.max_drawdown)
            && controls.trip_breaker()
        {
            tracing::error!(equity = format!("{:.2}", equity_now), capital = cfg.start_cash,
                max_dd = cfg.max_drawdown, "🛑 CIRCUIT BREAKER — drawdown atteint, exécution coupée");
        }

        // 2. Drain des signaux UDP reçus du radar.
        while let Ok(sig) = rx.try_recv() {
            match sig {
                WireSignal::Kill => tracing::warn!("⚡ KILL reçu — abstention"),
                WireSignal::Attack { side, price, .. } => {
                    let fair = price as f64;
                    last_fair = fair;
                    let blocked = market.is_none() || remaining_s <= cfg.end_window_block_secs;
                    let cooling = now_ms.saturating_sub(last_fire_ms) < cfg.cooldown_ms;
                    let gap_ok = match side {
                        Side::Up => fair - real_up >= cfg.gap_min,
                        Side::Down => real_up - fair >= cfg.gap_min,
                    };
                    if controls.is_breaker_tripped() || blocked || cooling || !gap_ok {
                        continue;
                    }
                    if let Some(m) = &market {
                        let (book, token) = if side == Side::Up {
                            (&up_book, &m.up_token_id)
                        } else {
                            (&down_book, &m.down_token_id)
                        };
                        let edge = (fair - real_up).abs();
                        // Aiguillage live → paper.
                        if controls.live_active() {
                            // Sizing sur la VRAIE collatéral CLOB. Tant qu'elle n'est pas lue, on
                            // s'abstient (jamais sizer un ordre réel sur le cash paper fictif).
                            let real_bk = *live_bankroll.lock().unwrap();
                            match real_bk {
                                None => tracing::warn!("LIVE actif mais bankroll réelle pas encore lue — tir ignoré"),
                                Some(bk) => {
                                    let order_price = book.best_ask().unwrap_or(real_up);
                                    let size_k = paper.kelly_size_for(edge, order_price, bk);
                                    if let Some(size) = bankroll::adjust_size_to_min(size_k, m.min_order_size) {
                                        let args = OrderArgs { side, price: order_price, size };
                                        let _ = live_executor::place_order(cfg.live_armed, live_creds.as_ref(), token, args).await;
                                        last_fire_ms = now_ms;
                                    }
                                }
                            }
                        } else if !controls.is_paper_paused()
                            && paper.fire(side, token, edge, book, m.tick_size, m.min_order_size, now_ms)
                        {
                            last_fire_ms = now_ms;
                        }
                    }
                }
            }
        }

        // Dashboard exécuteur (PM/position/PnL ; OBI laissé à 0).
        let lat_snap = lat.lock().unwrap().clone();
        {
            let mut d = dash.write().await;
            d.market_slug = market.as_ref().map(|m| m.slug.clone()).unwrap_or_default();
            d.remaining_s = remaining_s;
            d.fair_up = last_fair; d.real_up = real_up; d.gap = last_fair - real_up;
            d.in_position = paper.position.is_some();
            if let Some(p) = &paper.position {
                d.pos_side = p.side.as_str().into();
                d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
            }
            d.cash = paper.state.cash; d.equity = paper.equity(mark_bid);
            d.realized_pnl = paper.state.realized_pnl; d.drawdown = paper.drawdown();
            d.shots = paper.state.shots; d.wins = paper.state.wins; d.losses = paper.state.losses;
            d.hit_rate = paper.hit_rate();
            d.mode = controls.mode_label().into();
            d.paper_paused = controls.is_paper_paused();
            d.live_enabled = controls.is_live_enabled();
            d.live_paused = controls.is_live_paused();
            d.live_armed = cfg.live_armed;
            d.breaker_tripped = controls.is_breaker_tripped();
            d.initial_capital = cfg.start_cash;
            d.max_drawdown = cfg.max_drawdown;
            d.lat_polymarket_ms = lat_snap.polymarket_ms;
            d.live_bankroll = *live_bankroll.lock().unwrap();
        }

        log_throttle += 1;
        if log_throttle % 100 == 0 {
            tracing::info!(real = format!("{:.3}", real_up), shots = paper.state.shots,
                cash = format!("{:.2}", paper.state.cash), "executor");
        }
    }
}

/// Tâche Polymarket : (re)résout le marché 5 min et polle les carnets Up/Down (1 s).
/// L'exécuteur n'a pas besoin du strike (le `fair_up` arrive dans le paquet radar).
fn spawn_pm_task(pm: Arc<Mutex<PmShared>>) {
    tokio::spawn(async move {
        let client = PolymarketClient::new();
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        loop {
            poll.tick().await;
            let need = { let g = pm.lock().unwrap(); g.market.as_ref().map_or(true, |m| m.time_remaining_sec() <= 0) };
            if need {
                if let Ok(Some(m)) = client.get_current_btc_5m_market().await {
                    tracing::info!(slug = %m.slug, "=== nouveau marché (exécuteur) ===");
                    pm.lock().unwrap().market = Some(m);
                }
            }
            let (up_tok, dn_tok) = { let g = pm.lock().unwrap();
                match &g.market { Some(m) => (m.up_token_id.clone(), m.down_token_id.clone()), None => continue } };
            let up = client.get_book(&up_tok).await.ok();
            let dn = client.get_book(&dn_tok).await.ok();
            let mut g = pm.lock().unwrap();
            if let Some(up) = up { g.real_up = up.mid().unwrap_or(g.real_up); g.up_book = up; }
            if let Some(dn) = dn { g.down_book = dn; }
            g.remaining_s = g.market.as_ref().map(|m| m.time_remaining_sec()).unwrap_or(0);
        }
    });
}
