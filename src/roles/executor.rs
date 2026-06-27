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
use crate::polymarket::pm_poller::{spawn_pm_poller, PmShared};
use crate::state::RuntimeControls;
use crate::strategy::bankroll::{self, KellyParams, PaperEngine};

pub async fn run(cfg: Config, listen_port: u16) -> anyhow::Result<()> {
    tracing::info!(listen_port, dry_run = cfg.dry_run, "🎯 EXÉCUTEUR (Dublin) démarré");

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

    // `false` = l'exécuteur n'a pas besoin du strike (le fair arrive dans le paquet radar).
    let pm = Arc::new(Mutex::new(PmShared::default()));
    spawn_pm_poller(pm.clone(), false);

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
    let mut live_dd = bankroll::LiveDrawdown::default(); // drawdown sur la bankroll réelle (live)
    let mut live_pnl = bankroll::LivePnl::default();     // PnL réalisé live (Δ bankroll)
    let mut was_live = false;                            // détection de transition paper→live
    let mut live_shots: u64 = 0;                         // ordres live acceptés cette session
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                // Arrêt propre : l'état paper est déjà persisté à chaque clôture de position.
                tracing::info!("SIGINT reçu — arrêt propre (exécuteur)");
                break Ok(());
            }
            _ = tick.tick() => {}
        }
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

        // Circuit breaker (drawdown) — LIVE : vraie bankroll CLOB ; PAPER : equity fictive.
        let breaker_hit = if controls.live_active() {
            match *live_bankroll.lock().unwrap() {
                Some(real) => live_dd.breached(real, cfg.max_drawdown),
                None => false, // bankroll réelle pas encore lue
            }
        } else {
            bankroll::check_drawdown_breaker(paper.equity(mark_bid), cfg.start_cash, cfg.max_drawdown)
        };
        if !controls.is_breaker_tripped() && breaker_hit && controls.trip_breaker() {
            tracing::error!(mode = controls.mode_label(), max_dd = cfg.max_drawdown,
                "🛑 CIRCUIT BREAKER — drawdown atteint, exécution coupée");
        }

        // PnL live = Δ bankroll réelle depuis l'activation du live ; référence reposée à la bascule.
        let is_live = controls.live_active();
        if is_live && !was_live { live_pnl.reset(); live_shots = 0; }
        was_live = is_live;
        let live_pnl_val = if is_live {
            live_bankroll.lock().unwrap().map(|bk| live_pnl.update(bk))
        } else { None };

        // 2. Drain des signaux UDP reçus du radar.
        while let Ok(sig) = rx.try_recv() {
            match sig {
                WireSignal::Kill => tracing::warn!("⚡ KILL reçu — abstention"),
                WireSignal::Attack { side, price, .. } => {
                    let fair = price as f64;
                    last_fair = fair;
                    // gap = edge orienté selon le sens (toujours « fair en faveur du token visé »).
                    let gap = match side {
                        Side::Up => fair - real_up,
                        Side::Down => real_up - fair,
                    };
                    // Raison de rejet unique (loggée) — sinon `None` = on tente l'exécution.
                    let reject = if controls.is_breaker_tripped() {
                        Some("breaker déclenché")
                    } else if market.is_none() {
                        Some("pas de marché")
                    } else if remaining_s <= cfg.end_window_block_secs {
                        Some("fin de fenêtre")
                    } else if now_ms.saturating_sub(last_fire_ms) < cfg.cooldown_ms {
                        Some("cooldown")
                    } else if gap < cfg.gap_min {
                        Some("gap insuffisant")
                    } else {
                        None
                    };
                    if let Some(reason) = reject {
                        tracing::info!(reason, side = side.as_str(), fair = format!("{fair:.3}"),
                            real = format!("{real_up:.3}"), gap = format!("{gap:+.3}"),
                            gap_min = cfg.gap_min, "✗ signal rejeté");
                        continue;
                    }
                    if let Some(m) = &market {
                        let (book, token) = if side == Side::Up {
                            (&up_book, &m.up_token_id)
                        } else {
                            (&down_book, &m.down_token_id)
                        };
                        let edge = gap; // gap ≥ gap_min > 0 ici
                        // Aiguillage live → paper.
                        if is_live {
                            // Sizing sur la VRAIE collatéral CLOB. Tant qu'elle n'est pas lue, on
                            // s'abstient (jamais sizer un ordre réel sur le cash paper fictif).
                            match *live_bankroll.lock().unwrap() {
                                None => tracing::warn!("LIVE actif mais bankroll réelle pas encore lue — tir ignoré"),
                                Some(bk) => {
                                    let order_price = book.best_ask().unwrap_or(real_up);
                                    let size_k = paper.kelly_size_for(edge, order_price, bk);
                                    match bankroll::adjust_size_to_min(size_k, m.min_order_size) {
                                        None => tracing::info!(reason = "taille sous le minimum",
                                            kelly = format!("{size_k:.1}"), min = m.min_order_size,
                                            "✗ ordre live ignoré"),
                                        Some(size) => {
                                            let args = OrderArgs { side, price: order_price, size };
                                            match live_executor::place_order(cfg.live_armed, live_creds.as_ref(), token, m.neg_risk, args).await {
                                                Ok(live_executor::PlaceResult::Placed(id)) => {
                                                    live_shots += 1;
                                                    tracing::warn!(side = side.as_str(), order_id = %id,
                                                        price = format!("{order_price:.3}"), size,
                                                        "✅ ORDRE LIVE accepté");
                                                }
                                                Ok(live_executor::PlaceResult::DryRun) => tracing::warn!(
                                                    side = side.as_str(), price = format!("{order_price:.3}"), size,
                                                    "🔸 ordre live signé mais NON envoyé (LIVE_ARMED=false)"),
                                                Err(e) => tracing::error!(error = %e, "❌ ordre live échoué"),
                                            }
                                            last_fire_ms = now_ms;
                                        }
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
            d.live_pnl = live_pnl_val;
            d.live_shots = live_shots;
        }

        log_throttle += 1;
        if log_throttle % 100 == 0 {
            tracing::info!(real = format!("{:.3}", real_up), shots = paper.state.shots,
                cash = format!("{:.2}", paper.state.cash), "executor");
        }
    }
}
