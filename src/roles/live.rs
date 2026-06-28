//! Nœud **Live (Dublin)** — récepteur, exécution réelle uniquement.
//!
//! **Zéro code paper** : pas de `PaperEngine`, pas de simulation VWAP, pas d'écriture du journal
//! paper dans la hot-loop. Le nœud est *toujours live* (`live_enabled = true` au démarrage) ; le
//! Start/Stop du dashboard ne fait que basculer `live_paused`. Les ordres passent par l'`OrderEngine`
//! (acteur mpsc) — la hot loop 50 ms n'attend jamais un POST CLOB. Bankroll via `watch::channel`.

use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, watch};

use crate::concurrency::bus::Side;
use crate::config::Config;
use crate::dashboard;
use crate::net::udp;
use crate::net::wire::WireSignal;
use crate::polymarket::live_executor::{self, LiveCredentials};
use crate::polymarket::order_engine::{self, OrderCmd, OrderResult};
use crate::polymarket::pm_poller::{spawn_pm_poller, PmShared};
use crate::polymarket::relayer::{Market, PolyBook};
use crate::polymarket::pm_user_ws;
use crate::polymarket::pm_websocket;
use crate::state::RuntimeControls;
use crate::strategy::bankroll::{self, KellyParams};
use crate::strategy::live_position::LivePositionManager;

/// Contexte d'un BUY en attente de confirmation par l'OrderEngine.
struct PendingOpen {
    rx: oneshot::Receiver<OrderResult>,
    side: Side,
    token_id: String,
    neg_risk: bool,
    order_price: f64,
    size: f64,
    tick: f64,
    now_ms: u64,
}

pub async fn run(cfg: Config, listen_port: u16) -> anyhow::Result<()> {
    tracing::info!(listen_port, "🎯 LIVE (Dublin) démarré — exécution réelle");

    // Nœud toujours-live : live activé d'office ; le Start/Stop ne touche que `live_paused`.
    // Par sécurité, on démarre EN PAUSE (l'opérateur presse Start pour armer l'exécution).
    let controls = Arc::new(RuntimeControls::new());
    controls.live_enabled.store(true, Ordering::Relaxed);

    let live_creds = LiveCredentials::from_env();
    if let Some(ref c) = live_creds {
        if let Err(e) = live_executor::startup_poly(c).await {
            tracing::error!(error = %e, "🛑 startup Polymarket échoué — arrêt");
            return Err(e);
        }
    }
    if cfg.live_armed {
        tracing::warn!(creds = live_creds.is_some(), "⚠️  LIVE_ARMED=true — envoi réel possible");
    }
    if cfg.live_force_min_size {
        tracing::warn!("⚠️  LIVE_FORCE_MIN_SIZE=true — taille minimale forcée");
    }

    // Bankroll via watch::channel — zéro lock dans la hot loop.
    let (bk_tx, bk_rx) = watch::channel(None::<f64>);
    if let Some(creds) = live_creds.clone() {
        let tx = bk_tx.clone();
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(Duration::from_secs(cfg.bankroll_poll_secs));
            loop {
                poll.tick().await;
                match live_executor::get_collateral_balance(&creds).await {
                    Ok(usdc) => { let _ = tx.send(Some(usdc));
                        tracing::info!(usdc = format!("{usdc:.2}"), "💰 bankroll réelle CLOB"); }
                    Err(e) => tracing::warn!(error = %e, "lecture bankroll CLOB échouée"),
                }
            }
        });
    }

    // OrderEngine : acteur mpsc — POST CLOB hors hot loop.
    let engine_tx = live_creds.as_ref()
        .map(|c| order_engine::spawn_order_engine(c.clone(), cfg.live_armed, cfg.order_engine_queue));

    // User WS : une seule task ; on lui envoie le condition_id au rollover.
    let (user_ws_cond_tx, mut user_ws_fill_rx) = live_creds.as_ref()
        .map(|c| pm_user_ws::init_user_ws(c.clone()))
        .map(|(tx, rx)| (Some(tx), Some(rx)))
        .unwrap_or((None, None));

    let dash = dashboard::shared(cfg.dry_run, "live");
    {
        let (port, st, ct) = (cfg.dashboard_port, dash.clone(), controls.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct).await; });
    }

    let pm = Arc::new(Mutex::new(PmShared::default()));
    let ws_market_tx = pm_websocket::init_market_ws(pm.clone());
    spawn_pm_poller(pm.clone(), false, Some(ws_market_tx), live_creds.clone(), cfg.pm_ws_stale_threshold_ms);

    let lat = crate::latency::shared();
    {
        let l = lat.clone();
        tokio::spawn(async move { crate::latency::run(l, crate::latency::Probes::PmOnly).await; });
    }

    let kelly = KellyParams {
        kelly_fraction: cfg.kelly_fraction, max_size_pct: cfg.max_kelly_size_pct,
        tp_cents: cfg.take_profit_cents, sl_cents: cfg.stop_loss_cents, max_hold_secs: cfg.max_hold_secs,
    };
    let mut live_mgr = LivePositionManager::load_or_init(
        kelly,
        std::env::var("LIVE_STATE_PATH").unwrap_or_else(|_| "data/live_state.json".into()),
        std::env::var("LIVE_TRADES_PATH").unwrap_or_else(|_| "data/live_trades.jsonl".into()),
    );

    let mut rx = udp::listen(listen_port).await?;
    let mut last_fire_ms: u64 = 0;
    let mut last_fair: f64 = 0.5;
    let mut tick_interval = tokio::time::interval(Duration::from_millis(50));
    let mut log_throttle: u32 = 0;
    let mut live_dd = bankroll::LiveDrawdown::default();
    let mut live_pnl = bankroll::LivePnl::default();
    let mut was_active = false;
    let mut live_shots: u64 = 0;
    let mut pending_opens: Vec<PendingOpen> = Vec::new();
    let mut pending_close: Option<(oneshot::Receiver<OrderResult>, &'static str)> = None;
    let mut user_ws_condition_id: String = String::new();
    // Latence pipeline (mise à jour au dernier ordre soumis).
    let mut last_transport_ms: Option<u64> = None; // radar→live (NTP)
    let mut last_decide_ms: Option<u64> = None;     // recv UDP → try_send (mono-horloge)

    // Snapshot hoissé pour traitement immédiat du signal UDP (Bloc E).
    let mut now_ms: u64 = 0;
    let mut live_bankroll_val: Option<f64> = None;
    let mut market: Option<Market> = None;
    let mut real_up: f64 = 0.5;
    let mut up_book: Arc<PolyBook> = Arc::new(PolyBook::default());
    let mut down_book: Arc<PolyBook> = Arc::new(PolyBook::default());
    let mut remaining_s: i64 = 0;

    tracing::info!("🔄 boucle live démarrée — tick 50 ms actif");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT reçu — arrêt propre (live)");
                break Ok(());
            }
            Some(sig) = rx.recv() => {
                match sig {
                    WireSignal::Kill { .. } => tracing::warn!("⚡ KILL reçu — abstention"),
                    WireSignal::Attack { side, price, sent_ms, .. } => {
                        // Latence transport radar→live (requiert NTP sync) + chrono décision.
                        let recv_ms = chrono::Utc::now().timestamp_millis() as u64;
                        let recv_instant = Instant::now();
                        let transport_ms = recv_ms.saturating_sub(sent_ms);
                        let fair = price as f64;
                        last_fair = fair;
                        let gap = match side { Side::Up => fair - real_up, Side::Down => real_up - fair };
                        let reject = if controls.is_breaker_tripped() { Some("breaker déclenché") }
                            else if !controls.live_active() { Some("live en pause") }
                            else if market.is_none() { Some("pas de marché") }
                            else if remaining_s <= cfg.end_window_block_secs { Some("fin de fenêtre") }
                            else if now_ms.saturating_sub(last_fire_ms) < cfg.cooldown_ms { Some("cooldown") }
                            else if gap < cfg.gap_min { Some("gap insuffisant") }
                            else { None };
                        if let Some(reason) = reject {
                            tracing::info!(reason, side = side.as_str(), fair = format!("{fair:.3}"),
                                real = format!("{real_up:.3}"), gap = format!("{gap:+.3}"),
                                gap_min = cfg.gap_min, "✗ signal rejeté (live)");
                        } else if let Some(m) = &market {
                            let (book, token) = if side == Side::Up {
                                (&*up_book, &m.up_token_id)
                            } else {
                                (&*down_book, &m.down_token_id)
                            };
                            let edge = gap;
                            if live_mgr.position().is_some() || !pending_opens.is_empty() {
                                tracing::info!(reason = "position live déjà ouverte/pending", "✗ ordre live ignoré");
                            } else {
                                match (live_bankroll_val, engine_tx.as_ref()) {
                                    (None, _) => tracing::warn!("bankroll pas encore lue — tir ignoré"),
                                    (_, None) => tracing::warn!("OrderEngine absent — tir ignoré"),
                                    (Some(bk), Some(engine)) => {
                                        let order_price = book.best_ask().unwrap_or(real_up);
                                        let sized = if cfg.live_force_min_size {
                                            Some(m.min_order_size)
                                        } else {
                                            bankroll::adjust_size_to_min(
                                                kelly.kelly_size_for(edge, order_price, bk),
                                                m.min_order_size,
                                            )
                                        };
                                        match sized {
                                            None => tracing::info!(min = m.min_order_size, "✗ taille sous le minimum"),
                                            Some(size) if size * order_price > bk => tracing::warn!(
                                                cost = format!("{:.2}", size * order_price),
                                                bankroll = format!("{bk:.2}"),
                                                "✗ bankroll insuffisante"),
                                            Some(size) => {
                                                if cfg.live_force_min_size {
                                                    tracing::warn!(size, "⚠️ taille FORCÉE au minimum");
                                                }
                                                let (tx, rx_r) = oneshot::channel();
                                                let cmd = OrderCmd::Open {
                                                    side, token_id: token.clone(), neg_risk: m.neg_risk,
                                                    price: order_price, size, tick: m.tick_size,
                                                    min_order_size: m.min_order_size, now_ms, reply: tx,
                                                };
                                                if engine.try_send(cmd).is_ok() {
                                                    pending_opens.push(PendingOpen {
                                                        rx: rx_r, side, token_id: token.clone(),
                                                        neg_risk: m.neg_risk, order_price, size,
                                                        tick: m.tick_size, now_ms,
                                                    });
                                                    last_fire_ms = now_ms;
                                                    last_transport_ms = Some(transport_ms);
                                                    last_decide_ms = Some(recv_instant.elapsed().as_millis() as u64);
                                                    tracing::info!(side = side.as_str(), price = order_price, size,
                                                        transport_ms, "⚡ BUY soumis à OrderEngine");
                                                } else {
                                                    tracing::warn!("OrderEngine plein — tir ignoré");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                continue;
            }
            _ = tick_interval.tick() => {}
        }

        // ── Tick 50ms ────────────────────────────────────────────────────────────────
        now_ms = chrono::Utc::now().timestamp_millis() as u64;
        live_bankroll_val = *bk_rx.borrow();

        {
            let g = pm.lock().unwrap();
            market = g.market.clone();
            real_up = g.real_up;
            up_book = g.up_book.clone();
            down_book = g.down_book.clone();
            remaining_s = g.remaining_s;
        }

        // ── 0. Rollover user WS — notifie la task du nouveau condition_id ──────────────
        if let Some(ref m) = market {
            if m.condition_id != user_ws_condition_id && !m.condition_id.is_empty() {
                user_ws_condition_id = m.condition_id.clone();
                if let Some(ref tx) = user_ws_cond_tx {
                    let _ = tx.send(Some(m.condition_id.clone()));
                }
            }
        }

        // ── 0b. Drain fills user WS ────────────────────────────────────────────────────
        if let Some(ref mut fill_rx) = user_ws_fill_rx {
            if fill_rx.has_changed().unwrap_or(false) {
                if let Some(fill) = fill_rx.borrow_and_update().clone() {
                    tracing::info!(order_id = %fill.order_id, filled = fill.filled_size,
                        price = fill.avg_price, is_sell = fill.is_sell, "✅ fill confirmé via user WS");
                    if fill.is_sell {
                        if let Some((_, reason)) = pending_close.take() {
                            live_mgr.apply_close(
                                fill.order_id.clone(),
                                Some(fill.filled_size),
                                Some(fill.avg_price),
                                reason,
                            );
                            if let (Some(creds), tx) = (live_creds.clone(), bk_tx.clone()) {
                                tokio::spawn(async move {
                                    if let Ok(b) = live_executor::get_collateral_balance(&creds).await {
                                        let _ = tx.send(Some(b));
                                    }
                                });
                            }
                        }
                    }
                }
            }
        }

        // ── 1. Drain résultats OrderEngine ────────────────────────────────────────────
        pending_opens.retain_mut(|p| {
            match p.rx.try_recv() {
                Ok(res) => {
                    live_mgr.on_buy_result(res, p.side, &p.token_id, p.neg_risk,
                        p.order_price, p.size, p.tick, p.now_ms);
                    false
                }
                Err(oneshot::error::TryRecvError::Empty) => true,
                Err(_) => false,
            }
        });
        if let Some((r, reason)) = pending_close.as_mut() {
            match r.try_recv() {
                Ok(res) => { live_mgr.on_sell_result(res, reason); pending_close = None; }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(_) => { pending_close = None; }
            }
        }

        // ── 2. Live manage → OrderEngine SELL (non-bloquant) ─────────────────────────
        if pending_close.is_none() {
            if let (Some(pos), Some(engine)) = (live_mgr.position(), engine_tx.as_ref()) {
                if let Some(m) = &market {
                    let book = if pos.side == Side::Up { &*up_book } else { &*down_book };
                    if let Some(bid) = book.best_bid() {
                        let held_s = (now_ms.saturating_sub(pos.opened_ms) / 1000) as i64;
                        let reason = if bid >= pos.tp_price { Some("take_profit") }
                            else if bid <= pos.sl_price { Some("stop_loss") }
                            else if held_s >= kelly.max_hold_secs || remaining_s <= 30 { Some("max_hold") }
                            else { None };
                        if let Some(r) = reason {
                            let exit = match r { "take_profit" => pos.tp_price, "stop_loss" => pos.sl_price, _ => bid };
                            let (tx, rx_r) = oneshot::channel();
                            let cmd = OrderCmd::Close {
                                token_id: pos.token_id.clone(), side: pos.side, neg_risk: pos.neg_risk,
                                price: exit, size: pos.size, tick: m.tick_size, reason: r, reply: tx,
                            };
                            if engine.try_send(cmd).is_ok() {
                                pending_close = Some((rx_r, r));
                                tracing::info!(reason = r, "⚡ SELL soumis à OrderEngine");
                            }
                        }
                    }
                }
            }
        }

        // ── 3. Circuit breaker (drawdown sur bankroll réelle) ─────────────────────────
        let breaker_hit = live_bankroll_val.map_or(false, |real| live_dd.breached(real, cfg.max_drawdown));
        if !controls.is_breaker_tripped() && breaker_hit && controls.trip_breaker() {
            tracing::error!(max_dd = cfg.max_drawdown, "🛑 CIRCUIT BREAKER live — drawdown atteint");
        }

        let active = controls.live_active();
        if active && !was_active { live_pnl.reset(); live_shots = 0; }
        was_active = active;
        let live_pnl_val = if active { live_bankroll_val.map(|bk| live_pnl.update(bk)) } else { None };

        // ── 4. Dashboard (champs live uniquement) ─────────────────────────────────────
        let lat_snap = lat.lock().unwrap().clone();
        let pm_ws_stale_ms = {
            let last = pm.lock().unwrap().last_ws_ts_ms;
            if last > 0 { Some(now_ms.saturating_sub(last)) } else { None }
        };
        {
            let mut d = dash.write().await;
            d.market_slug = market.as_ref().map(|m| m.slug.clone()).unwrap_or_default();
            d.remaining_s = remaining_s;
            d.fair_up = last_fair; d.real_up = real_up; d.gap = last_fair - real_up;
            if let Some(p) = live_mgr.position() {
                d.in_position = true; d.pos_side = p.side.as_str().into();
                d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
            } else {
                d.in_position = false;
            }
            d.mode = if controls.is_breaker_tripped() { "BREAKER" }
                else if controls.live_active() { "LIVE" } else { "PAUSE" }.into();
            d.live_enabled = controls.is_live_enabled();
            d.live_paused = controls.is_live_paused();
            d.live_armed = cfg.live_armed;
            d.breaker_tripped = controls.is_breaker_tripped();
            d.max_drawdown = cfg.max_drawdown;
            d.lat_polymarket_ms = lat_snap.polymarket_ms;
            d.live_bankroll = live_bankroll_val;
            d.live_pnl = if live_mgr.state.shots > 0 { Some(live_mgr.state.realized_pnl) } else { live_pnl_val };
            d.live_shots = live_mgr.state.shots.max(live_shots);
            d.live_force_min = cfg.live_force_min_size;
            d.lat_last_buy_ms = live_mgr.last_buy_ms;
            d.lat_last_sell_ms = live_mgr.last_sell_ms;
            d.pm_ws_stale_ms = pm_ws_stale_ms;
            // Latence totale signal→ordre = transport (radar→live) + décision + POST CLOB.
            d.lat_transport_ms = last_transport_ms;
            d.lat_decide_ms = last_decide_ms;
            d.lat_post_ms = live_mgr.last_buy_ms; // BUY FAK : début POST → réponse CLOB
            d.lat_total_ms = match (last_transport_ms, last_decide_ms, live_mgr.last_buy_ms) {
                (Some(t), Some(d2), Some(p)) => Some(t + d2 + p),
                _ => None,
            };
        }

        log_throttle += 1;
        if log_throttle % 100 == 0 {
            tracing::info!(real = format!("{:.3}", real_up), live_shots = live_mgr.state.shots,
                bankroll = format!("{:?}", live_bankroll_val), "live");
        }
    }
}
