//! Nœud **Paper (machine séparée)** — récepteur de signaux, simulation pure.
//!
//! Reçoit les mêmes signaux UDP que le nœud live (le radar tire aux deux), mais n'exécute
//! QUE le `PaperEngine` (fills VWAP simulés, PnL fictif). **Zéro code live** : aucune
//! credential, aucun `OrderEngine`, aucun POST CLOB. Volontairement isolé du live pour ne
//! jamais en partager le CPU/les locks (cf. plan split paper/live).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::concurrency::bus::Side;
use crate::config::Config;
use crate::dashboard;
use crate::net::udp;
use crate::net::wire::WireSignal;
use crate::polymarket::pm_poller::{spawn_pm_poller, PmShared};
use crate::polymarket::relayer::{Market, PolyBook};
use crate::polymarket::pm_websocket;
use crate::state::RuntimeControls;
use crate::strategy::bankroll::{self, KellyParams, PaperEngine};

pub async fn run(cfg: Config, listen_port: u16) -> anyhow::Result<()> {
    tracing::info!(listen_port, "📝 PAPER (sim) démarré — aucun ordre réel");

    // Paper actif par défaut (live_* inutilisés ici).
    let controls = Arc::new(RuntimeControls::new());

    // Console de tuning à chaud — ici, seuls les réglages d'EXÉCUTION ont un effet (gap, cooldown,
    // Kelly) : le paper reçoit le score déjà décidé par le radar, il ne recalcule pas le signal.
    let tuning = crate::tuning::Tuning::load(&cfg);

    let dash = dashboard::shared(true, "paper");
    dash.write().await.trades_path =
        std::env::var("TRADES_PATH").unwrap_or_else(|_| "data/sniper_trades.jsonl".into());
    {
        let (port, st, ct, tn) = (cfg.dashboard_port, dash.clone(), controls.clone(), tuning.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct, Some(tn)).await; });
    }

    // Flux marché Polymarket (carnets pour fills VWAP + marks) — public, sans credential.
    let pm = Arc::new(Mutex::new(PmShared::default()));
    let ws_market_tx = pm_websocket::init_market_ws(pm.clone());
    spawn_pm_poller(pm.clone(), false, Some(ws_market_tx), None, cfg.pm_ws_stale_threshold_ms);

    let lat = crate::latency::shared();
    {
        let l = lat.clone();
        tokio::spawn(async move { crate::latency::run(l, crate::latency::Probes::PmOnly).await; });
    }

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
    if cfg.fixed_order_usd > 0.0 {
        tracing::warn!(usd = cfg.fixed_order_usd, "⚠️ FIXED_ORDER_USD actif — Kelly ignoré (tests)");
    }

    let mut rx = udp::listen(listen_port).await?;
    let mut last_fire_ms: u64 = 0;
    let mut last_series_ms: u64 = 0;
    let mut last_fair: f64 = 0.5;
    let mut tick_interval = tokio::time::interval(Duration::from_millis(50));
    let mut log_throttle: u32 = 0;
    let mut last_transport_ms: Option<u64> = None;

    // Snapshot hoissé pour traitement immédiat du signal UDP.
    let mut now_ms: u64 = 0;
    let mut market: Option<Market> = None;
    let mut real_up: f64 = 0.5;
    let mut up_book: Arc<PolyBook> = Arc::new(PolyBook::default());
    let mut down_book: Arc<PolyBook> = Arc::new(PolyBook::default());
    let mut remaining_s: i64 = 0;

    tracing::info!("🔄 boucle paper démarrée — tick 50 ms actif");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT reçu — arrêt propre (paper)");
                break Ok(());
            }
            Some(sig) = rx.recv() => {
                match sig {
                    WireSignal::Kill { .. } => tracing::warn!("⚡ KILL reçu — abstention"),
                    WireSignal::Attack { side, price, sent_ms, .. } => {
                        // Snapshot tuning (lock-free) : gap/cooldown/sizing réglables à chaud.
                        let tp = tuning.snapshot();
                        paper.update_sizing(tp.kelly_fraction, tp.max_kelly_size_pct, tp.kelly_price_max);
                        // Latence transport radar→paper (informatif ; requiert NTP sync).
                        last_transport_ms = Some((chrono::Utc::now().timestamp_millis() as u64).saturating_sub(sent_ms));
                        let fair = price as f64;
                        last_fair = fair;
                        let gap = match side { Side::Up => fair - real_up, Side::Down => real_up - fair };
                        let reject = if controls.is_breaker_tripped() { Some("breaker déclenché") }
                            else if controls.is_paper_paused() { Some("paper en pause") }
                            else if market.is_none() { Some("pas de marché") }
                            else if remaining_s <= cfg.end_window_block_secs { Some("fin de fenêtre") }
                            else if now_ms.saturating_sub(last_fire_ms) < tp.cooldown_ms as u64 { Some("cooldown") }
                            else if gap < tp.gap_min { Some("gap insuffisant") }
                            else { None };
                        if let Some(reason) = reject {
                            tracing::info!(reason, side = side.as_str(), fair = format!("{fair:.3}"),
                                real = format!("{real_up:.3}"), gap = format!("{gap:+.3}"),
                                gap_min = tp.gap_min, "✗ signal rejeté (paper)");
                        } else if let Some(m) = &market {
                            let (book, token) = if side == Side::Up {
                                (&*up_book, &m.up_token_id)
                            } else {
                                (&*down_book, &m.down_token_id)
                            };
                            if paper.fire(side, token, gap, book, m.tick_size, m.min_order_size, now_ms) {
                                last_fire_ms = now_ms;
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
        {
            let g = pm.lock().unwrap();
            market = g.market.clone();
            real_up = g.real_up;
            up_book = g.up_book.clone();
            down_book = g.down_book.clone();
            remaining_s = g.remaining_s;
        }

        // Échantillon série graphe (1/s, borné). Pas de BTC spot côté paper → 0.
        if now_ms.saturating_sub(last_series_ms) >= 1000 {
            crate::series::push(now_ms, last_fair, real_up, 0.0);
            last_series_ms = now_ms;
        }

        // ── Paper manage (TP/SL/max-hold) ─────────────────────────────────────────────
        let mark_bid = if let Some(p) = &paper.position {
            let bk = if p.side == Side::Up { &*up_book } else { &*down_book };
            bk.best_bid()
        } else { None };
        paper.manage(mark_bid, now_ms, remaining_s);

        // ── Circuit breaker (drawdown paper) ──────────────────────────────────────────
        let breaker_hit = bankroll::check_drawdown_breaker(paper.equity(mark_bid), cfg.start_cash, cfg.max_drawdown);
        if !controls.is_breaker_tripped() && breaker_hit && controls.trip_breaker() {
            tracing::error!(max_dd = cfg.max_drawdown, "🛑 CIRCUIT BREAKER paper — drawdown atteint");
        }

        // ── Dashboard (champs paper uniquement) ───────────────────────────────────────
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
            if let Some(p) = &paper.position {
                d.in_position = true; d.pos_side = p.side.as_str().into();
                d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
            } else {
                d.in_position = false;
            }
            d.cash = paper.state.cash; d.equity = paper.equity(mark_bid);
            d.realized_pnl = paper.state.realized_pnl; d.drawdown = paper.drawdown();
            d.shots = paper.state.shots; d.wins = paper.state.wins; d.losses = paper.state.losses;
            d.hit_rate = paper.hit_rate();
            d.mode = if controls.is_breaker_tripped() { "BREAKER" }
                else if controls.is_paper_paused() { "PAUSE" } else { "PAPER" }.into();
            d.paper_paused = controls.is_paper_paused();
            d.breaker_tripped = controls.is_breaker_tripped();
            d.initial_capital = cfg.start_cash;
            d.max_drawdown = cfg.max_drawdown;
            d.lat_polymarket_ms = lat_snap.polymarket_ms;
            d.pm_ws_stale_ms = pm_ws_stale_ms;
            d.lat_transport_ms = last_transport_ms;
            d.fixed_order_usd = cfg.fixed_order_usd;
        }

        log_throttle += 1;
        if log_throttle % 100 == 0 {
            tracing::info!(real = format!("{:.3}", real_up), shots = paper.state.shots,
                cash = format!("{:.2}", paper.state.cash), "paper");
        }
    }
}
