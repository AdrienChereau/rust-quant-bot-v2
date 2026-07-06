//! Exécuteur PAIR-GTC (stratégie utilisateur) — instance parallèle (port 8700).
//!
//! Boucle : 2 GTC près de 50/50 → fill d'un côté (règle de cross) → cancel de
//! l'autre → complétion taker quand la paire passe sous `PG_PAIR_TARGET` →
//! tenir jusqu'à la résolution. Voir `engines/pair_gtc.rs` pour la logique pure.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::bankroll::BankrollEngine;
use crate::config::Config;
use crate::connectors::binance;
use crate::connectors::polymarket::{Market, PolyBook, PolymarketClient};
use crate::dashboard::{Shared, SeriesPoint, WindowResult};
use crate::engines::pair_gtc::{Action, PairGtcConfig, PairGtcEngine};
use crate::inventory::PaperEngine;
use crate::signal::SignalTransport;
use crate::types::BookUpdate;

fn best_ask_level(book: &PolyBook) -> Option<(f64, f64)> {
    book.asks
        .iter()
        .filter(|l| l.price > 0.0 && l.size > 0.0)
        .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
        .map(|l| (l.price, l.size))
}

pub async fn run(cfg: Config, transport: Arc<dyn SignalTransport>, dash: Shared) -> anyhow::Result<()> {
    tracing::info!(
        role = "executor-gtc",
        dry_run = cfg.dry_run,
        dashboard_port = cfg.dashboard_port,
        "Nœud Exécuteur PAIR-GTC démarré"
    );

    // Draine les signaux radar (observabilité seulement : pas d'ordres à retirer,
    // les GTC virtuels ne bougent pas sur KILL dans cette v1).
    {
        let dash = dash.clone();
        tokio::spawn(async move {
            loop {
                match transport.recv_signal().await {
                    Ok(_) => {
                        let mut d = dash.write().await;
                        d.signals_received += 1;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
                }
            }
        });
    }

    // Feed Binance : uniquement pour le spot de résolution (close de fenêtre).
    let (spot_tx, spot_rx) = watch::channel::<Option<BookUpdate>>(None);
    let url = cfg.binance_ws_url.clone();
    tokio::spawn(async move {
        if let Err(e) = binance::run(url, spot_tx).await {
            tracing::error!(error = %e, "feed Binance (gtc) arrêté");
        }
    });

    let client = PolymarketClient::new();
    let mut paper = PaperEngine::load_or_init(
        cfg.start_cash, cfg.max_position, cfg.min_merge_threshold, cfg.safety_mult,
        cfg.state_path.clone(), cfg.trades_path.clone(),
    );
    let mut engine = PairGtcEngine::new(PairGtcConfig {
        size: cfg.pg_size,
        band: cfg.pg_band,
        entry_min_remaining: cfg.pg_entry_min_remaining,
        entry_deadline: cfg.pg_entry_deadline,
        pair_target: cfg.pg_pair_target,
        fee_per_pair: cfg.sc_fee_per_pair,
        require_rising: cfg.pg_require_rising,
    });
    // Historique des mids pour la règle « acheter pendant une montée » (rebond).
    let mut mid_hist: std::collections::VecDeque<(i64, f64, f64)> =
        std::collections::VecDeque::with_capacity(64);

    let mut current: Option<Market> = None;
    let mut strike: Option<f64> = None;
    let mut last_spot: Option<f64> = None;
    let mut last_realized = paper.state.realized_pnl;
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    let mut persist_ctr: u32 = 0;
    // Anti double-entrée après redémarrage : la machine à états n'étant pas
    // persistée, on saute la fenêtre déjà en cours au boot (cycle possiblement fait).
    let mut first_window = true;

    loop {
        poll.tick().await;

        let need_resolve = current.as_ref().map_or(true, |m| m.time_remaining_sec() <= 0);
        if need_resolve {
            if let (Some(prev), Some(prev_strike)) = (current.as_ref(), strike) {
                let close = binance::price_at_window_open(prev.window_ts + 300)
                    .await
                    .ok()
                    .or(last_spot);
                match close {
                    Some(c) => {
                        let up_won = c >= prev_strike;
                        paper.resolve(up_won);
                        paper.persist();
                        let delta = paper.state.realized_pnl - last_realized;
                        last_realized = paper.state.realized_pnl;
                        let mut d = dash.write().await;
                        d.windows.push(WindowResult {
                            start: prev.window_ts,
                            res: if up_won { "Up".into() } else { "Down".into() },
                            fills: 0, avg_up: 0.0, avg_dn: 0.0, pair_cost: 0.0,
                            imb_max: 0.0, imb_final: 0.0, deployed: 0.0, merged: 0.0,
                            rebate: 0.0, pnl: delta,
                        });
                        let n = d.windows.len();
                        if n > 60 {
                            d.windows.drain(0..n - 60);
                        }
                    }
                    None => tracing::warn!("résolution sautée : ni close kline ni spot disponible"),
                }
            }
            match client.get_current_btc_5m_market().await {
                Ok(Some(m)) => {
                    strike = binance::price_at_window_open(m.window_ts).await.ok();
                    if first_window && m.time_remaining_sec() < 295 {
                        // Fenêtre déjà entamée au démarrage → pas d'entrée (anti double-cycle).
                        engine.phase = crate::engines::pair_gtc::Phase::Done;
                        tracing::info!(slug = %m.slug, "[GTC] fenêtre en cours au boot → skip");
                    } else {
                        engine.reset_window();
                    }
                    first_window = false;
                    tracing::info!(slug = %m.slug, remaining_s = m.time_remaining_sec(), "=== [GTC] nouveau marché ===");
                    current = Some(m);
                }
                Ok(None) => { tokio::time::sleep(Duration::from_secs(2)).await; continue; }
                Err(e) => { tracing::error!(error=%e,"[GTC] découverte marché"); tokio::time::sleep(Duration::from_secs(2)).await; continue; }
            }
        }
        let Some(m) = &current else { continue };
        if strike.is_none() {
            if let Ok(s) = binance::price_at_window_open(m.window_ts).await {
                strike = Some(s);
            }
        }

        if let Some(bu) = spot_rx.borrow().clone() {
            if let Some(t) = bu.price_tick() {
                last_spot = Some(t.micro_price);
            }
        }

        // Carnets des deux côtés.
        let (up_book, down_book) = match (
            client.get_book(&m.up_token_id).await,
            client.get_book(&m.down_token_id).await,
        ) {
            (Ok(u), Ok(d)) => (u, d),
            _ => continue,
        };
        let (Some(up_mid), Some(down_mid)) = (up_book.mid(), down_book.mid()) else { continue };
        let (bb_up, ba_up) = (up_book.best_bid().unwrap_or(0.0), best_ask_level(&up_book).map(|x| x.0).unwrap_or(0.0));
        let (bb_dn, ba_dn) = (down_book.best_bid().unwrap_or(0.0), best_ask_level(&down_book).map(|x| x.0).unwrap_or(0.0));

        // Rebond : mid en hausse d'au moins ½ tick sur le lookback (règle « montée »).
        let now_ms = chrono::Utc::now().timestamp_millis();
        mid_hist.push_back((now_ms, up_mid, down_mid));
        let cutoff = now_ms - cfg.pg_rising_lookback_s * 1000;
        while mid_hist.front().map_or(false, |(t, _, _)| *t < cutoff - 2000) {
            mid_hist.pop_front();
        }
        let half_tick = m.tick_size / 2.0;
        let (rising_up, rising_dn) = mid_hist
            .iter()
            .find(|(t, _, _)| *t >= cutoff)
            .map(|(_, mu, md)| (up_mid >= mu + half_tick, down_mid >= md + half_tick))
            .unwrap_or((false, false));

        // Machine à états de la stratégie.
        for act in engine.on_tick(up_mid, bb_up, ba_up, bb_dn, ba_dn, m.tick_size, m.time_remaining_sec(), rising_up, rising_dn) {
            match act {
                Action::MakerFill { side, price, size } => {
                    if paper.try_buy(side.as_str(), price, size, "maker") {
                        tracing::info!(side = side.as_str(), px = format!("{price:.3}"),
                            size = format!("{size:.0}"), "[GTC] jambe fillée (cross)");
                    }
                }
                Action::CancelGtc { side } => {
                    tracing::info!(side = side.as_str(), "[GTC] cancel de l'ordre opposé");
                }
                Action::TakerBuy { side, price, size } => {
                    if paper.try_buy(side.as_str(), price, size, "taker") {
                        tracing::info!(side = side.as_str(), px = format!("{price:.3}"),
                            size = format!("{size:.0}"), "[GTC] paire complétée ✓");
                    }
                }
            }
        }

        // Dashboard.
        let equity = BankrollEngine::equity(&paper.state, up_mid, down_mid);
        let net = paper.state.up_balance - paper.state.down_balance;
        {
            let (rest_up, rest_dn) = engine.resting();
            let pair_cost = match &engine.phase {
                crate::engines::pair_gtc::Phase::Complete { pair_cost } => *pair_cost,
                _ => 0.0,
            };
            let mut d = dash.write().await;
            d.market_slug = m.slug.clone();
            d.remaining_s = m.time_remaining_sec();
            d.up_mid = up_mid;
            d.up_bid = rest_up;
            d.up_ask = ba_up;
            d.down_mid = down_mid;
            d.down_bid = rest_dn;
            d.down_ask = ba_dn;
            d.pair_cost = pair_cost;
            d.deployed = 0.0;
            d.trades_path = cfg.trades_path.clone();
            d.cash = paper.state.cash_usdc;
            d.up_bal = paper.state.up_balance;
            d.down_bal = paper.state.down_balance;
            d.realized_pnl = paper.state.realized_pnl;
            d.fills = paper.state.fills;
            d.equity = equity;
            d.net_exposure = net;
            d.sells = paper.state.sells;
            d.maker_fills = paper.state.maker_fills;
            d.taker_fills = paper.state.taker_fills;
            d.last_block_reason = engine.phase_label();
            d.series.push(SeriesPoint {
                t: chrono::Utc::now().timestamp_millis(),
                up_mid,
                down_mid,
                spot: last_spot.unwrap_or(0.0),
                up_bid: rest_up,
                up_ask: ba_up,
                equity,
                realized: paper.state.realized_pnl,
                imb: net,
            });
            let n = d.series.len();
            if n > 600 {
                d.series.drain(0..n - 600);
            }
        }

        tracing::info!(
            rem_s = m.time_remaining_sec(),
            mid_up = format!("{up_mid:.3}"),
            phase = engine.phase_label(),
            equity = format!("{equity:.2}"),
            "gtc"
        );

        persist_ctr += 1;
        if persist_ctr % 5 == 0 {
            paper.persist();
        }
    }
}
