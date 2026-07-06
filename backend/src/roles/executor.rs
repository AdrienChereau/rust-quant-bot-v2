//! Nœud Exécuteur (Dublin).
//! J6 : connecteur Polymarket (marché actif + carnet, rollover).
//! J7 : feed Binance (spot) + volatilité + pricing BS + quotes A-S reward-adjusted.
//! J8 (à venir) : exécution paper + inventaire + fusion CTF.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::bankroll::BankrollEngine;
use crate::config::Config;
use crate::connectors::binance;
use crate::connectors::polymarket::{Market, PolyBook, PolymarketClient};
use crate::dashboard::{Shared, SeriesPoint, WindowResult};
use crate::engines::spread_capture::{Side, SpreadCaptureConfig, SpreadCaptureEngine};
use crate::engines::{
    drift::DriftEngine, ofi::OfiEngine, pricing, radar::RadarEngine, volatility::VolatilityEngine,
};
use crate::execution::KillState;
use crate::inventory::PaperEngine;
use crate::signal::SignalTransport;
use crate::types::{BookUpdate, Signal, WireTick};

/// Meilleur ask (prix, taille) d'un carnet Polymarket (Vec non triés).
fn best_ask_level(book: &PolyBook) -> Option<(f64, f64)> {
    book.asks
        .iter()
        .filter(|l| l.price > 0.0 && l.size > 0.0)
        .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
        .map(|l| (l.price, l.size))
}

pub async fn run(cfg: Config, transport: Arc<dyn SignalTransport>, dash: Shared) -> anyhow::Result<()> {
    tracing::info!(
        role = "executor",
        dry_run = cfg.dry_run,
        dashboard_port = cfg.dashboard_port,
        "Nœud Exécuteur démarré"
    );

    // État KILL partagé (R5) entre la task signal et la boucle de cotation.
    let kill = Arc::new(KillState::new());

    // Écoute des signaux radar : KILL → pause ; Tick → source de données Tokyo
    // (spot/σ/drift/OFI/OBI calculés là-bas). Garde seq : les datagrammes UDP
    // réordonnés/dupliqués sont jetés (garde-fou n°2 du plan).
    let (remote_tx, remote_rx) =
        watch::channel::<Option<(WireTick, i64)>>(None);
    {
        let dash = dash.clone();
        let kill = kill.clone();
        let cooldown_ms = cfg.kill_pause_secs * 1000;
        tokio::spawn(async move {
            let mut last_seq: u64 = 0;
            loop {
                match transport.recv_signal().await {
                    Ok(Signal::Kill) => {
                        kill.trigger(cooldown_ms);
                        tracing::warn!("⚡ KILL — retrait des quotes + pause des fills");
                        let mut d = dash.write().await;
                        d.signals_received += 1;
                        d.paused = true;
                    }
                    Ok(Signal::Tick(t)) => {
                        if t.seq != 0 && t.seq <= last_seq {
                            continue; // réordonné/dupliqué → poubelle
                        }
                        last_seq = t.seq;
                        let _ = remote_tx
                            .send(Some((t, chrono::Utc::now().timestamp_millis())));
                    }
                    Ok(Signal::Heartbeat) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "réception signal");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }

    // Feed Binance local (spot BTC pour le pricing) + moteur de volatilité.
    let (spot_tx, spot_rx) = watch::channel::<Option<BookUpdate>>(None);
    let (sigma_tx, sigma_rx) = watch::channel::<f64>(cfg.volatility_floor);

    let url = cfg.binance_ws_url.clone();
    tokio::spawn(async move {
        if let Err(e) = binance::run(url, spot_tx).await {
            tracing::error!(error = %e, "feed Binance (exécuteur) arrêté");
        }
    });

    // Tâche volatilité : consomme le micro-price et publie le sigma annualisé.
    {
        let mut vol = VolatilityEngine::new(2000, cfg.volatility_floor);
        let mut rx = spot_rx.clone();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                let sample = rx.borrow().clone();
                if let Some(u) = sample {
                    if let Some(t) = u.price_tick() {
                        vol.update(t.ts_ms, t.micro_price);
                        let _ = sigma_tx.send(vol.annualized_sigma());
                    }
                }
            }
        });
    }

    quote_loop(cfg, spot_rx, sigma_rx, remote_rx, dash, kill).await
}

#[allow(clippy::too_many_arguments)]
async fn quote_loop(
    cfg: Config,
    spot_rx: watch::Receiver<Option<BookUpdate>>,
    sigma_rx: watch::Receiver<f64>,
    remote_rx: watch::Receiver<Option<(WireTick, i64)>>,
    dash: Shared,
    kill: Arc<KillState>,
) -> anyhow::Result<()> {
    let client = PolymarketClient::new();
    let bankroll = {
        // placeholder, recréé juste après avec equity initiale
        BankrollEngine::new(&cfg)
    };
    let mut bankroll = bankroll;
    // Moteur spread-capture taker (v5) — remplace tout le chemin maker.
    let mut sc = SpreadCaptureEngine::new(SpreadCaptureConfig {
        c_raw: cfg.sc_c_raw,
        fee_per_pair: cfg.sc_fee_per_pair,
        opening_leg_max: cfg.sc_opening_leg_max,
        max_imbalance: cfg.sc_max_imbalance,
        base_clip: cfg.sc_base_clip,
        max_clip: cfg.sc_max_clip,
        depth_gain: cfg.sc_depth_gain,
        max_clip_usdc: cfg.sc_max_clip_usdc,
        max_capital_per_market: cfg.sc_max_capital_per_market,
        min_seconds: cfg.sc_min_seconds,
        clip_interval_s: cfg.sc_clip_interval_s,
        gate_margin: cfg.sc_gate_margin,
        min_window_age_s: cfg.sc_min_window_age_s,
        completion_reserve: cfg.sc_completion_reserve,
        trend_filter: cfg.sc_trend_filter,
        pullback_filter: cfg.sc_pullback_filter,
        completion_max_price: cfg.sc_completion_max_price,
        completion_max_pair: cfg.sc_completion_max_pair,
    });
    // Buffer spot pour le micro-repli 5 s (timing H1 : acheter le pullback).
    let mut spot_hist: std::collections::VecDeque<(i64, f64)> =
        std::collections::VecDeque::with_capacity(32);
    let mut paper = PaperEngine::load_or_init(
        cfg.start_cash, cfg.max_position, cfg.min_merge_threshold, cfg.safety_mult,
        cfg.state_path.clone(), cfg.trades_path.clone(),
    );

    // Persistance de l'historique des fenêtres (le dashboard est en mémoire sinon :
    // chaque redémarrage perdrait la table analytique). JSONL append-only, rechargé
    // au boot — le PnL/cash reste dans paper_state, ceci ne stocke que l'affichage.
    let windows_path = std::env::var("WINDOWS_PATH")
        .unwrap_or_else(|_| "paper_windows_v8.jsonl".into());
    {
        let mut loaded: Vec<WindowResult> = Vec::new();
        if let Ok(txt) = std::fs::read_to_string(&windows_path) {
            for line in txt.lines().filter(|l| !l.trim().is_empty()) {
                if let Ok(w) = serde_json::from_str::<WindowResult>(line) {
                    loaded.push(w);
                }
            }
        }
        if !loaded.is_empty() {
            let n = loaded.len();
            if n > 2000 {
                loaded.drain(0..n - 2000);
            }
            tracing::info!(count = loaded.len(), "historique fenêtres rechargé");
            dash.write().await.windows = loaded;
        }
    }

    let mut current: Option<Market> = None;
    let mut strike: Option<f64> = None;
    let mut last_spot: Option<f64> = None;
    let mut last_window_slug: Option<String> = None;
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    let mut persist_ctr: u32 = 0;

    // Signal directionnel (Radar Tokyo). En mode `combined`, on le calcule localement
    // depuis le feed Binance de l'exécuteur ; en split, ces valeurs viendront du WireSignal.
    let mut drift_eng = DriftEngine::new(cfg.drift_halflife_secs);
    let mut ofi_eng = OfiEngine::new(5000);
    let radar_obi = RadarEngine::new(cfg.obi_depth_levels, cfg.obi_threshold, cfg.velocity_threshold);
    let mut last_realized = paper.state.realized_pnl; // pour le delta par fenêtre

    // v8 MAKER : bids restants + stats de fenêtre + disjoncteur de séries perdantes.
    let mut rest_up: Option<(f64, f64)> = None; // (prix, taille)
    let mut rest_dn: Option<(f64, f64)> = None;
    let mut win_rebate = 0.0f64; // rebate estimé de la fenêtre
    let mut win_merged = 0.0f64; // $ récupérés par merges dans la fenêtre
    let mut win_deployed = 0.0f64; // $ TOTAL achetés (cumulatif — sc.deployed() devient net des merges)
    let mut win_fills: u32 = 0;
    let mut win_imb_max = 0.0f64;
    let mut rebate_total = 0.0f64;
    let mut loss_streak: u32 = 0;
    let mut skip_ctr: u32 = 0; // fenêtres écoulées en régime "hard"
    let mut size_factor = 1.0f64;
    // Anti flip-flop : le drift doit garder son signe sc_trend_confirm_s avant
    // d'armer le directionnel (le couteau se pariait sur des micro-replis de 2 s).
    let mut trend_sign = true;
    let mut trend_since: i64 = 0;

    loop {
        poll.tick().await;

        let need_resolve = current.as_ref().map_or(true, |m| m.time_remaining_sec() <= 0);
        if need_resolve {
            // Résoudre le marché précédent (Up gagne si close ≥ open de la fenêtre).
            // Close = open de la fenêtre suivante (kline) ; à défaut, dernier spot observé.
            if let (Some(prev), Some(prev_strike)) = (current.as_ref(), strike) {
                // Le close OFFICIEL = open de la kline suivante. À :59.8 elle n'existe
                // pas encore → on RETRY jusqu'à 15 s au lieu de retomber en silence
                // sur le spot WS (qui peut être gelé — bug du 6 juil.). last_spot ne
                // sert plus que d'ultime secours, et il est déjà purgé si périmé.
                let mut close = None;
                for _ in 0..15 {
                    match binance::price_at_window_open(prev.window_ts + 300).await {
                        Ok(c) => { close = Some(c); break; }
                        Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
                    }
                }
                if close.is_none() {
                    tracing::error!("close kline introuvable après 15 s — fallback spot frais");
                    close = last_spot;
                }
                match close {
                    Some(c) => {
                        let up_won = c >= prev_strike;
                        // Stats de fenêtre capturées AVANT resolve/reset.
                        let rec = WindowResult {
                            start: prev.window_ts,
                            res: if up_won { "Up".into() } else { "Down".into() },
                            fills: win_fills,
                            avg_up: sc.avg(Side::Up),
                            avg_dn: sc.avg(Side::Down),
                            pair_cost: sc.pair_cost().unwrap_or(0.0),
                            imb_max: win_imb_max,
                            imb_final: sc.imbalance(),
                            deployed: win_deployed,
                            merged: win_merged,
                            rebate: win_rebate,
                            pnl: 0.0, // rempli après resolve
                        };
                        paper.resolve(up_won);
                        paper.persist();
                        let delta = paper.state.realized_pnl - last_realized;
                        last_realized = paper.state.realized_pnl;
                        // Disjoncteur BINAIRE (comportement mesuré de la cible sur 430+
                        // fenêtres : déployé médian 0$ après 2 pertes, taille PLEINE dès
                        // le retour — jamais de palier graduel). Après `streak_soft` pertes
                        // consécutives jouées : on saute UNE fenêtre, puis retour plein pot.
                        // Le ×0.25 est abandonné : il divisait par 4 les gains de sortie de
                        // série sans exister chez la cible.
                        if rec.deployed > 0.5 {
                            if delta <= 0.0 { loss_streak += 1 } else { loss_streak = 0 }
                        }
                        size_factor = if loss_streak >= cfg.sc_streak_soft && size_factor > 0.0 {
                            0.0 // pause d'une fenêtre
                        } else {
                            1.0 // retour taille pleine
                        };
                        let _ = skip_ctr;
                        rebate_total += win_rebate;
                        let final_rec = WindowResult { pnl: delta, ..rec };
                        // Persiste la fenêtre (append-only) avant de l'ajouter à l'affichage.
                        if let Ok(line) = serde_json::to_string(&final_rec) {
                            use std::io::Write;
                            if let Ok(mut fh) = std::fs::OpenOptions::new()
                                .create(true).append(true).open(&windows_path)
                            {
                                let _ = writeln!(fh, "{line}");
                            }
                        }
                        let mut d = dash.write().await;
                        d.windows.push(final_rec);
                        let n = d.windows.len();
                        if n > 2000 {
                            d.windows.drain(0..n - 2000);
                        }
                    }
                    None => tracing::warn!("résolution sautée : ni close kline ni spot disponible"),
                }
            }
            match client.get_current_btc_5m_market().await {
                Ok(Some(m)) => {
                    strike = binance::price_at_window_open(m.window_ts).await.ok();
                    sc.reset_window(); // état blended remis à zéro pour la nouvelle fenêtre
                    rest_up = None;
                    rest_dn = None;
                    win_rebate = 0.0;
                    win_merged = 0.0;
                    win_deployed = 0.0;
                    win_fills = 0;
                    win_imb_max = 0.0;
                    tracing::info!(
                        slug = %m.slug, remaining_s = m.time_remaining_sec(),
                        strike = ?strike, size_factor, loss_streak, "=== Nouveau marché BTC 5min ==="
                    );
                    current = Some(m);
                }
                Ok(None) => { tokio::time::sleep(Duration::from_secs(2)).await; continue; }
                Err(e) => { tracing::error!(error=%e,"résolution marché"); tokio::time::sleep(Duration::from_secs(2)).await; continue; }
            }
        }

        let Some(m) = &current else { continue };

        // Strike résilient : si la capture a échoué au rollover, on réessaie chaque
        // tick (le kline d'ouverture peut n'être disponible qu'après quelques secondes).
        if strike.is_none() {
            let w = m.window_ts;
            if let Ok(s) = binance::price_at_window_open(w).await {
                strike = Some(s);
                tracing::info!(strike = s, slug = %m.slug, "strike capturé (retry)");
            }
        }
        let Some(strike) = strike else { continue };

        // SOURCE DE DONNÉES : en split (USE_UDP_TRANSPORT), tout vient de Tokyo
        // (spot/σ/drift/OFI/OBI calculés au plus près de Binance, garde seq faite
        // à la réception). En local/combined, calcul local depuis le WS Binance.
        // GARDE DE FRAÎCHEUR commune : signal vieux → aucune quote, aucun
        // last_spot (le gel du 6 juil. a fait trader/résoudre sur un spot mort).
        let t_secs = m.time_remaining_sec().max(0) as f64;
        let t_years = pricing::years_from_secs(t_secs);
        let (spot, sigma, drift_ps, obi, ofi) = if cfg.use_udp_transport {
            let snap = *remote_rx.borrow();
            let Some((t, recv_ms)) = snap else { continue };
            let age_ms = chrono::Utc::now().timestamp_millis() - recv_ms;
            if age_ms > 3_000 {
                // le radar émet à 10 Hz : 3 s de silence = liaison morte
                rest_up = None;
                rest_dn = None;
                last_spot = None;
                tracing::warn!(age_ms, "signal Tokyo PÉRIMÉ — quotes retirées");
                continue;
            }
            (t.spot, t.sigma, t.drift, t.obi, t.ofi)
        } else {
            let bu = spot_rx.borrow().clone();
            let Some(bu) = bu else { continue };
            let Some(tick) = bu.price_tick() else { continue };
            let tick_age_ms = chrono::Utc::now().timestamp_millis() - bu.ts_ms as i64;
            if tick_age_ms > 10_000 {
                rest_up = None;
                rest_dn = None;
                last_spot = None; // ne JAMAIS résoudre avec un spot mort
                tracing::warn!(age_s = tick_age_ms / 1000, "spot Binance PÉRIMÉ — quotes retirées");
                continue;
            }
            let spot = tick.micro_price;
            drift_eng.update(tick.ts_ms, spot);
            let obi = radar_obi.calculate_obi(&bu.book);
            let bid_sz = bu.book.bids.values().next().copied().unwrap_or(0.0);
            let ask_sz = bu.book.asks.values().next().copied().unwrap_or(0.0);
            if let (Some(bb), Some(ba)) = (bu.book.best_bid(), bu.book.best_ask()) {
                ofi_eng.update(tick.ts_ms, bb, bid_sz, ba, ask_sz);
            }
            (spot, *sigma_rx.borrow(), drift_eng.per_sec(), obi, ofi_eng.value_norm())
        };
        last_spot = Some(spot);

        // Horizon du drift PLAFONNÉ : un momentum estimé sur ~25 s de mémoire n'a
        // pas à prédire 5 minutes (`sc_drift_horizon_s`). Même clamp qu'avant, la
        // seule différence en split est que le per-sec vient de Tokyo.
        let drift_log = pricing::clamp_drift(
            drift_ps * t_secs.min(cfg.sc_drift_horizon_s),
            sigma,
            t_years,
            cfg.drift_clamp_k,
        );

        // Juste valeur avec DRIFT (correctif tendance, validé en replay) → gate ⚡.
        let fair_up = pricing::fair_up_probability_drift(spot, strike, sigma, t_years, drift_log);

        // Carnets Up et Down (les deux côtés → permet la fusion CTF).
        let (up_book, down_book) = match (
            client.get_book(&m.up_token_id).await,
            client.get_book(&m.down_token_id).await,
        ) {
            (Ok(u), Ok(d)) => (u, d),
            _ => continue,
        };
        let (Some(up_mid), Some(down_mid)) = (up_book.mid(), down_book.mid()) else { continue };

        // Inventaire NET (R1) + equity/bankroll (R4).
        let net = paper.state.up_balance - paper.state.down_balance;
        let equity = BankrollEngine::equity(&paper.state, up_mid, down_mid);
        bankroll.observe(equity);
        if last_window_slug.as_deref() != Some(m.slug.as_str()) {
            bankroll.on_window_start(equity);
            last_window_slug = Some(m.slug.clone());
        }

        // === COPY MAKER V8 (copie complète 0xb27b, recalibrée sur 234 fenêtres) ===
        // Bids RESTANTS des deux rôles (directionnel côté tendance + complétion côté
        // déficitaire), remplis par règle de CROSS. Rebate estimé par fill maker.
        let paused = kill.is_paused(); // observabilité
        let now_s = chrono::Utc::now().timestamp();
        use chrono::Timelike;
        let sleeping = cfg.sc_sleep_hours_utc.contains(&chrono::Utc::now().hour());
        let (ask_up, ask_up_sz) = best_ask_level(&up_book).unwrap_or((0.0, 0.0));
        let (ask_dn, ask_dn_sz) = best_ask_level(&down_book).unwrap_or((0.0, 0.0));
        let (bb_up, bb_dn) = (
            up_book.best_bid().unwrap_or(0.0),
            down_book.best_bid().unwrap_or(0.0),
        );
        let sign_now = drift_ps > 0.0;
        if sign_now != trend_sign {
            trend_sign = sign_now;
            trend_since = now_s;
        }
        let trend_up = if now_s - trend_since >= cfg.sc_trend_confirm_s {
            Some(sign_now)
        } else {
            None // tendance non confirmée → complétion seule
        };
        let _ = &spot_hist; // (buffer pullback conservé pour le mode taker/tests)

        // 1) Quotes désirées → reprice discipline (> 1 tick d'écart = replace).
        let desired = if sleeping || paused {
            Vec::new()
        } else {
            sc.desired_bids(
                bb_up, bb_dn, fair_up, m.time_remaining_sec(), now_s, trend_up,
                m.tick_size, cfg.sc_directional_max, cfg.sc_directional_min, size_factor,
            )
        };
        let tick_sz = if m.tick_size > 0.0 { m.tick_size } else { 0.01 };
        for (side_rest, side) in [(&mut rest_up, Side::Up), (&mut rest_dn, Side::Down)] {
            let want = desired.iter().find(|b| b.side == side);
            match (want, side_rest.as_ref()) {
                (Some(b), Some((p, _))) if (b.price - p).abs() > tick_sz / 2.0 + 1e-9 => {
                    *side_rest = Some((b.price, b.size)); // cancel + repost (reprice)
                }
                (Some(b), None) => *side_rest = Some((b.price, b.size)),
                (None, Some(_)) => *side_rest = None, // plus de quote désirée → cancel
                _ => {} // on garde l'ordre en place (préserve la file en live)
            }
        }

        // 2) Fills par CROSS : le best ask traverse notre bid → fill certain.
        for (side_rest, side, ask, ask_sz) in [
            (&mut rest_up, Side::Up, ask_up, ask_up_sz),
            (&mut rest_dn, Side::Down, ask_dn, ask_dn_sz),
        ] {
            if let Some((price, size)) = *side_rest {
                if ask > 0.0 && ask <= price + 1e-9 {
                    let fill = size.min(ask_sz.max(1.0)).floor();
                    if fill >= 1.0 && paper.try_buy(side.as_str(), price, fill, "maker") {
                        sc.on_fill(side, price, fill, now_s);
                        win_fills += 1;
                        win_deployed += price * fill;
                        // Rebate estimé : part des frais taker payés par la contrepartie.
                        win_rebate += cfg.sc_rebate_rate * 0.07 * price * (1.0 - price) * fill;
                        if sc.imbalance().abs() > win_imb_max.abs() {
                            win_imb_max = sc.imbalance();
                        }
                        tracing::info!(
                            side = side.as_str(),
                            px = format!("{:.3}", price),
                            size = format!("{:.0}", fill),
                            imb = format!("{:.0}", sc.imbalance()),
                            "[V8] fill maker (cross)"
                        );
                        *side_rest = None; // re-quote au tick suivant (après cooldown)
                    }
                }
            }
        }

        // 2b) COMPLÉTION TAKER de dernier recours : une fenêtre ne doit JAMAIS finir
        // avec un seul côté. Si le bid de complétion maker n'a pas croisé et qu'on
        // approche de la fin, on paie l'ask (prime d'assurance, comme la cible qui
        // complète à −4,3¢ d'edge médian, jusqu'à >1$ la paire). Frais taker inclus,
        // pas de rebate sur ce fill.
        let remaining = m.time_remaining_sec();
        if (10..=45).contains(&remaining) && sc.imbalance().abs() >= 5.0 {
            let (side, ask, ask_sz) = if sc.imbalance() > 0.0 {
                (Side::Down, ask_dn, ask_dn_sz)
            } else {
                (Side::Up, ask_up, ask_up_sz)
            };
            if ask > 0.0 && ask <= 0.99 {
                let deficit = sc.imbalance().abs();
                let fill = deficit.min(ask_sz.max(1.0)).floor();
                let px_eff = ask + 0.07 * ask * (1.0 - ask); // fee_eq taker par part
                if fill >= 1.0 && paper.try_buy(side.as_str(), px_eff, fill, "taker") {
                    sc.on_fill(side, px_eff, fill, now_s);
                    win_fills += 1;
                    win_deployed += px_eff * fill;
                    tracing::info!(
                        side = side.as_str(),
                        px = format!("{:.3}", px_eff),
                        size = format!("{:.0}", fill),
                        pair = format!("{:?}", sc.pair_cost().map(|c| (c * 100.0).round() / 100.0)),
                        "[V8] complétion TAKER (assurance fin de fenêtre)"
                    );
                }
            }
        }

        // 3) MERGE différé et par BLOCS (mesuré chez la cible : Q1 100 s, médiane
        // 164 s, 805$/merge, 10 % seulement avant 60 s — jamais de goutte-à-goutte).
        // Avant 90 s on n'y touche pas : le livre directionnel reste intact pendant
        // l'accumulation, et le recyclage de budget ne relance pas de churn 50/50
        // précoce. Le seuil de bloc vient de MIN_MERGE_THRESHOLD (30 paires).
        if 300 - remaining >= 90 {
            let cash_before = paper.state.cash_usdc;
            paper.check_and_merge(0.1);
            let merged_now = (paper.state.cash_usdc - cash_before).max(0.0);
            if merged_now > 0.0 {
                win_merged += merged_now;
                sc.on_merge(merged_now); // 1 paire mergée = 1$ → budget fenêtre recyclé
            }
        }

        let position_value = BankrollEngine::position_value(&paper.state, up_mid, down_mid);
        let window_pnl = bankroll.window_pnl(equity);
        let drawdown = bankroll.drawdown_from_peak(equity);

        // Mise à jour du dashboard (état exécuteur + bankroll).
        {
            let mut d = dash.write().await;
            d.market_slug = m.slug.clone();
            d.remaining_s = m.time_remaining_sec();
            d.sigma = sigma;
            d.fair = fair_up;
            d.drift = drift_ps;
            d.obi_exec = obi;
            d.ofi = ofi;
            // v8 maker : « bid » = notre bid restant (0 si aucune quote posée).
            let buy_cap_up = rest_up.map(|(p, _)| p).unwrap_or(0.0);
            let buy_cap_dn = rest_dn.map(|(p, _)| p).unwrap_or(0.0);
            d.up_mid = up_mid;
            d.up_bid = buy_cap_up;
            d.up_ask = ask_up;
            d.down_mid = down_mid;
            d.down_bid = buy_cap_dn;
            d.down_ask = ask_dn;
            d.in_band = rest_up.is_some() || rest_dn.is_some();
            d.rebate_window = win_rebate;
            d.rebate_total = rebate_total + win_rebate;
            d.size_factor = size_factor;
            d.loss_streak = loss_streak;
            d.pair_cost = sc.pair_cost().unwrap_or(0.0);
            d.deployed = win_deployed;
            d.window_start = m.window_ts;
            d.params = crate::dashboard::StrategyParams {
                c_raw: cfg.sc_c_raw,
                fee_per_pair: cfg.sc_fee_per_pair,
                opening_leg_max: cfg.sc_opening_leg_max,
                gate_margin: cfg.sc_gate_margin,
                max_imbalance: cfg.sc_max_imbalance,
                base_clip: cfg.sc_base_clip,
                max_clip: cfg.sc_max_clip,
                depth_gain: cfg.sc_depth_gain,
                max_clip_usdc: cfg.sc_max_clip_usdc,
                max_capital_per_market: cfg.sc_max_capital_per_market,
                min_seconds: cfg.sc_min_seconds,
                clip_interval_s: cfg.sc_clip_interval_s,
                min_window_age_s: cfg.sc_min_window_age_s,
                completion_reserve: cfg.sc_completion_reserve,
                drift_horizon_s: cfg.sc_drift_horizon_s,
                trend_filter: cfg.sc_trend_filter,
                pullback_s: cfg.sc_pullback_s,
                completion_max_price: cfg.sc_completion_max_price,
                completion_max_pair: cfg.sc_completion_max_pair,
                drift_halflife_secs: cfg.drift_halflife_secs,
                drift_clamp_k: cfg.drift_clamp_k,
                volatility_floor: cfg.volatility_floor,
            };
            d.trades_path = cfg.trades_path.clone();
            d.cash = paper.state.cash_usdc;
            d.up_bal = paper.state.up_balance;
            d.down_bal = paper.state.down_balance;
            d.realized_pnl = paper.state.realized_pnl;
            d.fills = paper.state.fills;
            d.merges = paper.state.merges;
            d.latent = position_value;
            d.equity = equity;
            d.position_value = position_value;
            d.window_pnl = window_pnl;
            d.drawdown = drawdown;
            d.net_exposure = net;
            d.paused = paused;
            d.sells = paper.state.sells;
            d.maker_fills = paper.state.maker_fills;
            d.taker_fills = paper.state.taker_fills;
            d.last_block_reason =
                if sleeping { "sommeil (heures creuses UTC)".into() } else { String::new() };
            // Carnet Up : 6 meilleurs niveaux de chaque côté pour visualisation.
            let mut bids = up_book.bids.clone();
            bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
            let mut asks = up_book.asks.clone();
            asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
            d.book_bids = bids.iter().take(6)
                .map(|l| crate::dashboard::BookLevel { price: l.price, size: l.size }).collect();
            d.book_asks = asks.iter().take(6)
                .map(|l| crate::dashboard::BookLevel { price: l.price, size: l.size }).collect();

            // Série temporelle (ring ~10 min à 1/s) pour le graphique de fenêtre.
            d.series.push(SeriesPoint {
                t: chrono::Utc::now().timestamp_millis(),
                up_mid,
                down_mid,
                spot,
                up_bid: buy_cap_up, // notre bid maker restant Up
                up_ask: ask_up,
                equity,
                realized: paper.state.realized_pnl,
                imb: sc.imbalance(),
            });
            let n = d.series.len();
            if n > 600 {
                d.series.drain(0..n - 600);
            }
        }

        tracing::info!(
            rem_s = m.time_remaining_sec(),
            fair = format!("{:.3}", fair_up),
            drift = format!("{:+.4}", drift_log),
            obi = format!("{:+.2}", obi),
            ofi = format!("{:+.2}", ofi),
            ask_up = format!("{:.3}", ask_up),
            ask_dn = format!("{:.3}", ask_dn),
            pair = format!("{:.2}", sc.pair_cost().unwrap_or(0.0)),
            imb = format!("{:.0}", sc.imbalance()),
            deployed = format!("{:.2}", sc.deployed()),
            equity = format!("{:.2}", equity),
            net = format!("{:.0}", net),
            wpnl = format!("{:.2}", window_pnl),
            fills = paper.state.fills,
            state = if sleeping { "sleep" } else if paused { "kill(obs)" } else { "scan" },
            "sc"
        );

        persist_ctr += 1;
        if persist_ctr % 5 == 0 {
            paper.persist();
        }
    }
}

