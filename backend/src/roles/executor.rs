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
use crate::connectors::pm_ws;
#[cfg(feature = "live")]
use crate::live::engine::{LiveCtx, RestingOrder};
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
                        // Garde anti-réordonnancement : on jette les seq en retard…
                        // SAUF si le recul est massif = le radar a REDÉMARRÉ (seq
                        // repart à 1) — bug du 7 juil. : après chaque redeploy Tokyo,
                        // Dublin jetait tout le flux et restait sourd.
                        if t.seq != 0 && t.seq <= last_seq && last_seq - t.seq < 300 {
                            continue; // réordonné/dupliqué → poubelle
                        }
                        if t.seq < last_seq {
                            tracing::warn!(old = last_seq, new = t.seq, "seq radar réinitialisé (redémarrage Tokyo) — flux réaccepté");
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
    // ═══ MODE LIVE (DRY_RUN=false + feature `live`) : ordres réels sur le CLOB.
    // Mêmes décisions que le paper ; seule l'exécution change. Le PaperEngine
    // reste le grand livre (miroir alimenté par les fills réels).
    #[cfg(feature = "live")]
    let mut live: Option<LiveCtx> = if !cfg.dry_run {
        let creds = crate::live::auth::LiveCredentials::from_env()
            .ok_or_else(|| anyhow::anyhow!("DRY_RUN=false mais credentials POLY_* incomplets"))?;
        Some(LiveCtx::start(creds, cfg.live_armed).await?)
    } else {
        None
    };
    #[cfg(not(feature = "live"))]
    if !cfg.dry_run {
        anyhow::bail!("DRY_RUN=false requiert un build `--features live` (rustc ≥ 1.91)");
    }
    #[cfg(feature = "live")]
    let mut lrest_up: Option<RestingOrder> = None;
    #[cfg(feature = "live")]
    let mut lrest_dn: Option<RestingOrder> = None;
    #[cfg(feature = "live")]
    let mut last_insurance_ms: i64 = 0;
    #[cfg(feature = "live")]
    let mut last_place_ms: [i64; 2] = [0, 0]; // anti-churn : ≥4 s entre replaces par côté
    let mut last_nosig_log: i64 = 0; // throttle du warn « signal absent »
    // Carnets Polymarket en WS (v9) : la boucle passe à 4 Hz sur données live ;
    // le REST ne sert plus que de secours si le flux WS est périmé (>5 s).
    let pm_state: pm_ws::PmWsShared = std::sync::Arc::new(std::sync::RwLock::new(
        pm_ws::PmWsState::default(),
    ));
    let pm_tokens = pm_ws::spawn(pm_state.clone());
    let mut last_slow_tick_s: i64 = 0; // throttle 1 Hz (log + série + REST secours)
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
    let mut poll = tokio::time::interval(Duration::from_millis(250));
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
            let now_s_roll = chrono::Utc::now().timestamp();
            let _ = now_s_roll;
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
                        #[cfg(feature = "live")]
                        if let Some(lv) = live.as_mut() {
                            // Récolter les fills des ordres encore restants AVANT de
                            // les annuler — ils appartiennent à la fenêtre qui se clôt.
                            for (r, is_up) in [(lrest_up.take(), true), (lrest_dn.take(), false)] {
                                if let Some(r) = r {
                                    if let Some(f) = lv.harvest_and_cancel(&r, is_up).await {
                                        let side = if f.is_up { Side::Up } else { Side::Down };
                                        paper.apply_live_fill(side.as_str(), f.price, f.size, "maker");
                                        sc.on_fill(side, f.price, f.size, now_s_roll);
                                        lv.note_fill_cash(f.price, f.size);
                                    }
                                }
                            }
                            // REDEEM on-chain des résidus (gagnants + poussière) de la
                            // fenêtre résolue — le pUSD revient au wallet, le sync cash suit.
                            if paper.state.up_balance + paper.state.down_balance > 0.5 {
                                lv.redeem(&prev.condition_id).await;
                            }
                        }
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
                    let _ = pm_tokens.send(vec![m.up_token_id.clone(), m.down_token_id.clone()]);
                    #[cfg(feature = "live")]
                    if let Some(lv) = live.as_mut() {
                        lv.on_new_market(&m.condition_id, &m.up_token_id, &m.down_token_id).await;
                        lrest_up = None;
                        lrest_dn = None;
                    }
                    // Budget de fenêtre en % de bankroll (SC_BANKROLL_PCT>0) —
                    // recalculé à CHAQUE rollover ; le recyclage par merge du
                    // moteur (on_merge) réutilise ce budget dans la fenêtre.
                    if cfg.sc_bankroll_pct > 0.0 {
                        let bankroll = {
                            #[cfg(feature = "live")]
                            { live.as_ref().map(|lv| lv.cash).unwrap_or(paper.state.cash_usdc) }
                            #[cfg(not(feature = "live"))]
                            { paper.state.cash_usdc }
                        };
                        sc.cfg.max_capital_per_market = (bankroll * cfg.sc_bankroll_pct).max(1.0);
                        tracing::info!(
                            bankroll = format!("{bankroll:.2}"),
                            cap = format!("{:.2}", sc.cfg.max_capital_per_market),
                            "budget fenêtre = pct × bankroll"
                        );
                    }
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
            let Some((t, recv_ms)) = snap else {
                // JAMAIS rien reçu : dire pourquoi le bot ne fait rien (le cas
                // « reçu puis coupé » est couvert plus bas par PÉRIMÉ).
                let now = chrono::Utc::now().timestamp();
                if now - last_nosig_log >= 10 {
                    last_nosig_log = now;
                    tracing::warn!(
                        ecoute = %cfg.signal_addr,
                        "AUCUN tick Tokyo reçu — vérifier SIGNAL_TARGET(2) du radar + son restart + le chemin UDP"
                    );
                    let mut d = dash.write().await;
                    d.last_block_reason = "SIGNAL TOKYO ABSENT (UDP muet)".into();
                    d.market_slug = m.slug.clone();
                    d.remaining_s = m.time_remaining_sec();
                    d.cash = paper.state.cash_usdc;
                }
                continue;
            };
            let age_ms = chrono::Utc::now().timestamp_millis() - recv_ms;
            if age_ms > 3_000 {
                // le radar émet à 10 Hz : 3 s de silence = liaison morte
                rest_up = None;
                rest_dn = None;
                last_spot = None;
                #[cfg(feature = "live")]
                if let Some(lv) = live.as_ref() {
                    lv.cancel_all().await;
                    lrest_up = None;
                    lrest_dn = None;
                }
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

        // Carnets Up et Down : WS temps réel d'abord, REST en secours (1 Hz max).
        let now_ms_books = chrono::Utc::now().timestamp_millis() as u64;
        let ws_up = pm_ws::fresh_book(&pm_state, &m.up_token_id, now_ms_books, 5_000);
        let ws_dn = pm_ws::fresh_book(&pm_state, &m.down_token_id, now_ms_books, 5_000);
        let slow_tick = (now_ms_books / 1000) as i64 != last_slow_tick_s; // vrai ~1×/s
        let (up_book, down_book) = match (ws_up, ws_dn) {
            (Some(u), Some(d)) => (u, d),
            _ => {
                if !slow_tick {
                    continue; // pas de spam REST à 4 Hz quand le WS est muet
                }
                match (
                    client.get_book(&m.up_token_id).await,
                    client.get_book(&m.down_token_id).await,
                ) {
                    (Ok(u), Ok(d)) => (u, d),
                    _ => continue,
                }
            }
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
        // Interrupteur manuel (bouton dashboard) : OFF = aucune nouvelle quote,
        // aucune assurance ; en live les ordres restants sont annulés ci-dessous.
        let enabled = { dash.read().await.trading_enabled };
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
        // Veto OFI (v9) : le flux d'ordres Binance (fenêtre 5 s, calculé à Tokyo
        // en split) contredit franchement le drift → pas de pari directionnel ce
        // tick, la complétion continue. |OFI| < seuil = bruit, on n'agit pas.
        let trend_up = match trend_up {
            Some(true) if cfg.sc_ofi_confirm && ofi <= -cfg.sc_ofi_min => None,
            Some(false) if cfg.sc_ofi_confirm && ofi >= cfg.sc_ofi_min => None,
            t => t,
        };

        // 1) Quotes désirées → reprice discipline (> 1 tick d'écart = replace).
        let desired = if sleeping || !enabled {
            Vec::new()
        } else {
            sc.desired_bids(
                bb_up, bb_dn, fair_up, m.time_remaining_sec(), now_s, trend_up,
                m.tick_size, cfg.sc_directional_max, cfg.sc_directional_min, size_factor,
                cfg.sc_symmetric,
            )
        };
        let tick_sz = if m.tick_size > 0.0 { m.tick_size } else { 0.01 };

        // ═══ Signal Tokyo au service de l'INVENTAIRE (8 juil.) ═══
        let mut desired = desired;
        // KILL ASYMÉTRIQUE : sur alerte radar, seule l'ouverture EN DANGER est
        // retirée (celle que le mouvement va traverser) ; l'autre garde sa file.
        // Les complétions survivent toujours : le côté qui crashe, c'est
        // précisément celui qu'un déficit veut acheter moins cher.
        if paused {
            use crate::engines::spread_capture::endangered_side;
            match endangered_side(drift_ps, cfg.sc_urgency_drift) {
                Some(danger) => desired.retain(|b| b.completion || b.side != danger),
                None => desired.retain(|b| b.completion), // direction illisible → les 2 ouvertures sautent
            }
        }
        // URGENCE DE COMPLÉTION : le sous-jacent part dans le sens qui renchérit
        // notre déficit → le bid monte à ask−tick (agressif mais toujours maker).
        for b in desired.iter_mut() {
            if b.completion
                && crate::engines::spread_capture::completion_urgent(b.side, drift_ps, cfg.sc_urgency_drift)
            {
                let ask = if b.side == Side::Up { ask_up } else { ask_dn };
                if ask > tick_sz {
                    let aggressive = (((ask - tick_sz).max(b.price)) / tick_sz).floor() * tick_sz;
                    let capped = aggressive.min(cfg.sc_completion_max_price);
                    if capped > b.price + 1e-9 {
                        tracing::info!(side = ?b.side, from = b.price, to = capped,
                            drift = format!("{drift_ps:+.4}"), "complétion URGENTE (Tokyo)");
                        b.price = capped;
                    }
                }
            }
        }

        // ═══ CHEMIN LIVE : reconcile GTC réels + fills réels ═══
        #[cfg(feature = "live")]
        if let Some(lv) = live.as_mut() {
            let mut harvested: Vec<crate::live::engine::LiveFill> = Vec::new();
            // KILL/pause → tout annuler — mais JAMAIS sans récolter d'abord
            // (un cancel aveugle perd le fill arrivé entre-temps).
            if sleeping || !enabled {
                if lrest_up.is_some() || lrest_dn.is_some() {
                    for (lrest, is_up) in [(&mut lrest_up, true), (&mut lrest_dn, false)] {
                        if let Some(r) = lrest.take() {
                            if let Some(f) = lv.harvest_and_cancel(&r, is_up).await {
                                harvested.push(f);
                            }
                        }
                    }
                    lv.cancel_all().await;
                }
            } else {
                // Ouvertures : tailles STRICTEMENT égales des deux côtés (le bump
                // 1$ asymétrique a créé 95 orphelines le 7 juil.). Fenêtre de
                // faisabilité : commune ≥ minimums des 2 côtés ET ≤ tailles engine ;
                // vide (marché décidé) → AUCUNE ouverture.
                let min_shares = m.min_order_size.max(1.0);
                let open_common: Option<f64> = {
                    let ou = desired.iter().find(|b| b.side == Side::Up && !b.completion);
                    let od = desired.iter().find(|b| b.side == Side::Down && !b.completion);
                    match (ou, od) {
                        (Some(u), Some(d)) => {
                            let req_u = min_shares.max((1.0_f64 / u.price.max(0.01)).ceil());
                            let req_d = min_shares.max((1.0_f64 / d.price.max(0.01)).ceil());
                            let lo = req_u.max(req_d);
                            let hi = u.size.min(d.size);
                            if hi + 1e-9 >= lo && (u.price + d.price) * hi <= lv.cash {
                                Some(hi)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                };
                for (lrest, side, ask) in [
                    (&mut lrest_up, Side::Up, ask_up),
                    (&mut lrest_dn, Side::Down, ask_dn),
                ] {
                    let want = desired.iter().find(|b| b.side == side);
                    // ANTI-CROSS : un GTC au prix de l'ask deviendrait taker →
                    // on clampe strictement sous l'ask (préserve le statut maker).
                    let want_px = want.map(|b| {
                        let cap = if ask > 0.0 { ask - tick_sz } else { b.price };
                        ((b.price.min(cap)) / tick_sz).floor() * tick_sz
                    });
                    // Valeurs RAW Polymarket : taille ≥ min_order_size du marché,
                    // et jamais plus que le cash réel restant.
                    // Tailles (8 juil. — fin des 95 orphelines) :
                    //  · COMPLÉTION : jamais plus que le déficit. Si les minimums
                    //    PM (5 parts / 1$) exigeraient de SUR-acheter → résidu
                    //    accepté (perte bornée, spirale évitée).
                    //  · OUVERTURE : taille commune pré-calculée (ou rien).
                    let want_sz: Option<f64> = match (want, want_px) {
                        (Some(b), Some(px)) => {
                            let min_req = min_shares.max((1.0_f64 / px).ceil());
                            if b.completion {
                                let sz = (b.size * 100.0).floor() / 100.0;
                                if sz + 1e-9 < min_req {
                                    let now = chrono::Utc::now().timestamp();
                                    if now - last_nosig_log >= 30 {
                                        last_nosig_log = now;
                                        tracing::info!(side = ?side, deficit = sz, min_req,
                                            "résidu accepté — compléter exigerait de sur-acheter (minimums PM)");
                                    }
                                    None
                                } else {
                                    Some(sz)
                                }
                            } else {
                                open_common
                            }
                        }
                        _ => None,
                    };
                    let side_ix = if side == Side::Up { 0 } else { 1 };
                    match (want_sz, want_px, &*lrest) {
                        (Some(sz), Some(px), cur) if px >= 0.01 && px * sz <= lv.cash => {
                            // ANTI-CHURN (leçon du 7 juil. : replace toutes les 1-3 s =
                            // chasse le prix + fills partiels éparpillés) : reprice
                            // seulement si l'écart dépasse 2 ticks ET ≥4 s depuis le
                            // dernier placement de ce côté.
                            let reprice = match cur {
                                Some(r) => {
                                    (px - r.price).abs() > 2.0 * tick_sz + 1e-9
                                        && now_ms_books as i64 - last_place_ms[side_ix] >= 4_000
                                }
                                None => now_ms_books as i64 - last_place_ms[side_ix] >= 4_000,
                            };
                            if reprice {
                                if let Some(r) = lrest.take() {
                                    if let Some(f) = lv.harvest_and_cancel(&r, side == Side::Up).await {
                                        harvested.push(f);
                                    }
                                }
                                *lrest = lv.place_bid(side == Side::Up, px, sz).await;
                                last_place_ms[side_ix] = now_ms_books as i64;
                            }
                        }
                        (None, _, Some(r)) => {
                            let r2 = r.clone();
                            if let Some(f) = lv.harvest_and_cancel(&r2, side == Side::Up).await {
                                harvested.push(f);
                            }
                            *lrest = None;
                        }
                        _ => {}
                    }
                }
            }
            // Fills réels : WS temps réel (trade + états d'ordres) + récoltes
            // pré-annulation + poll (autorité + balayage anti double-ordre).
            let mut fills = lv.drain_ws(&mut lrest_up, &mut lrest_dn);
            fills.append(&mut harvested);
            fills.extend(lv.reconcile(&mut lrest_up, &mut lrest_dn, now_ms_books as i64).await);
            for f in fills {
                lv.note_fill_cash(f.price, f.size); // cash réel décrémenté sans attendre le CLOB
                let side = if f.is_up { Side::Up } else { Side::Down };
                let ltype = if f.maker { "maker" } else { "taker" };
                // INCONDITIONNEL : un fill réel est un FAIT — la comptabilité
                // l'enregistre toujours (bug des 36 vs 18 Up du 8 juil.).
                paper.apply_live_fill(side.as_str(), f.price, f.size, ltype);
                sc.on_fill(side, f.price, f.size, now_s);
                win_fills += 1;
                win_deployed += f.price * f.size;
                if f.maker {
                    win_rebate += cfg.sc_rebate_rate * 0.07 * f.price * (1.0 - f.price) * f.size;
                }
                if sc.imbalance().abs() > win_imb_max.abs() {
                    win_imb_max = sc.imbalance();
                }
                tracing::info!(
                    side = side.as_str(),
                    px = format!("{:.3}", f.price),
                    size = format!("{:.1}", f.size),
                    ltype,
                    imb = format!("{:.0}", sc.imbalance()),
                    "[LIVE] fill réel"
                );
            }
            // Assurance taker (jamais une fenêtre à un seul côté) — FAK réel,
            // au plus 1 tentative / 3 s (le fill revient par le WS).
            let remaining_l = m.time_remaining_sec();
            if enabled
                && (10..=45).contains(&remaining_l)
                && sc.imbalance().abs() >= 5.0
                && now_ms_books as i64 - last_insurance_ms >= 3_000
            {
                let (is_up, ask, ask_sz) = if sc.imbalance() > 0.0 {
                    (false, ask_dn, ask_dn_sz)
                } else {
                    (true, ask_up, ask_up_sz)
                };
                if ask > 0.0 && ask <= 0.99 {
                    // Assurance : JAMAIS plus que le déficit (le sur-achat forcé
                    // par les minimums relançait la spirale). Sous les minimums →
                    // résidu accepté, perte bornée.
                    let sz = ((sc.imbalance().abs().min(ask_sz.max(1.0))) * 100.0).floor() / 100.0;
                    let min_req = m.min_order_size.max(1.0).max((1.0_f64 / ask).ceil());
                    if sz + 1e-9 >= min_req && ask * sz <= lv.cash {
                        last_insurance_ms = now_ms_books as i64;
                        lv.place_insurance_fak(is_up, ask, sz).await;
                    }
                }
            }
            // MERGE on-chain (mêmes règles que le paper : ≥90 s de fenêtre, blocs
            // ≥ MIN_MERGE_THRESHOLD) — un seul en vol ; à la confirmation, le
            // miroir merge + le moteur recycle son budget + le cash se resynce.
            if let Some(pairs_done) = lv.poll_merge_done() {
                // Crédit EXACT : uniquement les paires de LA transaction (le
                // check_and_merge global sur-créditait → PnL fantôme au dashboard).
                let p = pairs_done
                    .min(paper.state.up_balance)
                    .min(paper.state.down_balance)
                    .max(0.0);
                paper.state.up_balance -= p;
                paper.state.down_balance -= p;
                paper.state.merges += 1;
                paper.persist();
                lv.cash += p; // le merge est on-chain : crédit RÉEL immédiat…
                lv.force_cash_resync(); // …et le CLOB tranche au prochain sync
                sc.on_merge(p);
                win_merged += p;
                tracing::info!(pairs_tx = pairs_done, credited = p, "merge on-chain appliqué (crédit exact)");
            }
            let elapsed_l = 300 - m.time_remaining_sec();
            let mirror_pairs = paper.state.up_balance.min(paper.state.down_balance);
            if elapsed_l >= 90
                && mirror_pairs >= cfg.min_merge_threshold
                && lv.merge_available()
            {
                let pairs = mirror_pairs.floor();
                match lv.start_merge(pairs).await {
                    crate::live::engine::MergeStart::WouldRevert => {
                        // La simulation refuse = les tokens n'y sont plus → le
                        // merge PRÉCÉDENT est passé malgré le timeout de suivi.
                        // Crédit EXACT + resync (la vérité on-chain tranchera).
                        let p = pairs
                            .min(paper.state.up_balance)
                            .min(paper.state.down_balance)
                            .max(0.0);
                        paper.state.up_balance -= p;
                        paper.state.down_balance -= p;
                        paper.state.merges += 1;
                        paper.persist();
                        lv.cash += p;
                        sc.on_merge(p);
                        win_merged += p;
                        lv.force_cash_resync();
                        tracing::info!(pairs_tx = pairs, credited = p,
                            "merge déjà passé on-chain (would revert) — crédit exact");
                    }
                    _ => {}
                }
            }

            // POSITIONS RÉELLES : le miroir s'aligne sur la vérité on-chain
            // (≤1×/60 s, ou immédiatement après un merge/redeem incertain).
            // Incident du 7 juil. : miroir « équilibré » ≠ réalité déséquilibrée
            // → aucune complétion pendant que la vraie jambe mourait.
            if let Some((ru, rd)) = lv.real_positions(now_ms_books as i64).await {
                let (mu, md) = (paper.state.up_balance, paper.state.down_balance);
                if (ru - mu).abs() > 0.5 || (rd - md).abs() > 0.5 {
                    tracing::warn!(
                        reel_up = ru, reel_dn = rd, miroir_up = mu, miroir_dn = md,
                        "positions désynchronisées — miroir ALIGNÉ sur la réalité"
                    );
                    paper.state.up_balance = ru;
                    paper.state.down_balance = rd;
                    // le moteur suit aussi (imbalance/complétion sur la vérité)
                    sc.shares_up = ru;
                    sc.shares_dn = rd;
                }
            }

            // Cash réel : sync CLOB (≤1×/10 s). Il n'y a PAS de cash miroir en
            // live — le state ne fait que REFLÉTER le wallet (bug du 8 juil. :
            // un cash fictif censurait les fills réels).
            lv.sync_cash(false).await;
            paper.state.cash_usdc = lv.cash;
            {
                let mut d = dash.write().await;
                d.live_collateral = lv.cash;
                d.live_wallet_pnl = lv.cash - lv.baseline;
            }
            // Miroir des bids pour le dashboard.
            rest_up = lrest_up.as_ref().map(|r| (r.price, r.size));
            rest_dn = lrest_dn.as_ref().map(|r| (r.price, r.size));
        }

        let live_mode = !cfg.dry_run;
        if !live_mode {
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

        } // fin du chemin PAPER (simulation cross-fill)

        // 2b) COMPLÉTION TAKER de dernier recours : une fenêtre ne doit JAMAIS finir
        // avec un seul côté. Si le bid de complétion maker n'a pas croisé et qu'on
        // approche de la fin, on paie l'ask (prime d'assurance, comme la cible qui
        // complète à −4,3¢ d'edge médian, jusqu'à >1$ la paire). Frais taker inclus,
        // pas de rebate sur ce fill.
        let remaining = m.time_remaining_sec();
        if !live_mode && enabled && (10..=45).contains(&remaining) && sc.imbalance().abs() >= 5.0 {
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
        if !live_mode && 300 - remaining >= 90 {
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
            if d.last_block_reason.starts_with("SIGNAL TOKYO") {
                d.last_block_reason.clear();
            }
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
            if slow_tick {
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
        }

        if slow_tick {
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
            state = if !enabled { "OFF(manuel)" } else if sleeping { "sleep" } else if paused { "kill(obs)" } else { "scan" },
            "sc"
        );
        }
        last_slow_tick_s = (now_ms_books / 1000) as i64;

        persist_ctr += 1;
        if persist_ctr % 20 == 0 {
            paper.persist(); // ~1×/5 s, comme avant le passage à 4 Hz
        }
    }
}

