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
use crate::live::engine::{LiveCtx, LiveSlot};
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
    // ÉCHELLE de quoting : jusqu'à N niveaux de prix par côté (LiveSlot = ordre
    // + adresse côté/niveau). Le grand livre s'en fiche (comptage par order_id).
    #[cfg(feature = "live")]
    let mut lrest: Vec<LiveSlot> = Vec::new();
    #[cfg(feature = "live")]
    let mut last_insurance_ms: i64 = 0;
    #[cfg(feature = "live")]
    let mut last_place_ms: [[i64; 4]; 2] = [[0; 4]; 2]; // anti-churn par (côté, niveau)
    // Cooldown STRATÉGIQUE (spec volume/rebates) : après un merge à paire > 1$,
    // pas d'ESCALADE (urgence) pendant 45 s — mais ouvertures et complétions au
    // complément continuent : elles ABAISSENT le blended (moyenne à la baisse).
    #[cfg(feature = "live")]
    let mut urgency_block_until: i64 = 0;
    // Epoch ms du dernier fill appliqué : un fill CLOB met quelques secondes à
    // se régler on-chain — aligner le miroir sur une lecture chaîne faite dans
    // cette fenêtre EFFACE des fills corrects (incident 18:15:34).
    #[cfg(feature = "live")]
    let mut last_fill_wall_ms: i64 = 0;
    // Qualité d'exécution : coût de paire blended AU MOMENT de chaque merge.
    let mut win_merge_cost_sum = 0.0f64;
    let mut win_merge_n: u32 = 0;
    // Taxe taker payée (7 % × p(1−p) × taille — formule VÉRIFIÉE sur les fills
    // du 8 juil. : 51,7¢ affiché = 50¢ exécuté + 0,105$ de frais sur 6 parts).
    let mut win_taker_fees = 0.0f64;
    let mut last_nosig_log: i64 = 0; // throttle du warn « signal absent »
    // Throttle du log « complétion agressive » : la cible est recalculée à
    // chaque boucle depuis le prix de base → sans mémoire, la même ligne
    // partait CHAQUE seconde (12 juil. 17:19 : 20+ lignes identiques).
    let mut last_aggr_px: [f64; 2] = [0.0; 2];
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
        opening_stop_s: cfg.sc_opening_stop_s,
        open_max_price: cfg.sc_open_max_price,
        dust_tol: cfg.sc_dust_tol,
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

    // MODE SKEW : momentum du carnet PM (ring 30 s), côté parié, cooldown
    // post-retournement (anti-whipsaw), dernier état loggé.
    let mut pm_hist: std::collections::VecDeque<(i64, f64)> = std::collections::VecDeque::new();
    #[allow(unused_mut)] // muté par la sortie éclair (chemin live uniquement)
    let mut skew_cool_until: i64 = 0;
    #[allow(unused_assignments, unused_mut, unused_variables)]
    let mut bet_side: Option<Side> = None; // inventaire skew détenu (sortie éclair si retournement)
    // Côté armé PERSISTANT (hystérésis) : armé au seuil plein, désarmé seulement
    // quand le signal retombe sous la MOITIÉ du seuil (incident 12 juil. 16:51 :
    // pm_mom oscillant pile à ±0.06 → arm/désarm toutes les 2-5 s → 2 sorties
    // éclair dans la même fenêtre).
    let mut skew_armed: Option<Side> = None;
    let mut last_skew_logged: Option<Side> = None;
    let mut last_pm_pull: Option<Side> = None; // pull demi-seuil (log de transition)
    // PERSISTANCE du lean PM (refonte 13 juil.) : le pm-momentum est un
    // détecteur de MOUVEMENT, pas de tendance — il tirait sur chaque jambe de
    // rebond (12 Up @0.46 revendus @0.23). Un lean ne compte que si son SIGNE
    // tient sc_pm_persist_s : un rebond de 20 s ne tient pas, un grind oui.
    let mut pm_lean_sign: i8 = 0;
    let mut pm_lean_since_ms: i64 = 0;
    // FAK d'accumulation : tiré UNE fois par armement (armé → pending).
    #[allow(unused_mut, unused_assignments, unused_variables)]
    let mut accum_fak_pending = false;
    // Parts FAK en vol (taille, epoch ms) : le fill met ~300 ms à revenir par
    // le WS — sans ce compteur, la quote maker posterait un 2e clip entier
    // pendant le trou (24 pour un cap de 12).
    #[allow(unused_mut, unused_assignments, unused_variables)]
    let mut accum_fak_inflight: (f64, i64) = (0.0, 0);
    // MESURE DE L'EDGE TOKYO : `dir_call` = dernière conviction directionnelle
    // FORTE (drift+OFI d'accord) de la fenêtre ; à la résolution on la compare
    // au gagnant réel. dir_wins/dir_total = notre précision directionnelle live —
    // c'est CE chiffre qui dira si Tokyo a un vrai edge (avant de miser gros).
    let mut dir_call: Option<Side> = None;
    let mut dir_wins: u32 = 0;
    let mut dir_total: u32 = 0;

    // v8 MAKER : bids restants + stats de fenêtre + disjoncteur de séries perdantes.
    let mut rest_up: Option<(f64, f64)> = None; // (prix, taille)
    let mut rest_dn: Option<(f64, f64)> = None;
    #[allow(unused_assignments, unused_mut)]
    let mut live_open_ct: Option<u32> = None; // live : nb réel d'ordres ouverts (échelle)
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
                            for slot in std::mem::take(&mut lrest) {
                                if let (Some(f), _) = lv.harvest_and_cancel(&slot.r, slot.is_up).await {
                                    let side = if f.is_up { Side::Up } else { Side::Down };
                                    paper.apply_live_fill(side.as_str(), f.price, f.size, "maker");
                                    sc.on_fill(side, f.price, f.size, now_s_roll);
                                    lv.note_fill_cash(f.price, f.size);
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
                        // PRÉCISION DIRECTIONNELLE : la conviction forte de Tokyo
                        // pour cette fenêtre a-t-elle visé le bon gagnant ?
                        if let Some(w) = dir_call.take() {
                            dir_total += 1;
                            let correct = (w == Side::Up) == up_won;
                            if correct {
                                dir_wins += 1;
                            }
                            tracing::info!(
                                call = w.as_str(), gagnant = if up_won { "up" } else { "down" },
                                correct, precision = format!("{dir_wins}/{dir_total}"),
                                "précision directionnelle Tokyo"
                            );
                        }
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
                    // SKEW : état remis à zéro — l'historique PM de l'ANCIEN marché
                    // comparé au mid du nouveau fabrique un momentum fantôme
                    // (incident 12 juil. 16:50:01 : pm_mom=-0.49 au rollover →
                    // 36 Down achetés sur du vide → sortie éclair -2,16 $).
                    pm_hist.clear();
                    skew_armed = None;
                    bet_side = None;
                    skew_cool_until = 0;
                    last_skew_logged = None;
                    pm_lean_sign = 0;
                    pm_lean_since_ms = 0;
                    accum_fak_pending = false;
                    accum_fak_inflight = (0.0, 0);
                    rest_up = None;
                    rest_dn = None;
                    win_rebate = 0.0;
                    win_merged = 0.0;
                    win_deployed = 0.0;
                    win_merge_cost_sum = 0.0;
                    win_merge_n = 0;
                    win_taker_fees = 0.0;
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
                        lrest.clear();
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
        let (spot, sigma, drift_ps, obi, ofi, sig_age_ms) = if cfg.use_udp_transport {
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
                if let Some(lv) = live.as_mut() {
                    // JAMAIS de cancel aveugle : un fill arrivé entre-temps serait
                    // perdu (famille des incidents des 7-8 juil.). La récolte lit
                    // size_matched d'abord ; en échec, l'ordre part en AUDIT.
                    for slot in std::mem::take(&mut lrest) {
                        if let (Some(f), _) = lv.harvest_and_cancel(&slot.r, slot.is_up).await {
                            let side = if f.is_up { Side::Up } else { Side::Down };
                            paper.apply_live_fill(side.as_str(), f.price, f.size, "maker");
                            sc.on_fill(side, f.price, f.size, chrono::Utc::now().timestamp());
                            lv.note_fill_cash(f.price, f.size);
                        }
                    }
                    lv.cancel_all().await;
                }
                tracing::warn!(age_ms, "signal Tokyo PÉRIMÉ — quotes retirées");
                continue;
            }
            (t.spot, t.sigma, t.drift, t.obi, t.ofi, age_ms)
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
            (spot, *sigma_rx.borrow(), drift_eng.per_sec(), obi, ofi_eng.value_norm(), tick_age_ms)
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

        // ═══ Signal Tokyo au service de l'INVENTAIRE ═══
        let mut desired = desired;
        // PULL DÉFENSIF de l'ouverture EN DANGER (celle que le mouvement va
        // traverser → sélection adverse : on se ferait remplir le PERDANT). Deux
        // déclencheurs :
        //  · KILL radar (OBI/vélocité) → pull, et si la direction est illisible,
        //    on retire les DEUX ouvertures (prudence maximale) ;
        //  · MICRO-PULL drift (seuil calibré `sc_urgency_drift`, échelle
        //    par-seconde) → même hors KILL, on retire la jambe menacée. C'est LA
        //    défense contre le résidu perdant : ne pas l'acquérir (8 juil. : 3,3 Up
        //    nus, invendables sous le minimum 5 parts — mieux vaut ne jamais les
        //    prendre). Les complétions survivent TOUJOURS (le côté qui crashe est
        //    précisément celui qu'un déficit veut acheter moins cher).
        use crate::engines::spread_capture::endangered_side;
        // Deux horloges : le DRIFT (EMA halflife 25 s) confirme un mouvement
        // installé ; l'OFI (flux d'ordres, fenêtre 5 s — « le plus prédictif du
        // prochain tick ») l'ANTICIPE. Sur un marché aussi rapide, le drift réagit
        // trop tard : le temps qu'il quitte « neutre », on s'est déjà fait remplir
        // le perdant. On pull donc sur le RAPIDE (OFI) autant que sur le lent
        // (drift) — 9 juil. : OBI/OFI « ACHAT fort » pendant que le drift affichait
        // « neutre ». Signal positif (pression acheteuse) → le prix monte → c'est
        // la jambe DOWN qui va crasher → on retire son ouverture.
        let drift_danger = endangered_side(drift_ps, cfg.sc_urgency_drift);
        let ofi_danger = endangered_side(ofi, cfg.sc_ofi_pull);
        let danger = match (drift_danger, ofi_danger) {
            (Some(a), Some(b)) if a == b => Some(a), // les deux d'accord
            (Some(a), None) => Some(a),              // drift seul (mouvement installé)
            (None, Some(b)) => Some(b),              // OFI seul (anticipation — le cas clé)
            _ => None,                               // rien, ou contradiction = bruit
        };
        if paused {
            match danger {
                Some(d) => desired.retain(|b| b.completion || b.side != d),
                None => desired.retain(|b| b.completion), // KILL + illisible → les 2 sautent
            }
        } else if let Some(d) = danger {
            desired.retain(|b| b.completion || b.side != d);
        }

        // ═══ MODE SKEW — MM incliné (validé utilisateur, test LIVE direct) ═══
        // Le symétrique gagne quand la fenêtre CROISE ; elle perd gros quand la
        // fenêtre meurt d'un côté sans croiser. Le skew détecte ce cas et se
        // blinde du côté fort : plus de taille sur le gagnant probable, zéro
        // ouverture côté faible, complétion PATIENTE (seulement quand le perdant
        // est bradé → paire grasse), et SORTIE ÉCLAIR en FAK si retournement.
        // Deux capteurs : Tokyo (drift+OFI alignés — les moves rapides) OU
        // momentum du carnet PM (les glissements lents que Binance ne voit pas).
        pm_hist.push_back((now_ms_books as i64, up_mid));
        while pm_hist.front().is_some_and(|(t, _)| now_ms_books as i64 - t > 30_000) {
            pm_hist.pop_front();
        }
        let look_ms = cfg.sc_pm_mom_look_s * 1000;
        // Base du momentum : l'échantillon le plus récent d'âge ≥ look_s quand
        // l'historique est plein ; SINON (début de fenêtre) le plus VIEUX
        // échantillon du même marché dès qu'il a ≥ 5 s. Une fenêtre BTC 5min
        // peut traverser 10→90¢ en 2-3 s : être aveugle 20 s raterait le move ;
        // le seuil 0.06 sur 5 s est de facto PLUS strict par seconde, donc pas
        // de sur-déclenchement au bruit d'ouverture. (pm_hist est vidé au
        // rollover — jamais de comparaison inter-marchés.)
        let pm_base = pm_hist
            .iter()
            .filter(|(t, _)| now_ms_books as i64 - t >= look_ms)
            .next_back()
            .or_else(|| {
                pm_hist
                    .front()
                    .filter(|(t, _)| now_ms_books as i64 - t >= 5_000)
            })
            .map(|(_, p)| *p);
        let pm_mom = pm_base.map(|b| up_mid - b).unwrap_or(0.0);
        // MARCHÉ TRANCHÉ (incident 23:47, 12 juil.) : quand un côté cote sous
        // sc_skew_complete_below, le mid du mourant vibre de ±10¢ sur un carnet
        // vide — TOUT signal pm est du bruit (armement, pull, urgence, exit).
        // On le met à zéro : il ne reste que la complétion patiente, la chaîne
        // de fin de fenêtre et le flatten. Tue aussi le spam « PULL PM ».
        let pm_mom = if bb_up > cfg.sc_skew_complete_below && bb_dn > cfg.sc_skew_complete_below
        {
            pm_mom
        } else {
            0.0
        };
        // Persistance du lean : signe du demi-seuil suivi en continu ; toute
        // inversion (ou retour au neutre) remet le chrono à zéro.
        let lean_sign: i8 = if pm_mom >= cfg.sc_pm_mom * 0.5 {
            1
        } else if pm_mom <= -cfg.sc_pm_mom * 0.5 {
            -1
        } else {
            0
        };
        if lean_sign != pm_lean_sign {
            pm_lean_sign = lean_sign;
            pm_lean_since_ms = now_ms_books as i64;
        }
        let pm_persisted = lean_sign != 0
            && now_ms_books as i64 - pm_lean_since_ms >= cfg.sc_pm_persist_s * 1000;
        let pm_side = if pm_mom >= cfg.sc_pm_mom && pm_persisted {
            Some(Side::Up)
        } else if pm_mom <= -cfg.sc_pm_mom && pm_persisted {
            Some(Side::Down)
        } else {
            None
        };
        let tokyo_winner = match (
            endangered_side(drift_ps, cfg.sc_taker_drift),
            endangered_side(ofi, cfg.sc_ofi_pull),
        ) {
            (Some(a), Some(b)) if a == b => Some(a.opposite()), // même perdant → gagnant
            _ => None,
        };
        if let Some(w) = tokyo_winner {
            dir_call = Some(w); // compteur de précision (Tokyo seul, sémantique inchangée)
        }
        // Armement avec HYSTÉRÉSIS : on arme au seuil plein, on ne désarme que
        // quand le signal retombe sous la moitié du seuil (anti flip-flop), et
        // tout désarmement impose un cooldown. À l'armement, gate de prix : un
        // gagnant au-dessus de SC_OPEN_MAX_PRICE est injouable (accumulation
        // veto de toute façon) → on n'arme pas à 0.91-0.95 pour rien.
        skew_armed = match skew_armed {
            Some(w) if cfg.sc_skew && !paused && !sleeping => {
                let pm_keep = match w {
                    Side::Up => pm_mom >= cfg.sc_pm_mom * 0.5,
                    Side::Down => pm_mom <= -cfg.sc_pm_mom * 0.5,
                };
                let tokyo_keep = match (
                    endangered_side(drift_ps, cfg.sc_taker_drift * 0.5),
                    endangered_side(ofi, cfg.sc_ofi_pull * 0.5),
                ) {
                    (Some(a), Some(b)) if a == b => a.opposite() == w,
                    _ => false,
                };
                if pm_keep || tokyo_keep {
                    Some(w)
                } else {
                    // signal éteint → désarmement + cooldown (sans écraser un
                    // cooldown plus long posé par la sortie éclair)
                    skew_cool_until = skew_cool_until.max(now_s + 15);
                    None
                }
            }
            Some(_) => {
                skew_cool_until = skew_cool_until.max(now_s + 15);
                None
            }
            None if cfg.sc_skew && !paused && !sleeping && now_s >= skew_cool_until => {
                // PLANCHER PAR SOURCE (doctrine utilisateur, 13 juil.) :
                //  · pm-momentum : gagnant ≥ sc_directional_min (0.40) — le
                //    carnet PM se fait piéger par les rebonds de token mort
                //    (incident 23:47 : Up 0.04→0.14 → 12 achetés @0.13,
                //    revendus @0.01). Le favori, jamais le couteau.
                //  · Tokyo (drift + OFI pleins) : droit d'armer SOUS 0.40 —
                //    le SEUL achat de perdant autorisé (« Tokyo crie qu'il
                //    va exploser »).
                let gate = |w: &Side, floor: f64| {
                    let bb_w = if *w == Side::Up { bb_up } else { bb_dn };
                    bb_w >= floor && bb_w <= cfg.sc_open_max_price
                };
                tokyo_winner
                    .filter(|w| gate(w, 0.01))
                    .or_else(|| pm_side.filter(|w| gate(w, cfg.sc_directional_min)))
            }
            None => None,
        };
        let skew_winner: Option<Side> = skew_armed;
        if skew_winner != last_skew_logged {
            if let Some(w) = skew_winner {
                let px_w = if w == Side::Up { up_mid } else { down_mid };
                tracing::info!(
                    side = w.as_str(),
                    source = if tokyo_winner.is_some() { "tokyo" } else { "pm-momentum" },
                    px_gagnant = format!("{px_w:.2}"),
                    pm_mom = format!("{pm_mom:+.3}"),
                    drift = format!("{drift_ps:+.5}"),
                    "SKEW armé — MM incliné côté fort"
                );
            } else if last_skew_logged.is_some() {
                tracing::info!("SKEW désarmé — retour symétrique");
            }
            // FAK d'accumulation : chaque armement autorise UN tir ; le
            // désarmement annule le tir en attente.
            accum_fak_pending = skew_winner.is_some();
            last_skew_logged = skew_winner;
        }
        // Pari en cours ? (surplus du gagnant ≤ net_cap) → on ne le complète pas
        // TANT QUE le perdant n'est pas bradé (la complétion sous
        // sc_skew_complete_below verrouille la paire grasse, sinon patience).
        // COLLANT (fenêtre 23:45, 12 juil.) : la patience est portée par
        // l'INVENTAIRE (bet_side), pas par l'armement qui respire — sinon le
        // moindre désarmement complète au complément (12 Down @0.70 accumulés,
        // puis 12 Up @0.28 achetés 10 s après → paire 0.98, pari gaspillé).
        // Le pari vit jusqu'à : perdant ≤ 0.20 (paire grasse) OU retournement
        // (sortie éclair) OU résolution (le gagnant paie 1 $).
        let net_cap = cfg.sc_trend_net_cap.max(cfg.sc_dir_tilt).max(1.0);
        let held_side = bet_side.or(skew_winner);
        let hold_dir_bet = held_side.is_some_and(|w| {
            let surplus = if w == Side::Up { sc.imbalance() } else { -sc.imbalance() };
            surplus > 1e-9 && surplus <= net_cap + 1e-9
        });
        if hold_dir_bet {
            let w = held_side.unwrap();
            let loser = w.opposite();
            let loser_bb = if loser == Side::Up { bb_up } else { bb_dn };
            if loser_bb > cfg.sc_skew_complete_below {
                desired.retain(|b| !(b.completion && b.side == loser));
            }
        }
        // Côté faible : AUCUNE ouverture pendant le skew (le blindage est
        // asymétrique par définition — c'est un pari assumé et borné).
        if let Some(w) = skew_winner {
            desired.retain(|b| b.completion || b.side == w);
        }
        // PULL PM au DEMI-SEUIL (fenêtre 17:55, 12 juil.) : le skew respirait
        // autour du seuil (armé +0.08 → désarmé → réarmé +0.16) et CHAQUE trou
        // rouvrait les ouvertures des deux côtés — les 6 Down nus @0.36 sont
        // partis dans un de ces trous. Gradation : carnet qui penche (≥ ½ seuil)
        // → plus d'ouverture du côté mourant ; seuil plein → skew complet.
        // PERSISTANT (refonte 13 juil.) : sans persistance, le pull musellait le
        // symétrique sur les fenêtres whipsaw (le lean alternait de camp toutes
        // les 10-20 s → presque aucun fill d'ouverture de toute la fenêtre).
        let pm_pull: Option<Side> = if !pm_persisted {
            None
        } else if pm_lean_sign > 0 {
            Some(Side::Down)
        } else {
            Some(Side::Up)
        };
        if let Some(d) = pm_pull {
            if last_pm_pull != Some(d) {
                tracing::info!(cote_retire = d.as_str(), pm_mom = format!("{pm_mom:+.3}"),
                    "PULL PM — le carnet penche : plus d'ouverture du côté mourant");
            }
            desired.retain(|b| b.completion || b.side != d);
        }
        last_pm_pull = pm_pull;
        // URGENCE DE COMPLÉTION : le sous-jacent part dans le sens qui renchérit
        // notre déficit → le bid monte à ask−tick (agressif mais toujours maker).
        #[cfg(feature = "live")]
        let urgency_blocked = now_s < urgency_block_until;
        #[cfg(not(feature = "live"))]
        let urgency_blocked = false;
        // FIN DE FENÊTRE (t-45 → t-20) : la complétion passe en maker AGRESSIF
        // (ask−tick) AVANT que le FAK taker ne tire à t-20 — 25 s de chance de
        // compléter sans payer la taxe (profil 0xb27b : complétions maker).
        let endgame = (20..=45).contains(&m.time_remaining_sec());
        for b in desired.iter_mut() {
            // URGENCE PM (fenêtre 17:55, 12 juil.) : le grind lent est INVISIBLE
            // pour le drift Binance (0.0000 pendant que Up passait 0.69→0.90) —
            // la complétion Up est restée clouée à 0.63 pendant 3 min. Le carnet
            // PM, lui, le voit (pm_mom jusqu'à +0.16) : quand il penche (≥ demi-
            // seuil) vers le côté qu'on doit acheter, la complétion CHASSE.
            let pm_for = if b.side == Side::Up { pm_mom } else { -pm_mom };
            let pm_urgent = pm_persisted
                && ((b.side == Side::Up && pm_lean_sign > 0)
                    || (b.side == Side::Down && pm_lean_sign < 0));
            if b.completion
                && ((!urgency_blocked
                    && (crate::engines::spread_capture::completion_urgent(b.side, drift_ps, cfg.sc_urgency_drift)
                        || pm_urgent))
                    || endgame)
            {
                let ask = if b.side == Side::Up { ask_up } else { ask_dn };
                if ask > tick_sz {
                    // Plafond de paire RAMPÉ — même confiance que le sauvetage
                    // taker (temps + signal). Incident 12 juil. 17:19 : plafond
                    // fixe 1,02 → bid Up cloué à 0,74 pendant que l'ask filait à
                    // 0,90+ ; à T−20 le taker était déjà hors plafond → 6 Down
                    // morts au flatten. La fenêtre T−45→T−20 doit pouvoir suivre
                    // l'ask (toujours maker, zéro frais) jusqu'au plafond rampé.
                    let avg_excess = sc.avg(match b.side { Side::Up => Side::Down, Side::Down => Side::Up });
                    let ramp_s = cfg.sc_rescue_ramp_s.max(1.0);
                    let time_ramp =
                        ((ramp_s - m.time_remaining_sec() as f64) / ramp_s).clamp(0.0, 1.0);
                    let taker_thr = cfg.sc_taker_drift.max(1e-9);
                    let sig_ramp =
                        ((drift_ps.abs() - taker_thr) / (2.0 * taker_thr)).clamp(0.0, 1.0);
                    // Confiance PM : un momentum du carnet nettement au-dessus du
                    // seuil = conviction du marché → payer l'assurance plus cher
                    // est +EV (même logique que la rampe signal du sauvetage).
                    let pm_ramp =
                        ((pm_for - cfg.sc_pm_mom) / (2.0 * cfg.sc_pm_mom)).clamp(0.0, 1.0);
                    let ramped_pair = cfg.sc_completion_max_pair
                        + time_ramp.max(sig_ramp).max(pm_ramp)
                            * (cfg.sc_rescue_max_pair - cfg.sc_completion_max_pair);
                    let pair_room = (ramped_pair - avg_excess).max(0.0);
                    let aggressive = (((ask - tick_sz).max(b.price)) / tick_sz).floor() * tick_sz;
                    let capped = ((aggressive.min(cfg.sc_completion_max_price).min(pair_room)) / tick_sz)
                        .floor() * tick_sz;
                    if capped > b.price + 1e-9 {
                        // Distinguer les deux déclencheurs : le drift=+0.0000 des
                        // logs du 8 juil. venait de la fin de fenêtre, PAS de Tokyo.
                        let side_ix = if b.side == Side::Up { 0 } else { 1 };
                        if (capped - last_aggr_px[side_ix]).abs() > tick_sz / 2.0 {
                            last_aggr_px[side_ix] = capped;
                            let cause = if endgame {
                                "fin de fenêtre"
                            } else if crate::engines::spread_capture::completion_urgent(
                                b.side, drift_ps, cfg.sc_urgency_drift,
                            ) {
                                "drift Tokyo"
                            } else {
                                "pm momentum"
                            };
                            tracing::info!(side = ?b.side, from = b.price, to = capped,
                                cap_paire = format!("{ramped_pair:.3}"),
                                drift = format!("{drift_ps:+.4}"), cause, "complétion agressive (maker)");
                        }
                        b.price = capped;
                    }
                }
            }
        }

        // ═══ CHEMIN LIVE : reconcile GTC réels + fills réels ═══
        #[cfg(feature = "live")]
        if let Some(lv) = live.as_mut() {
            let mut harvested: Vec<crate::live::engine::LiveFill> = Vec::new();
            // ═══ VÉRITÉ CLOB D'ABORD (principe : décider sur le CLOB, jamais
            // sur le miroir/dashboard) : on synchronise nos ordres restants avec
            // le carnet RÉEL (poll /data/orders + balayage des ordres fantômes)
            // AVANT toute décision de pose. Résultat : les slots lrest reflètent
            // la réalité Polymarket → on ne pose jamais atop un ordre en attente
            // qu'on n'aurait pas vu, ni ne reprice un ordre déjà fillé.
            let mut fills = lv.drain_ws(&mut lrest);
            fills.extend(lv.reconcile(&mut lrest, now_ms_books as i64).await);
            // ═══ SORTIE ÉCLAIR : le signal s'est RETOURNÉ contre l'inventaire
            // skew → on vend TOUT DE SUITE en FAK (perdre un peu maintenant
            // plutôt que beaucoup à la résolution), puis cooldown 60 s
            // anti-whipsaw et retour au MM symétrique.
            if let Some(held) = bet_side {
                let surplus =
                    if held == Side::Up { sc.imbalance() } else { -sc.imbalance() };
                let flipped = skew_winner == Some(held.opposite())
                    || (held == Side::Up && pm_mom <= -cfg.sc_pm_mom)
                    || (held == Side::Down && pm_mom >= cfg.sc_pm_mom);
                if surplus <= cfg.sc_dust_tol {
                    bet_side = None; // pari soldé (mergé/flattené) — plus rien à surveiller
                } else if flipped && enabled {
                    let held_bal = if held == Side::Up {
                        paper.state.up_balance
                    } else {
                        paper.state.down_balance
                    };
                    let bid = if held == Side::Up { bb_up } else { bb_dn };
                    let sz = surplus.min(held_bal);
                    if sz > 0.0 && bid > 0.0 {
                        if let Some((sold, avg)) =
                            lv.flatten_sell(held == Side::Up, sz, bid, tick_sz).await
                        {
                            let fee = 0.07 * avg * (1.0 - avg) * sold;
                            lv.cash -= fee;
                            win_taker_fees += fee;
                            paper.try_sell(held.as_str(), avg, sold, "taker");
                            let avg_cost = sc.avg(held);
                            match held {
                                Side::Up => {
                                    sc.shares_up = (sc.shares_up - sold).max(0.0);
                                    sc.cost_up = (sc.cost_up - avg_cost * sold).max(0.0);
                                }
                                Side::Down => {
                                    sc.shares_dn = (sc.shares_dn - sold).max(0.0);
                                    sc.cost_dn = (sc.cost_dn - avg_cost * sold).max(0.0);
                                }
                            }
                            tracing::warn!(
                                side = held.as_str(),
                                sold = format!("{sold:.2}"),
                                px = format!("{avg:.3}"),
                                cout_moyen = format!("{avg_cost:.3}"),
                                pm_mom = format!("{pm_mom:+.3}"),
                                "SORTIE ÉCLAIR — retournement contre l'inventaire skew, vendu au marché"
                            );
                            skew_cool_until = now_s + 60;
                            bet_side = None;
                        }
                    }
                }
            }
            // le pari devient actif dès qu'un surplus skew existe
            if bet_side.is_none() && hold_dir_bet {
                bet_side = skew_winner;
            }
            // ═══ FAK D'ACCUMULATION (refonte 13 juil., « la folie utile ») ═══
            // Le maker passif à bb+tick court derrière un grind et ne se fait
            // remplir qu'une fois sur trois (fenêtre 1783902600 : Down 55→95,
            // zéro accumulation remplie). À l'armement, signal PLEIN par
            // définition → on PAIE l'ask une fois, borné : prix ≤ sc_skew_fak_max,
            // taille ≤ cap net (restants compris), stop T−60. Le maker continue
            // ensuite pour le complément éventuel du cap.
            if accum_fak_pending && cfg.sc_skew_fak && enabled {
                if let Some(w) = skew_winner {
                    let is_up = w == Side::Up;
                    let (ask, ask_sz) =
                        if is_up { (ask_up, ask_up_sz) } else { (ask_dn, ask_dn_sz) };
                    let surplus = if is_up { sc.imbalance() } else { -sc.imbalance() };
                    let resting_open: f64 = lrest
                        .iter()
                        .filter(|s| s.is_up == is_up)
                        .map(|s| (s.r.size - s.r.matched).max(0.0))
                        .sum();
                    let inflight = if now_ms_books as i64 - accum_fak_inflight.1 < 10_000 {
                        accum_fak_inflight.0
                    } else {
                        0.0
                    };
                    let cap_room = (net_cap - surplus - resting_open - inflight).floor();
                    let sz = (cfg.sc_base_clip * cfg.sc_skew_mult)
                        .min(cap_room)
                        .min(ask_sz.max(1.0))
                        .floor();
                    let min_req = m.min_order_size.max(1.0).max((1.0_f64 / ask.max(0.01)).ceil());
                    if ask > 0.0
                        && ask <= cfg.sc_skew_fak_max
                        && m.time_remaining_sec() > cfg.sc_opening_stop_s
                        && sz >= min_req
                        && ask * sz <= lv.cash
                    {
                        accum_fak_pending = false;
                        accum_fak_inflight = (sz, now_ms_books as i64);
                        tracing::info!(
                            side = w.as_str(),
                            ask = format!("{ask:.3}"),
                            size = format!("{sz:.0}"),
                            pm_mom = format!("{pm_mom:+.3}"),
                            drift = format!("{drift_ps:+.5}"),
                            "ACCUMULATION taker — on paie l'ask à l'armement (signal plein)"
                        );
                        lv.place_insurance_fak(is_up, ask, sz).await;
                    } else if ask > cfg.sc_skew_fak_max
                        || m.time_remaining_sec() <= cfg.sc_opening_stop_s
                    {
                        accum_fak_pending = false; // conditions mortes : on n'attend pas
                    }
                }
            }
            // KILL/pause → tout annuler — mais JAMAIS sans récolter d'abord
            // (un cancel aveugle perd le fill arrivé entre-temps).
            if sleeping || !enabled {
                if !lrest.is_empty() {
                    for slot in std::mem::take(&mut lrest) {
                        if let (Some(f), _) = lv.harvest_and_cancel(&slot.r, slot.is_up).await {
                            harvested.push(f);
                        }
                    }
                    lv.cancel_all().await;
                }
            } else {
                let remaining_l_quote = m.time_remaining_sec();
                // Ouvertures : tailles STRICTEMENT égales des deux côtés (le bump
                // 1$ asymétrique a créé 95 orphelines le 7 juil.). Fenêtre de
                // faisabilité : commune ≥ minimums des 2 côtés ET ≤ tailles engine ;
                // vide (marché décidé) → AUCUNE ouverture.
                let min_shares = m.min_order_size.max(1.0);
                // Buffer anti-cross ADAPTATIF au σ, sur les OUVERTURES uniquement :
                // plus le marché bouge, plus on pose profond sous l'ask pour ne pas
                // croiser sur un saut d'ask pendant la latence du POST (9 juil. :
                // 3 crosses/fenêtre). Calme → ask−1 tick (on maximise les fills/
                // rebates) ; volatil → jusqu'à ask−3. Complétions/FAK gardent le
                // droit de croiser (ask−1) : ce sont des voies d'assurance.
                let open_extra = ((sigma - cfg.sc_cross_vol_lo) / cfg.sc_cross_vol_span.max(1e-9))
                    .floor()
                    .clamp(0.0, cfg.sc_cross_max_extra);
                let open_buf = (1.0 + open_extra) * tick_sz;
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
                // ANTI-CROSS dernier moment (8 juil., fill taker 51,7¢) :
                // l'ask du snapshot de début de tick peut être périmé au POST.
                // VETO ATOMIQUE : si l'ask d'UN côté à ouvrir vient de BAISSER
                // (le marché vient vers nous = scénario du cross), AUCUNE
                // ouverture ce tick — « les deux ou aucun » vaut aussi au
                // placement. Les complétions, elles, ne sont JAMAIS différées
                // (organe anti-directionnel) : re-clamp seul.
                let fresh_ask = |token: &str, fallback: f64| -> f64 {
                    let now2 = chrono::Utc::now().timestamp_millis() as u64;
                    pm_ws::fresh_book(&pm_state, token, now2, 5_000)
                        .as_ref()
                        .and_then(best_ask_level)
                        .map(|(p, _)| p)
                        .unwrap_or(fallback)
                };
                let veto_openings = open_common.is_some() && {
                    let fu = fresh_ask(&m.up_token_id, ask_up);
                    let fd = fresh_ask(&m.down_token_id, ask_dn);
                    fu + 1e-9 < ask_up || fd + 1e-9 < ask_dn
                };
                if veto_openings {
                    tracing::info!("veto ouverture : carnet en mouvement (ask en baisse) — on repasse au tick suivant");
                }
                // ═══ GRAND LIVRE DE L'EXPOSITION (incident 12 juil. 17:18) ═══
                // Compte par côté ce qui reste OUVERT au carnet. Deux règles au
                // point de POST :
                //  · une COMPLÉTION ne se pose que si PLUS RIEN d'autre ne reste
                //    ouvert de son côté — son besoin = le déficit TOTAL, tout
                //    résidu du même côté est un doublon en puissance (l'ouverture
                //    Down 0.28 et la complétion 0.27 ont TOUTES DEUX été remplies
                //    → 12 Down pour un besoin de 6, 6 orphelins morts à 0) ;
                //  · un cancel non confirmé GÈLE son côté pour ce tick — l'ordre
                //    est peut-être encore vivant, poster par-dessus = doublon.
                let mut open_side: [f64; 2] = [0.0; 2];
                for s in lrest.iter() {
                    open_side[if s.is_up { 0 } else { 1 }] +=
                        (s.r.size - s.r.matched).max(0.0);
                }
                let mut side_frozen: [bool; 2] = [false, false];
                for (side, ask) in [(Side::Up, ask_up), (Side::Down, ask_dn)] {
                    let want = desired.iter().find(|b| b.side == side);
                    let is_comp = want.map(|b| b.completion).unwrap_or(false);
                    // ANTI-CROSS : on clampe sous l'ask (préserve le maker). Les
                    // OUVERTURES prennent le buffer adaptatif (ask−open_buf) ; les
                    // COMPLÉTIONS restent à ask−1 tick (droit de croiser assumé).
                    let buf = if is_comp { tick_sz } else { open_buf };
                    let want_px = want.map(|b| {
                        let cap = if ask > 0.0 { ask - buf } else { b.price };
                        ((b.price.min(cap)) / tick_sz).floor() * tick_sz
                    });
                    // Valeurs RAW Polymarket : taille ≥ min_order_size du marché,
                    // et jamais plus que le cash réel restant.
                    //  · COMPLÉTION : jamais plus que le déficit (sous les minimums
                    //    → résidu accepté — désormais FLATTEN en fin de fenêtre).
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
                            } else if veto_openings {
                                None
                            } else {
                                open_common
                            }
                        }
                        _ => None,
                    };
                    let is_up = side == Side::Up;
                    let side_ix = if is_up { 0 } else { 1 };
                    // ═══ SKEW : quote d'ACCUMULATION du côté fort ═══
                    // Indépendante de l'atomicité des ouvertures (le blindage est
                    // asymétrique par définition). Taille = clip × mult, bornée
                    // par le net_cap restant. Maker, post-only, échelle normale.
                    let mut want_px = want_px;
                    let mut want_sz = want_sz;
                    let mut is_comp = is_comp;
                    let mut skew_acc = false;
                    if skew_winner == Some(side) && !is_comp {
                        let surplus = if is_up { sc.imbalance() } else { -sc.imbalance() };
                        // Le cap compte AUSSI ce qui est déjà posé au carnet côté
                        // gagnant : sans ça, 2 niveaux × 12 + un repost = 36 parts
                        // remplies en 3 s pour un cap de 12 (incident 12 juil.
                        // 16:50:09, imb=-36).
                        let resting_open: f64 = lrest
                            .iter()
                            .filter(|s| s.is_up == is_up)
                            .map(|s| (s.r.size - s.r.matched).max(0.0))
                            .sum();
                        let inflight = if now_ms_books as i64 - accum_fak_inflight.1 < 10_000 {
                            accum_fak_inflight.0
                        } else {
                            0.0
                        };
                        let cap_room = (net_cap - surplus - resting_open - inflight).floor();
                        let bb_side = if is_up { bb_up } else { bb_dn };
                        if remaining_l_quote > cfg.sc_opening_stop_s
                            && cap_room >= min_shares
                            && bb_side > 0.0
                            && bb_side <= cfg.sc_open_max_price
                        {
                            let cap = if ask > 0.0 { ask - open_buf } else { bb_side + tick_sz };
                            let px = (((bb_side + tick_sz).min(cap)) / tick_sz).floor() * tick_sz;
                            let sz = (cfg.sc_base_clip * cfg.sc_skew_mult).min(cap_room).floor();
                            let min_req = min_shares.max((1.0_f64 / px.max(0.01)).ceil());
                            if px >= 0.01 && sz >= min_req && px * sz <= lv.cash {
                                want_px = Some(px);
                                want_sz = Some(sz);
                                is_comp = false;
                                skew_acc = true;
                            }
                        } else {
                            want_sz = None; // cap atteint / fin de fenêtre : plus d'accumulation
                        }
                    }
                    // ÉCHELLE : complétion = 1 niveau (taille = déficit exact) ;
                    // ouverture = sc_ladder_levels niveaux espacés de
                    // sc_ladder_step_ticks — présence continue au carnet, capte
                    // les dips profonds à meilleur prix (profil vrai MM).
                    // Accumulation skew = 1 SEUL niveau : chaque niveau supplémentaire
                    // est une exposition nette additionnelle qui échappe au cap.
                    let n_levels: u8 = if is_comp || skew_acc {
                        1
                    } else {
                        cfg.sc_ladder_levels.clamp(1, 4) as u8
                    };
                    for lvl in 0..4u8 {
                        // cible de CE niveau (None = rien à poser → retrait)
                        let target: Option<(f64, f64)> = match (want_sz, want_px) {
                            (Some(sz), Some(px0)) if lvl < n_levels => {
                                let px = ((px0 - lvl as f64 * cfg.sc_ladder_step_ticks * tick_sz)
                                    / tick_sz)
                                    .floor()
                                    * tick_sz;
                                let min_req = min_shares.max((1.0_f64 / px.max(0.01)).ceil());
                                if px >= 0.01 && sz + 1e-9 >= min_req && px * sz <= lv.cash {
                                    Some((px, sz))
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        };
                        let slot_pos = lrest.iter().position(|s| s.is_up == is_up && s.level == lvl);
                        match (target, slot_pos) {
                            (Some((px, sz)), cur) => {
                                // ANTI-CHURN (7 juil.) : reprice seulement si l'écart
                                // dépasse 2 ticks ET ≥4 s depuis le dernier placement
                                // de ce (côté, niveau).
                                let cooled =
                                    now_ms_books as i64 - last_place_ms[side_ix][lvl as usize] >= 4_000;
                                let reprice = match cur {
                                    Some(i) => {
                                        (px - lrest[i].r.price).abs() > 2.0 * tick_sz + 1e-9 && cooled
                                    }
                                    None => cooled,
                                };
                                if reprice {
                                    let mut old_was_filled = false;
                                    if let Some(i) = cur {
                                        let slot = lrest.remove(i);
                                        let (f, safe) = lv.harvest_and_cancel(&slot.r, is_up).await;
                                        if let Some(f) = f {
                                            harvested.push(f);
                                            old_was_filled = true;
                                        }
                                        // Cancel non confirmé = fill en vol probable
                                        // (course 02:02 : les DEUX ordres exécutés).
                                        // On ne pose PAS le remplacement ce tick.
                                        if safe {
                                            open_side[side_ix] -=
                                                (slot.r.size - slot.r.matched).max(0.0);
                                        } else {
                                            side_frozen[side_ix] = true;
                                            old_was_filled = true;
                                        }
                                    }
                                    if old_was_filled {
                                        // L'ordre à repricer était DÉJÀ fillé :
                                        // l'inventaire vient de changer — rien posé
                                        // ce tick (19:25 le 8 juil. : achat DOUBLE).
                                        continue;
                                    }
                                    // GARDE-FOU EXPOSITION : côté gelé → rien ce
                                    // tick ; complétion → exclusivité totale (rien
                                    // d'autre ne doit rester ouvert de ce côté).
                                    if side_frozen[side_ix]
                                        || (is_comp && open_side[side_ix] > 0.01)
                                    {
                                        tracing::warn!(side = ?side,
                                            open = format!("{:.1}", open_side[side_ix]),
                                            gele = side_frozen[side_ix],
                                            "garde-fou exposition : POST refusé (résidu du même côté au carnet)");
                                        continue;
                                    }
                                    // RE-CLAMP au dernier moment : ask le plus frais
                                    // juste avant le POST.
                                    let tok = if is_up { &m.up_token_id } else { &m.down_token_id };
                                    let fa = fresh_ask(tok, ask);
                                    let px = if fa > buf {
                                        px.min(((fa - buf) / tick_sz).floor() * tick_sz)
                                    } else {
                                        px
                                    };
                                    if px >= 0.01 {
                                        // OUVERTURES en POST-ONLY : croiser = rejet
                                        // propre du CLOB (zéro taxe accidentelle).
                                        if let Some(r) = lv.place_bid(is_up, px, sz, !is_comp).await {
                                            open_side[side_ix] += (r.size - r.matched).max(0.0);
                                            lrest.push(LiveSlot { r, is_up, level: lvl });
                                        }
                                        last_place_ms[side_ix][lvl as usize] = now_ms_books as i64;
                                    }
                                }
                            }
                            (None, Some(i)) => {
                                let slot = lrest.remove(i);
                                let (f, safe) = lv.harvest_and_cancel(&slot.r, is_up).await;
                                if let Some(f) = f {
                                    harvested.push(f);
                                }
                                if safe {
                                    open_side[side_ix] -=
                                        (slot.r.size - slot.r.matched).max(0.0);
                                } else {
                                    side_frozen[side_ix] = true;
                                }
                            }
                            (None, None) => {}
                        }
                    }
                }
            }
            // Fills : ceux du CLOB drainés en tête de bloc (avant la pose) +
            // ceux récoltés pendant la pose (reprice/cancel de CE tick).
            fills.append(&mut harvested);
            for f in fills {
                last_fill_wall_ms = now_ms_books as i64;
                lv.note_fill_cash(f.price, f.size); // cash réel décrémenté sans attendre le CLOB
                if !f.maker {
                    // Taxe taker : décomptée du cash réel tout de suite (le sync
                    // CLOB la confirmera) + visible au dashboard. Chaque taker
                    // est une anomalie à expliquer (cross ou FAK d'assurance).
                    let fee = 0.07 * f.price * (1.0 - f.price) * f.size;
                    lv.cash -= fee;
                    win_taker_fees += fee;
                    tracing::warn!(
                        px = format!("{:.3}", f.price),
                        size = format!("{:.1}", f.size),
                        fee = format!("{:.3}", fee),
                        "fill TAKER — taxe payée (cross d'ouverture ou FAK)"
                    );
                }
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
            // Deux déclencheurs pour PAYER le marché (taker) et compléter :
            //  · FIN DE FENÊTRE (t-20 → t-10) : dernier recours, plus le temps
            //    d'espérer un fill maker ;
            //  · DRIFT FORT (seuil `sc_taker_drift`, ~2× le seuil de pull) : le
            //    sous-jacent renchérit franchement notre déficit → on complète
            //    MAINTENANT au marché plutôt que de laisser la jambe partir nue
            //    (« compenser la perte » demandé le 9 juil.). Borné au plafond de
            //    paire : au-delà, résidu accepté (perte bornée).
            let deficit_side = if sc.imbalance() > 0.0 { Side::Down } else { Side::Up };
            let taker_drift_urgent = crate::engines::spread_capture::completion_urgent(
                deficit_side, drift_ps, cfg.sc_taker_drift,
            );
            if enabled
                && !hold_dir_bet // pari directionnel en cours → on le laisse courir, pas de complétion
                && sc.imbalance().abs() >= 5.0
                && ((10..=20).contains(&remaining_l) || (taker_drift_urgent && remaining_l > 20))
                && now_ms_books as i64 - last_insurance_ms >= 3_000
            {
                let (is_up, ask, ask_sz) = if sc.imbalance() > 0.0 {
                    (false, ask_dn, ask_dn_sz)
                } else {
                    (true, ask_up, ask_up_sz)
                };
                let avg_excess_ins = sc.avg(if is_up { Side::Down } else { Side::Up });
                // PLAFOND DE PAIRE DU SAUVETAGE — piloté par la CONFIANCE.
                // Payer `a` pour la jambe gagnante est +EV ssi P(gagne) > a. La
                // confiance vient de DEUX sources, on prend la plus forte :
                //  · TEMPS : près de la résolution, le prix du marché EST P →
                //    rampe affine `clamp((RAMP_S − t)/RAMP_S, 0, 1)` (continue).
                //  · SIGNAL : un drift qui dépasse franchement le seuil taker =
                //    forte conviction de direction, MÊME loin de la fin (15:28 le
                //    9 juil. : drift −1e-4 = 4× le seuil à 70 s → le sauvetage
                //    aurait dû tirer, le plafond purement temporel l'a bloqué à 1¢).
                //    `clamp((|drift| − taker_thr)/(2·taker_thr), 0, 1)`.
                //   cap = base + max(conf_temps, conf_signal) · (rescue_max − base)
                let ramp_s = cfg.sc_rescue_ramp_s.max(1.0);
                let time_ramp = ((ramp_s - remaining_l as f64) / ramp_s).clamp(0.0, 1.0);
                let taker_thr = cfg.sc_taker_drift.max(1e-9);
                let sig_ramp = ((drift_ps.abs() - taker_thr) / (2.0 * taker_thr)).clamp(0.0, 1.0);
                // Confiance PM : même composante que la complétion maker — un
                // carnet qui pousse fort vers le gagnant justifie de payer plus.
                let pm_for_ins = if is_up { pm_mom } else { -pm_mom };
                let pm_ramp_ins =
                    ((pm_for_ins - cfg.sc_pm_mom) / (2.0 * cfg.sc_pm_mom)).clamp(0.0, 1.0);
                let conf = time_ramp.max(sig_ramp).max(pm_ramp_ins);
                let rescue_pair = cfg.sc_completion_max_pair
                    + conf * (cfg.sc_rescue_max_pair - cfg.sc_completion_max_pair);
                let pair_room_ins = rescue_pair - avg_excess_ins;
                if ask > 0.0 && ask <= 0.99 && ask <= pair_room_ins + 1e-9 {
                    // PURGE avant le FAK : tout ordre maker encore ouvert du même
                    // côté (la complétion agressive, typiquement) doit être annulé
                    // AVANT de payer le marché — sinon FAK + maker peuvent se
                    // remplir tous les deux = sur-achat du même besoin. Un cancel
                    // non confirmé (fill en vol) reporte le FAK au tick suivant.
                    let mut purge_unsafe = false;
                    let mut i = 0;
                    while i < lrest.len() {
                        if lrest[i].is_up == is_up {
                            let slot = lrest.remove(i);
                            let (f, safe) = lv.harvest_and_cancel(&slot.r, is_up).await;
                            if let Some(f) = f {
                                // Fill récolté à la purge : la boucle de compta des
                                // fills a DÉJÀ tourné ce tick → on comptabilise ici
                                // (mêmes écritures), le déficit se recalcule dessous.
                                last_fill_wall_ms = now_ms_books as i64;
                                lv.note_fill_cash(f.price, f.size);
                                let fside = if f.is_up { Side::Up } else { Side::Down };
                                paper.apply_live_fill(fside.as_str(), f.price, f.size, "maker");
                                sc.on_fill(fside, f.price, f.size, now_s);
                                win_fills += 1;
                                win_deployed += f.price * f.size;
                                win_rebate +=
                                    cfg.sc_rebate_rate * 0.07 * f.price * (1.0 - f.price) * f.size;
                                tracing::info!(
                                    side = fside.as_str(),
                                    px = format!("{:.3}", f.price),
                                    size = format!("{:.1}", f.size),
                                    imb = format!("{:.0}", sc.imbalance()),
                                    "[LIVE] fill réel (purge pré-sauvetage)"
                                );
                            }
                            if !safe {
                                purge_unsafe = true;
                            }
                        } else {
                            i += 1;
                        }
                    }
                    // Assurance : JAMAIS plus que le déficit (le sur-achat forcé
                    // par les minimums relançait la spirale). Sous les minimums →
                    // résidu accepté, perte bornée. Déficit RECALCULÉ après purge.
                    let sz = ((sc.imbalance().abs().min(ask_sz.max(1.0))) * 100.0).floor() / 100.0;
                    let min_req = m.min_order_size.max(1.0).max((1.0_f64 / ask).ceil());
                    if purge_unsafe {
                        tracing::warn!(
                            side = if is_up { "up" } else { "down" },
                            "sauvetage différé : cancel non confirmé du même côté (fill en vol probable)"
                        );
                    } else if sz + 1e-9 >= min_req && ask * sz <= lv.cash {
                        last_insurance_ms = now_ms_books as i64;
                        let cause = if taker_drift_urgent && remaining_l > 20 { "drift fort" } else { "fin de fenêtre" };
                        let pair_now = ask + avg_excess_ins;
                        tracing::info!(
                            side = if is_up { "up" } else { "down" }, ask = format!("{ask:.3}"),
                            size = format!("{sz:.1}"), rem = remaining_l,
                            drift = format!("{drift_ps:+.5}"), cause,
                            pair = format!("{pair_now:.3}"), cap = format!("{rescue_pair:.3}"),
                            "SAUVETAGE taker (paie le marché pour compléter la paire)"
                        );
                        lv.place_insurance_fak(is_up, ask, sz).await;
                    }
                }
            }
            // ═══ FLATTEN DU RÉSIDU — deux déclencheurs :
            //  · POUSSIÈRE (n'importe quand) : un résidu ≤ dust_tol est
            //    INCOMPLÉTABLE par définition (sous le minimum 5 parts) —
            //    attendre n'apporte RIEN et expose au directionnel (10 juil. :
            //    0,99 Up porté 4 min). Vendu IMMÉDIATEMENT, dès 5 s de calme
            //    après le dernier fill (ne pas vendre au milieu d'un fill
            //    partiel en cours, qui ressemble transitoirement à de la
            //    poussière).
            //  · FIN DE FENÊTRE (t-9 → t-3) : tout résidu que l'assurance n'a
            //    pas pu compléter (plafond de paire, minimums).
            // Vente FAK : l'exécution immédiate n'est pas soumise au plancher de
            // 5 parts (prouvé les 9-10 juil.). Sauf pari directionnel (dir_tilt).
            let imb_abs = sc.imbalance().abs();
            let fill_quiet_ms = now_ms_books as i64 - last_fill_wall_ms;
            let dust_case = imb_abs > 0.05
                && imb_abs <= cfg.sc_dust_tol
                && fill_quiet_ms >= 5_000
                && remaining_l > 9;
            let endgame_case = (3..=9).contains(&remaining_l) && imb_abs > 0.4;
            if enabled
                && !hold_dir_bet
                && (dust_case || endgame_case)
                && now_ms_books as i64 - last_insurance_ms >= 3_000
            {
                let excess_up = sc.imbalance() > 0.0;
                let side = if excess_up { Side::Up } else { Side::Down };
                let held = if excess_up { paper.state.up_balance } else { paper.state.down_balance };
                let sz = sc.imbalance().abs().min(held);
                let bid = if excess_up { bb_up } else { bb_dn };
                if sz > 0.0 && bid > 0.0 {
                    last_insurance_ms = now_ms_books as i64;
                    if let Some((sold, avg)) = lv.flatten_sell(excess_up, sz, bid, tick_sz).await {
                        // Comptabilité : vente réelle → miroir + moteur + frais taker.
                        let fee = 0.07 * avg * (1.0 - avg) * sold;
                        lv.cash -= fee;
                        win_taker_fees += fee;
                        paper.try_sell(side.as_str(), avg, sold, "taker");
                        let avg_cost = sc.avg(side);
                        match side {
                            Side::Up => {
                                sc.shares_up = (sc.shares_up - sold).max(0.0);
                                sc.cost_up = (sc.cost_up - avg_cost * sold).max(0.0);
                            }
                            Side::Down => {
                                sc.shares_dn = (sc.shares_dn - sold).max(0.0);
                                sc.cost_dn = (sc.cost_dn - avg_cost * sold).max(0.0);
                            }
                        }
                        let cause = if dust_case { "poussière (incomplétable)" } else { "fin de fenêtre" };
                        tracing::info!(
                            side = side.as_str(), sold = format!("{sold:.2}"),
                            px = format!("{avg:.3}"), recupere = format!("{:.2}", avg * sold),
                            fee = format!("{fee:.3}"), rem = remaining_l, cause,
                            "FLATTEN résidu — vendu au marché (au lieu de mourir nu)"
                        );
                    }
                }
            }
            // MERGE on-chain (mêmes règles que le paper : ≥90 s de fenêtre, blocs
            // ≥ MIN_MERGE_THRESHOLD) — un seul en vol ; à la confirmation, le
            // miroir merge + le moteur recycle son budget + le cash se resynce.
            if let Some(pairs_done) = lv.poll_merge_done() {
                // Qualité d'exécution + cooldown stratégique : blended AU merge.
                if let Some(blended) = sc.pair_cost() {
                    win_merge_cost_sum += blended;
                    win_merge_n += 1;
                    if blended > 1.0 + 1e-9 {
                        urgency_block_until = now_s + 45;
                        tracing::info!(blended = format!("{blended:.3}"),
                            "merge à paire > 1$ — escalade gelée 45 s (le quoting ≤1$ continue)");
                    }
                }
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
            // MERGE = pompe à volume, mais action ON-CHAIN IRRÉVERSIBLE : on ne
            // la déclenche JAMAIS sur le miroir. On lit les positions RÉELLES du
            // CLOB juste avant, et on merge min(real_up, real_down). La chaîne ne
            // montre jamais de tokens non encore réglés → merger min(real) est
            // conservateur par construction (fini les "would revert" sur paires
            // fantômes). On ne réaligne PAS le moteur ici (la chaîne peut laguer
            // un fill tout frais → sous-compte → sur-achat) : ce réalignement
            // reste dans le bloc gardé settle-quiet, juste en dessous.
            if lv.merge_available() {
                if let Some((ru, rd)) = lv.positions_force().await {
                    let pairs = ru.min(rd).floor();
                    if pairs >= cfg.min_merge_threshold {
                        match lv.start_merge(pairs).await {
                            crate::live::engine::MergeStart::WouldRevert => {
                                // « would revert » = les tokens ne sont pas (ou
                                // plus) mergeables on-chain. On ne crédite RIEN :
                                // resync forcé, la vérité on-chain tranche, retry
                                // dans 45 s. (Avec le merge sur min(real), ce cas
                                // devient rare : on ne demande que ce que la
                                // chaîne confirme déjà détenir.)
                                lv.force_cash_resync();
                                tracing::warn!(pairs_tx = pairs,
                                    "merge refusé (would revert) — AUCUN crédit, resync on-chain forcé");
                            }
                            _ => {}
                        }
                    }
                }
            }

            // POSITIONS RÉELLES : le miroir s'aligne sur la vérité on-chain
            // (≤1×/60 s, ou immédiatement après un merge/redeem incertain).
            // Incident du 7 juil. : miroir « équilibré » ≠ réalité déséquilibrée
            // → aucune complétion pendant que la vraie jambe mourait.
            let settle_quiet = now_ms_books as i64 - last_fill_wall_ms >= 15_000;
            if !settle_quiet {
                // fills récents : la chaîne est en retard sur le CLOB — tout
                // alignement maintenant écraserait la vérité fraîche du miroir.
            } else if let Some((ru, rd)) = lv.real_positions(now_ms_books as i64).await {
                let (mu, md) = (paper.state.up_balance, paper.state.down_balance);
                if (ru - mu).abs() > 0.5 || (rd - md).abs() > 0.5 {
                    tracing::warn!(
                        reel_up = ru, reel_dn = rd, miroir_up = mu, miroir_dn = md,
                        "positions désynchronisées — miroir ALIGNÉ sur la réalité"
                    );
                    paper.state.up_balance = ru;
                    paper.state.down_balance = rd;
                    // le moteur suit aussi (imbalance/complétion sur la vérité) —
                    // et les COÛTS sont mis à l'échelle : ajuster les parts sans
                    // les coûts gonfle l'avg (10 juil. 02:01 : 18→12 Down sans
                    // scaling → blended fantôme 1.248 → escalade gelée à tort).
                    let scale_u = if sc.shares_up > 1e-9 { ru / sc.shares_up } else { 0.0 };
                    let scale_d = if sc.shares_dn > 1e-9 { rd / sc.shares_dn } else { 0.0 };
                    sc.cost_up = (sc.cost_up * scale_u.min(1.0)).max(0.0);
                    sc.cost_dn = (sc.cost_dn * scale_d.min(1.0)).max(0.0);
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
                d.order_rtt_ms = lv.last_rtt_ms;
            }
            // Miroir des bids pour le dashboard.
            // Affichage : le MEILLEUR bid (niveau le plus haut) par côté + le
            // compte réel d'ordres ouverts (échelle comprise).
            rest_up = lrest.iter().filter(|s| s.is_up)
                .max_by(|a, b| a.r.price.total_cmp(&b.r.price))
                .map(|s| (s.r.price, s.r.size));
            rest_dn = lrest.iter().filter(|s| !s.is_up)
                .max_by(|a, b| a.r.price.total_cmp(&b.r.price))
                .map(|s| (s.r.price, s.r.size));
            live_open_ct = Some(lrest.len() as u32);
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
            d.signal_age_ms = sig_age_ms;
            d.open_orders = live_open_ct
                .unwrap_or(rest_up.is_some() as u32 + rest_dn.is_some() as u32);
            if d.last_block_reason.starts_with("SIGNAL TOKYO") {
                d.last_block_reason.clear();
            }
            d.rebate_window = win_rebate;
            d.rebate_total = rebate_total + win_rebate;
            d.size_factor = size_factor;
            d.loss_streak = loss_streak;
            d.pair_cost = sc.pair_cost().unwrap_or(0.0);
            d.merge_pair_avg = if win_merge_n > 0 { win_merge_cost_sum / win_merge_n as f64 } else { 0.0 };
            d.skew_side = skew_winner.map(|w| w.as_str().to_string()).unwrap_or_default();
            d.taker_fees_window = win_taker_fees;
            d.dir_wins = dir_wins;
            d.dir_total = dir_total;
            d.merged_window = win_merged;
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
        tracing::debug!(
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

