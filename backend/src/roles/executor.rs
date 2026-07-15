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
use crate::connectors::polymarket::{Market, PolyBook, PolymarketClient};
use crate::dashboard::{PurposeBreakdown, SeriesPoint, Shared, WindowResult};
use crate::engines::spread_capture::{Side, SpreadCaptureConfig, SpreadCaptureEngine};
use crate::engines::{
    drift::DriftEngine, ofi::OfiEngine, pricing, radar::RadarEngine, volatility::VolatilityEngine,
};
use crate::execution::KillState;
use crate::inventory::PaperEngine;
#[cfg(feature = "live")]
use crate::live::engine::{LiveCtx, LiveFill, LiveSlot, OrderIntent};
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

/// Applique immédiatement un fill déjà dédupliqué par `LiveCtx`. Cette fonction
/// est l'unique passage du fill CLOB vers le moteur et le miroir : aucun calcul
/// de quote ne doit s'exécuter avec ces deltas encore en attente.
#[cfg(feature = "live")]
#[allow(clippy::too_many_arguments)]
fn apply_live_fills(
    fills: Vec<LiveFill>,
    live: &mut LiveCtx,
    paper: &mut PaperEngine,
    sc: &mut SpreadCaptureEngine,
    now_s: i64,
    now_ms: i64,
    cfg: &Config,
    last_fill_wall_ms: &mut i64,
    win_taker_fees: &mut f64,
    win_fills: &mut u32,
    win_deployed: &mut f64,
    win_rebate: &mut f64,
    win_imb_max: &mut f64,
    purposes: &mut PurposeBreakdown,
) {
    for fill in fills.into_iter().filter(|f| f.size > 1e-9) {
        live.note_fill_cash(fill.price, fill.size);
        if !fill.maker {
            let fee = 0.07 * fill.price * (1.0 - fill.price) * fill.size;
            live.cash -= fee;
            *win_taker_fees += fee;
            purposes.for_order_intent(fill.intent.as_str()).taker_fees += fee;
            tracing::warn!(
                px = format!("{:.3}", fill.price),
                size = format!("{:.1}", fill.size),
                fee = format!("{fee:.3}"),
                intent = fill.intent.as_str(),
                "fill TAKER — taxe payée"
            );
        }
        let side = if fill.is_up { Side::Up } else { Side::Down };
        let liquidity = if fill.maker { "maker" } else { "taker" };
        paper.apply_live_fill_with_purpose(
            side.as_str(),
            fill.price,
            fill.size,
            liquidity,
            fill.intent.as_str(),
        );
        sc.on_fill(side, fill.price, fill.size, now_s);
        *last_fill_wall_ms = now_ms;
        *win_fills += 1;
        *win_deployed += fill.price * fill.size;
        let purpose = purposes.for_order_intent(fill.intent.as_str());
        purpose.fills += 1;
        purpose.buy_notional += fill.price * fill.size;
        if fill.maker {
            let rebate = cfg.sc_rebate_rate * 0.07 * fill.price * (1.0 - fill.price) * fill.size;
            *win_rebate += rebate;
            purpose.maker_rebate += rebate;
        }
        if sc.imbalance().abs() > win_imb_max.abs() {
            *win_imb_max = sc.imbalance();
        }
        tracing::info!(
            side = side.as_str(),
            px = format!("{:.3}", fill.price),
            size = format!("{:.1}", fill.size),
            liquidity,
            intent = fill.intent.as_str(),
            condition = %fill.condition_id.chars().take(12).collect::<String>(),
            imb = format!("{:.2}", sc.imbalance()),
            "[LIVE] fill réel appliqué avant décision"
        );
    }
}

pub async fn run(
    cfg: Config,
    transport: Arc<dyn SignalTransport>,
    dash: Shared,
) -> anyhow::Result<()> {
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
    let (remote_tx, remote_rx) = watch::channel::<Option<(WireTick, i64)>>(None);
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
                            tracing::warn!(
                                old = last_seq,
                                new = t.seq,
                                "seq radar réinitialisé (redémarrage Tokyo) — flux réaccepté"
                            );
                        }
                        last_seq = t.seq;
                        let _ = remote_tx.send(Some((t, chrono::Utc::now().timestamp_millis())));
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
        Some(LiveCtx::start(creds, cfg.live_armed, cfg.live_audit_max_age_s).await?)
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
    let pm_state: pm_ws::PmWsShared =
        std::sync::Arc::new(std::sync::RwLock::new(pm_ws::PmWsState::default()));
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
        tilt_mult: cfg.sc_skew_mult,
        patient_below: cfg.sc_skew_complete_below,
        open_pair_target: cfg.sc_open_pair_target,
    });
    let mut paper = PaperEngine::load_or_init(
        cfg.start_cash,
        cfg.max_position,
        cfg.min_merge_threshold,
        cfg.safety_mult,
        cfg.state_path.clone(),
        cfg.trades_path.clone(),
    );

    // Persistance de l'historique des fenêtres (le dashboard est en mémoire sinon :
    // chaque redémarrage perdrait la table analytique). JSONL append-only, rechargé
    // au boot — le PnL/cash reste dans paper_state, ceci ne stocke que l'affichage.
    let windows_path =
        std::env::var("WINDOWS_PATH").unwrap_or_else(|_| "paper_windows_v8.jsonl".into());
    {
        let mut loaded: Vec<WindowResult> = Vec::new();
        if let Ok(txt) = std::fs::read_to_string(&windows_path) {
            for line in txt.lines().filter(|l| !l.trim().is_empty()) {
                if let Ok(w) = serde_json::from_str::<WindowResult>(line) {
                    if w.deployed > 0.01 {
                        loaded.push(w); // fenêtres jouées uniquement
                    }
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
    // CHEMIN CHAUD (passe 3) : 100 ms — aligné sur la cadence du feed Binance
    // (depth20@100ms). Chaîne impulsion : détection 300-500 ms + UDP 105 ms +
    // boucle ≤ 100 ms + POST ≈ t+1 s sur une explosion de 5 s.
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    let mut persist_ctr: u32 = 0;

    // Signal directionnel (Radar Tokyo). En mode `combined`, on le calcule localement
    // depuis le feed Binance de l'exécuteur ; en split, ces valeurs viendront du WireSignal.
    let mut drift_eng = DriftEngine::new(cfg.drift_halflife_secs);
    let mut ofi_eng = OfiEngine::new(5000);
    let radar_obi = RadarEngine::new(
        cfg.obi_depth_levels,
        cfg.obi_threshold,
        cfg.velocity_threshold,
    );
    let mut last_realized = paper.state.realized_pnl; // pour le delta par fenêtre

    // GRAND MODÈLE 0xb (13 juil.) : le pari directionnel n'est plus un MODE à
    // états (armé/désarmé, hystérésis, cooldowns, pm-momentum) — c'est un TILT
    // continu issu de Binance, qui incline la cotation. Le carnet PM ne pilote
    // plus rien : un carnet mince se spoofe, le sous-jacent ne se spoofe pas.
    let mut last_tilt_sign: i8 = 0; // log de transition du tilt fort
    // Confirmation FAK : (côté du signal Tokyo lent, depuis quand).
    let mut tokyo_side_since: (Option<Side>, i64) = (None, 0);
    // FAK d'accumulation : throttle (côté, epoch ms) — un tir par rafale Tokyo.
    #[allow(unused_mut, unused_assignments, unused_variables)]
    let mut last_fak_side: Option<Side> = None;
    #[allow(unused_mut, unused_assignments, unused_variables)]
    let mut last_fak_ms: i64 = 0;
    // MESURE DE L'EDGE TOKYO : `dir_call` = dernière conviction directionnelle
    // FORTE (drift+OFI d'accord) de la fenêtre ; à la résolution on la compare
    // au gagnant réel. dir_wins/dir_total = notre précision directionnelle live —
    // c'est CE chiffre qui dira si Tokyo a un vrai edge (avant de miser gros).
    let mut dir_call: Option<Side> = None;
    let mut dir_wins: u32 = 0;
    let mut dir_total: u32 = 0;
    // ═══ LE FLOTTEUR (STRATEGIE.md) : imbalance CIBLE signée, TOUJOURS du
    // côté GAGNANT (doctrine ferme) — Tokyo d'abord, leader du prix ensuite.
    // Dwell anti-churn entre deux changements de cible.
    let mut float_sign: i8 = 0; // signe courant de la cible (+1 = surplus Up voulu)
    let mut last_float_ms: i64 = 0; // dernier changement de cible (dwell)
    // DISJONCTEUR DE FENÊTRE HACHÉE (15 juil., fenêtre 00:00 : collée au
    // strike à 10$ près, le leader PM a flippé 5× en 2 min → on a acheté
    // chaque sommet en « urgence prix », −25$). FENÊTRE GLISSANTE : N
    // retournements du leader dans les sc_chop_window_s dernières secondes →
    // directionnel coupé (cible 0, urgence prix OFF), on sauve les meubles :
    // quoting symétrique, complétions maker, merges et assurance de fin de
    // fenêtre continuent. Les retournements VIEILLISSENT : zigzag apaisé =
    // directionnel réarmé. Tokyo garde son rôle : blinder les creux (contact
    // ×2, FAK signal plein) — pas de voir les 10$.
    let mut prev_leader: i8 = 0;
    let mut leader_flip_ts: Vec<i64> = Vec::new();
    let mut was_chopped: bool = false;

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
    let mut win_purposes = PurposeBreakdown::default();
    let mut rebate_total = 0.0f64;
    let mut loss_streak: u32 = 0;
    let mut size_factor = 1.0f64;
    // Anti flip-flop : le drift doit garder son signe sc_trend_confirm_s avant
    // d'armer le directionnel (le couteau se pariait sur des micro-replis de 2 s).
    let mut trend_sign = true;
    let mut trend_since: i64 = 0;

    loop {
        poll.tick().await;

        let need_resolve = current
            .as_ref()
            .map_or(true, |m| m.time_remaining_sec() <= 0);
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
                        Ok(c) => {
                            close = Some(c);
                            break;
                        }
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
                                if let (Some(f), _) =
                                    lv.harvest_and_cancel(&slot.r, slot.is_up).await
                                {
                                    apply_live_fills(
                                        vec![f],
                                        lv,
                                        &mut paper,
                                        &mut sc,
                                        now_s_roll,
                                        chrono::Utc::now().timestamp_millis(),
                                        &cfg,
                                        &mut last_fill_wall_ms,
                                        &mut win_taker_fees,
                                        &mut win_fills,
                                        &mut win_deployed,
                                        &mut win_rebate,
                                        &mut win_imb_max,
                                        &mut win_purposes,
                                    );
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
                            purposes: win_purposes.clone(),
                            pnl_unattributed: 0.0,
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
                                call = w.as_str(),
                                gagnant = if up_won { "up" } else { "down" },
                                correct,
                                precision = format!("{dir_wins}/{dir_total}"),
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
                            if delta <= 0.0 {
                                loss_streak += 1
                            } else {
                                loss_streak = 0
                            }
                        }
                        size_factor = if loss_streak >= cfg.sc_streak_soft && size_factor > 0.0 {
                            0.0 // pause d'une fenêtre
                        } else {
                            1.0 // retour taille pleine
                        };
                        rebate_total += win_rebate;
                        let final_rec = WindowResult {
                            pnl: delta,
                            // Le PnL de résolution est conservé explicitement
                            // ici tant que les lots FIFO ne permettent pas une
                            // ventilation économiquement exacte par intention.
                            pnl_unattributed: delta,
                            ..rec
                        };
                        // FENÊTRES JOUÉES UNIQUEMENT (13 juil.) : une fenêtre sans
                        // le moindre déploiement (sommeil, pause, aucun fill) n'a
                        // rien à dire — ni au dashboard, ni au journal.
                        if final_rec.deployed > 0.01 {
                            // Persiste la fenêtre (append-only) avant l'affichage.
                            if let Ok(line) = serde_json::to_string(&final_rec) {
                                use std::io::Write;
                                if let Ok(mut fh) = std::fs::OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open(&windows_path)
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
                    }
                    None => tracing::warn!("résolution sautée : ni close kline ni spot disponible"),
                }
            }
            match client.get_current_btc_5m_market().await {
                Ok(Some(m)) => {
                    strike = binance::price_at_window_open(m.window_ts).await.ok();
                    sc.reset_window(); // état blended remis à zéro pour la nouvelle fenêtre
                    last_tilt_sign = 0;
                    tokyo_side_since = (None, 0);
                    float_sign = 0;
                    last_float_ms = 0;
                    prev_leader = 0;
                    leader_flip_ts.clear();
                    was_chopped = false;
                    #[cfg(feature = "live")]
                    {
                        last_fak_side = None;
                        last_fak_ms = 0;
                    }
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
                    win_purposes = PurposeBreakdown::default();
                    tracing::info!(
                        slug = %m.slug, remaining_s = m.time_remaining_sec(),
                        strike = ?strike, size_factor, loss_streak, "=== Nouveau marché BTC 5min ==="
                    );
                    let _ = pm_tokens.send(vec![m.up_token_id.clone(), m.down_token_id.clone()]);
                    #[cfg(feature = "live")]
                    if let Some(lv) = live.as_mut() {
                        lv.on_new_market(&m.condition_id, &m.up_token_id, &m.down_token_id)
                            .await;
                        lrest.clear();
                    }
                    // Budget de fenêtre en % de bankroll (SC_BANKROLL_PCT>0) —
                    // recalculé à CHAQUE rollover ; le recyclage par merge du
                    // moteur (on_merge) réutilise ce budget dans la fenêtre.
                    if cfg.sc_bankroll_pct > 0.0 {
                        let bankroll = {
                            #[cfg(feature = "live")]
                            {
                                live.as_ref()
                                    .map(|lv| lv.cash)
                                    .unwrap_or(paper.state.cash_usdc)
                            }
                            #[cfg(not(feature = "live"))]
                            {
                                paper.state.cash_usdc
                            }
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
                Ok(None) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
                Err(e) => {
                    tracing::error!(error=%e,"résolution marché");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
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
        let (spot, sigma, drift_ps, obi, ofi, impulse, sig_age_ms) = if cfg.use_udp_transport {
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
                            apply_live_fills(
                                vec![f],
                                lv,
                                &mut paper,
                                &mut sc,
                                chrono::Utc::now().timestamp(),
                                chrono::Utc::now().timestamp_millis(),
                                &cfg,
                                &mut last_fill_wall_ms,
                                &mut win_taker_fees,
                                &mut win_fills,
                                &mut win_deployed,
                                &mut win_rebate,
                                &mut win_imb_max,
                                &mut win_purposes,
                            );
                        }
                    }
                    lv.cancel_all().await;
                }
                tracing::warn!(age_ms, "signal Tokyo PÉRIMÉ — quotes retirées");
                continue;
            }
            (t.spot, t.sigma, t.drift, t.obi, t.ofi, t.impulse, age_ms)
        } else {
            let bu = spot_rx.borrow().clone();
            let Some(bu) = bu else { continue };
            let Some(tick) = bu.price_tick() else {
                continue;
            };
            let tick_age_ms = chrono::Utc::now().timestamp_millis() - bu.ts_ms as i64;
            if tick_age_ms > 10_000 {
                rest_up = None;
                rest_dn = None;
                last_spot = None; // ne JAMAIS résoudre avec un spot mort
                tracing::warn!(
                    age_s = tick_age_ms / 1000,
                    "spot Binance PÉRIMÉ — quotes retirées"
                );
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
            (
                spot,
                *sigma_rx.borrow(),
                drift_eng.per_sec(),
                obi,
                ofi_eng.value_norm(),
                0.0, // impulsion : calculée au radar (chemin UDP) uniquement
                tick_age_ms,
            )
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
        let (Some(up_mid), Some(down_mid)) = (up_book.mid(), down_book.mid()) else {
            continue;
        };

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
        #[cfg(feature = "live")]
        let enabled = enabled && !live.as_ref().is_some_and(LiveCtx::is_halted);
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

        // La vérité CLOB est appliquée AVANT de calculer `desired`. Sinon une
        // complétion peut être décidée avec le déficit d'avant le fill WS/poll.
        #[cfg(feature = "live")]
        if let Some(lv) = live.as_mut() {
            let mut pre_fills = lv.drain_ws(&mut lrest);
            pre_fills.extend(lv.reconcile(&mut lrest, now_ms_books as i64).await);
            apply_live_fills(
                pre_fills,
                lv,
                &mut paper,
                &mut sc,
                now_s,
                now_ms_books as i64,
                &cfg,
                &mut last_fill_wall_ms,
                &mut win_taker_fees,
                &mut win_fills,
                &mut win_deployed,
                &mut win_rebate,
                &mut win_imb_max,
                &mut win_purposes,
            );
        }

        // ═══ TILT BINANCE (grand modèle 0xb, 13 juil.) ═══
        // Paramètre CONTINU ∈ [−1,1] : plein à 2× le seuil taker de drift, veto
        // si l'OFI contredit franchement. Il incline la cotation continue
        // (contact/retrait, tailles asymétriques, patience de complétion) dans
        // desired_bids_symmetric — aucun état, aucun cooldown, aucun flip-flop.
        use crate::engines::spread_capture::endangered_side;
        let tilt_drift = (drift_ps / (2.0 * cfg.sc_taker_drift.max(1e-9))).clamp(-1.0, 1.0);
        // CHEMIN CHAUD : l'impulsion (déplacement 500 ms, vue en <1 s) prend la
        // main quand elle est plus forte que le drift (EMA 25 s, vue en ~2 s).
        let tilt_imp = (impulse / (2.0 * cfg.sc_impulse.max(1e-9))).clamp(-1.0, 1.0);
        let tilt_raw = if tilt_imp.abs() > tilt_drift.abs() {
            tilt_imp
        } else {
            tilt_drift
        };
        let tilt = if cfg.sc_ofi_confirm
            && ofi.abs() >= cfg.sc_ofi_min
            && ofi.signum() * drift_ps.signum() < 0.0
        {
            0.0 // OFI contre le drift : bruit, pas de tendance
        } else {
            tilt_raw
        };
        // Tokyo PLEIN (drift + OFI alignés) : compteur de précision + FAK.
        let tokyo_slow = match (
            endangered_side(drift_ps, cfg.sc_taker_drift),
            endangered_side(ofi, cfg.sc_ofi_pull),
        ) {
            (Some(a), Some(b)) if a == b => Some(a.opposite()),
            _ => None,
        };
        // L'IMPULSION seule suffit : une explosion de 5 s ne laisse pas le
        // temps d'attendre l'accord drift+OFI (fenêtre-explosion utilisateur).
        let impulse_winner = endangered_side(impulse, cfg.sc_impulse).map(|s| s.opposite());
        let tokyo_winner = tokyo_slow.or(impulse_winner);
        // CONFIRMATION FAK (13 juil. 18:20 : assuré au creux d'un balancier sur
        // un signal de 6 s → merge 1.06) : le signal LENT doit tenir
        // sc_fak_confirm_s du même côté avant de payer l'ask. L'impulsion —
        // l'explosion vraie — garde son droit de tir immédiat.
        if tokyo_slow != tokyo_side_since.0 {
            tokyo_side_since = (tokyo_slow, now_ms_books as i64);
        }
        let tokyo_confirmed = tokyo_slow.filter(|_| {
            now_ms_books as i64 - tokyo_side_since.1
                >= (cfg.sc_fak_confirm_s * 1000.0) as i64
        });
        #[cfg_attr(not(feature = "live"), allow(unused_variables))]
        let fak_trigger: Option<Side> = impulse_winner.or(tokyo_confirmed);
        if let Some(w) = tokyo_winner {
            dir_call = Some(w);
        }
        let tilt_sign: i8 = if tilt >= 0.5 {
            1
        } else if tilt <= -0.5 {
            -1
        } else {
            0
        };
        if tilt_sign != last_tilt_sign {
            if tilt_sign != 0 {
                tracing::info!(
                    cote = if tilt_sign > 0 { "up" } else { "down" },
                    tilt = format!("{tilt:+.2}"),
                    drift = format!("{drift_ps:+.5}"),
                    ofi = format!("{ofi:+.2}"),
                    "TILT fort — cotation inclinée côté favori"
                );
            } else {
                tracing::info!("TILT neutre — cotation symétrique");
            }
            last_tilt_sign = tilt_sign;
        }

        // ═══ LE FLOTTEUR : TOUJOURS du côté GAGNANT (doctrine ferme) ═══
        // (STRATEGIE.md §1) « On doit avoir plus du côté gagnant que du côté
        // perdant, tout le temps. » Le flotteur suit le gagnant du moment —
        // Tokyo d'abord (il voit le renversement 1-3 s avant le carnet), le
        // leader du prix ensuite — paire chère ou pas. Le mode contrarien de
        // 0xb (ticket pas cher quand ses paires gagnent) N'EST PAS copié :
        // c'est un luxe payé par ses rebates, que nous n'avons pas.
        let remaining_f = m.time_remaining_sec();
        let chopped;
        {
            // Leader du prix : bande morte 48-52 (0xb flippe à 47¢ Up médian).
            let leader: i8 = if bb_up >= 0.52 {
                1
            } else if bb_up > 0.0 && bb_up <= 0.48 {
                -1
            } else {
                0
            };
            // Zigzag en FENÊTRE GLISSANTE : un retournement = le leader change
            // de camp ; seuls ceux des sc_chop_window_s dernières secondes
            // comptent — le calme réarme le directionnel.
            if leader != 0 {
                if prev_leader != 0 && leader != prev_leader {
                    leader_flip_ts.push(now_ms_books as i64);
                }
                prev_leader = leader;
            }
            leader_flip_ts
                .retain(|t| now_ms_books as i64 - *t <= cfg.sc_chop_window_s * 1000);
            chopped = leader_flip_ts.len() >= cfg.sc_chop_flips as usize;
            if chopped != was_chopped {
                if chopped {
                    tracing::warn!(
                        flips = leader_flip_ts.len(),
                        fenetre_s = cfg.sc_chop_window_s,
                        "FENÊTRE HACHÉE — directionnel coupé (cible 0, urgence prix OFF), on sauve les meubles"
                    );
                } else {
                    tracing::info!("zigzag apaisé — directionnel réarmé");
                }
                was_chopped = chopped;
            }
            let tokyo_sign: i8 = tilt_sign; // conviction Binance (±0.5 confirmé)
            // AVEC le gagnant, toujours : Tokyo prend la main s'il crie, sinon
            // le leader du prix ; si le prix hésite (48-52), on garde le camp.
            let desired_sign: i8 = if chopped {
                0 // fenêtre hachée : plus de camp, on farme l'oscillation
            } else if tokyo_sign != 0 {
                tokyo_sign
            } else if leader != 0 {
                leader
            } else {
                float_sign
            };
            // CONVERSION DE FIN (loi 0xb §1) : sous T−60, si la poussière du
            // côté opposé au flotteur est bradée, la cible revient à 0 — la
            // complétion avale la poussière et convertit le flotteur en paires
            // certaines (jamais plus de perdant que de gagnant : on vise 0, pas
            // l'autre bord). Poussière chère → le flotteur court au redeem.
            let dust_px = if float_sign > 0 { bb_dn } else { bb_up };
            let convert_now = remaining_f <= 60
                && float_sign != 0
                && dust_px > 0.0
                && dust_px <= cfg.sc_conv_dust;
            let new_sign: i8 = if convert_now { 0 } else { desired_sign };
            if new_sign != float_sign
                && now_ms_books as i64 - last_float_ms >= cfg.sc_float_dwell_s * 1000
            {
                tracing::info!(
                    mode = if convert_now {
                        "conversion"
                    } else if chopped {
                        "haché — coupé"
                    } else {
                        "gagnant"
                    },
                    cible = new_sign as f64 * cfg.sc_float_shares,
                    paire = sc
                        .pair_cost()
                        .map(|p| format!("{:.3}", p))
                        .unwrap_or_else(|| "—".into()),
                    leader = if leader > 0 { "up" } else if leader < 0 { "down" } else { "—" },
                    tokyo = format!("{tilt:+.2}"),
                    "FLOTTEUR — nouvelle cible d'inventaire"
                );
                float_sign = new_sign;
                last_float_ms = now_ms_books as i64;
            }
        }
        let target_imb = float_sign as f64 * cfg.sc_float_shares;

        // 1) Quotes désirées → reprice discipline (> 1 tick d'écart = replace).
        let desired = if sleeping || !enabled {
            Vec::new()
        } else {
            sc.desired_bids(
                bb_up,
                bb_dn,
                fair_up,
                m.time_remaining_sec(),
                now_s,
                trend_up,
                m.tick_size,
                cfg.sc_directional_max,
                cfg.sc_directional_min,
                size_factor,
                cfg.sc_symmetric,
                tilt,
                target_imb,
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
        // Deux horloges : le DRIFT (EMA halflife 25 s) confirme un mouvement
        // installé ; l'OFI (flux d'ordres, fenêtre 5 s — « le plus prédictif du
        // prochain tick ») l'ANTICIPE. Sur un marché aussi rapide, le drift réagit
        // trop tard : le temps qu'il quitte « neutre », on s'est déjà fait remplir
        // le perdant. On pull donc sur le RAPIDE (OFI) autant que sur le lent
        // (drift) — 9 juil. : OBI/OFI « ACHAT fort » pendant que le drift affichait
        // « neutre ». Signal positif (pression acheteuse) → le prix monte → c'est
        // la jambe DOWN qui va crasher → on retire son ouverture.
        let drift_danger = endangered_side(drift_ps, cfg.sc_urgency_drift)
            // CHEMIN CHAUD : l'impulsion vaut un drift urgent — le cancel du
            // côté menacé part au tick de boucle (≤100 ms), c'est LE « il
            // annule son up » de la fenêtre-explosion.
            .or(endangered_side(impulse, cfg.sc_impulse));
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
            // URGENCE PRIX (13 juil. 19:31 : 10 Down @0.295 pendant que Up
            // grindait à 0.90 avec un tilt à 0.42 — AUCUN signal Binance ne
            // tirait, mais le marché avait déjà voté). Le côté qu'on doit
            // acheter est devenu le FAVORI (≥ 0.60) = le prix EST le signal :
            // la phase 2 de 0xb aspire le mourant sans condition de signal.
            let bb_deficit = if b.side == Side::Up { bb_up } else { bb_dn };
            let price_urgent = b.completion && bb_deficit >= 0.60;
            if b.completion
                && ((!urgency_blocked
                    && (crate::engines::spread_capture::completion_urgent(
                        b.side,
                        drift_ps,
                        cfg.sc_urgency_drift,
                    ) || price_urgent))
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
                    let avg_excess = sc.avg(match b.side {
                        Side::Up => Side::Down,
                        Side::Down => Side::Up,
                    });
                    let ramp_s = cfg.sc_rescue_ramp_s.max(1.0);
                    let time_ramp =
                        ((ramp_s - m.time_remaining_sec() as f64) / ramp_s).clamp(0.0, 1.0);
                    let taker_thr = cfg.sc_taker_drift.max(1e-9);
                    let sig_ramp =
                        ((drift_ps.abs() - taker_thr) / (2.0 * taker_thr)).clamp(0.0, 1.0);
                    // TILT (impulsion comprise) = composante de confiance : la
                    // chasse suit le marché au lieu de courir dessous quand le
                    // signal penche contre notre surplus (fenêtre 18:50 : chase
                    // plafonnée à 0,42 sous un ask qui filait).
                    let tilt_chase = match b.side {
                        Side::Up => tilt,
                        Side::Down => -tilt,
                    }
                    .clamp(0.0, 1.0);
                    // Confiance PRIX : payer `a` pour le favori est +EV ssi
                    // P(gagne) > a — et le prix du favori EST P. À bb 0.85 →
                    // conf 0.7 → plafond ~1.17 → la chasse colle à l'ask.
                    let price_conf = ((bb_deficit - 0.5) * 2.0).clamp(0.0, 1.0);
                    let base_pair = cfg.sc_completion_max_pair.min(1.0);
                    let rescue_ceiling = if cfg.sc_allow_loss_rescue {
                        cfg.sc_rescue_max_pair
                    } else {
                        base_pair
                    };
                    let ramped_pair = base_pair
                        + time_ramp.max(sig_ramp).max(tilt_chase).max(price_conf)
                            * (rescue_ceiling - base_pair);
                    let pair_room = (ramped_pair - avg_excess).max(0.0);
                    let aggressive = (((ask - tick_sz).max(b.price)) / tick_sz).floor() * tick_sz;
                    let capped = ((aggressive.min(cfg.sc_completion_max_price).min(pair_room))
                        / tick_sz)
                        .floor()
                        * tick_sz;
                    if capped > b.price + 1e-9 {
                        // Distinguer les deux déclencheurs : le drift=+0.0000 des
                        // logs du 8 juil. venait de la fin de fenêtre, PAS de Tokyo.
                        let side_ix = if b.side == Side::Up { 0 } else { 1 };
                        if (capped - last_aggr_px[side_ix]).abs() > tick_sz / 2.0 {
                            last_aggr_px[side_ix] = capped;
                            let cause = if endgame {
                                "fin de fenêtre"
                            } else if crate::engines::spread_capture::completion_urgent(
                                b.side,
                                drift_ps,
                                cfg.sc_urgency_drift,
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
            // Les fills WS/poll ont déjà été drainés et appliqués avant
            // `desired_bids` ci-dessus. Seuls les fills récoltés PENDANT cette
            // phase de cancel/reprice seront appliqués après les placements.
            let mut fills: Vec<LiveFill> = Vec::new();
            // ═══ FAK D'ACCUMULATION (grand modèle : « la folie utile ») ═══
            // Signal Tokyo PLEIN (drift + OFI alignés) → on paie l'ask UNE fois
            // par côté et par rafale (throttle 30 s), borné : prix ≤ fak_max,
            // cap net (surplus + restants compris), stop T−60. La cotation
            // inclinée (tilt) continue derrière en maker.
            // NB grand modèle : plus de sortie éclair — 0xb ne vend JAMAIS ; un
            // retournement se gère en achetant l'autre côté (rééquilibrage),
            // borné par le cap d'imbalance et le plafond de paire.
            if cfg.sc_skew_fak && enabled {
                if let Some(w) = fak_trigger {
                    let throttled = last_fak_side == Some(w)
                        && now_ms_books as i64 - last_fak_ms < 30_000;
                    let is_up = w == Side::Up;
                    let (ask, ask_sz) = if is_up {
                        (ask_up, ask_up_sz)
                    } else {
                        (ask_dn, ask_dn_sz)
                    };
                    // PURGE avant le tir (19:51, 13 juil.) : des ouvertures
                    // PÉRIMÉES du même côté (bids 15 ticks sous un marché parti)
                    // mangeaient le cap net → taille sous le minimum → le seul
                    // trade de la fenêtre jamais tiré. Elles n'ont plus de raison
                    // d'être : on les annule (récolte comptabilisée), le cap se
                    // libère. Un cancel incertain reporte le tir au tick suivant.
                    let mut purge_unsafe_acc = false;
                    if !throttled {
                        let mut i = 0;
                        while i < lrest.len() {
                            if lrest[i].is_up == is_up
                                && lrest[i].intent != OrderIntent::Completion
                            {
                                let slot = lrest.remove(i);
                                let (f, safe) = lv.harvest_and_cancel(&slot.r, is_up).await;
                                if let Some(f) = f {
                                    apply_live_fills(
                                        vec![f],
                                        lv,
                                        &mut paper,
                                        &mut sc,
                                        now_s,
                                        now_ms_books as i64,
                                        &cfg,
                                        &mut last_fill_wall_ms,
                                        &mut win_taker_fees,
                                        &mut win_fills,
                                        &mut win_deployed,
                                        &mut win_rebate,
                                        &mut win_imb_max,
                                        &mut win_purposes,
                                    );
                                }
                                if !safe {
                                    purge_unsafe_acc = true;
                                }
                            } else {
                                i += 1;
                            }
                        }
                    }
                    let surplus = if is_up {
                        sc.imbalance()
                    } else {
                        -sc.imbalance()
                    };
                    let resting_open = lv.open_exposure(&lrest).side(is_up);
                    let cap_room = (cfg.sc_trend_net_cap.max(1.0) - surplus - resting_open).floor();
                    let sz = (cfg.sc_base_clip * cfg.sc_skew_mult)
                        .min(cap_room)
                        .min(ask_sz.max(1.0))
                        .floor();
                    let min_req = m
                        .min_order_size
                        .max(1.0)
                        .max((1.0_f64 / ask.max(0.01)).ceil());
                    if !throttled
                        && !purge_unsafe_acc
                        && ask > 0.0
                        && ask <= cfg.sc_skew_fak_max
                        && m.time_remaining_sec() > cfg.sc_opening_stop_s
                        && sz >= min_req
                        && ask * sz <= lv.cash
                    {
                        tracing::info!(
                            side = w.as_str(),
                            ask = format!("{ask:.3}"),
                            size = format!("{sz:.0}"),
                            tilt = format!("{tilt:+.2}"),
                            drift = format!("{drift_ps:+.5}"),
                            "ACCUMULATION taker — Tokyo plein, on paie l'ask"
                        );
                        // MARKETABLE (13 juil. 15:35/15:37) : l'ask affiché a
                        // 150-500 ms d'âge quand l'ordre atterrit — dans un
                        // marché qui bouge (précisément quand le FAK tire), il
                        // a déjà fui → « no orders found to match ». On vise
                        // ask + 3 ticks, borné par le plafond : on ne paie le
                        // tampon QUE si le carnet a bougé pendant le vol.
                        let limit = (ask + 3.0 * tick_sz).min(cfg.sc_skew_fak_max);
                        if lv
                            .place_insurance_fak(is_up, limit, sz, OrderIntent::SkewAccumulation)
                            .await
                        {
                            last_fak_side = Some(w);
                            last_fak_ms = now_ms_books as i64;
                        } else {
                            // « no match » : re-tir au tick suivant avec l'ask
                            // FRAIS (throttle réduit à ~500 ms, pas 30 s).
                            last_fak_side = Some(w);
                            last_fak_ms = now_ms_books as i64 - 29_500;
                        }
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
                    let od = desired
                        .iter()
                        .find(|b| b.side == Side::Down && !b.completion);
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
                let exposure = lv.open_exposure(&lrest);
                let mut open_side: [f64; 2] = [exposure.side(true), exposure.side(false)];
                // Un audit signifie qu'on ne sait pas encore si le reliquat est
                // au carnet. Toute nouvelle pose du côté serait un doublon
                // potentiel, en particulier une complétion.
                let mut side_frozen: [bool; 2] = [
                    exposure.uncertain_side(true) > 1e-9,
                    exposure.uncertain_side(false) > 1e-9,
                ];
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
                    let want_px = want_px;
                    let want_sz = want_sz;
                    let is_comp = is_comp;
                    // ÉCHELONNEMENT 0xb (passe 2) : la complétion se pose en
                    // TRANCHES sur la chute du mourant (cap, ⅔·cap, ⅓·cap — le
                    // « 30/20/10 » utilisateur) dès que le déficit offre
                    // ≥ min_shares par tranche. Un reverse profond remplit les
                    // tranches basses → paire grasse ; sinon la tranche haute
                    // assure seule. Ouvertures : échelle classique.
                    let n_levels: u8 = if is_comp {
                        let total = want_sz.unwrap_or(0.0);
                        ((total / min_shares.max(1.0)).floor() as u8).clamp(1, 3)
                    } else {
                        cfg.sc_ladder_levels.clamp(1, 4) as u8
                    };
                    for lvl in 0..4u8 {
                        // cible de CE niveau (None = rien à poser → retrait)
                        let target: Option<(f64, f64)> = match (want_sz, want_px) {
                            (Some(sz), Some(px0)) if lvl < n_levels => {
                                let (px, sz) = if is_comp && n_levels > 1 {
                                    // Tranches : prix = cap × (n−lvl)/n, taille
                                    // = déficit/n (la tranche 0 prend le reste).
                                    let n = n_levels as f64;
                                    let frac = (n - lvl as f64) / n;
                                    let px = ((px0 * frac) / tick_sz).floor() * tick_sz;
                                    let part = ((sz / n) * 100.0).floor() / 100.0;
                                    let part = if lvl == 0 {
                                        ((sz - part * (n - 1.0)) * 100.0).floor() / 100.0
                                    } else {
                                        part
                                    };
                                    (px, part)
                                } else {
                                    let px = ((px0
                                        - lvl as f64 * cfg.sc_ladder_step_ticks * tick_sz)
                                        / tick_sz)
                                        .floor()
                                        * tick_sz;
                                    (px, sz)
                                };
                                let min_req = min_shares.max((1.0_f64 / px.max(0.01)).ceil());
                                if px >= 0.01 && sz + 1e-9 >= min_req && px * sz <= lv.cash {
                                    Some((px, sz))
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        };
                        let slot_pos = lrest
                            .iter()
                            .position(|s| s.is_up == is_up && s.level == lvl);
                        match (target, slot_pos) {
                            (Some((px, sz)), cur) => {
                                // ANTI-CHURN (7 juil.) : reprice seulement si l'écart
                                // dépasse 2 ticks ET ≥4 s depuis le dernier placement
                                // de ce (côté, niveau).
                                let cooled = now_ms_books as i64
                                    - last_place_ms[side_ix][lvl as usize]
                                    >= 4_000;
                                let reprice = match cur {
                                    Some(i) => {
                                        let resting = lrest[i].r.price;
                                        if is_comp {
                                            // CHASSE MONOTONE (13 juil. 15:21) : une
                                            // complétion ne redescend JAMAIS — le churn
                                            // au gré du tilt créait la course cancel/
                                            // repose → double transitoire → pause.
                                            (px - resting).abs() > 2.0 * tick_sz + 1e-9
                                                && cooled
                                                && px > resting + 1e-9
                                        } else {
                                            // FILE PRÉSERVÉE (14 juil., 0xb) : chaque
                                            // reprice = retour en FIN de file. Ses
                                            // ordres restent et vieillissent → servis
                                            // les premiers (270 fills/fenêtre vs 10).
                                            // On ne déplace une ouverture que si elle
                                            // devient DANGEREUSE (au-dessus de la
                                            // cible = plafond de paire violé) ou si
                                            // le touch l'a distancée. La tolérance de
                                            // 13 ticks (grille entière) a cloué les
                                            // bids Down à 0.74/0.76 pendant tout un
                                            // grind 0.76→0.98 (00:40 le 14 juil. :
                                            // 2 min 46 sans un ordre déplacé, zéro
                                            // fill sur la phase décidée). 0xb suit le
                                            // touch marche par marche — chaque niveau
                                            // chasse dès que SA cible s'éloigne de
                                            // plus d'un pas d'échelle (+1 tick de
                                            // jitter), le cooldown 4 s garde la file
                                            // sur les oscillations courtes.
                                            let chase_span =
                                                (cfg.sc_ladder_step_ticks + 1.0) * tick_sz;
                                            cooled
                                                && (resting > px + 1e-9
                                                    || px - resting > chase_span)
                                        }
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
                                        }
                                        // Cancel non confirmé = fill en vol probable
                                        // (course 02:02 : les DEUX ordres exécutés).
                                        // On ne pose PAS le remplacement ce tick.
                                        if safe {
                                            open_side[side_ix] -=
                                                (slot.r.size - slot.r.matched).max(0.0);
                                            // REPOSE DIFFÉRÉE (13 juil. 15:21) : même
                                            // un cancel « sûr » peut rester vivant 1-2 s
                                            // côté CLOB (ACK ≠ carnet). Le remplaçant
                                            // part au tick SUIVANT (100 ms), après le
                                            // drain/réconcile — jamais le même tick.
                                            side_frozen[side_ix] = true;
                                        } else {
                                            side_frozen[side_ix] = true;
                                        }
                                        old_was_filled = true;
                                    }
                                    if old_was_filled {
                                        // Cancel émis ce tick (ou ordre déjà fillé) :
                                        // rien posé ce tick (19:25 le 8 juil. : achat
                                        // DOUBLE ; 13 juil. 15:21 : course cancel/pose).
                                        continue;
                                    }
                                    // GARDE-FOU EXPOSITION : côté gelé → rien ce
                                    // tick ; complétion → le TOTAL posé (tranches
                                    // comprises) ne dépasse jamais le déficit.
                                    let comp_total = want_sz.unwrap_or(0.0) + 0.01;
                                    if side_frozen[side_ix]
                                        || (is_comp
                                            && (open_side[side_ix] + sz > comp_total
                                                || exposure.completion_side(is_up) + sz
                                                    > comp_total))
                                    {
                                        tracing::warn!(side = ?side,
                                            open = format!("{:.1}", open_side[side_ix]),
                                            gele = side_frozen[side_ix],
                                            "garde-fou exposition : POST refusé (résidu du même côté au carnet)");
                                        continue;
                                    }
                                    // RE-CLAMP au dernier moment : ask le plus frais
                                    // juste avant le POST.
                                    let tok = if is_up {
                                        &m.up_token_id
                                    } else {
                                        &m.down_token_id
                                    };
                                    let fa = fresh_ask(tok, ask);
                                    let px = if fa > buf {
                                        px.min(((fa - buf) / tick_sz).floor() * tick_sz)
                                    } else {
                                        px
                                    };
                                    if px >= 0.01 {
                                        // OUVERTURES en POST-ONLY : croiser = rejet
                                        // propre du CLOB (zéro taxe accidentelle).
                                        let intent = if is_comp {
                                            OrderIntent::Completion
                                        } else {
                                            OrderIntent::SymmetricOpen
                                        };
                                        if let Some(r) =
                                            lv.place_bid(is_up, px, sz, !is_comp, intent).await
                                        {
                                            open_side[side_ix] += (r.size - r.matched).max(0.0);
                                            lrest.push(LiveSlot {
                                                r,
                                                is_up,
                                                level: lvl,
                                                intent,
                                            });
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
                                    open_side[side_ix] -= (slot.r.size - slot.r.matched).max(0.0);
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
            apply_live_fills(
                fills,
                lv,
                &mut paper,
                &mut sc,
                now_s,
                now_ms_books as i64,
                &cfg,
                &mut last_fill_wall_ms,
                &mut win_taker_fees,
                &mut win_fills,
                &mut win_deployed,
                &mut win_rebate,
                &mut win_imb_max,
                &mut win_purposes,
            );
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
            // L'ÉCART se mesure à la CIBLE du flotteur, pas à zéro (loi 0xb) :
            // porter le flotteur voulu n'est pas être unijambiste — c'est
            // l'excédent AU-DELÀ de la cible qui appelle l'assurance.
            // DOCTRINE (15 juil., fenêtre 00:35, −8$) : un achat qui AUGMENTE
            // l'exposition est un PARI → maker uniquement (les creux, rôle de
            // Tokyo) ; le taker n'a le droit que de RÉDUIRE |imbalance| vers 0.
            // Le sauvetage à imb −1.67 / cible −12 achetait 10 Down @0.79-0.80
            // au sommet pour CONSTRUIRE le flotteur — puis Up a gagné. La part
            // assurable = le chemin de |imb| vers 0, jamais au travers.
            let dev_imb = sc.imbalance() - target_imb;
            let insurable = if dev_imb.signum() == sc.imbalance().signum() {
                dev_imb.abs().min(sc.imbalance().abs())
            } else {
                0.0 // combler vers la cible ÉLOIGNERAIT de 0 : pari → maker seul
            };
            let deficit_side = if dev_imb > 0.0 { Side::Down } else { Side::Up };
            let taker_drift_urgent = crate::engines::spread_capture::completion_urgent(
                deficit_side,
                drift_ps,
                cfg.sc_taker_drift,
            );
            // RÈGLE « JAMAIS UNIJAMBISTE » (13 juil., fenêtre 18:50 : 6 Down
            // morts à −3,85 $ pendant que la confirmation 8 s bloquait le FAK) :
            // un achat qui AUGMENTE l'exposition est un pari → confirmation ;
            // un achat qui la RÉDUIT est une assurance → JAMAIS de délai. Le
            // signal BRUT (tilt fort — impulsion comprise — ou Tokyo lent)
            // contre notre surplus déclenche le rééquilibrage immédiat.
            let bb_deficit_ins = if deficit_side == Side::Up { bb_up } else { bb_dn };
            let signal_against_surplus = match deficit_side {
                Side::Up => tilt >= 0.5,
                Side::Down => tilt <= -0.5,
            } || tokyo_slow == Some(deficit_side)
                // URGENCE PRIX : le côté à acheter est le favori ≥ 0.60 — le
                // marché a voté, pas besoin de Binance (19:31 : tilt 0.42,
                // Down @0.085, rien ne tirait). DÉSARMÉE en fenêtre hachée
                // (collée au strike, un carnet à 0.65 n'a rien voté : on a payé
                // chaque sommet 5× le 15 juil. 00:03-00:04) — les déclencheurs
                // Tokyo et la fin de fenêtre restent.
                || (!chopped && bb_deficit_ins >= 0.60);
            if enabled
                && insurable >= 5.0
                && ((10..=20).contains(&remaining_l)
                    || ((taker_drift_urgent || signal_against_surplus) && remaining_l > 20))
                && now_ms_books as i64 - last_insurance_ms >= 3_000
            {
                let (is_up, ask, ask_sz) = if dev_imb > 0.0 {
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
                // Le TILT (impulsion comprise) contre notre surplus est une
                // composante de confiance à part entière : à 0,57 le plafond
                // monte à ~1,14 — la chasse/le FAK rattrapent le marché au lieu
                // de courir dessous (fenêtre 18:50).
                let tilt_ramp_ins = if is_up { tilt } else { -tilt }.clamp(0.0, 1.0);
                let price_conf_ins = ((bb_deficit_ins - 0.5) * 2.0).clamp(0.0, 1.0);
                let conf = time_ramp.max(sig_ramp).max(tilt_ramp_ins).max(price_conf_ins);
                let base_pair = cfg.sc_completion_max_pair.min(1.0);
                let rescue_ceiling = if cfg.sc_allow_loss_rescue {
                    cfg.sc_rescue_max_pair
                } else {
                    base_pair
                };
                let rescue_pair = base_pair + conf * (rescue_ceiling - base_pair);
                let pair_room_ins = rescue_pair - avg_excess_ins;
                // BORNE EV (13 juil., « toujours compenser ») : compléter coûte
                // (ask + c − 1) CERTAIN ; tenir nu coûte c × ask en ESPÉRANCE
                // (le prix du favori EST sa probabilité). Compléter gagne ssi
                // (1−ask)(1−c) > frais (~3¢) — presque toujours. Le plafond de
                // paire fixe bloquait les compensations à coût élevé (c=0.54 :
                // interdit dès ask 0.69, l'EV autorise 0.93). Borne dure 0.95.
                let pair_room_ins = if cfg.sc_allow_loss_rescue {
                    let ev_room = (1.0 - 0.03 / (1.0 - avg_excess_ins).max(0.05)).min(0.95);
                    pair_room_ins.max(ev_room)
                } else {
                    pair_room_ins
                };
                let fee_per_share = 0.07 * ask * (1.0 - ask);
                let all_in_ask = ask + fee_per_share;
                if ask > 0.0 && ask <= 0.99 && all_in_ask <= pair_room_ins + 1e-9 {
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
                                apply_live_fills(
                                    vec![f],
                                    lv,
                                    &mut paper,
                                    &mut sc,
                                    now_s,
                                    now_ms_books as i64,
                                    &cfg,
                                    &mut last_fill_wall_ms,
                                    &mut win_taker_fees,
                                    &mut win_fills,
                                    &mut win_deployed,
                                    &mut win_rebate,
                                    &mut win_imb_max,
                                    &mut win_purposes,
                                );
                            }
                            if !safe {
                                purge_unsafe = true;
                            }
                        } else {
                            i += 1;
                        }
                    }
                    // Assurance : JAMAIS plus que la part ASSURABLE (réduction de
                    // |imb| vers 0 — pas de sur-achat, pas de traversée vers
                    // l'autre bord). Recalculée après purge (fills en vol).
                    let dev_p = sc.imbalance() - target_imb;
                    let insurable_p = if dev_p.signum() == sc.imbalance().signum() {
                        dev_p.abs().min(sc.imbalance().abs())
                    } else {
                        0.0
                    };
                    let sz = ((insurable_p.min(ask_sz.max(1.0))) * 100.0)
                        .floor()
                        / 100.0;
                    // FAK = exécution immédiate : le minimum de 5 parts ne
                    // s'applique QU'AUX ordres restants (prouvé 9-10 juil. :
                    // vente FAK de 0,56 part). Un déficit SOUS-MINIMUM (fill
                    // partiel — 13 juil. 19:35 : 4,37 Up, complétion refusée
                    // 3 min 30, morts à zéro) se complète en FAK : c'est la
                    // SEULE voie, le resting < 5 étant rejeté par le CLOB.
                    let min_req = 1.0_f64;
                    if purge_unsafe {
                        tracing::warn!(
                            side = if is_up { "up" } else { "down" },
                            "sauvetage différé : cancel non confirmé du même côté (fill en vol probable)"
                        );
                    } else if sz + 1e-9 >= min_req && (ask + fee_per_share) * sz <= lv.cash {
                        // Le label reflète le VRAI déclencheur (00:40:43 le
                        // 14 juil. : « fin de fenêtre » loggé à rem=256 alors
                        // que c'était l'urgence prix).
                        let cause = if (10..=20).contains(&remaining_l) {
                            "fin de fenêtre"
                        } else if taker_drift_urgent {
                            "drift fort"
                        } else if bb_deficit_ins >= 0.60 {
                            "urgence prix (le marché a voté)"
                        } else {
                            "tilt/Tokyo contre le surplus"
                        };
                        let pair_now = all_in_ask + avg_excess_ins;
                        tracing::info!(
                            side = if is_up { "up" } else { "down" },
                            ask = format!("{ask:.3}"),
                            size = format!("{sz:.1}"),
                            rem = remaining_l,
                            drift = format!("{drift_ps:+.5}"),
                            cause,
                            pair = format!("{pair_now:.3}"),
                            cap = format!("{rescue_pair:.3}"),
                            "SAUVETAGE taker (paie le marché pour compléter la paire)"
                        );
                        // MARKETABLE : même tampon que l'accumulation, borné
                        // par la place du plafond de paire rampé.
                        let limit = (ask + 3.0 * tick_sz).min(pair_room_ins).max(ask);
                        if lv
                            .place_insurance_fak(is_up, limit, sz, OrderIntent::Rescue)
                            .await
                        {
                            last_insurance_ms = now_ms_books as i64;
                        } else {
                            // « no match » : re-tir ~1 s plus tard, ask frais.
                            last_insurance_ms = now_ms_books as i64 - 2_000;
                        }
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
            // COUPE ANTICIPÉE DU PERDANT (règle 0xb : imbalance→0 AVANT la fin,
            // seuls les résidus GAGNANTS vont à la résolution). À T−9 le carnet
            // du mourant est déjà vide (18:54:50 : flatten refusé à 3¢, 5,97
            // Down morts à zéro) — on vend le résidu PERDANT dès T−15, pendant
            // qu'il reste des bids à 10-20¢. Le résidu gagnant, lui, court
            // jusqu'au payout (comportement 0xb : il redeem, ne brade jamais).
            let losing_residual = {
                let bb_side = if sc.imbalance() > 0.0 { bb_up } else { bb_dn };
                bb_side > 0.0 && bb_side < 0.50
            };
            let early_cut = (10..=15).contains(&remaining_l) && imb_abs > 0.4 && losing_residual;
            // ORDRE UTILISATEUR (14 juil.) : ZÉRO VENTE (0xb = 100 % achats).
            // Les résidus — poussière comprise — courent jusqu'à la résolution :
            // le gagnant paie au redeem, le perdant expire. SC_ALLOW_FLATTEN=true
            // pour réactiver.
            if enabled
                && cfg.sc_allow_flatten
                && (dust_case || endgame_case || early_cut)
                && now_ms_books as i64 - last_insurance_ms >= 3_000
            {
                let excess_up = sc.imbalance() > 0.0;
                let side = if excess_up { Side::Up } else { Side::Down };
                let held = if excess_up {
                    paper.state.up_balance
                } else {
                    paper.state.down_balance
                };
                let sz = sc.imbalance().abs().min(held);
                let bid = if excess_up { bb_up } else { bb_dn };
                if sz > 0.0 && bid > 0.0 {
                    last_insurance_ms = now_ms_books as i64;
                    if let Some((sold, avg)) = lv.flatten_sell(excess_up, sz, bid, tick_sz).await {
                        // Comptabilité : vente réelle → miroir + moteur + frais taker.
                        let fee = 0.07 * avg * (1.0 - avg) * sold;
                        lv.cash -= fee;
                        win_taker_fees += fee;
                        win_purposes.flatten.sell_notional += avg * sold;
                        win_purposes.flatten.taker_fees += fee;
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
                        let cause = if dust_case {
                            "poussière (incomplétable)"
                        } else {
                            "fin de fenêtre"
                        };
                        tracing::info!(
                            side = side.as_str(),
                            sold = format!("{sold:.2}"),
                            px = format!("{avg:.3}"),
                            recupere = format!("{:.2}", avg * sold),
                            fee = format!("{fee:.3}"),
                            rem = remaining_l,
                            cause,
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
                        tracing::info!(
                            blended = format!("{blended:.3}"),
                            "merge à paire > 1$ — escalade gelée 45 s (le quoting ≤1$ continue)"
                        );
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
                tracing::info!(
                    pairs_tx = pairs_done,
                    credited = p,
                    "merge on-chain appliqué (crédit exact)"
                );
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
                    lv.halt(format!(
                        "divergence inventaire confirmée (réel {ru:.2}/{rd:.2}, miroir {mu:.2}/{md:.2})"
                    ));
                    tracing::warn!(
                        reel_up = ru,
                        reel_dn = rd,
                        miroir_up = mu,
                        miroir_dn = md,
                        "positions désynchronisées — miroir aligné puis nouvelles poses arrêtées"
                    );
                    paper.state.up_balance = ru;
                    paper.state.down_balance = rd;
                    // le moteur suit aussi (imbalance/complétion sur la vérité) —
                    // et les COÛTS sont mis à l'échelle : ajuster les parts sans
                    // les coûts gonfle l'avg (10 juil. 02:01 : 18→12 Down sans
                    // scaling → blended fantôme 1.248 → escalade gelée à tort).
                    let scale_u = if sc.shares_up > 1e-9 {
                        ru / sc.shares_up
                    } else {
                        0.0
                    };
                    let scale_d = if sc.shares_dn > 1e-9 {
                        rd / sc.shares_dn
                    } else {
                        0.0
                    };
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
                d.audit_orders = lv.audit_count();
                d.audit_oldest_ms = lv.audit_oldest_age_ms(now_ms_books as i64);
                d.live_halted = lv.is_halted();
                d.live_halt_reason = lv.halted_reason.clone().unwrap_or_default();
            }
            // Miroir des bids pour le dashboard.
            // Affichage : le MEILLEUR bid (niveau le plus haut) par côté + le
            // compte réel d'ordres ouverts (échelle comprise).
            rest_up = lrest
                .iter()
                .filter(|s| s.is_up)
                .max_by(|a, b| a.r.price.total_cmp(&b.r.price))
                .map(|s| (s.r.price, s.r.size));
            rest_dn = lrest
                .iter()
                .filter(|s| !s.is_up)
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
        // L'écart se mesure à la CIBLE du flotteur (loi 0xb) — le résidu voulu
        // court au redeem, seul l'excédent au-delà s'assure. Et le taker ne
        // fait que RÉDUIRE |imb| vers 0 (jamais construire le flotteur).
        let dev_paper = sc.imbalance() - target_imb;
        let insurable_paper = if dev_paper.signum() == sc.imbalance().signum() {
            dev_paper.abs().min(sc.imbalance().abs())
        } else {
            0.0
        };
        if !live_mode && enabled && (10..=45).contains(&remaining) && insurable_paper >= 5.0 {
            let (side, ask, ask_sz) = if dev_paper > 0.0 {
                (Side::Down, ask_dn, ask_dn_sz)
            } else {
                (Side::Up, ask_up, ask_up_sz)
            };
            if ask > 0.0 && ask <= 0.99 {
                let deficit = insurable_paper;
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
            d.open_orders =
                live_open_ct.unwrap_or(rest_up.is_some() as u32 + rest_dn.is_some() as u32);
            if d.last_block_reason.starts_with("SIGNAL TOKYO") {
                d.last_block_reason.clear();
            }
            d.rebate_window = win_rebate;
            d.rebate_total = rebate_total + win_rebate;
            d.size_factor = size_factor;
            d.loss_streak = loss_streak;
            d.pair_cost = sc.pair_cost().unwrap_or(0.0);
            d.merge_pair_avg = if win_merge_n > 0 {
                win_merge_cost_sum / win_merge_n as f64
            } else {
                0.0
            };
            // Flotteur (cible signée côté gagnant) et tilt Binance, pour le dashboard.
            d.skew_side = if float_sign != 0 {
                format!("cible {target_imb:+.0} · tilt {tilt:+.2}")
            } else if tilt.abs() >= 0.1 {
                format!("{tilt:+.2}")
            } else {
                String::new()
            };
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
            d.last_block_reason = if d.live_halted {
                format!("ARRÊT LIVE : {}", d.live_halt_reason)
            } else if d.audit_orders > 0 {
                format!(
                    "audit/cancel en attente : {} ordre(s), âge max {} ms",
                    d.audit_orders, d.audit_oldest_ms
                )
            } else if sleeping {
                "sommeil (heures creuses UTC)".into()
            } else {
                String::new()
            };
            // Carnet Up : 6 meilleurs niveaux de chaque côté pour visualisation.
            let mut bids = up_book.bids.clone();
            bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
            let mut asks = up_book.asks.clone();
            asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
            d.book_bids = bids
                .iter()
                .take(6)
                .map(|l| crate::dashboard::BookLevel {
                    price: l.price,
                    size: l.size,
                })
                .collect();
            d.book_asks = asks
                .iter()
                .take(6)
                .map(|l| crate::dashboard::BookLevel {
                    price: l.price,
                    size: l.size,
                })
                .collect();

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
                state = if !enabled {
                    "OFF(manuel)"
                } else if sleeping {
                    "sleep"
                } else if paused {
                    "kill(obs)"
                } else {
                    "scan"
                },
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
