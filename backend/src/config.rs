//! Configuration du bot, chargée depuis l'environnement (`.env` + variables systemd).
//! Le rôle (`radar`|`executor`) détermine quelle boucle `main.rs` lance.

use std::env;
use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotRole {
    Radar,
    Executor,
}

impl BotRole {
    fn from_env() -> Self {
        // Priorité à BOT_ROLE explicite, sinon dérivé de la région AWS.
        let raw = env::var("BOT_ROLE")
            .ok()
            .or_else(|| env::var("AWS_REGION").ok())
            .unwrap_or_default()
            .to_lowercase();
        match raw.as_str() {
            "radar" | "ap-northeast-1" => BotRole::Radar,
            "executor" | "eu-west-1" => BotRole::Executor,
            // Défaut sûr en dev : exécuteur (ne touche pas Binance à haute fréquence).
            _ => BotRole::Executor,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // champs hérités v1-v5 conservés (réactivables par env)
pub struct Config {
    pub role: BotRole,
    pub dry_run: bool,
    pub live_armed: bool, // 2e verrou : sans lui les ordres sont signés mais JAMAIS postés

    // Réseau / dashboard
    pub dashboard_port: u16,
    pub binance_ws_url: String,
    pub signal_addr: SocketAddr,        // adresse locale d'écoute (exécuteur)
    pub signal_target: Option<SocketAddr>, // cible (radar → exécuteur)
    pub signal_target2: Option<SocketAddr>, // 2e cible optionnelle (radar → live)
    pub use_udp_transport: bool,        // false = loopback in-process (dev local)

    // Radar (J2)
    pub obi_depth_levels: usize,
    pub obi_threshold: f64,
    pub velocity_threshold: f64,

    // Volatilité (J4)
    pub volatility_floor: f64,

    // Signal MM (drift + OBI) — câblés dans la décision de cotation
    pub drift_halflife_secs: f64, // halflife EMA du drift (s)
    pub drift_clamp_k: f64,       // borne du drift à ±k·σ·√t
    pub obi_skew: f64,            // gain du skew OBI sur la fair de cotation

    // Pull anti-sélection-adverse : retirer le bid du côté qui décroche
    pub pull_net_min: f64,   // seuil de position nette (parts) avant de pull
    pub pull_slope: f64,     // baisse de mid/tick qui déclenche le pull
    pub loser_thresh: f64,   // fair < ce seuil ⇒ côté perdant quasi-certain ⇒ pull

    // Spread-capture taker (plan v5) — priors du guide, calibrés Phase A/C
    pub sc_c_raw: f64,
    pub sc_fee_per_pair: f64,
    pub sc_opening_leg_max: f64,
    pub sc_max_imbalance: f64,
    pub sc_base_clip: f64,
    pub sc_max_clip: f64,
    pub sc_depth_gain: f64,
    pub sc_max_clip_usdc: f64,
    pub sc_max_capital_per_market: f64,
    pub sc_min_seconds: i64,
    pub sc_clip_interval_s: i64,
    pub sc_gate_margin: f64,
    pub sc_min_window_age_s: i64,  // pas d'entrée avant N s d'âge de fenêtre
    pub sc_completion_reserve: f64, // fraction du capital réservée à la complétion
    pub sc_drift_horizon_s: f64,   // horizon max (s) d'extrapolation du drift dans la fair
    // v7 — rétro-ingénierie 0xb27b
    pub sc_trend_filter: bool,        // directionnel seulement dans le sens de la tendance
    pub sc_pullback_filter: bool,     // directionnel seulement sur micro-repli 5 s
    pub sc_pullback_s: i64,           // horizon du micro-repli (s)
    pub sc_completion_max_price: f64, // prix max d'une jambe de complétion
    pub sc_completion_max_pair: f64,  // plafond de paire pour la complétion
    // v8 maker (copie complète, recalibrée sur 234 fenêtres)
    pub sc_directional_max: f64,   // borne absolue du prix directionnel (0.90 — il charge jusqu'à 87c)
    pub sc_directional_min: f64,   // bid directionnel INTERDIT si best_bid < ce seuil (la cible accumule le favori 66-72c, jamais le couteau)
    pub sc_trend_confirm_s: i64,   // le drift doit garder son signe N s avant d'armer le directionnel (anti flip-flop)
    pub sc_ofi_confirm: bool,      // veto OFI : pas de directionnel si le flux d'ordres Binance contredit le drift
    pub sc_ofi_min: f64,           // seuil de contradiction (|OFI| ≥ min contre nous → veto ; en-dessous = bruit, on laisse)
    pub sc_rebate_rate: f64,       // rebate = rate × Σ 0.07·p(1−p)·taille (officiel : 20% part maker)
    pub sc_streak_soft: u32,       // pertes consécutives → taille ×0.25
    pub sc_streak_hard: u32,       // pertes consécutives → 1 fenêtre sur 3 à ×0.25
    pub sc_bankroll_pct: f64,      // >0 : budget/fenêtre = pct × bankroll (recalculé au rollover) ; 0 = cap fixe

    // Heures UTC sans NOUVELLES entrées (nuit : jour +6,3% vs nuit −2,2% mesuré)
    pub sc_sleep_hours_utc: Vec<u32>,
    // Sélecteur de stratégie ("sc" = spread-capture taker · "gtc" = pair-GTC utilisateur)
    pub strategy: String,
    // Pair-GTC (bot parallèle port 8700)
    pub pg_size: f64,               // X parts par ordre GTC
    pub pg_band: f64,               // |mid_up − 0,5| ≤ band pour entrer
    pub pg_entry_min_remaining: i64, // temps mini restant pour entrer (s)
    pub pg_entry_deadline: i64,     // annule les GTC non fillés sous N s
    pub pg_pair_target: f64,        // complète si avg + ask_opp + fee ≤ target
    pub pg_require_rising: bool,    // règle : compléter seulement sur un REBOND de l'opposé
    pub pg_rising_lookback_s: i64,  // lookback (s) pour juger « en train de remonter »

    // Avellaneda-Stoikov + reward (J7)
    pub gamma: f64,
    pub kappa: f64,
    pub our_size: f64,            // taille de nos ordres (tokens) — legacy/test
    pub reward_pool_per_min: f64, // pool de reward estimé ($/min)
    pub base_half_spread_cents: f64, // R2 : demi-spread de base (remplace le terme A-S mal échelonné)

    // Bankroll / gates (R4)
    pub bankroll_fraction: f64,    // max % equity par ordre
    pub max_net_exposure_pct: f64, // plafond |net|·mid vs equity
    pub min_cash_reserve_pct: f64, // cash minimum
    pub max_window_loss_pct: f64,  // stop si window_pnl/window_start < -X
    pub max_order_size: f64,       // plafond absolu tokens/ordre
    pub max_position: f64,         // plafond absolu de position par côté
    pub max_net_shares: f64,       // plafond de la jambe NETTE en parts (bug fix cap)
    pub paired_buy_margin: f64,    // achat pairé si up_ask+down_ask < 1 - margin
    pub flip_size: f64,            // taille cible du flip sur alarme Binance (parts)

    // Exécution maker (R3)
    pub maker_fill_prob: f64, // proba de fill maker par tick
    pub maker_only: bool,     // true = pas de fills taker

    // KILL / panic stop (R5)
    pub kill_pause_secs: i64,
    pub panic_stop_secs: i64,
    pub flatten_secs: i64, // garde-fou 3 (TTE) : aplatir la jambe nette sous ce TTE

    // Paper / inventaire (J8)
    pub start_cash: f64,
    pub state_path: String,
    pub trades_path: String,

    // Fusion CTF (J8/J11)
    pub min_merge_threshold: f64,
    pub safety_mult: f64,
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        let dashboard_port: u16 = env_or("PORT", 8767);
        let signal_port: u16 = env_or("SIGNAL_PORT", 9001);

        let signal_addr: SocketAddr = env::var("SIGNAL_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| ([127, 0, 0, 1], signal_port).into());

        let signal_target: Option<SocketAddr> = env::var("SIGNAL_TARGET")
            .ok()
            .and_then(|s| s.parse().ok());
        let signal_target2: Option<SocketAddr> = env::var("SIGNAL_TARGET2")
            .ok()
            .and_then(|s| s.parse().ok());

        Self {
            role: BotRole::from_env(),
            dry_run: env_or("DRY_RUN", true),
            live_armed: env_or("LIVE_ARMED", false),

            dashboard_port,
            binance_ws_url: env::var("BINANCE_WS_URL").unwrap_or_else(|_| {
                // Partial book depth : snapshot complet du top-20 à 100ms,
                // pas de resynchro par lastUpdateId nécessaire.
                "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms".to_string()
            }),
            signal_addr,
            signal_target,
            signal_target2,
            use_udp_transport: env_or("USE_UDP_TRANSPORT", false),

            obi_depth_levels: env_or("OBI_DEPTH_LEVELS", 5),
            obi_threshold: env_or("OBI_THRESHOLD", 0.85),
            velocity_threshold: env_or("VELOCITY_THRESHOLD", 5.0),

            // R1 (truth protocol) : floor MONTÉ — un σ plus élevé rapproche le fair du mid.
            volatility_floor: env_or("VOLATILITY_FLOOR", 0.80),

            drift_halflife_secs: env_or("DRIFT_HALFLIFE_SECS", 25.0),
            drift_clamp_k: env_or("DRIFT_CLAMP_K", 2.0),
            obi_skew: env_or("OBI_SKEW", 0.05),
            pull_net_min: env_or("PULL_NET_MIN", 12.0),
            pull_slope: env_or("PULL_SLOPE", 0.008),
            loser_thresh: env_or("LOSER_THRESH", 0.12),

            sc_c_raw: env_or("SC_C_RAW", 0.95),
            sc_fee_per_pair: env_or("SC_FEE_PER_PAIR", 0.03),
            sc_opening_leg_max: env_or("SC_OPENING_LEG_MAX", 0.55),
            sc_max_imbalance: env_or("SC_MAX_IMBALANCE", 40.0),
            sc_base_clip: env_or("SC_BASE_CLIP", 10.0),
            sc_max_clip: env_or("SC_MAX_CLIP", 20.0),
            sc_depth_gain: env_or("SC_DEPTH_GAIN", 60.0),
            sc_max_clip_usdc: env_or("SC_MAX_CLIP_USDC", 6.0),
            sc_max_capital_per_market: env_or("SC_MAX_CAPITAL_PER_MARKET", 20.0),
            sc_min_seconds: env_or("SC_MIN_SECONDS", 10),
            sc_clip_interval_s: env_or("SC_CLIP_INTERVAL_S", 15),
            sc_gate_margin: env_or("SC_GATE_MARGIN", 0.04),
            sc_min_window_age_s: env_or("SC_MIN_WINDOW_AGE_S", 15),
            sc_completion_reserve: env_or("SC_COMPLETION_RESERVE", 0.5),
            sc_drift_horizon_s: env_or("SC_DRIFT_HORIZON_S", 60.0),
            sc_trend_filter: std::env::var("SC_TREND_FILTER").map(|v| v != "false").unwrap_or(true),
            sc_pullback_filter: std::env::var("SC_PULLBACK_FILTER").map(|v| v != "false").unwrap_or(true),
            sc_pullback_s: env_or("SC_PULLBACK_S", 5),
            sc_completion_max_price: env_or("SC_COMPLETION_MAX_PRICE", 0.65),
            sc_completion_max_pair: env_or("SC_COMPLETION_MAX_PAIR", 1.05),
            sc_directional_max: env_or("SC_DIRECTIONAL_MAX", 0.90),
            sc_directional_min: env_or("SC_DIRECTIONAL_MIN", 0.40),
            sc_trend_confirm_s: env_or("SC_TREND_CONFIRM_S", 20),
            sc_ofi_confirm: env_or("SC_OFI_CONFIRM", true),
            sc_ofi_min: env_or("SC_OFI_MIN", 0.15),
            sc_rebate_rate: env_or("SC_REBATE_RATE", 0.20),
            sc_streak_soft: env_or("SC_STREAK_SOFT", 4),
            sc_streak_hard: env_or("SC_STREAK_HARD", 6),
            sc_bankroll_pct: env_or("SC_BANKROLL_PCT", 0.0),

            sc_sleep_hours_utc: std::env::var("SC_SLEEP_HOURS_UTC")
                .unwrap_or_else(|_| "22,23,0,1,2,3,8".into())
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect(),
            strategy: std::env::var("STRATEGY").unwrap_or_else(|_| "sc".into()),
            pg_size: env_or("PG_SIZE", 20.0),
            pg_band: env_or("PG_BAND", 0.10),
            pg_entry_min_remaining: env_or("PG_ENTRY_MIN_REMAINING", 180),
            pg_entry_deadline: env_or("PG_ENTRY_DEADLINE", 60),
            pg_pair_target: env_or("PG_PAIR_TARGET", 0.94),
            pg_require_rising: std::env::var("PG_REQUIRE_RISING")
                .map(|v| v != "false")
                .unwrap_or(true),
            pg_rising_lookback_s: env_or("PG_RISING_LOOKBACK_S", 10),

            gamma: env_or("AS_GAMMA", 0.1),
            kappa: env_or("AS_KAPPA", 1.5),
            our_size: env_or("OUR_SIZE", 50.0),
            reward_pool_per_min: env_or("REWARD_POOL_PER_MIN", 1.0),
            // 0.5¢ → quotes au touch (marchés ~1-2¢ de spread). Calibrable.
            base_half_spread_cents: env_or("BASE_HALF_SPREAD_CENTS", 0.5),

            bankroll_fraction: env_or("BANKROLL_FRACTION", 0.02),
            max_net_exposure_pct: env_or("MAX_NET_EXPOSURE_PCT", 0.15),
            min_cash_reserve_pct: env_or("MIN_CASH_RESERVE_PCT", 0.25),
            max_window_loss_pct: env_or("MAX_WINDOW_LOSS_PCT", 0.10),
            max_order_size: env_or("MAX_ORDER_SIZE", 100.0),
            max_position: env_or("MAX_POSITION", 500.0),
            max_net_shares: env_or("MAX_NET_SHARES", 40.0),
            paired_buy_margin: env_or("PAIRED_BUY_MARGIN", 0.01),
            flip_size: env_or("FLIP_SIZE", 40.0),

            maker_fill_prob: env_or("MAKER_FILL_PROB", 0.2),
            maker_only: env_or("MAKER_ONLY", true),

            kill_pause_secs: env_or("KILL_PAUSE_SECS", 5),
            panic_stop_secs: env_or("PANIC_STOP_SECS", 30),
            flatten_secs: env_or("FLATTEN_SECS", 20),

            start_cash: env_or("START_CASH", 100.0),
            state_path: env::var("STATE_PATH").unwrap_or_else(|_| "paper_state.json".into()),
            trades_path: env::var("TRADES_PATH").unwrap_or_else(|_| "paper_trades.jsonl".into()),

            min_merge_threshold: env_or("MIN_MERGE_THRESHOLD", 5.0),
            safety_mult: env_or("SAFETY_MULT", 3.0),
        }
    }
}
