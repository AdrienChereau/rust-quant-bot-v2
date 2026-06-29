//! Configuration du sniper, chargée depuis l'environnement (`.env`).

use std::env;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dry_run: bool,
    pub dashboard_port: u16,

    // Flux marché
    pub binance_ws_url: String,
    pub okx_ws_url: String,

    // OBI (P2/P3)
    pub obi_band_pct: f64,           // bande % autour du mid (0.0005 = 0.05 %)
    pub obi_top_n: usize,            // top-N niveaux BBO (0 = mode bande)
    pub obi_fire_threshold: f64,     // seuil de la magnitude consolidée
    pub obi_floor_per_exchange: f64, // chaque exchange doit dépasser ce floor
    pub weight_binance: f64,         // 0.65
    pub weight_okx: f64,             // 0.35

    // FSM sniper (P4)
    pub obi_dwell_ms: u64,           // persistance avant tir
    pub cooldown_ms: u64,
    pub gap_min: f64,                // |fair − real| minimal
    pub velocity_confirm: f64,       // |ΔP_1s| minimal (0 = désactivé)

    // Défensif (P4)
    pub vacuum_velocity: f64,        // ΔP_1s ≤ seuil → vide de liquidité
    pub vacuum_obi: f64,
    pub end_window_block_secs: i64,

    // Bankroll / Kelly (P5)
    pub start_cash: f64,
    pub kelly_fraction: f64,
    pub max_kelly_size_pct: f64,     // plafond taille / equity
    pub take_profit_cents: f64,
    pub stop_loss_cents: f64,
    pub max_hold_secs: i64,

    // Live testing (passage paper → réel)
    pub max_drawdown: f64,     // circuit breaker sur l'equity (en $)
    pub live_armed: bool,      // LIVE_ARMED : verrou matériel pour l'envoi RÉEL d'ordres
    pub live_force_min_size: bool, // LIVE_FORCE_MIN_SIZE : ignore Kelly, force la taille minimale
                                   // (agressif — micro-test plomberie sur petite bankroll)
    pub fixed_order_usd: f64,      // FIXED_ORDER_USD > 0 : ignore Kelly, force un notionnel fixe ($)
                                   // à chaque tir (plancher = minimum d'échange). Tests/comparaison.
    pub exit_buffer: f64,          // EXIT_BUFFER : marge sous le bid pour les sorties SL/max_hold
                                   // (garantit le fill de la vente ; la FAK price-improve).

    // Infrastructure live (Bloc D)
    pub pm_ws_stale_threshold_ms: u64, // skip REST book si WS < ce seuil (ms)
    pub bankroll_poll_secs: u64,       // fréquence refresh bankroll CLOB
    pub order_engine_queue: usize,     // capacité mpsc OrderEngine

    // Signal stack v2 (Étapes 1-10)
    pub obi_multilevel_lambda: f64,   // OBI_MULTILEVEL_LAMBDA=0.5
    pub score_fire_threshold: f64,    // SCORE_FIRE_THRESHOLD=0.35
    pub d2_gamma: f64,                // D2_GAMMA=0.50
    pub agg_trade_ws_url: String,     // AGG_TRADE_WS_URL
    pub tfi_window_ms: u64,           // TFI_WINDOW_MS=5000
    pub basis_threshold_usd: f64,     // BASIS_THRESHOLD_USD=20.0
    pub basis_stale_ms: u64,          // BASIS_STALE_MS=80
    pub basis_lambda: f64,            // BASIS_LAMBDA=0.90
    pub kalman_q00: f64,              // KALMAN_Q00=0.09
    pub kalman_q11: f64,              // KALMAN_Q11=0.01
    pub kalman_spike_sigma: f64,      // KALMAN_SPIKE_SIGMA=5.0
    pub kalman_reset_after_n: u32,    // KALMAN_RESET_AFTER_N=10
    pub kalman_r: f64,                // KALMAN_R=25.0
    pub vel_norm_factor: f64,         // VEL_NORM_FACTOR=5.0
    pub composite_w_obi: f64,         // COMPOSITE_W_OBI=0.40
    pub composite_w_tfi: f64,         // COMPOSITE_W_TFI=0.30
    pub composite_w_kalman: f64,      // COMPOSITE_W_KALMAN=0.20
    pub composite_w_basis: f64,       // COMPOSITE_W_BASIS=0.10
    pub ewma_lambda: f64,             // EWMA_LAMBDA=0.94
    pub ewma_score_lambda: f64,       // EWMA_SCORE_LAMBDA=0.9995
    pub kelly_price_max: f64,         // KELLY_PRICE_MAX=0.90
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            dry_run: env_or("DRY_RUN", true),
            dashboard_port: env_or("PORT", 8768),

            binance_ws_url: env::var("BINANCE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443/ws/btcusdt@depth".into()),
            okx_ws_url: env::var("OKX_WS_URL")
                .unwrap_or_else(|_| "wss://ws.okx.com:8443/ws/v5/public".into()),

            obi_band_pct: env_or("OBI_BAND_PCT", 0.0005),
            obi_top_n: env_or("OBI_TOP_N", 10usize),
            obi_fire_threshold: env_or("OBI_FIRE_THRESHOLD", 0.20),
            obi_floor_per_exchange: env_or("OBI_FLOOR_PER_EXCHANGE", 0.20),
            weight_binance: env_or("WEIGHT_BINANCE", 0.65),
            weight_okx: env_or("WEIGHT_OKX", 0.35),

            obi_dwell_ms: env_or("OBI_DWELL_MS", 0),
            cooldown_ms: env_or("COOLDOWN_MS", 3000),
            gap_min: env_or("GAP_MIN", 0.02),
            velocity_confirm: env_or("VELOCITY_CONFIRM", 0.0),

            vacuum_velocity: env_or("VACUUM_VELOCITY", -0.0010),
            vacuum_obi: env_or("VACUUM_OBI", -0.40),
            end_window_block_secs: env_or("END_WINDOW_BLOCK_SECS", 60),

            start_cash: env_or("START_CASH", 200.0),
            kelly_fraction: env_or("KELLY_FRACTION", 0.5),
            max_kelly_size_pct: env_or("MAX_KELLY_SIZE_PCT", 0.02),
            take_profit_cents: env_or("TAKE_PROFIT_CENTS", 4.0),
            stop_loss_cents: env_or("STOP_LOSS_CENTS", 3.0),
            max_hold_secs: env_or("MAX_HOLD_SECS", 60),

            max_drawdown: env_or("MAX_DRAWDOWN", 20.0),
            live_armed: env_or("LIVE_ARMED", false),
            live_force_min_size: env_or("LIVE_FORCE_MIN_SIZE", false),
            fixed_order_usd: env_or("FIXED_ORDER_USD", 0.0),
            exit_buffer: env_or("EXIT_BUFFER", 0.02),

            pm_ws_stale_threshold_ms: env_or("PM_WS_STALE_THRESHOLD_MS", 2000u64),
            bankroll_poll_secs: env_or("BANKROLL_POLL_SECS", 10u64),
            order_engine_queue: env_or("ORDER_ENGINE_QUEUE", 8usize),

            obi_multilevel_lambda: env_or("OBI_MULTILEVEL_LAMBDA", 0.5f64),
            score_fire_threshold: env_or("SCORE_FIRE_THRESHOLD", 0.35f64),
            d2_gamma: env_or("D2_GAMMA", 0.50f64),
            agg_trade_ws_url: env::var("AGG_TRADE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443/ws/btcusdt@aggTrade".into()),
            tfi_window_ms: env_or("TFI_WINDOW_MS", 5000u64),
            basis_threshold_usd: env_or("BASIS_THRESHOLD_USD", 20.0f64),
            basis_stale_ms: env_or("BASIS_STALE_MS", 80u64),
            basis_lambda: env_or("BASIS_LAMBDA", 0.90f64),
            kalman_q00: env_or("KALMAN_Q00", 0.09f64),
            kalman_q11: env_or("KALMAN_Q11", 0.01f64),
            kalman_spike_sigma: env_or("KALMAN_SPIKE_SIGMA", 5.0f64),
            kalman_reset_after_n: env_or("KALMAN_RESET_AFTER_N", 10u32),
            kalman_r: env_or("KALMAN_R", 25.0f64),
            vel_norm_factor: env_or("VEL_NORM_FACTOR", 5.0f64),
            composite_w_obi: env_or("COMPOSITE_W_OBI", 0.40f64),
            composite_w_tfi: env_or("COMPOSITE_W_TFI", 0.30f64),
            composite_w_kalman: env_or("COMPOSITE_W_KALMAN", 0.20f64),
            composite_w_basis: env_or("COMPOSITE_W_BASIS", 0.10f64),
            ewma_lambda: env_or("EWMA_LAMBDA", 0.94f64),
            ewma_score_lambda: env_or("EWMA_SCORE_LAMBDA", 0.9995f64),
            kelly_price_max: env_or("KELLY_PRICE_MAX", 0.90f64),
        }
    }
}
