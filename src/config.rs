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
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            dry_run: env_or("DRY_RUN", true),
            dashboard_port: env_or("PORT", 8768),

            binance_ws_url: env::var("BINANCE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms".into()),
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
        }
    }
}
