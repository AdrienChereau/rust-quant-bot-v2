//! Serveur de monitoring local (J9).
//!
//! Expose une petite API HTTP (sans framework lourd) sur `127.0.0.1:PORT` :
//!   - `GET /`            → dashboard (index.html)
//!   - `GET /style.css`, `/app.js`
//!   - `GET /state`       → snapshot JSON de l'état du bot
//! Les fichiers frontend sont embarqués à la compilation (binaire autonome).
//! Le frontend poll `/state` chaque seconde.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

const INDEX_HTML: &str = include_str!("../../frontend/index.html");
const RADAR_HTML: &str = include_str!("../../frontend/radar.html");
const RADAR_JS: &str = include_str!("../../frontend/radar.js");
const STYLE_CSS: &str = include_str!("../../frontend/style.css");
const APP_JS: &str = include_str!("../../frontend/app.js");

/// Snapshot partagé alimenté par les rôles (radar + exécuteur).
#[derive(Debug, Clone, Default, Serialize)]
pub struct DashboardState {
    pub dry_run: bool,
    /// Interrupteur manuel (bouton ON/OFF du dashboard) : false = aucune nouvelle
    /// quote ni assurance, ordres réels annulés — les positions vont à la résolution.
    pub trading_enabled: bool,
    pub role: String, // "radar" | "executor" — sélectionne l'interface servie sur /
    pub seq: u64,     // dernier seq de tick émis (radar)
    pub radar_log: Vec<(String, String)>, // (heure, événement) — ring 40 entrées
    // Radar
    pub binance_connected: bool,
    pub btc_micro: f64,
    pub obi: f64,
    pub kills_emitted: u64,
    // Exécuteur
    pub market_slug: String,
    pub remaining_s: i64,
    pub sigma: f64,
    pub fair: f64,
    pub drift: f64,    // drift log/s injecté dans p_up (correctif tendance)
    pub obi_exec: f64, // OBI carnet Binance côté exécuteur (skew de cotation)
    pub ofi: f64,      // OFI normalisé [-1,1] (flux d'ordres Binance)
    pub up_mid: f64,
    pub up_bid: f64,
    pub up_ask: f64,
    pub down_mid: f64,
    pub down_bid: f64,
    pub down_ask: f64,
    pub pulled_up: bool,   // bid Up retiré (côté qui décroche)
    pub pulled_down: bool, // bid Down retiré
    pub in_band: bool,
    #[serde(default)]
    pub signal_age_ms: i64, // latence : âge du dernier signal Tokyo (fraîcheur des données)
    #[serde(default)]
    pub open_orders: u32, // nombre de bids restants au carnet (échelle : 0-4)
    #[serde(default)]
    pub order_rtt_ms: u64, // RTT réel de la dernière requête CLOB (POST d'ordre ou poll) — Dublin ↔ serveur Polymarket
    #[serde(default)]
    pub audit_orders: u32, // ordres dont le statut terminal reste à confirmer
    #[serde(default)]
    pub audit_oldest_ms: i64,
    #[serde(default)]
    pub live_halted: bool,
    #[serde(default)]
    pub live_halt_reason: String,
    #[serde(default)]
    pub skew_side: String, // "" = symétrique · "up"/"down" = MM incliné côté fort (mode skew actif)
    // Spread-capture v5
    pub pair_cost: f64, // coût de paire blended courant (0 si un côté vide)
    #[serde(default)]
    pub merge_pair_avg: f64, // coût de paire moyen AU MOMENT des merges de la fenêtre (qualité d'exécution, cible ≈ 1,00)
    #[serde(default)]
    pub taker_fees_window: f64, // taxe taker payée cette fenêtre (7% × p(1−p) × taille par fill taker) — cible : 0
    #[serde(default)]
    pub merged_window: f64, // paires mergées dans la fenêtre COURANTE ($ recouvré ≈ paires)
    #[serde(default)]
    pub dir_wins: u32, // fenêtres où la conviction directionnelle FORTE de Tokyo a visé juste
    #[serde(default)]
    pub dir_total: u32, // fenêtres avec conviction forte (précision = wins/total ; l'edge Tokyo se juge ici)
    pub deployed: f64,        // $ déployés sur la fenêtre courante
    pub window_start: i64,    // unix s du début de la fenêtre courante (graphique)
    pub rebate_window: f64,   // rebate estimé de la fenêtre courante
    pub rebate_total: f64,    // rebate estimé cumulé depuis le lancement
    pub live_collateral: f64, // USDC réel du wallet (mode live, sync ≤10 s + fills)
    pub live_wallet_pnl: f64, // collatéral − baseline : le SEUL PnL qui fait foi en live
    pub size_factor: f64,     // disjoncteur de séries perdantes (1.0 / 0.25 / 0)
    pub loss_streak: u32,     // pertes consécutives en cours
    // Paramètres de stratégie réellement chargés (affichés dans le panneau « Stratégie »).
    pub params: StrategyParams,
    pub signals_received: u64,
    // Inventaire / PnL
    pub cash: f64,
    pub up_bal: f64,
    pub down_bal: f64,
    pub latent: f64,
    pub realized_pnl: f64,
    pub fills: u64,
    pub merges: u64,
    // Bankroll / risque (R4)
    pub equity: f64,
    pub position_value: f64,
    pub window_pnl: f64,
    pub drawdown: f64,
    pub net_exposure: f64,
    pub paused: bool,
    pub last_block_reason: String,
    pub sells: u64,
    pub maker_fills: u64,
    pub taker_fills: u64,
    // Carnet Up (quelques niveaux autour du mid) pour visualisation.
    pub book_bids: Vec<BookLevel>, // tri décroissant (meilleur en premier)
    pub book_asks: Vec<BookLevel>, // tri croissant
    // Séries temporelles (ring) pour les graphiques.
    pub series: Vec<SeriesPoint>,
    // Résultat réalisé par fenêtre résolue (rentabilité).
    pub windows: Vec<WindowResult>,
    // Chemin du journal de trades (pour l'endpoint /events) — non sérialisé.
    #[serde(skip)]
    pub trades_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

/// Point de série temporelle (1/s) pour le graphique de fenêtre (façon monitor).
#[derive(Debug, Clone, Default, Serialize)]
pub struct SeriesPoint {
    pub t: i64,        // unix ms
    pub up_mid: f64,   // share Up (vert)
    pub down_mid: f64, // share Down (rouge)
    pub spot: f64,     // BTC spot Binance (bleu, axe gauche)
    pub up_bid: f64,   // notre bid maker restant Up (0 si aucun)
    pub up_ask: f64,
    pub equity: f64,
    pub realized: f64,
    pub imb: f64, // imbalance de la fenêtre courante (courbe symlog)
}

/// Résultat d'une fenêtre résolue (format riche, aligné sur l'observatoire).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult {
    pub start: i64,
    pub res: String, // "Up" | "Down"
    pub fills: u32,
    pub avg_up: f64,
    pub avg_dn: f64,
    pub pair_cost: f64,
    pub imb_max: f64,
    pub imb_final: f64,
    pub deployed: f64,
    pub merged: f64,
    pub rebate: f64, // rebate maker estimé (rate × Σ 0,07·p(1−p)·taille)
    pub pnl: f64,    // PnL trading réalisé de la fenêtre (hors rebate)
    /// Attribution d'exécution par intention. Les PnL de règlement restent
    /// explicitement non attribués tant que les lots FIFO historiques ne sont
    /// pas disponibles (un chiffre inventé serait plus dangereux qu'un zéro).
    #[serde(default)]
    pub purposes: PurposeBreakdown,
    #[serde(default)]
    pub pnl_unattributed: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExecutionPurposeStats {
    pub fills: u32,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub taker_fees: f64,
    pub maker_rebate: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PurposeBreakdown {
    pub symmetric_open: ExecutionPurposeStats,
    pub completion: ExecutionPurposeStats,
    pub skew_accumulation: ExecutionPurposeStats,
    pub rescue: ExecutionPurposeStats,
    pub flatten: ExecutionPurposeStats,
}

impl PurposeBreakdown {
    #[cfg_attr(not(feature = "live"), allow(dead_code))]
    pub fn for_order_intent(&mut self, intent: &str) -> &mut ExecutionPurposeStats {
        match intent {
            "completion" => &mut self.completion,
            "skew_accumulation" => &mut self.skew_accumulation,
            "rescue" => &mut self.rescue,
            _ => &mut self.symmetric_open,
        }
    }
}

/// Paramètres de stratégie chargés au démarrage (source : .env / défauts).
#[derive(Debug, Clone, Default, Serialize)]
pub struct StrategyParams {
    pub c_raw: f64,
    pub fee_per_pair: f64,
    pub opening_leg_max: f64,
    pub gate_margin: f64,
    pub max_imbalance: f64,
    pub base_clip: f64,
    pub max_clip: f64,
    pub depth_gain: f64,
    pub max_clip_usdc: f64,
    pub max_capital_per_market: f64,
    pub min_seconds: i64,
    pub clip_interval_s: i64,
    pub min_window_age_s: i64,
    pub completion_reserve: f64,
    pub drift_horizon_s: f64,
    // v7
    pub trend_filter: bool,
    pub pullback_s: i64,
    pub completion_max_price: f64,
    pub completion_max_pair: f64,
    pub drift_halflife_secs: f64,
    pub drift_clamp_k: f64,
    pub volatility_floor: f64,
}

/// Renvoie les `max` dernières lignes du journal JSONL comme tableau JSON.
fn tail_json(path: &str, max: usize) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return "[]".into();
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let read_from = len.saturating_sub(64 * 1024);
    if f.seek(SeekFrom::Start(read_from)).is_err() {
        return "[]".into();
    }
    let mut buf = String::new();
    if f.read_to_string(&mut buf).is_err() {
        return "[]".into();
    }
    let mut lines: Vec<&str> = buf.lines().filter(|l| !l.trim().is_empty()).collect();
    // Si on a démarré au milieu du fichier, la 1re ligne est probablement tronquée.
    if read_from > 0 && !lines.is_empty() {
        lines.remove(0);
    }
    let start = lines.len().saturating_sub(max);
    format!("[{}]", lines[start..].join(","))
}

pub type Shared = Arc<RwLock<DashboardState>>;

pub fn shared(dry_run: bool, role: &str) -> Shared {
    // SÉCURITÉ DE DÉPLOIEMENT (demande du 8 juil.) : en LIVE, le bot démarre
    // TOUJOURS sur OFF — c'est un humain qui clique ON après avoir vérifié le
    // déploiement. Le paper, lui, démarre ON (aucun risque, données continues).
    let trading_enabled = dry_run;
    if !dry_run {
        tracing::warn!(
            "LIVE démarré interrupteur OFF — activer le trading via le bouton ON du dashboard"
        );
    }
    Arc::new(RwLock::new(DashboardState {
        dry_run,
        trading_enabled,
        role: role.into(),
        ..Default::default()
    }))
}

/// Journal d'événements radar (ring 40) — affiché en bas à droite de l'UI Tokyo.
pub fn radar_log(d: &mut DashboardState, msg: impl Into<String>) {
    let t = chrono::Utc::now().format("%H:%M:%S").to_string();
    d.radar_log.push((t, msg.into()));
    let n = d.radar_log.len();
    if n > 40 {
        d.radar_log.drain(0..n - 40);
    }
}

/// Lance le serveur HTTP de monitoring (boucle infinie).
pub async fn serve(port: u16, state: Shared) -> anyhow::Result<()> {
    // DASH_BIND=0.0.0.0 pour l'accès distant (Tailscale) ; défaut local-only.
    let bind = std::env::var("DASH_BIND").unwrap_or_else(|_| "127.0.0.1".into());
    let listener = TcpListener::bind((bind.as_str(), port)).await?;
    tracing::info!(port, bind, "Dashboard sur http://{bind}:{port}");

    loop {
        let (mut sock, _) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::debug!(error = %e, "accept");
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let Ok(n) = sock.read(&mut buf).await else {
                return;
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .split('?')
                .next()
                .unwrap_or("/");

            let (ctype, body) = match path {
                "/" | "/index.html" => {
                    // Le rôle choisit l'interface : Tokyo a la sienne.
                    let radar = state.read().await.role == "radar";
                    (
                        "text/html; charset=utf-8",
                        if radar {
                            RADAR_HTML.to_string()
                        } else {
                            INDEX_HTML.to_string()
                        },
                    )
                }
                "/style.css" => ("text/css; charset=utf-8", STYLE_CSS.to_string()),
                "/app.js" => ("application/javascript; charset=utf-8", APP_JS.to_string()),
                "/radar.js" => (
                    "application/javascript; charset=utf-8",
                    RADAR_JS.to_string(),
                ),
                "/start" | "/stop" => {
                    let on = path == "/start";
                    state.write().await.trading_enabled = on;
                    tracing::warn!(trading_enabled = on, "interrupteur manuel dashboard");
                    ("application/json", format!("{{\"trading_enabled\":{on}}}"))
                }
                "/state" => {
                    let s = state.read().await;
                    (
                        "application/json",
                        serde_json::to_string(&*s).unwrap_or_else(|_| "{}".into()),
                    )
                }
                "/logs" => {
                    let lines = crate::logbuf::tail(250);
                    (
                        "application/json",
                        serde_json::to_string(&lines).unwrap_or_else(|_| "[]".into()),
                    )
                }
                "/events" => {
                    let tp = { state.read().await.trades_path.clone() };
                    ("application/json", tail_json(&tp, 250))
                }
                _ => ("text/plain", "not found".to_string()),
            };
            let status = if path == "/state"
                || path == "/start"
                || path == "/stop"
                || path == "/logs"
                || path == "/events"
                || path == "/"
                || path == "/index.html"
                || path == "/style.css"
                || path == "/app.js"
                || path == "/radar.js"
            {
                "200 OK"
            } else {
                "404 Not Found"
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store, must-revalidate\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
        });
    }
}
