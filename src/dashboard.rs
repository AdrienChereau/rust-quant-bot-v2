//! Monitoring local (P6) — HTTP `/state` (JSON) + frontend embarqué.
//! Tourne sur une tâche séparée lisant un snapshot partagé → **zéro impact** sur
//! le hot-loop (OBI 50 ms + FSM).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::state::RuntimeControls;

const INDEX_HTML: &str = include_str!("../frontend/index.html");
const STYLE_CSS: &str = include_str!("../frontend/style.css");
const APP_JS: &str = include_str!("../frontend/app.js");

#[derive(Debug, Clone, Default, Serialize)]
pub struct DashState {
    pub dry_run: bool,
    /// Type de nœud : "live" | "paper" | "radar" | "mono" — pilote l'affichage du frontend.
    pub node_kind: String,
    // Radar
    pub binance_connected: bool,
    pub okx_connected: bool,
    pub btc_spot: f64,
    pub obi_binance: f64,
    pub obi_okx: f64,
    pub obi_consolidated: f64,
    pub agreement: bool,
    pub velocity: f64,
    // Sniper
    pub fsm_state: String,    // IDLE/ARMING/COOLDOWN
    pub market_slug: String,
    pub remaining_s: i64,
    pub fair_up: f64,
    pub real_up: f64,
    pub gap: f64,
    pub liquidity_vacuum: bool,
    // Conditions de tir (checklist AND)
    pub cond_agreement: bool, // accord OBI Binance+OKX
    pub cond_persist: bool,   // FSM en ARMING (persistance en cours/atteinte)
    pub cond_velocity: bool,  // vélocité confirme le sens
    pub cond_gap: bool,       // |fair − real| ≥ seuil
    pub cond_ready: bool,     // pas cooldown / vacuum / fin de fenêtre
    pub all_conditions: bool, // les 5 réunies
    // Position / PnL
    pub in_position: bool,
    pub pos_side: String,
    pub pos_entry: f64,
    pub pos_tp: f64,
    pub pos_sl: f64,
    pub cash: f64,
    pub equity: f64,
    pub realized_pnl: f64,
    pub drawdown: f64,
    pub shots: u64,
    pub wins: u64,
    pub losses: u64,
    pub hit_rate: f64,
    pub kelly_size: f64,
    // Latences TCP (ms) vers les exchanges — None = pas encore mesuré / timeout
    pub lat_binance_ms:    Option<f64>,
    pub lat_okx_ms:        Option<f64>,
    pub lat_polymarket_ms: Option<f64>,
    // Contrôle d'exécution (reflète RuntimeControls + config) — live testing
    pub mode: String,           // PAPER / PAUSE / LIVE / BREAKER
    pub paper_paused: bool,
    pub live_enabled: bool,
    pub live_paused: bool,
    pub live_armed: bool,       // verrou matériel d'envoi réel (env LIVE_ARMED)
    pub breaker_tripped: bool,
    pub initial_capital: f64,
    pub max_drawdown: f64,
    pub live_bankroll: Option<f64>, // vraie collatéral USDC (CLOB) — None si pas encore lue
    pub live_pnl: Option<f64>,      // PnL réalisé live (Δ bankroll depuis activation) — None hors live
    pub live_shots: u64,            // ordres live acceptés cette session
    pub live_force_min: bool,       // LIVE_FORCE_MIN_SIZE : taille minimale forcée (agressif)
    // Métriques latence ordre live (ms)
    pub lat_last_buy_ms: Option<u64>,   // BUY FAK : début POST → réponse CLOB
    pub lat_last_sell_ms: Option<u64>,  // SELL FAK : début POST → réponse CLOB
    pub pm_ws_stale_ms: Option<u64>,    // now − last_ws_ts_ms (Phase 2+)
    // Latence TOTALE pipeline signal→ordre (ms) — décomposée par leg + somme.
    pub lat_transport_ms: Option<u64>,  // radar→nœud (sent_ms → recv) — requiert NTP sync
    pub lat_decide_ms:    Option<u64>,  // recv UDP → soumission OrderEngine (mono-horloge)
    pub lat_post_ms:      Option<u64>,  // POST CLOB round-trip (du dernier ordre)
    pub lat_total_ms:     Option<u64>,  // transport + decide + post
}

pub type Shared = Arc<RwLock<DashState>>;

/// Construit l'état partagé du dashboard. `node_kind` ∈ {"live","paper","radar","mono"} pilote
/// l'affichage côté frontend (un nœud = une vue).
pub fn shared(dry_run: bool, node_kind: &str) -> Shared {
    Arc::new(RwLock::new(DashState { dry_run, node_kind: node_kind.to_string(), ..Default::default() }))
}

pub async fn serve(port: u16, state: Shared, controls: Arc<RuntimeControls>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "Dashboard sur http://0.0.0.0:{port}");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else { continue };
        let state = state.clone();
        let controls = controls.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let Ok(n) = sock.read(&mut buf).await else { return };
            let req = String::from_utf8_lossy(&buf[..n]);
            let mut tokens = req.split_whitespace();
            let method = tokens.next().unwrap_or("GET");
            let path = tokens.next().unwrap_or("/").split('?').next().unwrap_or("/");

            // Endpoints de contrôle (POST) — mutent les atomics lock-free.
            if method == "POST" {
                let node_kind = state.read().await.node_kind.clone();
                let ok = handle_control(path, &controls, &node_kind);
                let body = format!("{{\"ok\":{ok},\"mode\":\"{}\"}}", controls.mode_label());
                let _ = sock.write_all(http_resp("application/json", &body).as_bytes()).await;
                let _ = sock.flush().await;
                return;
            }

            let (ctype, body) = match path {
                "/" | "/index.html" => ("text/html; charset=utf-8", INDEX_HTML.to_string()),
                "/style.css" => ("text/css; charset=utf-8", STYLE_CSS.to_string()),
                "/app.js" => ("application/javascript; charset=utf-8", APP_JS.to_string()),
                "/state" => ("application/json", serde_json::to_string(&*state.read().await).unwrap_or_else(|_| "{}".into())),
                _ => ("text/plain", "not found".to_string()),
            };
            let _ = sock.write_all(http_resp(ctype, &body).as_bytes()).await;
            let _ = sock.flush().await;
        });
    }
}

/// Applique un endpoint de contrôle. Renvoie `true` si l'action est reconnue.
///
/// Start/Stop = **pause logicielle** (le process et les WebSockets restent chauds). La sémantique
/// dépend du `node_kind` :
/// - nœud `paper` → `paper_paused`.
/// - nœud `live`  → `live_paused` (le live reste *enabled* ; le verrou `LIVE_ARMED` env reste requis
///   pour l'envoi réel).
///
/// Les endpoints legacy `/mode/paper` `/mode/live` restent utilisables par le nœud `mono`.
fn handle_control(path: &str, c: &RuntimeControls, node_kind: &str) -> bool {
    match path {
        // Start/Stop génériques (pause logicielle) — sémantique selon le nœud.
        "/start" => match node_kind {
            "live" => {
                c.live_enabled.store(true, Ordering::Relaxed);
                c.live_paused.store(false, Ordering::Relaxed);
                true
            }
            _ => { c.paper_paused.store(false, Ordering::Relaxed); true }
        },
        "/stop" => match node_kind {
            "live" => { c.live_paused.store(true, Ordering::Relaxed); true }
            _ => { c.paper_paused.store(true, Ordering::Relaxed); true }
        },
        // Legacy (mono) : bascule PAPER ⇄ LIVE dans un même process.
        "/mode/paper" => {
            c.live_enabled.store(false, Ordering::Relaxed);
            c.live_paused.store(true, Ordering::Relaxed);
            c.paper_paused.store(false, Ordering::Relaxed);
            true
        }
        "/mode/live" => {
            c.live_enabled.store(true, Ordering::Relaxed);
            c.live_paused.store(false, Ordering::Relaxed);
            true
        }
        // Réarme après un déclenchement du circuit breaker (drawdown).
        "/breaker/reset" => { c.breaker_tripped.store(false, Ordering::Relaxed); true }
        _ => false,
    }
}

fn http_resp(ctype: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
