//! Tâche de polling Polymarket partagée (modes `mono` et `executor`).
//!
//! (Re)résout le marché BTC 5 min courant et polle les carnets Up/Down en fallback REST (1 s).
//! Le WS market prend le relai quand il est actif (`last_ws_ts_ms` récent).
//!
//! `fetch_strike` distingue les deux appelants :
//!   - `mono`     : calcule le fair localement (Black-Scholes) → a besoin du strike (`true`) ;
//!   - `executor` : reçoit le fair dans le paquet radar → pas besoin du strike (`false`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use super::relayer::{btc_price_at_window_open, PolymarketClient};
use crate::polymarket::live_executor::LiveCredentials;

pub use super::relayer::PmShared;

/// Lance la tâche de polling Polymarket en arrière-plan (1 s).
///
/// `ws_tx` : sender renvoyé par `init_market_ws()` — au rollover marché, envoie les nouveaux
/// tokens pour que le WS resouscrive in-session (une seule task WS active).
///
/// `live_creds` : si fourni, `preload_token_meta` est appelé à chaque rollover (cache Phase 1).
pub fn spawn_pm_poller(
    pm: Arc<Mutex<PmShared>>,
    fetch_strike: bool,
    ws_tx: Option<watch::Sender<Vec<String>>>,
    #[allow(unused_variables)] live_creds: Option<LiveCredentials>,
    ws_stale_threshold_ms: u64,
) {
    tokio::spawn(async move {
        let client = PolymarketClient::new();
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        loop {
            poll.tick().await;
            let need = { let g = pm.lock().unwrap(); g.market.as_ref().map_or(true, |m| m.time_remaining_sec() <= 0) };
            if need {
                if let Ok(Some(m)) = client.get_current_btc_5m_market().await {
                    let strike = if fetch_strike { btc_price_at_window_open(m.window_ts).await.ok() } else { None };
                    tracing::info!(slug = %m.slug, strike = ?strike, neg_risk = m.neg_risk, "=== nouveau marché ===");
                    // Pré-charge neg_risk+tick_size dans TOKEN_META (cache Phase 1).
                    #[cfg(feature = "live")]
                    if let Some(ref creds) = live_creds {
                        let ids = [m.up_token_id.as_str(), m.down_token_id.as_str()];
                        if let Err(e) = crate::polymarket::poly1271::preload_token_meta(creds, &ids).await {
                            tracing::warn!(error = %e, "preload_token_meta échoué");
                        }
                    }
                    // Notifie le WS market des nouveaux tokens (resouscription in-session).
                    if let Some(ref tx) = ws_tx {
                        let tokens = vec![m.up_token_id.clone(), m.down_token_id.clone()];
                        let _ = tx.send(tokens);
                    }
                    let mut g = pm.lock().unwrap();
                    g.market = Some(m); g.strike = strike;
                }
            }
            let (up_tok, dn_tok, win, last_ws_ts_ms) = {
                let g = pm.lock().unwrap();
                match &g.market {
                    Some(m) => (m.up_token_id.clone(), m.down_token_id.clone(), m.window_ts, g.last_ws_ts_ms),
                    None => continue,
                }
            };
            // Retry strike si manquant (mode mono uniquement).
            if fetch_strike && pm.lock().unwrap().strike.is_none() {
                if let Ok(s) = btc_price_at_window_open(win).await { pm.lock().unwrap().strike = Some(s); }
            }
            // Fallback REST carnets — skip si WS est récent (< 2 s).
            let now_ms = chrono::Utc::now().timestamp_millis() as u64;
            let ws_fresh = last_ws_ts_ms > 0 && now_ms.saturating_sub(last_ws_ts_ms) < ws_stale_threshold_ms;
            if !ws_fresh {
                let up = client.get_book(&up_tok).await.ok();
                let dn = client.get_book(&dn_tok).await.ok();
                let mut g = pm.lock().unwrap();
                if let Some(up) = up {
                    g.real_up = up.mid().unwrap_or(g.real_up);
                    g.up_best_bid = up.best_bid().unwrap_or(0.0);
                    g.up_best_ask = up.best_ask().unwrap_or(1.0);
                    g.up_book = Arc::new(up);
                }
                if let Some(dn) = dn {
                    g.down_best_bid = dn.best_bid().unwrap_or(0.0);
                    g.down_best_ask = dn.best_ask().unwrap_or(1.0);
                    g.down_book = Arc::new(dn);
                }
            }
            pm.lock().unwrap().remaining_s = pm.lock().unwrap().market.as_ref()
                .map(|m| m.time_remaining_sec()).unwrap_or(0);
        }
    });
}
