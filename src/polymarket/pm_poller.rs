//! Tâche de polling Polymarket partagée (modes `mono` et `executor`).
//!
//! (Re)résout le marché BTC 5 min courant et polle les carnets Up/Down toutes les 1 s.
//! `fetch_strike` distingue les deux appelants :
//!   - `mono`     : calcule le fair localement (Black-Scholes) → a besoin du strike (`true`) ;
//!   - `executor` : reçoit le fair dans le paquet radar → pas besoin du strike (`false`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::relayer::{btc_price_at_window_open, Market, PolyBook, PolymarketClient};

/// État Polymarket partagé, alimenté par la tâche de polling et lu par la hot-loop.
#[derive(Default)]
pub struct PmShared {
    pub market: Option<Market>,
    pub strike: Option<f64>, // ignoré par l'exécuteur (fair reçu par UDP)
    pub real_up: f64,
    pub up_book: PolyBook,
    pub down_book: PolyBook,
    pub remaining_s: i64,
}

/// Lance la tâche de polling Polymarket en arrière-plan (1 s).
pub fn spawn_pm_poller(pm: Arc<Mutex<PmShared>>, fetch_strike: bool) {
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
                    let mut g = pm.lock().unwrap();
                    g.market = Some(m); g.strike = strike;
                }
            }
            let (up_tok, dn_tok, win) = { let g = pm.lock().unwrap();
                match &g.market { Some(m) => (m.up_token_id.clone(), m.down_token_id.clone(), m.window_ts), None => continue } };
            // Retry strike si manquant (mode mono uniquement).
            if fetch_strike && pm.lock().unwrap().strike.is_none() {
                if let Ok(s) = btc_price_at_window_open(win).await { pm.lock().unwrap().strike = Some(s); }
            }
            let up = client.get_book(&up_tok).await.ok();
            let dn = client.get_book(&dn_tok).await.ok();
            let mut g = pm.lock().unwrap();
            if let Some(up) = up { g.real_up = up.mid().unwrap_or(g.real_up); g.up_book = up; }
            if let Some(dn) = dn { g.down_book = dn; }
            g.remaining_s = g.market.as_ref().map(|m| m.time_remaining_sec()).unwrap_or(0);
        }
    });
}
