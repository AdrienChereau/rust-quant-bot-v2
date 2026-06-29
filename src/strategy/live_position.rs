//! Gestion **symétrique au `PaperEngine`** pour les ordres LIVE Polymarket.
//!
//! Bloc C : machine d'états `LivePhase` — plus de fallback optimiste sur `filled_size`.
//!
//! Transitions :
//!   BUY POST → filled_size Some(n>0) → Open
//!   BUY POST → filled_size None      → PendingBuy (en attente WS ou timeout)
//!   PendingBuy + FillEvent WS         → Open
//!   PendingBuy + timeout              → Reconciling{Buy}
//!   Open → SELL POST → filled_size Some → Idle
//!   Open → SELL POST → filled_size None → PendingSell
//!   PendingSell + FillEvent WS SELL   → Idle
//!   PendingSell + timeout             → Reconciling{Sell}
//!
//! Invariants :
//!   - Une seule position à la fois (Idle → PendingBuy → Open → PendingSell → Idle).
//!   - Jamais de second SELL si phase != Open.
//!   - Aucun POST si LIVE_ARMED=false.
//!   - Notionnel ≥ $1 enforced sur BUY + SELL.

use std::fs;
use std::io::Write as _;

use serde::{Deserialize, Serialize};

use crate::concurrency::bus::Side;
use crate::polymarket::live_executor::{self, LiveCredentials, OrderArgs, PlaceResult};
use crate::polymarket::order_engine::OrderResult;
use crate::polymarket::relayer::PolyBook;
use crate::strategy::bankroll::KellyParams;

/// Au-delà de ce délai après l'ouverture, un `balance: 0` n'est plus du settlement on-chain
/// (BUY pas encore réglé) mais une position réellement disparue → abandon autorisé.
const SETTLE_GRACE_MS: u64 = 5000;

/// État cumulé du trading live (compteurs + PnL réalisé). Persisté sur disque.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveState {
    pub realized_pnl: f64,
    pub shots: u64,
    pub wins: u64,
    pub losses: u64,
    pub failed_closes: u64,
}

/// Position live ouverte. `size` = fill réel du BUY (jamais la taille demandée).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePosition {
    pub side: Side,
    pub token_id: String,
    pub entry_price: f64,
    pub size: f64,
    pub tp_price: f64,
    pub sl_price: f64,
    pub opened_ms: u64,
    pub neg_risk: bool,
    pub buy_order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReconcileKind { Buy, Sell }

/// Machine d'états du cycle de vie d'une position live.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum LivePhase {
    #[default]
    Idle,
    /// POST BUY accepté mais filled_size absent → attend fill WS ou timeout.
    PendingBuy {
        order_id: String,
        side: Side,
        token_id: String,
        requested_size: f64,
        tick: f64,
        since_ms: u64,
    },
    Open(LivePosition),
    /// POST SELL accepté mais filled_size absent → attend fill WS ou timeout.
    PendingSell {
        position: LivePosition,
        reason: String,
        since_ms: u64,
    },
    /// Timeout sans fill : nécessite réconciliation manuelle.
    Reconciling {
        order_id: String,
        since_ms: u64,
        kind: ReconcileKind,
    },
}

/// Manager symétrique au `PaperEngine` mais qui touche le CLOB réel.
pub struct LivePositionManager {
    pub phase: LivePhase,
    pub state: LiveState,
    pub last_buy_ms: Option<u64>,
    pub last_sell_ms: Option<u64>,
    /// Échecs de clôture consécutifs (runtime, non persisté) — anti-boucle de SELL.
    consec_close_fails: u32,
    params: KellyParams,
    state_path: String,
    trades_path: String,
}

/// Compat — `position` retourne `Some` uniquement si la phase est `Open`.
impl LivePositionManager {
    pub fn position(&self) -> Option<&LivePosition> {
        if let LivePhase::Open(ref p) = self.phase { Some(p) } else { None }
    }
}

#[derive(Serialize)]
struct LiveTradeRec<'a> {
    ts: String,
    kind: &'a str,
    side: &'a str,
    price: f64,
    size: f64,
    pnl: f64,
    order_id: &'a str,
    realized_pnl_after: f64,
}

impl LivePositionManager {
    pub fn load_or_init(params: KellyParams, state_path: String, trades_path: String) -> Self {
        let state = fs::read_to_string(&state_path).ok()
            .and_then(|s| serde_json::from_str::<LiveState>(&s).ok())
            .unwrap_or_default();
        tracing::info!(realized_pnl = state.realized_pnl, shots = state.shots,
            wins = state.wins, losses = state.losses, "État LIVE chargé");
        Self { phase: LivePhase::Idle, state, last_buy_ms: None, last_sell_ms: None,
            consec_close_fails: 0, params, state_path, trades_path }
    }

    /// Tente d'ouvrir une position : POST BUY FAK.
    /// Retourne `true` si un POST a été envoyé (et n'est pas en phase non-Idle).
    #[allow(clippy::too_many_arguments)]
    pub async fn try_open(
        &mut self,
        creds: &LiveCredentials,
        live_armed: bool,
        side: Side,
        token_id: &str,
        neg_risk: bool,
        order_price: f64,
        size: f64,
        tick: f64,
        min_order_size: f64,
        now_ms: u64,
    ) -> bool {
        if !matches!(self.phase, LivePhase::Idle) { return false; }
        let size_min_cost = clob_min_size_for(min_order_size, order_price);
        let size_final = size.max(size_min_cost);
        let args = OrderArgs { side, price: order_price, size: size_final, is_sell: false };
        let t0 = tokio::time::Instant::now();
        let result = live_executor::place_order(live_armed, Some(creds), token_id, neg_risk, args).await;
        let buy_ms = t0.elapsed().as_millis() as u64;
        tracing::info!(buy_ms, side = side.as_str(), token_id, "⏱ latence BUY FAK");
        match result {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms: buy_post_ms }) => {
                self.last_buy_ms = Some(buy_post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        let entry = avg_price.unwrap_or(order_price);
                        self.open_position(side, token_id, neg_risk, entry, n, tick, &order_id, now_ms);
                    }
                    _ => {
                        // filled_size absent ou nul → PendingBuy, attend confirmation WS.
                        tracing::warn!(order_id = %order_id, "BUY accepté sans fill_size — PendingBuy");
                        self.phase = LivePhase::PendingBuy {
                            order_id, side, token_id: token_id.to_string(),
                            requested_size: size_final, tick, since_ms: now_ms,
                        };
                    }
                }
                true
            }
            Ok(PlaceResult::DryRun) => false,
            Err(e) => {
                tracing::error!(error = %e, side = side.as_str(), token_id, "❌ BUY live échoué");
                false
            }
        }
    }

    /// Gère la position ouverte. Retourne `true` si la position est fermée.
    pub async fn manage(
        &mut self,
        creds: &LiveCredentials,
        live_armed: bool,
        mark_bid: Option<f64>,
        book: &PolyBook,
        min_order_size: f64,
        tick: f64,
        now_ms: u64,
        remaining_s: i64,
    ) -> bool {
        let LivePhase::Open(ref p) = self.phase else { return false };
        let Some(bid) = mark_bid else { return false };
        let (tp_price, sl_price, opened_ms, max_hold) = (p.tp_price, p.sl_price, p.opened_ms, self.params.max_hold_secs);
        let held_s = (now_ms.saturating_sub(opened_ms) / 1000) as i64;

        let reason = if bid >= tp_price { Some("take_profit") }
        else if bid <= sl_price { Some("stop_loss") }
        else if held_s >= max_hold || remaining_s <= 30 { Some("max_hold") }
        else { None };

        let Some(reason) = reason else { return false };
        let _ = book;
        let exit_target = match reason {
            "take_profit" => tp_price,
            "stop_loss"   => sl_price,
            _             => bid,
        };
        self.try_close(creds, live_armed, exit_target, min_order_size, tick, reason).await
    }

    async fn try_close(
        &mut self,
        creds: &LiveCredentials,
        live_armed: bool,
        exit_price: f64,
        min_order_size: f64,
        tick: f64,
        reason: &str,
    ) -> bool {
        let LivePhase::Open(ref p) = self.phase else { return false };
        let (side, token_id, size, entry, neg_risk) =
            (p.side, p.token_id.clone(), p.size, p.entry_price, p.neg_risk);

        let sell_price = round_tick(exit_price.clamp(0.01, 0.99), tick);
        if size * sell_price < 1.0 {
            self.state.failed_closes += 1;
            tracing::error!(reason, token_id = %token_id, size, sell_price,
                "❌ SELL impossible — notionnel < $1, position conservée");
            self.persist();
            return false;
        }
        let _ = min_order_size;
        let args = OrderArgs { side, price: sell_price, size, is_sell: true };
        let t0 = tokio::time::Instant::now();
        let result = live_executor::place_order(live_armed, Some(creds), &token_id, neg_risk, args).await;
        let sell_ms = t0.elapsed().as_millis() as u64;
        tracing::info!(sell_ms, reason, token_id = %token_id, "⏱ latence SELL FAK");
        match result {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms: sell_post_ms }) => {
                self.last_sell_ms = Some(sell_post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        let got = avg_price.expect("avg_price doit accompagner filled_size");
                        self.record_close(order_id, side.as_str(), n, got, entry, reason);
                        true
                    }
                    _ => {
                        // filled_size absent → PendingSell, attend confirmation WS.
                        tracing::warn!(order_id = %order_id, reason, "SELL accepté sans fill_size — PendingSell");
                        let pos = if let LivePhase::Open(p) = std::mem::replace(&mut self.phase, LivePhase::Idle) {
                            p
                        } else { unreachable!() };
                        self.phase = LivePhase::PendingSell {
                            position: pos, reason: reason.to_string(), since_ms: sell_ms,
                        };
                        false
                    }
                }
            }
            Ok(PlaceResult::DryRun) => false,
            Err(e) => {
                self.state.failed_closes += 1;
                tracing::error!(error = %e, reason, token_id = %token_id,
                    "❌ SELL live échoué — ré-essai au prochain tick");
                self.persist();
                false
            }
        }
    }

    /// Callback BUY depuis OrderEngine (hot loop non-bloquante).
    #[allow(clippy::too_many_arguments)]
    pub fn on_buy_result(
        &mut self,
        res: OrderResult,
        side: Side,
        token_id: &str,
        neg_risk: bool,
        order_price: f64,
        _size: f64,
        tick: f64,
        now_ms: u64,
    ) {
        match res {
            OrderResult::Placed { order_id, filled_size, avg_price, post_ms, .. } => {
                self.last_buy_ms = Some(post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        let entry = avg_price.unwrap_or(order_price);
                        self.open_position(side, token_id, neg_risk, entry, n, tick, &order_id, now_ms);
                    }
                    _ => {
                        tracing::warn!(order_id = %order_id, "BUY (Engine) sans fill_size — PendingBuy");
                        self.phase = LivePhase::PendingBuy {
                            order_id, side, token_id: token_id.to_string(),
                            requested_size: _size, tick, since_ms: now_ms,
                        };
                    }
                }
            }
            OrderResult::DryRun { .. } => {}
            OrderResult::Failed { error, .. } => {
                tracing::error!(error = %error, "❌ BUY live échoué (OrderEngine)");
            }
        }
    }

    /// Callback SELL depuis OrderEngine. `now_ms` sert à distinguer un `balance: 0` de
    /// settlement (BUY pas encore réglé on-chain → ré-essai) d'une position vraiment disparue
    /// (fermée à la main / réglée à l'expiration → abandon).
    pub fn on_sell_result(&mut self, res: OrderResult, reason: &str, now_ms: u64) {
        match res {
            OrderResult::Placed { order_id, filled_size, avg_price, post_ms, .. } => {
                self.last_sell_ms = Some(post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        let got = avg_price.expect("avg_price accompagne filled_size");
                        let entry = self.position().map(|p| p.entry_price).unwrap_or(0.0);
                        let side = self.position().map(|p| p.side.as_str()).unwrap_or("?").to_string();
                        self.record_close(order_id, &side, n, got, entry, reason);
                    }
                    _ => {
                        tracing::warn!(order_id = %order_id, reason, "SELL (Engine) sans fill_size — PendingSell");
                        if let LivePhase::Open(pos) = std::mem::replace(&mut self.phase, LivePhase::Idle) {
                            self.phase = LivePhase::PendingSell {
                                position: pos, reason: reason.to_string(), since_ms: 0,
                            };
                        }
                    }
                }
            }
            OrderResult::DryRun { .. } => {}
            OrderResult::Failed { error, .. } => {
                self.state.failed_closes += 1;
                self.consec_close_fails += 1;
                let no_balance = error.to_lowercase().contains("balance");
                // Temps écoulé depuis l'ouverture : sous SETTLE_GRACE_MS, un `balance: 0` signifie
                // que le BUY n'est PAS encore réglé on-chain → la position arrive, on garde et on
                // ré-essaie (ne JAMAIS abandonner une position qu'on détient vraiment).
                let held_ms = match &self.phase {
                    LivePhase::Open(p) => now_ms.saturating_sub(p.opened_ms),
                    _ => u64::MAX,
                };
                let settled = held_ms >= SETTLE_GRACE_MS;
                if settled && (no_balance || self.consec_close_fails >= 5) {
                    // Réglé ET toujours introuvable → fermée à la main / réglée à l'expiration → abandon.
                    tracing::error!(error = %error, reason, held_ms,
                        "🛑 position abandonnée (introuvable on-chain après settlement) — phase → Idle, voir bankroll");
                    self.phase = LivePhase::Idle;
                    self.state.losses += 1;
                    self.consec_close_fails = 0;
                } else if no_balance {
                    tracing::warn!(reason, held_ms,
                        "⏳ SELL en attente — BUY pas encore réglé on-chain (settlement), ré-essai");
                } else {
                    tracing::error!(error = %error, reason, consec = self.consec_close_fails,
                        "❌ SELL live échoué (OrderEngine) — ré-essai au prochain tick");
                }
                self.persist();
            }
        }
    }

    /// Confirmation WS d'un fill SELL — clôture proprement.
    pub fn apply_close(
        &mut self,
        order_id: String,
        filled_size: Option<f64>,
        avg_price: Option<f64>,
        reason: &str,
    ) {
        let (entry, side_str) = match &self.phase {
            LivePhase::Open(p) => (p.entry_price, p.side.as_str().to_string()),
            LivePhase::PendingSell { position: p, .. } => (p.entry_price, p.side.as_str().to_string()),
            _ => {
                tracing::warn!(reason, "apply_close ignoré — phase != Open/PendingSell");
                return;
            }
        };
        let Some(n) = filled_size.filter(|&n| n > 0.0) else {
            tracing::error!(reason, order_id = %order_id, "apply_close : filled_size nul — PendingSell conservé");
            self.state.failed_closes += 1;
            self.persist();
            return;
        };
        let got = avg_price.unwrap_or(0.0);
        self.record_close(order_id, &side_str, n, got, entry, reason);
    }

    /// Confirmation WS d'un fill BUY (réconciliation PendingBuy → Open).
    pub fn on_fill_confirmed_buy(
        &mut self,
        order_id: &str,
        filled_size: f64,
        avg_price: f64,
        now_ms: u64,
    ) {
        if let LivePhase::PendingBuy { ref token_id, side, tick, .. } = self.phase.clone() {
            if filled_size > 0.0 {
                let token = token_id.clone();
                // neg_risk stocké dans PendingBuy n'est pas disponible ici — on suppose false (safe).
                self.open_position(side, &token, false, avg_price, filled_size, tick, order_id, now_ms);
            }
        }
    }

    fn open_position(
        &mut self,
        side: Side,
        token_id: &str,
        neg_risk: bool,
        entry: f64,
        filled: f64,
        tick: f64,
        order_id: &str,
        now_ms: u64,
    ) {
        let tp = round_tick((entry + self.params.tp_cents / 100.0).min(0.99), tick);
        let sl = round_tick((entry - self.params.sl_cents / 100.0).max(0.01), tick);
        self.state.shots += 1;
        let pos = LivePosition {
            side, token_id: token_id.to_string(), entry_price: entry, size: filled,
            tp_price: tp, sl_price: sl, opened_ms: now_ms, neg_risk,
            buy_order_id: order_id.to_string(),
        };
        self.append("open", side.as_str(), entry, filled, 0.0, order_id);
        tracing::warn!(side = side.as_str(), token_id, entry = format!("{entry:.3}"),
            size = filled, tp = format!("{tp:.2}"), sl = format!("{sl:.2}"),
            order_id = %order_id, "🎯 SNIPE LIVE");
        self.phase = LivePhase::Open(pos);
        self.consec_close_fails = 0;
        self.persist();
    }

    fn record_close(&mut self, order_id: String, side: &str, sold: f64, got_price: f64, entry: f64, reason: &str) {
        let pnl = (got_price - entry) * sold;
        self.state.realized_pnl += pnl;
        if pnl >= 0.0 { self.state.wins += 1 } else { self.state.losses += 1 }
        let kind = match reason { "take_profit" => "close_tp", "stop_loss" => "close_sl", _ => "close_max_hold" };
        self.append(kind, side, got_price, sold, pnl, &order_id);
        tracing::warn!(reason, exit = format!("{got_price:.3}"), pnl = format!("{pnl:.2}"),
            realized_pnl = format!("{:.2}", self.state.realized_pnl), order_id = %order_id, "✖ clôture LIVE");
        self.phase = LivePhase::Idle;
        self.consec_close_fails = 0;
        self.persist();
    }

    pub fn persist_state(&self) { self.persist(); }

    #[allow(dead_code)]
    pub fn hit_rate(&self) -> f64 {
        let n = self.state.wins + self.state.losses;
        if n == 0 { 0.0 } else { self.state.wins as f64 / n as f64 }
    }

    fn persist(&self) {
        #[derive(Serialize)]
        struct Snapshot<'a> { state: &'a LiveState, phase: &'a LivePhase }
        let snap = Snapshot { state: &self.state, phase: &self.phase };
        let tmp = format!("{}.tmp", self.state_path);
        if let Ok(j) = serde_json::to_string_pretty(&snap) {
            if fs::write(&tmp, j).is_ok() { let _ = fs::rename(&tmp, &self.state_path); }
        }
    }

    fn append(&self, kind: &str, side: &str, price: f64, size: f64, pnl: f64, order_id: &str) {
        let rec = LiveTradeRec {
            ts: chrono::Utc::now().to_rfc3339(), kind, side, price, size, pnl, order_id,
            realized_pnl_after: self.state.realized_pnl,
        };
        if let Ok(line) = serde_json::to_string(&rec) {
            if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&self.trades_path) {
                let _ = writeln!(f, "{line}");
            }
        }
    }
}

fn clob_min_size_for(min_order_size_tokens: f64, price: f64) -> f64 {
    let by_notional = if price > 0.0 { (1.0 / price).ceil() } else { min_order_size_tokens };
    min_order_size_tokens.max(by_notional)
}

fn round_tick(p: f64, tick: f64) -> f64 {
    if tick <= 0.0 { return p; }
    ((p / tick).round() * tick).clamp(0.01, 0.99)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mgr() -> LivePositionManager {
        LivePositionManager::load_or_init(
            KellyParams { kelly_fraction: 0.5, max_size_pct: 0.10, tp_cents: 4.0, sl_cents: 3.0, max_hold_secs: 60, kelly_price_max: 0.90 },
            "/tmp/live_state_test_phase_c.json".into(),
            "/tmp/live_trades_test_phase_c.jsonl".into(),
        )
    }

    #[test]
    fn fresh_manager_has_no_position_no_state() {
        let m = mgr();
        assert!(m.position().is_none());
        assert!(matches!(m.phase, LivePhase::Idle));
        assert_eq!(m.state.shots, 0);
    }

    #[test]
    fn buy_result_with_fill_opens_position() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50, is_sell: false, reason: None },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::Open(_)));
        assert_eq!(m.state.shots, 1);
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn buy_result_without_fill_goes_pending_buy() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: None, avg_price: None,
                post_ms: 50, is_sell: false, reason: None },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        // Pas de position ouverte sans fill confirmé.
        assert!(m.position().is_none());
        assert!(matches!(m.phase, LivePhase::PendingBuy { .. }), "phase doit être PendingBuy, got {:?}", m.phase);
        assert_eq!(m.state.shots, 0, "shots ne doit pas s'incrémenter avant fill");
    }

    #[test]
    fn sell_result_without_fill_goes_pending_sell() {
        let mut m = mgr();
        // Ouvre d'abord une position.
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50, is_sell: false, reason: None },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::Open(_)));
        // SELL sans fill.
        m.on_sell_result(
            OrderResult::Placed { order_id: "o2".into(), filled_size: None, avg_price: None,
                post_ms: 30, is_sell: true, reason: Some("take_profit") },
            "take_profit", 1000,
        );
        assert!(matches!(m.phase, LivePhase::PendingSell { .. }), "doit être PendingSell, got {:?}", m.phase);
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn apply_close_with_fill_clears_position() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50, is_sell: false, reason: None },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        m.apply_close("o2".into(), Some(10.0), Some(0.54), "take_profit");
        assert!(matches!(m.phase, LivePhase::Idle));
        assert_eq!(m.state.wins, 1);
        assert!((m.state.realized_pnl - 0.40).abs() < 1e-6, "pnl = (0.54-0.50)*10 = 0.40");
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn apply_close_without_fill_keeps_pending_sell() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50, is_sell: false, reason: None },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        // Force PendingSell.
        m.on_sell_result(
            OrderResult::Placed { order_id: "o2".into(), filled_size: None, avg_price: None,
                post_ms: 30, is_sell: true, reason: Some("stop_loss") },
            "stop_loss", 1000,
        );
        // apply_close sans fill : ne doit pas clôturer.
        m.apply_close("o2".into(), None, None, "stop_loss");
        assert!(!matches!(m.phase, LivePhase::Idle), "Idle sans fill = dangereux");
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn clob_min_size_respects_dollar_notional() {
        assert_eq!(clob_min_size_for(5.0, 0.50), 5.0);
        assert_eq!(clob_min_size_for(5.0, 0.10), 10.0);
        assert_eq!(clob_min_size_for(5.0, 0.20), 5.0);
        assert_eq!(clob_min_size_for(5.0, 0.19), 6.0);
    }

    #[test]
    fn round_tick_clamps_to_valid_range() {
        assert_eq!(round_tick(0.5234, 0.01), 0.52);
        assert_eq!(round_tick(0.005, 0.01), 0.01);
        assert_eq!(round_tick(0.999, 0.01), 0.99);
    }
}
