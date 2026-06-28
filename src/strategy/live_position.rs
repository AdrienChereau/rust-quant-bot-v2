//! Gestion **symétrique au `PaperEngine`** pour les ordres LIVE Polymarket.
//!
//! Cycle d'un trade live :
//!   1. `try_open()`  — sur signal Fire, POST BUY FAK ; si fillé, ouvre une `LivePosition`
//!                      avec TP/SL calculés sur le prix d'exécution **réel** retourné par le CLOB.
//!   2. `manage()`    — à chaque tick : si TP/SL/max-hold/fin-de-fenêtre, déclenche `try_close()`.
//!   3. `try_close()` — POST SELL FAK des shares détenues ; sur fill (même partiel), met à jour le
//!                      PnL et libère la position. Sur échec/rejet → log d'alerte, position conservée
//!                      pour ré-essai au tick suivant.
//!
//! Invariants & garde-fous :
//!   - **Une seule position à la fois** (identique au paper).
//!   - Aucun POST tant que `LIVE_ARMED=false` (le `live_executor` court-circuite en `DryRun`).
//!   - L'ouverture est bloquée si breaker tripped ; la fermeture reste autorisée pour pouvoir sortir.
//!   - Le **notionnel minimum CLOB = $1** : on enforce `max(min_order_size_tokens, ceil_at_size_price)`
//!     sur le BUY ET sur le SELL (sinon le CLOB renvoie "invalid amount … min size: 1").
//!   - Persisté dans un fichier dédié (`LIVE_STATE_PATH`) — distinct du paper pour éviter toute fusion.
//!
//! Voir `bankroll::PaperEngine` pour la version paper. Les deux maintiennent leurs propres compteurs
//! et leur propre PnL — c'est volontaire : le live mesure de l'argent réel, le paper une simulation.

use std::fs;
use std::io::Write as _;

use serde::{Deserialize, Serialize};

use crate::concurrency::bus::Side;
use crate::polymarket::live_executor::{self, LiveCredentials, OrderArgs, PlaceResult};
use crate::polymarket::order_engine::OrderResult;
use crate::polymarket::relayer::PolyBook;
use crate::strategy::bankroll::KellyParams;

/// État cumulé du trading live (compteurs + PnL réalisé). Persisté sur disque.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveState {
    pub realized_pnl: f64,    // somme des PnL clôturés (USDC réels)
    pub shots: u64,           // BUY acceptés (= positions ouvertes)
    pub wins: u64,
    pub losses: u64,
    pub failed_closes: u64,   // SELL refusés ou en erreur → position bloquée, alerte
}

/// Position live ouverte. `size` reflète le **fill réel** du BUY, pas la taille demandée.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePosition {
    pub side: Side,
    pub token_id: String,
    pub entry_price: f64,
    pub size: f64,           // shares effectivement détenues après BUY (peut < taille demandée)
    pub tp_price: f64,
    pub sl_price: f64,
    pub opened_ms: u64,
    pub neg_risk: bool,      // requis pour signer le SELL avec le bon contrat
    pub buy_order_id: String,
}

/// Manager symétrique au `PaperEngine` mais qui touche le CLOB réel.
pub struct LivePositionManager {
    pub state: LiveState,
    pub position: Option<LivePosition>,
    pub last_buy_ms: Option<u64>,  // durée du dernier BUY POST (pour dashboard Phase 0)
    pub last_sell_ms: Option<u64>, // durée du dernier SELL POST (pour dashboard Phase 0)
    params: KellyParams,
    state_path: String,
    trades_path: String,
}

#[derive(Serialize)]
struct LiveTradeRec<'a> {
    ts: String,
    kind: &'a str,           // "open" / "close_tp" / "close_sl" / "close_max_hold" / "close_fail"
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
        Self { state, position: None, last_buy_ms: None, last_sell_ms: None, params, state_path, trades_path }
    }

    /// Tente d'ouvrir une position : POST BUY FAK + enregistrement si fill > 0.
    /// Renvoie `true` si une position a été créée. Pas d'effet si une position est déjà ouverte
    /// (un seul tir à la fois, comme le paper).
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
        if self.position.is_some() {
            return false; // un seul tir à la fois
        }
        // Garde notionnel ≥ $1 (sinon CLOB refuse).
        let size_min_cost = clob_min_size_for(min_order_size, order_price);
        let size_final = size.max(size_min_cost);
        let args = OrderArgs { side, price: order_price, size: size_final, is_sell: false };
        let t0 = tokio::time::Instant::now();
        let result = live_executor::place_order(live_armed, Some(creds), token_id, neg_risk, args).await;
        let buy_ms = t0.elapsed().as_millis();
        tracing::info!(buy_ms, side = side.as_str(), token_id, "⏱ latence BUY FAK");
        match result {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms: buy_post_ms }) => {
                self.last_buy_ms = Some(buy_post_ms);
                // Fill réel ou fallback sur la taille demandée si le CLOB n'a rien exposé.
                let entry = avg_price.unwrap_or(order_price);
                let filled = filled_size.unwrap_or(size_final);
                if filled <= 0.0 {
                    tracing::warn!(order_id = %order_id, "BUY accepté mais fill = 0 — pas de position");
                    return false;
                }
                let tp = round_tick((entry + self.params.tp_cents / 100.0).min(0.99), tick);
                let sl = round_tick((entry - self.params.sl_cents / 100.0).max(0.01), tick);
                self.state.shots += 1;
                self.position = Some(LivePosition {
                    side, token_id: token_id.to_string(), entry_price: entry, size: filled,
                    tp_price: tp, sl_price: sl, opened_ms: now_ms, neg_risk,
                    buy_order_id: order_id.clone(),
                });
                self.append("open", side.as_str(), entry, filled, 0.0, &order_id);
                tracing::warn!(side = side.as_str(), token_id, entry = format!("{entry:.3}"),
                    size = filled, tp = format!("{tp:.2}"), sl = format!("{sl:.2}"),
                    order_id = %order_id, "🎯 SNIPE LIVE");
                self.persist();
                true
            }
            Ok(PlaceResult::DryRun) => {
                // LIVE_ARMED=false : signé+loggé en amont, on n'ouvre pas de position.
                false
            }
            Err(e) => {
                tracing::error!(error = %e, side = side.as_str(), token_id, "❌ BUY live échoué");
                false
            }
        }
    }

    /// Gère la position ouverte. Si TP/SL/max-hold/fin-de-fenêtre atteint, déclenche un SELL FAK.
    /// Renvoie `true` si la position est fermée à l'issue de l'appel.
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
        let Some(p) = self.position.as_ref() else { return false };
        let Some(bid) = mark_bid else { return false };
        let (tp_price, sl_price, opened_ms, max_hold) = (p.tp_price, p.sl_price, p.opened_ms, self.params.max_hold_secs);
        let held_s = (now_ms.saturating_sub(opened_ms) / 1000) as i64;

        let reason = if bid >= tp_price { Some("take_profit") }
        else if bid <= sl_price { Some("stop_loss") }
        else if held_s >= max_hold || remaining_s <= 30 { Some("max_hold") }
        else { None };

        let Some(reason) = reason else { return false };

        // Prix de sortie : best_bid actuel pour TP/max_hold, sl_price pour le stop (approche conservatrice
        // côté paper ; en live on poste à `bid` ou en dessous pour maximiser la chance de fill FAK).
        let _ = book; // book gardé en signature pour symétrie/extension future (VWAP de sortie)
        let exit_target = match reason {
            "take_profit" => tp_price,
            "stop_loss"   => sl_price,
            _             => bid,
        };
        self.try_close(creds, live_armed, exit_target, min_order_size, tick, reason).await
    }

    /// POST SELL FAK des shares détenues. Sur fill : update PnL + position libérée.
    /// Sur échec : `failed_closes++`, position conservée pour ré-essai au tick suivant.
    async fn try_close(
        &mut self,
        creds: &LiveCredentials,
        live_armed: bool,
        exit_price: f64,
        min_order_size: f64,
        tick: f64,
        reason: &str,
    ) -> bool {
        let Some(p) = self.position.as_ref() else { return false };
        let (side, token_id, size, entry, neg_risk) =
            (p.side, p.token_id.clone(), p.size, p.entry_price, p.neg_risk);

        // Prix SELL : clampé entre [0.01, 0.99] et arrondi au tick.
        let sell_price = round_tick(exit_price.clamp(0.01, 0.99), tick);
        // Garde notionnel ≥ $1 sur le SELL aussi (sinon rejet CLOB).
        if size * sell_price < 1.0 {
            // Position trop petite pour être vendue ; on ne peut rien faire de plus.
            // Marqué comme alerte ; reste détenue jusqu'à résolution du marché.
            self.state.failed_closes += 1;
            tracing::error!(reason, token_id = %token_id, size, sell_price,
                cost = format!("{:.2}", size * sell_price),
                "❌ SELL impossible — notionnel < $1 minimum CLOB, position conservée jusqu'à résolution");
            self.persist();
            return false;
        }
        let _ = min_order_size; // référence gardée — la garde au-dessus est plus stricte

        let args = OrderArgs { side, price: sell_price, size, is_sell: true };
        let t0 = tokio::time::Instant::now();
        let result = live_executor::place_order(live_armed, Some(creds), &token_id, neg_risk, args).await;
        let sell_ms = t0.elapsed().as_millis();
        tracing::info!(sell_ms, reason, token_id = %token_id, "⏱ latence SELL FAK");
        match result {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms: sell_post_ms }) => {
                self.last_sell_ms = Some(sell_post_ms);
                let sold = filled_size.unwrap_or(size);
                let got_price = avg_price.unwrap_or(sell_price);
                if sold <= 0.0 {
                    self.state.failed_closes += 1;
                    tracing::error!(reason, token_id = %token_id,
                        "❌ SELL accepté par CLOB mais fill = 0 — position conservée, ré-essai au prochain tick");
                    self.persist();
                    return false;
                }
                let pnl = (got_price - entry) * sold;
                self.state.realized_pnl += pnl;
                if pnl >= 0.0 { self.state.wins += 1 } else { self.state.losses += 1 }
                let kind = match reason {
                    "take_profit" => "close_tp",
                    "stop_loss"   => "close_sl",
                    _             => "close_max_hold",
                };
                self.append(kind, side.as_str(), got_price, sold, pnl, &order_id);
                tracing::warn!(reason, token_id = %token_id, exit = format!("{got_price:.3}"),
                    pnl = format!("{pnl:.2}"), realized_pnl = format!("{:.2}", self.state.realized_pnl),
                    order_id = %order_id, "✖ clôture LIVE");
                self.position = None;
                self.persist();
                true
            }
            Ok(PlaceResult::DryRun) => {
                // LIVE_ARMED=false : on n'a pas vraiment vendu, position conservée.
                false
            }
            Err(e) => {
                self.state.failed_closes += 1;
                tracing::error!(error = %e, reason, token_id = %token_id, sell_price,
                    "❌ SELL live échoué — position conservée, ré-essai au prochain tick");
                self.persist();
                false
            }
        }
    }

    /// Callback appelé par executor.rs quand l'OrderEngine renvoie le résultat d'un BUY.
    #[allow(clippy::too_many_arguments)]
    pub fn on_buy_result(
        &mut self,
        res: OrderResult,
        side: Side,
        token_id: &str,
        neg_risk: bool,
        order_price: f64,
        size: f64,
        tick: f64,
        now_ms: u64,
    ) {
        match res {
            OrderResult::Placed { order_id, filled_size, avg_price, post_ms, .. } => {
                self.last_buy_ms = Some(post_ms);
                let entry = avg_price.unwrap_or(order_price);
                let filled = filled_size.unwrap_or(size);
                if filled <= 0.0 {
                    tracing::warn!(order_id = %order_id, "BUY accepté mais fill = 0 — pas de position");
                    return;
                }
                let tp = round_tick((entry + self.params.tp_cents / 100.0).min(0.99), tick);
                let sl = round_tick((entry - self.params.sl_cents / 100.0).max(0.01), tick);
                self.state.shots += 1;
                self.position = Some(LivePosition {
                    side, token_id: token_id.to_string(), entry_price: entry, size: filled,
                    tp_price: tp, sl_price: sl, opened_ms: now_ms, neg_risk,
                    buy_order_id: order_id.clone(),
                });
                self.append("open", side.as_str(), entry, filled, 0.0, &order_id);
                tracing::warn!(side = side.as_str(), token_id, entry = format!("{entry:.3}"),
                    size = filled, tp = format!("{tp:.2}"), sl = format!("{sl:.2}"),
                    order_id = %order_id, "🎯 SNIPE LIVE");
                self.persist();
            }
            OrderResult::DryRun { .. } => {}
            OrderResult::Failed { error, .. } => {
                tracing::error!(error = %error, "❌ BUY live échoué (OrderEngine)");
            }
        }
    }

    /// Callback appelé par executor.rs quand l'OrderEngine renvoie le résultat d'un SELL.
    pub fn on_sell_result(&mut self, res: OrderResult, reason: &str) {
        match res {
            OrderResult::Placed { order_id, filled_size, avg_price, post_ms, .. } => {
                self.last_sell_ms = Some(post_ms);
                self.apply_close(order_id, filled_size, avg_price, reason);
            }
            OrderResult::DryRun { .. } => {}
            OrderResult::Failed { error, .. } => {
                self.state.failed_closes += 1;
                tracing::error!(error = %error, reason, "❌ SELL live échoué (OrderEngine)");
                self.persist();
            }
        }
    }

    /// Met à jour PnL et libère la position après un SELL confirmé.
    pub fn apply_close(
        &mut self,
        order_id: String,
        filled_size: Option<f64>,
        avg_price: Option<f64>,
        reason: &str,
    ) {
        let Some(p) = self.position.take() else { return };
        let sold = filled_size.unwrap_or(p.size);
        let got_price = avg_price.unwrap_or(p.sl_price);
        if sold <= 0.0 {
            self.state.failed_closes += 1;
            tracing::error!(reason, "❌ SELL fill = 0 — position libérée, perte enregistrée");
            self.position = Some(p); // remet la position
            self.persist();
            return;
        }
        let pnl = (got_price - p.entry_price) * sold;
        self.state.realized_pnl += pnl;
        if pnl >= 0.0 { self.state.wins += 1 } else { self.state.losses += 1 }
        let kind = match reason { "take_profit" => "close_tp", "stop_loss" => "close_sl", _ => "close_max_hold" };
        self.append(kind, p.side.as_str(), got_price, sold, pnl, &order_id);
        tracing::warn!(reason, exit = format!("{got_price:.3}"), pnl = format!("{pnl:.2}"),
            realized_pnl = format!("{:.2}", self.state.realized_pnl), order_id = %order_id, "✖ clôture LIVE");
        self.persist();
    }

    /// Persiste l'état (exposé pour l'OrderEngine callback dans executor.rs).
    pub fn persist_state(&self) { self.persist(); }

    #[allow(dead_code)] // exposé pour le dashboard / tooling externe
    pub fn hit_rate(&self) -> f64 {
        let n = self.state.wins + self.state.losses;
        if n == 0 { 0.0 } else { self.state.wins as f64 / n as f64 }
    }

    fn persist(&self) {
        #[derive(Serialize)]
        struct Snapshot<'a> { state: &'a LiveState, position: &'a Option<LivePosition> }
        let snap = Snapshot { state: &self.state, position: &self.position };
        let tmp = format!("{}.tmp", self.state_path);
        if let Ok(j) = serde_json::to_string_pretty(&snap) {
            if fs::write(&tmp, j).is_ok() {
                let _ = fs::rename(&tmp, &self.state_path);
            }
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

/// Notionnel minimum CLOB = $1 : taille telle que `size * price ≥ 1.0`, sinon le CLOB rejette.
/// Aussi : `min_order_size` (en tokens) reste la borne basse.
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

    #[test]
    fn clob_min_size_respects_dollar_notional() {
        // À $0.50, 5 tokens = $2.50 → OK, on garde 5.
        assert_eq!(clob_min_size_for(5.0, 0.50), 5.0);
        // À $0.10, 5 tokens = $0.50 → en dessous de $1 ; il faut 10 tokens.
        assert_eq!(clob_min_size_for(5.0, 0.10), 10.0);
        // À $0.20, 5 tokens = $1.00 → exactement OK.
        assert_eq!(clob_min_size_for(5.0, 0.20), 5.0);
        // À $0.19 (cas réel observé : 5×0.19 = $0.95 → rejet), il faut ceil(1/0.19) = 6.
        assert_eq!(clob_min_size_for(5.0, 0.19), 6.0);
    }

    #[test]
    fn round_tick_clamps_to_valid_range() {
        assert_eq!(round_tick(0.5234, 0.01), 0.52);
        assert_eq!(round_tick(0.005, 0.01), 0.01); // clamp bas
        assert_eq!(round_tick(0.999, 0.01), 0.99); // clamp haut
    }

    #[test]
    fn fresh_manager_has_no_position_no_state() {
        let mgr = LivePositionManager::load_or_init(
            KellyParams { kelly_fraction: 0.5, max_size_pct: 0.10, tp_cents: 4.0, sl_cents: 3.0, max_hold_secs: 60 },
            "/tmp/live_state_test_fresh.json".into(),
            "/tmp/live_trades_test_fresh.jsonl".into(),
        );
        assert!(mgr.position.is_none());
        assert_eq!(mgr.state.shots, 0);
        assert_eq!(mgr.state.realized_pnl, 0.0);
    }
}
