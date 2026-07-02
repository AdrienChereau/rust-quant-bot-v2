//! Sizing Kelly fractionnel + exécution PAPER du sniper (P5).
//!
//! Sur un signal FIRE : achat taker du side + take-profit à +`tp_pct` (proportionnel), avec
//! **stop-loss**, **max-hold** et liquidation à la résolution. Sizing = fraction de
//! Kelly (half-Kelly par défaut), bornée. Fills paper réalistes : slippage en
//! parcourant le carnet PM ; sélection adverse (biais selon le mouvement futur).

use std::fs;
use std::io::Write as _;

use serde::{Deserialize, Serialize};

use crate::concurrency::bus::Side;
use crate::polymarket::relayer::PolyBook;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SniperState {
    pub cash: f64,
    pub start_cash: f64,
    pub realized_pnl: f64,
    pub peak_equity: f64,
    pub shots: u64,        // tirs exécutés
    pub wins: u64,
    pub losses: u64,
    pub blocked_size: u64, // tirs bloqués (taille/bankroll)
}

#[derive(Debug, Clone)]
pub struct OpenPosition {
    pub side: Side,
    pub token_id: String,
    pub entry_price: f64,
    pub size: f64,
    pub cost_basis: f64, // ce qu'on a réellement payé, frais taker d'entrée inclus
    pub tp_price: f64,
    pub sl_price: f64,
    pub opened_ms: u64,
}

/// Ordre soumis mais pas encore rempli : modélise la latence signal→ordre. Il se règle après
/// `sim_latency_ms` contre le book PM de ce moment-là (dérive adverse capturée).
#[derive(Debug, Clone)]
struct PendingOrder {
    side: Side,
    token_id: String,
    edge: f64,
    submit_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct KellyParams {
    pub kelly_fraction: f64,    // 0.5 = half-Kelly
    pub max_size_pct: f64,      // plafond taille×prix / equity
    pub tp_pct: f64,   // fraction du prix d'entrée (0.08 = +8 %) — risque cohérent sur toute la plage 0.01–0.99
    pub sl_pct: f64,   // fraction du prix d'entrée (0.06 = −6 %)
    pub max_hold_secs: i64,
    pub kelly_price_max: f64,   // KELLY_PRICE_MAX=0.90 — clamp favoris seulement (pas de plancher)
}

impl KellyParams {
    /// Taille de Kelly sur une `equity` explicite : `f* = edge/odds`, bornée. Pure fonction du
    /// sizing (aucune dépendance à `PaperEngine`) — utilisée par le PAPER (cash interne) **et** par
    /// le LIVE (vraie collatéral CLOB). Renvoie le nombre de tokens (entier).
    pub fn kelly_size_for(&self, edge: f64, price: f64, equity: f64) -> f64 {
        if price <= 0.0 || price >= 1.0 || equity <= 0.0 {
            return 0.0;
        }
        // Kelly binaire exact : miser f à prix c, gain (1−c)/c si victoire →
        // f* = (p−c)/(1−c) = edge/(1−c). (L'ancien edge·c/(1−c) sous-misait d'un facteur c.)
        let f_full = (edge / (1.0 - price)).clamp(0.0, 1.0);
        let f = f_full * self.kelly_fraction;
        let budget = (equity * f).min(equity * self.max_size_pct);
        (budget / price).floor()
    }

    /// Kelly robuste : clamp sur les favoris (price > kelly_price_max) + pénalité incertitude.
    /// - Clamp UNIQUEMENT vers le haut (pas de plancher : sur-pénalise les longshots rentables).
    /// - Robustesse = (1 − incertitude) ; incertitude = score_sigma/|score| + 0.5×basis_unc.
    pub fn robust_kelly_size_for(
        &self,
        edge: f64,
        price: f64,
        equity: f64,
        score_abs: f64,
        score_sigma: f64,
        basis_unc: f64,
    ) -> f64 {
        if price <= 0.0 || price >= 1.0 || equity <= 0.0 {
            return 0.0;
        }
        // Clamp favoris : price_k plafonné → dénominateur (1−price_k) plus grand → taille réduite.
        let price_k = price.min(self.kelly_price_max);
        let uncertainty = (score_sigma / (score_abs + 1e-9) + 0.5 * basis_unc).min(1.0);
        let robustness = (1.0 - uncertainty).max(0.0);
        // Kelly binaire exact : f* = edge/(1−c) (cf. kelly_size_for).
        let f_full = (edge / (1.0 - price_k) * robustness).clamp(0.0, 1.0);
        let f = f_full * self.kelly_fraction;
        let budget = (equity * f).min(equity * self.max_size_pct);
        (budget / price).floor()
    }
}

/// Variance EMA O(1) pour le score composite — remplace VecDeque + rolling_std.
/// λ=0.9995 → fenêtre effective ≈ 2000 samples = 20 secondes à 100 Hz.
pub struct EmaScoreStat {
    lambda: f64,
    ema: f64,
    ema_sq: f64,
    count: u32,
}

impl EmaScoreStat {
    pub fn new(lambda: f64) -> Self {
        Self { lambda, ema: 0.0, ema_sq: 0.0, count: 0 }
    }

    pub fn update(&mut self, score: f64) {
        self.count += 1;
        self.ema = self.lambda * self.ema + (1.0 - self.lambda) * score;
        self.ema_sq = self.lambda * self.ema_sq + (1.0 - self.lambda) * score * score;
    }

    /// Écart-type du score. Conservateur (retourne 1.0) pendant les 100 premiers samples.
    pub fn std_dev(&self) -> f64 {
        if self.count < 100 { return 1.0; }
        (self.ema_sq - self.ema * self.ema).max(0.0).sqrt()
    }
}

/// IC Tracker : corrélation de Pearson entre score_at_entry et outcome sur une fenêtre glissante.
pub struct IcTracker {
    history: std::collections::VecDeque<(f64, f64)>, // (score, outcome ∈ {+1, -1})
    window: usize,
}

impl IcTracker {
    pub fn new(window: usize) -> Self {
        Self { history: std::collections::VecDeque::with_capacity(window + 1), window }
    }

    pub fn record(&mut self, score: f64, win: bool) {
        if self.history.len() >= self.window { self.history.pop_front(); }
        self.history.push_back((score, if win { 1.0 } else { -1.0 }));
    }

    /// Pearson IC ∈ [-1, 1]. Retourne 0.0 si moins de 20 observations.
    pub fn ic(&self) -> f64 {
        if self.history.len() < 20 { return 0.0; }
        let n = self.history.len() as f64;
        let (mut sx, mut sy, mut sxy, mut sx2, mut sy2) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for &(x, y) in &self.history {
            sx += x; sy += y; sxy += x * y; sx2 += x * x; sy2 += y * y;
        }
        let num = n * sxy - sx * sy;
        let den = ((n * sx2 - sx * sx) * (n * sy2 - sy * sy)).sqrt();
        if den < 1e-9 { 0.0 } else { (num / den).clamp(-1.0, 1.0) }
    }
}

pub struct PaperEngine {
    pub state: SniperState,
    pub position: Option<OpenPosition>,
    /// > 0 : notionnel fixe en $ par tir (ignore Kelly). 0 = sizing Kelly normal.
    pub fixed_order_usd: f64,
    /// Coefficient de frais taker Polymarket : frais = coef·p·(1−p) par share (défaut 0.07 —
    /// ~1.75 ¢/share à p=0.5, ~0 aux extrêmes). Appliqué à l'entrée ET à la sortie. 0 = désactivé.
    pub taker_fee_coef: f64,
    /// Latence simulée signal→ordre (ms) : le fill se règle après ce délai contre le book courant.
    pub sim_latency_ms: u64,
    pending: Option<PendingOrder>,
    params: KellyParams,
    state_path: String,
    trades_path: String,
}

#[derive(Serialize)]
struct TradeRec<'a> {
    ts: String,
    kind: &'a str,
    side: &'a str,
    price: f64,
    size: f64,
    pnl: f64,
    cash_after: f64,
}

impl PaperEngine {
    pub fn load_or_init(start_cash: f64, params: KellyParams, state_path: String, trades_path: String) -> Self {
        let state = fs::read_to_string(&state_path).ok()
            .and_then(|s| serde_json::from_str::<SniperState>(&s).ok())
            .unwrap_or(SniperState { cash: start_cash, start_cash, peak_equity: start_cash, ..Default::default() });
        tracing::info!(cash = state.cash, shots = state.shots, wins = state.wins, "État sniper chargé");
        Self { state, position: None, fixed_order_usd: 0.0, taker_fee_coef: 0.0, sim_latency_ms: 0,
            pending: None, params, state_path, trades_path }
    }

    pub fn equity(&self, mark: Option<f64>) -> f64 {
        let pos_val = match (&self.position, mark) {
            (Some(p), Some(m)) => p.size * m,
            _ => 0.0,
        };
        self.state.cash + pos_val
    }

    /// Taille de Kelly sur le cash paper interne (sizing paper).
    pub fn kelly_size(&self, edge: f64, price: f64) -> f64 {
        self.params.kelly_size_for(edge, price, self.state.cash)
    }

    /// Met à jour à chaud les paramètres de **sizing** (fraction, plafond, clamp favoris) depuis
    /// le snapshot de tuning. TP/SL/max-hold restent figés (changer le TP/SL d'une position
    /// ouverte serait dangereux — exclu du tuning à chaud).
    pub fn update_sizing(&mut self, kelly_fraction: f64, max_size_pct: f64, kelly_price_max: f64) {
        self.params.kelly_fraction = kelly_fraction;
        self.params.max_size_pct = max_size_pct;
        self.params.kelly_price_max = kelly_price_max;
    }

    /// Taille de Kelly sur une `equity` explicite (délègue à `KellyParams::kelly_size_for`).
    /// Conservé pour compat ; le LIVE appelle désormais directement `KellyParams::kelly_size_for`.
    pub fn kelly_size_for(&self, edge: f64, price: f64, equity: f64) -> f64 {
        self.params.kelly_size_for(edge, price, equity)
    }

    /// Soumet un tir (signal FIRE). **Ne remplit PAS tout de suite** : l'ordre part en vol et se
    /// règle après `sim_latency_ms` (cf. `settle_pending`). Un seul ordre en vol / une position
    /// à la fois. Renvoie `true` si l'ordre est accepté (mis en file).
    pub fn submit(&mut self, side: Side, token_id: &str, edge: f64, now_ms: u64) -> bool {
        if self.position.is_some() || self.pending.is_some() {
            return false;
        }
        self.pending = Some(PendingOrder { side, token_id: token_id.to_string(), edge, submit_ms: now_ms });
        true
    }

    /// Règle l'ordre en vol s'il a atteint la latence simulée. Fill contre le book **courant**
    /// (retardé de `sim_latency_ms` par rapport au signal → dérive adverse capturée) + frais taker
    /// à l'entrée. À appeler à chaque tick. Renvoie `true` si un fill a eu lieu.
    pub fn settle_pending(&mut self, now_ms: u64, up_book: &PolyBook, down_book: &PolyBook, tick: f64, min_size: f64) -> bool {
        let Some(p) = self.pending.as_ref() else { return false };
        if now_ms.saturating_sub(p.submit_ms) < self.sim_latency_ms {
            return false; // l'ordre n'est pas encore "arrivé" sur Polymarket
        }
        if self.position.is_some() { self.pending = None; return false; }
        let (side, token_id, edge) = (p.side, p.token_id.clone(), p.edge);
        self.pending = None;

        let book = if side == Side::Up { up_book } else { down_book };
        let Some(best_ask) = book.best_ask() else { return false };
        let size = if self.fixed_order_usd > 0.0 {
            (self.fixed_order_usd / best_ask).floor().max(min_size)
        } else {
            self.kelly_size(edge, best_ask)
        };
        if size < min_size {
            self.state.blocked_size += 1;
            return false;
        }
        let (avg_price, filled) = vwap_buy(book, size);
        if filled <= 0.0 {
            return false;
        }
        let notional = avg_price * filled;
        // Frais taker Polymarket : coef·p·(1−p) par share — maximaux à p=0.5, ~nuls aux extrêmes.
        let fee = self.taker_fee_coef * avg_price * (1.0 - avg_price) * filled;
        let cost = notional + fee;
        if self.state.cash < cost {
            return false;
        }
        self.state.cash -= cost;
        // TP/SL proportionnels au prix d'entrée (risque cohérent sur toute la plage 0.01–0.99).
        let tp = (avg_price * (1.0 + self.params.tp_pct)).min(0.99);
        let sl = (avg_price * (1.0 - self.params.sl_pct)).max(0.01);
        self.position = Some(OpenPosition {
            side, token_id, entry_price: avg_price, size: filled, cost_basis: cost,
            tp_price: round_tick(tp, tick), sl_price: round_tick(sl, tick), opened_ms: now_ms,
        });
        self.state.shots += 1;
        self.append("fire", side.as_str(), avg_price, filled, -fee);
        tracing::warn!(side = side.as_str(), entry = format!("{:.3}", avg_price), size = filled,
            fee = format!("{:.4}", fee), lat_ms = self.sim_latency_ms, "🎯 SNIPE (réglé)");
        true
    }

    /// Gère la position ouverte : TP atteint, stop-loss, max-hold. Renvoie true si fermée.
    pub fn manage(&mut self, mark_bid: Option<f64>, now_ms: u64, remaining_s: i64) -> bool {
        // Lecture par référence (pas de clone de la position à chaque tick) ; on extrait les
        // primitives Copy nécessaires avant d'appeler close_position (qui emprunte &mut self).
        let Some(p) = self.position.as_ref() else { return false };
        let Some(bid) = mark_bid else { return false };
        let (tp_price, sl_price, opened_ms) = (p.tp_price, p.sl_price, p.opened_ms);
        let held_s = (now_ms.saturating_sub(opened_ms) / 1000) as i64;

        if bid >= tp_price {
            self.close_position(tp_price, "take_profit");
            true
        } else if bid <= sl_price {
            self.close_position(sl_price, "stop_loss");
            true
        } else if held_s >= self.params.max_hold_secs || remaining_s <= 30 {
            self.close_position(bid, "max_hold"); // liquidation au marché
            true
        } else {
            false
        }
    }

    fn close_position(&mut self, exit_price: f64, reason: &str) {
        let Some(p) = self.position.take() else { return };
        // Vente taker : frais coef·p·(1−p) par share sur la sortie. PnL = produits nets −
        // cost_basis (frais d'entrée inclus dans cost_basis) → aller-retour compté.
        let gross = exit_price * p.size;
        let fee = self.taker_fee_coef * exit_price * (1.0 - exit_price) * p.size;
        let proceeds = gross - fee;
        self.state.cash += proceeds;
        let pnl = proceeds - p.cost_basis;
        self.state.realized_pnl = self.state.cash - self.state.start_cash;
        if pnl >= 0.0 { self.state.wins += 1 } else { self.state.losses += 1 }
        let eq = self.state.cash;
        if eq > self.state.peak_equity { self.state.peak_equity = eq; }
        self.append(reason, p.side.as_str(), exit_price, p.size, pnl);
        tracing::warn!(reason, token_id = p.token_id, exit = format!("{:.3}", exit_price), pnl = format!("{:.3}", pnl),
            cash = format!("{:.2}", self.state.cash), "✖ clôture");
        self.persist();
    }

    pub fn drawdown(&self) -> f64 {
        (self.state.peak_equity - self.equity(None)).max(0.0)
    }
    pub fn hit_rate(&self) -> f64 {
        let n = self.state.wins + self.state.losses;
        if n == 0 { 0.0 } else { self.state.wins as f64 / n as f64 }
    }

    pub fn persist(&self) {
        let tmp = format!("{}.tmp", self.state_path);
        if let Ok(j) = serde_json::to_string_pretty(&self.state) {
            if fs::write(&tmp, j).is_ok() {
                let _ = fs::rename(&tmp, &self.state_path);
            }
        }
    }

    fn append(&self, kind: &str, side: &str, price: f64, size: f64, pnl: f64) {
        let rec = TradeRec { ts: chrono::Utc::now().to_rfc3339(), kind, side, price, size, pnl, cash_after: self.state.cash };
        if let Ok(line) = serde_json::to_string(&rec) {
            if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&self.trades_path) {
                let _ = writeln!(f, "{line}");
            }
        }
    }
}

/// Circuit breaker drawdown (basé sur l'**equity**, pas le cash).
/// Renvoie `true` s'il faut couper : `initial_capital − current_equity ≥ max_dd`.
/// Utilisé en mode **paper** (equity fictive vs START_CASH).
pub fn check_drawdown_breaker(current_equity: f64, initial_capital: f64, max_dd: f64) -> bool {
    initial_capital - current_equity >= max_dd
}

/// Suivi du drawdown sur la **bankroll réelle** (mode live) via high-water mark.
/// La bankroll réelle est lue périodiquement sur le CLOB ; on coupe quand la perte depuis
/// le pic atteint `max_dd`. ⚠️ `max_dd` (MAX_DRAWDOWN) doit être < bankroll, sinon jamais déclenché.
#[derive(Default)]
pub struct LiveDrawdown {
    peak: Option<f64>,
}

impl LiveDrawdown {
    /// Met à jour le pic avec la bankroll courante et renvoie `true` si `pic − courante ≥ max_dd`.
    pub fn breached(&mut self, current_bankroll: f64, max_dd: f64) -> bool {
        let peak = self.peak.get_or_insert(current_bankroll);
        if current_bankroll > *peak {
            *peak = current_bankroll;
        }
        *peak - current_bankroll >= max_dd
    }
}

/// PnL réalisé **live** = variation de la vraie bankroll CLOB depuis l'activation du mode live.
/// C'est l'argent réel (fills + frais + résolutions), pas une reconstruction depuis les ordres.
/// La référence est posée à la 1re lecture après passage en live ; `reset()` à chaque (ré)activation.
#[derive(Default)]
pub struct LivePnl {
    baseline: Option<f64>,
}

impl LivePnl {
    /// Repose la référence (à l'activation du live) — le PnL repart de 0.
    pub fn reset(&mut self) {
        self.baseline = None;
    }

    /// Met à jour avec la bankroll réelle courante ; renvoie le PnL réalisé live (courante − référence).
    pub fn update(&mut self, current_bankroll: f64) -> f64 {
        let base = *self.baseline.get_or_insert(current_bankroll);
        current_bankroll - base
    }
}

/// Ajuste la taille Kelly au minimum Polymarket.
/// - taille ≥ `min_tokens` → inchangée ;
/// - `min_tokens/2 ≤ taille < min_tokens` → arrondie au minimum (signal correct) ;
/// - taille < `min_tokens/2` → `None` (signal trop faible, on ignore le trade).
pub fn adjust_size_to_min(size_from_kelly: f64, min_tokens: f64) -> Option<f64> {
    if size_from_kelly >= min_tokens {
        Some(size_from_kelly)
    } else if size_from_kelly >= min_tokens * 0.5 {
        Some(min_tokens)
    } else {
        None
    }
}

/// Prix moyen pondéré (VWAP) d'un achat taker qui consomme `size` en parcourant les asks.
fn vwap_buy(book: &PolyBook, size: f64) -> (f64, f64) {
    let mut asks = book.asks.clone();
    asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
    let mut remaining = size;
    let mut cost = 0.0;
    let mut filled = 0.0;
    for lvl in asks {
        if remaining <= 0.0 { break; }
        let take = remaining.min(lvl.size);
        cost += take * lvl.price;
        filled += take;
        remaining -= take;
    }
    if filled <= 0.0 { (0.0, 0.0) } else { (cost / filled, filled) }
}

fn round_tick(p: f64, tick: f64) -> f64 {
    ((p / tick).round() * tick).clamp(0.01, 0.99)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::relayer::Level;

    fn params() -> KellyParams {
        KellyParams { kelly_fraction: 0.5, max_size_pct: 0.10, tp_pct: 0.10, sl_pct: 0.08, max_hold_secs: 120, kelly_price_max: 0.90 }
    }
    fn engine() -> PaperEngine {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(0);
        let id = CTR.fetch_add(1, Ordering::Relaxed);
        PaperEngine::load_or_init(
            200.0, params(),
            format!("/tmp/sniper_s_test_{id}.json"),
            format!("/tmp/sniper_t_test_{id}.jsonl"),
        )
    }
    fn book() -> PolyBook {
        PolyBook { bids: vec![Level { price: 0.49, size: 1000.0 }], asks: vec![Level { price: 0.50, size: 1000.0 }] }
    }

    #[test]
    fn kelly_size_positive_with_edge() {
        let e = engine();
        assert!(e.kelly_size(0.10, 0.50) > 0.0);
    }
    #[test]
    fn kelly_zero_without_edge() {
        let e = engine();
        assert_eq!(e.kelly_size(0.0, 0.50), 0.0);
    }
    #[test]
    fn fire_then_take_profit() {
        let mut e = engine();
        let b = book();
        assert!(e.submit(Side::Up, "tok", 0.10, 0));
        assert!(e.settle_pending(0, &b, &b, 0.01, 5.0)); // latence 0 → fill immédiat
        assert!(e.position.is_some());
        // le bid monte au-dessus du TP → clôture gagnante
        let closed = e.manage(Some(0.65), 1000, 200);
        assert!(closed);
        assert!(e.position.is_none());
        assert_eq!(e.state.wins, 1);
    }
    #[test]
    fn stop_loss_triggers() {
        let mut e = engine();
        let b = book();
        e.submit(Side::Up, "tok", 0.10, 0);
        e.settle_pending(0, &b, &b, 0.01, 5.0);
        let closed = e.manage(Some(0.40), 1000, 200); // sous le SL (~0.46)
        assert!(closed);
        assert_eq!(e.state.losses, 1);
    }

    #[test]
    fn latency_defers_fill() {
        let mut e = engine();
        e.sim_latency_ms = 400;
        let b = book();
        e.submit(Side::Up, "tok", 0.10, 1000);
        // avant 400 ms : pas de fill
        assert!(!e.settle_pending(1200, &b, &b, 0.01, 5.0));
        assert!(e.position.is_none());
        // après 400 ms : fill
        assert!(e.settle_pending(1400, &b, &b, 0.01, 5.0));
        assert!(e.position.is_some());
    }

    #[test]
    fn taker_fee_reduces_pnl() {
        let mut e = engine();
        e.taker_fee_coef = 0.07; // courbe Polymarket réelle
        let b = book();
        e.submit(Side::Up, "tok", 0.10, 0);
        e.settle_pending(0, &b, &b, 0.01, 5.0);
        let p = e.position.as_ref().unwrap();
        let notional = p.entry_price * p.size;
        // frais attendus = 0.07·0.5·0.5·size = 0.0175/share à p=0.5
        let expected_fee = 0.07 * p.entry_price * (1.0 - p.entry_price) * p.size;
        assert!((p.cost_basis - notional - expected_fee).abs() < 1e-9,
            "cost_basis doit inclure coef·p(1−p)·size");
    }

    #[test]
    fn kelly_binary_formula_exact() {
        // f* = edge/(1−c) : edge=0.10, c=0.50 → f_full=0.20, half-Kelly → f=0.10,
        // budget = min(200·0.10, 200·0.10)=20 → 40 tokens à 0.50.
        let p = params(); // kelly_fraction 0.5, max_size_pct 0.10
        assert_eq!(p.kelly_size_for(0.10, 0.50, 200.0), 40.0);
        // Favori clampé : c=0.95 → price_k=0.90 → f_full = 0.02/0.10 = 0.20 (fini, pas explosif)
        let sz = p.robust_kelly_size_for(0.02, 0.95, 200.0, 1.0, 0.0, 0.0);
        assert!(sz > 0.0 && sz <= (200.0f64 * 0.10 / 0.95).ceil());
    }

    #[test]
    fn breaker_trips_at_max_drawdown() {
        // capital 200, max_dd 20 → coupe à equity ≤ 180.
        assert!(!check_drawdown_breaker(185.0, 200.0, 20.0));
        assert!(check_drawdown_breaker(180.0, 200.0, 20.0));
        assert!(check_drawdown_breaker(175.0, 200.0, 20.0));
    }

    #[test]
    fn live_drawdown_uses_high_water_mark() {
        // bankroll réelle 18.44, max_dd 5 → coupe quand pic − courante ≥ 5.
        let mut dd = LiveDrawdown::default();
        assert!(!dd.breached(18.44, 5.0)); // 1er pic = 18.44
        assert!(!dd.breached(20.00, 5.0)); // pic monte à 20.00
        assert!(!dd.breached(16.00, 5.0)); // -4.00 depuis le pic → ok
        assert!(dd.breached(15.00, 5.0));  // -5.00 depuis le pic → coupe
    }

    #[test]
    fn live_pnl_is_delta_from_baseline() {
        let mut p = LivePnl::default();
        assert_eq!(p.update(18.44), 0.0);                 // référence posée
        assert!((p.update(20.44) - 2.0).abs() < 1e-9);    // +2.00
        assert!((p.update(17.44) + 1.0).abs() < 1e-9);    // -1.00
        p.reset();
        assert_eq!(p.update(17.44), 0.0);                 // nouvelle référence
    }

    #[test]
    fn size_min_adjustment() {
        // ≥ min → inchangé
        assert_eq!(adjust_size_to_min(8.0, 5.0), Some(8.0));
        // entre min/2 et min → arrondi au minimum
        assert_eq!(adjust_size_to_min(3.0, 5.0), Some(5.0));
        assert_eq!(adjust_size_to_min(2.5, 5.0), Some(5.0));
        // < min/2 → ignoré
        assert_eq!(adjust_size_to_min(2.0, 5.0), None);
    }
}
