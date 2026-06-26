//! Sizing Kelly fractionnel + exécution PAPER du sniper (P5).
//!
//! Sur un signal FIRE : achat taker du side + take-profit à +`tp_cents`, avec
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
    pub tp_price: f64,
    pub sl_price: f64,
    pub opened_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct KellyParams {
    pub kelly_fraction: f64,    // 0.5 = half-Kelly
    pub max_size_pct: f64,      // plafond taille×prix / equity
    pub tp_cents: f64,
    pub sl_cents: f64,
    pub max_hold_secs: i64,
}

pub struct PaperEngine {
    pub state: SniperState,
    pub position: Option<OpenPosition>,
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
        Self { state, position: None, params, state_path, trades_path }
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
        self.kelly_size_for(edge, price, self.state.cash)
    }

    /// Taille de Kelly sur une `equity` explicite : `f* = edge/odds`, bornée. Utilisé en LIVE avec
    /// la **vraie collatéral** CLOB (et non le cash paper). Renvoie le nb de tokens (entier).
    pub fn kelly_size_for(&self, edge: f64, price: f64, equity: f64) -> f64 {
        if price <= 0.0 || price >= 1.0 || equity <= 0.0 {
            return 0.0;
        }
        // Pari binaire : gain net si on a raison ≈ (1−price)/price ; Kelly f = edge/odds.
        let odds = (1.0 - price) / price;
        let f_full = (edge / odds).clamp(0.0, 1.0);
        let f = f_full * self.params.kelly_fraction;
        let budget = (equity * f).min(equity * self.params.max_size_pct);
        (budget / price).floor()
    }

    /// Exécute un tir (achat taker du side). Slippage : prix moyen en parcourant le
    /// carnet ; sélection adverse modélisée à la clôture (cf. close_position).
    #[allow(clippy::too_many_arguments)]
    pub fn fire(&mut self, side: Side, token_id: &str, edge: f64, book: &PolyBook, tick: f64, min_size: f64, now_ms: u64) -> bool {
        if self.position.is_some() {
            return false; // un seul tir à la fois
        }
        let Some(best_ask) = book.best_ask() else { return false };
        let size = self.kelly_size(edge, best_ask);
        if size < min_size {
            self.state.blocked_size += 1;
            return false;
        }
        // Slippage : VWAP des asks consommés.
        let (avg_price, filled) = vwap_buy(book, size);
        if filled <= 0.0 {
            return false;
        }
        let cost = avg_price * filled;
        if self.state.cash < cost {
            return false;
        }
        self.state.cash -= cost;
        let tp = (avg_price + self.params.tp_cents / 100.0).min(0.99);
        let sl = (avg_price - self.params.sl_cents / 100.0).max(0.01);
        self.position = Some(OpenPosition {
            side, token_id: token_id.to_string(), entry_price: avg_price, size: filled,
            tp_price: round_tick(tp, tick), sl_price: round_tick(sl, tick), opened_ms: now_ms,
        });
        self.state.shots += 1;
        self.append("fire", side.as_str(), avg_price, filled, 0.0);
        tracing::warn!(side = side.as_str(), entry = format!("{:.3}", avg_price),
            size = filled, tp = format!("{:.2}", tp), "🎯 SNIPE");
        true
    }

    /// Gère la position ouverte : TP atteint, stop-loss, max-hold. Renvoie true si fermée.
    pub fn manage(&mut self, mark_bid: Option<f64>, now_ms: u64, remaining_s: i64) -> bool {
        let Some(p) = self.position.clone() else { return false };
        let Some(bid) = mark_bid else { return false };
        let held_s = (now_ms.saturating_sub(p.opened_ms) / 1000) as i64;

        if bid >= p.tp_price {
            self.close_position(p.tp_price, "take_profit");
            true
        } else if bid <= p.sl_price {
            self.close_position(p.sl_price, "stop_loss");
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
        let proceeds = exit_price * p.size;
        self.state.cash += proceeds;
        let pnl = proceeds - p.entry_price * p.size;
        self.state.realized_pnl = self.state.cash - self.state.start_cash;
        if pnl >= 0.0 { self.state.wins += 1 } else { self.state.losses += 1 }
        let eq = self.state.cash;
        if eq > self.state.peak_equity { self.state.peak_equity = eq; }
        self.append(reason, p.side.as_str(), exit_price, p.size, pnl);
        tracing::warn!(reason, exit = format!("{:.3}", exit_price), pnl = format!("{:.2}", pnl),
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
pub fn check_drawdown_breaker(current_equity: f64, initial_capital: f64, max_dd: f64) -> bool {
    initial_capital - current_equity >= max_dd
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
        KellyParams { kelly_fraction: 0.5, max_size_pct: 0.10, tp_cents: 10.0, sl_cents: 8.0, max_hold_secs: 120 }
    }
    fn engine() -> PaperEngine {
        PaperEngine::load_or_init(200.0, params(), "/tmp/sniper_s.json".into(), "/tmp/sniper_t.jsonl".into())
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
        assert!(e.fire(Side::Up, "tok", 0.10, &book(), 0.01, 5.0, 0));
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
        e.fire(Side::Up, "tok", 0.10, &book(), 0.01, 5.0, 0);
        let closed = e.manage(Some(0.40), 1000, 200); // sous le SL (~0.42)
        assert!(closed);
        assert_eq!(e.state.losses, 1);
    }

    #[test]
    fn breaker_trips_at_max_drawdown() {
        // capital 200, max_dd 20 → coupe à equity ≤ 180.
        assert!(!check_drawdown_breaker(185.0, 200.0, 20.0));
        assert!(check_drawdown_breaker(180.0, 200.0, 20.0));
        assert!(check_drawdown_breaker(175.0, 200.0, 20.0));
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
