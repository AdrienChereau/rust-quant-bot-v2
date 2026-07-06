//! Bankroll & gestion du risque (R4).
//!
//! Equity, PnL de fenêtre, drawdown, et **décisions typées** (Allowed/Blocked) qui
//! déterminent si — et de quelle taille — un ordre est autorisé. MM **neutre** :
//! AUCUN gate d'edge directionnel ; on borne l'exposition, le cash et la perte.

#![allow(dead_code)] // module hérité (gates R4 conservés pour l'armement live)
use crate::config::Config;
use crate::connectors::polymarket::Market;
use crate::inventory::PaperState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum BlockReason {
    CashReserve,
    MaxNetExposure,
    MaxWindowLoss,
    MaxPosition,
    BelowMinOrder,
    NoInventory,
    EndWindow,
    Paused,
    PairedTooExpensive,
}

#[derive(Debug, Clone, Copy)]
pub enum TradeDecision {
    Allowed { size: f64 },
    Blocked { reason: BlockReason },
}

pub struct BankrollEngine {
    bankroll_fraction: f64,
    max_net_exposure_pct: f64,
    min_cash_reserve_pct: f64,
    max_window_loss_pct: f64,
    max_order_size: f64,
    max_position: f64,
    max_net_shares: f64, // cap de la jambe NETTE en parts (bug fix : |net|×mid s'effondrait)
    paired_buy_margin: f64,
    end_window_secs: i64,

    window_start_equity: f64,
    peak_equity: f64,
}

impl BankrollEngine {
    pub fn new(cfg: &Config) -> Self {
        Self {
            bankroll_fraction: cfg.bankroll_fraction,
            max_net_exposure_pct: cfg.max_net_exposure_pct,
            min_cash_reserve_pct: cfg.min_cash_reserve_pct,
            max_window_loss_pct: cfg.max_window_loss_pct,
            max_order_size: cfg.max_order_size,
            max_position: cfg.max_position,
            max_net_shares: cfg.max_net_shares,
            paired_buy_margin: cfg.paired_buy_margin,
            end_window_secs: 60,
            window_start_equity: cfg.start_cash,
            peak_equity: cfg.start_cash,
        }
    }

    // ── Métriques ──
    pub fn position_value(s: &PaperState, up_mid: f64, down_mid: f64) -> f64 {
        s.up_balance * up_mid + s.down_balance * down_mid
    }
    pub fn equity(s: &PaperState, up_mid: f64, down_mid: f64) -> f64 {
        s.cash_usdc + Self::position_value(s, up_mid, down_mid)
    }
    pub fn net_exposure(s: &PaperState) -> f64 {
        s.up_balance - s.down_balance
    }
    pub fn window_pnl(&self, equity: f64) -> f64 {
        equity - self.window_start_equity
    }
    pub fn drawdown_from_peak(&self, equity: f64) -> f64 {
        (self.peak_equity - equity).max(0.0)
    }

    pub fn on_window_start(&mut self, equity: f64) {
        self.window_start_equity = equity;
    }
    pub fn observe(&mut self, equity: f64) {
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }
    }

    /// Taille d'ordre brute selon l'equity, plafonnée cash/expo/position, plancher
    /// marché. Renvoie 0 si rien d'autorisé (le filtrage min se fait dans evaluate_*).
    fn raw_size(&self, equity: f64, price: f64, cash: f64, side_balance: f64) -> f64 {
        if price <= 0.0 {
            return 0.0;
        }
        let by_equity = equity * self.bankroll_fraction / price;
        let cash_avail = (cash - equity * self.min_cash_reserve_pct).max(0.0);
        let by_cash = cash_avail / price;
        let by_position = (self.max_position - side_balance).max(0.0);
        by_equity
            .min(by_cash)
            .min(by_position)
            .min(self.max_order_size)
    }

    /// Décision d'ACHAT (maker) d'un côté. `adds_to_net_abs` = l'achat augmente-t-il |net| ?
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_buy(
        &self,
        s: &PaperState,
        equity: f64,
        price: f64,
        side_mid: f64,
        side_balance: f64,
        remaining_s: i64,
        adds_to_net_abs: bool,
        market: &Market,
    ) -> TradeDecision {
        // Garde-fou fin de fenêtre : pas d'augmentation d'expo nette.
        if remaining_s < self.end_window_secs && adds_to_net_abs {
            return TradeDecision::Blocked { reason: BlockReason::EndWindow };
        }
        // Stop perte de fenêtre.
        if self.window_pnl(equity) < -self.max_window_loss_pct * self.window_start_equity.max(1.0) {
            return TradeDecision::Blocked { reason: BlockReason::MaxWindowLoss };
        }
        // Plafond d'exposition nette EN PARTS (si l'ordre l'aggrave). Le risque d'une
        // jambe nue est son nombre de parts (elle vaut 0 ou 1 à la résolution), PAS
        // `|net|×mid` qui s'effondre pour un côté cheap et autorisait ~200 parts nues.
        if adds_to_net_abs && Self::net_exposure(s).abs() >= self.max_net_shares {
            return TradeDecision::Blocked { reason: BlockReason::MaxNetExposure };
        }
        let _ = (side_mid, self.max_net_exposure_pct); // conservés pour compat/monitoring
        if side_balance >= self.max_position {
            return TradeDecision::Blocked { reason: BlockReason::MaxPosition };
        }
        if s.cash_usdc <= equity * self.min_cash_reserve_pct {
            return TradeDecision::Blocked { reason: BlockReason::CashReserve };
        }
        // Arrondi à l'entier de tokens (incrément de TAILLE, pas le tick de PRIX).
        let size = self.raw_size(equity, price, s.cash_usdc, side_balance).floor();
        // Plancher = minimum d'ordre dur du marché (rewards_min_size est un seuil
        // d'ÉLIGIBILITÉ aux rewards, pas de validité d'ordre).
        if size < market.min_order_size {
            return TradeDecision::Blocked { reason: BlockReason::BelowMinOrder };
        }
        TradeDecision::Allowed { size }
    }

    /// Décision de VENTE (maker) — on ne vend que ce qu'on détient.
    pub fn evaluate_sell(&self, side_balance: f64, market: &Market) -> TradeDecision {
        if side_balance <= 0.0 {
            return TradeDecision::Blocked { reason: BlockReason::NoInventory };
        }
        let size = side_balance.min(self.max_order_size).floor();
        // Si on détient moins que le minimum d'ordre, on liquide quand même tout
        // (sortie de position) — sinon on resterait coincé avec du résidu.
        if size <= 0.0 {
            return TradeDecision::Blocked { reason: BlockReason::BelowMinOrder };
        }
        let _ = market;
        TradeDecision::Allowed { size }
    }

    /// Achat pairé arbitrage : seulement si up_ask + down_ask < 1 − marge.
    pub fn evaluate_paired_buy(&self, up_ask: f64, down_ask: f64) -> TradeDecision {
        if up_ask + down_ask < 1.0 - self.paired_buy_margin {
            TradeDecision::Allowed { size: 0.0 } // taille gérée par l'appelant
        } else {
            TradeDecision::Blocked { reason: BlockReason::PairedTooExpensive }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    fn market() -> Market {
        Market {
            condition_id: "c".into(),
            slug: "s".into(),
            up_token_id: "u".into(),
            down_token_id: "d".into(),
            up_price: 0.5,
            down_price: 0.5,
            end_time: Utc::now() + Duration::seconds(300),
            window_ts: 0,
            rewards_max_spread: 4.5,
            rewards_min_size: 50.0,
            tick_size: 0.01,
            min_order_size: 5.0,
            neg_risk: false,
        }
    }

    fn cfg() -> Config {
        std::env::set_var("START_CASH", "200");
        Config::from_env()
    }

    fn state(cash: f64, up: f64, down: f64) -> PaperState {
        PaperState { cash_usdc: cash, up_balance: up, down_balance: down, ..Default::default() }
    }

    #[test]
    fn buy_allowed_with_capital() {
        let b = BankrollEngine::new(&cfg());
        let s = state(200.0, 0.0, 0.0);
        // equity 200, fraction 0.02, price 0.5 → ~8 tokens ≥ min 5 → autorisé.
        match b.evaluate_buy(&s, 200.0, 0.5, 0.5, 0.0, 200, true, &market()) {
            TradeDecision::Allowed { size } => assert!(size >= 5.0),
            d => panic!("attendu Allowed, eu {d:?}"),
        }
    }

    #[test]
    fn end_window_blocks_net_increasing_buy() {
        let b = BankrollEngine::new(&cfg());
        let s = state(200.0, 0.0, 0.0);
        let d = b.evaluate_buy(&s, 200.0, 0.5, 0.5, 0.0, 30, true, &market());
        assert!(matches!(d, TradeDecision::Blocked { reason: BlockReason::EndWindow }));
    }

    #[test]
    fn cash_reserve_blocks_buy() {
        let b = BankrollEngine::new(&cfg());
        // cash sous la réserve (25% de l'equity).
        let s = state(10.0, 0.0, 0.0);
        let d = b.evaluate_buy(&s, 200.0, 0.5, 0.5, 0.0, 200, true, &market());
        assert!(matches!(d, TradeDecision::Blocked { reason: BlockReason::CashReserve }));
    }

    #[test]
    fn sell_blocked_without_inventory() {
        let b = BankrollEngine::new(&cfg());
        assert!(matches!(
            b.evaluate_sell(0.0, &market()),
            TradeDecision::Blocked { reason: BlockReason::NoInventory }
        ));
    }
}
