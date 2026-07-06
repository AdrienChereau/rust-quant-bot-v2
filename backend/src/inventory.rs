//! Inventaire & exécution PAPIER (J8).
//!
//! - Simule les fills quand le carnet croise nos quotes (sur Up ET Down).
//! - Suit cash / balances Up,Down / PnL réalisé & latent.
//! - **Fusion CTF (vélocité du capital)** : `check_and_merge` détruit les paires
//!   Up+Down → +1 USDC chacune, sous garde-fou gas (`should_merge`).
//! - Résolution à l'échéance (le côté gagnant vaut 1, l'autre 0).
//! - Persistance JSON **atomique** (écriture temp + rename) — on n'écrase jamais
//!   l'état pour "reset" ; log des trades append-only (`.jsonl`).

use std::fs;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PaperState {
    pub cash_usdc: f64,
    pub up_balance: f64,
    pub down_balance: f64,
    pub realized_pnl: f64,
    pub fills: u64,
    #[serde(default)]
    pub sells: u64,
    #[serde(default)]
    pub maker_fills: u64,
    #[serde(default)]
    pub taker_fills: u64,
    pub merges: u64,
    pub markets_resolved: u64,
}

/// Coût de gas estimé d'une fusion CTF (USDC). En paper, modèle statique ;
/// en réel (J11) : `eth_estimateGas × gas_price × POL_usd`.
pub fn estimate_merge_gas_usdc() -> f64 {
    // ~150k gas × ~50 gwei × prix POL ≈ < 0,01 $ (cf. plan).
    0.01
}

pub struct PaperEngine {
    pub state: PaperState,
    start_cash: f64,
    max_position: f64,
    min_merge_threshold: f64,
    safety_mult: f64,
    state_path: String,
    trades_path: String,
}

#[derive(Serialize)]
struct TradeRecord<'a> {
    ts: String,
    kind: &'a str,
    side: &'a str,
    liquidity_type: &'a str,
    price: f64,
    size: f64,
    cash_after: f64,
}

impl PaperEngine {
    pub fn load_or_init(
        start_cash: f64,
        max_position: f64,
        min_merge_threshold: f64,
        safety_mult: f64,
        state_path: String,
        trades_path: String,
    ) -> Self {
        let state = fs::read_to_string(&state_path)
            .ok()
            .and_then(|s| serde_json::from_str::<PaperState>(&s).ok())
            .unwrap_or(PaperState {
                cash_usdc: start_cash,
                ..Default::default()
            });
        tracing::info!(
            cash = state.cash_usdc, up = state.up_balance, down = state.down_balance,
            fills = state.fills, merges = state.merges, "État paper chargé"
        );
        Self {
            state,
            start_cash,
            max_position,
            min_merge_threshold,
            safety_mult,
            state_path,
            trades_path,
        }
    }

    fn side_balance(&self, side: &str) -> f64 {
        if side == "up" { self.state.up_balance } else { self.state.down_balance }
    }

    /// Applique un ACHAT de `size` tokens à `price` (R3 : taille et prix décidés en
    /// amont par execution.rs/bankroll). Vérifie cash et plafond de position.
    pub fn try_buy(&mut self, side: &str, price: f64, size: f64, liquidity_type: &str) -> bool {
        if size <= 0.0 {
            return false;
        }
        let cost = size * price;
        if self.state.cash_usdc < cost {
            return false;
        }
        if self.side_balance(side) + size > self.max_position {
            return false; // hard-limit position
        }
        self.state.cash_usdc -= cost;
        match side {
            "up" => self.state.up_balance += size,
            _ => self.state.down_balance += size,
        }
        self.state.fills += 1;
        if liquidity_type == "maker" { self.state.maker_fills += 1 } else { self.state.taker_fills += 1 }
        self.append_trade("buy", side, liquidity_type, price, size);
        true
    }

    /// Applique une VENTE de `size` tokens à `price`. Pas de vente à découvert :
    /// on ne vend que ce qu'on détient.
    #[allow(dead_code)] // réservé au chemin live
    pub fn try_sell(&mut self, side: &str, price: f64, size: f64, liquidity_type: &str) -> bool {
        let held = self.side_balance(side);
        let qty = size.min(held);
        if qty <= 0.0 {
            return false;
        }
        self.state.cash_usdc += qty * price;
        match side {
            "up" => self.state.up_balance -= qty,
            _ => self.state.down_balance -= qty,
        }
        self.state.fills += 1;
        self.state.sells += 1;
        if liquidity_type == "maker" { self.state.maker_fills += 1 } else { self.state.taker_fills += 1 }
        self.append_trade("sell", side, liquidity_type, price, qty);
        true
    }

    /// Décide si la fusion vaut le coût de gas (vélocité du capital).
    /// gain ≈ collatéral libéré × rendement attendu sur le temps restant.
    pub fn should_merge(&self, mergeable: f64, expected_yield_per_usdc: f64) -> bool {
        if mergeable < self.min_merge_threshold {
            return false;
        }
        let gas = estimate_merge_gas_usdc();
        let freed = mergeable; // 1 USDC par paire
        let gain = freed * expected_yield_per_usdc;
        gain >= self.safety_mult * gas
    }

    /// Fusionne les paires Up+Down disponibles si rentable. `expected_yield_per_usdc`
    /// vient du moteur de reward (J7). Retourne le montant fusionné.
    pub fn check_and_merge(&mut self, expected_yield_per_usdc: f64) -> f64 {
        let mergeable = self.state.up_balance.min(self.state.down_balance);
        if !self.should_merge(mergeable, expected_yield_per_usdc) {
            return 0.0;
        }
        self.state.up_balance -= mergeable;
        self.state.down_balance -= mergeable;
        self.state.cash_usdc += mergeable; // 1 USDC par paire détruite
        self.state.merges += 1;
        self.append_trade("merge", "ctf", "n/a", 1.0, mergeable);
        tracing::info!(
            merged = mergeable, cash = self.state.cash_usdc,
            "[CTF] Fusion — collatéral libéré"
        );
        mergeable
    }

    /// **Garde-fou 3 (TTE Killswitch)** — aplatit la jambe NETTE non-matchée près de
    /// l'expiry en la vendant (taker) au bid de son carnet.
    ///
    /// Les paires matchées `min(up, down)` valent 1 USDC à la résolution : on les laisse
    /// intactes (ou la fusion CTF s'en charge). On ne liquide que le **résidu
    /// directionnel** `|up − down|`, sinon il roule jusqu'à la résolution et paie 0 si
    /// c'est le côté perdant (bug observé : jambe nue tenue jusqu'au bout → perte sèche).
    /// Retourne la taille aplatie.
    #[allow(dead_code)] // réservé au chemin live
    pub fn flatten_net(&mut self, up_bid: Option<f64>, down_bid: Option<f64>) -> f64 {
        let net = self.state.up_balance - self.state.down_balance;
        if net > 1e-9 {
            if let Some(px) = up_bid {
                let qty = net.min(self.state.up_balance);
                if self.try_sell("up", px.max(0.0), qty, "taker") {
                    return qty;
                }
            }
        } else if net < -1e-9 {
            if let Some(px) = down_bid {
                let qty = (-net).min(self.state.down_balance);
                if self.try_sell("down", px.max(0.0), qty, "taker") {
                    return qty;
                }
            }
        }
        0.0
    }

    /// Résolution à l'échéance : le côté gagnant vaut 1 USDC/token, l'autre 0.
    pub fn resolve(&mut self, up_won: bool) {
        let payout = if up_won {
            self.state.up_balance
        } else {
            self.state.down_balance
        };
        // Coût de base déjà déduit du cash à l'achat → le payout est du cash brut.
        self.state.cash_usdc += payout;
        self.state.realized_pnl = self.state.cash_usdc - self.start_cash;
        self.append_trade("resolve", if up_won { "up" } else { "down" }, "n/a", 1.0, payout);
        self.state.up_balance = 0.0;
        self.state.down_balance = 0.0;
        self.state.markets_resolved += 1;
        tracing::info!(
            up_won, payout, cash = self.state.cash_usdc,
            realized_pnl = self.state.realized_pnl, "Marché résolu"
        );
    }

    /// PnL latent : valeur de marché des positions (mid) − déjà payé est implicite.
    #[allow(dead_code)] // réservé au chemin live
    pub fn mark_to_market(&self, up_mid: f64, down_mid: f64) -> f64 {
        self.state.up_balance * up_mid + self.state.down_balance * down_mid
    }

    /// Écriture atomique de l'état : fichier temporaire puis rename.
    pub fn persist(&self) {
        let tmp = format!("{}.tmp", self.state_path);
        match serde_json::to_string_pretty(&self.state) {
            Ok(json) => {
                if fs::write(&tmp, json).is_ok() && fs::rename(&tmp, &self.state_path).is_err() {
                    tracing::error!("rename de l'état paper échoué");
                }
            }
            Err(e) => tracing::error!(error = %e, "sérialisation état"),
        }
    }

    fn append_trade(&self, kind: &str, side: &str, liquidity_type: &str, price: f64, size: f64) {
        let rec = TradeRecord {
            ts: chrono::Utc::now().to_rfc3339(),
            kind,
            side,
            liquidity_type,
            price,
            size,
            cash_after: self.state.cash_usdc,
        };
        if let Ok(line) = serde_json::to_string(&rec) {
            if let Ok(mut f) = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(Path::new(&self.trades_path))
            {
                let _ = writeln!(f, "{line}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> PaperEngine {
        PaperEngine::load_or_init(
            100.0,
            1000.0, // max_position
            5.0,
            3.0,
            "/tmp/test_state_nonexistent.json".into(),
            "/tmp/test_trades_nonexistent.jsonl".into(),
        )
    }

    #[test]
    fn buy_applies_cost_and_balance() {
        let mut e = engine();
        e.state.cash_usdc = 100.0;
        assert!(e.try_buy("up", 0.25, 50.0, "maker"));
        assert_eq!(e.state.up_balance, 50.0);
        assert!((e.state.cash_usdc - (100.0 - 50.0 * 0.25)).abs() < 1e-9);
        assert_eq!(e.state.maker_fills, 1);
    }

    #[test]
    fn buy_blocked_when_insufficient_cash() {
        let mut e = engine();
        e.state.cash_usdc = 5.0;
        assert!(!e.try_buy("up", 0.50, 50.0, "maker")); // coût 25 > cash 5
        assert_eq!(e.state.up_balance, 0.0);
    }

    #[test]
    fn buy_blocked_at_max_position() {
        let mut e = PaperEngine::load_or_init(
            1000.0, 60.0, 5.0, 3.0,
            "/tmp/t_s.json".into(), "/tmp/t_t.jsonl".into(),
        );
        e.state.up_balance = 40.0;
        assert!(!e.try_buy("up", 0.10, 50.0, "maker")); // 40+50 > 60
    }

    #[test]
    fn sell_reduces_balance_increases_cash() {
        let mut e = engine();
        e.state.up_balance = 40.0;
        e.state.cash_usdc = 10.0;
        assert!(e.try_sell("up", 0.60, 30.0, "maker"));
        assert_eq!(e.state.up_balance, 10.0);
        assert!((e.state.cash_usdc - (10.0 + 30.0 * 0.60)).abs() < 1e-9);
        assert_eq!(e.state.sells, 1);
    }

    #[test]
    fn no_short_selling() {
        let mut e = engine();
        e.state.up_balance = 0.0;
        assert!(!e.try_sell("up", 0.60, 30.0, "maker"));
        assert_eq!(e.state.up_balance, 0.0);
    }

    #[test]
    fn merge_frees_collateral() {
        let mut e = engine();
        e.state.up_balance = 50.0;
        e.state.down_balance = 30.0;
        e.state.cash_usdc = 10.0;
        let merged = e.check_and_merge(1.0); // bon rendement → rentable
        assert_eq!(merged, 30.0);
        assert_eq!(e.state.up_balance, 20.0);
        assert_eq!(e.state.down_balance, 0.0);
        assert!((e.state.cash_usdc - 40.0).abs() < 1e-9);
    }

    #[test]
    fn no_merge_below_threshold() {
        let mut e = engine();
        e.state.up_balance = 3.0; // < min_merge_threshold (5)
        e.state.down_balance = 3.0;
        assert_eq!(e.check_and_merge(1.0), 0.0);
    }

    #[test]
    fn flatten_sells_naked_excess_only() {
        // 100 Up / 30 Down → 70 Up nus. Flatten vend 70 Up au bid, laisse 30 paires.
        let mut e = engine();
        e.state.cash_usdc = 0.0;
        e.state.up_balance = 100.0;
        e.state.down_balance = 30.0;
        let flat = e.flatten_net(Some(0.90), Some(0.05));
        assert!((flat - 70.0).abs() < 1e-9);
        assert!((e.state.up_balance - 30.0).abs() < 1e-9);
        assert!((e.state.down_balance - 30.0).abs() < 1e-9); // paires intactes
        assert!((e.state.cash_usdc - 63.0).abs() < 1e-9); // 70 × 0.90
        assert_eq!(e.state.taker_fills, 1);
    }

    #[test]
    fn flatten_short_down_excess() {
        let mut e = engine();
        e.state.up_balance = 20.0;
        e.state.down_balance = 127.0; // net = −107 Down nus
        let flat = e.flatten_net(Some(0.98), Some(0.02));
        assert!((flat - 107.0).abs() < 1e-9);
        assert!((e.state.down_balance - 20.0).abs() < 1e-9);
    }

    #[test]
    fn flatten_noop_when_flat() {
        let mut e = engine();
        e.state.up_balance = 40.0;
        e.state.down_balance = 40.0;
        assert_eq!(e.flatten_net(Some(0.5), Some(0.5)), 0.0);
    }

    #[test]
    fn resolve_pays_winning_side() {
        let mut e = engine();
        e.state.cash_usdc = 50.0;
        e.state.up_balance = 40.0;
        e.state.down_balance = 40.0;
        e.resolve(true); // Up gagne → +40 cash
        assert!((e.state.cash_usdc - 90.0).abs() < 1e-9);
        assert_eq!(e.state.up_balance, 0.0);
    }
}
