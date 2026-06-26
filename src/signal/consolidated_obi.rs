//! OBI consolidé Binance + OKX (P3) — confirmation cross-exchange anti-bluff.
//!
//! ⚠️ Correction truth protocol : le déclenchement N'EST PAS une moyenne pondérée
//! (qui laisse Binance noyer un désaccord d'OKX — ex. +0.90/−0.10 → 0.55 > seuil →
//! tire à tort). C'est une **porte d'ACCORD** :
//!   - même signe sur les deux exchanges,
//!   - chacun au-dessus du `floor` en valeur absolue.
//! La **magnitude** pondérée `0.65·B + 0.35·OKX` ne sert qu'à doser le sizing.

use crate::concurrency::bus::Side;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConsolidatedDecision {
    pub fire: bool,
    pub side: Option<Side>,
    pub strength: f64, // magnitude pondérée, signée (pour le sizing)
}

pub struct ConsolidatedObi {
    floor: f64,
    fire_threshold: f64,
    w_binance: f64,
    w_okx: f64,
}

impl ConsolidatedObi {
    pub fn new(floor: f64, fire_threshold: f64, w_binance: f64, w_okx: f64) -> Self {
        Self { floor, fire_threshold, w_binance, w_okx }
    }

    /// Magnitude pondérée signée (sizing).
    pub fn magnitude(&self, obi_binance: f64, obi_okx: f64) -> f64 {
        self.w_binance * obi_binance + self.w_okx * obi_okx
    }

    pub fn evaluate(&self, obi_binance: f64, obi_okx: f64) -> ConsolidatedDecision {
        let same_sign = obi_binance.signum() == obi_okx.signum() && obi_binance != 0.0;
        let both_strong =
            obi_binance.abs() >= self.floor && obi_okx.abs() >= self.floor;
        let mag = self.magnitude(obi_binance, obi_okx);

        if same_sign && both_strong && mag.abs() >= self.fire_threshold {
            let side = if mag > 0.0 { Side::Up } else { Side::Down };
            ConsolidatedDecision { fire: true, side: Some(side), strength: mag }
        } else {
            ConsolidatedDecision { fire: false, side: None, strength: mag }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> ConsolidatedObi {
        // floor 0.50 par exchange, seuil de magnitude 0.55, pondération 0.65/0.35.
        ConsolidatedObi::new(0.50, 0.55, 0.65, 0.35)
    }

    #[test]
    fn bluff_binance_only_does_not_fire() {
        // L'exemple du PDF : Binance +0.90, OKX −0.10 → moyenne 0.55 MAIS désaccord → PAS de tir.
        let d = engine().evaluate(0.90, -0.10);
        assert!(!d.fire, "le bluff mono-exchange ne doit pas tirer");
    }

    #[test]
    fn both_ignite_fires_up() {
        let d = engine().evaluate(0.85, 0.80);
        assert!(d.fire);
        assert_eq!(d.side, Some(Side::Up));
    }

    #[test]
    fn both_negative_fires_down() {
        let d = engine().evaluate(-0.85, -0.80);
        assert!(d.fire);
        assert_eq!(d.side, Some(Side::Down));
    }

    #[test]
    fn okx_below_floor_blocks() {
        // Même signe mais OKX trop faible (< floor) → pas d'accord.
        let d = engine().evaluate(0.95, 0.30);
        assert!(!d.fire);
    }

    #[test]
    fn agreement_but_weak_magnitude_blocks() {
        // Les deux juste au floor → magnitude 0.50 < seuil 0.55 → pas de tir.
        let d = engine().evaluate(0.50, 0.50);
        assert!(!d.fire);
    }
}
