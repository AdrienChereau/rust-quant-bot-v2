//! Moteur d'**Order Flow Imbalance** (OFI) — Radar Tokyo.
//!
//! Contrairement à l'OBI (photo instantanée du déséquilibre du carnet), l'OFI mesure
//! le *flux signé* des files d'attente au meilleur bid/ask entre deux snapshots
//! (Cont, Kukanov & Stoikov 2014). C'est le signal le plus prédictif du **prochain
//! tick** BTC — donc de la toxicité des fills côté Polymarket.
//!
//! Incrément par snapshot (`P` prix, `q` taille) :
//!     e_b = q_b · 1{P_b ≥ P_b_prev} − q_b_prev · 1{P_b ≤ P_b_prev}
//!     e_a = q_a · 1{P_a ≤ P_a_prev} − q_a_prev · 1{P_a ≥ P_a_prev}
//!     OFI = e_b − e_a          (>0 = pression acheteuse)
//!
//! On maintient la somme glissante sur une fenêtre (`window_ms`, ~5 s comme le TFI),
//! plus une version normalisée dans [−1, 1] pour la composition du score.

use std::collections::VecDeque;

pub struct OfiEngine {
    window_ms: u64,
    history: VecDeque<(u64, f64)>,       // (ts_ms, incrément OFI)
    prev: Option<(f64, f64, f64, f64)>,  // (bid_px, bid_sz, ask_px, ask_sz)
}

impl OfiEngine {
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            history: VecDeque::with_capacity(256),
            prev: None,
        }
    }

    /// Intègre un snapshot top-of-book.
    pub fn update(&mut self, ts_ms: u64, bid_px: f64, bid_sz: f64, ask_px: f64, ask_sz: f64) {
        if let Some((pb, qb, pa, qa)) = self.prev {
            let e_b = if bid_px > pb {
                bid_sz
            } else if bid_px < pb {
                -qb
            } else {
                bid_sz - qb
            };
            let e_a = if ask_px < pa {
                ask_sz
            } else if ask_px > pa {
                -qa
            } else {
                ask_sz - qa
            };
            self.history.push_back((ts_ms, e_b - e_a));
            while let Some((ts, _)) = self.history.front() {
                if ts_ms.saturating_sub(*ts) > self.window_ms {
                    self.history.pop_front();
                } else {
                    break;
                }
            }
        }
        self.prev = Some((bid_px, bid_sz, ask_px, ask_sz));
    }

    /// OFI brut : somme signée des incréments sur la fenêtre (unités de taille).
    pub fn value_raw(&self) -> f64 {
        self.history.iter().map(|(_, v)| v).sum()
    }

    /// OFI normalisé dans [−1, 1] : somme signée / somme des valeurs absolues.
    pub fn value_norm(&self) -> f64 {
        let mut signed = 0.0;
        let mut abs = 0.0;
        for (_, v) in &self.history {
            signed += v;
            abs += v.abs();
        }
        if abs < 1e-12 {
            0.0
        } else {
            (signed / abs).clamp(-1.0, 1.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_flow_is_near_zero() {
        let mut o = OfiEngine::new(5000);
        for i in 0..20 {
            // Carnet stable : bid/ask prix et tailles constants → incréments nuls.
            o.update(i * 100, 100.0, 10.0, 101.0, 10.0);
        }
        assert!(o.value_raw().abs() < 1e-9);
        assert!(o.value_norm().abs() < 1e-9);
    }

    #[test]
    fn growing_bid_is_bullish() {
        let mut o = OfiEngine::new(5000);
        o.update(0, 100.0, 10.0, 101.0, 10.0);
        // La taille au bid grossit à prix constant → pression acheteuse.
        o.update(100, 100.0, 25.0, 101.0, 10.0);
        assert!(o.value_raw() > 0.0, "ofi={}", o.value_raw());
        assert!(o.value_norm() > 0.5);
    }

    #[test]
    fn ask_stepping_down_is_bearish() {
        let mut o = OfiEngine::new(5000);
        o.update(0, 100.0, 10.0, 101.0, 10.0);
        // L'ask descend (vente agressive) → pression vendeuse → OFI négatif.
        o.update(100, 100.0, 10.0, 100.5, 12.0);
        assert!(o.value_raw() < 0.0, "ofi={}", o.value_raw());
        assert!(o.value_norm() < 0.0);
    }

    #[test]
    fn bid_lifted_up_is_bullish() {
        let mut o = OfiEngine::new(5000);
        o.update(0, 100.0, 10.0, 101.0, 10.0);
        // Le bid monte (achat agressif) → OFI positif.
        o.update(100, 100.5, 8.0, 101.0, 10.0);
        assert!(o.value_raw() > 0.0, "ofi={}", o.value_raw());
    }

    #[test]
    fn window_purges_old_increments() {
        let mut o = OfiEngine::new(1000);
        for i in 0..50 {
            o.update(i * 100, 100.0, 10.0 + i as f64, 101.0, 10.0);
        }
        assert!(o.history.len() <= 11);
    }
}
