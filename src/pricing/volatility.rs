//! Volatilité annualisée glissante (porté du MM bot) pour le fair_up B&S.

use std::collections::VecDeque;

pub(crate) const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 3600.0;

/// Volatilité EWMA (RiskMetrics λ=0.94) — complémentaire à VolatilityTracker.
/// Blendée 50/50 avec la vol réalisée pour le pricing B&S.
///
/// ⚠️ La variance suivie est une **variance PAR SECONDE** : chaque tick contribue
/// `r²/dt` (variance instantanée annualisable), pas `r²` brut. Sans la division par
/// `dt`, à 100 Hz la σ serait sous-estimée d'un facteur √100 ≈ 10× (l'EWMA collerait
/// en permanence au `floor` et n'apporterait aucune information).
pub struct EwmaVolatility {
    pub lambda: f64,   // 0.94
    variance: f64,     // variance par seconde
    floor: f64,
    last_ms: u64,
    initialized: bool,
}

impl EwmaVolatility {
    pub fn new(lambda: f64, floor: f64) -> Self {
        Self { lambda, variance: 0.0, floor, last_ms: 0, initialized: false }
    }

    pub fn update(&mut self, now_ms: u64, log_return: f64) {
        if !self.initialized {
            self.last_ms = now_ms;
            self.initialized = true;
            return;
        }
        // dt borné : 1 ms (deux ticks même horodatage) → 1 s (gros trou de flux).
        let dt = (now_ms.saturating_sub(self.last_ms) as f64 / 1000.0).clamp(1e-3, 1.0);
        self.last_ms = now_ms;
        let inst_var_per_sec = log_return * log_return / dt;
        self.variance = self.lambda * self.variance + (1.0 - self.lambda) * inst_var_per_sec;
    }

    pub fn annualized_sigma(&self) -> f64 {
        (self.variance * SECONDS_PER_YEAR).sqrt().max(self.floor)
    }
}

#[cfg(test)]
mod ewma_tests {
    use super::*;

    #[test]
    fn dt_scaling_makes_sigma_meaningful() {
        // Returns ~5e-5 toutes les 10 ms (≈ 100 Hz). Avec dt-scaling la σ doit
        // dépasser largement le floor 0.0 (sinon l'EWMA serait inerte).
        let mut e = EwmaVolatility::new(0.94, 0.0);
        let mut now = 0u64;
        for _ in 0..500 {
            now += 10;
            e.update(now, 5e-5);
        }
        let sigma = e.annualized_sigma();
        // sans /dt : sigma ≈ 0.009 ; avec /dt (×100 en variance) : sigma ≈ 0.09
        assert!(sigma > 0.05, "σ={sigma} — l'EWMA doit réagir, pas coller au floor");
    }

    #[test]
    fn floor_respected_when_flat() {
        let mut e = EwmaVolatility::new(0.94, 0.80);
        for i in 1..100 { e.update(i * 10, 0.0); }
        assert_eq!(e.annualized_sigma(), 0.80);
    }
}

pub struct VolatilityTracker {
    window_ms: u64,
    floor: f64,
    history: VecDeque<(u64, f64)>,
}

impl VolatilityTracker {
    pub fn new(window_ms: u64, floor: f64) -> Self {
        Self { window_ms, floor, history: VecDeque::with_capacity(256) }
    }

    pub fn update(&mut self, ts_ms: u64, price: f64) {
        if price <= 0.0 {
            return;
        }
        self.history.push_back((ts_ms, price));
        while let Some((ts, _)) = self.history.front() {
            if ts_ms.saturating_sub(*ts) > self.window_ms {
                self.history.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn annualized_sigma(&self) -> f64 {
        if self.history.len() < 2 {
            return self.floor;
        }
        let first = self.history.front().unwrap().0;
        let last = self.history.back().unwrap().0;
        let span = (last.saturating_sub(first)) as f64 / 1000.0;
        if span <= 0.0 {
            return self.floor;
        }
        let mut rv = 0.0;
        let mut prev: Option<f64> = None;
        for (_, p) in &self.history {
            if let Some(pp) = prev {
                let r = (p / pp).ln();
                rv += r * r;
            }
            prev = Some(*p);
        }
        ((rv / span) * SECONDS_PER_YEAR).sqrt().max(self.floor)
    }
}
