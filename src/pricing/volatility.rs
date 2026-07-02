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
        // dt borné : 1 ms (deux ticks même horodatage) → 10 s (gros trou de flux).
        let dt = (now_ms.saturating_sub(self.last_ms) as f64 / 1000.0).clamp(1e-3, 10.0);
        self.last_ms = now_ms;
        let inst_var_per_sec = log_return * log_return / dt;
        self.variance = self.lambda * self.variance + (1.0 - self.lambda) * inst_var_per_sec;
    }

    pub fn annualized_sigma(&self) -> f64 {
        (self.variance * SECONDS_PER_YEAR).sqrt().max(self.floor)
    }
}

/// σ de DÉCISION (stratégie v1, docs/STRATEGY.md §2) : deux EWMA sur retours échantillonnés
/// à **1 s** — jamais des ticks (le bruit bid-ask gonfle la RV ×3-5, bug mesuré : σ 170 % vs
/// σ* calibrée 35 %). `σ̂ = max(rapide, lente)` : la rapide (λ=0.94/s ≈ 17 s de mémoire) monte
/// vite après un choc, la lente (λ=0.99/s ≈ 100 s) évite la sous-estimation prolongée.
/// ⚠️ La sémantique de λ dépend du pas d'update : appliquer λ=0.94 par tick à 100 Hz donnerait
/// ~170 ms de mémoire — d'où l'échantillonnage interne strict à 1 s.
pub struct DualEwmaVol {
    fast: EwmaVolatility,
    slow: EwmaVolatility,
    floor: f64,
    last_ms: u64,
    last_price: f64,
}

impl DualEwmaVol {
    pub fn new(lambda_fast: f64, lambda_slow: f64, floor: f64) -> Self {
        Self {
            fast: EwmaVolatility::new(lambda_fast, 0.0),
            slow: EwmaVolatility::new(lambda_slow, 0.0),
            floor,
            last_ms: 0,
            last_price: 0.0,
        }
    }

    /// À appeler à chaque tick — n'échantillonne qu'UN retour par seconde en interne.
    pub fn update(&mut self, now_ms: u64, price: f64) {
        if price <= 0.0 {
            return;
        }
        if self.last_price <= 0.0 {
            self.last_price = price;
            self.last_ms = now_ms;
            return;
        }
        if now_ms.saturating_sub(self.last_ms) < 1000 {
            return;
        }
        let r = (price / self.last_price).ln();
        self.fast.update(now_ms, r);
        self.slow.update(now_ms, r);
        self.last_ms = now_ms;
        self.last_price = price;
    }

    /// σ annualisée de décision : max(rapide, lente), plancher.
    pub fn annualized_sigma(&self) -> f64 {
        self.fast
            .annualized_sigma()
            .max(self.slow.annualized_sigma())
            .max(self.floor)
    }

    /// Composante rapide seule (diagnostic dashboard).
    pub fn fast_sigma(&self) -> f64 {
        self.fast.annualized_sigma().max(self.floor)
    }
}

#[cfg(test)]
mod dual_tests {
    use super::*;

    #[test]
    fn samples_at_one_hz_not_per_tick() {
        // 100 ticks à 10 Hz avec micro-oscillation bid-ask : sans échantillonnage 1 s,
        // la σ exploserait ; ici seuls ~10 retours 1 s (quasi nuls) sont pris.
        let mut v = DualEwmaVol::new(0.94, 0.99, 0.0);
        for i in 0..100u64 {
            let bounce = if i % 2 == 0 { 60_000.0 } else { 60_003.0 }; // ±5 bps de bounce
            v.update(i * 100, bounce);
        }
        // les retours 1 s sont ~0 ou ±5 bps → σ doit rester < 1.0 (le bounce tick donnerait >>1)
        assert!(v.annualized_sigma() < 1.0, "σ={}", v.annualized_sigma());
    }

    #[test]
    fn max_of_fast_and_slow_with_floor() {
        let mut v = DualEwmaVol::new(0.94, 0.99, 0.10);
        // marché plat → σ = plancher
        for i in 0..30u64 {
            v.update(i * 1000, 60_000.0);
        }
        assert_eq!(v.annualized_sigma(), 0.10);
        // choc : gros retours 1 s → la rapide monte, le max suit
        let mut px = 60_000.0;
        for i in 30..60u64 {
            px *= 1.001; // +10 bps/s ≈ 560 % annualisé
            v.update(i * 1000, px);
        }
        assert!(v.annualized_sigma() > 1.0, "σ={}", v.annualized_sigma());
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
