//! Moteur de drift / momentum (Radar Tokyo).
//!
//! Estime le **drift log par seconde** du micro-price via une EMA *décroissante dans
//! le temps* (robuste au tick rate : le flux Binance arrive à ~10 Hz mais peut jitter).
//! Ce drift alimente [`super::pricing::fair_up_probability_drift`] : c'est le correctif
//! validé en replay (Phase 2) qui empêche la juste valeur de rester ancrée à ~50/50
//! sur une fenêtre en tendance.
//!
//! Mise à jour, entre deux ticks séparés de `dt` secondes :
//!     rate  = ln(p / p_prev) / dt          (taux de dérive instantané, par seconde)
//!     decay = 0.5^(dt / halflife)          (poids temporel de l'ancienne valeur)
//!     μ     = μ_prev · decay + (1 − decay) · rate

pub struct DriftEngine {
    halflife_secs: f64,
    mu: f64,                  // drift log par seconde (EMA)
    last: Option<(u64, f64)>, // (ts_ms, micro_price)
    initialized: bool,
}

impl DriftEngine {
    pub fn new(halflife_secs: f64) -> Self {
        Self {
            halflife_secs: halflife_secs.max(1e-3),
            mu: 0.0,
            last: None,
            initialized: false,
        }
    }

    /// Intègre un nouveau point de micro-price.
    pub fn update(&mut self, ts_ms: u64, micro_price: f64) {
        if micro_price <= 0.0 {
            return;
        }
        if let Some((last_ts, last_px)) = self.last {
            let dt = (ts_ms.saturating_sub(last_ts)) as f64 / 1000.0;
            if dt > 0.0 && last_px > 0.0 {
                let rate = (micro_price / last_px).ln() / dt;
                let decay = 0.5_f64.powf(dt / self.halflife_secs);
                if self.initialized {
                    self.mu = self.mu * decay + (1.0 - decay) * rate;
                } else {
                    self.mu = rate;
                    self.initialized = true;
                }
            }
        }
        self.last = Some((ts_ms, micro_price));
    }

    /// Drift log par seconde courant.
    pub fn per_sec(&self) -> f64 {
        self.mu
    }

    /// Déplacement log attendu sur `secs` secondes (μ · secs), **non clampé** :
    /// l'appelant borne via [`super::pricing::clamp_drift`].
    #[allow(dead_code)] // utilisé par les tests
    pub fn drift_over(&self, secs: f64) -> f64 {
        self.mu * secs.max(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_series_has_zero_drift() {
        let mut d = DriftEngine::new(25.0);
        for i in 0..50 {
            d.update(i * 100, 60000.0);
        }
        assert!(d.per_sec().abs() < 1e-9, "mu={}", d.per_sec());
    }

    #[test]
    fn steady_uptrend_has_positive_drift() {
        let mut d = DriftEngine::new(25.0);
        let mut p = 60000.0;
        for i in 0..300 {
            p *= 1.0 + 5e-5; // hausse régulière
            d.update(i * 100, p);
        }
        assert!(d.per_sec() > 0.0, "mu={}", d.per_sec());
        // Le déplacement attendu sur 60 s va dans le bon sens.
        assert!(d.drift_over(60.0) > 0.0);
    }

    #[test]
    fn downtrend_has_negative_drift() {
        let mut d = DriftEngine::new(25.0);
        let mut p = 60000.0;
        for i in 0..300 {
            p *= 1.0 - 5e-5;
            d.update(i * 100, p);
        }
        assert!(d.per_sec() < 0.0, "mu={}", d.per_sec());
    }

    #[test]
    fn reacts_to_regime_change() {
        // Plat puis hausse : le drift doit devenir nettement positif.
        let mut d = DriftEngine::new(10.0);
        for i in 0..100 {
            d.update(i * 100, 60000.0);
        }
        let mut p = 60000.0;
        for i in 100..300 {
            p *= 1.0 + 1e-4;
            d.update(i * 100, p);
        }
        assert!(d.per_sec() > 1e-5, "mu={}", d.per_sec());
    }
}
