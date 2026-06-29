//! Signal basis cross-exchange (mid_OKX − mid_BNB) : EWMA + staleness.
//! Si OKX trop ancien (> stale_ms) : (0.0, 1.0) — poids basis nul dans le score composite.

pub struct BasisSignal {
    stale_ms: u64,   // ex. 80 ms
    threshold: f64,  // amplitude USD pour normalisation (ex. 20.0)
    lambda: f64,     // EWMA (ex. 0.90)
    smoothed: f64,
}

impl BasisSignal {
    pub fn new(stale_ms: u64, threshold: f64, lambda: f64) -> Self {
        Self { stale_ms, threshold, lambda, smoothed: 0.0 }
    }

    /// Retourne `(basis_norm ∈ [-1,1], uncertainty ∈ [0,1])`.
    pub fn evaluate(
        &mut self,
        mid_bnb: f64,
        mid_okx: f64,
        okx_ts_ms: u64,
        now_ms: u64,
    ) -> (f64, f64) {
        let age = now_ms.saturating_sub(okx_ts_ms);
        if age > self.stale_ms {
            return (0.0, 1.0);
        }
        let raw = mid_okx - mid_bnb;
        self.smoothed = self.lambda * self.smoothed + (1.0 - self.lambda) * raw;
        let norm = (self.smoothed / self.threshold).clamp(-1.0, 1.0);
        let unc = (age as f64 / self.stale_ms as f64).min(1.0);
        (norm, unc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_returns_zero_max_unc() {
        let mut b = BasisSignal::new(80, 20.0, 0.90);
        // okx_ts=0, now=200 → age=200 > 80
        let (norm, unc) = b.evaluate(60000.0, 60020.0, 0, 200);
        assert_eq!(norm, 0.0);
        assert_eq!(unc, 1.0);
    }

    #[test]
    fn fresh_basis_converges() {
        let mut b = BasisSignal::new(80, 20.0, 0.90);
        for i in 0u64..30 {
            b.evaluate(60000.0, 60020.0, i * 10, i * 10);
        }
        let (norm, unc) = b.evaluate(60000.0, 60020.0, 300, 300);
        assert!(norm > 0.8, "norm={norm}");
        assert!(unc < 1.0);
    }
}
