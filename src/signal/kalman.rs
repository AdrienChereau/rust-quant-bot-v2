//! Filtre de Kalman 2D scalaire : état [prix (USD), vélocité (USD/s)].
//! Gate Mahalanobis : spike flash ignoré sans polluer la covariance.
//! Reset forcé après N rejets consécutifs : évite le lockout sur un vrai plateau post-annonce.

pub struct KalmanFilter {
    pub x0: f64,
    pub x1: f64,
    p00: f64,
    p01: f64,
    p11: f64,
    q00: f64,
    q11: f64,
    r: f64,
    spike_sigma: f64,
    last_ms: u64,
    initialized: bool,
    consecutive_rejected: u32,
    reset_after_n: u32,
}

impl KalmanFilter {
    pub fn new(q00: f64, q11: f64, r: f64, spike_sigma: f64, reset_after_n: u32) -> Self {
        Self {
            x0: 0.0, x1: 0.0,
            p00: q00 * 100.0, p01: 0.0, p11: q11 * 100.0,
            q00, q11, r, spike_sigma,
            last_ms: 0, initialized: false,
            consecutive_rejected: 0, reset_after_n,
        }
    }

    pub fn update(&mut self, now_ms: u64, mid: f64) {
        if !self.initialized {
            self.x0 = mid;
            self.last_ms = now_ms;
            self.initialized = true;
            return;
        }
        let dt = (now_ms.saturating_sub(self.last_ms) as f64 / 1000.0).min(0.5);
        self.last_ms = now_ms;

        // Predict
        let x0p = self.x0 + dt * self.x1;
        let x1p = self.x1;
        let p00p = self.p00 + dt * (self.p01 + self.p01) + dt * dt * self.p11 + self.q00;
        let p01p = self.p01 + dt * self.p11;
        let p11p = self.p11 + self.q11;

        // Gate Mahalanobis
        let inn = mid - x0p;
        let inn_sigma = (p00p + self.r).sqrt();
        if inn.abs() > self.spike_sigma * inn_sigma {
            self.consecutive_rejected += 1;
            if self.consecutive_rejected < self.reset_after_n {
                return; // spike transitoire : P inchangé, zéro ringing
            }
            // Vrai plateau (annonce macro) → reset forcé
            self.x0 = mid;
            self.x1 = 0.0;
            self.p00 = self.q00 * 100.0;
            self.p01 = 0.0;
            self.p11 = self.q11 * 100.0;
            self.consecutive_rejected = 0;
            return;
        }
        self.consecutive_rejected = 0;

        // Update
        let s = p00p + self.r;
        let k0 = p00p / s;
        let k1 = p01p / s;
        self.x0 = x0p + k0 * inn;
        self.x1 = x1p + k1 * inn;
        self.p00 = (1.0 - k0) * p00p;
        self.p01 = (1.0 - k0) * p01p;
        self.p11 = p11p - k1 * p01p;
    }

    pub fn velocity(&self) -> f64 {
        self.x1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter() -> KalmanFilter {
        KalmanFilter::new(0.09, 0.01, 25.0, 5.0, 10)
    }

    #[test]
    fn tracks_constant_price() {
        let mut k = filter();
        for i in 0u64..100 {
            k.update(i * 10, 60000.0);
        }
        assert!((k.x0 - 60000.0).abs() < 1.0, "x0={}", k.x0);
        assert!(k.x1.abs() < 1.0, "x1={}", k.x1);
    }

    #[test]
    fn single_spike_rejected() {
        let mut k = filter();
        for i in 0u64..50 {
            k.update(i * 10, 60000.0);
        }
        let vel_before = k.x1;
        k.update(510, 65000.0); // spike isolé +5000 USD
        assert!((k.x1 - vel_before).abs() < 0.5, "spike doit être absorbé");
    }

    #[test]
    fn reset_after_sustained_deviation() {
        let mut k = filter();
        for i in 0u64..50 {
            k.update(i * 10, 60000.0);
        }
        // 15 ticks consécutifs hors gate → reset forcé après reset_after_n=10
        for i in 0u64..15 {
            k.update(510 + i * 10, 65000.0);
        }
        assert!((k.x0 - 65000.0).abs() < 200.0, "x0 devrait snapper à 65000, x0={}", k.x0);
    }
}
