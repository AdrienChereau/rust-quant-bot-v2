//! Vélocité du prix spot sur l'horizon 1 s (ΔP_1s) — P2.

use std::collections::VecDeque;

pub struct VelocityTracker {
    window_ms: u64,
    history: VecDeque<(u64, f64)>, // (ts_ms, mid)
}

impl VelocityTracker {
    pub fn new(window_ms: u64) -> Self {
        Self { window_ms, history: VecDeque::with_capacity(64) }
    }

    pub fn update(&mut self, ts_ms: u64, mid: f64) {
        self.history.push_back((ts_ms, mid));
        while let Some((ts, _)) = self.history.front() {
            if ts_ms.saturating_sub(*ts) > self.window_ms {
                self.history.pop_front();
            } else {
                break;
            }
        }
    }

    /// Variation relative du mid sur la fenêtre : (mid_now − mid_old) / mid_old.
    pub fn velocity(&self) -> f64 {
        if self.history.len() < 2 {
            return 0.0;
        }
        let (_, old) = *self.history.front().unwrap();
        let (_, now) = *self.history.back().unwrap();
        if old == 0.0 {
            return 0.0;
        }
        (now - old) / old
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn velocity_detects_drop() {
        let mut v = VelocityTracker::new(1000);
        v.update(0, 60000.0);
        v.update(500, 59940.0); // −0.10 %
        assert!((v.velocity() - (-0.001)).abs() < 1e-6);
    }

    #[test]
    fn flat_is_zero() {
        let mut v = VelocityTracker::new(1000);
        v.update(0, 60000.0);
        v.update(500, 60000.0);
        assert_eq!(v.velocity(), 0.0);
    }
}
