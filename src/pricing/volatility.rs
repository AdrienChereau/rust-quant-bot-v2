//! Volatilité annualisée glissante (porté du MM bot) pour le fair_up B&S.

use std::collections::VecDeque;

pub(crate) const SECONDS_PER_YEAR: f64 = 365.0 * 24.0 * 3600.0;

/// Volatilité EWMA (RiskMetrics λ=0.94) — complémentaire à VolatilityTracker.
/// Blendée 50/50 avec la vol réalisée pour le pricing B&S.
pub struct EwmaVolatility {
    pub lambda: f64,   // 0.94
    variance: f64,
    floor: f64,
}

impl EwmaVolatility {
    pub fn new(lambda: f64, floor: f64) -> Self {
        Self { lambda, variance: 0.0, floor }
    }

    pub fn update(&mut self, log_return: f64) {
        self.variance = self.lambda * self.variance + (1.0 - self.lambda) * log_return * log_return;
    }

    pub fn annualized_sigma(&self) -> f64 {
        (self.variance * SECONDS_PER_YEAR).sqrt().max(self.floor)
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
