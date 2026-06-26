//! Pricing Black-Scholes binaire (P3) — porté de `polymarket-monolith`.
//! P(Up) = N(d2), d2 = (ln(spot/strike) − ½σ²t) / (σ√t).

use statrs::distribution::{ContinuousCDF, Normal};

fn normal_cdf(x: f64) -> f64 {
    Normal::new(0.0, 1.0).unwrap().cdf(x)
}

/// Probabilité "Up" (fair_up). Robuste aux cas dégénérés (t→0, σ→0).
pub fn fair_up_probability(spot: f64, strike: f64, sigma_annual: f64, t_years: f64) -> f64 {
    if spot <= 0.0 || strike <= 0.0 {
        return 0.5;
    }
    if t_years <= 0.0 || sigma_annual <= 0.0 {
        return if spot > strike { 1.0 } else if spot < strike { 0.0 } else { 0.5 };
    }
    let d2 = ((spot / strike).ln() - 0.5 * sigma_annual * sigma_annual * t_years)
        / (sigma_annual * t_years.sqrt());
    normal_cdf(d2)
}

pub fn years_from_secs(secs: f64) -> f64 {
    secs / (365.0 * 24.0 * 3600.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    const Y5: f64 = 300.0 / (365.0 * 24.0 * 3600.0);

    #[test]
    fn atm_near_half() {
        assert!((fair_up_probability(60000.0, 60000.0, 0.8, Y5) - 0.5).abs() < 0.02);
    }
    #[test]
    fn itm_high() {
        assert!(fair_up_probability(60500.0, 60000.0, 0.8, Y5) > 0.7);
    }
    #[test]
    fn expiry_indicator() {
        assert_eq!(fair_up_probability(60001.0, 60000.0, 0.8, 0.0), 1.0);
    }
}
