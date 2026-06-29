//! Score composite continu ∈ [-1, 1] : OBI multilevel + TFI + vélocité Kalman + basis.
//! Poids du basis réduit proportionnellement à son incertitude (staleness OKX).

pub struct CompositeWeights {
    pub w_obi: f64,    // 0.40
    pub w_tfi: f64,    // 0.30
    pub w_kalman: f64, // 0.20
    pub w_basis: f64,  // 0.10 (nominal)
}

impl Default for CompositeWeights {
    fn default() -> Self {
        Self { w_obi: 0.40, w_tfi: 0.30, w_kalman: 0.20, w_basis: 0.10 }
    }
}

/// Score composite normalisé ∈ [-1, 1].
pub fn score(
    obi: f64,
    tfi: f64,
    vel_norm: f64,
    basis_norm: f64,
    basis_unc: f64,
    w: &CompositeWeights,
) -> f64 {
    let eff_w_basis = w.w_basis * (1.0 - basis_unc);
    let total_w = w.w_obi + w.w_tfi + w.w_kalman + eff_w_basis;
    if total_w <= 0.0 {
        return 0.0;
    }
    ((w.w_obi * obi + w.w_tfi * tfi + w.w_kalman * vel_norm + eff_w_basis * basis_norm)
        / total_w)
        .clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_bullish_gives_positive() {
        let w = CompositeWeights::default();
        assert!(score(0.8, 0.7, 0.6, 0.5, 0.0, &w) > 0.6);
    }

    #[test]
    fn stale_basis_not_counted() {
        let w = CompositeWeights::default();
        let s_fresh = score(0.5, 0.5, 0.5, 1.0, 0.0, &w);
        let s_stale = score(0.5, 0.5, 0.5, 1.0, 1.0, &w);
        assert!((s_fresh - s_stale).abs() < 0.10);
    }

    #[test]
    fn clamped_to_unit_interval() {
        let w = CompositeWeights::default();
        assert_eq!(score(1.0, 1.0, 1.0, 1.0, 0.0, &w), 1.0);
        assert_eq!(score(-1.0, -1.0, -1.0, -1.0, 0.0, &w), -1.0);
    }
}
