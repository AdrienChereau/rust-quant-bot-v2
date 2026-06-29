//! Carnet d'ordres L2 local + OBI 0.15 % (P2).
//!
//! Clés = prix×100 (entiers, pas de f64 en clé de BTreeMap). La méthode `.range()`
//! extrait la tranche 0.15 % autour du mid en O(log n) — anti-spoofing (le bruit/les
//! gros ordres fictifs au-delà de 0.20 % sont ignorés).

use std::collections::BTreeMap;

const PRICE_SCALE: f64 = 100.0;

#[derive(Debug, Clone, Default)]
pub struct OrderBookL2 {
    pub bids: BTreeMap<u64, f64>, // prix×100 → quantité
    pub asks: BTreeMap<u64, f64>,
    pub band_pct: f64, // ex. 0.0015
}

impl OrderBookL2 {
    pub fn new(band_pct: f64) -> Self {
        Self { bids: BTreeMap::new(), asks: BTreeMap::new(), band_pct }
    }

    /// Met à jour un niveau (quantité 0 = suppression).
    pub fn update_level(&mut self, is_bid: bool, price_raw: f64, quantity: f64) {
        let key = (price_raw * PRICE_SCALE) as u64;
        let book = if is_bid { &mut self.bids } else { &mut self.asks };
        if quantity <= 0.0 {
            book.remove(&key);
        } else {
            book.insert(key, quantity);
        }
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|&k| k as f64 / PRICE_SCALE)
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|&k| k as f64 / PRICE_SCALE)
    }
    pub fn mid_price(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }

    /// OBI restreint à la bande `band_pct` autour du mid ∈ [-1, 1].
    pub fn calculate_obi(&self) -> f64 {
        let Some(mid) = self.mid_price() else { return 0.0 };
        let bid_min_key = (mid * (1.0 - self.band_pct) * PRICE_SCALE) as u64;
        let ask_max_key = (mid * (1.0 + self.band_pct) * PRICE_SCALE) as u64;
        let mid_key = (mid * PRICE_SCALE) as u64;

        let bid_vol: f64 = self.bids.range(bid_min_key..=mid_key).map(|(_, &v)| v).sum();
        let ask_vol: f64 = self.asks.range(mid_key..=ask_max_key).map(|(_, &v)| v).sum();

        let denom = bid_vol + ask_vol;
        if denom == 0.0 { return 0.0; }
        (bid_vol - ask_vol) / denom
    }

    /// OBI top-N niveaux BBO (comme le MM bot) : ultra-réactif, capte la pression immédiate.
    /// `n=0` → repli sur la bande.
    pub fn calculate_obi_topn(&self, n: usize) -> f64 {
        if n == 0 { return self.calculate_obi(); }
        let bid_vol: f64 = self.bids.values().rev().take(n).sum();
        let ask_vol: f64 = self.asks.values().take(n).sum();
        let denom = bid_vol + ask_vol;
        if denom == 0.0 { return 0.0; }
        (bid_vol - ask_vol) / denom
    }

    /// Microprice : mid pondéré par le volume top-of-book (anti-bruit spread).
    /// `(bid_px × ask_qty + ask_px × bid_qty) / (bid_qty + ask_qty)`
    pub fn microprice(&self) -> Option<f64> {
        let (&bk, &bq) = self.bids.iter().next_back()?;
        let (&ak, &aq) = self.asks.iter().next()?;
        let bp = bk as f64 / PRICE_SCALE;
        let ap = ak as f64 / PRICE_SCALE;
        let bq_f = bq as f64; // cast explicite : robustesse si le type BTreeMap change
        let aq_f = aq as f64;
        let denom = bq_f + aq_f;
        if denom <= 0.0 { return None; }
        Some((bp * aq_f + ap * bq_f) / denom)
    }

    /// OBI multi-niveaux avec décroissance exponentielle (Σ e^{-λi} × obi_i / Σ e^{-λi}).
    /// Capture la structure de profondeur au-delà du BBO — anti-spoofing plus robuste.
    /// `lambda=0.5` → poids 1, 0.61, 0.37, 0.22 … sur les niveaux successifs.
    pub fn calculate_obi_multilevel(&self, n_levels: usize, lambda: f64) -> f64 {
        // Les deux côtés doivent exister (sinon OBI indéfini). Évite aussi l'overflow u64
        // de `bp + u64::MAX` quand un côté est vide.
        let (bp, ak) = match (self.bids.keys().next_back(), self.asks.keys().next()) {
            (Some(&b), Some(&a)) => (b, a),
            _ => return 0.0,
        };
        let mid_key = bp / 2 + ak / 2; // midpoint sans overflow
        let bids: Vec<f64> = self.bids.range(..=mid_key).rev().take(n_levels)
            .map(|(_, &v)| v).collect();
        let asks: Vec<f64> = self.asks.range(mid_key..).take(n_levels)
            .map(|(_, &v)| v).collect();

        let (mut num, mut den) = (0.0f64, 0.0f64);
        for (i, (bq, aq)) in bids.iter().zip(asks.iter()).enumerate() {
            let w = (-lambda * i as f64).exp();
            let obi_i = (bq - aq) / (bq + aq + 1e-9);
            num += w * obi_i;
            den += w;
        }
        if den > 0.0 { num / den } else { 0.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> OrderBookL2 {
        OrderBookL2::new(0.0015)
    }

    #[test]
    fn obi_balanced_is_zero() {
        let mut b = book();
        b.update_level(true, 60000.0, 10.0);
        b.update_level(false, 60001.0, 10.0);
        assert!(b.calculate_obi().abs() < 1e-9);
    }

    #[test]
    fn obi_bid_heavy_positive() {
        let mut b = book();
        // mid ≈ 60000, bande 0.15 % ≈ ±90 $.
        b.update_level(true, 59990.0, 50.0);
        b.update_level(true, 59950.0, 30.0);
        b.update_level(false, 60010.0, 5.0);
        assert!(b.calculate_obi() > 0.7, "obi={}", b.calculate_obi());
    }

    #[test]
    fn anti_spoofing_ignores_outside_band() {
        let mut b = book();
        b.update_level(true, 59990.0, 10.0);
        b.update_level(false, 60010.0, 10.0);
        let obi_before = b.calculate_obi();
        // Gros ordre fictif à −0.30 % (au-delà de la bande 0.15 %) → ignoré.
        b.update_level(true, 59820.0, 10000.0);
        assert!((b.calculate_obi() - obi_before).abs() < 1e-9, "spoof hors bande doit être ignoré");
    }

    #[test]
    fn empty_book_obi_zero() {
        assert_eq!(book().calculate_obi(), 0.0);
    }
}
