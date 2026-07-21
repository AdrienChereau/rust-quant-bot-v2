//! Cœur HFT du Nœud Radar (Tokyo) : calcul de l'Order Book Imbalance (OBI) et de
//! la vélocité du micro-price à 10 Hz sur un buffer circulaire glissant de 1000 ms,
//! avec détection d'emballement (flash-crash / cascade de liquidation) → `Signal::Kill`.

use std::collections::VecDeque;

use crate::types::{BinanceOrderBook, Signal};

pub struct RadarEngine {
    lookback_ms: u64,
    history: VecDeque<(u64, f64)>, // (timestamp_ms, micro_price)
    obi_depth_levels: usize,
    obi_threshold: f64,
    velocity_threshold: f64,
    /// Dernière vélocité brute calculée ($ de déplacement du micro-price sur la
    /// fenêtre lookback) — exposée pour le WireTick (fix 21 juil. : le champ
    /// vélocité du paquet était un drapeau 0/1, la garde aval comparait son
    /// seuil 45 $/s à un zéro permanent).
    last_velocity: f64,
}

impl RadarEngine {
    pub fn new(obi_depth_levels: usize, obi_threshold: f64, velocity_threshold: f64) -> Self {
        Self {
            lookback_ms: 1000,
            history: VecDeque::with_capacity(128),
            obi_depth_levels,
            obi_threshold,
            velocity_threshold,
            last_velocity: 0.0,
        }
    }

    /// Order Book Imbalance sur `obi_depth_levels` niveaux de profondeur.
    pub fn calculate_obi(&self, book: &BinanceOrderBook) -> f64 {
        let bid_volume: f64 = book.bids.values().take(self.obi_depth_levels).sum();
        let ask_volume: f64 = book.asks.values().take(self.obi_depth_levels).sum();

        if bid_volume + ask_volume == 0.0 {
            return 0.0;
        }
        (bid_volume - ask_volume) / (bid_volume + ask_volume)
    }

    /// Enregistre le micro-price courant, met à jour le buffer glissant et
    /// renvoie `Signal::Kill` si OBI extrême ET vélocité violente sont corrélés.
    pub fn tick(&mut self, current_time_ms: u64, book: &BinanceOrderBook) -> Option<Signal> {
        let current_micro_price = book.calculate_micro_price()?;
        self.history.push_back((current_time_ms, current_micro_price));

        // Purge du buffer : ne garder que les 1000 dernières ms.
        while let Some((ts, _)) = self.history.front() {
            if current_time_ms.saturating_sub(*ts) > self.lookback_ms {
                self.history.pop_front();
            } else {
                break;
            }
        }

        if self.history.len() < 2 {
            return None;
        }

        let (_, oldest_price) = *self.history.front()?;
        let velocity = current_micro_price - oldest_price;
        self.last_velocity = velocity;
        let obi = self.calculate_obi(book);

        if obi.abs() >= self.obi_threshold && velocity.abs() >= self.velocity_threshold {
            return Some(Signal::Kill);
        }
        None
    }

    /// Vélocité brute du dernier tick ($ sur la fenêtre lookback).
    pub fn last_velocity(&self) -> f64 {
        self.last_velocity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OrderedFloat;
    use std::cmp::Reverse;

    fn book(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> BinanceOrderBook {
        let mut b = BinanceOrderBook::new();
        for (p, q) in bids {
            b.bids.insert(Reverse(OrderedFloat(*p)), *q);
        }
        for (p, q) in asks {
            b.asks.insert(OrderedFloat(*p), *q);
        }
        b
    }

    #[test]
    fn obi_balanced_is_zero() {
        let r = RadarEngine::new(5, 0.8, 1.0);
        let b = book(&[(100.0, 10.0)], &[(101.0, 10.0)]);
        assert!((r.calculate_obi(&b)).abs() < 1e-9);
    }

    #[test]
    fn obi_bid_heavy_is_positive() {
        let r = RadarEngine::new(5, 0.8, 1.0);
        let b = book(&[(100.0, 90.0)], &[(101.0, 10.0)]);
        assert!(r.calculate_obi(&b) > 0.7);
    }

    #[test]
    fn kill_fires_on_imbalance_plus_velocity() {
        let mut r = RadarEngine::new(5, 0.8, 1.0);
        // t=0 : prix ~100, fortement déséquilibré côté ask (vente massive).
        let b0 = book(&[(99.0, 5.0)], &[(100.0, 95.0)]);
        assert!(r.tick(0, &b0).is_none()); // un seul point
        // t=500 : le prix s'est effondré, OBI toujours extrême.
        let b1 = book(&[(90.0, 5.0)], &[(91.0, 95.0)]);
        assert_eq!(r.tick(500, &b1), Some(Signal::Kill));
    }

    #[test]
    fn no_kill_when_calm() {
        let mut r = RadarEngine::new(5, 0.8, 1.0);
        let b0 = book(&[(100.0, 10.0)], &[(101.0, 10.0)]);
        let b1 = book(&[(100.0, 11.0)], &[(101.0, 10.0)]);
        r.tick(0, &b0);
        assert!(r.tick(500, &b1).is_none());
    }
}
