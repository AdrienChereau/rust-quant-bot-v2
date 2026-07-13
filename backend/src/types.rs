//! Structures de données centrales du bot (carnet L2 Binance, signaux HFT,
//! quotes Polymarket, inventaire). Partagé entre les rôles Radar et Exécuteur.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Alias de prix pour la clarté des signatures.
#[allow(dead_code)] // hérité
pub type Price = OrderedFloat;

/// Wrapper `f64` totalement ordonné, utilisable comme clé de `BTreeMap`.
/// NaN est traité comme égal (jamais produit par les flux de prix).
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub struct OrderedFloat(pub f64);

impl Eq for OrderedFloat {}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Carnet d'ordres L2 local de Binance.
/// Les bids sont triés du plus cher au moins cher via `Reverse`,
/// les asks du moins cher au plus cher.
#[derive(Debug, Clone, Default)]
pub struct BinanceOrderBook {
    pub last_update_id: u64,
    pub bids: BTreeMap<Reverse<OrderedFloat>, f64>,
    pub asks: BTreeMap<OrderedFloat, f64>,
}

impl BinanceOrderBook {
    pub fn new() -> Self {
        Self {
            last_update_id: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Meilleur prix acheteur (plus haut bid).
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next().map(|k| k.0 .0)
    }

    /// Meilleur prix vendeur (plus bas ask).
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|k| k.0)
    }

    /// Prix milieu simple.
    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }

    /// Micro-price pondéré par la profondeur du top-of-book :
    /// `(bid·ask_qty + ask·bid_qty) / (bid_qty + ask_qty)`.
    pub fn calculate_micro_price(&self) -> Option<f64> {
        let best_bid = self.best_bid()?;
        let best_ask = self.best_ask()?;
        let bid_depth = *self.bids.values().next()?;
        let ask_depth = *self.asks.values().next()?;

        if bid_depth + ask_depth == 0.0 {
            return None;
        }
        Some(((best_bid * ask_depth) + (best_ask * bid_depth)) / (bid_depth + ask_depth))
    }
}

/// Tick de prix publié par le connecteur Binance vers le reste du bot.
#[derive(Debug, Copy, Clone, Serialize)]
pub struct PriceTick {
    pub mid: f64,
    pub micro_price: f64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub ts_ms: u64,
}

/// Snapshot du carnet Binance publié sur un canal `watch` vers le radar.
#[derive(Debug, Clone)]
pub struct BookUpdate {
    pub book: BinanceOrderBook,
    pub ts_ms: u64,
}

impl BookUpdate {
    /// Construit un `PriceTick` à partir du carnet (None si carnet vide).
    pub fn price_tick(&self) -> Option<PriceTick> {
        Some(PriceTick {
            mid: self.book.mid()?,
            micro_price: self.book.calculate_micro_price()?,
            best_bid: self.book.best_bid()?,
            best_ask: self.book.best_ask()?,
            ts_ms: self.ts_ms,
        })
    }
}

/// Signaux HFT transcontinentaux (1 ou 65 octets sur le réseau).
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Signal {
    Kill,      // 0x4B 'K' sur le fil
    Heartbeat, // 0x48 'H'
    /// Tick signal complet Tokyo → Dublin (drift/OFI/OBI calculés AU RADAR,
    /// au plus près de Binance) : 0x54 'T' + 64 octets LE.
    Tick(WireTick),
}

/// Charge utile du tick radar (10 Hz). `seq` croît strictement : l'exécuteur
/// jette les datagrammes réordonnés/dupliqués (garde-fou UDP n°2 du plan).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WireTick {
    pub seq: u64,
    pub ts_ms: u64,   // horodatage émission (staleness côté exécuteur)
    pub spot: f64,    // micro-price Binance
    pub sigma: f64,   // volatilité annualisée EWMA
    pub drift: f64,   // drift log PAR SECONDE (l'exécuteur applique horizon+clamp)
    pub ofi: f64,     // order flow imbalance normalisé
    pub obi: f64,
    pub velocity: f64,
    /// CHEMIN CHAUD (passe 3) : déplacement du micro-price sur ~500 ms —
    /// détecteur d'IMPULSION, voit en <1 s ce que l'EMA drift (25 s) met
    /// 2 s à confirmer. 0.0 si absent (rétro-compatibilité 65 octets).
    pub impulse: f64,
}

impl WireTick {
    pub fn encode(&self) -> [u8; 73] {
        let mut b = [0u8; 73];
        b[0] = 0x54;
        for (i, v) in [
            self.seq,
            self.ts_ms,
            self.spot.to_bits(),
            self.sigma.to_bits(),
            self.drift.to_bits(),
            self.ofi.to_bits(),
            self.obi.to_bits(),
            self.velocity.to_bits(),
            self.impulse.to_bits(),
        ]
        .iter()
        .enumerate()
        {
            b[1 + i * 8..9 + i * 8].copy_from_slice(&v.to_le_bytes());
        }
        b
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < 65 || b[0] != 0x54 {
            return None;
        }
        let mut w = [0u64; 8];
        for (i, slot) in w.iter_mut().enumerate() {
            *slot = u64::from_le_bytes(b[1 + i * 8..9 + i * 8].try_into().ok()?);
        }
        // Rétro-compatible : une trame 65 octets (ancien radar) = impulse 0.
        let impulse = if b.len() >= 73 {
            f64::from_bits(u64::from_le_bytes(b[65..73].try_into().ok()?))
        } else {
            0.0
        };
        Some(Self {
            seq: w[0],
            ts_ms: w[1],
            spot: f64::from_bits(w[2]),
            sigma: f64::from_bits(w[3]),
            drift: f64::from_bits(w[4]),
            ofi: f64::from_bits(w[5]),
            obi: f64::from_bits(w[6]),
            velocity: f64::from_bits(w[7]),
            impulse,
        })
    }
}

/// Quote produite par le moteur de risque (côté exécuteur).
#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)] // hérité v1
pub struct Quote {
    pub bid_price: f64,
    pub ask_price: f64,
    pub size: f64,
    pub timestamp: DateTime<Utc>,
}

/// Inventaire et PnL du bot (mode paper au J8).
#[derive(Debug, Clone, Default, Serialize)]
#[allow(dead_code)] // hérité v1
pub struct BotInventory {
    pub yes_balance: f64,
    pub no_balance: f64,
    pub cash_usdc: f64,
    pub real_pnl: f64,
    pub latent_pnl: f64,
}

#[cfg(test)]
mod wire_tests {
    use super::*;

    #[test]
    fn wiretick_roundtrip() {
        let t = WireTick {
            seq: 42,
            ts_ms: 1_783_353_000_123,
            spot: 63483.99,
            sigma: 0.85,
            drift: -0.000123,
            ofi: 0.77,
            obi: -0.31,
            velocity: 1.0,
            impulse: 0.00031,
        };
        let b = t.encode();
        assert_eq!(b.len(), 73);
        assert_eq!(b[0], 0x54);
        assert_eq!(WireTick::decode(&b), Some(t));
        // rétro-compatibilité : trame courte (ancien radar 65 octets) → impulse 0
        let old = WireTick::decode(&b[..65]).expect("trame 65 acceptée");
        assert_eq!(old.impulse, 0.0);
        assert_eq!(old.drift, t.drift);
        // trames invalides
        assert_eq!(WireTick::decode(&b[..64]), None);
        let mut bad = b;
        bad[0] = 0x4B;
        assert_eq!(WireTick::decode(&bad), None);
    }
}
