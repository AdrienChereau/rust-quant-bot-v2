//! Bus de signaux inter-thread (crossbeam, lock-free MPMC).
//!
//! Le moteur Radar (Binance/OKX) émet des signaux `Attack`/`Kill` que le moteur
//! Sniper (Polymarket) intercepte en quelques µs.

use crossbeam_channel::{bounded, Receiver, Sender};

/// Côté du marché binaire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Side {
    Up,
    Down,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Up => "up",
            Side::Down => "down",
        }
    }
}

/// Signaux émis par le radar.
#[derive(Debug, Clone, Copy)]
pub enum Signal {
    /// Feu : OBI consolidé confirmé. `strength` ∈ [0,1] (force pondérée, pour le sizing).
    Attack { side: Side, strength: f64 },
    /// Vide de liquidité / effondrement → annuler/s'abstenir.
    Kill,
}

#[derive(Clone)]
pub struct SignalBus {
    tx: Sender<Signal>,
    rx: Receiver<Signal>,
}

impl SignalBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = bounded(capacity);
        Self { tx, rx }
    }
    pub fn sender(&self) -> Sender<Signal> {
        self.tx.clone()
    }
    pub fn receiver(&self) -> Receiver<Signal> {
        self.rx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bus_relays_signal() {
        let bus = SignalBus::new(8);
        let tx = bus.sender();
        tx.send(Signal::Attack { side: Side::Up, strength: 0.8 }).unwrap();
        match bus.receiver().recv().unwrap() {
            Signal::Attack { side, strength } => {
                assert_eq!(side, Side::Up);
                assert!((strength - 0.8).abs() < 1e-9);
            }
            _ => panic!("attendu Attack"),
        }
    }
}
