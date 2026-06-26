//! Protocole UDP binaire **6 octets** (zéro JSON/Serde) — radar → exécuteur.
//!
//! La sérialisation/désérialisation Serde coûte de précieuses µs sur le chemin chaud.
//! On envoie un payload brut de 6 octets, en **Little Endian** :
//! * `Byte 0` : *kind* — `0x00` DOWN, `0x01` UP, `0xFF` KILL.
//! * `Byte 1` : taille indicative (u8, ≤ 255 tokens) — l'exécuteur recalcule le sizing Kelly.
//! * `Bytes 2..6` : prix cible (`fair_up` côté radar) encodé en `f32` LE.
//!
//! Le KILL (vide de liquidité, CEX-dérivé) n'existe pas dans la spec d'origine ; on l'ajoute
//! via `kind = 0xFF` (bytes 1..6 ignorés) pour porter le signal d'abstention au nœud d'exécution.

use crate::concurrency::bus::Side;

pub const WIRE_LEN: usize = 6;

const KIND_DOWN: u8 = 0x00;
const KIND_UP: u8 = 0x01;
const KIND_KILL: u8 = 0xFF;

/// Signal transporté sur le fil (UDP). Variante d'attaque (avec side/size/prix) ou KILL.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WireSignal {
    /// Tir : side + taille indicative + prix cible (fair_up).
    Attack { side: Side, size: u8, price: f32 },
    /// Abstention : vide de liquidité détecté côté radar.
    Kill,
}

impl WireSignal {
    /// Sérialise en 6 octets (Little Endian).
    pub fn to_bytes(&self) -> [u8; WIRE_LEN] {
        let mut buf = [0u8; WIRE_LEN];
        match self {
            WireSignal::Attack { side, size, price } => {
                buf[0] = match side {
                    Side::Up => KIND_UP,
                    Side::Down => KIND_DOWN,
                };
                buf[1] = *size;
                buf[2..6].copy_from_slice(&price.to_le_bytes());
            }
            WireSignal::Kill => {
                buf[0] = KIND_KILL;
                // bytes 1..6 = 0, ignorés à la réception.
            }
        }
        buf
    }

    /// Désérialise depuis un buffer reçu. Renvoie `None` si la taille ou le kind est invalide.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < WIRE_LEN {
            return None;
        }
        match buf[0] {
            KIND_KILL => Some(WireSignal::Kill),
            KIND_UP => Some(WireSignal::Attack {
                side: Side::Up,
                size: buf[1],
                price: f32::from_le_bytes(buf[2..6].try_into().ok()?),
            }),
            KIND_DOWN => Some(WireSignal::Attack {
                side: Side::Down,
                size: buf[1],
                price: f32::from_le_bytes(buf[2..6].try_into().ok()?),
            }),
            _ => None, // kind inconnu → paquet rejeté
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_attack_up() {
        let s = WireSignal::Attack { side: Side::Up, size: 12, price: 0.6234 };
        let b = s.to_bytes();
        assert_eq!(b.len(), 6);
        assert_eq!(b[0], KIND_UP);
        assert_eq!(b[1], 12);
        let back = WireSignal::from_bytes(&b).unwrap();
        match back {
            WireSignal::Attack { side, size, price } => {
                assert_eq!(side, Side::Up);
                assert_eq!(size, 12);
                assert!((price - 0.6234).abs() < 1e-6);
            }
            _ => panic!("attendu Attack"),
        }
    }

    #[test]
    fn roundtrip_attack_down() {
        let s = WireSignal::Attack { side: Side::Down, size: 255, price: 0.01 };
        let back = WireSignal::from_bytes(&s.to_bytes()).unwrap();
        assert_eq!(back, WireSignal::Attack { side: Side::Down, size: 255, price: 0.01 });
    }

    #[test]
    fn roundtrip_kill() {
        let b = WireSignal::Kill.to_bytes();
        assert_eq!(b[0], KIND_KILL);
        assert_eq!(WireSignal::from_bytes(&b), Some(WireSignal::Kill));
    }

    #[test]
    fn little_endian_price() {
        // 1.0_f32 LE = 00 00 80 3F
        let s = WireSignal::Attack { side: Side::Up, size: 1, price: 1.0 };
        let b = s.to_bytes();
        assert_eq!(&b[2..6], &[0x00, 0x00, 0x80, 0x3F]);
    }

    #[test]
    fn rejects_short_buffer() {
        assert_eq!(WireSignal::from_bytes(&[0x01, 0x00]), None);
    }

    #[test]
    fn rejects_unknown_kind() {
        assert_eq!(WireSignal::from_bytes(&[0x42, 0, 0, 0, 0, 0]), None);
    }
}
