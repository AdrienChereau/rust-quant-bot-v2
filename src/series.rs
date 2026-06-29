//! Série temporelle bornée pour le graphe d'entrées/sorties (endpoint `/series`).
//!
//! **Zéro impact hot loop :** un seul `push` borné **1×/seconde** (échantillonné dans le bras
//! tick 50 ms), pas par tick OBI. Le dashboard lit le snapshot sur sa task séparée.

use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

use serde::Serialize;

#[derive(Clone, Copy, Serialize)]
pub struct Point {
    pub t: u64,    // epoch ms
    pub fair: f64, // fair_up B&S (avec décalage d2)
    pub real: f64, // mid Polymarket Up
    pub spot: f64, // BTC spot (0 si indisponible, ex. nœud paper)
}

static RING: OnceLock<Mutex<VecDeque<Point>>> = OnceLock::new();
const CAP: usize = 900; // ~15 min à 1 point/s

fn ring() -> &'static Mutex<VecDeque<Point>> {
    RING.get_or_init(|| Mutex::new(VecDeque::with_capacity(CAP)))
}

pub fn push(t: u64, fair: f64, real: f64, spot: f64) {
    if let Ok(mut g) = ring().lock() {
        if g.len() >= CAP { g.pop_front(); }
        g.push_back(Point { t, fair, real, spot });
    }
}

pub fn snapshot() -> Vec<Point> {
    ring().lock().map(|g| g.iter().cloned().collect()).unwrap_or_default()
}
