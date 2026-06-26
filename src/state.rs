//! Contrôle d'exécution **lock-free** lu dans la hot-loop (coût quasi nul).
//!
//! Le serveur HTTP du dashboard mute ces flags via les endpoints POST ; la boucle de trading les
//! lit en `Ordering::Relaxed` (aucun verrou, aucune contention). Sépare le *contrôle d'état* (ici)
//! de l'*observabilité* (`DashState` sous `RwLock`, hors chemin critique).

use std::sync::atomic::{AtomicBool, Ordering};

/// État par défaut **sûr** : paper actif, live désarmé + en pause, breaker non déclenché.
#[derive(Debug)]
pub struct RuntimeControls {
    /// Pause du mode paper (le bouton "pause" du dashboard).
    pub paper_paused: AtomicBool,
    /// Le mode live est-il sélectionné ? (n'implique pas l'envoi réel — cf. `live_armed`).
    pub live_enabled: AtomicBool,
    /// Pause du mode live (vrai par défaut : on n'exécute pas tant qu'on ne l'a pas relâché).
    pub live_paused: AtomicBool,
    /// Circuit breaker (drawdown) — quand vrai, AUCUN signal n'est exécuté.
    pub breaker_tripped: AtomicBool,
}

impl Default for RuntimeControls {
    fn default() -> Self {
        Self {
            paper_paused: AtomicBool::new(false),
            live_enabled: AtomicBool::new(false),
            live_paused: AtomicBool::new(true),
            breaker_tripped: AtomicBool::new(false),
        }
    }
}

impl RuntimeControls {
    pub fn new() -> Self {
        Self::default()
    }

    // Lectures hot-loop (Relaxed = 0 blocage).
    pub fn is_breaker_tripped(&self) -> bool { self.breaker_tripped.load(Ordering::Relaxed) }
    pub fn is_paper_paused(&self) -> bool { self.paper_paused.load(Ordering::Relaxed) }
    pub fn is_live_enabled(&self) -> bool { self.live_enabled.load(Ordering::Relaxed) }
    pub fn is_live_paused(&self) -> bool { self.live_paused.load(Ordering::Relaxed) }

    /// Le mode live est-il **actif** (sélectionné ET non en pause) ? (≠ envoi réel armé.)
    pub fn live_active(&self) -> bool {
        self.is_live_enabled() && !self.is_live_paused()
    }

    /// Déclenche le breaker (idempotent). Renvoie `true` si transition.
    pub fn trip_breaker(&self) -> bool {
        !self.breaker_tripped.swap(true, Ordering::Relaxed)
    }

    /// Libellé du mode courant, pour le dashboard.
    pub fn mode_label(&self) -> &'static str {
        if self.is_breaker_tripped() {
            "BREAKER"
        } else if self.live_active() {
            "LIVE"
        } else if !self.is_paper_paused() {
            "PAPER"
        } else {
            "PAUSE"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_defaults() {
        let c = RuntimeControls::new();
        assert!(!c.is_paper_paused());
        assert!(!c.is_live_enabled());
        assert!(c.is_live_paused());
        assert!(!c.is_breaker_tripped());
        assert_eq!(c.mode_label(), "PAPER");
        assert!(!c.live_active());
    }

    #[test]
    fn breaker_overrides_mode_label() {
        let c = RuntimeControls::new();
        assert!(c.trip_breaker());
        assert!(!c.trip_breaker()); // déjà déclenché
        assert_eq!(c.mode_label(), "BREAKER");
    }

    #[test]
    fn live_active_requires_enabled_and_unpaused() {
        let c = RuntimeControls::new();
        c.live_enabled.store(true, Ordering::Relaxed);
        assert!(!c.live_active()); // encore en pause
        c.live_paused.store(false, Ordering::Relaxed);
        assert!(c.live_active());
        assert_eq!(c.mode_label(), "LIVE");
    }
}
