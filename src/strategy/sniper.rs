//! Machine à états du sniper (P4) : `IDLE → ARMING → FIRE → COOLDOWN`.
//!
//! On ne tire JAMAIS sur une lecture OBI instantanée (bruit). L'OBI consolidé doit
//! RESTER confirmé pendant `dwell_ms`, puis le tir n'a lieu que si TOUTES les
//! conditions sont réunies (vélocité + gap fair/real + défensif + cooldown).

use crate::concurrency::bus::Side;
use crate::signal::consolidated_obi::ConsolidatedDecision;

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Idle,
    Arming { since_ms: u64, side: Side },
    Cooldown { until_ms: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Action {
    None,
    Fire { side: Side, strength: f64 },
    Kill,
}

/// Entrée d'un tick (toutes les ~50 ms).
#[derive(Debug, Clone, Copy)]
pub struct TickInput {
    pub now_ms: u64,
    pub decision: ConsolidatedDecision, // OBI consolidé (porte d'accord)
    pub fair_up: f64,                    // B&S
    pub real_up: f64,                    // mid Polymarket
    pub velocity: f64,                   // ΔP_1s
    pub liquidity_vacuum: bool,          // ΔP_1s ≤ seuil ET OBI ≤ seuil
    pub blocked: bool,                   // fin de fenêtre ou blackout macro
}

pub struct Sniper {
    state: State,
    dwell_ms: u64,
    cooldown_ms: u64,
    gap_min: f64,
    velocity_confirm: f64,
}

impl Sniper {
    pub fn new(dwell_ms: u64, cooldown_ms: u64, gap_min: f64, velocity_confirm: f64) -> Self {
        Self { state: State::Idle, dwell_ms, cooldown_ms, gap_min, velocity_confirm }
    }

    pub fn is_armed(&self) -> bool {
        matches!(self.state, State::Arming { .. })
    }
    pub fn in_cooldown(&self) -> bool {
        matches!(self.state, State::Cooldown { .. })
    }

    /// Passe en ARMING si le signal OBI est confirmé.
    fn try_arm(&mut self, i: &TickInput) {
        if i.decision.fire {
            if let Some(side) = i.decision.side {
                self.state = State::Arming { since_ms: i.now_ms, side };
            }
        }
    }

    /// Toutes les conditions de tir (hors persistance OBI, déjà vérifiée).
    fn fire_conditions(&self, side: Side, i: &TickInput) -> bool {
        // 1. Vélocité confirme le sens (désactivé si velocity_confirm <= 0).
        let vel_ok = if self.velocity_confirm <= 0.0 {
            true
        } else {
            match side {
                Side::Up => i.velocity >= self.velocity_confirm,
                Side::Down => i.velocity <= -self.velocity_confirm,
            }
        };
        // 2. Gap |fair − real| ≥ gap_min : écart B&S/PM suffisant indépendamment du sens.
        //    (Le sens du gap est souvent opposé à l'OBI car PM anticipe déjà le move ;
        //     on garde juste l'amplitude pour éviter de tirer quand les prix sont au même niveau.)
        let gap_ok = (i.fair_up - i.real_up).abs() >= self.gap_min;
        vel_ok && gap_ok
    }

    /// Avance la machine d'un tick. Renvoie l'action à exécuter.
    pub fn step(&mut self, i: &TickInput) -> Action {
        // Vide de liquidité → KILL prioritaire + cooldown.
        if i.liquidity_vacuum {
            self.state = State::Cooldown { until_ms: i.now_ms + self.cooldown_ms };
            return Action::Kill;
        }
        // Blackout (fin de fenêtre / macro) → on désarme, pas de tir.
        if i.blocked {
            self.state = State::Idle;
            return Action::None;
        }

        match self.state {
            State::Cooldown { until_ms } => {
                if i.now_ms < until_ms {
                    return Action::None;
                }
                // Cooldown expiré → on réévalue le signal dans le même tick.
                self.state = State::Idle;
                self.try_arm(i);
                Action::None
            }
            State::Idle => {
                self.try_arm(i);
                // Quand dwell=0 : on peut tirer immédiatement sur le tick de l'accord.
                if let State::Arming { since_ms, side } = self.state {
                    if i.now_ms.saturating_sub(since_ms) >= self.dwell_ms
                        && self.fire_conditions(side, i)
                    {
                        self.state =
                            State::Cooldown { until_ms: i.now_ms + self.cooldown_ms };
                        return Action::Fire { side, strength: i.decision.strength };
                    }
                }
                Action::None
            }
            State::Arming { since_ms, side } => {
                // L'accord OBI doit rester valide et du même côté.
                if !i.decision.fire || i.decision.side != Some(side) {
                    self.state = State::Idle;
                    return Action::None;
                }
                // Persistance insuffisante → on continue de confirmer.
                if i.now_ms.saturating_sub(since_ms) < self.dwell_ms {
                    return Action::None;
                }
                // Dwell atteint → tir si toutes les conditions passent.
                if self.fire_conditions(side, i) {
                    self.state = State::Cooldown { until_ms: i.now_ms + self.cooldown_ms };
                    Action::Fire { side, strength: i.decision.strength }
                } else {
                    Action::None // armé, on attend que le gap/vélocité s'aligne
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision_up() -> ConsolidatedDecision {
        ConsolidatedDecision { fire: true, side: Some(Side::Up), strength: 0.7 }
    }
    fn decision_none() -> ConsolidatedDecision {
        ConsolidatedDecision { fire: false, side: None, strength: 0.1 }
    }

    fn input(now: u64, d: ConsolidatedDecision) -> TickInput {
        TickInput {
            now_ms: now,
            decision: d,
            fair_up: 0.60, // gap +0.10 vs real
            real_up: 0.50,
            velocity: 0.001, // confirme Up
            liquidity_vacuum: false,
            blocked: false,
        }
    }

    fn sniper() -> Sniper {
        // dwell 200ms, cooldown 3s, gap_min 0.03, velocity_confirm 0.0005
        Sniper::new(200, 3000, 0.03, 0.0005)
    }

    #[test]
    fn transient_spike_below_dwell_does_not_fire() {
        let mut s = sniper();
        assert_eq!(s.step(&input(0, decision_up())), Action::None); // arme
        assert!(s.is_armed());
        // spike disparaît avant la fin du dwell
        assert_eq!(s.step(&input(100, decision_none())), Action::None);
        assert!(!s.is_armed()); // reset
    }

    #[test]
    fn sustained_signal_fires_after_dwell() {
        let mut s = sniper();
        s.step(&input(0, decision_up())); // arme à t=0
        assert_eq!(s.step(&input(100, decision_up())), Action::None); // encore en dwell
        let a = s.step(&input(250, decision_up())); // dwell dépassé
        assert_eq!(a, Action::Fire { side: Side::Up, strength: 0.7 });
        assert!(s.in_cooldown());
    }

    #[test]
    fn no_fire_when_gap_too_small() {
        let mut s = sniper();
        let mut i = input(0, decision_up());
        i.real_up = 0.59; // gap = 0.01 < 0.03
        s.step(&i);
        let mut i2 = input(250, decision_up());
        i2.real_up = 0.59;
        assert_eq!(s.step(&i2), Action::None); // armé mais pas de tir
    }

    #[test]
    fn no_fire_when_velocity_contradicts() {
        let mut s = sniper();
        let mut i = input(0, decision_up());
        i.velocity = -0.001; // prix baisse alors qu'on veut Up
        s.step(&i);
        let mut i2 = input(250, decision_up());
        i2.velocity = -0.001;
        assert_eq!(s.step(&i2), Action::None);
    }

    #[test]
    fn liquidity_vacuum_triggers_kill() {
        let mut s = sniper();
        let mut i = input(0, decision_up());
        i.liquidity_vacuum = true;
        assert_eq!(s.step(&i), Action::Kill);
        assert!(s.in_cooldown());
    }

    #[test]
    fn cooldown_blocks_immediate_refire() {
        let mut s = sniper();
        s.step(&input(0, decision_up()));
        assert_eq!(
            s.step(&input(250, decision_up())),
            Action::Fire { side: Side::Up, strength: 0.7 }
        );
        // juste après → cooldown, pas de nouveau tir
        assert_eq!(s.step(&input(300, decision_up())), Action::None);
        // après cooldown → réarme
        assert_eq!(s.step(&input(3300, decision_up())), Action::None);
        assert!(s.is_armed());
    }
}
