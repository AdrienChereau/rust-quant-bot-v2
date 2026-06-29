//! Machine à états du sniper (P4) : `IDLE → ARMING → FIRE → COOLDOWN`.
//!
//! Le score composite continu remplace la porte OBI binaire. L'OBI ne suffit pas :
//! le signal doit rester confirmé pendant `dwell_ms`, puis toutes les conditions
//! (score_ok + gap_ok + défensif + cooldown) doivent être réunies.

use crate::concurrency::bus::Side;

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

/// Entrée d'un tick — nourrit la FSM.
#[derive(Debug, Clone, Copy)]
pub struct TickInput {
    pub now_ms: u64,
    pub score: f64,           // score composite ∈ [-1, 1]
    pub score_sigma: f64,     // std dev EMA du score (pour le sizing Kelly robuste)
    pub basis_unc: f64,       // incertitude basis OKX (pour le sizing Kelly robuste)
    pub fair_up: f64,         // B&S avec décalage d2
    pub real_up: f64,         // mid Polymarket
    pub kalman_velocity: f64, // USD/s (informatif — déjà intégré dans le score)
    pub liquidity_vacuum: bool,
    pub blocked: bool,
}

pub struct Sniper {
    state: State,
    dwell_ms: u64,
    cooldown_ms: u64,
    gap_min: f64,
    score_fire_threshold: f64, // |score| ≥ seuil pour armer/tirer
}

impl Sniper {
    pub fn new(
        dwell_ms: u64,
        cooldown_ms: u64,
        gap_min: f64,
        score_fire_threshold: f64,
    ) -> Self {
        Self { state: State::Idle, dwell_ms, cooldown_ms, gap_min, score_fire_threshold }
    }

    pub fn is_armed(&self) -> bool {
        matches!(self.state, State::Arming { .. })
    }
    pub fn in_cooldown(&self) -> bool {
        matches!(self.state, State::Cooldown { .. })
    }

    fn score_ok(&self, score: f64) -> bool {
        score.abs() >= self.score_fire_threshold
    }

    fn side_from_score(score: f64) -> Side {
        if score > 0.0 { Side::Up } else { Side::Down }
    }

    fn try_arm(&mut self, i: &TickInput) {
        if self.score_ok(i.score) {
            let side = Self::side_from_score(i.score);
            self.state = State::Arming { since_ms: i.now_ms, side };
        }
    }

    fn gap_ok(&self, i: &TickInput) -> bool {
        (i.fair_up - i.real_up).abs() >= self.gap_min
    }

    /// Avance la FSM d'un tick. Renvoie l'action à exécuter.
    pub fn step(&mut self, i: &TickInput) -> Action {
        if i.liquidity_vacuum {
            self.state = State::Cooldown { until_ms: i.now_ms + self.cooldown_ms };
            return Action::Kill;
        }
        if i.blocked {
            self.state = State::Idle;
            return Action::None;
        }

        match self.state {
            State::Cooldown { until_ms } => {
                if i.now_ms < until_ms {
                    return Action::None;
                }
                self.state = State::Idle;
                self.try_arm(i);
                Action::None
            }
            State::Idle => {
                self.try_arm(i);
                if let State::Arming { since_ms, side } = self.state {
                    if i.now_ms.saturating_sub(since_ms) >= self.dwell_ms && self.gap_ok(i) {
                        self.state = State::Cooldown { until_ms: i.now_ms + self.cooldown_ms };
                        return Action::Fire { side, strength: i.score.abs() };
                    }
                }
                Action::None
            }
            State::Arming { since_ms, side } => {
                let new_side = Self::side_from_score(i.score);
                if !self.score_ok(i.score) || new_side != side {
                    self.state = State::Idle;
                    return Action::None;
                }
                if i.now_ms.saturating_sub(since_ms) < self.dwell_ms {
                    return Action::None;
                }
                if self.gap_ok(i) {
                    self.state = State::Cooldown { until_ms: i.now_ms + self.cooldown_ms };
                    Action::Fire { side, strength: i.score.abs() }
                } else {
                    Action::None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_input(now: u64, score: f64) -> TickInput {
        TickInput {
            now_ms: now,
            score,
            score_sigma: 0.3,
            basis_unc: 0.1,
            fair_up: 0.60,
            real_up: 0.50, // gap = 0.10 > gap_min
            kalman_velocity: 5.0,
            liquidity_vacuum: false,
            blocked: false,
        }
    }

    fn sniper() -> Sniper {
        // dwell 200ms, cooldown 3s, gap_min 0.03, score_fire_threshold 0.35
        Sniper::new(200, 3000, 0.03, 0.35)
    }

    #[test]
    fn transient_spike_below_dwell_does_not_fire() {
        let mut s = sniper();
        assert_eq!(s.step(&make_input(0, 0.8)), Action::None); // arme
        assert!(s.is_armed());
        assert_eq!(s.step(&make_input(100, 0.1)), Action::None); // score < threshold → reset
        assert!(!s.is_armed());
    }

    #[test]
    fn sustained_signal_fires_after_dwell() {
        let mut s = sniper();
        s.step(&make_input(0, 0.8));
        assert_eq!(s.step(&make_input(100, 0.8)), Action::None);
        let a = s.step(&make_input(250, 0.8));
        assert_eq!(a, Action::Fire { side: Side::Up, strength: 0.8 });
        assert!(s.in_cooldown());
    }

    #[test]
    fn no_fire_when_gap_too_small() {
        let mut s = sniper();
        let mut i = make_input(0, 0.8);
        i.real_up = 0.59; // gap = 0.01 < 0.03
        s.step(&i);
        let mut i2 = make_input(250, 0.8);
        i2.real_up = 0.59;
        assert_eq!(s.step(&i2), Action::None);
    }

    #[test]
    fn score_below_threshold_does_not_arm() {
        let mut s = sniper();
        assert_eq!(s.step(&make_input(0, 0.20)), Action::None); // 0.20 < 0.35
        assert!(!s.is_armed());
    }

    #[test]
    fn sign_flip_resets_arming() {
        let mut s = sniper();
        s.step(&make_input(0, 0.8));  // arm Up
        assert!(s.is_armed());
        s.step(&make_input(100, -0.8)); // flip → Down → reset
        assert!(!s.is_armed());
    }

    #[test]
    fn liquidity_vacuum_triggers_kill() {
        let mut s = sniper();
        let mut i = make_input(0, 0.8);
        i.liquidity_vacuum = true;
        assert_eq!(s.step(&i), Action::Kill);
        assert!(s.in_cooldown());
    }

    #[test]
    fn cooldown_blocks_immediate_refire() {
        let mut s = sniper();
        s.step(&make_input(0, 0.8));
        assert_eq!(s.step(&make_input(250, 0.8)), Action::Fire { side: Side::Up, strength: 0.8 });
        assert_eq!(s.step(&make_input(300, 0.8)), Action::None);
        assert_eq!(s.step(&make_input(3300, 0.8)), Action::None);
        assert!(s.is_armed());
    }
}
