//! Stratégie PAIR-GTC (technique utilisateur) — bot parallèle, port 8700.
//!
//! Boucle par fenêtre 5 min :
//!   1. Marché proche de 50/50 (|mid_up − 0,5| ≤ band) et assez de temps restant →
//!      poster DEUX ordres GTC de X parts : bid Up et bid Down au touch (+1 tick).
//!   2. Un côté se fait fill (règle de CROSS : le meilleur ask traverse notre bid —
//!      fill certain, pas probabiliste) → on ANNULE l'autre GTC immédiatement.
//!   3. On attend que la courbe bouge : on complète l'opposé en TAKER quand
//!      `avg_jambe + ask_opposé + frais ≤ pair_target` (filtre : paire ≤ 0,94 $
//!      ⇒ ≥ 6¢ bruts / ≥ 3¢ nets verrouillés — leçon du test « complétion à 0,99 »).
//!   4. On tient jusqu'à la résolution. GTC non fillés annulés sous `entry_deadline`.
//!
//! HONNÊTETÉ : la règle de cross ignore la file d'attente FIFO — le vrai taux de
//! fill maker sera différent. C'est conservateur (il faut que le carnet traverse
//! réellement notre prix) mais pas exact.

#[derive(Debug, Clone)]
pub struct PairGtcConfig {
    pub size: f64,               // X parts par GTC
    pub band: f64,               // |mid_up − 0,5| ≤ band pour entrer
    pub entry_min_remaining: i64, // n'entrer que si ≥ N s restantes
    pub entry_deadline: i64,     // annuler les GTC non fillés sous N s restantes
    pub pair_target: f64,        // compléter si avg + ask_opp + fee ≤ target
    pub fee_per_pair: f64,       // frais supposés par paire (inclus dans les prix)
    /// Règle utilisateur « acheter pendant une montée » : la complétion taker exige
    /// que le token opposé soit en train de REMONTER (confirmation de rebond).
    /// NB : ne peut pas s'appliquer à la jambe GTC — un bid resting ne se fait
    /// remplir que quand le prix descend à travers lui (physique du carnet).
    pub require_rising: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn other(&self) -> Side {
        match self {
            Side::Up => Side::Down,
            Side::Down => Side::Up,
        }
    }
}

/// Phase de la fenêtre courante.
#[derive(Debug, Clone, PartialEq)]
pub enum Phase {
    Idle,
    Armed { bid_up: f64, bid_dn: f64 },
    HoldingLeg { side: Side, avg: f64, size: f64 },
    Complete { pair_cost: f64 },
    Done,
}

/// Action à exécuter par la boucle (paper aujourd'hui, live demain).
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Fill maker de notre GTC (prix fee-inclusive) — à passer au PaperEngine.
    MakerFill { side: Side, price: f64, size: f64 },
    /// Annulation du GTC opposé (info/log — en live : DELETE /order).
    CancelGtc { side: Side },
    /// Complétion taker de la paire (prix fee-inclusive).
    TakerBuy { side: Side, price: f64, size: f64 },
}

pub struct PairGtcEngine {
    pub cfg: PairGtcConfig,
    pub phase: Phase,
}

impl PairGtcEngine {
    pub fn new(cfg: PairGtcConfig) -> Self {
        Self { cfg, phase: Phase::Idle }
    }

    pub fn reset_window(&mut self) {
        self.phase = Phase::Idle;
    }

    fn fee_share(&self) -> f64 {
        self.cfg.fee_per_pair / 2.0
    }

    /// Coût de paire si on complétait maintenant à `ask_opp` (fee-inclusive).
    pub fn completion_cost(&self, ask_opp: f64) -> Option<f64> {
        if let Phase::HoldingLeg { avg, .. } = self.phase {
            Some(avg + ask_opp + self.fee_share())
        } else {
            None
        }
    }

    /// Un tick de marché. `rising_up`/`rising_dn` : le mid du côté est en train de
    /// remonter (calculé par la boucle sur un lookback de quelques secondes).
    #[allow(clippy::too_many_arguments)]
    pub fn on_tick(
        &mut self,
        mid_up: f64,
        best_bid_up: f64,
        best_ask_up: f64,
        best_bid_dn: f64,
        best_ask_dn: f64,
        tick_size: f64,
        remaining_s: i64,
        rising_up: bool,
        rising_dn: bool,
    ) -> Vec<Action> {
        let mut out = Vec::new();
        match self.phase.clone() {
            Phase::Idle => {
                // Entrée : proche de 50/50, assez de temps, carnet complet.
                if (mid_up - 0.5).abs() <= self.cfg.band
                    && remaining_s >= self.cfg.entry_min_remaining
                    && best_bid_up > 0.0
                    && best_bid_dn > 0.0
                {
                    let bid_up = (best_bid_up + tick_size).min(0.99);
                    let bid_dn = (best_bid_dn + tick_size).min(0.99);
                    self.phase = Phase::Armed { bid_up, bid_dn };
                    // (les GTC sont virtuels en paper ; en live : 2 POST /order GTC)
                }
            }
            Phase::Armed { bid_up, bid_dn } => {
                // Deadline : plus le temps de jouer → tout annuler.
                if remaining_s < self.cfg.entry_deadline {
                    out.push(Action::CancelGtc { side: Side::Up });
                    out.push(Action::CancelGtc { side: Side::Down });
                    self.phase = Phase::Done;
                    return out;
                }
                // Règle de CROSS : le meilleur ask traverse notre bid → fill certain.
                let up_crossed = best_ask_up > 0.0 && best_ask_up <= bid_up + 1e-9;
                let dn_crossed = best_ask_dn > 0.0 && best_ask_dn <= bid_dn + 1e-9;
                // Si les deux traversent au même tick (rare), on prend le cross le
                // plus profond (meilleur prix relatif).
                let fill_side = match (up_crossed, dn_crossed) {
                    (true, true) => {
                        if (bid_up - best_ask_up) >= (bid_dn - best_ask_dn) {
                            Some(Side::Up)
                        } else {
                            Some(Side::Down)
                        }
                    }
                    (true, false) => Some(Side::Up),
                    (false, true) => Some(Side::Down),
                    _ => None,
                };
                if let Some(side) = fill_side {
                    let raw = match side {
                        Side::Up => bid_up,
                        Side::Down => bid_dn,
                    };
                    let price = raw + self.fee_share();
                    out.push(Action::MakerFill { side, price, size: self.cfg.size });
                    out.push(Action::CancelGtc { side: side.other() });
                    self.phase = Phase::HoldingLeg { side, avg: price, size: self.cfg.size };
                }
            }
            Phase::HoldingLeg { side, avg, size } => {
                // Complétion : l'opposé est devenu assez cheap pour verrouiller,
                // ET (règle utilisateur) il est en train de REMONTER — on achète
                // le rebond, jamais le couteau qui tombe encore.
                let opp = side.other();
                let (ask_opp, opp_rising) = match opp {
                    Side::Up => (best_ask_up, rising_up),
                    Side::Down => (best_ask_dn, rising_dn),
                };
                if ask_opp > 0.0 && remaining_s >= 2 {
                    let cost = avg + ask_opp + self.fee_share();
                    let rising_ok = !self.cfg.require_rising || opp_rising;
                    if cost <= self.cfg.pair_target && rising_ok {
                        let price = ask_opp + self.fee_share();
                        out.push(Action::TakerBuy { side: opp, price, size });
                        self.phase = Phase::Complete { pair_cost: avg + price };
                    }
                }
                // Pas de deadline ici : la jambe se tient jusqu'à la résolution
                // (spec utilisateur). Si la complétion ne vient jamais → jambe nue.
            }
            Phase::Complete { .. } | Phase::Done => {}
        }
        out
    }

    /// Libellé court de la phase pour le dashboard/logs.
    pub fn phase_label(&self) -> String {
        match &self.phase {
            Phase::Idle => "idle".into(),
            Phase::Armed { bid_up, bid_dn } => {
                format!("armed {:.2}/{:.2}", bid_up, bid_dn)
            }
            Phase::HoldingLeg { side, avg, .. } => {
                format!("leg {} @{:.2} → attend l'opposé", side.as_str(), avg)
            }
            Phase::Complete { pair_cost } => format!("paire {:.2}$ ✓", pair_cost),
            Phase::Done => "done".into(),
        }
    }

    /// Nos bids GTC restants (pour l'affichage « ordres en cours »).
    pub fn resting(&self) -> (f64, f64) {
        match &self.phase {
            Phase::Armed { bid_up, bid_dn } => (*bid_up, *bid_dn),
            _ => (0.0, 0.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PairGtcConfig {
        PairGtcConfig {
            size: 20.0,
            band: 0.10,
            entry_min_remaining: 180,
            entry_deadline: 60,
            pair_target: 0.94,
            fee_per_pair: 0.03,
            require_rising: true,
        }
    }

    fn eng() -> PairGtcEngine {
        PairGtcEngine::new(cfg())
    }

    #[test]
    fn enters_only_near_5050_with_time() {
        let mut e = eng();
        // Trop loin de 50/50 → reste idle.
        e.on_tick(0.70, 0.69, 0.71, 0.29, 0.31, 0.01, 250, true, true);
        assert_eq!(e.phase, Phase::Idle);
        // Pas assez de temps → idle.
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 100, true, true);
        assert_eq!(e.phase, Phase::Idle);
        // OK → armed, bids = touch + 1 tick.
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 250, true, true);
        assert_eq!(e.phase, Phase::Armed { bid_up: 0.50, bid_dn: 0.50 });
    }

    #[test]
    fn cross_fills_one_side_and_cancels_other() {
        let mut e = eng();
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 250, true, true);
        // L'ask Up tombe à 0.50 = traverse notre bid Up → fill Up, cancel Down.
        let acts = e.on_tick(0.48, 0.47, 0.50, 0.51, 0.53, 0.01, 240, true, true);
        assert!(matches!(acts[0], Action::MakerFill { side: Side::Up, .. }));
        assert!(matches!(acts[1], Action::CancelGtc { side: Side::Down }));
        assert!(matches!(e.phase, Phase::HoldingLeg { side: Side::Up, .. }));
    }

    #[test]
    fn completion_only_under_pair_target() {
        let mut e = eng();
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 250, true, true);
        e.on_tick(0.48, 0.47, 0.50, 0.51, 0.53, 0.01, 240, true, true); // fill Up @ 0.515 (fee incl.)
        // Down ask 0.45 : 0.515 + 0.45 + 0.015 = 0.98 > 0.94 → PAS de complétion.
        let acts = e.on_tick(0.55, 0.54, 0.56, 0.44, 0.45, 0.01, 200, true, true);
        assert!(acts.is_empty());
        // Down ask 0.40 : 0.515 + 0.40 + 0.015 = 0.93 ≤ 0.94 → complétion.
        let acts = e.on_tick(0.60, 0.59, 0.61, 0.39, 0.40, 0.01, 150, true, true);
        assert!(matches!(acts[0], Action::TakerBuy { side: Side::Down, .. }));
        match &e.phase {
            Phase::Complete { pair_cost } => assert!(*pair_cost < 0.95, "pc={pair_cost}"),
            p => panic!("phase inattendue {p:?}"),
        }
    }

    #[test]
    fn deadline_cancels_unfilled_gtc() {
        let mut e = eng();
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 250, true, true);
        let acts = e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 50, true, true);
        assert_eq!(acts.len(), 2); // deux cancels
        assert_eq!(e.phase, Phase::Done);
    }

    #[test]
    fn completion_waits_for_rebound() {
        // Règle « acheter pendant une montée » : opposé assez cheap MAIS encore en
        // train de tomber → on attend ; dès qu'il remonte → complétion.
        let mut e = eng();
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 250, true, true);
        e.on_tick(0.48, 0.47, 0.50, 0.51, 0.53, 0.01, 240, true, true); // jambe Up
        // Down à 0.40 : prix OK (0.515+0.40+0.015=0.93 ≤ 0.94) mais rising_dn=false.
        let acts = e.on_tick(0.60, 0.59, 0.61, 0.39, 0.40, 0.01, 200, true, false);
        assert!(acts.is_empty(), "ne doit pas acheter un opposé qui tombe encore");
        // Le rebond arrive → complétion.
        let acts = e.on_tick(0.60, 0.59, 0.61, 0.39, 0.40, 0.01, 190, true, true);
        assert!(matches!(acts[0], Action::TakerBuy { side: Side::Down, .. }));
    }

    #[test]
    fn complete_phase_is_terminal() {
        let mut e = eng();
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 250, true, true);
        e.on_tick(0.48, 0.47, 0.50, 0.51, 0.53, 0.01, 240, true, true);
        e.on_tick(0.60, 0.59, 0.61, 0.39, 0.40, 0.01, 150, true, true);
        // Plus aucune action ensuite, même si les prix rebougent.
        let acts = e.on_tick(0.30, 0.29, 0.31, 0.69, 0.71, 0.01, 100, true, true);
        assert!(acts.is_empty());
    }

    #[test]
    fn one_entry_per_window_after_reset() {
        let mut e = eng();
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 250, true, true);
        e.on_tick(0.50, 0.49, 0.51, 0.49, 0.51, 0.01, 50, true, true); // deadline → Done
        assert_eq!(e.phase, Phase::Done);
        e.reset_window();
        assert_eq!(e.phase, Phase::Idle);
    }
}
