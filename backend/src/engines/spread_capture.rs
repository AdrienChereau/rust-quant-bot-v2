//! Moteur SPREAD-CAPTURE TAKER (plan v5) — validé Phase A sur 100 fenêtres réelles.
//!
//! Stratégie (guide polyresearchrobotics + gate Binance ⚡) : on achète en **taker**
//! chaque côté **sur son creux, à des moments différents** — jamais les deux d'un coup
//! (instantanément `ask_up + ask_dn > 1$`, toujours). Le coût de paire *blended*
//! passe sous 1 $ grâce à l'oscillation, le profit est verrouillé à l'entrée, et on
//! **tient jusqu'à la résolution** (aucune vente, aucun stop — vendre détruit le hedge).
//!
//! Résultat Phase A (replay 100 fenêtres) : guide seul = non rentable après frais ;
//! guide + gate `ask ≤ fair_drift − M` = +11,4 % net de frais, médiane paire 0,785 $.
//!
//! Les frais taker sont inclus dans le prix d'exécution (`ask + fee_share`) : les
//! coûts moyens sont donc fee-inclusive et la règle `avg_up + avg_dn < 1` reste
//! exactement la condition de profit au règlement.

#[derive(Debug, Clone)]
pub struct SpreadCaptureConfig {
    pub c_raw: f64,                  // plafond blended brut (0.95 prudent)
    pub fee_per_pair: f64,           // frais taker par paire (prior 0.03 — recalibré au 1er fill live)
    pub opening_leg_max: f64,        // plafond de la PREMIÈRE jambe d'un côté (0.55)
    pub max_imbalance: f64,          // |shares_up − shares_dn| max après achat (40)
    pub base_clip: f64,              // clip de base (10)
    pub max_clip: f64,               // clip max (20)
    pub depth_gain: f64,             // clip += gain × profondeur du creux (60)
    pub max_clip_usdc: f64,          // $ max par fill (6)
    pub max_capital_per_market: f64, // $ max déployés par fenêtre (20)
    pub min_seconds: i64,            // pas de nouvelle jambe sous N s de la clôture (10)
    pub clip_interval_s: i64,        // cadence min entre 2 clips d'un même côté (15)
    pub gate_margin: f64,            // ⚡ ask ≤ fair − M (0.04)
    pub min_window_age_s: i64,       // pas d'entrée avant N s d'âge de fenêtre (15)
    /// Fraction de `max_capital_per_market` RÉSERVÉE à la complétion : les achats
    /// qui AUGMENTENT le déséquilibre (paris d'ouverture) ne peuvent pas pousser le
    /// déployé au-delà de (1 − réserve) × capital ; les achats qui RÉDUISENT le
    /// déséquilibre (verrouillage de paires) disposent du budget complet.
    pub completion_reserve: f64,
    // — v7, rétro-ingénierie 0xb27b (2 383 fills analysés) —
    /// H1 : les fills directionnels exigent l'alignement avec la tendance 30-60 s
    /// (70 % d'alignement mesuré chez la cible) …
    pub trend_filter: bool,
    /// … ET un micro-repli 5 s contre le côté acheté (43 % d'alignement 5 s mesuré :
    /// il achète le pullback dans la tendance).
    pub pullback_filter: bool,
    /// H6 : la complétion (réduire l'imbalance) BYPASSE le gate fair (chez la cible :
    /// edge médian −4,3¢ = prime d'assurance), mais reste bornée :
    pub completion_max_price: f64, // prix max d'une jambe de complétion (0.35)
    pub completion_max_pair: f64,  // la paire complétée doit rester ≤ ce plafond (0.99)
}

/// Durée d'une fenêtre (marchés 5 min uniquement).
const WINDOW_SECS: i64 = 300;

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
}

/// Décision d'achat taker (marketable limit à `price`, fee incluse).
#[allow(dead_code)] // chemin taker v5 conservé pour les tests
#[derive(Debug, Clone)]
pub struct BuyDecision {
    pub side: Side,
    pub price: f64, // prix d'exécution fee-inclusive
    pub size: f64,  // parts (entier)
}

/// Quote maker désirée (bid restant) — mode exécution v8 (copie complète).
#[derive(Debug, Clone, PartialEq)]
pub struct BidQuote {
    pub side: Side,
    pub price: f64,      // prix du bid (pas de frais modélisés côté maker)
    pub size: f64,       // parts
    pub completion: bool, // true = jambe d'assurance (réduit l'imbalance)
}

/// État blended d'une fenêtre + logique d'entrée.
pub struct SpreadCaptureEngine {
    pub cfg: SpreadCaptureConfig,
    pub shares_up: f64,
    pub cost_up: f64,
    pub shares_dn: f64,
    pub cost_dn: f64,
    last_clip_up: Option<i64>, // horodatage (s) du dernier clip par côté
    last_clip_dn: Option<i64>,
}

impl SpreadCaptureEngine {
    pub fn new(cfg: SpreadCaptureConfig) -> Self {
        Self {
            cfg,
            shares_up: 0.0,
            cost_up: 0.0,
            shares_dn: 0.0,
            cost_dn: 0.0,
            last_clip_up: None,
            last_clip_dn: None,
        }
    }

    /// MERGE de `pairs` paires (Up+Down → 1$) : les parts sortent de l'inventaire
    /// au coût moyen de chaque côté → le budget de la fenêtre est RECYCLÉ (la
    /// cible redéploie son capital 2-3× par fenêtre grâce aux merges continus).
    /// Les coûts moyens restent inchangés.
    pub fn on_merge(&mut self, pairs: f64) {
        let p = pairs.min(self.shares_up).min(self.shares_dn).max(0.0);
        if p <= 0.0 {
            return;
        }
        self.cost_up -= self.avg(Side::Up) * p;
        self.cost_dn -= self.avg(Side::Down) * p;
        self.shares_up -= p;
        self.shares_dn -= p;
        self.cost_up = self.cost_up.max(0.0);
        self.cost_dn = self.cost_dn.max(0.0);
    }

    /// Nouvelle fenêtre 5 min : état blended remis à zéro.
    pub fn reset_window(&mut self) {
        self.shares_up = 0.0;
        self.cost_up = 0.0;
        self.shares_dn = 0.0;
        self.cost_dn = 0.0;
        self.last_clip_up = None;
        self.last_clip_dn = None;
    }

    pub fn avg(&self, side: Side) -> f64 {
        let (sh, c) = match side {
            Side::Up => (self.shares_up, self.cost_up),
            Side::Down => (self.shares_dn, self.cost_dn),
        };
        if sh > 0.0 { c / sh } else { 0.0 }
    }

    /// Coût de paire blended courant (None tant qu'un côté est vide).
    pub fn pair_cost(&self) -> Option<f64> {
        if self.shares_up > 0.0 && self.shares_dn > 0.0 {
            Some(self.avg(Side::Up) + self.avg(Side::Down))
        } else {
            None
        }
    }

    pub fn imbalance(&self) -> f64 {
        self.shares_up - self.shares_dn
    }

    pub fn deployed(&self) -> f64 {
        self.cost_up + self.cost_dn
    }

    #[allow(dead_code)] // chemin taker v5 (tests)
    fn fee_share(&self) -> f64 {
        self.cfg.fee_per_pair / 2.0
    }

    /// Évalue UN côté. `ask` = meilleur ask affiché, `ask_size` = profondeur affichée,
    /// `fair` = juste valeur Binance de CE côté. `trend_up` = signe de la tendance
    /// 30-60 s Binance ; `pullback` = micro-repli 5 s contre CE côté (timing H1).
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)] // chemin taker v5 (tests)
    fn decide_side(
        &self,
        side: Side,
        ask: f64,
        ask_size: f64,
        fair: f64,
        remaining_s: i64,
        now_s: i64,
        trend_up: bool,
        pullback: bool,
    ) -> Option<BuyDecision> {
        let c = &self.cfg;
        if remaining_s < c.min_seconds || ask <= 0.0 || ask_size <= 0.0 {
            return None;
        }
        let last = match side {
            Side::Up => self.last_clip_up,
            Side::Down => self.last_clip_dn,
        };
        if let Some(t) = last {
            if now_s - t < c.clip_interval_s {
                return None;
            }
        }
        let ask_eff = ask + self.fee_share();
        let (my_shares, other_shares) = match side {
            Side::Up => (self.shares_up, self.shares_dn),
            Side::Down => (self.shares_dn, self.shares_up),
        };
        let other_avg = self.avg(match side {
            Side::Up => Side::Down,
            Side::Down => Side::Up,
        });
        let is_completion = my_shares < other_shares;
        let mut clip;
        if is_completion {
            // — JAMBE DE COMPLÉTION (H6) : réduit l'imbalance, verrouille des paires.
            // Bypasse le gate fair (prime d'assurance, comme la cible : −4,3¢ d'edge
            // médian assumé) mais reste bornée : jambe ≤ completion_max_price ET
            // paire résultante ≤ completion_max_pair (nos garde-fous — la cible a
            // perdu −674$ en complétant à 123¢, on ne copie pas cette partie).
            if ask_eff > c.completion_max_price {
                return None;
            }
            if other_shares > 0.0 && ask_eff > c.completion_max_pair - other_avg {
                return None;
            }
            clip = (c.base_clip + c.depth_gain * (c.completion_max_price - ask_eff))
                .clamp(0.0, c.max_clip);
            clip = clip.min(other_shares - my_shares); // vise l'équilibre, jamais au-delà
            let capital_room = c.max_capital_per_market - self.deployed();
            clip = clip.min((capital_room / ask_eff).max(0.0));
        } else {
            // — JAMBE DIRECTIONNELLE (H1+H2) : creuse l'imbalance dans le sens de la
            // tendance, sur un micro-repli, sous la juste valeur.
            if WINDOW_SECS - remaining_s < c.min_window_age_s {
                return None;
            }
            let trend_ok = match side {
                Side::Up => trend_up,
                Side::Down => !trend_up,
            };
            if c.trend_filter && !trend_ok {
                return None;
            }
            if c.pullback_filter && !pullback {
                return None;
            }
            let ceiling = if other_shares > 0.0 {
                c.c_raw - other_avg
            } else {
                c.opening_leg_max
            };
            if ask_eff > ceiling {
                return None;
            }
            // ⚡ Gate Binance : n'acheter que sous la juste valeur driftée.
            if ask > fair - c.gate_margin {
                return None;
            }
            clip = (c.base_clip + c.depth_gain * (ceiling - ask_eff)).clamp(0.0, c.max_clip);
            let room = c.max_imbalance + other_shares - my_shares;
            clip = clip.min(room.max(0.0));
            let capital_cap = c.max_capital_per_market * (1.0 - c.completion_reserve);
            let capital_room = capital_cap - self.deployed();
            clip = clip.min((capital_room / ask_eff).max(0.0));
        }
        clip = clip.min(c.max_clip_usdc / ask_eff.max(0.01));
        clip = clip.min(ask_size); // borné par la profondeur affichée (fill honnête)
        let clip = clip.floor();
        if clip < 1.0 {
            return None;
        }
        Some(BuyDecision { side, price: ask_eff, size: clip })
    }

    /// v8 MAKER — quotes désirées (bids restants), recalibré sur 234 fenêtres :
    /// - directionnel : côté de la tendance, prix = min(best_bid+tick, fair−M,
    ///   directional_max 0.90) — SANS plafond blended ni opening cap (il charge le
    ///   favori jusqu'à 87¢) ; poche d'ouverture ; cooldown après fill.
    /// - complétion : côté déficitaire, prix = min(best_bid+tick, completion caps),
    ///   taille = le déficit ; budget complet ; dès la 1re seconde.
    /// `size_factor` = disjoncteur de séries perdantes (1.0 / 0.25 / 0).
    /// `trend_up` : None = tendance NON confirmée (drift instable) → aucun bid
    /// directionnel (la complétion reste active). `directional_min` : jamais de
    /// directionnel sur un côté que le marché price sous ce seuil (la cible
    /// accumule le FAVORI à 66-72¢, jamais le couteau qui tombe).
    /// Aucun frais modélisé côté maker (les rebates réels sont estimés à part).
    #[allow(clippy::too_many_arguments)]
    pub fn desired_bids(
        &self,
        best_bid_up: f64,
        best_bid_dn: f64,
        fair_up: f64,
        remaining_s: i64,
        now_s: i64,
        trend_up: Option<bool>,
        tick: f64,
        directional_max: f64,
        directional_min: f64,
        size_factor: f64,
    ) -> Vec<BidQuote> {
        let c = &self.cfg;
        let mut out = Vec::new();
        if remaining_s < c.min_seconds || size_factor <= 0.0 {
            return out;
        }
        let tick = if tick > 0.0 { tick } else { 0.01 };
        for side in [Side::Up, Side::Down] {
            let (my, other, bb, fair) = match side {
                Side::Up => (self.shares_up, self.shares_dn, best_bid_up, fair_up),
                Side::Down => (self.shares_dn, self.shares_up, best_bid_dn, 1.0 - fair_up),
            };
            if bb <= 0.0 {
                continue;
            }
            let other_avg = self.avg(match side {
                Side::Up => Side::Down,
                Side::Down => Side::Up,
            });
            let last = match side {
                Side::Up => self.last_clip_up,
                Side::Down => self.last_clip_dn,
            };
            let cooled = last.map_or(true, |t| now_s - t >= c.clip_interval_s);
            let is_completion = my < other;
            let (price_cap, mut size, capital_cap) = if is_completion {
                let cap = c
                    .completion_max_price
                    .min(if other > 0.0 { c.completion_max_pair - other_avg } else { c.completion_max_price });
                (cap, (other - my).min(c.max_clip), c.max_capital_per_market)
            } else {
                // Directionnel : tendance CONFIRMÉE + côté favori uniquement.
                let Some(tu) = trend_up else { continue };
                let trend_ok = match side {
                    Side::Up => tu,
                    Side::Down => !tu,
                };
                if (c.trend_filter && !trend_ok)
                    || bb < directional_min
                    || WINDOW_SECS - remaining_s < c.min_window_age_s
                    || !cooled
                {
                    continue;
                }
                let cap = (fair - c.gate_margin).min(directional_max);
                let room = (c.max_imbalance + other - my).max(0.0);
                (cap, c.base_clip.min(room), c.max_capital_per_market * (1.0 - c.completion_reserve))
            };
            if is_completion && !cooled {
                continue;
            }
            // Prix du bid : on améliore le touch d'un tick, borné par le cap.
            let price = ((bb + tick).min(price_cap) / tick).floor() * tick;
            if price < 0.01 {
                continue;
            }
            size = size.min(c.max_clip_usdc / price.max(0.01));
            let capital_room = capital_cap - self.deployed();
            size = size.min((capital_room / price).max(0.0));
            size = (size * size_factor).floor();
            if size < 1.0 {
                continue;
            }
            out.push(BidQuote { side, price, size, completion: is_completion });
        }
        out
    }

    /// Évalue les deux côtés pour ce tick.
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)] // chemin taker v5 (tests)
    pub fn decide(
        &self,
        ask_up: f64,
        ask_up_size: f64,
        ask_dn: f64,
        ask_dn_size: f64,
        fair_up: f64,
        remaining_s: i64,
        now_s: i64,
        trend_up: bool,
        pullback_up: bool,
        pullback_dn: bool,
    ) -> Vec<BuyDecision> {
        let mut out = Vec::new();
        if let Some(d) = self.decide_side(
            Side::Up, ask_up, ask_up_size, fair_up, remaining_s, now_s, trend_up, pullback_up,
        ) {
            out.push(d);
        }
        if let Some(d) = self.decide_side(
            Side::Down, ask_dn, ask_dn_size, 1.0 - fair_up, remaining_s, now_s, trend_up,
            pullback_dn,
        ) {
            out.push(d);
        }
        out
    }

    /// À appeler après un fill effectif (paper ou live).
    pub fn on_fill(&mut self, side: Side, price: f64, size: f64, now_s: i64) {
        match side {
            Side::Up => {
                self.shares_up += size;
                self.cost_up += size * price;
                self.last_clip_up = Some(now_s);
            }
            Side::Down => {
                self.shares_dn += size;
                self.cost_dn += size * price;
                self.last_clip_dn = Some(now_s);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SpreadCaptureConfig {
        SpreadCaptureConfig {
            c_raw: 0.95,
            fee_per_pair: 0.03,
            opening_leg_max: 0.55,
            max_imbalance: 40.0,
            base_clip: 10.0,
            max_clip: 20.0,
            depth_gain: 60.0,
            max_clip_usdc: 6.0,
            max_capital_per_market: 20.0,
            min_seconds: 10,
            clip_interval_s: 15,
            gate_margin: 0.04,
            min_window_age_s: 15,
            completion_reserve: 0.5,
            trend_filter: true,
            pullback_filter: true,
            completion_max_price: 0.35,
            completion_max_pair: 0.99,
        }
    }

    fn eng() -> SpreadCaptureEngine {
        SpreadCaptureEngine::new(cfg())
    }

    // Jambe DIRECTIONNELLE : trend + pullback + gate fair + plafonds.
    #[test]
    fn directional_requires_trend_alignment() {
        let e = eng();
        // Tendance baissiere -> achat Up directionnel refuse, meme bradee.
        assert!(e.decide_side(Side::Up, 0.40, 100.0, 0.55, 200, 0, false, true).is_none());
        // Tendance haussiere -> OK.
        assert!(e.decide_side(Side::Up, 0.40, 100.0, 0.55, 200, 0, true, true).is_some());
    }

    #[test]
    fn directional_requires_pullback() {
        let e = eng();
        // Pas de micro-repli 5s -> on n'achete pas (on ne court pas apres le prix).
        assert!(e.decide_side(Side::Up, 0.40, 100.0, 0.55, 200, 0, true, false).is_none());
        assert!(e.decide_side(Side::Up, 0.40, 100.0, 0.55, 200, 0, true, true).is_some());
    }

    #[test]
    fn gate_binance_blocks_knife_catching() {
        let e = eng();
        // ask 0.30 mais fair 0.32 -> 0.30 > 0.32-0.04 -> refus.
        assert!(e.decide_side(Side::Up, 0.30, 100.0, 0.32, 200, 0, true, true).is_none());
        assert!(e.decide_side(Side::Up, 0.30, 100.0, 0.45, 200, 0, true, true).is_some());
    }

    #[test]
    fn opening_leg_above_cap_rejected() {
        let e = eng();
        assert!(e.decide_side(Side::Up, 0.60, 100.0, 0.90, 200, 0, true, true).is_none());
        assert!(e.decide_side(Side::Up, 0.40, 100.0, 0.50, 200, 0, true, true).is_some());
    }

    #[test]
    fn deeper_dip_bigger_clip() {
        let e = eng();
        let sh = e.decide_side(Side::Up, 0.50, 1000.0, 0.60, 200, 0, true, true).unwrap();
        let dp = e.decide_side(Side::Up, 0.35, 1000.0, 0.60, 200, 0, true, true).unwrap();
        assert!(dp.size > sh.size);
    }

    #[test]
    fn imbalance_cap_enforced() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.30, 40.0, 0);
        assert!(e.decide_side(Side::Up, 0.25, 100.0, 0.45, 200, 100, true, true).is_none());
    }

    #[test]
    fn no_entry_in_first_seconds_of_window() {
        let e = eng();
        assert!(e.decide_side(Side::Up, 0.40, 100.0, 0.55, 290, 0, true, true).is_none());
        assert!(e.decide_side(Side::Up, 0.40, 100.0, 0.55, 280, 0, true, true).is_some());
    }

    #[test]
    fn clip_interval_throttles_same_side() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.40, 10.0, 100);
        assert!(e.decide_side(Side::Up, 0.35, 100.0, 0.55, 200, 105, true, true).is_none());
        assert!(e.decide_side(Side::Up, 0.35, 100.0, 0.55, 200, 120, true, true).is_some());
    }

    #[test]
    fn no_new_leg_under_min_seconds() {
        let e = eng();
        assert!(e.decide_side(Side::Up, 0.30, 100.0, 0.50, 9, 0, true, true).is_none());
    }

    // Jambe de COMPLETION : bypasse fair/trend/pullback, bornee en prix et en paire.
    #[test]
    fn completion_bypasses_gate_and_trend() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.55, 30.0, 0); // long Up -> Down = completion
        // fair Down tres basse (0.05), tendance Up, pas de pullback : achete quand meme
        // le cote mourant a 10c (prime d'assurance, comme la cible).
        let d = e.decide_side(Side::Down, 0.10, 100.0, 0.05, 200, 100, true, false);
        assert!(d.is_some(), "la completion doit bypasser le gate");
    }

    #[test]
    fn completion_bounded_in_price_and_pair() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.55, 30.0, 0);
        // jambe > completion_max_price (0.35) -> refus (pas d'assurance hors de prix).
        assert!(e.decide_side(Side::Down, 0.40, 100.0, 0.60, 200, 100, true, true).is_none());
        // paire resultante > 0.99 -> refus : avg_up 0.565 (fee incl) -> plafond 0.425,
        // mais completion_max_price = 0.35 borne avant. Testons le plafond paire :
        let mut e2 = eng();
        e2.on_fill(Side::Up, 0.80, 20.0, 0); // avg fee incl 0.815 (16,3 $ déployés)
        // 0.99 - 0.815 = 0.175 -> ask 0.20 (eff 0.215) refuse, ask 0.15 (eff 0.165) ok.
        assert!(e2.decide_side(Side::Down, 0.20, 100.0, 0.50, 200, 100, true, true).is_none());
        assert!(e2.decide_side(Side::Down, 0.15, 100.0, 0.50, 200, 100, true, true).is_some());
    }

    #[test]
    fn completion_never_overshoots_balance() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.50, 8.0, 0);
        let d = e.decide_side(Side::Down, 0.10, 100.0, 0.50, 200, 100, true, true).unwrap();
        assert!(d.size <= 8.0, "complete au plus jusqu'a l'equilibre, size={}", d.size);
    }

    #[test]
    fn completion_allowed_in_first_seconds() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.50, 20.0, 0);
        // min_window_age ne s'applique qu'au directionnel.
        assert!(e.decide_side(Side::Down, 0.10, 100.0, 0.50, 292, 100, true, true).is_some());
    }

    #[test]
    fn completion_reserve_keeps_powder() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.50, 20.0, 0); // 10$ deployes (poche ouverture pleine)
        // Nouveau directionnel Up -> refus (poche epuisee).
        assert!(e.decide_side(Side::Up, 0.30, 100.0, 0.45, 200, 100, true, true).is_none());
        // Completion Down -> budget complet -> accepte.
        assert!(e.decide_side(Side::Down, 0.30, 100.0, 0.45, 200, 100, true, true).is_some());
    }

    #[test]
    fn capital_cap_enforced() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.50, 30.0, 0);   // 15 $
        e.on_fill(Side::Down, 0.40, 12.0, 0); // 4,8 $ -> total 19,8 $
        assert!(e.decide_side(Side::Down, 0.30, 100.0, 0.50, 200, 100, true, true).is_none());
    }

    #[test]
    fn pair_cost_below_one_after_both_dips() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.43, 20.0, 0);
        e.on_fill(Side::Down, 0.35, 20.0, 60);
        let pc = e.pair_cost().unwrap();
        assert!((pc - 0.78).abs() < 1e-9);
    }

    #[test]
    fn merge_recycles_window_budget() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.50, 20.0, 0);
        e.on_fill(Side::Down, 0.48, 20.0, 0);
        let before = e.deployed();
        assert!(before > 19.0);
        e.on_merge(20.0); // 20 paires fusionnees -> tout ressort au cout moyen
        assert!(e.deployed() < 1e-9, "deployed={}", e.deployed());
        assert!(e.shares_up.abs() < 1e-9 && e.shares_dn.abs() < 1e-9);
        // borne : on ne peut pas merger plus que min(up, dn)
        e.on_fill(Side::Up, 0.60, 10.0, 0);
        e.on_merge(99.0);
        assert!((e.shares_up - 10.0).abs() < 1e-9); // rien a merger (dn=0)
    }

    #[test]
    fn no_directional_without_confirmed_trend() {
        let mut e = eng();
        // tendance non confirmee -> zero directionnel, mais la completion vit.
        let q = e.desired_bids(0.48, 0.50, 0.60, 200, 0, None, 0.01, 0.90, 0.40, 1.0);
        assert!(q.is_empty());
        e.on_fill(Side::Up, 0.60, 12.0, 0);
        let q = e.desired_bids(0.48, 0.30, 0.60, 200, 100, None, 0.01, 0.90, 0.40, 1.0);
        assert_eq!(q.len(), 1);
        assert!(q[0].completion && q[0].side == Side::Down);
    }

    #[test]
    fn no_directional_on_dying_side() {
        let e = eng();
        // drift dit Down mais le marche price Down a 33c (< 40c) -> pas de pari
        // sur le couteau qui tombe ; et pas de directionnel Up (tendance Down).
        let q = e.desired_bids(0.65, 0.33, 0.35, 200, 0, Some(false), 0.01, 0.90, 0.40, 1.0);
        assert!(q.iter().all(|b| b.completion), "{q:?}");
    }

    // ── v8 MAKER : desired_bids ──
    #[test]
    fn maker_directional_on_trend_side_only() {
        let e = eng();
        // tendance Up, fenetre agee, fair large -> bid directionnel Up seulement.
        let q = e.desired_bids(0.48, 0.50, 0.60, 200, 0, Some(true), 0.01, 0.90, 0.40, 1.0);
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].side, Side::Up);
        assert!(!q[0].completion);
        // prix = min(bb+tick, fair-M) = min(0.49, 0.56) = 0.49
        assert!((q[0].price - 0.49).abs() < 1e-9, "px={}", q[0].price);
    }

    #[test]
    fn maker_directional_capped_by_fair_and_absolute() {
        let e = eng();
        // fair 0.50 -> cap 0.46 < bb+tick 0.49 -> bid a 0.46.
        let q = e.desired_bids(0.48, 0.0, 0.50, 200, 0, Some(true), 0.01, 0.90, 0.40, 1.0);
        assert!((q[0].price - 0.46).abs() < 1e-9, "px={}", q[0].price);
        // borne absolue 0.90 meme si fair tres haute.
        let q = e.desired_bids(0.95, 0.0, 0.99, 200, 0, Some(true), 0.01, 0.90, 0.40, 1.0);
        assert!(q.is_empty() || q[0].price <= 0.90 + 1e-9);
    }

    #[test]
    fn maker_completion_targets_deficit_and_caps() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.60, 12.0, 0); // long Up -> completion Down
        let q = e.desired_bids(0.40, 0.30, 0.70, 200, 100, Some(true), 0.01, 0.90, 0.40, 1.0);
        let comp: Vec<_> = q.iter().filter(|b| b.completion).collect();
        assert_eq!(comp.len(), 1);
        assert_eq!(comp[0].side, Side::Down);
        assert!(comp[0].size <= 12.0, "vise le deficit, size={}", comp[0].size);
        // cap completion : min(bb+tick 0.31, 0.65, 1.05-0.60=0.45) = 0.31
        assert!((comp[0].price - 0.31).abs() < 1e-9, "px={}", comp[0].price);
    }

    #[test]
    fn maker_size_factor_circuit_breaker() {
        let e = eng();
        let full = e.desired_bids(0.48, 0.0, 0.60, 200, 0, Some(true), 0.01, 0.90, 0.40, 1.0);
        let quarter = e.desired_bids(0.48, 0.0, 0.60, 200, 0, Some(true), 0.01, 0.90, 0.40, 0.25);
        let zero = e.desired_bids(0.48, 0.0, 0.60, 200, 0, Some(true), 0.01, 0.90, 0.40, 0.0);
        assert!(quarter[0].size <= (full[0].size * 0.25).ceil());
        assert!(zero.is_empty());
    }

    #[test]
    fn maker_cooldown_after_fill() {
        let mut e = eng();
        e.on_fill(Side::Up, 0.49, 10.0, 100);
        // 5 s apres : cooldown directionnel actif -> pas de bid Up.
        let q = e.desired_bids(0.48, 0.0, 0.60, 200, 105, Some(true), 0.01, 0.90, 0.40, 1.0);
        assert!(q.iter().all(|b| b.side != Side::Up));
        // 20 s apres : de nouveau quote.
        let q = e.desired_bids(0.48, 0.0, 0.60, 200, 120, Some(true), 0.01, 0.90, 0.40, 1.0);
        assert!(q.iter().any(|b| b.side == Side::Up));
    }

    #[test]
    fn fill_bounded_by_displayed_depth() {
        let e = eng();
        let d = e.decide_side(Side::Up, 0.35, 3.0, 0.60, 200, 0, true, true).unwrap();
        assert!(d.size <= 3.0);
    }
}
