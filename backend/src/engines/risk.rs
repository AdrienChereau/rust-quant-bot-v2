//! Moteur de risque (J7) : Avellaneda-Stoikov + reward-adjusted spread.
//!
//! 1. **Skew d'inventaire (A-S)** :
//!      reservation = fair − inventory · γ · σ² · t
//!      half_spread_AS = ½ · γ · σ² · t + (1/γ) · ln(1 + γ/κ)
//! 2. **Reward-adjusted** : le profit vient surtout des liquidity rewards Polymarket
//!    (score `S(v,s) = ((v−s)/v)²`). On estime notre part de score vs les concurrents
//!    présents dans la bande `rewardsMaxSpread`, et on **resserre** le spread d'une
//!    subvention proportionnelle → cotation plus agressive près du mid.
//!
//! Tous les prix sont des probabilités Polymarket dans [0.01, 0.99].

#![allow(dead_code)] // module hérité (quoting Avellaneda-Stoikov v1, remplacé par le maker v8)
use crate::connectors::polymarket::PolyBook;

#[derive(Debug, Clone)]
pub struct QuoteInputs {
    pub fair: f64,            // proba "Up" du modèle BS (J5)
    pub mid: f64,            // mid du carnet Polymarket
    pub sigma: f64,          // vol annualisée (J4)
    pub t_years: f64,        // horizon restant
    pub inventory: f64,      // position NETTE (net = up − down ; convention R1)
    pub gamma: f64,          // aversion au risque A-S (pilote le skew d'inventaire)
    pub kappa: f64,          // (conservé pour compat ; non utilisé depuis R2)
    pub base_half_spread_cents: f64, // R2 : demi-spread de base (cents)
    pub tick: f64,           // pas de prix (0.01)
    pub rewards_max_spread_cents: f64, // v, en cents (ex 4.5)
    pub our_size: f64,       // taille de nos ordres (pour le score reward)
    pub reward_pool_per_min: f64, // pool de reward estimé $/min (config/API)
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QuoteResult {
    pub reservation: f64,
    pub half_spread_as: f64,
    pub expected_reward: f64,
    pub half_spread_final: f64,
    pub bid: f64,
    pub ask: f64,
    pub in_reward_band: bool,
}

/// Score de reward Polymarket d'un ordre à `s` cents du mid : `((v−s)/v)²`.
pub fn reward_score(v_cents: f64, s_cents: f64) -> f64 {
    if v_cents <= 0.0 || s_cents < 0.0 || s_cents > v_cents {
        return 0.0;
    }
    let r = (v_cents - s_cents) / v_cents;
    r * r
}

/// Estime le score total des concurrents présents dans la bande de reward
/// (somme de `score · size` des deux côtés du carnet, dans `v` cents du mid).
pub fn competitor_q(book: &PolyBook, mid: f64, v_cents: f64) -> f64 {
    let mut q = 0.0;
    for lvl in book.bids.iter().chain(book.asks.iter()) {
        let s_cents = (lvl.price - mid).abs() * 100.0;
        if s_cents <= v_cents {
            q += reward_score(v_cents, s_cents) * lvl.size;
        }
    }
    q
}

fn clamp_tick(p: f64, tick: f64) -> f64 {
    let snapped = (p / tick).round() * tick;
    snapped.clamp(0.01, 0.99)
}

/// Quotes **inside-spread** (façon poly-maker `get_order_prices`) : on se pose juste à
/// l'intérieur du touch — `bid = best_bid + tick`, `ask = best_ask − tick` — donc on
/// **achète près du bid (sous le mid)** → les paires coûtent < 1 $ → la fusion à 1 $
/// est rentable. **Pas de subvention reward** (edge pur) ni de skew de prix A-S
/// (l'inventaire se gère par la taille/les gates côté exécuteur, pas par le prix).
pub fn compute_quote(inp: &QuoteInputs, book: &PolyBook) -> QuoteResult {
    let tick = if inp.tick > 0.0 { inp.tick } else { 0.01 };
    let top_bid = book.best_bid().unwrap_or((inp.mid - tick).clamp(0.01, 0.99));
    let top_ask = book.best_ask().unwrap_or((inp.mid + tick).clamp(0.01, 0.99));

    // On améliore chaque côté du touch d'un tick (on devient le meilleur bid/ask).
    let mut bid = top_bid + tick;
    let mut ask = top_ask - tick;

    // Anti-croisement : ne jamais traverser le touch adverse.
    if bid >= top_ask - 1e-9 {
        bid = top_bid;
    }
    if ask <= top_bid + 1e-9 {
        ask = top_ask;
    }
    // Anti self-cross : si nos deux prix se rejoignent, on revient au touch.
    if (bid - ask).abs() < tick - 1e-9 {
        bid = top_bid;
        ask = top_ask;
    }

    let bid = clamp_tick(bid, tick);
    let ask = clamp_tick(ask, tick);
    let v = inp.rewards_max_spread_cents;
    let in_reward_band = (bid - inp.mid).abs() * 100.0 <= v && (ask - inp.mid).abs() * 100.0 <= v;

    QuoteResult {
        reservation: inp.mid,
        half_spread_as: (ask - bid) / 2.0,
        expected_reward: 0.0,
        half_spread_final: (ask - bid) / 2.0,
        bid,
        ask,
        in_reward_band,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::polymarket::{Level, PolyBook};

    fn base_inputs() -> QuoteInputs {
        QuoteInputs {
            fair: 0.50,
            mid: 0.50,
            sigma: 0.6,
            t_years: 300.0 / (365.0 * 24.0 * 3600.0),
            inventory: 0.0,
            gamma: 0.1,
            kappa: 1.5,
            base_half_spread_cents: 2.0,
            tick: 0.01,
            rewards_max_spread_cents: 4.5,
            our_size: 50.0,
            reward_pool_per_min: 0.0,
        }
    }

    fn empty_book() -> PolyBook {
        PolyBook::default()
    }

    #[test]
    fn symmetric_when_flat_inventory() {
        let inp = base_inputs();
        let q = compute_quote(&inp, &empty_book());
        // Réservation = fair quand inventaire nul.
        assert!((q.reservation - 0.50).abs() < 1e-9);
        // Quotes symétriques autour du mid, à un tick près (arrondi au pas).
        assert!((0.50 - q.bid - (q.ask - 0.50)).abs() <= inp.tick + 1e-9);
        assert!(q.bid < 0.50 && q.ask > 0.50 && q.bid < q.ask);
    }

    #[test]
    fn reward_score_peaks_at_mid() {
        assert!((reward_score(4.5, 0.0) - 1.0).abs() < 1e-9);
        assert!(reward_score(4.5, 2.25) < reward_score(4.5, 0.5));
        assert_eq!(reward_score(4.5, 5.0), 0.0); // hors bande
    }

    #[test]
    fn quotes_inside_the_spread() {
        // Spread large (0.47/0.53) → on améliore d'un tick des deux côtés.
        let inp = base_inputs();
        let mut book = PolyBook::default();
        book.bids.push(Level { price: 0.47, size: 100.0 });
        book.asks.push(Level { price: 0.53, size: 100.0 });
        let q = compute_quote(&inp, &book);
        assert!((q.bid - 0.48).abs() < 1e-9, "bid={}", q.bid);
        assert!((q.ask - 0.52).abs() < 1e-9, "ask={}", q.ask);
        // On achète SOUS le mid et on vend AU-DESSUS → paire < 1$.
        assert!(q.bid < inp.mid && q.ask > inp.mid);
    }

    #[test]
    fn tight_spread_falls_back_to_touch() {
        // Spread de 2 ticks (0.49/0.51) : améliorer croiserait → on revient au touch.
        let inp = base_inputs();
        let mut book = PolyBook::default();
        book.bids.push(Level { price: 0.49, size: 100.0 });
        book.asks.push(Level { price: 0.51, size: 100.0 });
        let q = compute_quote(&inp, &book);
        assert!((q.bid - 0.49).abs() < 1e-9, "bid={}", q.bid);
        assert!((q.ask - 0.51).abs() < 1e-9, "ask={}", q.ask);
    }
}
