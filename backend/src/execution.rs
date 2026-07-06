//! Exécution market-making (R3) : sémantique **maker** N / N+1.
//!
//! - Tick N : on POSTE des quotes (`PostedQuotes`).
//! - Tick N+1 : on tente des fills MAKER contre les quotes du tick précédent —
//!   le marché vient à NOTRE prix. Fill **probabiliste** (file simplifiée) et
//!   **plafonné par la liquidité** des niveaux croisés. Garde conservatrice :
//!   notre quote doit être *strictement* meilleure que le TOB.
//! - KILL (R5) : `KillState` partagé ; pendant la pause, AUCUN fill ni quote.

#![allow(dead_code)] // module hérité (fill probabiliste v1, remplacé par la règle de cross)
use std::sync::atomic::{AtomicI64, Ordering};

use crate::bankroll::{BankrollEngine, BlockReason, TradeDecision};
use crate::connectors::polymarket::{Market, PolyBook};
use crate::inventory::PaperEngine;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// État KILL partagé entre la task de réception du signal et la boucle de cotation.
#[derive(Default)]
pub struct KillState {
    paused_until_ms: AtomicI64,
}

impl KillState {
    pub fn new() -> Self {
        Self { paused_until_ms: AtomicI64::new(0) }
    }
    /// Arme une pause de `cooldown_ms` à partir de maintenant (ne raccourcit jamais).
    pub fn trigger(&self, cooldown_ms: i64) {
        let until = now_ms() + cooldown_ms;
        let cur = self.paused_until_ms.load(Ordering::Relaxed);
        if until > cur {
            self.paused_until_ms.store(until, Ordering::Relaxed);
        }
    }
    pub fn is_paused(&self) -> bool {
        now_ms() < self.paused_until_ms.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PostedQuotes {
    pub up_bid: f64,
    pub up_ask: f64,
    pub dn_bid: f64,
    pub dn_ask: f64,
}

#[derive(Debug, Default)]
pub struct FillReport {
    pub fills: u32,
    pub last_block: Option<BlockReason>,
}

pub struct ExecutionEngine {
    posted: Option<PostedQuotes>,
    rng: u64,
    maker_fill_prob: f64,
}

impl ExecutionEngine {
    pub fn new(maker_fill_prob: f64) -> Self {
        Self {
            posted: None,
            rng: 0x9E3779B97F4A7C15 ^ (now_ms() as u64),
            maker_fill_prob,
        }
    }

    /// xorshift64 → [0,1).
    fn rand01(&mut self) -> f64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        (x >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Tick N : enregistre les quotes à matcher au tick suivant.
    pub fn post_quotes(&mut self, q: PostedQuotes) {
        self.posted = Some(q);
    }

    pub fn clear_quotes(&mut self) {
        self.posted = None;
    }

    /// Liquidité (somme des tailles) des asks ≤ `price` (ce qui pourrait croiser notre bid).
    fn ask_liquidity_at_or_below(book: &PolyBook, price: f64) -> f64 {
        book.asks.iter().filter(|l| l.price <= price + 1e-9).map(|l| l.size).sum()
    }
    /// Liquidité des bids ≥ `price` (ce qui pourrait croiser notre ask).
    fn bid_liquidity_at_or_above(book: &PolyBook, price: f64) -> f64 {
        book.bids.iter().filter(|l| l.price >= price - 1e-9).map(|l| l.size).sum()
    }

    /// Tick N+1 : tente les fills maker contre `self.posted`. Applique via le bankroll.
    #[allow(clippy::too_many_arguments)]
    pub fn simulate_maker_fills(
        &mut self,
        paper: &mut PaperEngine,
        bankroll: &BankrollEngine,
        up_book: &PolyBook,
        down_book: &PolyBook,
        up_mid: f64,
        down_mid: f64,
        market: &Market,
        remaining_s: i64,
    ) -> FillReport {
        let mut rep = FillReport::default();
        let Some(p) = self.posted else { return rep };

        // (côté, est_achat, posted_price, book, mid)
        let actions: [(&str, bool, f64, &PolyBook, f64); 4] = [
            ("up", true, p.up_bid, up_book, up_mid),
            ("up", false, p.up_ask, up_book, up_mid),
            ("down", true, p.dn_bid, down_book, down_mid),
            ("down", false, p.dn_ask, down_book, down_mid),
        ];

        for (side, is_buy, price, book, mid) in actions {
            let (Some(best_bid), Some(best_ask)) = (book.best_bid(), book.best_ask()) else {
                continue;
            };
            // Modèle maker : on est POSÉ au (ou dans le) touch et frappé par le flux.
            //   - BUY : notre bid ne croise pas (< best_ask) ET est compétitif (≥ best_bid).
            //   - SELL: notre ask ne croise pas (> best_bid) ET est compétitif (≤ best_ask).
            // Le fill arrive de façon probabiliste (flux entrant ; pas de tick trades en paper).
            let competitive = if is_buy {
                price < best_ask - 1e-9 && price >= best_bid - 1e-9
            } else {
                price > best_bid + 1e-9 && price <= best_ask + 1e-9
            };
            if !competitive {
                continue;
            }
            // Fill probabiliste (file d'attente / flux simplifié).
            if self.rand01() > self.maker_fill_prob {
                continue;
            }
            // Plafond de taille = liquidité présente au touch (proxy de flux).
            let liq = if is_buy {
                Self::bid_liquidity_at_or_above(book, best_bid)
            } else {
                Self::ask_liquidity_at_or_below(book, best_ask)
            };
            if liq <= 0.0 {
                continue;
            }

            let net = BankrollEngine::net_exposure(&paper.state);
            let side_balance = if side == "up" { paper.state.up_balance } else { paper.state.down_balance };
            let equity = BankrollEngine::equity(&paper.state, up_mid, down_mid);

            if is_buy {
                let adds = if side == "up" { net >= 0.0 } else { net <= 0.0 };
                match bankroll.evaluate_buy(&paper.state, equity, price, mid, side_balance, remaining_s, adds, market) {
                    TradeDecision::Allowed { size } => {
                        let fill = size.min(liq).floor();
                        if fill > 0.0 && paper.try_buy(side, price, fill, "maker") {
                            rep.fills += 1;
                        }
                    }
                    TradeDecision::Blocked { reason } => rep.last_block = Some(reason),
                }
            } else {
                match bankroll.evaluate_sell(side_balance, market) {
                    TradeDecision::Allowed { size } => {
                        let fill = size.min(liq).floor();
                        if fill > 0.0 && paper.try_sell(side, price, fill, "maker") {
                            rep.fills += 1;
                        }
                    }
                    TradeDecision::Blocked { reason } => rep.last_block = Some(reason),
                }
            }
        }
        rep
    }
}
