//! Contexte d'exécution LIVE — fait le pont entre la boucle de l'executor
//! (identique au paper : mêmes décisions, mêmes stats) et le CLOB réel.
//!
//! Responsabilités :
//!   • poser/annuler/repricer les bids GTC (+ FAK d'assurance)
//!   • collecter les fills : WS user (rapide) + poll /data/orders (autorité)
//!   • fournir à l'executor des `LiveFill` prêts à injecter dans sc.on_fill /
//!     le miroir comptable (PaperEngine, qui reste le grand livre)
//!
//! Le PaperEngine sert de MIROIR : chaque fill réel y est enregistré via
//! `try_buy` au prix réel — cash/balances/PnL suivent la même comptabilité
//! que le paper (comparaison A/B directe). Écart connu v1 : pas de merge ni
//! de redeem on-chain → le collatéral réel (affiché à part) diverge du miroir
//! du montant des positions non encore réglées.

use std::collections::HashMap;

use tokio::sync::{mpsc, watch};

use super::auth::LiveCredentials;
use super::orders::{self, OrderArgs, PlaceResult};
use super::user_ws::{self, FillEvent};

/// Fill prêt pour la boucle stratégie.
#[derive(Debug, Clone)]
pub struct LiveFill {
    pub is_up: bool,
    pub price: f64,
    pub size: f64,
    pub maker: bool,
}

/// Un de NOS ordres restants.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub order_id: String,
    pub price: f64,
    pub size: f64,
    pub matched: f64, // cumul fillé déjà comptabilisé
}

pub struct LiveCtx {
    pub creds: LiveCredentials,
    pub armed: bool,
    cond_tx: watch::Sender<Option<String>>,
    fill_rx: mpsc::UnboundedReceiver<FillEvent>,
    // token_ids du marché courant (associe asset_id/ordre → côté Up/Down)
    up_token: String,
    dn_token: String,
    condition_id: String,
    // ordre_id → (is_up, maker) pour attribuer les fills WS
    order_side: HashMap<String, (bool, bool)>,
    last_poll_ms: i64,
}

impl LiveCtx {
    /// Démarrage : auth SDK + sync allowance + WS user + heartbeats (dead-man).
    pub async fn start(creds: LiveCredentials, armed: bool) -> anyhow::Result<Self> {
        orders::startup(&creds).await?;
        let collateral = super::auth::get_collateral_balance(&creds).await?;
        tracing::info!(collateral, armed, "LIVE démarré — collatéral USDC réel");
        let (cond_tx, fill_rx) = user_ws::spawn(creds.clone());
        tokio::spawn(orders::run_heartbeats(creds.clone()));
        Ok(Self {
            creds,
            armed,
            cond_tx,
            fill_rx,
            up_token: String::new(),
            dn_token: String::new(),
            condition_id: String::new(),
            order_side: HashMap::new(),
            last_poll_ms: 0,
        })
    }

    /// Rollover de fenêtre : annule tout sur l'ancien marché, bascule le WS user.
    pub async fn on_new_market(&mut self, condition_id: &str, up_token: &str, dn_token: &str) {
        if !self.condition_id.is_empty() && self.armed {
            if let Err(e) = orders::cancel_market_orders(&self.creds, &self.condition_id).await {
                tracing::warn!(error = %e, "cancel-market-orders au rollover");
            }
        }
        self.condition_id = condition_id.to_string();
        self.up_token = up_token.to_string();
        self.dn_token = dn_token.to_string();
        self.order_side.clear();
        let _ = self.cond_tx.send(Some(condition_id.to_string()));
    }

    /// Pose un bid GTC. Renvoie l'ordre restant (ou None en dry-run/échec).
    /// `price` doit déjà être clampé sous l'ask par l'appelant (anti-cross).
    pub async fn place_bid(
        &mut self,
        is_up: bool,
        price: f64,
        size: f64,
    ) -> Option<RestingOrder> {
        let token = if is_up { &self.up_token } else { &self.dn_token };
        let args = OrderArgs { price, size, is_sell: false, gtc: true };
        match orders::place_order(self.armed, &self.creds, token, args).await {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, .. }) => {
                self.order_side.insert(order_id.clone(), (is_up, true));
                // Fill immédiat possible (GTC marketable malgré le clamp) : compté tout de suite.
                let matched = filled_size.unwrap_or(0.0);
                if matched > 0.0 {
                    tracing::info!(matched, "GTC partiellement fillé au POST");
                }
                let _ = avg_price;
                Some(RestingOrder { order_id, price, size, matched: 0.0 })
            }
            Ok(PlaceResult::DryRun) => None,
            Err(e) => {
                tracing::warn!(error = %e, is_up, price, size, "place_bid refusé");
                None
            }
        }
    }

    /// FAK d'assurance (complétion taker fin de fenêtre).
    pub async fn place_insurance_fak(&mut self, is_up: bool, price: f64, size: f64) {
        let token = if is_up { &self.up_token } else { &self.dn_token };
        let args = OrderArgs { price, size, is_sell: false, gtc: false };
        match orders::place_order(self.armed, &self.creds, token, args).await {
            Ok(PlaceResult::Placed { order_id, .. }) => {
                self.order_side.insert(order_id, (is_up, false));
            }
            Ok(PlaceResult::DryRun) => {}
            Err(e) => tracing::warn!(error = %e, "assurance FAK refusée"),
        }
    }

    pub async fn cancel(&self, order_id: &str) {
        if self.armed {
            let _ = orders::cancel_order(&self.creds, order_id).await;
        }
    }

    pub async fn cancel_all(&self) {
        if self.armed && !self.condition_id.is_empty() {
            let _ = orders::cancel_market_orders(&self.creds, &self.condition_id).await;
        }
    }

    /// Draine les fills du WS user (voie rapide). Les ordres inconnus (autre
    /// process, vieux marché) sont ignorés avec un log.
    pub fn drain_ws_fills(&mut self) -> Vec<LiveFill> {
        let mut out = Vec::new();
        while let Ok(f) = self.fill_rx.try_recv() {
            let side = self.order_side.get(&f.order_id).copied().or_else(|| {
                // Attribution de secours par asset_id (ordre posé avant un restart).
                if f.asset_id == self.up_token { Some((true, true)) }
                else if f.asset_id == self.dn_token { Some((false, true)) }
                else { None }
            });
            match side {
                Some((is_up, maker)) if !f.is_sell => {
                    out.push(LiveFill { is_up, price: f.price, size: f.size, maker })
                }
                Some(_) => tracing::warn!(order_id = %f.order_id, "fill SELL inattendu (ignoré)"),
                None => tracing::debug!(order_id = %f.order_id, "fill d'un ordre inconnu (ignoré)"),
            }
        }
        out
    }

    /// Réconciliation par poll (autorité) — à appeler ~1×/3 s. Compare
    /// `size_matched` du CLOB au cumul déjà comptabilisé sur nos ordres
    /// restants et synthétise les fills manqués par le WS. Met aussi à jour
    /// `matched`/présence des ordres (absent = fillé en entier ou annulé).
    pub async fn reconcile(
        &mut self,
        rest_up: &mut Option<RestingOrder>,
        rest_dn: &mut Option<RestingOrder>,
        now_ms: i64,
    ) -> Vec<LiveFill> {
        let mut out = Vec::new();
        if !self.armed || self.condition_id.is_empty() || now_ms - self.last_poll_ms < 3_000 {
            return out;
        }
        self.last_poll_ms = now_ms;
        let open = match orders::open_orders(&self.creds, &self.condition_id).await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "poll /data/orders échoué");
                return out;
            }
        };
        for (rest, is_up) in [(rest_up, true), (rest_dn, false)] {
            let Some(r) = rest.as_mut() else { continue };
            match open.iter().find(|o| o.id == r.order_id) {
                Some(o) => {
                    let matched = o.matched();
                    if matched > r.matched + 1e-9 {
                        let delta = matched - r.matched;
                        tracing::info!(order_id = %r.order_id, delta, "fill rattrapé par le poll");
                        out.push(LiveFill { is_up, price: r.price, size: delta, maker: true });
                        r.matched = matched;
                    }
                }
                None => {
                    // Plus au carnet : fillé en entier, ou annulé. On compte le
                    // reliquat comme fillé UNIQUEMENT si le WS ne l'a pas déjà
                    // fait — impossible à distinguer ici sans /data/order/{id} ;
                    // on interroge l'ordre individuellement pour trancher.
                    match super::auth::l2_request(
                        &self.creds, "GET", &format!("/data/order/{}", r.order_id), None, "",
                    ).await {
                        Ok(text) => {
                            let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                            let matched: f64 = v.get("size_matched")
                                .and_then(|s| s.as_str()).and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            if matched > r.matched + 1e-9 {
                                let delta = matched - r.matched;
                                tracing::info!(order_id = %r.order_id, delta, "fill final rattrapé (ordre clos)");
                                out.push(LiveFill { is_up, price: r.price, size: delta, maker: true });
                            }
                        }
                        Err(e) => tracing::warn!(error = %e, "GET /data/order échoué"),
                    }
                    *rest = None; // l'ordre n'existe plus → re-quote au tick suivant
                }
            }
        }
        out
    }

    /// Marque un fill WS comme comptabilisé sur l'ordre restant correspondant
    /// (évite le double comptage WS + poll).
    pub fn note_ws_fill(
        rest_up: &mut Option<RestingOrder>,
        rest_dn: &mut Option<RestingOrder>,
        f: &LiveFill,
    ) {
        let rest = if f.is_up { rest_up } else { rest_dn };
        let done = match rest.as_mut() {
            Some(r) => {
                r.matched += f.size;
                r.matched >= r.size - 1e-6
            }
            None => false,
        };
        if done {
            *rest = None; // entièrement fillé → re-quote possible
        }
    }
}
