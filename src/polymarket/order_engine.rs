//! Acteur mpsc pour les ordres LIVE — découple la hot loop du réseau.
//!
//! La boucle 50 ms n'attend jamais un POST CLOB : elle envoie un `OrderCmd` dans le canal
//! et récupère le résultat via un `oneshot` au tick suivant. L'acteur exécute les ordres
//! séquentiellement (un à la fois) pour éviter toute course de position.

use tokio::sync::{mpsc, oneshot};

use crate::concurrency::bus::Side;
use crate::polymarket::live_executor::{self, LiveCredentials, OrderArgs, PlaceResult};

/// Commande envoyée à l'acteur OrderEngine.
pub enum OrderCmd {
    Open {
        side: Side,
        token_id: String,
        neg_risk: bool,
        price: f64,
        size: f64,
        tick: f64,
        min_order_size: f64,
        now_ms: u64,
        reply: oneshot::Sender<OrderResult>,
    },
    Close {
        token_id: String,
        side: Side,
        neg_risk: bool,
        price: f64,
        size: f64,
        tick: f64,
        reason: &'static str,
        reply: oneshot::Sender<OrderResult>,
    },
}

/// Résultat d'un ordre exécuté par l'acteur.
#[derive(Debug)]
pub enum OrderResult {
    Placed {
        order_id: String,
        filled_size: Option<f64>,
        avg_price: Option<f64>,
        post_ms: u64,
        is_sell: bool,
        reason: Option<&'static str>,  // non-None si fermeture
    },
    DryRun { is_sell: bool },
    Failed { error: String, is_sell: bool, reason: Option<&'static str> },
}

/// Lance l'acteur en arrière-plan et renvoie le canal de commandes.
pub fn spawn_order_engine(creds: LiveCredentials, live_armed: bool, queue: usize) -> mpsc::Sender<OrderCmd> {
    let (tx, mut rx) = mpsc::channel::<OrderCmd>(queue);
    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            let result = execute_cmd(cmd, &creds, live_armed).await;
            // Le résultat est envoyé via le oneshot inclus dans la commande.
            // Si le receiver est déjà dropped (hot loop abandonnée), on ignore l'erreur.
            match result {
                (r, reply) => { let _ = reply.send(r); }
            }
        }
    });
    tx
}

async fn execute_cmd(
    cmd: OrderCmd,
    creds: &LiveCredentials,
    live_armed: bool,
) -> (OrderResult, oneshot::Sender<OrderResult>) {
    match cmd {
        OrderCmd::Open { side, token_id, neg_risk, price, size, tick, min_order_size, now_ms: _, reply } => {
            let size_final = ensure_notional(size, price, min_order_size);
            let sell_price = round_tick(price.clamp(0.01, 0.99), tick);
            let args = OrderArgs { side, price: sell_price, size: size_final, is_sell: false };
            let r = match live_executor::place_order(live_armed, Some(creds), &token_id, neg_risk, args).await {
                Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms }) =>
                    OrderResult::Placed { order_id, filled_size, avg_price, post_ms, is_sell: false, reason: None },
                Ok(PlaceResult::DryRun) => OrderResult::DryRun { is_sell: false },
                Err(e) => OrderResult::Failed { error: e.to_string(), is_sell: false, reason: None },
            };
            (r, reply)
        }
        OrderCmd::Close { token_id, side, neg_risk, price, size, tick, reason, reply } => {
            let sell_price = round_tick(price.clamp(0.01, 0.99), tick);
            let args = OrderArgs { side, price: sell_price, size, is_sell: true };
            let r = match live_executor::place_order(live_armed, Some(creds), &token_id, neg_risk, args).await {
                Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms }) =>
                    OrderResult::Placed { order_id, filled_size, avg_price, post_ms, is_sell: true, reason: Some(reason) },
                Ok(PlaceResult::DryRun) => OrderResult::DryRun { is_sell: true },
                Err(e) => OrderResult::Failed { error: e.to_string(), is_sell: true, reason: Some(reason) },
            };
            (r, reply)
        }
    }
}

fn ensure_notional(size: f64, price: f64, min_order_size: f64) -> f64 {
    let by_notional = if price > 0.0 { (1.0 / price).ceil() } else { min_order_size };
    size.max(min_order_size).max(by_notional)
}

fn round_tick(p: f64, tick: f64) -> f64 {
    if tick <= 0.0 { return p; }
    ((p / tick).round() * tick).clamp(0.01, 0.99)
}
