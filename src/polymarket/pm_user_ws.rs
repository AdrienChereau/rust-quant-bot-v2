//! WebSocket Polymarket user — confirmation des fills (Phase 4).
//!
//! Endpoint : `wss://ws-subscriptions-clob.polymarket.com/ws/user`
//! Auth L2 : même HMAC que les en-têtes REST — envoyé dans le message de souscription.
//! Subscribe : `{"markets": ["<condition_id>"], "type": "user", ...auth headers...}`
//!
//! Events reçus :
//! - `order`  : changement de statut d'un ordre (MATCHED → FILLED)
//! - `trade`  : fill partiel ou total confirmé
//!
//! Publie les fills confirmés dans un `watch::Sender<Option<FillEvent>>` que le callback
//! `on_sell_result` dans executor.rs peut lire sans attendre.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::live_executor::{build_l2_headers, LiveCredentials};

const PM_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const PING_INTERVAL_S: u64 = 10;
const MAX_BACKOFF_S: u64 = 60;

/// Événement de fill confirmé par le WS user.
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub order_id: String,
    pub filled_size: f64,
    pub avg_price: f64,
    pub is_taker: bool,
}

/// Lance la tâche WS user en arrière-plan.
/// Retourne un receiver que l'appelant peut surveiller à chaque tick.
pub fn spawn_user_ws(
    creds: LiveCredentials,
    condition_id: String,
) -> watch::Receiver<Option<FillEvent>> {
    let (tx, rx) = watch::channel(None::<FillEvent>);
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            match run_ws_session(&creds, &condition_id, &tx).await {
                Ok(()) => tracing::info!("pm_user_ws: session terminée, reconnexion dans {backoff}s"),
                Err(e) => tracing::warn!(error = %e, backoff, "pm_user_ws: erreur, reconnexion dans {backoff}s"),
            }
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_S);
        }
    });
    rx
}

async fn run_ws_session(
    creds: &LiveCredentials,
    condition_id: &str,
    tx: &watch::Sender<Option<FillEvent>>,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(PM_USER_WS_URL).await?;
    let (mut sink, mut stream) = ws.split();

    // Auth L2 HMAC — timestamp courant, méthode GET, path /ws/user.
    let ts = chrono::Utc::now().timestamp().to_string();
    let headers = build_l2_headers(creds, &ts, "GET", "/ws/user", "")?;

    let mut auth_map = serde_json::Map::new();
    for (k, v) in &headers {
        auth_map.insert(k.clone(), serde_json::Value::String(v.clone()));
    }

    let sub = serde_json::json!({
        "markets": [condition_id],
        "type": "user",
        "auth": auth_map,
    });
    sink.send(Message::Text(sub.to_string())).await?;
    tracing::info!(condition_id, "pm_user_ws: souscription user envoyée");

    let mut ping_interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_S));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(txt))) => {
                        process_message(&txt, tx);
                    }
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => {}
                }
            }
            _ = ping_interval.tick() => {
                sink.send(Message::Ping(vec![])).await?;
            }
        }
    }
}

fn process_message(txt: &str, tx: &watch::Sender<Option<FillEvent>>) {
    let events: Vec<UserEvent> = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => return,
    };
    for ev in events {
        // On traite les `trade` events — fills confirmés par le matching engine.
        if ev.event_type.as_deref() == Some("trade") {
            if let (Some(order_id), Some(size_s), Some(price_s)) =
                (&ev.order_id, &ev.size, &ev.price)
            {
                let filled_size: f64 = size_s.parse().unwrap_or(0.0);
                let avg_price: f64 = price_s.parse().unwrap_or(0.0);
                if filled_size > 0.0 && avg_price > 0.0 {
                    tracing::info!(order_id = %order_id, filled_size, avg_price, "pm_user_ws: fill confirmé");
                    let _ = tx.send(Some(FillEvent {
                        order_id: order_id.clone(),
                        filled_size,
                        avg_price,
                        is_taker: ev.taker_order_id.as_deref() == Some(order_id),
                    }));
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct UserEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    order_id: Option<String>,
    taker_order_id: Option<String>,
    size: Option<String>,
    price: Option<String>,
}
