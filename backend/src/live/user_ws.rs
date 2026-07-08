//! WS user Polymarket — fills confirmés en temps réel (porté du legacy
//! `pm_user_ws.rs` avec deux corrections critiques) :
//!   1. **maker_orders[]** parsé : quand NOTRE ordre restant est fillé, notre
//!      order_id est dans le tableau `maker_orders` de l'event trade, pas dans
//!      `taker_order_id` (bug historique « fills maker orphelins » — mémoire).
//!   2. **mpsc non-lossy** au lieu d'un watch : deux fills rapprochés ne
//!      s'écrasent plus.
//! Le poll REST /data/orders reste l'AUTORITÉ (voir orders.rs) — ce WS est la
//! voie rapide.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::auth::LiveCredentials;

const PM_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const PING_INTERVAL_S: u64 = 10;
const MAX_BACKOFF_S: u64 = 30;

/// Fill confirmé (taker OU maker) publié vers l'executor.
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub order_id: String,
    pub asset_id: String, // token concerné ("" si non fourni)
    pub size: f64,
    pub price: f64,
    pub is_sell: bool,
}

/// Event `order` du canal user : état TEMPS RÉEL d'un de nos ordres
/// (PLACEMENT/UPDATE/CANCELLATION) avec son `size_matched` ABSOLU.
/// C'est la surveillance continue demandée après l'incident du 8 juil.
/// (fill non détecté → 3 GTC posés quasi en même temps).
#[derive(Debug, Clone)]
pub struct OrderUpdate {
    pub order_id: String,
    pub price: f64,
    pub size: f64,         // taille originale (0 si absente)
    pub size_matched: f64, // cumul ABSOLU fillé
    pub kind: String,      // PLACEMENT | UPDATE | CANCELLATION
}

#[derive(Debug, Clone)]
pub enum UserMsg {
    Fill(FillEvent),
    Order(OrderUpdate),
}

/// Lance la task WS user au boot. L'executor envoie le `condition_id` du marché
/// courant dans le watch (rollover → resouscription in-session) et draine les
/// fills depuis le mpsc.
pub fn spawn(
    creds: LiveCredentials,
) -> (watch::Sender<Option<String>>, mpsc::UnboundedReceiver<UserMsg>) {
    let (cond_tx, mut cond_rx) = watch::channel(None::<String>);
    let (fill_tx, fill_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            let condition_id = loop {
                let maybe = cond_rx.borrow().clone();
                if let Some(id) = maybe { break id; }
                if cond_rx.changed().await.is_err() { return; }
            };
            match run_session(&creds, &condition_id, &mut cond_rx, &fill_tx).await {
                Ok(()) => {
                    // Fin PROPRE (rollover/fermeture serveur) : reconnexion
                    // immédiate — le backoff est réservé aux vraies erreurs.
                    backoff = 1;
                    tracing::info!("user_ws: session terminée, reconnexion immédiate");
                }
                Err(e) => {
                    tracing::warn!(error = %e, backoff, "user_ws: erreur, reconnexion");
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF_S);
                }
            }
        }
    });
    (cond_tx, fill_rx)
}

async fn run_session(
    creds: &LiveCredentials,
    initial_condition_id: &str,
    cond_rx: &mut watch::Receiver<Option<String>>,
    fill_tx: &mpsc::UnboundedSender<UserMsg>,
) -> anyhow::Result<()> {
    let (ws, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(PM_USER_WS_URL))
        .await
        .map_err(|_| anyhow::anyhow!("user_ws: timeout connexion"))??;
    tracing::info!("user_ws: connecté");
    let (mut sink, mut stream) = ws.split();
    subscribe(&mut sink, creds, initial_condition_id).await?;

    let mut ping = tokio::time::interval(Duration::from_secs(PING_INTERVAL_S));
    ping.tick().await;
    let mut last_msg = tokio::time::Instant::now();

    loop {
        tokio::select! {
            msg = stream.next() => {
                last_msg = tokio::time::Instant::now();
                match msg {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(txt))) => process_message(&txt, fill_tx),
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => {}
                }
            }
            _ = ping.tick() => {
                // Le canal user est silencieux sans activité : pas de watchdog agressif,
                // mais un ping régulier pour garder le NAT/LB éveillé.
                if last_msg.elapsed() > Duration::from_secs(300) {
                    return Err(anyhow::anyhow!("user_ws silencieux >5 min (zombie présumé)"));
                }
                sink.send(Message::Ping(vec![].into())).await?;
            }
            Ok(()) = cond_rx.changed() => {
                // Le serveur REJETTE une 2e souscription sur la même connexion
                // (« INVALID OPERATION » à chaque rollover — le canal était
                // sourd dès la 2e fenêtre). Une souscription par connexion :
                // on ferme, la boucle parente reconnecte avec le nouveau marché.
                tracing::info!("user_ws: rollover → reconnexion (une souscription par connexion)");
                return Ok(());
            }
        }
    }
}

async fn subscribe(
    sink: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    creds: &LiveCredentials,
    condition_id: &str,
) -> anyhow::Result<()> {
    // Auth officielle du canal user : {apiKey, secret, passphrase} — PAS de HMAC.
    let sub = serde_json::json!({
        "auth": {
            "apiKey": creds.api_key,
            "secret": creds.api_secret,
            "passphrase": creds.passphrase,
        },
        "markets": [condition_id],
        "type": "user",
    });
    sink.send(Message::Text(sub.to_string().into()))
        .await
        .map_err(|e| anyhow::anyhow!("user_ws send: {e}"))
}

fn process_message(txt: &str, fill_tx: &mpsc::UnboundedSender<UserMsg>) {
    // Diagnostic (validation live) : trace brute de TOUT ce que le canal envoie
    // — c'est notre seul moyen de vérifier le schéma réel des events maker.
    if txt.contains("INVALID OPERATION") {
        // La souscription a été REJETÉE : aucun event trade/order n'arrivera —
        // le poll 3 s reste la seule source (il tient, mais on perd le temps réel).
        tracing::error!("user_ws: souscription REJETÉE (INVALID OPERATION) — vérifier apiKey/secret/passphrase et le format markets[]");
        return;
    }
    tracing::info!(raw = %txt.chars().take(400).collect::<String>(), "user_ws event");
    let events = parse_events::<UserEvent>(txt);
    for ev in events {
        // Détection tolérante : `event_type` officiel, sinon `type` legacy.
        let et = ev
            .event_type
            .clone()
            .or_else(|| ev.type_field.clone())
            .unwrap_or_default()
            .to_lowercase();
        let is_order_ev = et == "order"
            || matches!(ev.type_field.as_deref(), Some("PLACEMENT" | "UPDATE" | "CANCELLATION"));
        if et == "trade" {
            process_trade(&ev, fill_tx);
        } else if is_order_ev {
            // État temps réel d'UN de nos ordres, avec size_matched ABSOLU.
            let Some(id) = ev.id.clone() else { continue };
            let f = |v: &Option<String>| v.as_deref().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            let _ = fill_tx.send(UserMsg::Order(OrderUpdate {
                order_id: id,
                price: f(&ev.price),
                size: f(&ev.original_size),
                size_matched: f(&ev.size_matched),
                kind: ev.type_field.clone().unwrap_or(et),
            }));
        }
    }
}

fn process_trade(ev: &UserEvent, fill_tx: &mpsc::UnboundedSender<UserMsg>) {
    let price: f64 = ev.price.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let is_sell = ev.side.as_deref() == Some("SELL");

    // Cas 1 : NOUS sommes le taker (assurance FAK) → taker_order_id.
    if let Some(id) = ev.taker_order_id.clone() {
        let size: f64 = ev.size.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        if size > 0.0 && price > 0.0 {
            let _ = fill_tx.send(UserMsg::Fill(FillEvent {
                order_id: id,
                asset_id: ev.asset_id.clone().unwrap_or_default(),
                size,
                price,
                is_sell,
            }));
        }
    }
    // Cas 2 : NOUS sommes maker → notre ordre est dans maker_orders[].
    // (`side` de l'event = côté du TAKER → notre sens est l'inverse.)
    for m in &ev.maker_orders {
        let size: f64 = m.matched_amount.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let px: f64 = m.price.as_deref().and_then(|s| s.parse().ok()).unwrap_or(price);
        if size > 0.0 && px > 0.0 {
            tracing::info!(order_id = %m.order_id, size, px, "user_ws: fill MAKER");
            let _ = fill_tx.send(UserMsg::Fill(FillEvent {
                order_id: m.order_id.clone(),
                asset_id: m.asset_id.clone().unwrap_or_default(),
                size,
                price: px,
                is_sell: !is_sell,
            }));
        }
    }
}

/// Parse un texte JSON en `Vec<T>` — tableau ou objet unique.
fn parse_events<T: serde::de::DeserializeOwned>(txt: &str) -> Vec<T> {
    if let Ok(v) = serde_json::from_str::<Vec<T>>(txt) { return v; }
    if let Ok(v) = serde_json::from_str::<T>(txt) { return vec![v]; }
    vec![]
}

#[derive(Deserialize)]
struct UserEvent {
    event_type: Option<String>, // "trade" | "order" (schéma officiel)
    #[serde(rename = "type")]
    type_field: Option<String>, // "TRADE" | "PLACEMENT" | "UPDATE" | "CANCELLATION"
    id: Option<String>,         // order_id (events `order`)
    original_size: Option<String>,
    size_matched: Option<String>,
    taker_order_id: Option<String>,
    asset_id: Option<String>,
    size: Option<String>,
    price: Option<String>,
    side: Option<String>, // côté du TAKER
    #[serde(default)]
    maker_orders: Vec<MakerOrderEntry>,
}

#[derive(Deserialize)]
struct MakerOrderEntry {
    order_id: String,
    asset_id: Option<String>,
    matched_amount: Option<String>,
    price: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taker_fill_parsed() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let txt = serde_json::json!([{
            "type": "trade", "taker_order_id": "ord-1", "asset_id": "TOK",
            "size": "10.5", "price": "0.52", "side": "BUY"
        }]).to_string();
        process_message(&txt, &tx);
        let UserMsg::Fill(f) = rx.try_recv().unwrap() else { panic!("Fill attendu") };
        assert_eq!(f.order_id, "ord-1");
        assert!(!f.is_sell);
        assert!((f.size - 10.5).abs() < 1e-9);
    }

    #[test]
    fn order_event_parsed() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let txt = serde_json::json!({
            "event_type": "order", "type": "UPDATE", "id": "gtc-7",
            "price": "0.56", "original_size": "5", "size_matched": "5",
            "asset_id": "TOK", "side": "BUY"
        }).to_string();
        process_message(&txt, &tx);
        let UserMsg::Order(u) = rx.try_recv().unwrap() else { panic!("Order attendu") };
        assert_eq!(u.order_id, "gtc-7");
        assert!((u.size_matched - 5.0).abs() < 1e-9);
        assert_eq!(u.kind, "UPDATE");
    }

    #[test]
    fn maker_fill_parsed_from_maker_orders() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Un taker SELL nous frappe → NOTRE ordre maker est un BUY.
        let txt = serde_json::json!({
            "type": "trade", "taker_order_id": "autre", "side": "SELL",
            "size": "10", "price": "0.48",
            "maker_orders": [
                {"order_id": "notre-gtc", "asset_id": "TOK", "matched_amount": "7", "price": "0.48"}
            ]
        }).to_string();
        process_message(&txt, &tx);
        let UserMsg::Fill(f1) = rx.try_recv().unwrap() else { panic!() }; // taker (pas à nous)
        assert_eq!(f1.order_id, "autre");
        let UserMsg::Fill(f2) = rx.try_recv().unwrap() else { panic!() };
        assert_eq!(f2.order_id, "notre-gtc");
        assert!((f2.size - 7.0).abs() < 1e-9);
        assert!(!f2.is_sell, "taker SELL → notre maker est BUY");
    }

    #[test]
    fn legacy_order_event_now_parsed() {
        // Avant le 8 juil. cet event était IGNORÉ — c'est ce qui a permis à un
        // fill de passer inaperçu (3 GTC quasi simultanés). Il est désormais parsé.
        let (tx, mut rx) = mpsc::unbounded_channel();
        process_message(r#"{"type":"UPDATE","id":"x","size_matched":"3"}"#, &tx);
        let UserMsg::Order(u) = rx.try_recv().unwrap() else { panic!("Order attendu") };
        assert_eq!(u.order_id, "x");
        assert!((u.size_matched - 3.0).abs() < 1e-9);
    }

    #[test]
    fn unrelated_event_ignored() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        process_message(r#"{"event_type":"book","asset_id":"TOK"}"#, &tx);
        assert!(rx.try_recv().is_err());
    }
}
