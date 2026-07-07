//! Ordres LIVE via SDK `polymarket_client_sdk_v2` (POLY_1271 / sig_type 3) —
//! porté du legacy `poly1271.rs` et ÉTENDU pour le maker :
//!   • `order_type` paramétrable : GTC (bids restants) / FAK (assurance taker)
//!   • `cancel_order` (DELETE /order), `cancel_market_orders` (rollover/KILL)
//!   • `open_orders` (GET /data/orders — réconciliation, autorité sur le WS)
//!   • `run_heartbeats` (POST /v1/heartbeats, dead-man switch : process mort →
//!     le CLOB annule nos ordres tout seul en ~15 s — doc Polymarket)

use std::str::FromStr as _;

use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer};
use polymarket_client_sdk_v2::clob::types::{OrderSignature, OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::POLYGON;
use serde::Deserialize;

use super::auth::{l2_request, LiveCredentials, CLOB_BASE};

/// Paramètres d'un ordre (le token_id sélectionne Up/Down côté appelant).
#[derive(Debug, Clone, Copy)]
pub struct OrderArgs {
    pub price: f64,    // prix limite ∈ (0,1), arrondi au tick par le SDK
    pub size: f64,     // parts (arrondi 2 décimales — LOT_SIZE_SCALE)
    pub is_sell: bool, // false = BUY
    pub gtc: bool,     // true = GTC (bid restant) · false = FAK (taker)
}

#[derive(Debug, PartialEq)]
pub enum PlaceResult {
    /// Signé + loggé, rien envoyé (LIVE_ARMED=false).
    DryRun,
    Placed {
        order_id: String,
        filled_size: Option<f64>, // rempli immédiatement (FAK, ou GTC marketable)
        avg_price: Option<f64>,
        post_ms: u64,
    },
}

// ─── Caches (init au boot par `startup`) ───
static CACHED_LOCAL_SIGNER: std::sync::OnceLock<
    LocalSigner<alloy::signers::k256::ecdsa::SigningKey>,
> = std::sync::OnceLock::new();
static CACHED_AUTH_CLIENT: std::sync::OnceLock<
    tokio::sync::Mutex<Client<Authenticated<Normal>>>,
> = std::sync::OnceLock::new();

/// Métadonnées RAW Polymarket par token (tick exact, neg_risk) — préchargées au
/// rollover : les arrondis se font sur LES décimales du marché, pas une constante.
static TOKEN_META: std::sync::Mutex<Option<std::collections::HashMap<String, u32>>> =
    std::sync::Mutex::new(None);

/// Précharge le tick size réel de chaque token (appelé à chaque rollover).
pub async fn preload_token_meta(token_ids: &[&str]) -> anyhow::Result<()> {
    let lock = CACHED_AUTH_CLIENT
        .get()
        .ok_or_else(|| anyhow::anyhow!("client live non initialisé"))?;
    let client = lock.lock().await;
    let mut map = std::collections::HashMap::new();
    for &tid in token_ids {
        let token = U256::from_str(tid).map_err(|e| anyhow::anyhow!("token_id: {e}"))?;
        let tick = client.tick_size(token).await.map_err(|e| anyhow::anyhow!("tick_size: {e}"))?;
        let dp = tick.minimum_tick_size.as_decimal().scale();
        map.insert(tid.to_string(), dp);
    }
    tracing::info!(?map, "tick sizes RAW Polymarket préchargés");
    *TOKEN_META.lock().unwrap() = Some(map);
    Ok(())
}

fn price_dp_for(token_id: &str) -> u32 {
    TOKEN_META
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(token_id).copied())
        .unwrap_or(2) // btc-updown-5m : tick 0.01
}

/// Démarrage live : parse le signer, authentifie le client SDK (une fois),
/// sync le cache balance-allowance (obligatoire en sig_type 3).
pub async fn startup(creds: &LiveCredentials) -> anyhow::Result<()> {
    creds.log_config_check();
    if CACHED_LOCAL_SIGNER.get().is_none() {
        let s = LocalSigner::from_str(&creds.private_key)
            .map_err(|e| anyhow::anyhow!("POLY_PRIVATE_KEY: {e}"))?
            .with_chain_id(Some(POLYGON));
        let _ = CACHED_LOCAL_SIGNER.set(s);
    }
    if CACHED_AUTH_CLIENT.get().is_none() {
        let signer = local_signer(creds)?;
        let client = authenticated_client(creds, &signer).await?;
        let _ = CACHED_AUTH_CLIENT.set(tokio::sync::Mutex::new(client));
        tracing::info!("client POLY_1271 authentifié et mis en cache");
    }
    super::auth::sync_balance_allowance(creds, "COLLATERAL", None)
        .await
        .map_err(|e| anyhow::anyhow!("sync balance-allowance (deposit wallet): {e}"))?;
    Ok(())
}

/// Place un ordre limit (GTC ou FAK). `live_armed=false` → signé, loggé, PAS posté.
pub async fn place_order(
    live_armed: bool,
    creds: &LiveCredentials,
    token_id: &str,
    args: OrderArgs,
) -> anyhow::Result<PlaceResult> {
    let signer = local_signer(creds)?;
    let token = U256::from_str(token_id).map_err(|e| anyhow::anyhow!("token_id: {e}"))?;
    let price = decimal_from_f64(args.price, price_dp_for(token_id), "price")?; // décimales RAW du tick PM
    let size = decimal_from_f64(args.size, 2, "size")?; // lot max 2 décimales (SDK)
    let side = if args.is_sell { Side::Sell } else { Side::Buy };
    let order_type = if args.gtc { OrderType::GTC } else { OrderType::FAK };
    let order_type_log = if args.gtc { "GTC" } else { "FAK" };

    let lock = CACHED_AUTH_CLIENT
        .get()
        .ok_or_else(|| anyhow::anyhow!("client live non initialisé (startup non appelé)"))?;
    let client = lock.lock().await;

    let signable = client
        .limit_order()
        .token_id(token)
        .side(side)
        .price(price)
        .size(size)
        .order_type(order_type)
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("build order: {e}"))?;
    let signed = client
        .sign(&signer, signable)
        .await
        .map_err(|e| anyhow::anyhow!("sign: {e}"))?;
    if !matches!(&signed.signature, OrderSignature::Wrapped(_)) {
        anyhow::bail!("signature POLY_1271 inattendue (attendu ERC-7739 Wrapped) — vérifier le SDK");
    }
    tracing::info!(
        token = %token_id.chars().take(10).collect::<String>(),
        order_type = order_type_log,
        price = %price,  // le Decimal réellement envoyé (arrondi tick + normalisé)
        size = %size,
        is_sell = args.is_sell,
        "ordre LIVE signé"
    );
    if !live_armed {
        return Ok(PlaceResult::DryRun);
    }

    let t0 = std::time::Instant::now();
    let resp = client
        .post_order(signed)
        .await
        .map_err(|e| anyhow::anyhow!("POST /order: {e}"))?;
    let post_ms = t0.elapsed().as_millis() as u64;
    let to_f64 = |d: &Decimal| f64::from_str(&d.to_string()).ok();
    let making = to_f64(&resp.making_amount);
    let taking = to_f64(&resp.taking_amount);
    // BUY : making = USDC dépensés, taking = shares reçus (inverse en SELL).
    let (filled_size, avg_price) = match (making, taking) {
        (Some(m), Some(t)) => {
            let (shares, usdc) = if args.is_sell { (m, t) } else { (t, m) };
            if shares > 0.0 { (Some(shares), Some(usdc / shares)) } else { (Some(0.0), None) }
        }
        _ => (None, None),
    };
    tracing::info!(post_ms, order_id = %resp.order_id, ?filled_size, ?avg_price, "✅ ordre LIVE accepté");
    Ok(PlaceResult::Placed { order_id: resp.order_id, filled_size, avg_price, post_ms })
}

/// Annule un ordre. Idempotent côté appelant : un ordre déjà fillé/annulé revient
/// dans `not_canceled` — on loggue et on continue (la réconciliation tranche).
pub async fn cancel_order(creds: &LiveCredentials, order_id: &str) -> anyhow::Result<bool> {
    let body = serde_json::json!({ "orderID": order_id }).to_string();
    let text = l2_request(creds, "DELETE", "/order", None, &body).await?;
    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
    let ok = v
        .get("canceled")
        .and_then(|c| c.as_array())
        .map(|a| a.iter().any(|x| x.as_str() == Some(order_id)))
        .unwrap_or(false);
    if !ok {
        tracing::debug!(order_id, resp = %text, "cancel non appliqué (déjà fillé/annulé ?)");
    }
    Ok(ok)
}

/// Annule tous nos ordres d'un marché (rollover, KILL, staleness signal).
pub async fn cancel_market_orders(creds: &LiveCredentials, condition_id: &str) -> anyhow::Result<()> {
    let body = serde_json::json!({ "market": condition_id }).to_string();
    let text = l2_request(creds, "DELETE", "/cancel-market-orders", None, &body).await?;
    tracing::info!(market = %condition_id.chars().take(12).collect::<String>(), resp = %text, "cancel-market-orders");
    Ok(())
}

/// Ordre ouvert tel que renvoyé par GET /data/orders.
#[allow(dead_code)] // champs utiles au debug/réconciliation future
#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrder {
    pub id: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub side: String, // "BUY"/"SELL"
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub original_size: String,
    #[serde(default)]
    pub size_matched: String,
}

impl OpenOrder {
    pub fn matched(&self) -> f64 {
        self.size_matched.parse().unwrap_or(0.0)
    }
}

/// Nos ordres ouverts sur un marché — l'AUTORITÉ de réconciliation (le WS user
/// peut rater des fills ; ce poll tranche : size_matched ↑ = fill, ordre absent
/// = fillé ou annulé).
pub async fn open_orders(creds: &LiveCredentials, condition_id: &str) -> anyhow::Result<Vec<OpenOrder>> {
    let q = format!("market={condition_id}");
    let text = l2_request(creds, "GET", "/data/orders", Some(&q), "").await?;
    serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("orders JSON: {e} — {text}"))
}

/// Boucle heartbeat (dead-man switch, doc Polymarket) : POST /v1/heartbeats
/// toutes les 4 s. Si le process meurt, le CLOB annule nos ordres en ~10-15 s —
/// c'est le garde-fou « ordres orphelins » gratuit. Sur 400 : le serveur renvoie
/// le bon id, on l'adopte. Sur erreur réseau : retry, les ordres survivent tant
/// que le CLOB tolère le trou (marge 5 s).
pub async fn run_heartbeats(creds: LiveCredentials) {
    let mut hb_id = String::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(4));
    loop {
        tick.tick().await;
        let body = serde_json::json!({ "heartbeat_id": hb_id }).to_string();
        match l2_request(&creds, "POST", "/v1/heartbeats", None, &body).await {
            Ok(text) => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(id) = v.get("heartbeat_id").and_then(|x| x.as_str()) {
                        hb_id = id.to_string();
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                // 400 = id périmé : le serveur donne le bon id dans la réponse.
                if let Some(idx) = msg.find("heartbeat_id") {
                    if let Some(id) = msg[idx..].split('"').nth(2) {
                        hb_id = id.to_string();
                        continue;
                    }
                }
                tracing::warn!(error = %msg, "heartbeat CLOB en échec (retry 4 s)");
            }
        }
    }
}

// ─── plomberie SDK ───

async fn authenticated_client<S: Signer>(
    creds: &LiveCredentials,
    signer: &S,
) -> anyhow::Result<Client<Authenticated<Normal>>> {
    let funder: Address = creds.funder.parse().map_err(|e| anyhow::anyhow!("funder: {e}"))?;
    let api_key = creds.api_key.parse().map_err(|e| anyhow::anyhow!("POLY_API_KEY: {e}"))?;
    let sdk_creds = Credentials::new(api_key, creds.api_secret.clone(), creds.passphrase.clone());
    Client::new(CLOB_BASE, Config::default())?
        .authentication_builder(signer)
        .funder(funder)
        .signature_type(SignatureType::Poly1271)
        .credentials(sdk_creds)
        .authenticate()
        .await
        .map_err(|e| anyhow::anyhow!("authenticate: {e}"))
}

fn local_signer(
    creds: &LiveCredentials,
) -> anyhow::Result<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>> {
    if let Some(s) = CACHED_LOCAL_SIGNER.get() {
        return Ok(s.clone());
    }
    Ok(LocalSigner::from_str(&creds.private_key)
        .map_err(|e| anyhow::anyhow!("POLY_PRIVATE_KEY: {e}"))?
        .with_chain_id(Some(POLYGON)))
}

fn decimal_from_f64(v: f64, decimal_places: u32, field: &str) -> anyhow::Result<Decimal> {
    if !v.is_finite() || v <= 0.0 {
        anyhow::bail!("{field} invalide: {v}");
    }
    let d = Decimal::from_f64_retain(v).ok_or_else(|| anyhow::anyhow!("{field} invalide: {v}"))?;
    // normalize() retire les zéros traînants : 0.7300 (scale 4) → 0.73 (scale 2),
    // sinon le SDK rejette « price decimal places > tick size decimal places ».
    Ok(d.round_dp(decimal_places).normalize())
}
