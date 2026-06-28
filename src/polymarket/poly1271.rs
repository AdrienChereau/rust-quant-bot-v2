//! Placement d'ordres **POLY_1271** (sig_type=3 / deposit wallet) via `polymarket_client_sdk_v2`.
//!
//! Ordres V2 + signature ERC-7739 — incompatible avec le chemin EIP-712 V1 alloy.

use std::str::FromStr as _;

use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::clob::types::{OrderSignature, OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::{POLYGON};

use super::live_executor::{LiveCredentials, OrderArgs, PlaceResult, CLOB_BASE};

// ─── Caches pré-chargés au démarrage (startup_poly) ────────────────────────────────────────────

/// LocalSigner pré-parsé — évite de décoder la clé hex à chaque ordre POLY_1271.
static CACHED_LOCAL_SIGNER: std::sync::OnceLock<
    LocalSigner<alloy::signers::k256::ecdsa::SigningKey>
> = std::sync::OnceLock::new();

/// Client SDK authentifié — évite de refaire `authenticate()` à chaque ordre (~100-300 ms).
/// Le Mutex tokio permet l'accès exclusif en cas d'usage concurrent (exceptionnel).
static CACHED_AUTH_CLIENT: std::sync::OnceLock<
    tokio::sync::Mutex<Client<Authenticated<Normal>>>
> = std::sync::OnceLock::new();

/// Métadonnées par token_id — évite les appels `neg_risk()` + `tick_size()` per-ordre.
static TOKEN_META: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, TokenMeta>>
> = std::sync::OnceLock::new();

#[derive(Clone)]
struct TokenMeta { neg_risk: bool, price_dp: u32 }

/// Appelé par `startup_poly` pour pré-parser le signer une seule fois.
pub fn init_signer(creds: &LiveCredentials) -> anyhow::Result<()> {
    if CACHED_LOCAL_SIGNER.get().is_some() { return Ok(()); }
    let s = LocalSigner::from_str(&creds.private_key)
        .map_err(|e| anyhow::anyhow!("POLY_PRIVATE_KEY: {e}"))?
        .with_chain_id(Some(POLYGON));
    let _ = CACHED_LOCAL_SIGNER.set(s);
    tracing::info!("LocalSigner POLY_1271 pré-parsé");
    Ok(())
}

/// Appelé par `startup_poly` — initialise le client SDK authentifié une seule fois.
/// Gain : supprime `authenticate()` (~100-300 ms) de la hot-path d'ordre.
pub async fn init_auth_client(creds: &LiveCredentials) -> anyhow::Result<()> {
    if CACHED_AUTH_CLIENT.get().is_some() { return Ok(()); }
    let signer = local_signer(creds)?;
    let client = authenticated_client(creds, &signer).await?;
    let _ = CACHED_AUTH_CLIENT.set(tokio::sync::Mutex::new(client));
    // Initialise aussi le cache TOKEN_META vide (peuplé au rollover par preload_token_meta).
    let _ = TOKEN_META.set(std::sync::Mutex::new(std::collections::HashMap::new()));
    tracing::info!("Client POLY_1271 authentifié mis en cache");
    Ok(())
}

/// Pré-charge neg_risk + tick_size pour les token_ids du marché courant.
/// Appelé depuis `pm_poller` à chaque rollover marché — invalide le cache précédent.
pub async fn preload_token_meta(creds: &LiveCredentials, token_ids: &[&str]) -> anyhow::Result<()> {
    let signer = local_signer(creds)?;
    // Utilise le client caché si dispo, sinon en crée un temporaire.
    let meta_map = if let Some(lock) = CACHED_AUTH_CLIENT.get() {
        let client = lock.lock().await;
        fetch_meta_for_tokens(&*client, token_ids).await?
    } else {
        let client = authenticated_client(creds, &signer).await?;
        fetch_meta_for_tokens(&client, token_ids).await?
    };
    if let Some(cache) = TOKEN_META.get() {
        let mut map = cache.lock().unwrap();
        map.clear();
        map.extend(meta_map);
    }
    tracing::info!(tokens = token_ids.len(), "token metadata pré-chargée");
    Ok(())
}

async fn fetch_meta_for_tokens(
    client: &Client<Authenticated<Normal>>,
    token_ids: &[&str],
) -> anyhow::Result<std::collections::HashMap<String, TokenMeta>> {
    let mut map = std::collections::HashMap::new();
    for &token_id in token_ids {
        let token = U256::from_str(token_id).map_err(|e| anyhow::anyhow!("token_id: {e}"))?;
        let (neg, tick) = tokio::join!(client.neg_risk(token), client.tick_size(token));
        let neg = neg.map_err(|e| anyhow::anyhow!("{e}"))?;
        let tick = tick.map_err(|e| anyhow::anyhow!("{e}"))?;
        let price_dp = tick.minimum_tick_size.as_decimal().scale();
        map.insert(token_id.to_string(), TokenMeta { neg_risk: neg.neg_risk, price_dp });
    }
    Ok(map)
}

/// Supprime les métadonnées d'un token du cache (appelé sur `tick_size_change` WS).
pub fn invalidate_token_meta(token_id: &str) {
    if let Some(cache) = TOKEN_META.get() {
        cache.lock().unwrap().remove(token_id);
    }
}

/// Dérive ou crée les credentials L2 CLOB (flow L1) — à lancer en local one-shot.
pub async fn derive_api_creds(creds: &LiveCredentials) -> anyhow::Result<Credentials> {
    let signer = local_signer(creds)?;
    let funder: Address = creds.funder.parse().map_err(|e| anyhow::anyhow!("funder: {e}"))?;
    let client = Client::new(CLOB_BASE, Config::default())?
        .authentication_builder(&signer)
        .funder(funder)
        .signature_type(map_signature_type(creds.sig_type))
        .authenticate()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(client.credentials().clone())
}

/// Place un ordre FAK limit via SDK V2 + POLY_1271.
pub async fn place_order_poly1271(
    live_armed: bool,
    creds: &LiveCredentials,
    token_id: &str,
    args: OrderArgs,
) -> anyhow::Result<PlaceResult> {
    let signer = local_signer(creds)?;
    let token = U256::from_str(token_id).map_err(|e| anyhow::anyhow!("token_id: {e}"))?;

    // Récupère les métadonnées depuis le cache si dispo (peuplé au rollover par preload_token_meta).
    // Sinon fallback réseau (neg_risk + tick_size) — path lent, ne devrait arriver qu'au 1er ordre.
    let (price_dp, neg_risk_val) = if let Some(cache) = TOKEN_META.get() {
        // Cloner AVANT le if/else : le MutexGuard (std) ne doit pas être tenu à travers le `.await`
        // du fallback réseau ci-dessous, sinon le future n'est plus `Send` (→ tokio::spawn refusé).
        let cached_meta = cache.lock().unwrap().get(token_id).cloned();
        if let Some(meta) = cached_meta {
            tracing::debug!(token_id, neg_risk = meta.neg_risk, "métadonnées depuis cache");
            (meta.price_dp, meta.neg_risk)
        } else {
            // Fallback : réseau
            let client_tmp = authenticated_client(creds, &signer).await?;
            let (neg, tick) = tokio::join!(client_tmp.neg_risk(token), client_tmp.tick_size(token));
            let neg = neg.map_err(|e| anyhow::anyhow!("{e}"))?;
            let tick = tick.map_err(|e| anyhow::anyhow!("{e}"))?;
            tracing::warn!(token_id, "cache TOKEN_META miss — fallback réseau neg_risk+tick_size");
            (tick.minimum_tick_size.as_decimal().scale(), neg.neg_risk)
        }
    } else {
        // Cache non initialisé (startup_poly pas encore appelé) — fallback réseau complet.
        let client_tmp = authenticated_client(creds, &signer).await?;
        let (neg, tick) = tokio::join!(client_tmp.neg_risk(token), client_tmp.tick_size(token));
        let neg = neg.map_err(|e| anyhow::anyhow!("{e}"))?;
        let tick = tick.map_err(|e| anyhow::anyhow!("{e}"))?;
        (tick.minimum_tick_size.as_decimal().scale(), neg.neg_risk)
    };
    tracing::debug!(token_id, neg_risk = neg_risk_val, "order metadata prêt");

    let price = decimal_from_f64(args.price, price_dp, "price")?;
    let size = decimal_from_f64(args.size, 2, "size")?; // lot max 2 décimales (SDK)
    // `args.side` (Up/Down) sélectionne le **token** côté appelant ; ici on porte BUY/SELL.
    let side = if args.is_sell { Side::Sell } else { Side::Buy };

    // Utilise le client caché (init_auth_client au démarrage) ou en crée un si absent.
    // Le guard est tenu le temps du sign+post (ordre unique en cours de toute façon).
    if let Some(cache_lock) = CACHED_AUTH_CLIENT.get() {
        let client = cache_lock.lock().await;
        place_with_client(&*client, &signer, token, side, price, size, live_armed, args.is_sell).await
    } else {
        // Fallback : client temporaire (startup_poly pas encore appelé).
        tracing::warn!("CACHED_AUTH_CLIENT absent — authenticate() per-ordre (lent)");
        let client_tmp = authenticated_client(creds, &signer).await?;
        place_with_client(&client_tmp, &signer, token, side, price, size, live_armed, args.is_sell).await
    }
}

async fn place_with_client<S: Signer>(
    client: &Client<Authenticated<Normal>>,
    signer: &S,
    token: U256,
    side: Side,
    price: Decimal,
    size: Decimal,
    live_armed: bool,
    is_sell: bool,
) -> anyhow::Result<PlaceResult> {
    let signable = client
        .limit_order()
        .token_id(token)
        .side(side)
        .price(price)
        .size(size)
        .order_type(OrderType::FAK)
        .build()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let signed = client.sign(signer, signable).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let sig_len = match &signed.signature {
        OrderSignature::Wrapped(s) => s.len(),
        OrderSignature::Ecdsa(_) => 65,
        _ => 0,
    };
    if !matches!(&signed.signature, OrderSignature::Wrapped(_)) {
        tracing::error!(
            signature_len = sig_len,
            "POLY_1271 : signature ECDSA — attendu ERC-7739 Wrapped (~600+ car.). \
             Vérifiez polymarket_client_sdk_v2 >= 0.6.0-canary.1"
        );
        anyhow::bail!("signature POLY_1271 invalide ({sig_len} car.) — rebuild avec SDK canary");
    }
    tracing::warn!(
        order_type = "FAK",
        signature_type = 3,
        signature_len = sig_len,
        "LIVE order POLY_1271 signé"
    );

    if !live_armed {
        return Ok(PlaceResult::DryRun);
    }

    let post_t0 = std::time::Instant::now();
    let resp = client.post_order(signed).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let post_ms = post_t0.elapsed().as_millis() as u64;
    // Conversion Decimal → f64 (le SDK renvoie déjà en unités humaines, pas en base units).
    use std::str::FromStr as _;
    let to_f64 = |d: &polymarket_client_sdk_v2::types::Decimal|
        f64::from_str(&d.to_string()).ok();
    let making = to_f64(&resp.making_amount);
    let taking = to_f64(&resp.taking_amount);
    // BUY  : making = USDC dépensés, taking = shares reçus.
    // SELL : making = shares donnés, taking = USDC reçus.
    let (filled_size, avg_price) = match (making, taking) {
        (Some(m), Some(t)) => {
            let (shares, usdc) = if is_sell { (m, t) } else { (t, m) };
            if shares > 0.0 { (Some(shares), Some(usdc / shares)) } else { (Some(0.0), None) }
        }
        _ => (None, None),
    };
    tracing::warn!(post_ms, order_id = %resp.order_id, ?filled_size, ?avg_price,
        "✅ ordre LIVE POLY_1271 accepté");
    Ok(PlaceResult::Placed { order_id: resp.order_id, filled_size, avg_price, post_ms })
}

async fn authenticated_client<S: Signer>(
    creds: &LiveCredentials,
    signer: &S,
) -> anyhow::Result<Client<Authenticated<Normal>>> {
    let funder: Address = creds.funder.parse().map_err(|e| anyhow::anyhow!("funder: {e}"))?;
    let api_key = creds.api_key.parse().map_err(|e| anyhow::anyhow!("POLY_API_KEY: {e}"))?;
    let sdk_creds = Credentials::new(
        api_key,
        creds.api_secret.clone(),
        creds.passphrase.clone(),
    );
    Client::new(CLOB_BASE, Config::default())?
        .authentication_builder(signer)
        .funder(funder)
        .signature_type(SignatureType::Poly1271)
        .credentials(sdk_creds)
        .authenticate()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

fn local_signer(creds: &LiveCredentials) -> anyhow::Result<LocalSigner<alloy::signers::k256::ecdsa::SigningKey>> {
    // Retourne le signer pré-parsé si dispo (init_signer appelé au démarrage), sinon parse.
    if let Some(s) = CACHED_LOCAL_SIGNER.get() {
        return Ok(s.clone());
    }
    Ok(LocalSigner::from_str(&creds.private_key)
        .map_err(|e| anyhow::anyhow!("POLY_PRIVATE_KEY: {e}"))?
        .with_chain_id(Some(POLYGON)))
}

fn map_signature_type(sig_type: u8) -> SignatureType {
    match sig_type {
        0 => SignatureType::Eoa,
        1 => SignatureType::Proxy,
        2 => SignatureType::GnosisSafe,
        3 => SignatureType::Poly1271,
        _ => SignatureType::GnosisSafe,
    }
}

fn decimal_from_f64(v: f64, decimal_places: u32, field: &str) -> anyhow::Result<Decimal> {
    if !v.is_finite() || v <= 0.0 {
        anyhow::bail!("{field} invalide: {v}");
    }
    // Évite les artefacts f64 (0.01 → 28 décimales) — le SDK rejette vs tick size.
    let d = Decimal::from_f64_retain(v)
        .ok_or_else(|| anyhow::anyhow!("{field} invalide: {v}"))?;
    Ok(d.round_dp(decimal_places))
}
