//! Placement d'ordres **POLY_1271** (sig_type=3 / deposit wallet) via `polymarket_client_sdk_v2`.
//!
//! Ordres V2 + signature ERC-7739 — incompatible avec le chemin EIP-712 V1 alloy.

use std::str::FromStr as _;

use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::{POLYGON};

use super::live_executor::{LiveCredentials, OrderArgs, PlaceResult, CLOB_BASE};
use crate::concurrency::bus::Side as BotSide;

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
    let client = authenticated_client(creds, &signer).await?;
    let token = U256::from_str(token_id).map_err(|e| anyhow::anyhow!("token_id: {e}"))?;

    // Lookup neg-risk + tick size (cache SDK) — requis pour contrat V2 et arrondi prix.
    let neg = client.neg_risk(token).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    tracing::debug!(token_id, neg_risk = neg.neg_risk, "neg-risk résolu");
    let tick = client.tick_size(token).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let price_dp = tick.minimum_tick_size.as_decimal().scale();

    let price = decimal_from_f64(args.price, price_dp, "price")?;
    let size = decimal_from_f64(args.size, 2, "size")?; // lot max 2 décimales (SDK)
    let side = match args.side {
        BotSide::Up | BotSide::Down => Side::Buy,
    };

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

    let signed = client.sign(&signer, signable).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    tracing::warn!(
        order_type = "FAK",
        signature_type = 3,
        signature_len = signed.signature.as_bytes().len(),
        "LIVE order POLY_1271 signé"
    );

    if !live_armed {
        return Ok(PlaceResult::DryRun);
    }

    let resp = client.post_order(signed).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    tracing::warn!(order_id = %resp.order_id, "✅ ordre LIVE POLY_1271 accepté");
    Ok(PlaceResult::Placed(resp.order_id))
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
