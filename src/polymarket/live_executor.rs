//! Exécution **LIVE** Polymarket — FAK + signature EIP-712 (alloy) + auth L2 (HMAC).
//!
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │ ⚠️  SÉCURITÉ — DEUX VERROUS INDÉPENDANTS                                                   │
//! │   • feature `live` (compilation) : sans elle, `sign_order_eip712` renvoie une erreur →    │
//! │     aucune signature, aucun envoi. Build paper/AWS par défaut = pas d'alloy.              │
//! │   • `LIVE_ARMED` (runtime) : sans lui, l'ordre est SIGNÉ + LOGGÉ mais **jamais POSTé**     │
//! │     (= étape « Dry-Run Live »). Avec lui → POST réel (« Micro-Test Live »).               │
//! │ Parité finale = le micro-trade : un ordre accepté par le CLOB. Le code seul ne la prouve  │
//! │ pas — d'où l'adresse de contrat + le type de signature à CONFIRMER (cf. constantes).      │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//!
//! Struct EIP-712 = champs EXACTS du contrat CTF Exchange (≠ struct simplifiée du spec d'origine).

// Certains helpers L2 ne sont câblés au POST qu'une fois la parité validée.
#![allow(dead_code)]

use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;

use crate::concurrency::bus::Side;

const CLOB_BASE: &str = "https://clob.polymarket.com";
const ORDER_TYPE_FAK: &str = "FAK"; // Fill-And-Kill — JAMAIS FOK.

// EIP-712 domain Polymarket (Polygon, chainId 137). ⚠️ verifyingContract = CTF Exchange standard ;
// DIFFÉRENT pour les marchés "neg-risk" (0xC5d563A36AE78145C45a50134d48A1215220f80a) — à confirmer.
const EXCHANGE_CTF: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
const CHAIN_ID: u64 = 137;

type HmacSha256 = Hmac<Sha256>;

const BASE_UNITS: f64 = 1_000_000.0; // 6 décimales (USDC / shares).

/// Credentials L2 Polymarket — générés en amont (flow L1 hors bot), injectés via `.env`.
/// `private_key` n'est JAMAIS loggé.
#[derive(Clone)]
pub struct LiveCredentials {
    pub api_key: String,
    pub api_secret: String, // base64 url-safe
    pub passphrase: String,
    pub funder: String,         // adresse maker (proxy ou EOA) — collatéral de trading
    pub signer_address: String, // adresse EOA — auth L2 (POLY_ADDRESS) et signer EIP-712 si proxy
    pub private_key: String,    // clé EOA qui signe l'EIP-712
    pub sig_type: u8,           // POLY_SIG_TYPE : 0=EOA, 1=Magic proxy, 2=Gnosis Safe (3 non supporté)
}

impl LiveCredentials {
    /// Charge depuis l'environnement. `None` si un seul champ manque (→ pas de live possible).
    /// `POLY_SIGNER_ADDRESS` = adresse de ton EOA (celle liée à l'API key) ; par défaut = funder
    /// (correct seulement en sig_type 0 où EOA == funder).
    pub fn from_env() -> Option<Self> {
        let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let funder = get("POLY_FUNDER_ADDRESS")?;
        let mut private_key = get("POLY_PRIVATE_KEY")?;
        if !private_key.starts_with("0x") && !private_key.starts_with("0X") {
            private_key = format!("0x{private_key}");
        }
        Some(Self {
            api_key: get("POLY_API_KEY")?,
            api_secret: get("POLY_API_SECRET")?,
            passphrase: get("POLY_PASSPHRASE")?,
            signer_address: get("POLY_SIGNER_ADDRESS").unwrap_or_else(|| funder.clone()),
            funder,
            private_key,
            sig_type: std::env::var("POLY_SIG_TYPE").ok().and_then(|v| v.parse().ok()).unwrap_or(2),
        })
    }
}

/// Paramètres d'un ordre à placer (côté stratégie).
#[derive(Debug, Clone, Copy)]
pub struct OrderArgs {
    pub side: Side,
    pub price: f64, // prix limite ∈ (0,1)
    pub size: f64,  // nb de shares
}

/// Résultat d'une tentative de placement.
#[derive(Debug, PartialEq)]
pub enum PlaceResult {
    /// Signé + loggé, rien envoyé (dry-run / pas de credentials).
    DryRun,
    /// Ordre réellement accepté par le CLOB (id renvoyé).
    Placed(String),
}

/// Champs bruts de l'ordre, source unique pour la signature ET le JSON (cohérence garantie).
struct OrderFields {
    salt: u64,
    maker: String,
    signer: String,
    taker: String,
    token_id: String,
    maker_amount: u128,
    taker_amount: u128,
    expiration: u64,
    nonce: u64,
    fee_rate_bps: u64,
    side: u8, // 0 = Buy, 1 = Sell
    signature_type: u8,
}

impl OrderFields {
    fn build(token_id: &str, args: OrderArgs, creds: &LiveCredentials) -> Self {
        // BUY : on paie `price*size` USDC pour recevoir `size` shares.
        let usdc = (args.price * args.size * BASE_UNITS).round() as u128;
        let shares = (args.size * BASE_UNITS).round() as u128;
        let side = match args.side { Side::Up | Side::Down => 0u8 }; // on achète toujours le bon token
        OrderFields {
            salt: rand_salt(),
            maker: creds.funder.clone(),
            // sig_type 0 (EOA) : maker == signer == funder
            // sig_type 1/2 (proxy) : maker = funder (proxy), signer = EOA
            signer: if creds.sig_type == 0 {
                creds.funder.clone()
            } else {
                creds.signer_address.clone()
            },
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: token_id.to_string(),
            maker_amount: usdc,
            taker_amount: shares,
            expiration: 0, // FAK : pas d'expiration
            nonce: 0,
            fee_rate_bps: 0,
            side,
            signature_type: creds.sig_type,
        }
    }
}

/// Représentation JSON de l'ordre (corps du `POST /order`).
#[derive(Debug, Serialize)]
struct OrderJson {
    salt: String,
    maker: String,
    signer: String,
    taker: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    expiration: String,
    nonce: String,
    #[serde(rename = "feeRateBps")]
    fee_rate_bps: String,
    side: u8,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    signature: String,
}

impl OrderJson {
    fn from_fields(f: &OrderFields, signature: String) -> Self {
        OrderJson {
            salt: f.salt.to_string(),
            maker: f.maker.clone(),
            signer: f.signer.clone(),
            taker: f.taker.clone(),
            token_id: f.token_id.clone(),
            maker_amount: f.maker_amount.to_string(),
            taker_amount: f.taker_amount.to_string(),
            expiration: f.expiration.to_string(),
            nonce: f.nonce.to_string(),
            fee_rate_bps: f.fee_rate_bps.to_string(),
            side: f.side,
            signature_type: f.signature_type,
            signature,
        }
    }
}

#[derive(Debug, Serialize)]
struct PlaceRequest {
    order: OrderJson,
    owner: String,
    #[serde(rename = "orderType")]
    order_type: String,
}

/// Place un ordre. Signe (feature `live`), LOGGE la requête JSON (avec `"orderType":"FAK"`), puis :
/// - `live_armed == false` → renvoie `DryRun` (rien envoyé) ;
/// - `live_armed == true`  → POST réel au CLOB.
pub async fn place_order(
    live_armed: bool,
    creds: Option<&LiveCredentials>,
    token_id: &str,
    args: OrderArgs,
) -> anyhow::Result<PlaceResult> {
    let Some(creds) = creds else {
        tracing::warn!(?args, token_id, "LIVE — pas de credentials POLY_*, ordre ignoré");
        return Ok(PlaceResult::DryRun);
    };

    let fields = OrderFields::build(token_id, args, creds);
    let signature = sign_order_eip712(&fields, &creds.private_key)?;
    let req = PlaceRequest {
        order: OrderJson::from_fields(&fields, signature),
        owner: creds.api_key.clone(),
        order_type: ORDER_TYPE_FAK.into(),
    };
    let json = serde_json::to_string(&req)?;
    tracing::warn!(order_type = ORDER_TYPE_FAK, request = %json, "LIVE order signé");

    if !live_armed {
        return Ok(PlaceResult::DryRun); // Dry-Run Live : signé + loggé, NON envoyé.
    }
    post_order(creds, &json).await
}

/// POST réel `/order` avec en-têtes L2 HMAC. Atteint seulement si `live_armed` ET signé.
async fn post_order(creds: &LiveCredentials, body: &str) -> anyhow::Result<PlaceResult> {
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs().to_string();
    let headers = build_l2_headers(creds, &ts, "POST", "/order", body)?;
    let mut req = reqwest::Client::new()
        .post(format!("{CLOB_BASE}/order"))
        .header("Content-Type", "application/json");
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.body(body.to_string()).send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("CLOB /order {status}: {text}");
    }
    let id = serde_json::from_str::<serde_json::Value>(&text).ok()
        .and_then(|v| v.get("orderID").and_then(|x| x.as_str().map(String::from)))
        .unwrap_or_else(|| text.clone());
    tracing::warn!(order_id = %id, "✅ ordre LIVE accepté");
    Ok(PlaceResult::Placed(id))
}

/// Lit la **vraie collatéral USDC** du compte via le CLOB (auth L2, `signature_type`).
/// Read-only : sert de pré-flight d'auth (mêmes en-têtes que le POST d'ordre) ET de bankroll live.
/// `GET /balance-allowance?asset_type=COLLATERAL&signature_type=N`.
pub async fn get_collateral_balance(creds: &LiveCredentials) -> anyhow::Result<f64> {
    // Polymarket L2 : le chemin signé doit inclure la query string complète pour les GET.
    let full_path = format!("/balance-allowance?asset_type=COLLATERAL&signature_type={}", creds.sig_type);
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs().to_string();
    let headers = build_l2_headers(creds, &ts, "GET", &full_path, "")?;
    let mut req = reqwest::Client::new().get(format!("{CLOB_BASE}{full_path}"));
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("CLOB /balance-allowance {status}: {text}");
    }
    // Réponse attendue : { "balance": "<base units>", ... }
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("balance JSON '{text}': {e}"))?;
    let raw = v.get("balance").and_then(|b| b.as_str())
        .ok_or_else(|| anyhow::anyhow!("champ 'balance' absent: {text}"))?;
    let base: f64 = raw.parse().map_err(|e| anyhow::anyhow!("balance '{raw}': {e}"))?;
    Ok(base / BASE_UNITS)
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Signature EIP-712 — réelle sous la feature `live` (alloy), sinon erreur (verrou compilation).
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "live")]
fn sign_order_eip712(f: &OrderFields, private_key: &str) -> anyhow::Result<String> {
    use alloy::primitives::{Address, U256};
    use alloy::signers::SignerSync;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::{eip712_domain, SolStruct};

    alloy::sol! {
        struct Order {
            uint256 salt;
            address maker;
            address signer;
            address taker;
            uint256 tokenId;
            uint256 makerAmount;
            uint256 takerAmount;
            uint256 expiration;
            uint256 nonce;
            uint256 feeRateBps;
            uint8 side;
            uint8 signatureType;
        }
    }

    let parse_addr = |s: &str| s.parse::<Address>().map_err(|e| anyhow::anyhow!("adresse '{s}': {e}"));
    let order = Order {
        salt: U256::from(f.salt),
        maker: parse_addr(&f.maker)?,
        signer: parse_addr(&f.signer)?,
        taker: parse_addr(&f.taker)?,
        tokenId: f.token_id.parse::<U256>().map_err(|e| anyhow::anyhow!("tokenId: {e}"))?,
        makerAmount: U256::from(f.maker_amount),
        takerAmount: U256::from(f.taker_amount),
        expiration: U256::from(f.expiration),
        nonce: U256::from(f.nonce),
        feeRateBps: U256::from(f.fee_rate_bps),
        side: f.side,
        signatureType: f.signature_type,
    };

    let domain = eip712_domain! {
        name: "Polymarket CTF Exchange",
        version: "1",
        chain_id: CHAIN_ID,
        verifying_contract: parse_addr(EXCHANGE_CTF)?,
    };
    let hash = order.eip712_signing_hash(&domain);

    let signer: PrivateKeySigner = private_key.parse().map_err(|e| anyhow::anyhow!("clé privée: {e}"))?;
    let sig = signer.sign_hash_sync(&hash).map_err(|e| anyhow::anyhow!("signature: {e}"))?;
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

#[cfg(not(feature = "live"))]
fn sign_order_eip712(_f: &OrderFields, _private_key: &str) -> anyhow::Result<String> {
    anyhow::bail!("signature EIP-712 non compilée — rebuild avec `--features live`")
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Auth L2 (HMAC-SHA256) pour POST /order.
// ─────────────────────────────────────────────────────────────────────────────────────────────

/// `signature = base64url( HMAC_SHA256( base64url_decode(secret), ts+method+path+body ) )`.
pub fn build_l2_headers(
    creds: &LiveCredentials,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let sig = l2_signature(&creds.api_secret, timestamp, method, path, body)?;
    Ok(vec![
        // POLY_ADDRESS = l'EOA signataire (à qui l'API key est rattachée), PAS le funder.
        ("POLY_ADDRESS".into(), creds.signer_address.clone()),
        ("POLY_SIGNATURE".into(), sig),
        ("POLY_TIMESTAMP".into(), timestamp.to_string()),
        ("POLY_API_KEY".into(), creds.api_key.clone()),
        ("POLY_PASSPHRASE".into(), creds.passphrase.clone()),
    ])
}

fn l2_signature(secret_b64: &str, ts: &str, method: &str, path: &str, body: &str) -> anyhow::Result<String> {
    let key = base64::engine::general_purpose::URL_SAFE
        .decode(secret_b64)
        .map_err(|e| anyhow::anyhow!("secret base64 invalide: {e}"))?;
    let mut mac = HmacSha256::new_from_slice(&key).map_err(|e| anyhow::anyhow!("clé HMAC: {e}"))?;
    mac.update(format!("{ts}{method}{path}{body}").as_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE.encode(mac.finalize().into_bytes()))
}

/// Salt pseudo-aléatoire.
fn rand_salt() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    nanos ^ (nanos.rotate_left(17).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> LiveCredentials {
        LiveCredentials {
            api_key: "test-key".into(),
            api_secret: base64::engine::general_purpose::URL_SAFE.encode([7u8; 32]),
            passphrase: "pass".into(),
            funder: "0x0000000000000000000000000000000000000abc".into(),
            signer_address: "0x0000000000000000000000000000000000000abc".into(),
            // clé de test connue (compte de dev Ethereum jamais utilisé en prod).
            private_key: "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d".into(),
            sig_type: 2,
        }
    }

    #[test]
    fn order_signer_follows_sig_type() {
        let mut c = creds();
        c.sig_type = 0;
        c.funder = "0x00000000000000000000000000000000000000f0".into();
        c.signer_address = "0x00000000000000000000000000000000000000e0".into();
        let f0 = OrderFields::build("1", OrderArgs { side: Side::Up, price: 0.5, size: 1.0 }, &c);
        assert_eq!(f0.maker, c.funder);
        assert_eq!(f0.signer, c.funder); // EOA : signer == funder

        c.sig_type = 2;
        let f2 = OrderFields::build("1", OrderArgs { side: Side::Up, price: 0.5, size: 1.0 }, &c);
        assert_eq!(f2.maker, c.funder);
        assert_eq!(f2.signer, c.signer_address); // proxy : signer = EOA
    }

    #[test]
    fn order_type_is_fak_and_amounts_base_units() {
        let f = OrderFields::build("12345", OrderArgs { side: Side::Up, price: 0.50, size: 10.0 }, &creds());
        let req = PlaceRequest {
            order: OrderJson::from_fields(&f, String::new()),
            owner: "k".into(),
            order_type: ORDER_TYPE_FAK.into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"orderType\":\"FAK\""), "{json}");
        assert!(!json.contains("FOK"));
        assert_eq!(req.order.maker_amount, "5000000"); // 0.50*10*1e6
        assert_eq!(req.order.taker_amount, "10000000");
        assert_eq!(req.order.signature_type, 2);
    }

    #[test]
    fn l2_signature_deterministic_and_b64() {
        let c = creds();
        let a = l2_signature(&c.api_secret, "1700000000", "POST", "/order", "{}").unwrap();
        let b = l2_signature(&c.api_secret, "1700000000", "POST", "/order", "{}").unwrap();
        assert_eq!(a, b);
        assert!(base64::engine::general_purpose::URL_SAFE.decode(&a).is_ok());
        assert_ne!(a, l2_signature(&c.api_secret, "1700000001", "POST", "/order", "{}").unwrap());
    }

    #[test]
    fn l2_get_includes_query_in_path() {
        let c = creds();
        let path = "/balance-allowance?asset_type=COLLATERAL&signature_type=2";
        let sig = l2_signature(&c.api_secret, "1700000000", "GET", path, "").unwrap();
        assert_ne!(
            sig,
            l2_signature(&c.api_secret, "1700000000", "GET", "/balance-allowance", "").unwrap()
        );
    }

    #[cfg(not(feature = "live"))]
    #[test]
    fn sign_refuses_without_feature() {
        let f = OrderFields::build("1", OrderArgs { side: Side::Up, price: 0.5, size: 5.0 }, &creds());
        assert!(sign_order_eip712(&f, &creds().private_key).is_err());
    }

    // Sous `--features live` : la signature doit être un secp256k1 valide récupérable vers le signer
    // (preuve que le pipeline hash→sign→recover est interne-cohérent ; la PARITÉ Polymarket reste
    // à valider par le micro-trade).
    #[cfg(feature = "live")]
    #[test]
    fn sign_roundtrips_to_signer_address() {
        use alloy::primitives::{Address, U256};
        use alloy::signers::local::PrivateKeySigner;
        use alloy::sol_types::{eip712_domain, SolStruct};

        let c = creds();
        let f = OrderFields::build("12345", OrderArgs { side: Side::Up, price: 0.5, size: 5.0 }, &c);
        let sig_hex = sign_order_eip712(&f, &c.private_key).unwrap();
        assert!(sig_hex.starts_with("0x") && sig_hex.len() == 132); // 65 octets

        // Reconstruit le hash et vérifie la récupération d'adresse.
        alloy::sol! {
            struct Order { uint256 salt; address maker; address signer; address taker;
                uint256 tokenId; uint256 makerAmount; uint256 takerAmount; uint256 expiration;
                uint256 nonce; uint256 feeRateBps; uint8 side; uint8 signatureType; }
        }
        let order = Order {
            salt: U256::from(f.salt), maker: f.maker.parse().unwrap(), signer: f.signer.parse().unwrap(),
            taker: f.taker.parse().unwrap(), tokenId: f.token_id.parse().unwrap(),
            makerAmount: U256::from(f.maker_amount), takerAmount: U256::from(f.taker_amount),
            expiration: U256::from(f.expiration), nonce: U256::from(f.nonce),
            feeRateBps: U256::from(f.fee_rate_bps), side: f.side, signatureType: f.signature_type,
        };
        let domain = eip712_domain! { name: "Polymarket CTF Exchange", version: "1",
            chain_id: CHAIN_ID, verifying_contract: EXCHANGE_CTF.parse::<Address>().unwrap(), };
        let hash = order.eip712_signing_hash(&domain);

        let signer: PrivateKeySigner = c.private_key.parse().unwrap();
        let bytes = hex::decode(sig_hex.trim_start_matches("0x")).unwrap();
        let sig = alloy::primitives::Signature::try_from(bytes.as_slice()).unwrap();
        let recovered = sig.recover_address_from_prehash(&hash).unwrap();
        assert_eq!(recovered, signer.address());
    }
}
