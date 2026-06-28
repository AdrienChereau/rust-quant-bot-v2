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

/// Client HTTP partagé — connexions TCP/TLS maintenues entre les appels (Keep-Alive).
/// Sans ça, chaque ordre refait un handshake TCP+TLS (~150-250 ms).
static HTTP: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    reqwest::Client::builder()
        .tcp_keepalive(std::time::Duration::from_secs(15))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .tcp_nodelay(true)          // élimine le buffering Nagle (~0-40ms par ordre)
        .pool_max_idle_per_host(1)  // 1 seul endpoint CLOB
        .build()
        .expect("reqwest client init")
});

/// Signer EIP-712 pré-parsé (chemin sig_type != 3) — évite de décoder la clé hex à chaque ordre.
#[cfg(feature = "live")]
static CACHED_SIGNER: std::sync::OnceLock<alloy::signers::local::PrivateKeySigner> =
    std::sync::OnceLock::new();

/// Clé HMAC pré-décodée (base64 url-safe → bytes) — évite un decode par ordre (~50µs).
static CACHED_HMAC_KEY: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

pub(crate) const CLOB_BASE: &str = "https://clob.polymarket.com";
const ORDER_TYPE_FAK: &str = "FAK"; // Fill-And-Kill — JAMAIS FOK.

// EIP-712 domain Polymarket (Polygon, chainId 137). verifyingContract dépend du type de marché :
// CTF Exchange standard vs. NegRisk CTF Exchange. Le mauvais contrat → signature invalide (rejet).
const EXCHANGE_CTF: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
const EXCHANGE_CTF_NEG: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";
const CHAIN_ID: u64 = 137;

/// Adresse du contrat de vérification EIP-712 selon le type de marché.
fn exchange_for(neg_risk: bool) -> &'static str {
    if neg_risk { EXCHANGE_CTF_NEG } else { EXCHANGE_CTF }
}

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
    pub sig_type: u8,           // POLY_SIG_TYPE : 0=EOA, 1=Magic proxy, 2=Gnosis Safe, 3=deposit wallet POLY_1271
}

impl LiveCredentials {
    /// Charge depuis l'environnement. `None` si un seul champ manque (→ pas de live possible).
    /// `POLY_SIGNER_ADDRESS` = adresse de ton EOA (celle liée à l'API key) ; par défaut = funder
    /// (correct seulement en sig_type 0 où EOA == funder).
    pub fn from_env() -> Option<Self> {
        let get = |k: &str| trim_env(k);
        // Accepte les deux conventions de nommage (Rust bot ET scripts py-clob-client).
        let get2 = |a: &str, b: &str| get(a).or_else(|| get(b));
        let funder = get2("POLY_FUNDER_ADDRESS", "POLY_FUNDER")?;
        let mut private_key = get("POLY_PRIVATE_KEY")?;
        if !private_key.starts_with("0x") && !private_key.starts_with("0X") {
            private_key = format!("0x{private_key}");
        }
        Some(Self {
            api_key: get("POLY_API_KEY")?,
            api_secret: get("POLY_API_SECRET")?,
            passphrase: get2("POLY_PASSPHRASE", "POLY_API_PASSPHRASE")?,
            signer_address: get("POLY_SIGNER_ADDRESS").unwrap_or_else(|| funder.clone()),
            funder,
            private_key,
            sig_type: trim_env("POLY_SIG_TYPE").and_then(|v| v.parse().ok()).unwrap_or(2),
        })
    }

    /// Charge les champs minimum pour dériver des creds L2 (sans POLY_API_*).
    pub fn from_env_for_derive() -> Option<Self> {
        let get = |k: &str| trim_env(k);
        let funder = get("POLY_FUNDER_ADDRESS")?;
        let mut private_key = get("POLY_PRIVATE_KEY")?;
        if !private_key.starts_with("0x") && !private_key.starts_with("0X") {
            private_key = format!("0x{private_key}");
        }
        Some(Self {
            api_key: String::new(),
            api_secret: String::new(),
            passphrase: String::new(),
            signer_address: get("POLY_SIGNER_ADDRESS").unwrap_or_else(|| funder.clone()),
            funder,
            private_key,
            sig_type: trim_env("POLY_SIG_TYPE").and_then(|v| v.parse().ok()).unwrap_or(2),
        })
    }

    /// Vérifie la cohérence des credentials (log WARN si problème — cause fréquente de 401).
    pub fn log_config_check(&self) {
        if matches!(self.sig_type, 1 | 2)
            && self.signer_address.eq_ignore_ascii_case(&self.funder)
        {
            tracing::warn!(
                sig_type = self.sig_type,
                "POLY_SIGNER_ADDRESS == POLY_FUNDER_ADDRESS — avec un proxy, POLY_SIGNER_ADDRESS \
                 doit être l'adresse MetaMask EOA (pas le proxy Polymarket) → 401 probable"
            );
        }
        #[cfg(feature = "live")]
        if let Ok(derived) = self.derived_signer_address() {
            if !derived.eq_ignore_ascii_case(&self.signer_address) {
                tracing::warn!(
                    signer_env = %self.signer_address,
                    signer_key = %derived,
                    "POLY_SIGNER_ADDRESS ne correspond pas à POLY_PRIVATE_KEY → 401 probable"
                );
            }
        }
        tracing::info!(
            sig_type = self.sig_type,
            funder = %self.funder,
            signer = %self.signer_address,
            api_key_prefix = %self.api_key.chars().take(8).collect::<String>(),
            "credentials POLY chargées"
        );
    }

    #[cfg(feature = "live")]
    fn derived_signer_address(&self) -> anyhow::Result<String> {
        use alloy::signers::local::PrivateKeySigner;
        let signer: PrivateKeySigner = self.private_key.parse()
            .map_err(|e| anyhow::anyhow!("clé privée: {e}"))?;
        Ok(format!("{:#x}", signer.address()))
    }
}

fn trim_env(key: &str) -> Option<String> {
    let v = std::env::var(key).ok()?;
    let v = v.trim().trim_matches('"').trim_matches('\'').to_string();
    if v.is_empty() { None } else { Some(v) }
}

/// Paramètres d'un ordre à placer (côté stratégie).
/// `side` désigne le **token** (Up/Down) ; le sens BUY/SELL est porté par `is_sell`.
/// - BUY (entry) : on dépense `price * size` USDC pour recevoir `size` tokens du `side`.
/// - SELL (exit) : on donne `size` tokens du `side` pour recevoir `price * size` USDC.
#[derive(Debug, Clone, Copy)]
pub struct OrderArgs {
    pub side: Side,
    pub price: f64,    // prix limite ∈ (0,1)
    pub size: f64,     // nb de shares
    pub is_sell: bool, // false = BUY (entry), true = SELL (exit)
}

/// Résultat d'une tentative de placement. Pour un fill réel, on renvoie aussi la taille fillée
/// et le prix moyen quand le CLOB les expose (sinon `None` → l'appelant doit fallback).
#[derive(Debug, PartialEq)]
pub enum PlaceResult {
    /// Signé + loggé, rien envoyé (dry-run / pas de credentials).
    DryRun,
    /// Ordre réellement accepté par le CLOB.
    Placed {
        order_id: String,
        filled_size: Option<f64>, // tokens effectivement fillés ; None si CLOB n'a pas exposé
        avg_price: Option<f64>,   // prix moyen d'exécution ; None si CLOB n'a pas exposé
        post_ms: u64,             // durée du POST HTTP (début → réponse CLOB)
    },
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
        let usdc = (args.price * args.size * BASE_UNITS).round() as u128;
        let shares = (args.size * BASE_UNITS).round() as u128;
        // BUY  : on donne USDC, on reçoit des shares  → maker=USDC,  taker=shares, side=0
        // SELL : on donne des shares, on reçoit USDC  → maker=shares, taker=USDC,  side=1
        let (maker_amount, taker_amount, side) = if args.is_sell {
            (shares, usdc, 1u8)
        } else {
            (usdc, shares, 0u8)
        };
        OrderFields {
            salt: rand_salt(),
            maker: creds.funder.clone(),
            // sig_type 0/3 (EOA / deposit wallet) : maker == signer == funder
            // sig_type 1/2 (proxy) : maker = funder (proxy), signer = EOA
            signer: match creds.sig_type {
                0 | 3 => creds.funder.clone(),
                _ => creds.signer_address.clone(),
            },
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: token_id.to_string(),
            maker_amount,
            taker_amount,
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
    neg_risk: bool,
    args: OrderArgs,
) -> anyhow::Result<PlaceResult> {
    let Some(creds) = creds else {
        tracing::warn!(?args, token_id, "LIVE — pas de credentials POLY_*, ordre ignoré");
        return Ok(PlaceResult::DryRun);
    };

    if creds.sig_type == 3 {
        // POLY_1271 : le SDK V2 résout neg-risk en interne (cf. poly1271.rs).
        #[cfg(feature = "live")]
        return crate::polymarket::poly1271::place_order_poly1271(live_armed, creds, token_id, args).await;
        #[cfg(not(feature = "live"))]
        anyhow::bail!("sig_type=3 (POLY_1271) requiert `cargo build --features live`");
    }

    let fields = OrderFields::build(token_id, args, creds);
    let signature = sign_order_eip712(&fields, neg_risk, &creds.private_key)?;
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
    let mut req = HTTP.clone()
        .post(format!("{CLOB_BASE}/order"))
        .header("Content-Type", "application/json");
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let post_t0 = std::time::Instant::now();
    let resp = req.body(body.to_string()).send().await?;
    let post_ms = post_t0.elapsed().as_millis() as u64;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("CLOB /order {status}: {text}");
    }
    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
    let order_id = v.get("orderID").and_then(|x| x.as_str().map(String::from))
        .unwrap_or_else(|| text.clone());
    // CLOB renvoie makingAmount/takingAmount (USDC base units pour le BUY's `taking`).
    // Sur BUY  : takingAmount = shares reçus,  makingAmount = USDC dépensés.
    // Sur SELL : makingAmount = shares donnés, takingAmount = USDC reçus.
    let parse_amount = |k: &str| v.get(k).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok());
    let (filled_size, avg_price) = match (parse_amount("makingAmount"), parse_amount("takingAmount")) {
        (Some(making), Some(taking)) => {
            // Sens déduit du body envoyé (parsing JSON pour récupérer le "side").
            let is_sell = serde_json::from_str::<serde_json::Value>(body).ok()
                .and_then(|b| b.pointer("/order/side").and_then(|s| s.as_u64()).map(|n| n == 1))
                .unwrap_or(false);
            let (shares_base, usdc_base) = if is_sell { (making, taking) } else { (taking, making) };
            let shares = shares_base / BASE_UNITS;
            let usdc = usdc_base / BASE_UNITS;
            if shares > 0.0 { (Some(shares), Some(usdc / shares)) } else { (Some(0.0), None) }
        }
        _ => (None, None),
    };
    tracing::warn!(post_ms, order_id = %order_id, ?filled_size, ?avg_price, "✅ ordre LIVE accepté");
    Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms })
}

/// Lit la **vraie collatéral USDC** du compte via le CLOB (auth L2, `signature_type`).
/// Read-only : sert de pré-flight d'auth (mêmes en-têtes que le POST d'ordre) ET de bankroll live.
/// `GET /balance-allowance?asset_type=COLLATERAL&signature_type=N`.
pub async fn get_collateral_balance(creds: &LiveCredentials) -> anyhow::Result<f64> {
    // Comme py-clob-client : HMAC signe `/balance-allowance` SANS query ; query dans l'URL seulement.
    const SIGN_PATH: &str = "/balance-allowance";
    let query = format!("asset_type=COLLATERAL&signature_type={}", creds.sig_type);
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs().to_string();
    let headers = build_l2_headers(creds, &ts, "GET", SIGN_PATH, "")?;
    let mut req = HTTP.clone().get(format!("{CLOB_BASE}{SIGN_PATH}?{query}"));
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

/// Rafraîchit le cache on-chain CLOB (requis après funding pour deposit wallet sig_type=3).
/// `GET /balance-allowance/update?asset_type=COLLATERAL&signature_type=N`
pub async fn sync_balance_allowance(creds: &LiveCredentials) -> anyhow::Result<()> {
    const SIGN_PATH: &str = "/balance-allowance/update";
    let query = format!("asset_type=COLLATERAL&signature_type={}", creds.sig_type);
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs().to_string();
    let headers = build_l2_headers(creds, &ts, "GET", SIGN_PATH, "")?;
    let mut req = HTTP.clone().get(format!("{CLOB_BASE}{SIGN_PATH}?{query}"));
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("CLOB /balance-allowance/update {status}: {text}");
    }
    tracing::info!(sig_type = creds.sig_type, "cache balance-allowance CLOB synchronisé");
    Ok(())
}

/// Appelé au démarrage mono/executor : vérifie creds + sync cache si deposit wallet.
/// Propage l'échec de la sync (deposit wallet sig_type=3) pour que l'appelant décide d'arrêter
/// plutôt que de trader avec un cache de balance potentiellement périmé.
pub async fn startup_poly(creds: &LiveCredentials) -> anyhow::Result<()> {
    creds.log_config_check();
    // Parse le signer EIP-712 une seule fois (chemin sig_type != 3).
    #[cfg(feature = "live")]
    {
        use alloy::signers::local::PrivateKeySigner;
        match creds.private_key.parse::<PrivateKeySigner>() {
            Ok(s) => { let _ = CACHED_SIGNER.set(s); }
            Err(e) => tracing::warn!(error = %e, "pré-parse signer EIP-712 échoué"),
        }
        // Cache aussi le LocalSigner + client authentifié POLY_1271 si sig_type=3.
        if creds.sig_type == 3 {
            crate::polymarket::poly1271::init_signer(creds)?;
            crate::polymarket::poly1271::init_auth_client(creds).await?;
        }
    }
    // Pré-decode la clé HMAC une seule fois (évite ~50µs base64 decode par ordre).
    if let Ok(key) = base64::engine::general_purpose::URL_SAFE.decode(&creds.api_secret) {
        let _ = CACHED_HMAC_KEY.set(key);
    }
    if creds.sig_type == 3 {
        sync_balance_allowance(creds).await
            .map_err(|e| anyhow::anyhow!("sync balance-allowance échouée (deposit wallet): {e}"))?;
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Signature EIP-712 — réelle sous la feature `live` (alloy), sinon erreur (verrou compilation).
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[cfg(feature = "live")]
fn sign_order_eip712(f: &OrderFields, neg_risk: bool, private_key: &str) -> anyhow::Result<String> {
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
        verifying_contract: parse_addr(exchange_for(neg_risk))?,
    };
    let hash = order.eip712_signing_hash(&domain);

    // Utilise le signer pré-parsé si disponible (startup_poly), sinon parse la clé à la volée.
    let sig = if let Some(cached) = CACHED_SIGNER.get() {
        cached.sign_hash_sync(&hash).map_err(|e| anyhow::anyhow!("signature: {e}"))?
    } else {
        let signer: PrivateKeySigner = private_key.parse().map_err(|e| anyhow::anyhow!("clé privée: {e}"))?;
        signer.sign_hash_sync(&hash).map_err(|e| anyhow::anyhow!("signature: {e}"))?
    };
    Ok(format!("0x{}", hex::encode(sig.as_bytes())))
}

#[cfg(not(feature = "live"))]
fn sign_order_eip712(_f: &OrderFields, _neg_risk: bool, _private_key: &str) -> anyhow::Result<String> {
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
    let decoded;
    let key: &[u8] = if let Some(cached) = CACHED_HMAC_KEY.get() {
        cached
    } else {
        decoded = base64::engine::general_purpose::URL_SAFE
            .decode(secret_b64)
            .map_err(|e| anyhow::anyhow!("secret base64 invalide: {e}"))?;
        &decoded
    };
    let mut mac = HmacSha256::new_from_slice(key).map_err(|e| anyhow::anyhow!("clé HMAC: {e}"))?;
    mac.update(format!("{ts}{method}{path}{body}").as_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE.encode(mac.finalize().into_bytes()))
}

/// Salt aléatoire (CSPRNG) — nonce unique par ordre, sans dépendre de la résolution d'horloge.
fn rand_salt() -> u64 {
    use rand::RngCore as _;
    rand::thread_rng().next_u64()
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
        let f0 = OrderFields::build("1", OrderArgs { side: Side::Up, price: 0.5, size: 1.0, is_sell: false }, &c);
        assert_eq!(f0.maker, c.funder);
        assert_eq!(f0.signer, c.funder); // EOA : signer == funder

        c.sig_type = 2;
        let f2 = OrderFields::build("1", OrderArgs { side: Side::Up, price: 0.5, size: 1.0, is_sell: false }, &c);
        assert_eq!(f2.maker, c.funder);
        assert_eq!(f2.signer, c.signer_address); // proxy : signer = EOA

        c.sig_type = 3;
        c.funder = "0x00000000000000000000000000000000000000d0".into();
        c.signer_address = "0x00000000000000000000000000000000000000e0".into();
        let f3 = OrderFields::build("1", OrderArgs { side: Side::Up, price: 0.5, size: 1.0, is_sell: false }, &c);
        assert_eq!(f3.maker, c.funder);
        assert_eq!(f3.signer, c.funder); // deposit wallet : maker == signer == funder
    }

    #[test]
    fn order_type_is_fak_and_amounts_base_units() {
        let f = OrderFields::build("12345", OrderArgs { side: Side::Up, price: 0.50, size: 10.0, is_sell: false }, &creds());
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
    fn l2_balance_signs_path_without_query() {
        let c = creds();
        let sig = l2_signature(&c.api_secret, "1700000000", "GET", "/balance-allowance", "").unwrap();
        assert_ne!(
            sig,
            l2_signature(&c.api_secret, "1700000000", "GET", "/balance-allowance?asset_type=COLLATERAL&signature_type=2", "").unwrap()
        );
    }

    #[cfg(not(feature = "live"))]
    #[test]
    fn sign_refuses_without_feature() {
        let f = OrderFields::build("1", OrderArgs { side: Side::Up, price: 0.5, size: 5.0, is_sell: false }, &creds());
        assert!(sign_order_eip712(&f, false, &creds().private_key).is_err());
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
        let f = OrderFields::build("12345", OrderArgs { side: Side::Up, price: 0.5, size: 5.0, is_sell: false }, &c);
        let sig_hex = sign_order_eip712(&f, false, &c.private_key).unwrap();
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
