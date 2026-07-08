//! Auth Polymarket LIVE — credentials L2 + signature HMAC (porté du legacy
//! `live_executor.rs`, tag `legacy-sniper-v2`, réduit au chemin sig_type 3).
//!
//! ┌───────────────────────────────────────────────────────────────────────────┐
//! │ ⚠️  DEUX VERROUS INDÉPENDANTS                                              │
//! │  • feature `live` (compilation) : ce module n'existe pas sans elle.        │
//! │  • `LIVE_ARMED` (runtime) : sans lui, les ordres sont signés + loggés      │
//! │    mais JAMAIS postés (répétition générale).                               │
//! └───────────────────────────────────────────────────────────────────────────┘

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const CLOB_BASE: &str = "https://clob.polymarket.com";
pub const BASE_UNITS: f64 = 1_000_000.0; // 6 décimales (USDC)

type HmacSha256 = Hmac<Sha256>;

/// Client HTTP partagé — Keep-Alive TCP/TLS entre les appels (sans ça, chaque
/// ordre refait un handshake ~150-250 ms).
pub static HTTP: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    reqwest::Client::builder()
        .tcp_keepalive(std::time::Duration::from_secs(15))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .tcp_nodelay(true)
        .pool_max_idle_per_host(2)
        .build()
        .expect("reqwest client init")
});

/// Clé HMAC pré-décodée (base64 url-safe → bytes) — un decode au boot, pas par ordre.
static CACHED_HMAC_KEY: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

/// Credentials L2 Polymarket — depuis le `.env`. `private_key` n'est JAMAIS loggée.
#[derive(Clone)]
pub struct LiveCredentials {
    pub api_key: String,
    pub api_secret: String, // base64 url-safe
    pub passphrase: String,
    pub funder: String,         // deposit wallet (collatéral)
    pub signer_address: String, // EOA — auth L2 (POLY_ADDRESS)
    pub private_key: String,    // clé EOA (signature des ordres via SDK)
    pub sig_type: u8,           // 3 = POLY_1271 deposit wallet (seul chemin supporté ici)
}

impl LiveCredentials {
    pub fn from_env() -> Option<Self> {
        let get = |k: &str| trim_env(k);
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
            sig_type: trim_env("POLY_SIG_TYPE").and_then(|v| v.parse().ok()).unwrap_or(3),
        })
    }

    pub fn log_config_check(&self) {
        if self.sig_type != 3 {
            tracing::warn!(
                sig_type = self.sig_type,
                "seul sig_type=3 (deposit wallet POLY_1271) est supporté par ce portage"
            );
        }
        tracing::info!(
            sig_type = self.sig_type,
            funder = %self.funder,
            signer = %self.signer_address,
            api_key_prefix = %self.api_key.chars().take(8).collect::<String>(),
            "credentials POLY chargées"
        );
    }
}

fn trim_env(key: &str) -> Option<String> {
    let v = std::env::var(key).ok()?;
    let v = v.trim().trim_matches('"').trim_matches('\'').to_string();
    if v.is_empty() { None } else { Some(v) }
}

/// En-têtes L2 : `signature = base64url(HMAC_SHA256(secret, ts+method+path+body))`.
/// `POLY_ADDRESS` = l'EOA signataire (rattachée à l'API key), PAS le funder.
pub fn build_l2_headers(
    creds: &LiveCredentials,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let sig = l2_signature(&creds.api_secret, timestamp, method, path, body)?;
    Ok(vec![
        ("POLY_ADDRESS".into(), creds.signer_address.clone()),
        ("POLY_SIGNATURE".into(), sig),
        ("POLY_TIMESTAMP".into(), timestamp.to_string()),
        ("POLY_API_KEY".into(), creds.api_key.clone()),
        ("POLY_PASSPHRASE".into(), creds.passphrase.clone()),
    ])
}

fn l2_signature(secret_b64: &str, ts: &str, method: &str, path: &str, body: &str) -> anyhow::Result<String> {
    // get_or_init : le decode n'a lieu qu'UNE fois (l'ancien code testait `get()`
    // sans jamais `set()` → cache mort, decode à chaque requête).
    let key: &[u8] = if let Some(cached) = CACHED_HMAC_KEY.get() {
        cached
    } else {
        let decoded = base64::engine::general_purpose::URL_SAFE
            .decode(secret_b64)
            .map_err(|e| anyhow::anyhow!("secret base64 invalide: {e}"))?;
        CACHED_HMAC_KEY.get_or_init(|| decoded)
    };
    let mut mac = HmacSha256::new_from_slice(key).map_err(|e| anyhow::anyhow!("clé HMAC: {e}"))?;
    mac.update(format!("{ts}{method}{path}{body}").as_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE.encode(mac.finalize().into_bytes()))
}

pub fn now_ts() -> anyhow::Result<String> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .to_string())
}

/// Requête L2 générique (GET/DELETE/POST). Comme py-clob-client : le HMAC signe
/// le PATH SANS query string ; la query n'apparaît que dans l'URL.
pub async fn l2_request(
    creds: &LiveCredentials,
    method: &str,
    sign_path: &str,
    query: Option<&str>,
    body: &str,
) -> anyhow::Result<String> {
    let ts = now_ts()?;
    let headers = build_l2_headers(creds, &ts, method, sign_path, body)?;
    let url = match query {
        Some(q) => format!("{CLOB_BASE}{sign_path}?{q}"),
        None => format!("{CLOB_BASE}{sign_path}"),
    };
    let mut req = match method {
        "GET" => HTTP.get(&url),
        "DELETE" => HTTP.delete(&url),
        "POST" => HTTP.post(&url),
        m => anyhow::bail!("méthode {m} non gérée"),
    };
    if !body.is_empty() {
        req = req.header("Content-Type", "application/json");
    }
    for (k, v) in headers {
        req = req.header(k, v);
    }
    let resp = if body.is_empty() { req.send().await? } else { req.body(body.to_string()).send().await? };
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("CLOB {method} {sign_path} {status}: {text}");
    }
    Ok(text)
}

/// Collatéral USDC réel du compte (pré-flight d'auth + bankroll live).
pub async fn get_collateral_balance(creds: &LiveCredentials) -> anyhow::Result<f64> {
    let q = format!("asset_type=COLLATERAL&signature_type={}", creds.sig_type);
    let text = l2_request(creds, "GET", "/balance-allowance", Some(&q), "").await?;
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("balance JSON '{text}': {e}"))?;
    let raw = v.get("balance").and_then(|b| b.as_str())
        .ok_or_else(|| anyhow::anyhow!("champ 'balance' absent: {text}"))?;
    let base: f64 = raw.parse().map_err(|e| anyhow::anyhow!("balance '{raw}': {e}"))?;
    Ok(base / BASE_UNITS)
}

/// Solde CONDITIONAL RÉEL d'un token (parts on-chain, cache rafraîchi d'abord).
/// C'est la VÉRITÉ des positions — le miroir doit s'y aligner (incident du
/// 7 juil. : miroir équilibré, réalité déséquilibrée → aucune complétion).
pub async fn get_conditional_balance(creds: &LiveCredentials, token_id: &str) -> anyhow::Result<f64> {
    let q = format!("asset_type=CONDITIONAL&token_id={token_id}&signature_type={}", creds.sig_type);
    let _ = l2_request(creds, "GET", "/balance-allowance/update", Some(&q), "").await;
    let text = l2_request(creds, "GET", "/balance-allowance", Some(&q), "").await?;
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| anyhow::anyhow!("balance JSON '{text}': {e}"))?;
    let raw = v.get("balance").and_then(|b| b.as_str())
        .ok_or_else(|| anyhow::anyhow!("champ 'balance' absent: {text}"))?;
    let base: f64 = raw.parse().map_err(|e| anyhow::anyhow!("balance '{raw}': {e}"))?;
    Ok(base / BASE_UNITS)
}

/// Rafraîchit le cache on-chain CLOB. `asset_type` = COLLATERAL (boot, sig_type 3)
/// ou CONDITIONAL + token_id (obligatoire avant SELL/merge après un BUY — leçon mémoire).
pub async fn sync_balance_allowance(
    creds: &LiveCredentials,
    asset_type: &str,
    token_id: Option<&str>,
) -> anyhow::Result<()> {
    let mut q = format!("asset_type={asset_type}&signature_type={}", creds.sig_type);
    if let Some(t) = token_id {
        q.push_str(&format!("&token_id={t}"));
    }
    l2_request(creds, "GET", "/balance-allowance/update", Some(&q), "").await?;
    tracing::info!(asset_type, ?token_id, "cache balance-allowance CLOB synchronisé");
    Ok(())
}
