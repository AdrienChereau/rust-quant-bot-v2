//! Connecteur Polymarket (J6) — lecture.
//!
//! - Gamma REST : résout le marché "BTC Up/Down 5 min" actif (slug par fenêtre de
//!   300s) et ses tokens Up/Down, prix, échéance et paramètres de reward.
//! - CLOB REST `/book` : snapshot complet du carnet (toute la profondeur), pollé
//!   périodiquement. Donne best bid/ask, mid et la profondeur nécessaire à
//!   l'estimation de `Q_min_total` (rewards, J7).
//!
//! Note : la résolution du marché se fait via **Chainlink BTC/USD** (pas Binance) ;
//! Binance n'est qu'un proxy rapide du fair value. Le strike de référence est le
//! prix Chainlink à l'ouverture de la fenêtre.

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde::Deserialize;

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const CLOB_BASE: &str = "https://clob.polymarket.com";
const WINDOW_SEC: i64 = 300;

/// Marché Up/Down résolu.
#[derive(Debug, Clone)]
#[allow(dead_code)] // métadonnées marché complètes (rewards/neg_risk servent au live)
pub struct Market {
    pub condition_id: String,
    pub slug: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub up_price: f64,
    pub down_price: f64,
    pub end_time: DateTime<Utc>,
    pub window_ts: i64,
    pub rewards_max_spread: f64, // en cents
    pub rewards_min_size: f64,
    pub tick_size: f64,
    pub min_order_size: f64,
    pub neg_risk: bool,
}

impl Market {
    pub fn time_remaining_sec(&self) -> i64 {
        (self.end_time - Utc::now()).num_seconds()
    }
}

/// Un niveau de carnet (prix, taille).
#[derive(Debug, Clone, Copy)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

/// Snapshot complet du carnet CLOB d'un token (toute la profondeur).
#[derive(Debug, Clone, Default)]
pub struct PolyBook {
    pub bids: Vec<Level>, // best bid = prix max
    pub asks: Vec<Level>, // best ask = prix min
}

impl PolyBook {
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.iter().map(|l| l.price).fold(None, |m, p| {
            Some(m.map_or(p, |x: f64| x.max(p)))
        })
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.iter().map(|l| l.price).fold(None, |m, p| {
            Some(m.map_or(p, |x: f64| x.min(p)))
        })
    }
    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }
}

#[derive(Clone)]
pub struct PolymarketClient {
    http: reqwest::Client,
}

impl PolymarketClient {
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("client reqwest");
        Self { http }
    }

    fn current_window() -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        (now / WINDOW_SEC) * WINDOW_SEC
    }

    /// Résout le marché BTC 5 min actif (fenêtre courante puis suivante).
    pub async fn get_current_btc_5m_market(&self) -> anyhow::Result<Option<Market>> {
        let base = Self::current_window();
        for window_ts in [base, base + WINDOW_SEC] {
            let slug = format!("btc-updown-5m-{window_ts}");
            match self.fetch_market(&slug, window_ts).await {
                Ok(Some(m)) if m.time_remaining_sec() > 0 => return Ok(Some(m)),
                Ok(_) => continue,
                Err(e) => tracing::debug!(%slug, error = %e, "résolution marché échouée"),
            }
        }
        Ok(None)
    }

    async fn fetch_market(&self, slug: &str, window_ts: i64) -> anyhow::Result<Option<Market>> {
        let url = format!("{GAMMA_BASE}/events/slug/{slug}");
        let resp = self.http.get(&url).send().await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let event: serde_json::Value = resp.json().await?;
        let Some(m) = event.get("markets").and_then(|v| v.as_array()).and_then(|a| a.first())
        else {
            return Ok(None);
        };

        // Champs encodés en strings JSON.
        let outcomes = parse_json_str_array(m.get("outcomes"));
        let prices = parse_json_str_array(m.get("outcomePrices"));
        let token_ids = parse_json_str_array(m.get("clobTokenIds"));
        if token_ids.len() < 2 {
            return Ok(None);
        }

        let up_idx = outcomes
            .iter()
            .position(|o| {
                let o = o.to_lowercase();
                o == "up" || o == "yes"
            })
            .unwrap_or(0);
        let dn_idx = 1 - up_idx;

        let end_str = m
            .get("endDate")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let end_time = DateTime::parse_from_rfc3339(end_str)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| anyhow::anyhow!("endDate invalide '{end_str}': {e}"))?;

        Ok(Some(Market {
            condition_id: m.get("conditionId").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
            slug: slug.to_string(),
            up_token_id: token_ids[up_idx].clone(),
            down_token_id: token_ids[dn_idx].clone(),
            up_price: prices.get(up_idx).and_then(|s| s.parse().ok()).unwrap_or(0.5),
            down_price: prices.get(dn_idx).and_then(|s| s.parse().ok()).unwrap_or(0.5),
            end_time,
            window_ts,
            rewards_max_spread: num_field(m, "rewardsMaxSpread").unwrap_or(4.5),
            rewards_min_size: num_field(m, "rewardsMinSize").unwrap_or(0.0),
            tick_size: num_field(m, "orderPriceMinTickSize").unwrap_or(0.01),
            min_order_size: num_field(m, "orderMinSize").unwrap_or(5.0),
            neg_risk: m.get("negRisk").and_then(|v| v.as_bool()).unwrap_or(false),
        }))
    }

    /// Snapshot complet du carnet CLOB d'un token.
    pub async fn get_book(&self, token_id: &str) -> anyhow::Result<PolyBook> {
        let url = format!("{CLOB_BASE}/book");
        let resp = self
            .http
            .get(&url)
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?;
        let raw: RawBook = resp.json().await?;
        Ok(PolyBook {
            bids: raw.bids.iter().filter_map(Level::from_raw).collect(),
            asks: raw.asks.iter().filter_map(Level::from_raw).collect(),
        })
    }
}

impl Default for PolymarketClient {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
struct RawBook {
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
}

#[derive(Debug, Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

impl Level {
    fn from_raw(r: &RawLevel) -> Option<Level> {
        Some(Level {
            price: r.price.parse().ok()?,
            size: r.size.parse().ok()?,
        })
    }
}

/// Parse un champ qui est soit un array JSON, soit une string contenant un array JSON.
fn parse_json_str_array(v: Option<&serde_json::Value>) -> Vec<String> {
    match v {
        Some(serde_json::Value::Array(a)) => a
            .iter()
            .map(|x| x.as_str().map(str::to_string).unwrap_or_else(|| x.to_string()))
            .collect(),
        Some(serde_json::Value::String(s)) => {
            serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
        }
        _ => vec![],
    }
}

/// Lit un champ numérique qu'il soit encodé en nombre ou en string.
fn num_field(m: &serde_json::Value, key: &str) -> Option<f64> {
    match m.get(key) {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}
