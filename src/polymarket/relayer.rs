//! Connecteur Polymarket (porté du MM bot) : marché BTC 5 min actif (`real_up` = mid)
//! + carnet CLOB. REST polling (le côté PM n'est pas le chemin latence-critique).

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde::Deserialize;

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const CLOB_BASE: &str = "https://clob.polymarket.com";
const WINDOW_SEC: i64 = 300;

#[derive(Debug, Clone)]
pub struct Market {
    pub slug: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub end_time: DateTime<Utc>,
    pub window_ts: i64,
    pub tick_size: f64,
    pub min_order_size: f64,
}

impl Market {
    pub fn time_remaining_sec(&self) -> i64 {
        (self.end_time - Utc::now()).num_seconds()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Default)]
pub struct PolyBook {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl PolyBook {
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.iter().map(|l| l.price).fold(None, |m, p| Some(m.map_or(p, |x: f64| x.max(p))))
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.iter().map(|l| l.price).fold(None, |m, p| Some(m.map_or(p, |x: f64| x.min(p))))
    }
    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()? + self.best_ask()?) / 2.0)
    }
    /// Liquidité cumulée des asks ≤ price (pour le slippage d'un achat taker).
    pub fn ask_liquidity_to(&self, price: f64) -> f64 {
        self.asks.iter().filter(|l| l.price <= price + 1e-9).map(|l| l.size).sum()
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
            .expect("reqwest");
        Self { http }
    }

    fn current_window() -> i64 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        (now / WINDOW_SEC) * WINDOW_SEC
    }

    pub async fn get_current_btc_5m_market(&self) -> anyhow::Result<Option<Market>> {
        let base = Self::current_window();
        for window_ts in [base, base + WINDOW_SEC] {
            let slug = format!("btc-updown-5m-{window_ts}");
            if let Ok(Some(m)) = self.fetch_market(&slug, window_ts).await {
                if m.time_remaining_sec() > 0 {
                    return Ok(Some(m));
                }
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
        let Some(m) = event.get("markets").and_then(|v| v.as_array()).and_then(|a| a.first()) else {
            return Ok(None);
        };
        let outcomes = parse_str_array(m.get("outcomes"));
        let token_ids = parse_str_array(m.get("clobTokenIds"));
        if token_ids.len() < 2 {
            return Ok(None);
        }
        let up_idx = outcomes.iter().position(|o| {
            let o = o.to_lowercase();
            o == "up" || o == "yes"
        }).unwrap_or(0);
        let dn_idx = 1 - up_idx;
        let end_str = m.get("endDate").and_then(|v| v.as_str()).unwrap_or_default();
        let end_time = DateTime::parse_from_rfc3339(end_str)
            .map(|d| d.with_timezone(&Utc))
            .map_err(|e| anyhow::anyhow!("endDate '{end_str}': {e}"))?;
        Ok(Some(Market {
            slug: slug.to_string(),
            up_token_id: token_ids[up_idx].clone(),
            down_token_id: token_ids[dn_idx].clone(),
            end_time,
            window_ts,
            tick_size: num_field(m, "orderPriceMinTickSize").unwrap_or(0.01),
            min_order_size: num_field(m, "orderMinSize").unwrap_or(5.0),
        }))
    }

    pub async fn get_book(&self, token_id: &str) -> anyhow::Result<PolyBook> {
        let url = format!("{CLOB_BASE}/book");
        let raw: RawBook = self.http.get(&url).query(&[("token_id", token_id)]).send().await?
            .error_for_status()?.json().await?;
        Ok(PolyBook {
            bids: raw.bids.iter().filter_map(Level::from_raw).collect(),
            asks: raw.asks.iter().filter_map(Level::from_raw).collect(),
        })
    }
}

impl Default for PolymarketClient {
    fn default() -> Self { Self::new() }
}

/// Prix d'ouverture BTC à `window_ts` (kline 1m Binance) — proxy du strike.
pub async fn btc_price_at_window_open(window_ts: i64) -> anyhow::Result<f64> {
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={}&limit=1",
        window_ts * 1000
    );
    let arr: Vec<Vec<serde_json::Value>> = reqwest::Client::new()
        .get(&url).timeout(std::time::Duration::from_secs(10)).send().await?
        .error_for_status()?.json().await?;
    arr.first().and_then(|k| k.get(1)).and_then(|v| v.as_str()).and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("open introuvable window {window_ts}"))
}

#[derive(Deserialize)]
struct RawBook {
    #[serde(default)] bids: Vec<RawLevel>,
    #[serde(default)] asks: Vec<RawLevel>,
}
#[derive(Deserialize)]
struct RawLevel { price: String, size: String }
impl Level {
    fn from_raw(r: &RawLevel) -> Option<Level> {
        Some(Level { price: r.price.parse().ok()?, size: r.size.parse().ok()? })
    }
}

fn parse_str_array(v: Option<&serde_json::Value>) -> Vec<String> {
    match v {
        Some(serde_json::Value::Array(a)) => a.iter()
            .map(|x| x.as_str().map(str::to_string).unwrap_or_else(|| x.to_string())).collect(),
        Some(serde_json::Value::String(s)) => serde_json::from_str::<Vec<String>>(s).unwrap_or_default(),
        _ => vec![],
    }
}
fn num_field(m: &serde_json::Value, key: &str) -> Option<f64> {
    match m.get(key) {
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}
