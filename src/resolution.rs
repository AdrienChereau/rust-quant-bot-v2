//! Résolution OFFICIELLE Polymarket — l'arbitre du paper v1.
//!
//! Le règlement interne (spot Binance vs strike) s'est révélé faussable (feed figé → 41 %
//! d'issues fausses, toutes en notre faveur). Désormais le paper attend la résolution
//! officielle via `gamma-api.polymarket.com/events/slug/btc-updown-5m-{window_ts}`
//! (`outcomePrices ["1","0"]` = Up gagnant). Le label Binance ne sert plus que de
//! fallback signalé si l'API reste muette après ~2 min.
//!
//! Task dédiée hors hot loop : requêtes réseau jamais dans la boucle de décision.

use std::time::Duration;

use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy)]
pub struct ResolutionResult {
    pub window_ts: i64,
    /// `Some(true)` = Up officiel, `Some(false)` = Down officiel, `None` = introuvable
    /// après tous les essais (fallback Binance à décider par l'appelant).
    pub up: Option<bool>,
}

/// Lance la task résolveur. Envoie un `window_ts` dans le Sender → reçoit un
/// `ResolutionResult` dans le Receiver (retries 5 s, ~2 min max).
pub fn spawn() -> (mpsc::Sender<i64>, mpsc::Receiver<ResolutionResult>) {
    let (req_tx, mut req_rx) = mpsc::channel::<i64>(16);
    let (res_tx, res_rx) = mpsc::channel::<ResolutionResult>(16);
    tokio::spawn(async move {
        while let Some(wts) = req_rx.recv().await {
            let mut up = None;
            // La résolution UMA arrive 3-8 min après la fermeture ; le prix PM tranche dès ~3 min.
            // On réessaie jusqu'à ~13 min (156 × 5 s) avant d'abandonner.
            for attempt in 0..156 {
                match fetch_official(wts).await {
                    Ok(Some(u)) => {
                        up = Some(u);
                        break;
                    }
                    Ok(None) => {}
                    Err(e) if attempt == 0 => tracing::warn!(error = %e, wts, "résolution officielle : 1er essai raté"),
                    Err(_) => {}
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            let _ = res_tx.send(ResolutionResult { window_ts: wts, up }).await;
        }
    });
    (req_tx, res_rx)
}

/// Interroge Gamma pour une fenêtre close. `Ok(None)` = pas encore résolue.
async fn fetch_official(window_ts: i64) -> anyhow::Result<Option<bool>> {
    let url = format!("https://gamma-api.polymarket.com/events/slug/btc-updown-5m-{window_ts}");
    let v: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "curl/8.4.0") // Cloudflare rejette les UA par défaut
        .timeout(Duration::from_secs(10))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let m = &v["markets"][0];
    let outcomes: Vec<String> = serde_json::from_str(m["outcomes"].as_str().unwrap_or("[]"))?;
    let prices: Vec<String> = serde_json::from_str(m["outcomePrices"].as_str().unwrap_or("[]"))?;
    let up_price = outcomes.iter().zip(prices.iter())
        .find(|(o, _)| o.as_str() == "Up")
        .and_then(|(_, p)| p.parse::<f64>().ok());
    let resolved = m["umaResolutionStatus"].as_str() == Some("resolved");
    match up_price {
        // Résolu officiellement → verdict ferme.
        Some(p) if resolved => Ok(Some(p >= 0.5)),
        // Pas encore "resolved" mais le prix PM a déjà tranché (≥0.98 / ≤0.02) → on accepte.
        Some(p) if p >= 0.98 => Ok(Some(true)),
        Some(p) if p <= 0.02 => Ok(Some(false)),
        _ => Ok(None),
    }
}
