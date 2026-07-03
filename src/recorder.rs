//! Enregistreur de fenêtres — la matière première de la calibration.
//!
//! Écrit dans `data/windows.jsonl` :
//!  - une ligne `sample` par seconde : (window_ts, remaining_s, spot, strike, sigma, score, fair, real)
//!  - une ligne `outcome` au rollover de fenêtre : (window_ts, strike, spot_close, up)
//!
//! Le script `scripts/calibrate.py` joint les deux par `window_ts` et calcule :
//! Brier(fair) vs Brier(prix PM) vs Brier(0.5) + grid-search (σ, γ) minimisant le Brier.
//! C'est ce qui permet de savoir — AVANT de trader — si notre proba bat le marché.
//!
//! Coût : 1 append/s sur le bras tick (hors hot path OBI), négligeable.
//!
//! ⚠️ Caveat : `spot` = mid Binance ; Polymarket résout sur son propre oracle. L'écart est
//! faible mais non nul — à garder en tête pour les fenêtres qui finissent à ±1-2 $ du strike.

use std::fs;
use std::io::Write as _;

pub struct WindowRecorder {
    path: String,
    last_sample_ms: u64,
    cur_window_ts: i64,
    last_spot: f64,
    last_strike: f64,
}

impl WindowRecorder {
    pub fn new(path: String) -> Self {
        if let Some(dir) = std::path::Path::new(&path).parent() {
            let _ = fs::create_dir_all(dir);
        }
        Self { path, last_sample_ms: 0, cur_window_ts: 0, last_spot: 0.0, last_strike: 0.0 }
    }

    /// À appeler à chaque tick (échantillonne à 1 Hz ; détecte le rollover de fenêtre).
    /// `up_ask`/`down_ask` : meilleurs asks des deux books — pour une EV au prix d'exécution
    /// réel (croiser le spread), pas au mid flatteur. 0.0 si book vide.
    #[allow(clippy::too_many_arguments)]
    pub fn sample(
        &mut self,
        now_ms: u64,
        window_ts: i64,
        remaining_s: i64,
        spot: f64,
        strike: f64,
        sigma: f64,
        score: f64,
        fair: f64,
        real: f64,
        up_ask: f64,
        down_ask: f64,
    ) {
        // Rollover : la fenêtre précédente est finie → issue = dernier spot vu vs son strike.
        if window_ts != self.cur_window_ts {
            if self.cur_window_ts != 0 && self.last_strike > 0.0 && self.last_spot > 0.0 {
                self.append(&serde_json::json!({
                    "kind": "outcome",
                    "window_ts": self.cur_window_ts,
                    "strike": self.last_strike,
                    "spot_close": self.last_spot,
                    "up": self.last_spot > self.last_strike,
                }));
            }
            self.cur_window_ts = window_ts;
        }
        self.last_spot = spot;
        self.last_strike = strike;

        if now_ms.saturating_sub(self.last_sample_ms) < 1000 {
            return;
        }
        self.last_sample_ms = now_ms;
        self.append(&serde_json::json!({
            "kind": "sample",
            "ts": now_ms,
            "window_ts": window_ts,
            "remaining_s": remaining_s,
            "spot": spot,
            "strike": strike,
            "sigma": sigma,
            "score": score,
            "fair": fair,
            "real": real,
            "up_ask": up_ask,
            "down_ask": down_ask,
        }));
    }

    /// Issue OFFICIELLE (résolution Polymarket/Chainlink) — prioritaire sur le label Binance
    /// dans `calibrate.py`.
    pub fn record_official(&self, window_ts: i64, up: bool) {
        self.append(&serde_json::json!({
            "kind": "outcome_official",
            "window_ts": window_ts,
            "up": up,
        }));
    }

    fn append(&self, v: &serde_json::Value) {
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&self.path) {
            let _ = writeln!(f, "{v}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollover_writes_outcome() {
        let path = format!("/tmp/windows_test_{}.jsonl", std::process::id());
        let _ = std::fs::remove_file(&path);
        let mut r = WindowRecorder::new(path.clone());
        // fenêtre 1 : deux samples (spot finit au-dessus du strike)
        r.sample(1_000, 300, 250, 60_100.0, 60_000.0, 0.5, 0.1, 0.6, 0.55, 0.56, 0.46);
        r.sample(2_500, 300, 248, 60_150.0, 60_000.0, 0.5, 0.1, 0.6, 0.55, 0.56, 0.46);
        // rollover → outcome up=true pour la fenêtre 300
        r.sample(3_500, 600, 300, 60_150.0, 60_050.0, 0.5, 0.0, 0.5, 0.5, 0.51, 0.51);
        let content = std::fs::read_to_string(&path).unwrap();
        let outcome: Vec<_> = content.lines().filter(|l| l.contains("\"outcome\"")).collect();
        assert_eq!(outcome.len(), 1);
        assert!(outcome[0].contains("\"up\":true"));
        assert!(outcome[0].contains("\"window_ts\":300"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn samples_throttled_to_1hz() {
        let path = format!("/tmp/windows_test_hz_{}.jsonl", std::process::id());
        let _ = std::fs::remove_file(&path);
        let mut r = WindowRecorder::new(path.clone());
        for i in 0..30u64 {
            r.sample(1_000 + i * 100, 300, 250, 60_000.0, 60_000.0, 0.5, 0.0, 0.5, 0.5, 0.51, 0.51);
        }
        // 3 s de ticks à 10 Hz → 3 samples max
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.lines().filter(|l| l.contains("\"sample\"")).count() <= 3);
        let _ = std::fs::remove_file(&path);
    }
}
