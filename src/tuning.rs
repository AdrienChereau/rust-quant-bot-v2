//! Console de tuning à chaud — change les réglages sans redémarrer ni ralentir le hot loop.
//!
//! **Pourquoi c'est gratuit pour le hot loop :** le hot loop lit un *snapshot cohérent* via
//! `ArcSwap::load()` — un échange de pointeur atomique, lock-free, quelques nanosecondes. Le
//! dashboard écrit un nouveau snapshot via `store()`. Jamais d'I/O ni de verrou dans le hot
//! loop, et le snapshot est tout-ou-rien (impossible de lire un scénario à moitié appliqué).
//!
//! Toute écriture est **validée contre des bornes** (`scenarios.json` → `bounds`) puis
//! **journalisée** (`params_changes.jsonl`) — on ne pousse jamais une valeur hors-borne, et
//! chaque changement laisse une trace.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};

use crate::config::Config;

/// Réglages pilotables à chaud. Tous en `f64` (le frontend manipule un seul type ;
/// `cooldown_ms` est casté en `u64` à la lecture).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunableParams {
    // ── Signal (nœud mono : le radar produit le score, le paper ne le recalcule pas) ──
    pub score_fire_threshold: f64,
    pub d2_gamma: f64,
    pub vel_norm_factor: f64,
    pub w_obi: f64,
    pub w_tfi: f64,
    pub w_kalman: f64,
    pub w_basis: f64,
    pub basis_threshold_usd: f64,
    // ── Exécution (mono + paper) ──
    pub gap_min: f64,
    pub cooldown_ms: f64,
    pub kelly_fraction: f64,
    pub max_kelly_size_pct: f64,
    pub kelly_price_max: f64,
    // ── Défensif ──
    pub vacuum_velocity: f64,
    pub vacuum_obi: f64,
}

/// Ordre canonique des champs (frontend + sérialisation map).
pub const FIELDS: &[&str] = &[
    "score_fire_threshold", "d2_gamma", "vel_norm_factor",
    "w_obi", "w_tfi", "w_kalman", "w_basis", "basis_threshold_usd",
    "gap_min", "cooldown_ms", "kelly_fraction", "max_kelly_size_pct", "kelly_price_max",
    "vacuum_velocity", "vacuum_obi",
];

impl TunableParams {
    pub fn from_config(c: &Config) -> Self {
        Self {
            score_fire_threshold: c.score_fire_threshold,
            d2_gamma: c.d2_gamma,
            vel_norm_factor: c.vel_norm_factor,
            w_obi: c.composite_w_obi,
            w_tfi: c.composite_w_tfi,
            w_kalman: c.composite_w_kalman,
            w_basis: c.composite_w_basis,
            basis_threshold_usd: c.basis_threshold_usd,
            gap_min: c.gap_min,
            cooldown_ms: c.cooldown_ms as f64,
            kelly_fraction: c.kelly_fraction,
            max_kelly_size_pct: c.max_kelly_size_pct,
            kelly_price_max: c.kelly_price_max,
            vacuum_velocity: c.vacuum_velocity,
            vacuum_obi: c.vacuum_obi,
        }
    }

    pub fn get(&self, key: &str) -> Option<f64> {
        Some(match key {
            "score_fire_threshold" => self.score_fire_threshold,
            "d2_gamma" => self.d2_gamma,
            "vel_norm_factor" => self.vel_norm_factor,
            "w_obi" => self.w_obi,
            "w_tfi" => self.w_tfi,
            "w_kalman" => self.w_kalman,
            "w_basis" => self.w_basis,
            "basis_threshold_usd" => self.basis_threshold_usd,
            "gap_min" => self.gap_min,
            "cooldown_ms" => self.cooldown_ms,
            "kelly_fraction" => self.kelly_fraction,
            "max_kelly_size_pct" => self.max_kelly_size_pct,
            "kelly_price_max" => self.kelly_price_max,
            "vacuum_velocity" => self.vacuum_velocity,
            "vacuum_obi" => self.vacuum_obi,
            _ => return None,
        })
    }

    fn set(&mut self, key: &str, v: f64) -> bool {
        match key {
            "score_fire_threshold" => self.score_fire_threshold = v,
            "d2_gamma" => self.d2_gamma = v,
            "vel_norm_factor" => self.vel_norm_factor = v,
            "w_obi" => self.w_obi = v,
            "w_tfi" => self.w_tfi = v,
            "w_kalman" => self.w_kalman = v,
            "w_basis" => self.w_basis = v,
            "basis_threshold_usd" => self.basis_threshold_usd = v,
            "gap_min" => self.gap_min = v,
            "cooldown_ms" => self.cooldown_ms = v,
            "kelly_fraction" => self.kelly_fraction = v,
            "max_kelly_size_pct" => self.max_kelly_size_pct = v,
            "kelly_price_max" => self.kelly_price_max = v,
            "vacuum_velocity" => self.vacuum_velocity = v,
            "vacuum_obi" => self.vacuum_obi = v,
            _ => return false,
        }
        true
    }

    pub fn to_map(&self) -> BTreeMap<String, f64> {
        FIELDS.iter().map(|&k| (k.to_string(), self.get(k).unwrap())).collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bound {
    pub min: f64,
    pub max: f64,
    pub default: f64,
}

/// `scenarios.json` : bornes acceptables + presets nommés. Chargé au démarrage, lecture seule
/// ensuite. Si le fichier est absent, on retombe sur les bornes par défaut intégrées.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScenariosFile {
    #[serde(default)]
    pub bounds: BTreeMap<String, Bound>,
    #[serde(default)]
    pub scenarios: BTreeMap<String, BTreeMap<String, f64>>,
}

/// Bornes intégrées (fallback si `scenarios.json` absent). Alignées sur le plan signal v2.
fn default_file() -> ScenariosFile {
    let b = |min, max, default| Bound { min, max, default };
    let bounds = BTreeMap::from([
        ("score_fire_threshold".into(), b(0.20, 0.60, 0.35)),
        ("d2_gamma".into(), b(0.0, 1.0, 0.50)),
        ("vel_norm_factor".into(), b(5.0, 200.0, 5.0)),
        ("w_obi".into(), b(0.0, 1.0, 0.40)),
        ("w_tfi".into(), b(0.0, 1.0, 0.30)),
        ("w_kalman".into(), b(0.0, 1.0, 0.20)),
        ("w_basis".into(), b(0.0, 1.0, 0.10)),
        ("basis_threshold_usd".into(), b(5.0, 100.0, 20.0)),
        ("gap_min".into(), b(0.0, 0.10, 0.02)),
        ("cooldown_ms".into(), b(0.0, 30000.0, 3000.0)),
        ("kelly_fraction".into(), b(0.10, 0.50, 0.50)),
        ("max_kelly_size_pct".into(), b(0.005, 0.10, 0.02)),
        ("kelly_price_max".into(), b(0.80, 0.98, 0.90)),
        ("vacuum_velocity".into(), b(-0.01, 0.0, -0.0010)),
        ("vacuum_obi".into(), b(-1.0, 0.0, -0.40)),
    ]);
    let scenarios = BTreeMap::from([
        ("conservateur".into(), BTreeMap::from([
            ("score_fire_threshold".into(), 0.45),
            ("kelly_fraction".into(), 0.25),
            ("d2_gamma".into(), 0.35),
            ("cooldown_ms".into(), 5000.0),
        ])),
        ("aggressif".into(), BTreeMap::from([
            ("score_fire_threshold".into(), 0.30),
            ("kelly_fraction".into(), 0.50),
            ("d2_gamma".into(), 0.60),
            ("cooldown_ms".into(), 2000.0),
        ])),
        ("macro-event".into(), BTreeMap::from([
            ("score_fire_threshold".into(), 0.55),
            ("cooldown_ms".into(), 8000.0),
        ])),
    ]);
    ScenariosFile { bounds, scenarios }
}

/// État partagé de la console. `params` = snapshot lock-free ; `file` = bornes/scénarios (lecture
/// seule) ; `audit_path` = journal append-only des changements.
pub struct Tuning {
    pub params: ArcSwap<TunableParams>,
    pub file: ScenariosFile,
    pub audit_path: String,
}

pub type SharedTuning = Arc<Tuning>;

impl Tuning {
    /// Charge la console : params depuis la config (env), bornes/scénarios depuis `SCENARIOS_PATH`
    /// (`scenarios.json` par défaut), sinon bornes intégrées.
    pub fn load(cfg: &Config) -> SharedTuning {
        let path = std::env::var("SCENARIOS_PATH").unwrap_or_else(|_| "scenarios.json".into());
        let file = fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<ScenariosFile>(&s).ok())
            .map(|mut f| {
                if f.bounds.is_empty() { f.bounds = default_file().bounds; }
                f
            })
            .unwrap_or_else(|| {
                tracing::info!(%path, "scenarios.json absent — bornes intégrées par défaut");
                default_file()
            });
        let audit_path = std::env::var("PARAMS_AUDIT_PATH")
            .unwrap_or_else(|_| "data/params_changes.jsonl".into());
        Arc::new(Tuning {
            params: ArcSwap::from_pointee(TunableParams::from_config(cfg)),
            file,
            audit_path,
        })
    }

    /// Snapshot courant (lock-free). À appeler une fois par tick dans le hot loop.
    pub fn snapshot(&self) -> Arc<TunableParams> {
        self.params.load_full()
    }

    /// Applique un lot de modifications, validé contre les bornes. Tout-ou-rien : si une seule
    /// valeur est hors-borne ou inconnue, rien n'est appliqué et on renvoie les erreurs.
    pub fn apply_updates(
        &self,
        updates: &BTreeMap<String, f64>,
        source: &str,
    ) -> Result<TunableParams, Vec<String>> {
        let mut errors = Vec::new();
        for (k, &v) in updates {
            match self.file.bounds.get(k) {
                None => errors.push(format!("paramètre inconnu ou non réglable : {k}")),
                Some(b) if v < b.min || v > b.max =>
                    errors.push(format!("{k}={v} hors borne [{}, {}]", b.min, b.max)),
                Some(_) => {}
            }
        }
        if !errors.is_empty() {
            return Err(errors);
        }

        let current = self.params.load();
        let before = current.to_map();
        let mut next = TunableParams::clone(&current);
        for (k, &v) in updates {
            next.set(k, v);
        }
        self.params.store(Arc::new(next.clone()));
        self.audit(source, updates, &before);
        Ok(next)
    }

    /// Applique un scénario nommé (lui-même validé contre les bornes).
    pub fn apply_scenario(&self, name: &str, source: &str) -> Result<TunableParams, Vec<String>> {
        match self.file.scenarios.get(name) {
            Some(updates) => self.apply_updates(updates, &format!("{source}:scenario={name}")),
            None => Err(vec![format!("scénario inconnu : {name}")]),
        }
    }

    fn audit(&self, source: &str, updates: &BTreeMap<String, f64>, before: &BTreeMap<String, f64>) {
        let changes: BTreeMap<&String, serde_json::Value> = updates.iter().map(|(k, &v)| {
            (k, serde_json::json!({ "old": before.get(k), "new": v }))
        }).collect();
        let rec = serde_json::json!({
            "ts": chrono::Utc::now().to_rfc3339(),
            "source": source,
            "changes": changes,
        });
        if let Some(dir) = std::path::Path::new(&self.audit_path).parent() {
            let _ = fs::create_dir_all(dir);
        }
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&self.audit_path) {
            let _ = writeln!(f, "{rec}");
        }
        tracing::warn!(source, ?updates, "⚙️  paramètres mis à jour (tuning à chaud)");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuning() -> SharedTuning {
        let cfg = test_cfg();
        Arc::new(Tuning {
            params: ArcSwap::from_pointee(TunableParams::from_config(&cfg)),
            file: default_file(),
            audit_path: format!("/tmp/params_audit_test_{}.jsonl", std::process::id()),
        })
    }

    fn test_cfg() -> Config {
        // Valeurs par défaut suffisantes pour le test (from_env lit l'environnement vide).
        Config::from_env()
    }

    #[test]
    fn in_bounds_update_applies() {
        let t = tuning();
        let upd = BTreeMap::from([("score_fire_threshold".to_string(), 0.45)]);
        assert!(t.apply_updates(&upd, "test").is_ok());
        assert_eq!(t.snapshot().score_fire_threshold, 0.45);
    }

    #[test]
    fn out_of_bounds_rejected_and_unchanged() {
        let t = tuning();
        let before = t.snapshot().score_fire_threshold;
        let upd = BTreeMap::from([("score_fire_threshold".to_string(), 5.0)]); // > max 0.60
        assert!(t.apply_updates(&upd, "test").is_err());
        assert_eq!(t.snapshot().score_fire_threshold, before); // inchangé
    }

    #[test]
    fn unknown_key_rejected() {
        let t = tuning();
        let upd = BTreeMap::from([("does_not_exist".to_string(), 1.0)]);
        assert!(t.apply_updates(&upd, "test").is_err());
    }

    #[test]
    fn scenario_applies_multiple_fields() {
        let t = tuning();
        assert!(t.apply_scenario("conservateur", "test").is_ok());
        let s = t.snapshot();
        assert_eq!(s.score_fire_threshold, 0.45);
        assert_eq!(s.kelly_fraction, 0.25);
    }

    #[test]
    fn partial_batch_is_all_or_nothing() {
        let t = tuning();
        let before = t.snapshot().d2_gamma;
        // d2_gamma valide MAIS gap_min hors borne → tout rejeté
        let upd = BTreeMap::from([
            ("d2_gamma".to_string(), 0.7),
            ("gap_min".to_string(), 99.0),
        ]);
        assert!(t.apply_updates(&upd, "test").is_err());
        assert_eq!(t.snapshot().d2_gamma, before);
    }
}
