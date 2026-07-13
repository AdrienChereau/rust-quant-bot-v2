//! Contexte d'exécution LIVE — fait le pont entre la boucle de l'executor
//! (identique au paper : mêmes décisions, mêmes stats) et le CLOB réel.
//!
//! Responsabilités :
//!   • poser/annuler/repricer les bids GTC (+ FAK d'assurance)
//!   • collecter les fills : WS user (rapide) + poll /data/orders (autorité)
//!   • fournir à l'executor des `LiveFill` prêts à injecter dans sc.on_fill /
//!     le miroir comptable (PaperEngine, qui reste le grand livre)
//!
//! Le PaperEngine sert de MIROIR : chaque fill réel y est enregistré via
//! `try_buy` au prix réel — cash/balances/PnL suivent la même comptabilité
//! que le paper (comparaison A/B directe). Écart connu v1 : pas de merge ni
//! de redeem on-chain → le collatéral réel (affiché à part) diverge du miroir
//! du montant des positions non encore réglées.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use super::auth::LiveCredentials;
use super::orders::{self, CancelStatus, OrderArgs, PlaceResult};
use super::relayer::{RelayerCtx, TxOutcome};

/// Résultat du lancement d'un merge.
#[derive(Debug, PartialEq)]
pub enum MergeStart {
    Submitted,
    WouldRevert, // déjà mergé on-chain (probable) → aligner le miroir
    Err,
}
use super::user_ws::{self, OrderUpdate};

/// Intention économique de l'ordre. Elle est propagée jusqu'au journal des
/// fills afin de distinguer les revenus du market making des assurances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderIntent {
    SymmetricOpen,
    Completion,
    SkewAccumulation,
    Rescue,
}

impl OrderIntent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SymmetricOpen => "symmetric_open",
            Self::Completion => "completion",
            Self::SkewAccumulation => "skew_accumulation",
            Self::Rescue => "rescue",
        }
    }
}

/// Fill prêt pour la boucle stratégie.
#[derive(Debug, Clone)]
pub struct LiveFill {
    pub is_up: bool,
    pub price: f64,
    pub size: f64,
    pub maker: bool,
    pub intent: OrderIntent,
    pub condition_id: String,
}

/// Un slot de l'ÉCHELLE de quoting : un ordre restant + son adresse
/// (côté, niveau). Niveau 0 = bb+tick (ou complétion), niveau 1 = un cran
/// plus bas — un vrai MM échelonne sa présence au carnet.
#[derive(Debug, Clone)]
pub struct LiveSlot {
    pub r: RestingOrder,
    pub is_up: bool,
    pub level: u8,
    pub intent: OrderIntent,
}

/// Un de NOS ordres restants.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub order_id: String,
    pub price: f64,
    pub size: f64,
    pub matched: f64,   // cumul fillé déjà comptabilisé (recopié du grand livre)
    pub placed_ms: i64, // epoch ms du POST — un ordre trop frais peut être absent du poll (lag d'indexation)
    pub intent: OrderIntent,
    pub condition_id: String,
}

/// UNE ligne du GRAND LIVRE par ordre (clé = order_id). C'est la SEULE source de
/// vérité de comptabilité des fills. Incident du 8 juil. 23:33 : le WS ré-émet
/// chaque trade 3-4 fois (statuts MATCHED/MINED/CONFIRMED) et par plusieurs
/// canaux → un ordre Down de 6 parts compté 4× → inventaire fantôme −24 →
/// 18 Up nus. La parade structurelle : on ne compte JAMAIS un montant de trade ;
/// on ne suit que le `size_matched` ABSOLU par ordre, et on n'émet que le DELTA
/// au-delà de ce qui est déjà compté. Rejouer le même event = delta 0.
#[derive(Debug, Clone)]
struct LedgerEntry {
    is_up: bool,
    size: f64,    // taille d'origine de l'ordre (plafond dur du cumul)
    counted: f64, // cumul déjà ÉMIS en LiveFill (le size_matched absolu déjà pris en compte)
    price: f64,   // prix limite (valorise les fills maker)
    intent: OrderIntent,
    condition_id: String,
}

#[derive(Debug, Clone)]
struct OrderMeta {
    is_up: bool,
    maker: bool,
    intent: OrderIntent,
    condition_id: String,
}

/// Ordre dont le statut final reste incertain. Il reste volontairement dans
/// l'exposition jusqu'à confirmation terminale afin de ne jamais repost par-dessus.
#[derive(Debug, Clone)]
struct AuditOrder {
    r: RestingOrder,
    is_up: bool,
    tries: u32,
    cancel_requested: bool,
}

/// Exposition acheteuse encore susceptible d'être exécutée. Les audits sont
/// comptés à leur taille restante maximale : c'est conservateur par conception.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenExposure {
    pub up: f64,
    pub down: f64,
    pub completion_up: f64,
    pub completion_down: f64,
    pub uncertain_up: f64,
    pub uncertain_down: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrderTerminalState {
    Terminal,
    Open,
    Unknown,
}

fn order_terminal_state(value: &serde_json::Value) -> OrderTerminalState {
    let status = value
        .get("status")
        .or_else(|| value.get("order_status"))
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_ascii_uppercase();
    if [
        "CANCELED",
        "CANCELLED",
        "MATCHED",
        "FILLED",
        "EXPIRED",
        "CLOSED",
    ]
    .iter()
    .any(|needle| status.contains(needle))
    {
        OrderTerminalState::Terminal
    } else if ["LIVE", "OPEN", "PENDING", "MATCHING"]
        .iter()
        .any(|needle| status.contains(needle))
    {
        OrderTerminalState::Open
    } else {
        OrderTerminalState::Unknown
    }
}

impl OpenExposure {
    pub fn side(self, is_up: bool) -> f64 {
        if is_up {
            self.up
        } else {
            self.down
        }
    }

    pub fn completion_side(self, is_up: bool) -> f64 {
        if is_up {
            self.completion_up
        } else {
            self.completion_down
        }
    }

    pub fn uncertain_side(self, is_up: bool) -> f64 {
        if is_up {
            self.uncertain_up
        } else {
            self.uncertain_down
        }
    }
}

fn aggregate_open_exposure(rest: &[LiveSlot], audit: &[AuditOrder]) -> OpenExposure {
    let mut out = OpenExposure::default();
    let mut add = |is_up: bool, intent: OrderIntent, qty: f64, uncertain: bool| {
        let qty = qty.max(0.0);
        if is_up {
            out.up += qty;
            if intent == OrderIntent::Completion {
                out.completion_up += qty;
            }
            if uncertain {
                out.uncertain_up += qty;
            }
        } else {
            out.down += qty;
            if intent == OrderIntent::Completion {
                out.completion_down += qty;
            }
            if uncertain {
                out.uncertain_down += qty;
            }
        }
    };
    for slot in rest {
        add(slot.is_up, slot.intent, slot.r.size - slot.r.matched, false);
    }
    for audit in audit {
        add(
            audit.is_up,
            audit.r.intent,
            audit.r.size - audit.r.matched,
            true,
        );
    }
    out
}

pub struct LiveCtx {
    pub creds: LiveCredentials,
    pub armed: bool,
    cond_tx: watch::Sender<Option<String>>,
    fill_rx: mpsc::UnboundedReceiver<OrderUpdate>,
    // token_ids du marché courant (associe asset_id/ordre → côté Up/Down)
    up_token: String,
    dn_token: String,
    condition_id: String,
    // ordre_id → métadonnées immuables, y compris intention et marché d'origine.
    order_side: HashMap<String, OrderMeta>,
    /// GRAND LIVRE par ordre : la seule comptabilité de fills. Tous les canaux
    /// (POST, event `order`, poll, récolte) créditent via `credit()` avec un
    /// size_matched ABSOLU → aucun double comptage possible. Purgé au rollover.
    ledger: HashMap<String, LedgerEntry>,
    last_poll_ms: i64,
    /// Collatéral USDC réel : sync CLOB ~10 s + décrément IMMÉDIAT à chaque fill
    /// (plusieurs fills/merges par fenêtre → on doit savoir en quasi temps réel).
    pub cash: f64,
    /// Collatéral au tout premier démarrage (persisté data/live_baseline.json) —
    /// référence du PnL wallet RÉEL affiché au dashboard.
    pub baseline: f64,
    last_cash_sync_ms: i64,
    /// Merge/redeem on-chain via le relayer officiel (None = clés absentes → désactivé).
    relayer: Option<RelayerCtx>,
    merge_inflight: Option<(f64, tokio::sync::oneshot::Receiver<TxOutcome>)>,
    last_merge_attempt_ms: i64, // cooldown anti-spam (429 Cloudflare du 7 juil.)
    last_pos_sync_ms: i64,
    pub positions_dirty: bool, // merge/redeem incertain → resync des positions
    /// RTT de la DERNIÈRE requête CLOB (ms) : POST d'ordre ou poll /data/orders.
    /// C'est la latence réseau réelle Dublin → serveur Polymarket → Dublin.
    pub last_rtt_ms: u64,
    /// Fills constatés à la RÉPONSE du POST (GTC marketable) : émis au prix
    /// moyen réellement exécuté, dès le tick courant (le journal doit montrer
    /// le prix PAYÉ, pas notre limite — écarts 45,0 vs 45,7 du 8 juil.).
    post_fills: Vec<LiveFill>,
    /// Ordres de VENTE (flatten FAK) : leurs events WS ne doivent JAMAIS être
    /// crédités comme des achats par le grand livre.
    sell_ids: std::collections::HashSet<String>,
    /// Ordres retirés SANS lecture réussie de leur size_matched (GET /data/order
    /// en échec au moment du cancel). Un fill ne doit JAMAIS être perdu : le poll
    /// réinterroge ces ordres jusqu'à obtenir la vérité (incident du 8 juil. :
    /// Up 5@56¢ fillé, lecture ratée, ordre oublié → fausse complétion à 62¢).
    audit: Vec<AuditOrder>,
    audit_max_age_ms: i64,
    /// Un invariant d'exécution suspend les nouvelles poses le temps de se
    /// réconcilier — PAUSE DE SÉCURITÉ, pas une mort définitive (décision
    /// utilisateur 13 juil. : le halt permanent laissait la position nue sans
    /// défense pendant les 3 dernières minutes de la fenêtre 15:20). La boucle
    /// annule/récolte tout pendant la pause ; la reprise est automatique dès
    /// que l'audit est vide (≥10 s) ou au rollover.
    pub halted_reason: Option<String>,
    halted_since_ms: i64,
}

impl LiveCtx {
    /// Démarrage : auth SDK + sync allowance + WS user + heartbeats (dead-man).
    pub async fn start(
        creds: LiveCredentials,
        armed: bool,
        audit_max_age_s: i64,
    ) -> anyhow::Result<Self> {
        orders::startup(&creds).await?;
        let collateral = super::auth::get_collateral_balance(&creds).await?;
        tracing::info!(collateral, armed, "LIVE démarré — collatéral USDC réel");
        let (cond_tx, fill_rx) = user_ws::spawn(creds.clone());
        tokio::spawn(orders::run_heartbeats(creds.clone()));
        let cash0 = collateral;
        // Baseline du PnL wallet : lue si déjà posée, sinon posée MAINTENANT.
        // (Pour repartir de zéro après un refinancement : supprimer/renommer le
        // fichier data/live_baseline.json avant de redémarrer.)
        let baseline = {
            let path = std::path::Path::new("data/live_baseline.json");
            let existing = std::fs::read_to_string(path)
                .ok()
                .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                .and_then(|v| v.get("baseline").and_then(|b| b.as_f64()));
            match existing {
                Some(b) => b,
                None => {
                    let _ = std::fs::create_dir_all("data");
                    let _ = std::fs::write(path, format!("{{\"baseline\":{collateral}}}"));
                    tracing::info!(collateral, "baseline PnL wallet posée");
                    collateral
                }
            }
        };
        let creds2 = creds.clone();
        Ok(Self {
            creds,
            armed,
            cond_tx,
            fill_rx,
            up_token: String::new(),
            dn_token: String::new(),
            condition_id: String::new(),
            order_side: HashMap::new(),
            ledger: HashMap::new(),
            last_poll_ms: 0,
            cash: cash0,
            baseline,
            last_cash_sync_ms: 0,
            relayer: RelayerCtx::from_env(&creds2),
            merge_inflight: None,
            last_merge_attempt_ms: 0,
            last_pos_sync_ms: 0,
            positions_dirty: false,
            last_rtt_ms: 0,
            post_fills: Vec::new(),
            sell_ids: std::collections::HashSet::new(),
            audit: Vec::new(),
            audit_max_age_ms: audit_max_age_s.max(1) * 1_000,
            halted_reason: None,
            halted_since_ms: 0,
        })
    }

    /// Rollover de fenêtre : annule tout sur l'ancien marché, bascule le WS user.
    pub async fn on_new_market(&mut self, condition_id: &str, up_token: &str, dn_token: &str) {
        if self.halted_reason.take().is_some() {
            tracing::warn!("LIVE repris au rollover — pause de sécurité levée (marché neuf)");
        }
        if !self.condition_id.is_empty() && self.armed {
            if let Err(e) = orders::cancel_market_orders(&self.creds, &self.condition_id).await {
                tracing::warn!(error = %e, "cancel-market-orders au rollover");
            }
        }
        self.condition_id = condition_id.to_string();
        self.up_token = up_token.to_string();
        self.dn_token = dn_token.to_string();
        self.order_side.clear();
        self.ledger.clear(); // nouveau marché = nouveaux order_ids
        self.sell_ids.clear();
        self.post_fills.clear();
        self.audit.clear();
        // Une confirmation d'ancien merge ne doit jamais créditer le miroir de
        // la nouvelle condition. La position réelle sera resynchronisée.
        self.merge_inflight = None;
        self.positions_dirty = true;
        let _ = self.cond_tx.send(Some(condition_id.to_string()));
        // Métadonnées RAW du nouveau marché (tick exact par token).
        if let Err(e) = orders::preload_token_meta(&[up_token, dn_token]).await {
            tracing::warn!(error = %e, "préchargement tick sizes échoué (fallback 2 déc.)");
        }
        // Re-sync du collatéral au rollover (règlements/redeems éventuels).
        self.sync_cash(true).await;
    }

    /// Sync du collatéral réel (forcé, ou au plus 1×/10 s).
    pub async fn sync_cash(&mut self, force: bool) {
        let now = chrono::Utc::now().timestamp_millis();
        if !force && now - self.last_cash_sync_ms < 10_000 {
            return;
        }
        self.last_cash_sync_ms = now;
        match super::auth::get_collateral_balance(&self.creds).await {
            Ok(c) => self.cash = c,
            Err(e) => tracing::warn!(error = %e, "sync collatéral échoué"),
        }
    }

    /// Décrément immédiat à chaque fill BUY (le sync CLOB confirmera derrière).
    pub fn note_fill_cash(&mut self, price: f64, size: f64) {
        self.cash = (self.cash - price * size).max(0.0);
    }

    pub fn merge_available(&self) -> bool {
        let now = chrono::Utc::now().timestamp_millis();
        self.relayer.is_some()
            && self.merge_inflight.is_none()
            && self.armed
            && now - self.last_merge_attempt_ms >= 10_000 // garde technique anti-spam
                                                          // (le VRAI cooldown stratégique — paire > 1$ — vit dans l'executor ;
                                                          //  ici : 10 s entre tentatives, pénalité +35 s sur échec relayer)
    }

    /// Force la resynchronisation du collatéral au prochain tick.
    pub fn force_cash_resync(&mut self) {
        self.last_cash_sync_ms = 0;
        self.positions_dirty = true;
    }

    /// Arrête les nouvelles poses après une violation d'invariant. Les ordres
    /// existants sont ensuite annulés/réconciliés par la boucle normale.
    pub fn halt(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        if self.halted_reason.is_none() {
            tracing::error!(reason = %reason,
                "LIVE en PAUSE DE SÉCURITÉ — invariant violé (annulation générale, reprise auto quand l'audit est vide)");
            self.halted_reason = Some(reason);
            self.halted_since_ms = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Reprise automatique : la pause de sécurité se lève dès que toutes les
    /// incertitudes sont résolues (audit vide) et qu'au moins 10 s ont passé —
    /// le balayage/l'audit ont alors récolté ou annulé tout ordre zombie.
    fn try_resume(&mut self, now_ms: i64) {
        if self.halted_reason.is_some()
            && self.audit.is_empty()
            && now_ms - self.halted_since_ms >= 10_000
        {
            tracing::warn!(
                "LIVE repris — incohérence résorbée (audit vide), retour à la cotation"
            );
            self.halted_reason = None;
        }
    }

    pub fn is_halted(&self) -> bool {
        self.halted_reason.is_some()
    }

    pub fn audit_count(&self) -> u32 {
        self.audit.len() as u32
    }

    pub fn audit_oldest_age_ms(&self, now_ms: i64) -> i64 {
        self.audit
            .iter()
            .map(|audit| now_ms.saturating_sub(audit.r.placed_ms))
            .max()
            .unwrap_or(0)
    }

    /// Exposition de tous les achats encore vivants. Un ordre en audit reste
    /// compté au reliquat complet jusqu'à preuve qu'il est terminal.
    pub fn open_exposure(&self, rest: &[LiveSlot]) -> OpenExposure {
        aggregate_open_exposure(rest, &self.audit)
    }

    fn audit_order(&mut self, r: RestingOrder, is_up: bool, cancel_requested: bool) {
        if self.audit.iter().any(|a| a.r.order_id == r.order_id) {
            return;
        }
        self.audit.push(AuditOrder {
            r,
            is_up,
            tries: 0,
            cancel_requested,
        });
    }

    /// Positions RÉELLES (up, down) en parts, depuis le CLOB (vérité on-chain).
    /// None si pas dû (≥60 s depuis le dernier sync, sauf `positions_dirty`).
    pub async fn real_positions(&mut self, now_ms: i64) -> Option<(f64, f64)> {
        if !self.positions_dirty && now_ms - self.last_pos_sync_ms < 60_000 {
            return None;
        }
        if self.up_token.is_empty() {
            return None;
        }
        self.last_pos_sync_ms = now_ms;
        self.positions_dirty = false;
        let up = super::auth::get_conditional_balance(&self.creds, &self.up_token).await;
        let dn = super::auth::get_conditional_balance(&self.creds, &self.dn_token).await;
        match (up, dn) {
            (Ok(u), Ok(d)) => Some((u, d)),
            (u, d) => {
                tracing::warn!(?u, ?d, "sync positions réelles échoué");
                None
            }
        }
    }

    /// Positions RÉELLES on-chain, lecture directe — pour l'AMONT d'une décision
    /// irréversible (merge), qui ne doit JAMAIS partir sur le miroir. N'affecte
    /// pas la cadence de `real_positions` (le réalignement du moteur reste, lui,
    /// gardé par le settle-quiet). Interroge balance-allowance CONDITIONAL.
    pub async fn positions_force(&mut self) -> Option<(f64, f64)> {
        if self.up_token.is_empty() {
            return None;
        }
        let up = super::auth::get_conditional_balance(&self.creds, &self.up_token).await;
        let dn = super::auth::get_conditional_balance(&self.creds, &self.dn_token).await;
        match (up, dn) {
            (Ok(u), Ok(d)) => Some((u, d)),
            (u, d) => {
                tracing::warn!(
                    ?u,
                    ?d,
                    "lecture positions on-chain (merge) échouée — merge sauté ce tick"
                );
                None
            }
        }
    }

    /// Lance un MERGE on-chain de `pairs` paires (un seul en vol, cooldown 45 s).
    /// `WouldRevert` = la simulation du relayer refuse — quasi toujours parce que
    /// les paires sont DÉJÀ mergées on-chain (tx précédente passée malgré un
    /// timeout de suivi) → l'appelant doit aligner le miroir.
    pub async fn start_merge(&mut self, pairs: f64) -> MergeStart {
        self.last_merge_attempt_ms = chrono::Utc::now().timestamp_millis();
        let cond = self.condition_id.clone();
        let Some(r) = self.relayer.as_mut() else {
            return MergeStart::Err;
        };
        match r.merge(&cond, pairs).await {
            Ok(rx) => {
                self.merge_inflight = Some((pairs, rx));
                MergeStart::Submitted
            }
            Err(e) => {
                let msg = e.to_string();
                tracing::warn!(error = %msg, "merge relayer refusé");
                // Pénalité : échec relayer (429/refus) → prochaine tentative à +45 s.
                self.last_merge_attempt_ms = chrono::Utc::now().timestamp_millis() + 35_000;
                if msg.contains("would revert") || msg.contains("reverted") {
                    MergeStart::WouldRevert
                } else {
                    MergeStart::Err
                }
            }
        }
    }

    /// Merge confirmé ? → renvoie le nombre de paires à créditer au miroir.
    pub fn poll_merge_done(&mut self) -> Option<f64> {
        let (pairs, rx) = self.merge_inflight.as_mut()?;
        match rx.try_recv() {
            Ok(TxOutcome::Confirmed) => {
                let p = *pairs;
                self.merge_inflight = None;
                self.last_merge_attempt_ms = 0; // volume : merge suivant sans attendre
                self.last_cash_sync_ms = 0; // force le resync du collatéral
                self.positions_dirty = true;
                Some(p)
            }
            Ok(TxOutcome::Failed(e)) => {
                tracing::warn!(error = %e, "merge on-chain échoué — resync des positions réelles");
                self.merge_inflight = None;
                self.positions_dirty = true; // la vérité tranchera (mergé ou pas)
                None
            }
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => None,
            Err(_) => {
                self.merge_inflight = None;
                None
            }
        }
    }

    /// REDEEM (fin de fenêtre résolue) : brûle tout le solde de la condition,
    /// le pUSD revient au wallet (fire-and-forget, le sync cash suit).
    pub async fn redeem(&mut self, condition_id: &str) {
        if !self.armed {
            return;
        }
        if let Some(r) = self.relayer.as_mut() {
            match r.redeem(condition_id).await {
                Ok(_rx) => self.last_cash_sync_ms = 0,
                Err(e) => tracing::warn!(error = %e, "redeem relayer refusé"),
            }
        }
    }

    /// Pose un bid GTC. Renvoie l'ordre restant (ou None en dry-run/échec).
    /// `price` doit déjà être clampé sous l'ask par l'appelant (anti-cross).
    pub async fn place_bid(
        &mut self,
        is_up: bool,
        price: f64,
        size: f64,
        post_only: bool,
        intent: OrderIntent,
    ) -> Option<RestingOrder> {
        if self.is_halted() {
            tracing::warn!(
                intent = intent.as_str(),
                "ordre refusé : exécuteur live arrêté"
            );
            return None;
        }
        let token = if is_up {
            &self.up_token
        } else {
            &self.dn_token
        };
        let args = OrderArgs {
            price,
            size,
            is_sell: false,
            gtc: true,
            post_only,
        };
        match orders::place_order(self.armed, &self.creds, token, args).await {
            Ok(PlaceResult::Placed {
                order_id,
                filled_size,
                avg_price,
                post_ms,
            }) => {
                self.last_rtt_ms = post_ms;
                let condition_id = self.condition_id.clone();
                self.order_side.insert(
                    order_id.clone(),
                    OrderMeta {
                        is_up,
                        maker: true,
                        intent,
                        condition_id: condition_id.clone(),
                    },
                );
                // Enregistre l'ordre au grand livre (counted=0), puis crédite le
                // fill immédiat éventuel (GTC marketable) au prix RÉEL — taker.
                // Comme tout passe par `credit`, les events WS/poll ne
                // recompteront jamais cette portion (idempotence par order_id).
                let matched = filled_size.unwrap_or(0.0);
                let px = avg_price.filter(|p| *p > 0.0).unwrap_or(price);
                if let Some(f) = Self::credit_with_meta(
                    &mut self.ledger,
                    &order_id,
                    is_up,
                    size,
                    matched,
                    px,
                    false,
                    intent,
                    &condition_id,
                ) {
                    tracing::info!(
                        matched = f.size,
                        px,
                        "GTC fillé au POST (marketable) — compté (taker)"
                    );
                    self.post_fills.push(f);
                }
                let counted = self
                    .ledger
                    .get(&order_id)
                    .map(|e| e.counted)
                    .unwrap_or(matched);
                Some(RestingOrder {
                    order_id,
                    price,
                    size,
                    matched: counted,
                    placed_ms: chrono::Utc::now().timestamp_millis(),
                    intent,
                    condition_id,
                })
            }
            Ok(PlaceResult::PostOnlyRejected) | Ok(PlaceResult::DryRun) => None,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("post-only") || msg.contains("POST_ONLY") {
                    // Le CLOB renvoie le rejet post-only en HTTP 400 (pas dans le
                    // corps 200) : comportement VOULU, pas une erreur.
                    tracing::info!(
                        is_up,
                        price,
                        "post-only rejeté (aurait croisé) — repose au tick suivant"
                    );
                } else {
                    tracing::warn!(error = %msg, is_up, price, size, "place_bid refusé");
                }
                None
            }
        }
    }

    /// FLATTEN d'un résidu : VENTE FAK immédiate (exécution marché — la preuve
    /// du 9 juil. : une vente de 0,99 part passe, le plancher 5 parts ne
    /// s'applique qu'aux ordres RESTANTS). Récupère la valeur résiduelle au
    /// lieu du pile-ou-face à la résolution. Renvoie (parts vendues, prix moyen).
    pub async fn flatten_sell(
        &mut self,
        is_up: bool,
        size: f64,
        bid: f64,
        tick: f64,
    ) -> Option<(f64, f64)> {
        if !self.armed || bid <= 0.0 {
            return None;
        }
        let token = if is_up {
            self.up_token.clone()
        } else {
            self.dn_token.clone()
        };
        // Taille TRONQUÉE vers le bas (jamais vendre plus qu'on détient — leçon
        // mémoire : round_dp arrondirait 4.9958 → 5.00 → rejet balance).
        let sz = (size * 100.0).floor() / 100.0;
        if sz <= 0.0 {
            return None;
        }
        // Marketable : limite 2 ticks SOUS le bid pour balayer, plancher 1¢.
        let px = ((bid - 2.0 * tick).max(0.01) / tick).floor() * tick;
        // Prérequis sig_type 3 : rafraîchir l'allowance CONDITIONAL avant un SELL
        // (sinon rejet « balance 0 » — note mémoire).
        if let Err(e) =
            super::auth::sync_balance_allowance(&self.creds, "CONDITIONAL", Some(&token)).await
        {
            tracing::warn!(error = %e, "allowance CONDITIONAL avant SELL échouée — flatten sauté ce tick");
            return None;
        }
        let args = OrderArgs {
            price: px,
            size: sz,
            is_sell: true,
            gtc: false,
            post_only: false,
        };
        match orders::place_order(self.armed, &self.creds, &token, args).await {
            Ok(PlaceResult::Placed {
                order_id,
                filled_size,
                avg_price,
                post_ms,
            }) => {
                self.last_rtt_ms = post_ms;
                self.sell_ids.insert(order_id);
                let sold = filled_size.unwrap_or(0.0);
                if sold > 1e-9 {
                    let avg = avg_price.filter(|p| *p > 0.0).unwrap_or(px);
                    self.cash += avg * sold; // produit de la vente → wallet
                    Some((sold, avg))
                } else {
                    None
                }
            }
            Ok(_) => None,
            Err(e) => {
                tracing::warn!(error = %e, "flatten SELL refusé");
                None
            }
        }
    }

    /// FAK d'assurance (complétion taker fin de fenêtre). Un FAK n'est jamais
    /// resting : son fill est dans la RÉPONSE du POST — comptabilisé ici même,
    /// sans dépendre du WS (un FAK invisible relançait l'assurance en boucle).
    pub async fn place_insurance_fak(
        &mut self,
        is_up: bool,
        price: f64,
        size: f64,
        intent: OrderIntent,
    ) {
        if self.is_halted() {
            tracing::warn!(
                intent = intent.as_str(),
                "FAK refusé : exécuteur live arrêté"
            );
            return;
        }
        let token = if is_up {
            &self.up_token
        } else {
            &self.dn_token
        };
        let args = OrderArgs {
            price,
            size,
            is_sell: false,
            gtc: false,
            post_only: false,
        };
        match orders::place_order(self.armed, &self.creds, token, args).await {
            Ok(PlaceResult::Placed {
                order_id,
                filled_size,
                avg_price,
                post_ms,
            }) => {
                self.last_rtt_ms = post_ms;
                let condition_id = self.condition_id.clone();
                self.order_side.insert(
                    order_id.clone(),
                    OrderMeta {
                        is_up,
                        maker: false,
                        intent,
                        condition_id: condition_id.clone(),
                    },
                );
                let matched = filled_size.unwrap_or(0.0);
                let px = avg_price.filter(|p| *p > 0.0).unwrap_or(price);
                // NOTIONNEL FAK (13 juil. 18:20) : un BUY FAK dépense le montant
                // (size × limite) et rend PLUS de parts si le fill est meilleur
                // (12 @ limite 0.67 remplis à 0.64 → 12,5625 parts). La réponse
                // du POST est la vérité de CET ordre : elle relève le plafond du
                // grand livre — sinon part fantôme → désync → pause.
                if let Some(f) = Self::credit_with_meta(
                    &mut self.ledger,
                    &order_id,
                    is_up,
                    size.max(matched),
                    matched,
                    px,
                    false,
                    intent,
                    &condition_id,
                ) {
                    self.post_fills.push(f);
                }
            }
            Ok(_) => {} // DryRun ; PostOnlyRejected impossible (FAK, post_only=false)
            Err(e) => tracing::warn!(error = %e, "assurance FAK refusée"),
        }
    }

    /// Récolte avant annulation, puis ne libère l'exposition que lorsqu'un état
    /// terminal est confirmé. Un fill partiel n'autorise jamais à remplacer le
    /// reliquat : il peut encore être exécuté après le DELETE.
    pub async fn harvest_and_cancel(
        &mut self,
        r: &RestingOrder,
        is_up: bool,
    ) -> (Option<LiveFill>, bool) {
        let mut fill = None;
        if self.armed {
            match super::auth::l2_request(
                &self.creds,
                "GET",
                &format!("/data/order/{}", r.order_id),
                None,
                "",
            )
            .await
            {
                Ok(text) => {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                    let v = v.get("data").cloned().unwrap_or(v);
                    let matched: f64 = v
                        .get("size_matched")
                        .and_then(|s| s.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if let Some(f) = Self::credit_with_meta(
                        &mut self.ledger,
                        &r.order_id,
                        is_up,
                        r.size,
                        matched,
                        r.price,
                        true,
                        r.intent,
                        &r.condition_id,
                    ) {
                        tracing::info!(order_id = %r.order_id, delta = f.size, "fill récolté à l'annulation");
                        fill = Some(f);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, order_id = %r.order_id,
                        "récolte pré-annulation échouée → ordre mis en AUDIT (le poll retentera)");
                }
            }
            match orders::cancel_order(&self.creds, &r.order_id).await {
                Ok(CancelStatus::Cancelled) => return (fill, true),
                Ok(
                    CancelStatus::AlreadyClosed | CancelStatus::StillOpen | CancelStatus::Unknown,
                )
                | Err(_) => {
                    self.audit_order(r.clone(), is_up, true);
                    tracing::warn!(
                        order_id = %r.order_id.chars().take(12).collect::<String>(),
                        "cancel non terminal — côté gelé jusqu'à confirmation finale"
                    );
                    return (fill, false);
                }
            }
        }
        (fill, true)
    }

    pub async fn cancel_all(&self) {
        if self.armed && !self.condition_id.is_empty() {
            let _ = orders::cancel_market_orders(&self.creds, &self.condition_id).await;
        }
    }

    /// LE CHOKE-POINT de comptabilité. Crédite l'ordre `id` d'un `size_matched`
    /// ABSOLU et n'émet QUE le delta au-delà de ce qui est déjà compté. Rejouer
    /// le même montant (event ré-émis, autre canal) → delta 0. Plafonné à la
    /// taille d'origine de l'ordre. C'est ce qui rend le double comptage
    /// STRUCTURELLEMENT impossible (bug des −24 Down du 8 juil.).
    #[cfg_attr(not(test), allow(dead_code))]
    fn credit(
        ledger: &mut HashMap<String, LedgerEntry>,
        id: &str,
        is_up: bool,
        size_hint: f64,
        abs_matched: f64,
        price: f64,
        maker: bool,
    ) -> Option<LiveFill> {
        Self::credit_with_meta(
            ledger,
            id,
            is_up,
            size_hint,
            abs_matched,
            price,
            maker,
            OrderIntent::SymmetricOpen,
            "",
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn credit_with_meta(
        ledger: &mut HashMap<String, LedgerEntry>,
        id: &str,
        is_up: bool,
        size_hint: f64,
        abs_matched: f64,
        price: f64,
        maker: bool,
        intent: OrderIntent,
        condition_id: &str,
    ) -> Option<LiveFill> {
        let e = ledger.entry(id.to_string()).or_insert(LedgerEntry {
            is_up,
            // Taille connue de l'ordre = plafond dur. Un size_matched aberrant
            // (> taille) ne doit JAMAIS gonfler le plafond ; ce n'est qu'en
            // l'absence de taille connue (hint 0) qu'on se rabat sur l'absolu.
            size: if size_hint > 0.0 {
                size_hint
            } else {
                abs_matched
            },
            counted: 0.0,
            price,
            intent,
            condition_id: condition_id.to_string(),
        });
        // Une taille connue plus grande (hint fiable arrivé après) fait grandir
        // le plafond ; jamais un simple size_matched.
        if size_hint > e.size {
            e.size = size_hint;
        }
        // plafond dur : ne jamais compter au-delà de la taille de l'ordre.
        let target = abs_matched.min(e.size);
        if target > e.counted + 1e-9 {
            let delta = target - e.counted;
            e.counted = target;
            Some(LiveFill {
                is_up: e.is_up,
                price: if e.price > 0.0 { e.price } else { price },
                size: delta,
                maker,
                intent: e.intent,
                condition_id: e.condition_id.clone(),
            })
        } else {
            None
        }
    }

    /// Draine le WS user. Les events `order` portent le `size_matched` ABSOLU de
    /// NOS ordres → seule voie de comptabilité temps réel (via `credit`). Les
    /// events `trade` sont IGNORÉS pour la compta : le serveur les ré-émet par
    /// statut (MATCHED/MINED/CONFIRMED) et ils portent des montants PAR TRADE —
    /// les compter quadruplait l'inventaire (23:33 le 8 juil.). Le poll et le
    /// POST couvrent tout fill qu'un event `order` raterait, avec le même absolu.
    pub fn drain_ws(&mut self, rest: &mut Vec<LiveSlot>) -> Vec<LiveFill> {
        let mut out = std::mem::take(&mut self.post_fills);
        while let Ok(u) = self.fill_rx.try_recv() {
            if u.size_matched <= 0.0 {
                continue;
            }
            // ATTRIBUTION STRICTE : on ne crédite que les ACHATS QUE NOUS avons
            // posés. Une VENTE (flatten) ou un ordre MANUEL de l'utilisateur sur
            // le même compte serait sinon compté comme un achat du bot
            // (inventaire fantôme — famille du 8 juil.).
            if self.sell_ids.contains(&u.order_id) {
                continue;
            }
            let Some(meta) = self.order_side.get(&u.order_id).cloned() else {
                tracing::info!(order_id = %u.order_id.chars().take(12).collect::<String>(),
                    "event order EXTERNE (manuel/vente ?) — ignoré par la compta bot");
                continue;
            };
            if meta.condition_id != self.condition_id {
                tracing::warn!(order_id = %u.order_id, event_market = %meta.condition_id,
                    active_market = %self.condition_id, "fill tardif d'un ancien marché ignoré");
                continue;
            }
            let is_up = if !u.asset_id.is_empty() {
                u.asset_id == self.up_token
            } else {
                meta.is_up
            };
            if let Some(f) = Self::credit_with_meta(
                &mut self.ledger,
                &u.order_id,
                is_up,
                u.size,
                u.size_matched,
                u.price,
                meta.maker,
                meta.intent,
                &meta.condition_id,
            ) {
                tracing::info!(order_id = %u.order_id, delta = f.size,
                    "fill (event order — size_matched absolu, temps réel)");
                out.push(f);
            }
        }
        // Slots : matched recopié du grand livre + libération des complets.
        for s in rest.iter_mut() {
            if let Some(e) = self.ledger.get(&s.r.order_id) {
                s.r.matched = e.counted;
            }
        }
        rest.retain(|s| s.r.matched < s.r.size - 1e-6);
        out
    }

    /// Réconciliation par poll (autorité, ~1×/3 s). Le CLOB donne le
    /// `size_matched` ABSOLU de chaque ordre → on le passe à `credit` : il émet
    /// le delta manqué par le WS, ou rien s'il est déjà compté. Même absolu, même
    /// choke-point que le temps réel → jamais de double comptage entre les voies.
    pub async fn reconcile(&mut self, rest: &mut Vec<LiveSlot>, now_ms: i64) -> Vec<LiveFill> {
        let mut out = Vec::new();
        if !self.armed || self.condition_id.is_empty() || now_ms - self.last_poll_ms < 3_000 {
            return out;
        }
        self.last_poll_ms = now_ms;
        // AUDIT d'abord : les ordres à lecture ratée sont réinterrogés jusqu'à
        // vérité connue (un ordre clos reste consultable sur /data/order).
        let mut audit = std::mem::take(&mut self.audit);
        for entry in &mut audit {
            if now_ms.saturating_sub(entry.r.placed_ms) > self.audit_max_age_ms {
                self.halt(format!(
                    "audit/cancel non terminal depuis plus de {} ms",
                    self.audit_max_age_ms
                ));
            }
            match super::auth::l2_request(
                &self.creds,
                "GET",
                &format!("/data/order/{}", entry.r.order_id),
                None,
                "",
            )
            .await
            {
                Ok(text) => {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                    let v = v.get("data").cloned().unwrap_or(v);
                    let matched: f64 = v
                        .get("size_matched")
                        .and_then(|s| s.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if let Some(f) = Self::credit_with_meta(
                        &mut self.ledger,
                        &entry.r.order_id,
                        entry.is_up,
                        entry.r.size,
                        matched,
                        entry.r.price,
                        true,
                        entry.r.intent,
                        &entry.r.condition_id,
                    ) {
                        tracing::info!(order_id = %entry.r.order_id, delta = f.size, "fill récupéré par AUDIT");
                        out.push(f);
                    }
                    match order_terminal_state(&v) {
                        OrderTerminalState::Terminal => entry.tries = u32::MAX,
                        OrderTerminalState::Open if entry.cancel_requested => {
                            match orders::cancel_order(&self.creds, &entry.r.order_id).await {
                                Ok(CancelStatus::Cancelled) => entry.tries = u32::MAX,
                                _ => entry.tries += 1,
                            }
                        }
                        OrderTerminalState::Open | OrderTerminalState::Unknown => entry.tries += 1,
                    }
                }
                Err(e) => {
                    entry.tries += 1;
                    if entry.tries >= 40 {
                        tracing::error!(order_id = %entry.r.order_id, error = %e,
                            "AUDIT non résolu après 40 échecs — exécution live arrêtée");
                        self.halt("audit/cancel non terminal après 40 tentatives");
                    }
                }
            }
        }
        audit.retain(|entry| entry.tries != u32::MAX);
        self.audit = audit;
        // Pause de sécurité : reprise automatique dès que l'audit est vide.
        self.try_resume(now_ms);
        let t0 = std::time::Instant::now();
        let open = match orders::open_orders(&self.creds, &self.condition_id).await {
            Ok(o) => {
                self.last_rtt_ms = t0.elapsed().as_millis() as u64;
                o
            }
            Err(e) => {
                tracing::warn!(error = %e, "poll /data/orders échoué");
                return out;
            }
        };
        let mut tracked_ids: Vec<String> =
            self.audit.iter().map(|a| a.r.order_id.clone()).collect();
        tracked_ids.extend(rest.iter().map(|s| s.r.order_id.clone()));
        let mut new_audit: Vec<AuditOrder> = Vec::new();
        let mut i = 0;
        while i < rest.len() {
            let (order_id, size, price, matched0, placed_ms, is_up, intent, condition_id) = {
                let s = &rest[i];
                (
                    s.r.order_id.clone(),
                    s.r.size,
                    s.r.price,
                    s.r.matched,
                    s.r.placed_ms,
                    s.is_up,
                    s.intent,
                    s.r.condition_id.clone(),
                )
            };
            match open.iter().find(|o| o.id == order_id) {
                Some(o) => {
                    if let Some(f) = Self::credit_with_meta(
                        &mut self.ledger,
                        &order_id,
                        is_up,
                        size,
                        o.matched(),
                        price,
                        true,
                        intent,
                        &condition_id,
                    ) {
                        tracing::info!(order_id = %order_id, delta = f.size, "fill rattrapé par le poll");
                        out.push(f);
                    }
                    if let Some(e) = self.ledger.get(&order_id) {
                        rest[i].r.matched = e.counted;
                    }
                    i += 1;
                }
                None => {
                    // LAG D'INDEXATION (8 juil. 18:16) : un ordre posté il y a
                    // <10 s peut ne pas encore apparaître dans /data/orders —
                    // le slot libéré à tort = ordre DOUBLE posé par-dessus.
                    if now_ms - placed_ms < 10_000 {
                        i += 1;
                        continue;
                    }
                    // Absent du carnet = fillé en entier ou annulé : on lit son
                    // size_matched final et on crédite le reliquat éventuel.
                    match super::auth::l2_request(
                        &self.creds,
                        "GET",
                        &format!("/data/order/{}", order_id),
                        None,
                        "",
                    )
                    .await
                    {
                        Ok(text) => {
                            let v: serde_json::Value =
                                serde_json::from_str(&text).unwrap_or_default();
                            let v = v.get("data").cloned().unwrap_or(v);
                            let matched: f64 = v
                                .get("size_matched")
                                .and_then(|s| s.as_str())
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            if let Some(f) = Self::credit_with_meta(
                                &mut self.ledger,
                                &order_id,
                                is_up,
                                size,
                                matched,
                                price,
                                true,
                                intent,
                                &condition_id,
                            ) {
                                tracing::info!(order_id = %order_id, delta = f.size, "fill final rattrapé (ordre clos)");
                                out.push(f);
                            }
                            if order_terminal_state(&v) != OrderTerminalState::Terminal {
                                new_audit.push(AuditOrder {
                                    r: RestingOrder {
                                        order_id: order_id.clone(),
                                        price,
                                        size,
                                        matched: matched0,
                                        placed_ms,
                                        intent,
                                        condition_id,
                                    },
                                    is_up,
                                    tries: 0,
                                    cancel_requested: false,
                                });
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, order_id = %order_id,
                                "GET /data/order échoué (ordre absent du poll) → AUDIT");
                            new_audit.push(AuditOrder {
                                r: RestingOrder {
                                    order_id: order_id.clone(),
                                    price,
                                    size,
                                    matched: matched0,
                                    placed_ms,
                                    intent,
                                    condition_id,
                                },
                                is_up,
                                tries: 0,
                                cancel_requested: false,
                            });
                        }
                    }
                    rest.remove(i); // l'ordre n'existe plus → re-quote au tick suivant
                }
            }
        }
        self.audit.extend(new_audit);
        // BALAYAGE ANTI DOUBLE-ORDRE : tout ordre OUVERT au carnet inconnu des
        // slots/audit → annulé, et crédité via le grand livre (donc jamais
        // recompté s'il y était déjà via son POST).
        for o in &open {
            if tracked_ids.iter().any(|t| t == &o.id) {
                continue;
            }
            let meta = self.order_side.get(&o.id).cloned().unwrap_or(OrderMeta {
                is_up: o.asset_id == self.up_token,
                maker: true,
                intent: OrderIntent::SymmetricOpen,
                condition_id: self.condition_id.clone(),
            });
            let is_up = meta.is_up;
            if meta.intent == OrderIntent::Completion
                && rest.iter().any(|slot| {
                    slot.is_up == is_up
                        && slot.intent == OrderIntent::Completion
                        && slot.r.order_id != o.id
                })
            {
                self.halt(format!(
                    "double complétion détectée côté {}",
                    if is_up { "Up" } else { "Down" }
                ));
            }
            tracing::error!(order_id = %o.id, is_up,
                "ordre OUVERT au carnet INCONNU du suivi — récolté puis cancel demandé");
            let px = o.price.parse().unwrap_or(0.0);
            let sz = o.original_size.parse().unwrap_or(0.0);
            if let Some(f) = Self::credit_with_meta(
                &mut self.ledger,
                &o.id,
                is_up,
                sz,
                o.matched(),
                px,
                meta.maker,
                meta.intent,
                &meta.condition_id,
            ) {
                out.push(f);
            }
            let r = RestingOrder {
                order_id: o.id.clone(),
                price: px,
                size: sz,
                matched: o.matched(),
                placed_ms: now_ms,
                intent: meta.intent,
                condition_id: meta.condition_id,
            };
            match orders::cancel_order(&self.creds, &o.id).await {
                Ok(CancelStatus::Cancelled) => {
                    tracing::info!(order_id = %o.id, "ordre inconnu annulé confirmé");
                }
                _ => self.audit_order(r, is_up, true),
            }
        }
        out.retain(|f| f.size > 1e-9);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Le bug du 8 juil. 23:33 : un ordre Down de 6 parts, re-signalé par le WS à
    // chaque statut (MATCHED/MINED/CONFIRMED) et par plusieurs canaux, compté 4×
    // → inventaire fantôme. `credit` doit rendre ce scénario IMPOSSIBLE.
    #[test]
    fn credit_is_idempotent_across_repeats_and_channels() {
        let mut l: HashMap<String, LedgerEntry> = HashMap::new();
        let mut total = 0.0;
        // même size_matched absolu (6) émis 4 fois (statuts + canaux) :
        for _ in 0..4 {
            if let Some(f) = LiveCtx::credit(&mut l, "ord-A", false, 6.0, 6.0, 0.80, true) {
                total += f.size;
            }
        }
        assert!(
            (total - 6.0).abs() < 1e-9,
            "6 parts comptées 1×, pas 24 : {total}"
        );
    }

    #[test]
    fn credit_emits_only_incremental_deltas() {
        let mut l: HashMap<String, LedgerEntry> = HashMap::new();
        // fills partiels cumulatifs 3 → 5 → 5 (répété) → 6
        let mut total = 0.0;
        for m in [3.0, 5.0, 5.0, 6.0] {
            if let Some(f) = LiveCtx::credit(&mut l, "ord-B", true, 6.0, m, 0.50, true) {
                total += f.size;
            }
        }
        assert!((total - 6.0).abs() < 1e-9, "somme des deltas = 6 : {total}");
    }

    #[test]
    fn credit_post_fill_then_order_event_no_double() {
        let mut l: HashMap<String, LedgerEntry> = HashMap::new();
        // POST : 6 fillés au marché (taker)
        let post = LiveCtx::credit(&mut l, "ord-C", true, 6.0, 6.0, 0.56, false).unwrap();
        assert!((post.size - 6.0).abs() < 1e-9);
        assert!(!post.maker);
        // l'event `order` rapporte le même size_matched absolu (6) → rien de plus
        assert!(LiveCtx::credit(&mut l, "ord-C", true, 6.0, 6.0, 0.56, true).is_none());
    }

    #[test]
    fn credit_fak_notional_overfill_counted_fully() {
        // BUY FAK : limite 0.67, rempli 12.5625 @ 0.64 (notionnel). Le POST
        // relève le plafond → tout est compté ; les ré-émissions WS = delta 0.
        let mut l = HashMap::new();
        let f = LiveCtx::credit(&mut l, "oid", false, 12.5625, 12.5625, 0.64, false)
            .expect("fill");
        assert!((f.size - 12.5625).abs() < 1e-9, "tout le fill réel: {}", f.size);
        assert!(LiveCtx::credit(&mut l, "oid", false, 12.0, 12.5625, 0.64, false).is_none());
    }

    #[test]
    fn credit_never_exceeds_order_size() {
        let mut l: HashMap<String, LedgerEntry> = HashMap::new();
        // un size_matched aberrant (> taille) est plafonné à la taille de l'ordre
        let f = LiveCtx::credit(&mut l, "ord-D", true, 6.0, 99.0, 0.10, true).unwrap();
        assert!((f.size - 6.0).abs() < 1e-9, "plafonné à 6 : {}", f.size);
    }

    #[test]
    fn credit_preserves_order_intent_across_channels() {
        let mut l: HashMap<String, LedgerEntry> = HashMap::new();
        let first = LiveCtx::credit_with_meta(
            &mut l,
            "completion-1",
            false,
            6.0,
            3.0,
            0.42,
            true,
            OrderIntent::Completion,
            "market-a",
        )
        .unwrap();
        assert_eq!(first.intent, OrderIntent::Completion);
        assert_eq!(first.condition_id, "market-a");
        let second = LiveCtx::credit_with_meta(
            &mut l,
            "completion-1",
            false,
            6.0,
            6.0,
            0.42,
            true,
            OrderIntent::SymmetricOpen,
            "market-b",
        )
        .unwrap();
        assert_eq!(second.intent, OrderIntent::Completion);
        assert_eq!(second.condition_id, "market-a");
    }

    #[test]
    fn audit_exposure_blocks_replacement_at_remaining_size() {
        let r = RestingOrder {
            order_id: "audit-1".into(),
            price: 0.27,
            size: 6.0,
            matched: 1.8,
            placed_ms: 0,
            intent: OrderIntent::Completion,
            condition_id: "market-a".into(),
        };
        let audit = vec![AuditOrder {
            r,
            is_up: false,
            tries: 0,
            cancel_requested: true,
        }];
        let exposure = aggregate_open_exposure(&[], &audit);
        assert!((exposure.down - 4.2).abs() < 1e-9);
        assert!((exposure.completion_down - 4.2).abs() < 1e-9);
        assert!((exposure.uncertain_down - 4.2).abs() < 1e-9);
    }

    #[test]
    fn terminal_status_requires_an_explicit_terminal_value() {
        let open = serde_json::json!({ "status": "LIVE" });
        let closed = serde_json::json!({ "status": "CANCELED" });
        let unknown = serde_json::json!({ "size_matched": "6" });
        assert_eq!(order_terminal_state(&open), OrderTerminalState::Open);
        assert_eq!(order_terminal_state(&closed), OrderTerminalState::Terminal);
        assert_eq!(order_terminal_state(&unknown), OrderTerminalState::Unknown);
    }
}
