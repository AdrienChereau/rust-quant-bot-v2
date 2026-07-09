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

use tokio::sync::{mpsc, watch};

use super::auth::LiveCredentials;
use super::orders::{self, OrderArgs, PlaceResult};
use super::relayer::{RelayerCtx, TxOutcome};

/// Résultat du lancement d'un merge.
#[derive(Debug, PartialEq)]
pub enum MergeStart {
    Submitted,
    WouldRevert, // déjà mergé on-chain (probable) → aligner le miroir
    Err,
}
use super::user_ws::{self, OrderUpdate};

/// Fill prêt pour la boucle stratégie.
#[derive(Debug, Clone)]
pub struct LiveFill {
    pub is_up: bool,
    pub price: f64,
    pub size: f64,
    pub maker: bool,
}

/// Un de NOS ordres restants.
#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub order_id: String,
    pub price: f64,
    pub size: f64,
    pub matched: f64,   // cumul fillé déjà comptabilisé (recopié du grand livre)
    pub placed_ms: i64, // epoch ms du POST — un ordre trop frais peut être absent du poll (lag d'indexation)
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
    // ordre_id → (is_up, maker) — secours d'attribution quand l'asset_id manque
    order_side: HashMap<String, (bool, bool)>,
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
    /// Fills constatés à la RÉPONSE du POST (GTC marketable) : émis au prix
    /// moyen réellement exécuté, dès le tick courant (le journal doit montrer
    /// le prix PAYÉ, pas notre limite — écarts 45,0 vs 45,7 du 8 juil.).
    post_fills: Vec<LiveFill>,
    /// Ordres retirés SANS lecture réussie de leur size_matched (GET /data/order
    /// en échec au moment du cancel). Un fill ne doit JAMAIS être perdu : le poll
    /// réinterroge ces ordres jusqu'à obtenir la vérité (incident du 8 juil. :
    /// Up 5@56¢ fillé, lecture ratée, ordre oublié → fausse complétion à 62¢).
    audit: Vec<(RestingOrder, bool, u32)>, // (ordre, is_up, tentatives)
}

impl LiveCtx {
    /// Démarrage : auth SDK + sync allowance + WS user + heartbeats (dead-man).
    pub async fn start(creds: LiveCredentials, armed: bool) -> anyhow::Result<Self> {
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
            post_fills: Vec::new(),
            audit: Vec::new(),
        })
    }

    /// Rollover de fenêtre : annule tout sur l'ancien marché, bascule le WS user.
    pub async fn on_new_market(&mut self, condition_id: &str, up_token: &str, dn_token: &str) {
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
                tracing::warn!(?u, ?d, "lecture positions on-chain (merge) échouée — merge sauté ce tick");
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
        let Some(r) = self.relayer.as_mut() else { return MergeStart::Err };
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
    ) -> Option<RestingOrder> {
        let token = if is_up { &self.up_token } else { &self.dn_token };
        let args = OrderArgs { price, size, is_sell: false, gtc: true };
        match orders::place_order(self.armed, &self.creds, token, args).await {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, .. }) => {
                self.order_side.insert(order_id.clone(), (is_up, true));
                // Enregistre l'ordre au grand livre (counted=0), puis crédite le
                // fill immédiat éventuel (GTC marketable) au prix RÉEL — taker.
                // Comme tout passe par `credit`, les events WS/poll ne
                // recompteront jamais cette portion (idempotence par order_id).
                let matched = filled_size.unwrap_or(0.0);
                let px = avg_price.filter(|p| *p > 0.0).unwrap_or(price);
                if let Some(f) =
                    Self::credit(&mut self.ledger, &order_id, is_up, size, matched, px, false)
                {
                    tracing::info!(matched = f.size, px, "GTC fillé au POST (marketable) — compté (taker)");
                    self.post_fills.push(f);
                }
                let counted = self.ledger.get(&order_id).map(|e| e.counted).unwrap_or(matched);
                Some(RestingOrder { order_id, price, size, matched: counted, placed_ms: chrono::Utc::now().timestamp_millis() })
            }
            Ok(PlaceResult::DryRun) => None,
            Err(e) => {
                tracing::warn!(error = %e, is_up, price, size, "place_bid refusé");
                None
            }
        }
    }

    /// FAK d'assurance (complétion taker fin de fenêtre). Un FAK n'est jamais
    /// resting : son fill est dans la RÉPONSE du POST — comptabilisé ici même,
    /// sans dépendre du WS (un FAK invisible relançait l'assurance en boucle).
    pub async fn place_insurance_fak(&mut self, is_up: bool, price: f64, size: f64) {
        let token = if is_up { &self.up_token } else { &self.dn_token };
        let args = OrderArgs { price, size, is_sell: false, gtc: false };
        match orders::place_order(self.armed, &self.creds, token, args).await {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, .. }) => {
                self.order_side.insert(order_id.clone(), (is_up, false));
                let matched = filled_size.unwrap_or(0.0);
                let px = avg_price.filter(|p| *p > 0.0).unwrap_or(price);
                if let Some(f) =
                    Self::credit(&mut self.ledger, &order_id, is_up, size, matched, px, false)
                {
                    self.post_fills.push(f);
                }
            }
            Ok(PlaceResult::DryRun) => {}
            Err(e) => tracing::warn!(error = %e, "assurance FAK refusée"),
        }
    }

    pub async fn cancel(&self, order_id: &str) {
        if self.armed {
            let _ = orders::cancel_order(&self.creds, order_id).await;
        }
    }

    /// TROU CRITIQUE corrigé (7 juil. : −21$ invisibles) : avant d'annuler un
    /// ordre (reprice/retrait/rollover), on RÉCOLTE son size_matched — tout
    /// fill partiel survenu pendant sa vie est comptabilisé, plus jamais perdu.
    pub async fn harvest_and_cancel(&mut self, r: &RestingOrder, is_up: bool) -> Option<LiveFill> {
        let mut fill = None;
        if self.armed {
            match super::auth::l2_request(
                &self.creds, "GET", &format!("/data/order/{}", r.order_id), None, "",
            ).await {
                Ok(text) => {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                    let v = v.get("data").cloned().unwrap_or(v);
                    let matched: f64 = v.get("size_matched")
                        .and_then(|s| s.as_str()).and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if let Some(f) = Self::credit(
                        &mut self.ledger, &r.order_id, is_up, r.size, matched, r.price, true,
                    ) {
                        tracing::info!(order_id = %r.order_id, delta = f.size, "fill récolté à l'annulation");
                        fill = Some(f);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, order_id = %r.order_id,
                        "récolte pré-annulation échouée → ordre mis en AUDIT (le poll retentera)");
                    self.audit.push((r.clone(), is_up, 0));
                }
            }
            let _ = orders::cancel_order(&self.creds, &r.order_id).await;
        }
        fill
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
    fn credit(
        ledger: &mut HashMap<String, LedgerEntry>,
        id: &str,
        is_up: bool,
        size_hint: f64,
        abs_matched: f64,
        price: f64,
        maker: bool,
    ) -> Option<LiveFill> {
        let e = ledger.entry(id.to_string()).or_insert(LedgerEntry {
            is_up,
            // Taille connue de l'ordre = plafond dur. Un size_matched aberrant
            // (> taille) ne doit JAMAIS gonfler le plafond ; ce n'est qu'en
            // l'absence de taille connue (hint 0) qu'on se rabat sur l'absolu.
            size: if size_hint > 0.0 { size_hint } else { abs_matched },
            counted: 0.0,
            price,
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
    pub fn drain_ws(
        &mut self,
        rest_up: &mut Option<RestingOrder>,
        rest_dn: &mut Option<RestingOrder>,
    ) -> Vec<LiveFill> {
        let mut out = std::mem::take(&mut self.post_fills);
        while let Ok(u) = self.fill_rx.try_recv() {
            if u.size_matched <= 0.0 {
                continue;
            }
            // Côté déduit de l'asset_id (robuste), secours order_side.
            let is_up = if !u.asset_id.is_empty() {
                u.asset_id == self.up_token
            } else {
                self.order_side.get(&u.order_id).map(|(up, _)| *up).unwrap_or(false)
            };
            if let Some(f) = Self::credit(
                &mut self.ledger, &u.order_id, is_up, u.size, u.size_matched, u.price, true,
            ) {
                tracing::info!(order_id = %u.order_id, delta = f.size,
                    "fill (event order — size_matched absolu, temps réel)");
                out.push(f);
            }
        }
        // Slots : matched recopié du grand livre + libération si complet.
        for rest in [rest_up, rest_dn] {
            if let Some(r) = rest.as_mut() {
                if let Some(e) = self.ledger.get(&r.order_id) {
                    r.matched = e.counted;
                }
            }
            if rest.as_ref().is_some_and(|r| r.matched >= r.size - 1e-6) {
                *rest = None;
            }
        }
        out
    }

    /// Réconciliation par poll (autorité, ~1×/3 s). Le CLOB donne le
    /// `size_matched` ABSOLU de chaque ordre → on le passe à `credit` : il émet
    /// le delta manqué par le WS, ou rien s'il est déjà compté. Même absolu, même
    /// choke-point que le temps réel → jamais de double comptage entre les voies.
    pub async fn reconcile(
        &mut self,
        rest_up: &mut Option<RestingOrder>,
        rest_dn: &mut Option<RestingOrder>,
        now_ms: i64,
    ) -> Vec<LiveFill> {
        let mut out = Vec::new();
        if !self.armed || self.condition_id.is_empty() || now_ms - self.last_poll_ms < 3_000 {
            return out;
        }
        self.last_poll_ms = now_ms;
        // AUDIT d'abord : les ordres à lecture ratée sont réinterrogés jusqu'à
        // vérité connue (un ordre clos reste consultable sur /data/order).
        let mut audit = std::mem::take(&mut self.audit);
        for (r, is_up, tries) in &mut audit {
            match super::auth::l2_request(
                &self.creds, "GET", &format!("/data/order/{}", r.order_id), None, "",
            ).await {
                Ok(text) => {
                    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                    let v = v.get("data").cloned().unwrap_or(v);
                    let matched: f64 = v.get("size_matched")
                        .and_then(|s| s.as_str()).and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if let Some(f) = Self::credit(
                        &mut self.ledger, &r.order_id, *is_up, r.size, matched, r.price, true,
                    ) {
                        tracing::info!(order_id = %r.order_id, delta = f.size, "fill récupéré par AUDIT");
                        out.push(f);
                    }
                    *tries = u32::MAX; // tranché → sortie de l'audit
                }
                Err(e) => {
                    *tries += 1;
                    if *tries >= 40 {
                        tracing::error!(order_id = %r.order_id, error = %e,
                            "AUDIT abandonné après 40 échecs — filet = resync positions on-chain (60 s)");
                        *tries = u32::MAX;
                    }
                }
            }
        }
        audit.retain(|(_, _, t)| *t != u32::MAX);
        self.audit = audit;
        let open = match orders::open_orders(&self.creds, &self.condition_id).await {
            Ok(o) => o,
            Err(e) => {
                tracing::warn!(error = %e, "poll /data/orders échoué");
                return out;
            }
        };
        let mut tracked_ids: Vec<String> = self.audit.iter().map(|(o, _, _)| o.order_id.clone()).collect();
        tracked_ids.extend(rest_up.iter().map(|r| r.order_id.clone()));
        tracked_ids.extend(rest_dn.iter().map(|r| r.order_id.clone()));
        let mut new_audit: Vec<(RestingOrder, bool, u32)> = Vec::new();
        for (rest, is_up) in [(rest_up, true), (rest_dn, false)] {
            let Some(r) = rest.as_mut() else { continue };
            match open.iter().find(|o| o.id == r.order_id) {
                Some(o) => {
                    if let Some(f) = Self::credit(
                        &mut self.ledger, &r.order_id, is_up, r.size, o.matched(), r.price, true,
                    ) {
                        tracing::info!(order_id = %r.order_id, delta = f.size, "fill rattrapé par le poll");
                        out.push(f);
                    }
                    if let Some(e) = self.ledger.get(&r.order_id) {
                        r.matched = e.counted;
                    }
                }
                None => {
                    // LAG D'INDEXATION (8 juil. 18:16) : un ordre posté il y a
                    // <10 s peut ne pas encore apparaître dans /data/orders —
                    // le slot libéré à tort = ordre DOUBLE posé par-dessus.
                    if now_ms - r.placed_ms < 10_000 {
                        continue;
                    }
                    // Absent du carnet = fillé en entier ou annulé : on lit son
                    // size_matched final et on crédite le reliquat éventuel.
                    match super::auth::l2_request(
                        &self.creds, "GET", &format!("/data/order/{}", r.order_id), None, "",
                    ).await {
                        Ok(text) => {
                            let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                            let v = v.get("data").cloned().unwrap_or(v);
                            let matched: f64 = v.get("size_matched")
                                .and_then(|s| s.as_str()).and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            if let Some(f) = Self::credit(
                                &mut self.ledger, &r.order_id, is_up, r.size, matched, r.price, true,
                            ) {
                                tracing::info!(order_id = %r.order_id, delta = f.size, "fill final rattrapé (ordre clos)");
                                out.push(f);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, order_id = %r.order_id,
                                "GET /data/order échoué (ordre clos) → AUDIT");
                            new_audit.push((r.clone(), is_up, 0));
                        }
                    }
                    *rest = None; // l'ordre n'existe plus → re-quote au tick suivant
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
            let is_up = self
                .order_side
                .get(&o.id)
                .map(|(u, _)| *u)
                .unwrap_or(o.asset_id == self.up_token);
            tracing::error!(order_id = %o.id, is_up,
                "ordre OUVERT au carnet INCONNU du suivi — annulé + crédité");
            let px = o.price.parse().unwrap_or(0.0);
            let sz = o.original_size.parse().unwrap_or(0.0);
            if let Some(f) = Self::credit(&mut self.ledger, &o.id, is_up, sz, o.matched(), px, true) {
                out.push(f);
            }
            let _ = orders::cancel_order(&self.creds, &o.id).await;
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
        assert!((total - 6.0).abs() < 1e-9, "6 parts comptées 1×, pas 24 : {total}");
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
    fn credit_never_exceeds_order_size() {
        let mut l: HashMap<String, LedgerEntry> = HashMap::new();
        // un size_matched aberrant (> taille) est plafonné à la taille de l'ordre
        let f = LiveCtx::credit(&mut l, "ord-D", true, 6.0, 99.0, 0.10, true).unwrap();
        assert!((f.size - 6.0).abs() < 1e-9, "plafonné à 6 : {}", f.size);
    }
}
