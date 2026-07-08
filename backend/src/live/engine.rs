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
use super::user_ws::{self, UserMsg};

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
    pub matched: f64,   // cumul fillé déjà comptabilisé
    pub placed_ms: i64, // epoch ms du POST — un ordre trop frais peut être absent du poll (lag d'indexation)
}

pub struct LiveCtx {
    pub creds: LiveCredentials,
    pub armed: bool,
    cond_tx: watch::Sender<Option<String>>,
    fill_rx: mpsc::UnboundedReceiver<UserMsg>,
    // token_ids du marché courant (associe asset_id/ordre → côté Up/Down)
    up_token: String,
    dn_token: String,
    condition_id: String,
    // ordre_id → (is_up, maker) pour attribuer les fills WS
    order_side: HashMap<String, (bool, bool)>,
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
                // Fill immédiat (GTC marketable malgré le clamp) : émis TOUT DE
                // SUITE au prix moyen réel, et inscrit dans la baseline matched
                // (les canaux WS/poll ne le recompteront pas).
                let matched = filled_size.unwrap_or(0.0);
                if matched > 0.0 {
                    let px = avg_price.filter(|p| *p > 0.0).unwrap_or(price);
                    tracing::info!(matched, px, "GTC fillé au POST (marketable) — compté immédiatement");
                    self.post_fills.push(LiveFill { is_up, price: px, size: matched, maker: false });
                }
                Some(RestingOrder { order_id, price, size, matched, placed_ms: chrono::Utc::now().timestamp_millis() })
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
                self.order_side.insert(order_id, (is_up, false));
                let matched = filled_size.unwrap_or(0.0);
                if matched > 0.0 {
                    let px = avg_price.filter(|p| *p > 0.0).unwrap_or(price);
                    self.post_fills.push(LiveFill { is_up, price: px, size: matched, maker: false });
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
                    if matched > r.matched + 1e-9 {
                        let delta = matched - r.matched;
                        tracing::info!(order_id = %r.order_id, delta, "fill récolté à l'annulation");
                        fill = Some(LiveFill { is_up, price: r.price, size: delta, maker: true });
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

    /// Draine le WS user : fills (`trade`) ET états d'ordres (`order`,
    /// size_matched ABSOLU temps réel). Toute émission de fill d'un ordre
    /// SUIVI passe par sa baseline `matched` — un même fill signalé par
    /// plusieurs canaux (trade, order, poll, récolte) ne compte qu'UNE fois.
    pub fn drain_ws(
        &mut self,
        rest_up: &mut Option<RestingOrder>,
        rest_dn: &mut Option<RestingOrder>,
    ) -> Vec<LiveFill> {
        let mut out = std::mem::take(&mut self.post_fills);
        while let Ok(msg) = self.fill_rx.try_recv() {
            match msg {
                UserMsg::Fill(f) => {
                    // Attribution STRICTE par order_id connu : l'event trade contient
                    // aussi l'ordre du taker ADVERSE — un fallback par asset_id
                    // compterait son fill comme le nôtre (double comptage).
                    match self.order_side.get(&f.order_id).copied() {
                        Some((is_up, maker)) if !f.is_sell => {
                            match Self::find_tracked(rest_up, rest_dn, &mut self.audit, &f.order_id) {
                                Some(r) => {
                                    // Borné au restant de l'ordre : idempotent.
                                    let allowed = (r.size - r.matched).max(0.0).min(f.size);
                                    if allowed > 1e-9 {
                                        r.matched += allowed;
                                        out.push(LiveFill { is_up, price: f.price, size: allowed, maker });
                                    }
                                }
                                None if !maker => {
                                    // FAK taker : jamais resting, le trade est l'unique canal.
                                    out.push(LiveFill { is_up, price: f.price, size: f.size, maker });
                                }
                                None => {
                                    // Maker connu mais suivi NULLE PART = fuite. On compte
                                    // ce fill et on ouvre une baseline d'audit pour la suite.
                                    tracing::error!(order_id = %f.order_id,
                                        "fill d'un maker NON SUIVI — compté + mis en AUDIT");
                                    self.audit.push((RestingOrder {
                                        order_id: f.order_id.clone(),
                                        price: f.price,
                                        size: 1e9, // taille réelle inconnue : pas de cap
                                        matched: f.size,
                                        placed_ms: 0,
                                    }, is_up, 0));
                                    out.push(LiveFill { is_up, price: f.price, size: f.size, maker });
                                }
                            }
                        }
                        Some(_) => tracing::warn!(order_id = %f.order_id, "fill SELL inattendu (ignoré)"),
                        None => tracing::debug!(order_id = %f.order_id, "fill d'un ordre inconnu (ignoré)"),
                    }
                }
                UserMsg::Order(u) => {
                    // Surveillance temps réel (8 juil.) : le size_matched ABSOLU de
                    // l'event tranche IMMÉDIATEMENT — plus d'attente du poll 3 s.
                    let Some((is_up, maker)) = self.order_side.get(&u.order_id).copied() else {
                        if u.kind != "CANCELLATION" && u.size_matched > 0.0 {
                            tracing::warn!(order_id = %u.order_id, kind = %u.kind,
                                "event order d'un ordre INCONNU (autre process ?)");
                        }
                        continue;
                    };
                    if !maker {
                        continue; // FAK : compté par son event trade uniquement
                    }
                    match Self::find_tracked(rest_up, rest_dn, &mut self.audit, &u.order_id) {
                        Some(r) => {
                            if u.size_matched > r.matched + 1e-9 {
                                let delta = u.size_matched - r.matched;
                                r.matched = u.size_matched;
                                tracing::info!(order_id = %u.order_id, delta,
                                    "fill vu par event ORDER (temps réel)");
                                let px = if r.price > 0.0 { r.price } else { u.price };
                                out.push(LiveFill { is_up, price: px, size: delta, maker });
                            }
                        }
                        None if u.size_matched > 1e-9 => {
                            tracing::error!(order_id = %u.order_id,
                                "ordre CONNU mais non suivi (event order) — fill compté + AUDIT");
                            out.push(LiveFill { is_up, price: u.price, size: u.size_matched, maker });
                            self.audit.push((RestingOrder {
                                order_id: u.order_id.clone(),
                                price: u.price,
                                size: u.size.max(u.size_matched),
                                matched: u.size_matched,
                                placed_ms: 0,
                            }, is_up, 0));
                        }
                        None => {}
                    }
                }
            }
        }
        // Slots entièrement fillés → libérés (re-quote possible).
        for rest in [rest_up, rest_dn] {
            if rest.as_ref().is_some_and(|r| r.matched >= r.size - 1e-6) {
                *rest = None;
            }
        }
        out
    }

    /// Localise un ordre suivi (slot Up, slot Down, ou entrée d'audit).
    fn find_tracked<'a>(
        rest_up: &'a mut Option<RestingOrder>,
        rest_dn: &'a mut Option<RestingOrder>,
        audit: &'a mut Vec<(RestingOrder, bool, u32)>,
        id: &str,
    ) -> Option<&'a mut RestingOrder> {
        if rest_up.as_ref().is_some_and(|r| r.order_id == id) {
            return rest_up.as_mut();
        }
        if rest_dn.as_ref().is_some_and(|r| r.order_id == id) {
            return rest_dn.as_mut();
        }
        audit.iter_mut().find(|(o, _, _)| o.order_id == id).map(|(o, _, _)| o)
    }

    /// Réconciliation par poll (autorité) — à appeler ~1×/3 s. Compare
    /// `size_matched` du CLOB au cumul déjà comptabilisé sur nos ordres
    /// restants et synthétise les fills manqués par le WS. Met aussi à jour
    /// `matched`/présence des ordres (absent = fillé en entier ou annulé).
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
        // AUDIT d'abord : les ordres retirés sans lecture réussie sont réinterrogés
        // jusqu'à vérité connue (un ordre clos reste consultable sur /data/order).
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
                    if matched > r.matched + 1e-9 {
                        let delta = matched - r.matched;
                        tracing::info!(order_id = %r.order_id, delta, "fill récupéré par AUDIT");
                        out.push(LiveFill { is_up: *is_up, price: r.price, size: delta, maker: true });
                    }
                    *tries = u32::MAX; // tranché → sortie de l'audit
                }
                Err(e) => {
                    *tries += 1;
                    if *tries >= 40 {
                        // ~2 min d'échecs : on abandonne BRUYAMMENT — le sync des
                        // positions on-chain (60 s) reste le filet de sécurité.
                        tracing::error!(order_id = %r.order_id, error = %e,
                            "AUDIT abandonné après 40 échecs — fill éventuel invisible au journal (positions resyncées on-chain)");
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
                    let matched = o.matched();
                    if matched > r.matched + 1e-9 {
                        let delta = matched - r.matched;
                        tracing::info!(order_id = %r.order_id, delta, "fill rattrapé par le poll");
                        out.push(LiveFill { is_up, price: r.price, size: delta, maker: true });
                        r.matched = matched;
                    }
                }
                None => {
                    // LAG D'INDEXATION (8 juil. 18:16) : un ordre posté il y a
                    // <10 s peut ne pas encore apparaître dans /data/orders —
                    // le slot libéré à tort = ordre DOUBLE posé par-dessus.
                    if now_ms - r.placed_ms < 10_000 {
                        continue;
                    }
                    // Plus au carnet : fillé en entier, ou annulé. On compte le
                    // reliquat comme fillé UNIQUEMENT si le WS ne l'a pas déjà
                    // fait — impossible à distinguer ici sans /data/order/{id} ;
                    // on interroge l'ordre individuellement pour trancher.
                    match super::auth::l2_request(
                        &self.creds, "GET", &format!("/data/order/{}", r.order_id), None, "",
                    ).await {
                        Ok(text) => {
                            let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                            let v = v.get("data").cloned().unwrap_or(v); // enveloppe éventuelle
                            let matched: f64 = v.get("size_matched")
                                .and_then(|s| s.as_str()).and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            if matched > r.matched + 1e-9 {
                                let delta = matched - r.matched;
                                tracing::info!(order_id = %r.order_id, delta, "fill final rattrapé (ordre clos)");
                                out.push(LiveFill { is_up, price: r.price, size: delta, maker: true });
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
        // BALAYAGE ANTI DOUBLE-ORDRE (8 juil.) : tout ordre OUVERT au carnet
        // doit coïncider avec ce que le bot suit (slots + audit). Un survivant
        // inconnu = ordre fantôme → ANNULÉ immédiatement, et audité pour que
        // ses fills éventuels entrent quand même dans la comptabilité.
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
                "ordre OUVERT au carnet INCONNU du suivi — annulé + audité");
            self.audit.push((RestingOrder {
                order_id: o.id.clone(),
                price: o.price.parse().unwrap_or(0.0),
                size: o.original_size.parse().unwrap_or(0.0),
                matched: o.matched(),
                placed_ms: 0,
            }, is_up, 0));
            out.push(LiveFill {
                is_up,
                price: o.price.parse().unwrap_or(0.0),
                size: o.matched(),
                maker: true,
            });
            let _ = orders::cancel_order(&self.creds, &o.id).await;
        }
        out.retain(|f| f.size > 1e-9);
        out
    }

}
