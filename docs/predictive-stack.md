# Stack prédictif BTC Up/Down 5m / 15m / 1h — design cible

> **Principe directeur** : à ces horizons, l'alpha n'est pas dans un modèle exotique — il est dans
> (1) une σ bien estimée **par régime**, (2) la latence Binance→décision, (3) la microstructure du
> book, (4) des seuils **nets de frais**. Le ML n'est qu'une couche corrective calibrée par-dessus
> le modèle paramétrique, jamais un remplacement.
>
> Ce document est la roadmap complète. [STRATEGY.md](STRATEGY.md) = la **v1** (paramétrique pur),
> seule autorisée au premier go-live. Chaque étage suivant est *gated* par le Brier out-of-sample.
>
> ⚠️ Le plan d'origine référence un repo `poly-maker-rs` (13 crates `pm-*`) absent de cette
> machine. Le mapping ci-dessous ancre chaque brique dans **ce repo** (`rust-quant-bot-v2`).

---

## 0. État réel vs plan (ce qui existe déjà ici)

| Brique du plan | Équivalent local | État |
|---|---|---|
| `pm-recorder` (ticks + book PM + résolutions) | `src/recorder.rs` → `data/windows.jsonl` | ✅ v0 (1 Hz + asks + issues) — à enrichir §8 |
| Juge Brier vs mid PM | `scripts/calibrate.py` | ✅ anti-arnaque (1 trade/fenêtre, fill ask, z-score, frais 0.07·p(1−p)) |
| Frais taker `0.07·p(1−p)` | `calibrate.py::taker_fee` | ✅ |
| Modèle Φ(d2) | `src/pricing/black_scholes.rs` | ✅ (+ `fair_up_with_d2_shift` à retirer du chemin de décision) |
| aggTrades Binance | `src/binance/trade_feed.rs` | ✅ (spot ; perp à ajouter) |
| Paper latency-aware + frais | `src/strategy/bankroll.rs` (`submit`/`settle_pending`) | ✅ (file d'attente maker NON modélisée — biais connu) |
| Book PM + strike + fenêtres | `src/polymarket/*` | ✅ |
| Vol multi-échelle HAR | — | ❌ à construire (§2) |
| Features OFI/staleness z-scorées | — | ❌ (§3) |
| Couche logistique v2 / isotonique | — | ❌ (§4) |
| Maker A-S, mint-matching | — | ❌ (§5.3) — après GO taker seulement |

**Leçons durement acquises à ne pas re-payer** (validées sur 2 600+ trades paper) :
- σ estimée sur **ticks** = ×3-5 trop haute (bruit microstructure). **Toujours des retours 1 s.**
  (Bug vécu : σ 170 % vs σ* calibrée 35 %.) La sémantique de λ dépend du pas d'échantillonnage.
- Un PnL paper positif avec TP>SL et fills au mid est un **artefact**. Seul le juge
  (Brier + z-score par fenêtre) compte.
- Notre IC≈0 a tué *notre* taker (score composite + TP/SL 60 s + σ fausse). Il ne dit **rien**
  sur le taker staleness-value de ce plan — c'est précisément ce que le juge doit tester.

---

## 1. Cadre probabiliste

```
p_up = Φ(d)      d = [ ln(S/K) + (μ − σ²/2)·τ ] / (σ·√τ)
```
- **μ = 0** — le drift est indétectable à τ ≤ 1h ; en ajouter un dégrade le Brier
  (interdit sans preuve out-of-sample).
- Le terme −σ²/2·τ est négligeable (< 0.1 bp à 1h) mais gratuit : le garder pour la propreté.
- **τ contre l'horloge de résolution** (sync chrony), pas l'horloge locale.

**Correction des queues** (décisif quand |d| > 1.5, là où vendre le « sûr » à 0.95+ perd sur
les jumps) — retours 1 s BTC ≈ Student-t (ν ≈ 3-4) :
```
p_up = (1−w)·Φ(d) + w·Φ(d/k)         w ≈ 0.05–0.10, k ≈ 3 (composante jump)
```
2 paramètres, fittés par max-vraisemblance sur les fenêtres résolues. (Choisi vs CDF Student-t :
équivalent, plus stable numériquement en Rust.)

**Incertitude de résolution Chainlink** — la résolution est Chainlink, le spot est Binance :
```
d_eff = ln(S/K) / √(σ²τ + σ_CL²)
```
σ_CL estimé sur les écarts Chainlink−Binance loggés aux résolutions (typiquement quelques bps).
Négligeable à τ > 10 min, **décisif dans les 2 dernières minutes** quand |S−K| ~ quelques bps.

## 2. Volatilité multi-échelle (60 % de l'effort)

Un EWMA unique est trop nerveux pour 1h et en retard au changement de régime. Cible :
**prévision de variance intégrée par horizon**.

- **2.1 Estimateurs réalisés** (retours 1 s, **perp Binance de préférence** — il lead le spot) :
  RV_1m/5m/30m/2h + **variation bipower** `BV = (π/2)·Σ|r_i||r_{i−1}|` (robuste aux jumps) ;
  composante jump `J = max(0, RV − BV)`.
- **2.2 HAR-RV par horizon** h ∈ {5m, 15m, 1h} :
  `σ̂²(t,t+h) = β₀ + β₁·BV_5m + β₂·BV_30m + β₃·BV_2h + β₄·J_30m` — moindres carrés,
  walk-forward hebdomadaire, coefficients exportés JSON, hot-reload. 5 paramètres/horizon.
- **2.3 Saisonnalité intra-journalière** : profil multiplicatif s(u) par tranche de 15 min UTC
  (4 semaines glissantes) ; `σ²_eff·τ = σ̂²_base · ∫ s(u) du`. (La vol double à l'ouverture US.)
- **2.4 Événements programmés** (CPI, FOMC, NFP) : multiplicateur plancher ×2-4 sur ±10 min,
  fichier de config statique mis à jour à la main — pas d'automatisation fragile.
- **2.5 Plancher vol implicite** : DVOL Deribit (ou IV ATM courte) comme borne inférieure à 1h —
  protège contre l'EWMA endormie avant un mouvement anticipé par le marché d'options.

## 3. Features microstructure (l'alpha du 5-15 min)

Calcul continu en Rust, **z-scores sur fenêtre glissante 24 h** (moyenne/σ stockés par le recorder).

| Feature | Définition | Fenêtres | Rôle |
|---|---|---|---|
| **OFI** (Cont et al.) | Σ signée des variations de taille au best bid/ask Binance | 1s, 5s, 30s | Meilleur prédicteur court terme |
| Trade imbalance | (vol acheteur agressif − vendeur)/total (aggTrades) | 10s, 60s | Momentum de flux |
| Book imbalance | (Q_bid−Q_ask)/(Q_bid+Q_ask), top+5 niveaux | instantané | Pression directionnelle |
| Basis perp−spot | Δ(perp−spot) sur 30 s | 30s | Le perp lead le spot |
| **Staleness PM** | `p_model − mid_PM` + âge du dernier update du mid | instantané | **LE signal taker** |
| Imbalance book PM | (profondeur Up−Down)/total, 3 niveaux | instantané | Drift du mid PM ; guide le quoting |
| Flux taker PM | volume signé des trades PM sur 60 s | 60s | Flux toxique → pull quotes |

## 4. Couche de fusion ML (progressive, jamais en v1)

Chaque étage *gated* par le Brier out-of-sample du précédent :

1. **v1 — paramétrique pur** : `p̂ = Φ(d_eff)` avec la σ §2 + mélange de gaussiennes. Zéro ML.
   Le baseline à battre, **seul autorisé au premier go-live**. ([STRATEGY.md](STRATEGY.md))
2. **v2 — correction logistique** (sweet spot effort/rendement) :
   `logit(p̂) = logit(p_Φ) + β·x`, x = features §3 z-scorées, ridge, walk-forward, par bucket
   d'horizon. ~10 paramètres, interprétable, inférence Rust en 3 lignes, quasi impossible à
   sur-fitter avec des centaines de fenêtres/jour. Entraînement offline Python → JSON → hot-reload.
3. **v3 — LightGBM** (seulement si v2 plafonne) : gate strict
   `Brier(v3) < Brier(v2) − marge` sur 2 semaines out-of-sample. Inférence = arbres exportés en
   Rust généré (pas de binding runtime).

**Pas de deep learning** : ~380 fenêtres/jour/actif, régimes non stationnaires, un opérateur solo
n'a pas le budget d'itération pour débugger un NN qui overfit — l'alpha est dans les features.

**Calibration** : régression isotonique par bucket (horizon × décile de τ), hebdomadaire.
Vérification continue : reliability curve + Brier décomposé dans le rapport quotidien.

## 5. Décision et exécution

- **5.1 Frais** : taker `0.07·p(1−p)` (max ~1.75 % à p=0.5), maker rebate 20 %.
  `edge_min_taker(p, τ) = 0.07·p(1−p) + ½spread + buffer(τ)` — buffer calibré sur l'adverse
  selection mesurée en paper (move du mid dans les 5 s après nos fills).
- **5.2 Taker (5m/15m)** : croiser quand `p̂ − ask > edge_min` (symétrique Down). Cooldown par
  fenêtre.
- **5.3 Maker (1h, et 15m hors stress)** — Avellaneda-Stoikov adapté aux binaires :
  prix de réservation `r = p̂ − γ·q·Var[Δp̂]` (q = inventaire delta-équivalent) ; demi-spread
  `δ = δ₀(τ,σ) +` composante adverse-selection (flux taker PM récent) ; **pull des quotes** si
  |Δp̂| > seuil sur 2 s ou feed Binance stale > 2 s ; cutoffs fin de fenêtre
  {1h : 5 min, 15m : 90 s, 5m : 45 s} — après, seul le taker « sniping » avec d_eff (σ_CL) ;
  **mint-matching** : BUY des deux côtés quand somme < $1 − buffer.
- **5.4 Sizing** : Kelly fractionné (**¼ live, ½ paper**) sur l'edge calibré. Caps : par fenêtre,
  par heure, par horizon ; BTC+ETH = 0.6 + 0.6 d'une même unité de risque (corr ~0.8).
  Kill-switch : perte jour > X %, désync ws_user, feed stale.

## 6. Validation et métriques

- **Brier/log-loss bucketisés** : horizon × décile de τ × bucket de |d|, **comparés au mid PM**
  (le benchmark à battre). On n'active un (horizon, bucket) que si on y bat le mid.
- **Attribution P&L** : edge théorique vs réalisé ; adverse selection par fill ; frais ;
  maker vs taker séparés.
- **Backtest replay** déterministe depuis les logs tick (§8).
- **Go-live par horizon, indépendamment** : ≥ 1 semaine paper, Brier(modèle) < Brier(mid) ET
  P&L > 0 — appliqué séparément à 1h, 15m, 5m. Ordre attendu : **1h (maker) → 15m → 5m**
  (le plus exigeant en latence).
- Règle anti-arnaque du juge (acquise ici) : 1 trade/fenêtre, fill au ask, z > 1.96 sur
  ≥ 100 fenêtres, EV ~monotone en seuil.

## 7. Architecture réseau / infra

Réalité géographique : matching Binance = **Tokyo (ap-northeast-1)** ; CLOB Polymarket =
**US-East**. On ne peut pas être rapide aux deux bouts → **le cerveau près de là où on agit
(Polymarket), optimiser le chemin de l'information (Binance)**.

- **Phase A — Paper (Mac, actuel)** : ws publics, tout local. Suffisant pour valider le modèle
  (le Brier ne dépend pas de la latence ; le P&L taker simulé en dépend — biais connu du paper).
- **Phase B — VPS us-east-1** (go-live 1h/15m) : 1 instance (c7g.medium/c6i.large), chrony.
  Feeds : Binance ws publics (~150-250 ms depuis Tokyo), CLOB ws + REST (~10-40 ms intra-région).
  Budget bout-en-bout : événement Binance → ordre ≈ **170-300 ms**. Suffit pour maker 1h/15m
  et taker 15m. ⚠️ Note : l'infra historique de ce repo vise Dublin (eu-west-1) — pour ce plan,
  la cible devient **us-east-1** (proximité CLOB).
- **Phase C — Relay Tokyo** (*seulement si* le taker 5m montre de l'edge en paper mais le rate
  en live) : t4g.small ap-northeast-1, ws Binance locaux (~1-5 ms), deltas binaires compacts sur
  TCP persistant (backbone AWS, one-way ~75 ms). Gain net 50-120 ms, ~$15/mois.
  **Mesurer avant de construire** : logger timestamp-exchange vs réception en phase B ;
  si p50 < 200 ms et peu de trades 5m ratés → phase C inutile.

Process (un binaire tokio, tasks découplées par ring buffers lock-free) :
```
binance_feed ─┐
              ├─> feature_engine ─> model (p̂, <50 µs/tick) ─> quoter/taker ─> executor
pm_market_ws ─┘                                                                  │
recorder (hors chemin critique) <────────────────────────────────────────────────┘
watchdog (feed stale, désync, kill-switch)
```
Monitoring : Prometheus (lag feed p50/p99, âge des quotes, fill rate, adverse selection, Brier
glissant, P&L) + alertes Telegram/ntfy. Reconnexion backoff+jitter, détection de gaps de
séquence, cross-check spot/perp (divergence > seuil → feed suspect → pull quotes).

## 8. Pipeline de données et d'entraînement

- **Recorder enrichi** (upgrade de `src/recorder.rs`) : par fenêtre — strike capturé, ticks
  Binance downsamplés à 100 ms (+ ticks bruts dans ±2 min de l'expiry), snapshots book PM à
  250 ms, nos quotes/fills, **résolution finale et prix Chainlink**. Format Parquet,
  1 fichier/jour/actif. À déployer **immédiatement même en paper** — sans données, rien ne se fitte.
- **Offline Python** (`training/`) : fit HAR (§2.2), saisonnalité (§2.3), mélange de gaussiennes
  (§1), σ_CL (§1), logistique v2 (§4), isotonique (§4) → `model_params.json` versionné,
  hot-reload dans le bot.
- **Cadence** : re-fit hebdomadaire ; alerte si un coefficient bouge de > 2 écarts-types
  (drift de régime).

## 9. Étapes d'implémentation (dans CE repo)

1. ✅ Ce document (`docs/predictive-stack.md`) + [STRATEGY.md](STRATEGY.md) (v1).
2. **Recorder v1** (`src/recorder.rs`) : ✅ JSONL 1 Hz + asks + issues → upgrade §8
   (ticks 100 ms, book PM 250 ms, prix de résolution Chainlink, Parquet) — prérequis de tout.
3. **Feeds enrichis** (`src/binance/`) : ajouter le **perp** (fstream) à côté du spot ;
   les tailles bid/ask du bookTicker suffisent pour l'OFI top-of-book.
4. **Vol multi-échelle** (`src/pricing/volatility.rs`) : trait `VolEstimator`, impls `Ewma`
   (baseline, retours 1 s) et `HarVol` (§2). **A/B au Brier sur le même flux avant bascule.**
5. **Config multi-horizon** : slugs 5m/15m/1h (vérifier via Gamma), `Params` par horizon,
   cutoffs et take_edge horizon-dépendants et fee-aware.
6. **`src/signal/features.rs`** : OFI, imbalances, staleness (§3), z-scores 24 h.
7. **Mode taker v1** (STRATEGY.md) dans `src/strategy/` — paper d'abord sur 15m.
8. **Pipeline `training/`** (Python) : activable dès ≥ 2 semaines de données recorder.
9. **Brier bucketisé** : étendre `scripts/calibrate.py` (buckets horizon × τ × |d|).
10. **Infra** : VPS us-east-1 au go-live 1h ; histogrammes de latence dès la phase B ;
    décision relay Tokyo uniquement sur mesures.

## Vérification

- Tests unitaires : estimateurs vol sur vecteurs connus, mélange de gaussiennes vs scipy,
  replay déterministe du recorder.
- **A/B Brier** : nouvelle σ vs EWMA baseline sur les mêmes fenêtres — le nouveau modèle doit
  gagner **avant** d'être branché.
- Paper multi-horizon ≥ 1 semaine **par horizon**, critère go-live par horizon.
- Latence : histogrammes exchange-timestamp → réception → ordre, publiés dès la phase B.
