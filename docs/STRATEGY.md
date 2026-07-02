# Cryptonite — Stratégie v3 : « Value calibrée, hold-to-resolution »

> Un seul trade : **acheter le token dont notre probabilité calibrée dépasse le prix ask
> d'une marge nette de frais, et tenir jusqu'à la résolution.**
> Pas de TP/SL, pas de score composite, pas de course de vitesse.
>
> Ce document est la **v1 (paramétrique pur)** du stack complet décrit dans
> [predictive-stack.md](predictive-stack.md) — vol HAR multi-échelle, queues Student,
> σ_CL Chainlink, features OFI/staleness, couche logistique gated, maker A-S, infra us-east-1.
> La v1 est le baseline à battre et la seule autorisée au premier go-live.

---

## 1. Ce que les données ont tué (et qu'on ne ressuscite pas)

| Idée | Verdict | Preuve |
|---|---|---|
| Sniping taker piloté par score composite (OBI+TFI+Kalman+basis) | ☠️ mort | IC ≈ 0 à 400 ms (1673 trades) **et** à 100 ms (959 trades) |
| TP/SL sur le prix du token | ☠️ mort | PnL = artefact d'asymétrie TP>SL + fills de stop optimistes ; 2× spread payé |
| Vol réalisée sur ticks (100 Hz / 2 s) | ☠️ mort | σ gonflée ×3-5 par le bruit de microstructure (170 % vs σ* = 35 % calibré) |
| d2_gamma = 0.50 (décalage du fair par le score) | ☠️ mort | Jamais calibré ; fabrique les gaps qu'il prétend détecter |
| Market-making neutre 5-min | ☠️ mort (bot précédent) | Sélection adverse + pas de rewards |

## 2. Le modèle — trois variables, dont une seule à estimer

La courbe d'un marché up/down 5-min est une fonction quasi déterministe de :

```
p̂ = Φ( d2 )        d2 = [ ln(S/K) − ½σ̂²τ ] / ( σ̂ √τ )
```

1. **Moneyness `ln(S/K)`** — observée (spot Binance vs strike d'ouverture). Driver dominant :
   la courbe PM est une image retardée du spot passée dans Φ.
2. **Temps restant `τ`** — observé. Début de fenêtre : courbe molle (p ~ 0.5). Fin de fenêtre :
   le gamma explose — 0.05 % de spot près du strike = 10-20 points de p.
3. **Volatilité `σ̂` à l'horizon τ** — **la seule variable estimée. Tout l'edge (ou toute
   l'erreur) est ici.**

### L'estimateur de σ (le cœur du travail)

- Retours **échantillonnés à 1 s** (jamais des ticks : bruit bid-ask → σ ×3-5, bug prouvé).
- **Deux horizons blendés** : `σ̂ = max(EWMA_rapide λ=0.94, EWMA_lent λ=0.99)` —
  le `max` protège au changement de régime (le rapide monte vite après un choc,
  le lent évite la sous-estimation prolongée).
- **Plancher** de sécurité ; piste ultérieure : sanity check vs vol implicite Deribit courte.
- ⚠️ La sémantique de λ dépend de la fréquence d'update : λ=0.94 **par seconde** ≈ 17 s de
  mémoire. Appliqué par tick à 100 Hz ça ferait 170 ms — d'où l'obligation du pas de 1 s.
- **Queues grasses** : les retours 1 s ne sont pas gaussiens. Près du strike, négligeable ;
  loin du strike (p < 0.10 ou > 0.90), le gaussien **sous-estime le retournement** →
  garde n°3 ci-dessous.

## 3. La décision — une formule, un seuil

```
frais(p)    = 0.07 · p · (1 − p)          # taker Polymarket : ~1.75 % à p=0.5, →0 aux extrêmes
edge_up     = p̂       − ask_up   − frais(ask_up)
edge_down   = (1 − p̂) − ask_down − frais(ask_down)

TRADE si max(edge_up, edge_down) > θ      # θ unique, net de frais (défaut 0.02)
        → acheter le côté correspondant AU ASK, tenir jusqu'à résolution.
```

Le seuil dépendant de p est **automatique** : comme frais(p) culmine à p=0.5 et s'annule aux
extrêmes, exiger `edge_net > θ` revient à demander ~3-4 points bruts à 0.50 et ~2 points à 0.85.
Pas besoin d'un second paramètre.

## 4. Les cinq gardes (tout ce qui reste du "défensif")

1. **Fenêtre de tir** : `τ ∈ [τ_min=30 s, τ_max=240 s]`. Avant : courbe molle, rien à gagner.
   Après : zone Chainlink (garde 2).
2. **Zone grise oracle** : la résolution est **Chainlink**, notre spot est Binance. En fin de
   fenêtre, si `|S − K|` < seuil (à mesurer — cf. §6), l'issue dépend de l'agrégation Chainlink,
   pas de notre modèle → **abstention**. On logge l'écart aux résolutions pour chiffrer la zone.
3. **Anti-queues grasses** : ne jamais acheter un token au-dessus de **0.93** (vendre le
   « sûr » à 0.97 perd sur les jumps — le gaussien sous-estime cette queue).
4. **Un trade par fenêtre**, hold to resolution — pas de moyennage, pas de sortie anticipée.
5. **Breaker bankroll** inchangé (drawdown max).

## 5. Exécution — le lead-lag comme *timing*, pas comme signal

Le lead-lag Binance→PM (quelques centaines de ms à quelques s de staleness selon la liquidité)
ne revient **pas** dans la décision comme un score : il sert uniquement à **choisir l'instant**
d'exécuter un trade déjà décidé par l'edge :

> si `p̂` vient de bouger (spot Binance) et que l'ask PM n'a pas encore suivi,
> l'ask est *stale* → exécuter maintenant = meilleur prix.

Concrètement : la condition `edge_net > θ` calculée sur l'ask **capture déjà** la staleness
(un ask en retard = edge apparent plus grand). Aucun paramètre supplémentaire.

## 6. Les paramètres — avant / après

| **Avant (~25)** | **Après (8, dont 4 quasi figés)** |
|---|---|
| 4 poids composite + 5 Kalman + 3 basis + TFI window + vel_norm + score_threshold + d2_gamma + gap_min + dwell + cooldown + tp/sl/max_hold + vacuum ×2 + kelly ×3 | `λ_rapide=0.94` · `λ_lent=0.99` · `σ_plancher` · `θ=0.02` · `τ_min=30` · `τ_max=240` · `p_max_achat=0.93` · `kelly_fraction` (+cap) |

Le pipeline OBI/TFI/Kalman/basis reste dans le code (radar, dashboard, recherche) mais
**sort du chemin de décision**. Il ne pourra y revenir que si le juge (§7) prouve qu'il
améliore le Brier — ce qu'il n'a pas fait jusqu'ici.

## 7. Validation — le juge décide, pas nous

`scripts/calibrate.py` sur `data/windows.jsonl` (recorder 1 Hz + issues), règle **GO** :

1. **Brier(p̂ calibrée) < Brier(prix PM)** sur le jeu de test (fenêtres impaires) ;
2. **une ligne d'EV avec z > 1.96 sur ≥ 100 fenêtres tradées** (1 trade/fenêtre, fill au ask,
   frais 0.07·p(1−p)) ;
3. **EV ~monotone** en fonction du seuil.

Les trois, sinon NO-GO. À date (31 fenêtres) : échantillon insuffisant, marché devant au Brier.
Verdict attendu à **≥ 300 fenêtres résolues** (~25 h de collecte).

À logger en plus (TODO recorder) : le prix de résolution effectif (Chainlink) par fenêtre pour
mesurer l'écart Chainlink−Binance et calibrer la zone grise (garde n°2).

## 8. Roadmap

1. ✅ Recorder (features + asks + issues) — collecte en cours.
2. ✅ Juge anti-arnaque (1 trade/fenêtre, ask, z-score) + frais 0.07·p(1−p).
3. ⏳ 300 fenêtres → verdict GO/NO-GO.
4. Si GO : implémentation du mode `resolution` (cette page = spec), paper 200 fenêtres,
   puis live micro-bankroll avec la matrice de complétude d'avant-live.
5. Si NO-GO : le 5-min est enterré (3 stratégies tuées en paper, 0 $ brûlé) → marchés plus
   longs (horaires/quotidiens) où la calibration bat la vitesse, ou arrêt.

## 9. Pistes plus tard (pas maintenant)

- Vol implicite Deribit courte comme plancher/sanity de σ̂.
- Maker au lieu de taker (rebate ~20 %) — exige de modéliser la file d'attente et l'adverse
  selection que notre paper sous-estime ; seulement après un GO taker.
- Correction de queues (Student-t / jump) pour les p extrêmes, si la garde 0.93 se révèle
  trop conservatrice dans les données.
