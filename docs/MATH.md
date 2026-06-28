# État mathématique — rust-quant-bot

Toutes les formules avec leur emplacement dans le code. Sert de référence pour les futures sessions.

## 1. OBI consolidé (porte d'accord cross-exchange) — `src/signal/consolidated_obi.rs`

Order-Book Imbalance par exchange ∈ [−1, +1]. Le tir n'est **pas** une moyenne pondérée (un Binance
fort noierait un désaccord OKX). C'est une **porte d'ACCORD** :

- même signe : `sign(OBI_binance) == sign(OBI_okx)` et `OBI_binance ≠ 0` ;
- chacun au-dessus du plancher : `|OBI_b| ≥ floor` ET `|OBI_okx| ≥ floor` ;
- magnitude pondérée suffisante : `|mag| ≥ fire_threshold`, avec
  `mag = w_b·OBI_b + w_okx·OBI_okx` (défaut `w_b=0.65`, `w_okx=0.35`).

`mag` (signée) sert **uniquement au sizing** (force du signal). Side = `Up` si `mag>0`, sinon `Down`.
Paramètres env : `OBI_FLOOR_PER_EXCHANGE` (0.20), `OBI_FIRE_THRESHOLD` (0.20), `WEIGHT_BINANCE/OKX`.

## 2. Fair value Black-Scholes binaire — `src/pricing/black_scholes.rs`

Probabilité « Up » à l'échéance de la fenêtre 5 min :

```
P(Up) = N(d2),  d2 = ( ln(spot/strike) − ½·σ²·t ) / ( σ·√t )
```

- `spot` = BTC spot (Binance), `strike` = prix BTC à l'ouverture de la fenêtre (kline 1 m, radar).
- `σ` = volatilité annualisée (`src/pricing/volatility.rs`, EWMA), `t` = `years_from_secs(remaining_s)`.
- Cas dégénérés : `t→0` ou `σ→0` → indicatrice (1 si spot>strike, 0 si <, 0.5 si =).
- Fenêtre déterministe de 300 s (`WINDOW_SEC`, `src/roles/radar.rs`).

`fair_up` est la juste valeur ; le `gap = fair_up − real_up` (real_up = mid Polymarket) est l'edge.

## 3. Sizing Kelly fractionnel — `src/strategy/bankroll.rs` (`KellyParams::kelly_size_for`)

Pari binaire, cote `odds = (1−price)/price`, fraction de Kelly :

```
f*   = clamp(edge / odds, 0, 1)
f    = f* · kelly_fraction                    (défaut half-Kelly, kelly_fraction=0.5)
budget = min( equity·f , equity·max_size_pct ) (plafond max_size_pct, défaut 0.02)
size = floor( budget / price )                 (nb de tokens entier)
```

- **Paper** : `equity` = cash fictif interne (`PaperEngine`).
- **Live** : `equity` = vraie collatéral USDC CLOB (bankroll). Même fonction pure (extraite du couplage
  paper en v3 → le live ne dépend plus de `PaperEngine`).
- Env : `KELLY_FRACTION`, `MAX_KELLY_SIZE_PCT`. `LIVE_FORCE_MIN_SIZE` ignore Kelly → taille minimale.

## 4. Sortie de position : TP / SL / max-hold — `bankroll.rs`, `src/strategy/live_position.rs`

- Take-profit : `bid ≥ entry + tp_cents` (env `TAKE_PROFIT_CENTS`, défaut 4¢).
- Stop-loss : `bid ≤ entry − sl_cents` (env `STOP_LOSS_CENTS`, défaut 3¢).
- Max-hold : `held_s ≥ max_hold_secs` (env `MAX_HOLD_SECS`, 60 s) ou `remaining_s ≤ 30` (proche échéance).
- Paper : sélection adverse modélisée à la clôture (biais selon mouvement futur) + slippage VWAP en
  parcourant le carnet PM à l'entrée.

## 5. Circuit breaker (drawdown) — `bankroll.rs`

- Paper : `check_drawdown_breaker(equity, start_cash, max_drawdown)` → trip si
  `start_cash − equity ≥ max_drawdown`.
- Live : high-water mark sur la **bankroll réelle** (`LiveDrawdown::breached`). Env `MAX_DRAWDOWN` ($).
- Breaker déclenché → aucun signal exécuté (les deux nœuds, indépendamment). Reset via `/breaker/reset`.

## 6. Latence totale signal→ordre (v3) — `src/roles/live.rs`, `src/net/wire.rs`

```
lat_total = lat_transport + lat_decide + lat_post
```

- `transport` = `recv_ms − sent_ms` (timestamp radar embarqué dans le paquet — requiert NTP sync).
- `decide` = `Instant` entre réception UDP et soumission OrderEngine (mono-horloge, fiable).
- `post` = round-trip POST CLOB du dernier ordre (`live_mgr.last_buy_ms`).

## Conclusion edge (rappel mémoire projet)

Sur l'analogue paper du monolithe : pas d'edge net démontré sur l'horizon 5 min (sélection adverse +
pas de rewards). Le split v3 vise d'abord à **fiabiliser et accélérer le chemin live** et à mesurer
proprement, pas à valider l'edge — qui reste à établir.
