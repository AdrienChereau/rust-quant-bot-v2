# rust-quant-bot — point d'entrée (où on en est)

Sniper front-running **Binance+OKX → Polymarket** (fenêtres BTC 5 min). Le radar (Tokyo) calcule un
signal OBI consolidé et tire en UDP vers les nœuds d'exécution. **Architecture à 3 nœuds isolés**
depuis la v3 (split paper/live).

> Langue : répondre en **français**. Mot d'ordre du projet : **la rapidité du chemin live**.

## Topologie (v3.0.0-split)

| Nœud | Rôle CLI | Build | Dashboard | Fichiers d'état | Machine |
|------|----------|-------|-----------|-----------------|---------|
| **Radar** | `radar` | défaut | `:8768` | — | Tokyo |
| **Live**  | `live`  | `--features live` | `:8769` | `data/live_state.json` + `.jsonl` | Dublin (eu-west-1) |
| **Paper** | `paper` | défaut | `:8768` | `data/sniper_state.json` + `.jsonl` | **machine séparée** |

- Le radar **tire aux deux, LIVE d'abord** (UDP), paper ensuite (`TARGET_PAPER_IP` optionnel).
- Le nœud **live ne contient AUCUN code paper** et inversement → le paper ne peut jamais voler le
  CPU/les locks du live (process + machines séparés). C'est le cœur du refactor v3.
- `mono` (radar+exécuteur in-process) reste pour le dev local. `executor` = alias legacy de `live`.

## Build / run

```bash
cargo build --release                 # paper + radar (n'importe quel rustc)
cargo build --release --features live  # live (rustc >= 1.91 — AWS only, pas en local 1.86)
cargo test                             # 59 tests
# local 3-process : radar TARGET_LIVE=127.0.0.1:8080 TARGET_PAPER=127.0.0.1:8081
#                   live --listen-port 8080 (PORT=8769) ; paper --listen-port 8081 (PORT=8768)
```

## Start/Stop = pause logicielle

Bouton dashboard → `POST /start` | `/stop` → bascule un AtomicBool (`live_paused` ou `paper_paused`).
Le process et les WebSockets restent chauds. `LIVE_ARMED=true` (env) reste requis pour l'envoi réel.

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — topologie, hot-path, locks, isolation.
- [docs/MATH.md](docs/MATH.md) — OBI, Black-Scholes, Kelly, TP/SL, breaker (avec refs fichier:ligne).
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — déploiement (`deploy/*.sh`), env, NTP, rollback.
- [docs/CHANGELOG.md](docs/CHANGELOG.md) — historique aligné sur les tags git.

## Rollback

Tags git : `v2.0.0-mono-toggle` (avant split, paper+live couplés) · `v3.0.0-split` (après).
`deploy/rollback.sh <live|paper|radar> <tag>`.
