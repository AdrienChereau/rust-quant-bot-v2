# Changelog — rust-quant-bot

Aligné sur les tags git. `deploy/rollback.sh <role> <tag>` pour revenir à une version.

## v3.0.0-split — séparation paper / live en deux nœuds isolés

**Objectif** : le paper ne doit JAMAIS consommer la vitesse du live. Deux nœuds, deux machines, zéro
code partagé dans la hot-loop.

- **Rôles séparés** : `src/roles/live.rs` (zéro code paper) + `src/roles/paper.rs` (zéro code live)
  remplacent `roles/executor.rs`. Nouvelles sous-commandes CLI `live` / `paper` ; `executor` = alias
  legacy de `live` ; `mono` conservé pour le dev.
- **Sizing Kelly découplé** : `kelly_size_for` extrait en méthode de `KellyParams` → le live ne dépend
  plus de `PaperEngine`.
- **Radar dual-target** : tire en UDP au live **d'abord**, puis au paper (`TARGET_PAPER_IP` optionnel,
  `TARGET_LIVE_IP/PORT`).
- **Wire v2 (14 octets)** : ajout d'un timestamp d'émission `sent_ms` pour mesurer la latence transport.
- **Dashboard par nœud** : champ `node_kind` (live/paper/radar/mono) → une vue par nœud ; bouton
  **Start/Stop** = pause logicielle (`POST /start|/stop`) remplaçant la bascule PAPER⇄LIVE.
- **Latence totale signal→ordre** sur le dashboard live : transport + décision + POST CLOB.
- **Déploiement** : units systemd `live`/`paper`/`radar` + scripts `deploy/{build,deploy,ctl,rollback,
  preflight}.sh` + `hosts.env`.
- **Docs** : `CLAUDE.md`, `docs/{ARCHITECTURE,MATH,RUNBOOK,CHANGELOG}.md`.
- Tests : 59 passent (incl. roundtrip wire v2). Build live = rustc ≥ 1.91 (AWS).

## v2.0.0-mono-toggle — état avant split (référence de rollback)

Paper + live couplés dans un seul process (nœud Exécuteur Dublin), bascule via un bouton switch
PAPER⇄LIVE sur le dashboard. Le paper s'exécutait dans la même hot-loop que le live (walk VWAP
synchrone, manage 50 ms, RwLock dashboard) → taxe de latence sur le chemin live.
