# Runbook — déploiement & exploitation (v3 split paper/live)

## Pré-requis

1. `cp deploy/hosts.env.example deploy/hosts.env` puis renseigner `LIVE_HOST` / `PAPER_HOST` /
   `RADAR_HOST` (gitignored).
2. Secrets Polymarket dans `~/.env` **sur le nœud live uniquement** (jamais en git) :
   `POLY_PRIVATE_KEY`, `POLY_FUNDER_ADDRESS`, `POLY_SIGNER_ADDRESS`, `POLY_API_KEY/SECRET/PASSPHRASE`,
   `POLY_SIG_TYPE=3` (deposit wallet). Voir mémoire `polymarket-deposit-wallet-sigtype3`.
3. **NTP synchronisé** sur les 3 nœuds (`timedatectl set-ntp true`) — sinon la latence transport
   (radar→nœud) est biaisée. Les autres legs restent fiables.
4. Toolchain : le **live** requiert rustc ≥ 1.91 (`rustup update stable` sur AWS). Le local (1.86) ne
   build que paper/radar.

## Déploiement

```bash
deploy/deploy.sh paper            # build défaut distant + restart sur PAPER_HOST
deploy/deploy.sh live             # build --features live distant + restart sur LIVE_HOST
deploy/deploy.sh radar            # build + restart sur RADAR_HOST
deploy/deploy.sh live --dry-run   # rsync à blanc (vérifie chemins/units)
```

Le script rsync exclut `.env`, `data/*.json|jsonl|log` et `target/`. Les unités systemd sont copiées
sous `/etc/systemd/system/` et le service redémarré.

## Ordre de démarrage recommandé

1. `deploy/preflight.sh` → doit afficher `OK — balance CLOB : … USDC`.
2. Live démarré **en pause** (`live_paused=true` par défaut, `LIVE_ARMED=false`).
3. Radar + paper démarrés.
4. Vérifier les dashboards : live `:8769`, paper `:8768`, radar `:8768`.
5. Armer le réel : `LIVE_ARMED=true` dans `~/.env` (nœud live) → `deploy/ctl.sh live restart`.
6. Lancer l'exécution : bouton **START** du dashboard live (ou `deploy/ctl.sh live run`).

## Contrôle courant

```bash
deploy/ctl.sh live status|logs|restart   # systemd (arrêt DUR)
deploy/ctl.sh live pause|run             # pause LOGICIELLE (POST /stop|/start) — WS restent chaudes
deploy/ctl.sh paper pause|run
```

Start/Stop dashboard = pause logicielle (AtomicBool). Privilégier `pause` à `stop` pour ne pas perdre
les connexions WS / le contexte de position.

## Vérifications santé

- Dashboard live : `lat_total_ms` raisonnable, `live_bankroll` lue, `node_kind=live`.
- Logs radar : `🚀 signal UDP envoyé` apparaît **deux fois** par tir (live puis paper) si paper activé.
- Le live ne doit plus présenter de pic de latence corrélé au paper (process/machines séparés).

## Rollback

```bash
deploy/rollback.sh live  v2.0.0-mono-toggle   # revient à l'archi couplée (paper+live un process)
deploy/rollback.sh paper v3.0.0-split
```

Tags : `v2.0.0-mono-toggle` (avant split) · `v3.0.0-split` (archi actuelle). Les `data/*.json` ne sont
jamais écrasés (règle `never-delete-data-files`) ; un restart recharge l'état via `load_or_init`.

## Règles de sécurité

- Ne jamais `rm`/écraser `data/*.json|jsonl` pour « reset » : `pkill` + relance recharge l'état.
- `LIVE_ARMED=true` = envoi réel : armer seulement après preflight, surveiller, désarmer si doute.
- `LIVE_FORCE_MIN_SIZE=true` = agressif (Kelly ignoré, taille minimale) : micro-test plomberie only.
