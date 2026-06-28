#!/usr/bin/env bash
# Déploie un rôle sur son hôte : rsync binaire + frontend + unit systemd, puis restart.
# Usage: deploy/deploy.sh <live|paper|radar> [--dry-run]
#
# ⚠️ Le binaire LIVE doit être buildé sur un hôte rustc>=1.91 (cf. build.sh live). En pratique on
# build le live SUR la box Dublin elle-même (toolchain AWS récent) plutôt que de pousser un binaire
# cross-compilé. Ce script pousse alors les sources + déclenche le build distant pour le live.
#
# Le fichier ~/.env (secrets Polymarket) n'est JAMAIS poussé : déposé manuellement une fois.
# Les JSON d'état (data/*.json) ne sont JAMAIS écrasés (cf. règle never-delete-data-files).
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/hosts.env

ROLE="${1:?usage: deploy.sh <live|paper|radar> [--dry-run]}"
DRY="${2:-}"
RSYNC_OPTS=(-az --exclude 'data/*.json' --exclude 'data/*.jsonl' --exclude 'data/*.log' --exclude '.env')
[[ "$DRY" == "--dry-run" ]] && RSYNC_OPTS+=(--dry-run) && echo "(DRY-RUN)"

case "$ROLE" in
  live)   HOST="$LIVE_HOST";  UNIT=rust-quant-bot-live.service;  SVC=rust-quant-bot-live ;;
  paper)  HOST="$PAPER_HOST"; UNIT=rust-quant-bot-paper.service; SVC=rust-quant-bot-paper ;;
  radar)  HOST="$RADAR_HOST"; UNIT=rust-quant-bot-radar.service; SVC=rust-quant-bot-radar ;;
  *) echo "rôle inconnu: $ROLE"; exit 1 ;;
esac

echo "▶ déploiement $ROLE → $HOST:$REMOTE_DIR"
# 1. Sources (pour build live distant) + frontend + unit + Cargo.
rsync "${RSYNC_OPTS[@]}" --exclude 'target/' --exclude '.git/' ./ "$HOST:$REMOTE_DIR/"
[[ "$DRY" == "--dry-run" ]] && { echo "(dry-run : pas de build/restart)"; exit 0; }

# 2. Build distant selon le rôle, install unit, restart.
if [[ "$ROLE" == "live" ]]; then
  ssh "$HOST" "cd $REMOTE_DIR && RUSTFLAGS='-C target-cpu=native' cargo build --release --features live"
else
  ssh "$HOST" "cd $REMOTE_DIR && RUSTFLAGS='-C target-cpu=native' cargo build --release"
fi
ssh "$HOST" "sudo cp $REMOTE_DIR/deploy/$UNIT /etc/systemd/system/$UNIT && sudo systemctl daemon-reload && sudo systemctl restart $SVC && systemctl --no-pager status $SVC | head -5"
echo "✓ $ROLE déployé et redémarré."
