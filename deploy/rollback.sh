#!/usr/bin/env bash
# Rollback d'un nœud vers un tag git. Usage: deploy/rollback.sh <live|paper|radar> <tag>
# Ex: deploy/rollback.sh live v2.0.0-mono-toggle
# Checkout du tag → rebuild distant → restart. Les data/*.json ne sont jamais touchés.
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/hosts.env

ROLE="${1:?usage: rollback.sh <live|paper|radar> <tag>}"
TAG="${2:?tag git requis (ex: v2.0.0-mono-toggle)}"

case "$ROLE" in
  live)  HOST="$LIVE_HOST";  SVC=rust-quant-bot-live;  FEAT="--features live" ;;
  paper) HOST="$PAPER_HOST"; SVC=rust-quant-bot-paper; FEAT="" ;;
  radar) HOST="$RADAR_HOST"; SVC=rust-quant-bot-radar; FEAT="" ;;
  *) echo "rôle inconnu: $ROLE"; exit 1 ;;
esac

echo "⏪ rollback $ROLE → tag $TAG sur $HOST"
ssh "$HOST" "cd $REMOTE_DIR && git fetch --tags && git checkout $TAG && RUSTFLAGS='-C target-cpu=native' cargo build --release $FEAT && sudo systemctl restart $SVC && systemctl --no-pager status $SVC | head -5"
echo "✓ $ROLE rollback sur $TAG terminé."
