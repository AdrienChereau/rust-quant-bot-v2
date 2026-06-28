#!/usr/bin/env bash
# Preflight LIVE : vérifie credentials + balance CLOB AVANT d'armer (LIVE_ARMED=true).
# Usage: deploy/preflight.sh  (s'exécute sur le nœud live, ~/.env chargé)
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/hosts.env

echo "▶ preflight LIVE sur $LIVE_HOST"
ssh "$LIVE_HOST" "cd $REMOTE_DIR && set -a && source .env && set +a && cargo run --release --features live -- poly verify"
echo ""
echo "Si 'OK — balance CLOB : … USDC' s'affiche : credentials valides."
echo "Pour armer l'envoi réel : passer LIVE_ARMED=true dans ~/.env puis: deploy/ctl.sh live restart"
