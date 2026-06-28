#!/usr/bin/env bash
# Contrôle d'un nœud. Usage: deploy/ctl.sh <live|paper|radar> <start|stop|restart|status|logs|pause|run>
#  - start/stop/restart/status/logs : systemd (arrêt DUR du process).
#  - pause/run : PAUSE LOGICIELLE via le dashboard (POST /stop | /start) — process reste chaud.
set -euo pipefail
cd "$(dirname "$0")/.."
source deploy/hosts.env

ROLE="${1:?usage: ctl.sh <live|paper|radar> <start|stop|restart|status|logs|pause|run>}"
ACT="${2:?action requise}"

case "$ROLE" in
  live)  HOST="$LIVE_HOST";  SVC=rust-quant-bot-live;  DASH="$LIVE_DASH_PORT" ;;
  paper) HOST="$PAPER_HOST"; SVC=rust-quant-bot-paper; DASH="$PAPER_DASH_PORT" ;;
  radar) HOST="$RADAR_HOST"; SVC=rust-quant-bot-radar; DASH="$RADAR_DASH_PORT" ;;
  *) echo "rôle inconnu: $ROLE"; exit 1 ;;
esac

case "$ACT" in
  start|stop|restart) ssh "$HOST" "sudo systemctl $ACT $SVC && systemctl --no-pager status $SVC | head -5" ;;
  status) ssh "$HOST" "systemctl --no-pager status $SVC | head -12" ;;
  logs)   ssh "$HOST" "tail -n 80 $REMOTE_DIR/data/${SVC#rust-quant-bot-}.log" ;;
  # Pause logicielle : SSH-tunnel local vers le dashboard du nœud (0.0.0.0:DASH).
  pause)  ssh "$HOST" "curl -s -X POST http://127.0.0.1:$DASH/stop"  && echo "" ;;
  run)    ssh "$HOST" "curl -s -X POST http://127.0.0.1:$DASH/start" && echo "" ;;
  *) echo "action inconnue: $ACT"; exit 1 ;;
esac
