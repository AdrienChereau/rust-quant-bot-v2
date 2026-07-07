#!/usr/bin/env bash
# Déploiement sur un nœud : ./deploy.sh radar|executor
# Prérequis : repo cloné dans ~/rust-quant-bot-v2, .env rempli dans backend/.
set -euo pipefail
ROLE=${1:?usage: ./deploy.sh radar|executor|live}
REPO=~/rust-quant-bot-v2
cd "$REPO"
git pull --ff-only
cd backend
mkdir -p data
if [ "$ROLE" = "live" ]; then
  cargo build --release --features live   # rustc >= 1.91 requis (AWS OK)
  # Binaire DÉDIÉ : un deploy paper/radar ne peut plus écraser le live.
  # cp direct = "Text file busy" si le service tourne → copie temporaire + mv
  # (rename atomique, remplace l'entrée même si l'ancien binaire s'exécute).
  cp target/release/polymarket_mm_bot target/release/.pmb-live.tmp
  mv target/release/.pmb-live.tmp target/release/polymarket_mm_bot-live
else
  cargo build --release
fi
sudo cp "$REPO/deploy/poly-$ROLE.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable "poly-$ROLE"
sudo systemctl restart "poly-$ROLE"
sleep 3
systemctl status "poly-$ROLE" --no-pager -l | head -12
