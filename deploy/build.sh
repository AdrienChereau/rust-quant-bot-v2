#!/usr/bin/env bash
# Build release d'un rôle. Usage: deploy/build.sh <live|paper|radar>
#  - live  : --features live (signing EIP-712 ; requiert rustc >= 1.91).
#  - paper/radar : build par défaut (aucun code live compilé).
set -euo pipefail
cd "$(dirname "$0")/.."

ROLE="${1:?usage: build.sh <live|paper|radar>}"
export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"

case "$ROLE" in
  live)
    echo "▶ build LIVE (--features live, RUSTFLAGS=$RUSTFLAGS)"
    cargo build --release --features live
    ;;
  paper|radar)
    echo "▶ build $ROLE (par défaut, RUSTFLAGS=$RUSTFLAGS)"
    cargo build --release
    ;;
  *) echo "rôle inconnu: $ROLE (attendu live|paper|radar)"; exit 1 ;;
esac
echo "✓ binaire : target/release/rust-quant-bot"
