#!/usr/bin/env python3
"""Dérive les credentials L2 Polymarket (POLY_API_KEY/SECRET/PASSPHRASE).

Usage:
  python3 -m venv .venv && source .venv/bin/activate
  pip install -r scripts/requirements.txt
  set -a && source .env.local && set +a   # ou export manuel des POLY_*
  python scripts/derive_poly_creds.py

Variables requises : POLY_PRIVATE_KEY, POLY_FUNDER_ADDRESS
Optionnel : POLY_SIG_TYPE (défaut 2 = Gnosis Safe proxy Polymarket)
"""

import os
import sys
from pathlib import Path

try:
    from py_clob_client.client import ClobClient
except ImportError:
    print("Installe d'abord : pip install -r scripts/requirements.txt", file=sys.stderr)
    sys.exit(1)

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


def load_dotenv() -> None:
    """Charge .env puis .env.local (local écrase .env) si présents."""
    root = Path(__file__).resolve().parent.parent
    for name in (".env", ".env.local"):
        path = root / name
        if not path.is_file():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            if key and val and key not in os.environ:
                os.environ[key] = val


def main() -> None:
    load_dotenv()

    private_key = os.environ.get("POLY_PRIVATE_KEY", "").strip()
    funder = os.environ.get("POLY_FUNDER_ADDRESS", "").strip()
    sig_type = int(os.environ.get("POLY_SIG_TYPE", "2"))

    if not private_key or not funder:
        print(
            "Variables requises : POLY_PRIVATE_KEY, POLY_FUNDER_ADDRESS\n"
            "Optionnel : POLY_SIG_TYPE (défaut 2)",
            file=sys.stderr,
        )
        sys.exit(1)

    if not private_key.startswith("0x"):
        private_key = f"0x{private_key}"

    client = ClobClient(
        HOST,
        key=private_key,
        chain_id=CHAIN_ID,
        signature_type=sig_type,
        funder=funder,
    )
    creds = client.create_or_derive_api_creds()

    print("# Copie ces valeurs dans .env (ne pas committer)")
    print(f"POLY_API_KEY={creds.api_key}")
    print(f"POLY_API_SECRET={creds.api_secret}")
    print(f"POLY_PASSPHRASE={creds.api_passphrase}")


if __name__ == "__main__":
    main()
