#!/usr/bin/env python3
"""Re-jugement des trades v1 avec la RÉSOLUTION OFFICIELLE Polymarket (Chainlink).

Nos règlements paper utilisent le spot Binance vs strike — même source que le modèle →
biais possible en notre faveur près du strike. Ce script va chercher l'issue officielle
de chaque fenêtre tradée via `gamma-api.polymarket.com/events/slug/btc-updown-5m-{ts}`
(outcomePrices ["1","0"] = Up gagnant) et recompte W/L + PnL.

Usage : python3 scripts/rejudge.py [data/sniper_trades_v1.jsonl]
Stdlib uniquement.
"""
import json, ssl, sys, time, urllib.request
from datetime import datetime, timezone

FEE_COEF = 0.07
GAMMA = "https://gamma-api.polymarket.com/events/slug/btc-updown-5m-{ts}"

# Python.org sur macOS n'a souvent pas les certificats système : certifi si dispo,
# sinon contexte non vérifié (API publique, lecture seule — acceptable ici).
try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl._create_unverified_context()

def official_up(window_ts, cache={}):
    """True si Up a gagné, False si Down, None si indisponible/non résolu."""
    if window_ts in cache:
        return cache[window_ts]
    try:
        req = urllib.request.Request(
            GAMMA.format(ts=window_ts),
            headers={"User-Agent": "curl/8.4.0", "Accept": "application/json"},  # Cloudflare rejette l'UA urllib
        )
        with urllib.request.urlopen(req, timeout=10, context=SSL_CTX) as r:
            e = json.load(r)
        m = e.get("markets", [{}])[0]
        if m.get("umaResolutionStatus") != "resolved":
            cache[window_ts] = None
            return None
        outcomes = json.loads(m.get("outcomes", "[]"))
        prices = json.loads(m.get("outcomePrices", "[]"))
        up = None
        for o, p in zip(outcomes, prices):
            if o == "Up":
                up = (p == "1")
        cache[window_ts] = up
        return up
    except Exception:
        cache[window_ts] = None
        return None

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "data/sniper_trades_v1.jsonl"
    rows = [json.loads(l) for l in open(path)]
    # apparier fire → resolution dans l'ordre du fichier
    pairs, cur = [], None
    for r in rows:
        if r["kind"] == "fire":
            cur = r
        elif r["kind"] == "resolution" and cur is not None:
            pairs.append((cur, r))
            cur = None
    print(f"trades appariés : {len(pairs)}")

    agree = flip = unknown = 0
    w_off = l_off = 0
    pnl_binance = pnl_off = 0.0
    flips = []
    for f, r in pairs:
        ts = datetime.fromisoformat(f["ts"].replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
        window_ts = int(ts // 300) * 300
        up = official_up(window_ts)
        time.sleep(0.15)  # politesse API
        won_binance = r["pnl"] > 0
        pnl_binance += r["pnl"]
        if up is None:
            unknown += 1
            pnl_off += r["pnl"]  # faute d'info, on garde le verdict Binance
            continue
        won_off = (f["side"] == "up") == up
        cost = f["price"] * f["size"] + FEE_COEF * f["price"] * (1 - f["price"]) * f["size"]
        p_off = (f["size"] if won_off else 0.0) - cost
        pnl_off += p_off
        if won_off:
            w_off += 1
        else:
            l_off += 1
        if won_off == won_binance:
            agree += 1
        else:
            flip += 1
            flips.append((window_ts, f["side"], f["price"], "Binance=W" if won_binance else "Binance=L",
                          "officiel=W" if won_off else "officiel=L"))

    n_known = agree + flip
    print(f"\n── Verdict officiel (Chainlink) vs nos labels (Binance) ──")
    print(f"fenêtres vérifiées : {n_known} | introuvables/non résolues : {unknown}")
    print(f"accord : {agree} | DÉSACCORD (issue flippée) : {flip}"
          + (f"  ({flip/n_known*100:.1f}%)" if n_known else ""))
    if n_known:
        print(f"\nW/L officiel : {w_off} / {l_off}  (hit {w_off/(w_off+l_off)*100:.1f}%)")
        print(f"PnL labels Binance : {pnl_binance:+.2f} $")
        print(f"PnL labels OFFICIELS : {pnl_off:+.2f} $")
    for fl in flips:
        print("  flip:", fl)

if __name__ == "__main__":
    main()
