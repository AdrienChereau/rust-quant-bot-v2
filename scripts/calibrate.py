#!/usr/bin/env python3
"""Calibration offline — le juge de l'edge.

Lit data/windows.jsonl (samples 1 Hz + outcomes par fenêtre) et répond à LA question :
« notre probabilité bat-elle le prix Polymarket ? » — au Brier score, sur données réelles.

Sorties :
  1. Brier(fair loggé) vs Brier(prix PM) vs Brier(0.5) — global + par tranche de temps restant.
  2. Grid-search (sigma, gamma) minimisant le Brier — TRAIN sur fenêtres paires,
     TEST sur fenêtres impaires (anti-surapprentissage).
  3. Table d'EV : pour chaque seuil de divergence |p_cal − prix|, l'EV réalisé moyen par
     trade hold-to-resolution (frais taker inclus) et le nombre d'opportunités.

Usage : python3 scripts/calibrate.py [data/windows.jsonl] [--fee-bps 10]
Stdlib uniquement (math, json).
"""
import json, math, sys
from collections import defaultdict

SECONDS_PER_YEAR = 365.0 * 24 * 3600

def phi(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def fair(spot, strike, sigma, remaining_s, gamma, score):
    t = max(remaining_s, 1) / SECONDS_PER_YEAR
    if spot <= 0 or strike <= 0 or sigma <= 0:
        return 0.5
    d2 = (math.log(spot / strike) - 0.5 * sigma * sigma * t) / (sigma * math.sqrt(t))
    return phi(d2 + gamma * score)

def brier(pairs):  # pairs = [(p, y)] ; y ∈ {0,1}
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs) if pairs else float("nan")

def bucket(rs):
    if rs > 240: return "240-300s"
    if rs > 180: return "180-240s"
    if rs > 120: return "120-180s"
    if rs > 60:  return "60-120s"
    return "0-60s"

def main():
    path = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "data/windows.jsonl"
    fee_bps = 10.0
    if "--fee-bps" in sys.argv:
        fee_bps = float(sys.argv[sys.argv.index("--fee-bps") + 1])
    fee = fee_bps / 10_000.0

    outcomes, samples = {}, []
    with open(path) as f:
        for line in f:
            try: r = json.loads(line)
            except json.JSONDecodeError: continue
            if r.get("kind") == "outcome":
                outcomes[r["window_ts"]] = 1.0 if r["up"] else 0.0
            elif r.get("kind") == "sample":
                samples.append(r)

    rows = [s for s in samples if s["window_ts"] in outcomes
            and s["spot"] > 0 and s["strike"] > 0 and 0.01 <= s["real"] <= 0.99]
    for s in rows:
        s["y"] = outcomes[s["window_ts"]]
    nw = len({s["window_ts"] for s in rows})
    print(f"fenêtres résolues: {nw} | samples exploitables: {len(rows)} | frais: {fee_bps} bps")
    if nw < 30:
        print(f"⚠️  <30 fenêtres — laisser tourner l'enregistreur avant de conclure.")
    if not rows:
        return

    # ── 1. Brier de l'existant ────────────────────────────────────────────────
    print("\n── Brier (plus bas = mieux) ──")
    print(f"{'tranche':>10} | {'n':>6} | {'PM (prix)':>9} | {'fair loggé':>10} | {'0.5':>6}")
    groups = defaultdict(list)
    for s in rows: groups[bucket(s["remaining_s"])].append(s)
    for b in ["240-300s", "180-240s", "120-180s", "60-120s", "0-60s"]:
        g = groups.get(b, [])
        if not g: continue
        print(f"{b:>10} | {len(g):>6} | {brier([(s['real'],s['y']) for s in g]):>9.4f} |"
              f" {brier([(s['fair'],s['y']) for s in g]):>10.4f} | {brier([(0.5,s['y']) for s in g]):>6.4f}")
    print(f"{'GLOBAL':>10} | {len(rows):>6} | {brier([(s['real'],s['y']) for s in rows]):>9.4f} |"
          f" {brier([(s['fair'],s['y']) for s in rows]):>10.4f} | {brier([(0.5,s['y']) for s in rows]):>6.4f}")

    # ── 2. Grid-search (sigma, gamma) — train pair / test impair ─────────────
    train = [s for s in rows if (s["window_ts"] // 300) % 2 == 0]
    test  = [s for s in rows if (s["window_ts"] // 300) % 2 == 1]
    best = (None, None, float("inf"))
    for sg in [x / 100 for x in range(10, 205, 5)]:          # sigma 0.10 → 2.00
        for gm in [0.0, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0]:
            b = brier([(fair(s["spot"], s["strike"], sg, s["remaining_s"], gm, s["score"]), s["y"])
                       for s in train])
            if b < best[2]: best = (sg, gm, b)
    sg, gm, btrain = best
    btest = brier([(fair(s["spot"], s["strike"], sg, s["remaining_s"], gm, s["score"]), s["y"]) for s in test])
    bpm_test = brier([(s["real"], s["y"]) for s in test])
    print(f"\n── Calibration (train {len(train)} / test {len(test)}) ──")
    print(f"σ* = {sg:.2f} ({sg*100:.0f}% annualisé) | γ* = {gm:.2f} | Brier train {btrain:.4f}")
    print(f"TEST : fair calibré {btest:.4f} vs prix PM {bpm_test:.4f} → "
          + ("✅ ON BAT LE MARCHÉ" if btest < bpm_test else "❌ le marché nous bat — pas d'edge de calibration"))

    # ── 3. Table d'EV hold-to-resolution (sur TEST uniquement) ───────────────
    print(f"\n── EV hold-to-resolution (test, frais {fee_bps} bps) ──")
    print(f"{'seuil':>6} | {'trades':>6} | {'EV moyen $/1$':>13} | {'EV total':>8}")
    for thr in [0.03, 0.05, 0.08, 0.10, 0.15]:
        evs = []
        for s in test:
            p = fair(s["spot"], s["strike"], sg, s["remaining_s"], gm, s["score"])
            up_px, down_px = s["real"], 1.0 - s["real"]
            if p - up_px > thr and up_px > 0.01:      # acheter UP
                ev = (1.0 - up_px if s["y"] == 1.0 else -up_px) - fee * up_px
                evs.append(ev)
            elif (1.0 - p) - down_px > thr and down_px > 0.01:  # acheter DOWN
                ev = (1.0 - down_px if s["y"] == 0.0 else -down_px) - fee * down_px
                evs.append(ev)
        if evs:
            print(f"{thr:>6.2f} | {len(evs):>6} | {sum(evs)/len(evs):>+13.4f} | {sum(evs):>+8.2f}")
        else:
            print(f"{thr:>6.2f} | {0:>6} | {'—':>13} | {'—':>8}")
    print("\nNB: samples ≠ trades indépendants (plusieurs samples/fenêtre). L'EV moyen est le bon")
    print("    indicateur ; le total est optimiste. Conclure sur ≥300 fenêtres résolues (~25h).")

if __name__ == "__main__":
    main()
