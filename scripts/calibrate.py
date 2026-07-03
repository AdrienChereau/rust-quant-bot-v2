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

def taker_fee(px):
    """Frais taker Polymarket 5-min : 0.07·p·(1−p) — ~1.75 % à p=0.5, →0 aux extrêmes."""
    return 0.07 * px * (1.0 - px)

def main():
    path = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "data/windows.jsonl"

    outcomes, official, samples = {}, {}, []
    with open(path) as f:
        for line in f:
            try: r = json.loads(line)
            except json.JSONDecodeError: continue
            if r.get("kind") == "outcome":
                outcomes[r["window_ts"]] = 1.0 if r["up"] else 0.0
            elif r.get("kind") == "outcome_official":
                official[r["window_ts"]] = 1.0 if r["up"] else 0.0
            elif r.get("kind") == "sample":
                samples.append(r)

    # L'issue OFFICIELLE (résolution Polymarket/Chainlink) prime sur le label Binance.
    both = [w for w in official if w in outcomes]
    mismatch = [w for w in both if official[w] != outcomes[w]]
    if both:
        print(f"labels : {len(official)} officiels, {len(outcomes)} Binance | "
              f"désaccord sur {len(mismatch)}/{len(both)} fenêtres communes")
    outcomes.update(official)

    # Fenêtres à spot FIGÉ (feed Binance mort) = données empoisonnées → exclues.
    from collections import defaultdict as _dd
    spots = _dd(set)
    for s in samples: spots[s["window_ts"]].add(round(s["spot"], 2))
    frozen = {w for w, sp in spots.items() if len(sp) <= 1}
    if frozen:
        print(f"⚠️  {len(frozen)} fenêtres à spot figé (feed mort) exclues de la calibration")

    rows = [s for s in samples if s["window_ts"] in outcomes and s["window_ts"] not in frozen
            and s["spot"] > 0 and s["strike"] > 0 and 0.01 <= s["real"] <= 0.99]
    for s in rows:
        s["y"] = outcomes[s["window_ts"]]
    nw = len({s["window_ts"] for s in rows})
    print(f"fenêtres résolues: {nw} | samples exploitables: {len(rows)} | frais: 0.07·p(1−p)")
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

    # ── 3. EV hold-to-resolution HONNÊTE : 1 trade max/fenêtre, fill au ASK ──
    # - pseudo-réplication tuée : on ne prend que la PREMIÈRE divergence de chaque fenêtre ;
    # - exécution réelle : achat au best ask du book (si absent des vieux logs → mid + ½ spread
    #   supposé de 1 cent) ; frais taker sur le notionnel.
    half_spread = 0.01
    if "--half-spread" in sys.argv:
        half_spread = float(sys.argv[sys.argv.index("--half-spread") + 1])
    by_win = defaultdict(list)
    for s in test:
        by_win[s["window_ts"]].append(s)
    for w in by_win.values():
        w.sort(key=lambda s: s["ts"])

    # Gardes de la stratégie v3 (docs/STRATEGY.md) : τ ∈ [30, 240] s, jamais acheter > 0.93,
    # edge NET de frais 0.07·p(1−p) — le seuil devient p-dépendant automatiquement.
    print(f"\n── EV hold-to-resolution (test, 1 trade/fenêtre, fill au ask, frais 0.07·p(1−p)) ──")
    print(f"{'seuil':>6} | {'fenêtres':>8} | {'EV moyen $/1$':>13} | {'z':>5} | verdict")
    for thr in [0.02, 0.03, 0.05, 0.08, 0.10]:
        evs = []
        for w in by_win.values():
            for s in w:
                if not (30 <= s["remaining_s"] <= 240):
                    continue
                p = fair(s["spot"], s["strike"], sg, s["remaining_s"], gm, s["score"])
                up_px = s.get("up_ask") or (s["real"] + half_spread)
                down_px = s.get("down_ask") or (1.0 - s["real"] + half_spread)
                ev = None
                if 0.01 < up_px <= 0.93 and p - up_px - taker_fee(up_px) > thr:
                    ev = (1.0 - up_px if s["y"] == 1.0 else -up_px) - taker_fee(up_px)
                elif 0.01 < down_px <= 0.93 and (1.0 - p) - down_px - taker_fee(down_px) > thr:
                    ev = (1.0 - down_px if s["y"] == 0.0 else -down_px) - taker_fee(down_px)
                if ev is not None:
                    evs.append(ev)
                    break  # UN SEUL trade par fenêtre — premier signal
        if len(evs) >= 2:
            m = sum(evs) / len(evs)
            var = sum((e - m) ** 2 for e in evs) / (len(evs) - 1)
            z = m / math.sqrt(var / len(evs)) if var > 0 else 0.0
            verdict = "✅ significatif" if z > 1.96 else "bruit"
            print(f"{thr:>6.2f} | {len(evs):>8} | {m:>+13.4f} | {z:>5.2f} | {verdict}")
        else:
            print(f"{thr:>6.2f} | {len(evs):>8} | {'—':>13} | {'—':>5} | échantillon vide")
    print("\nRègle de décision : GO seulement si (a) Brier test < Brier PM, ET (b) une ligne d'EV")
    print("z > 1.96 sur ≥100 fenêtres tradées, ET (c) EV ~monotone en fonction du seuil.")

if __name__ == "__main__":
    main()
