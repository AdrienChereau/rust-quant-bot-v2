#!/usr/bin/env python3
"""
Phase A (plan v5) — Validation offline de la strategie SPREAD-CAPTURE TAKER
(guide polyresearchrobotics) sur fenetres BTC 5min reelles, AVANT tout code bot.

Regles simulees (guide) :
  - taker : on achete un cote quand son ask (approx mid+1c) passe sous le plafond
  - plafond blended : ask_side <= C_eff − avg_autre_cote  (jamais les deux d'un coup)
  - premiere jambe d'un cote : ask <= OPENING_LEG_MAX (0.55)
  - clips profondeur : clip = BASE + DEPTH_GAIN*(plafond − ask), borne MAX_CLIP,
    MAX_CLIP_USDC, re-equilibrage imbalance <= MAX_IMBALANCE
  - pas de sortie : on tient jusqu'a la resolution (winner paie 1$)
Variante B (notre sauce ⚡) : + gate Binance  ask <= fair_drift(cote) − GATE_M
  (fair avec drift EMA halflife 25s, clampe ±4σ√τ — valide en Phase 2 : +5.5→+10.2%).

Frais : 3 scenarios (le vrai bareme est incertain — taker_base_fee=1000 bps declare,
mais fee reellement facture invisible sur le tape public) :
  fee0   : 0
  fee3c  : 1.5c/share par jambe (≈ FEE_PER_PAIR 3c du guide)
  feeFull: 0.10 × min(p,1−p) par share (formule officielle a 1000 bps)

LIMITES (honnetete) : prix Polymarket = historique 1-min interpole → les creux
intra-minute sont lisses (sous-estime les opportunites ET la nervosite reelle).
Ask approxime = mid + 1c. Resultats = tendance directionnelle, pas PnL executable.

Usage: python3 spread_capture_replay.py [n_fenetres=100]
"""
import json, math, statistics, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor

# --- parametres du guide (priors) ---
C_RAW = 0.95
OPENING_LEG_MAX = 0.55
MAX_IMBALANCE = 40.0
BASE_CLIP = 10.0
MAX_CLIP = 20.0
DEPTH_GAIN = 60.0
MAX_CLIP_USDC = 6.0
MAX_CAPITAL_PER_MARKET = 20.0
MIN_SECONDS = 10
CLIP_INTERVAL_S = 15          # cadence min entre 2 clips d'un meme cote
SPREAD_HALF = 0.01            # ask ≈ mid + 1c (approximation, cf. LIMITES)
# --- notre sauce ---
GATE_M = 0.04                 # ask <= fair_drift − 4c
DRIFT_HALFLIFE = 25.0
DRIFT_CLAMP_K = 4.0

def curl(u):
    return subprocess.run(["curl", "-s", "--max-time", "20", u], capture_output=True, text=True).stdout

def get(u):
    try:
        return json.loads(curl(u))
    except Exception:
        return None

def erf(x):
    s = -1 if x < 0 else 1
    x = abs(x)
    t = 1 / (1 + 0.3275911 * x)
    y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * math.exp(-x * x)
    return s * y

def Phi(x):
    return 0.5 * (1 + erf(x / math.sqrt(2)))

def clamp(v, a, b):
    return max(a, min(b, v))

def fetch(start):
    end = start + 300
    ev = get(f"https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{start}")
    if not ev:
        return None
    m = ev[0]["markets"][0]
    try:
        outc = json.loads(m["outcomes"]); toks = json.loads(m["clobTokenIds"]); pr = json.loads(m["outcomePrices"])
    except Exception:
        return None
    if pr[0] not in ("0", "1"):
        return None
    ph = get(f"https://clob.polymarket.com/prices-history?market={toks[outc.index('Up')]}&startTs={start-60}&endTs={end+60}&fidelity=1")
    hs = sorted(ph.get("history", []), key=lambda x: x["t"]) if ph else []
    kl = get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s&startTime={start*1000}&endTime={end*1000}&limit=1000")
    if len(hs) < 4 or not kl:
        return None
    return dict(start=start, O=float(kl[0][1]), spot=[float(k[4]) for k in kl],
                poly=[[h["t"] - start, h["p"]] for h in hs], up_win=(pr[outc.index("Up")] == "1"))

def fee_per_share(model, price):
    if model == "fee0":
        return 0.0
    if model == "fee3c":
        return 0.015
    return 0.10 * min(price, 1.0 - price)  # feeFull (1000 bps officiel)

def sim(w, use_gate, fee_model):
    O, spot, poly, end = w["O"], w["spot"], w["poly"], 300

    def pu(t):
        if t <= poly[0][0]:
            return poly[0][1]
        if t >= poly[-1][0]:
            return poly[-1][1]
        for i in range(1, len(poly)):
            if t <= poly[i][0]:
                a, b = poly[i - 1], poly[i]
                f = (t - a[0]) / (b[0] - a[0])
                return a[1] + f * (b[1] - a[1])

    rs, prev = [], None
    for p in spot:
        if prev is not None:
            rs.append(math.log(p / prev))
        prev = p
    # σ CAUSALE (EWMA λ=0.94 sur les retours PASSÉS uniquement). L'ancienne version
    # utilisait pstdev(toute la fenêtre) = biais de lookahead (~+2,5-3 pts de ROI).
    alpha = 1 - 0.5 ** (1 / DRIFT_HALFLIFE)
    mu = 0.0
    var_ewma = None
    sig = 1e-6
    shares = {"up": 0.0, "dn": 0.0}
    cost = {"up": 0.0, "dn": 0.0}
    fees = 0.0
    last_clip = {"up": -999, "dn": -999}

    for t in range(end + 1):
        s = spot[min(t, len(spot) - 1)]
        if t > 0:
            r = rs[min(t - 1, len(rs) - 1)]
            mu = alpha * r + (1 - alpha) * mu
            var_ewma = r * r if var_ewma is None else 0.94 * var_ewma + 0.06 * r * r
            sig = max(math.sqrt(var_ewma), 1e-6)
        remaining = end - t
        if remaining < MIN_SECONDS:
            break
        tau = max(remaining, 1)
        drift = clamp(mu * tau, -DRIFT_CLAMP_K * sig * math.sqrt(tau), DRIFT_CLAMP_K * sig * math.sqrt(tau))
        fair_up = clamp(Phi((math.log(s / O) + drift) / (sig * math.sqrt(tau))), 0.01, 0.99)
        mid_up = pu(t)
        asks = {"up": mid_up + SPREAD_HALF, "dn": (1.0 - mid_up) + SPREAD_HALF}
        fairs = {"up": fair_up, "dn": 1.0 - fair_up}
        fee_pair = 2 * fee_per_share(fee_model, 0.5) if fee_model != "feeFull" else 0.0
        c_eff = C_RAW - (0.03 if fee_model != "fee0" else 0.0)

        for side, other in (("up", "dn"), ("dn", "up")):
            ask = asks[side]
            if t - last_clip[side] < CLIP_INTERVAL_S:
                continue
            # plafond applicable : premiere jambe vs blended
            if shares[other] > 0:
                ceiling = c_eff - (cost[other] / shares[other])
            else:
                ceiling = OPENING_LEG_MAX
            if ask > ceiling:
                continue
            if use_gate and ask > fairs[side] - GATE_M:
                continue
            # clip profondeur
            clip = clamp(BASE_CLIP + DEPTH_GAIN * (ceiling - ask), 0.0, MAX_CLIP)
            clip = min(clip, MAX_CLIP_USDC / max(ask, 0.01))
            # imbalance apres achat
            imb_after = (shares[side] + clip) - shares[other]
            if imb_after > MAX_IMBALANCE:
                clip = max(0.0, MAX_IMBALANCE + shares[other] - shares[side])
            if cost["up"] + cost["dn"] + clip * ask > MAX_CAPITAL_PER_MARKET:
                clip = max(0.0, (MAX_CAPITAL_PER_MARKET - cost["up"] - cost["dn"]) / ask)
            clip = math.floor(clip)
            if clip < 1:
                continue
            shares[side] += clip
            cost[side] += clip * ask
            fees += clip * fee_per_share(fee_model, ask)
            last_clip[side] = t

    winner = "up" if w["up_win"] else "dn"
    settlement = shares[winner] * 1.0
    deployed = cost["up"] + cost["dn"]
    net = settlement - deployed - fees
    paired = min(shares["up"], shares["dn"])
    pair_cost = (cost["up"] / shares["up"] + cost["dn"] / shares["dn"]) if shares["up"] and shares["dn"] else None
    orphan = abs(shares["up"] - shares["dn"])
    return dict(net=net, deployed=deployed, fees=fees, paired=paired, pair_cost=pair_cost,
                orphan=orphan, traded=deployed > 0)

def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    now = int(time.time())
    base = now // 300 * 300
    starts = [base - 300 * k for k in range(3, 3 + int(n * 1.4))]
    print(f"fetch de ~{len(starts)} fenetres (jusqu'a {n} exploitables)…")
    wins = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for w in ex.map(fetch, starts):
            if w:
                wins.append(w)
    wins = wins[:n]
    print(f"fenetres exploitables : {len(wins)}\n")

    configs = [("A guide pur", False), ("B guide+gate ⚡", True)]
    fee_models = ["fee0", "fee3c", "feeFull"]
    print(f"{'variante':<18}{'frais':<9}{'PnL net':>9}{'deploye':>9}{'ROI':>7}{'med.paire':>10}{'orphel.moy':>11}{'fenetres tradees':>17}")
    results = {}
    for name, gate in configs:
        for fm in fee_models:
            rows = [sim(w, gate, fm) for w in wins]
            traded = [r for r in rows if r["traded"]]
            pnl = sum(r["net"] for r in traded)
            dep = sum(r["deployed"] for r in traded)
            pcs = [r["pair_cost"] for r in traded if r["pair_cost"]]
            orph = statistics.mean([r["orphan"] for r in traded]) if traded else 0
            med = statistics.median(pcs) if pcs else float("nan")
            roi = pnl / dep * 100 if dep else 0
            results[(name, fm)] = (pnl, dep, roi, med)
            print(f"{name:<18}{fm:<9}{pnl:>+9.2f}{dep:>9.0f}{roi:>+6.1f}%{med:>9.3f}${orph:>11.1f}{len(traded):>10}/{len(wins)}")
    print("\nGate Phase A : PnL net > 0 et mediane paire < 0.90 sur B/fee3c →",
          "PASS" if results[("B guide+gate ⚡", "fee3c")][0] > 0 and results[("B guide+gate ⚡", "fee3c")][3] < 0.90 else "FAIL")

if __name__ == "__main__":
    main()
