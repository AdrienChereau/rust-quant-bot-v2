#!/usr/bin/env python3
"""
Phase 2 — Harness de validation du signal (juge de paix avant le port Rust).

Rejoue N fenetres BTC 5min reelles (Binance 1s + historique carnet Polymarket)
et compare le PnL du market making sous plusieurs variantes de juste valeur p_up,
pour verifier que le TERME DE DRIFT corrige les fenetres en tendance (cf. 9:20).

ATTENTION — modele de fill OPTIMISTE (on fill au mid des que mid croise fair±M).
Les ROI absolus NE SONT PAS executables. Seule la comparaison relative (A vs B vs C)
est valide : elle isole l'effet du signal a modele de fill constant.
Le fill realiste (queue-position, garde-fou 4) arrive en Phase 3.

Resultat mesure (27 fenetres, 1er juillet 2026) :
  A baseline (sans drift) : ROI +5.5%  , 15/27 gagnantes
  B + drift               : ROI +10.2% , 20/27 gagnantes   <- gain principal
  C + drift + regime      : ROI +10.5% , 20/27 gagnantes   (gate quasi inerte)
  Par type : TENDANCE base/paire 91c -> 78c (le drift fait le travail).
  Pertes residuelles = base>100c en marche plat = probleme de fill, pas de signal.

Usage: python3 replay_harness.py [n_windows]
"""
import json, subprocess, math, statistics, sys

def curl(u):
    return subprocess.run(["curl", "-s", "--max-time", "25", u], capture_output=True, text=True).stdout

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

# --- parametres strategie (a porter en Rust une fois valides) ---
MARGIN = 0.05          # marge sous la juste valeur pour quoter
CAP = 300              # cap unites par cote
LOT = 8                # taille par fill
STOP_OPEN = 270        # arret d'ouverture (T-30s)
DRIFT_HALFLIFE = 25.0  # halflife (s) de l'EMA de drift mu
DRIFT_CLAMP_SIG = 4.0  # clamp du drift a +/- N * sigma*sqrt(tau)
REGIME_THR = 1.0       # seuil de regime en unites sigma (0 = desactive)

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
    up_tok = toks[outc.index("Up")]
    ph = get(f"https://clob.polymarket.com/prices-history?market={up_tok}&startTs={start-60}&endTs={end+60}&fidelity=1")
    hs = sorted(ph.get("history", []), key=lambda x: x["t"]) if ph else []
    kl = get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s&startTime={start*1000}&endTime={end*1000}&limit=1000")
    if len(hs) < 4 or not kl:
        return None
    return dict(O=float(kl[0][1]), spot=[float(k[4]) for k in kl],
                poly=[[h["t"] - start, h["p"]] for h in hs],
                up_win=(pr[outc.index("Up")] == "1"), start=start,
                title=ev[0]["title"].replace("Bitcoin Up or Down - ", ""))

def sim(w, use_drift, reg_thr):
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
    sig = statistics.pstdev(rs) or 1e-6
    alpha = 1 - 0.5 ** (1 / DRIFT_HALFLIFE)
    mu = 0.0
    upU = upC = dnU = dnC = 0
    for t in range(end + 1):
        s = spot[min(t, len(spot) - 1)]
        tau = max(end - t, 1)
        if t > 0:
            mu = alpha * rs[min(t - 1, len(rs) - 1)] + (1 - alpha) * mu
        drift = clamp(mu * tau, -DRIFT_CLAMP_SIG * sig * math.sqrt(tau), DRIFT_CLAMP_SIG * sig * math.sqrt(tau)) if use_drift else 0.0
        fair = clamp(Phi((math.log(s / O) + drift) / (sig * math.sqrt(tau))), 0.01, 0.99)
        cum = math.log(s / O) / (sig * math.sqrt(max(t, 1)))  # mouvement cumule en sigma
        strong_up = reg_thr > 0 and cum > reg_thr
        strong_dn = reg_thr > 0 and cum < -reg_thr
        mid = pu(t); buyUp = fair - MARGIN; buyDn = fair + MARGIN
        if t < STOP_OPEN:
            if mid <= buyUp and upU < CAP and not strong_dn:
                upU += LOT; upC += LOT * mid
            if mid >= buyDn and dnU < CAP and not strong_up:
                dnU += LOT; dnC += LOT * (1 - mid)
    pnl = (upU * (1 if w["up_win"] else 0) - upC) + (dnU * (0 if w["up_win"] else 1) - dnC)
    return dict(pnl=pnl, cap=upC + dnC, basis=(upC / upU + dnC / dnU) if upU and dnU else 0)

def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 27
    now = int(subprocess.run(["date", "+%s"], capture_output=True, text=True).stdout.strip())
    base = now // 300 * 300
    wins = []
    for k in range(3, 3 + n + 10):
        w = fetch(base - 300 * k)
        if w:
            wins.append(w)
        if len(wins) >= n:
            break
    print(f"fenetres chargees: {len(wins)}  (fill OPTIMISTE - comparaison relative seule)\n")
    for name, ud, rt in [("A baseline (no drift)", False, 0), ("B drift", True, 0), ("C drift+regime", True, REGIME_THR)]:
        tp = tc = pos = 0
        for w in wins:
            r = sim(w, ud, rt); tp += r["pnl"]; tc += r["cap"]; pos += r["pnl"] > 0
        print(f"{name:24} ROI {tp/tc*100 if tc else 0:+6.1f}%  gagnantes {pos:3}/{len(wins)}  PnL ${tp:+.0f}")

if __name__ == "__main__":
    main()
