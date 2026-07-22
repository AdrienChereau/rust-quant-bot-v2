# Chasse aux baleines — scan rétroactif du « banging the close » sur les
# fenêtres BTC 5 min. Pour chaque fenêtre des dernières 48 h :
#  1) Binance 1 s : un MARTEAU tardif (≥ SEUIL_MOVE $ en ≤5 s dans les
#     25 dernières secondes) qui RETOURNE le gagnant vs strike (open fenêtre) ;
#  2) si marteau : trades Polymarket par wallet — accumulation du côté
#     finalement GAGNANT à ≤ 0.45 entre T−180 s et T−15 s (le pré-positionnement).
import json, time, urllib.request, sys
from collections import defaultdict

SEUIL_MOVE = 18.0     # $ de mouvement en ≤5 s
SEUIL_WHALE = 150.0   # $ de coût pré-positionné par wallet
PX_CHEAP = 0.45
HOURS = 48

def get(url, retry=2):
    for i in range(retry + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "scan/1.0"})
            return json.load(urllib.request.urlopen(req, timeout=10))
        except Exception:
            if i == retry:
                return None
            time.sleep(0.6)

def market_of(start):
    ev = get(f"https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{start}")
    if not ev or not isinstance(ev, list) or not ev:
        return None
    try:
        m = ev[0]["markets"][0]
        outc = json.loads(m["outcomes"])
        pr = json.loads(m.get("outcomePrices", '["",""]'))
        res = None
        if pr and pr[0] in ("0", "1"):
            res = "Up" if pr[outc.index("Up")] == "1" else "Down"
        return dict(cid=m["conditionId"], res=res)
    except Exception:
        return None

def closes(start):
    kl = get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s"
             f"&startTime={start*1000}&endTime={(start+300)*1000}&limit=1000")
    if not kl or len(kl) < 290:
        return None, None
    return float(kl[0][1]), [float(k[4]) for k in kl[:300]]

def trades_of(cid, start):
    out = []
    for off in (0, 500, 1000):
        t = get(f"https://data-api.polymarket.com/trades?market={cid}&limit=500&offset={off}")
        if not t:
            break
        out += t
        if len(t) < 500:
            break
        time.sleep(0.15)
    return out

now = int(time.time())
first = (now - HOURS * 3600) // 300 * 300
wins = list(range(first, (now // 300 - 3) * 300, 300))
print(f"scan de {len(wins)} fenêtres ({HOURS} h)", flush=True)

hammered = []
whale_totals = defaultdict(lambda: dict(n=0, cost=0.0, profit=0.0))
n_res = 0
sample_keys_shown = False
for i, start in enumerate(wins):
    if i % 100 == 0:
        print(f"  ... {i}/{len(wins)}", flush=True)
    strike, c = closes(start)
    if not c:
        continue
    # marteau : max |Δ 5 s| dans les 25 dernières secondes + retournement vs strike
    mx, at = 0.0, 0
    for t in range(276, len(c)):
        d = abs(c[t] - c[t - 5])
        if d > mx:
            mx, at = d, t
    before = c[273] - strike
    after = c[-1] - strike
    flipped = (before > 0) != (after > 0) and abs(before) > 1e-9
    if mx < SEUIL_MOVE or not flipped:
        continue
    mk = market_of(start)
    time.sleep(0.12)
    if not mk or not mk["res"]:
        continue
    n_res += 1
    trs = trades_of(mk["cid"], start)
    whales = defaultdict(lambda: dict(cost=0.0, sz=0.0, n=0, pmin=1.0, tmin=999, tmax=0))
    for tr in trs:
        try:
            if not sample_keys_shown:
                print("  [schéma trade]", sorted(tr.keys()), flush=True)
                sample_keys_shown = True
            ts = int(tr.get("timestamp", 0))
            rel = ts - start
            if not (120 <= rel <= 285):
                continue
            if str(tr.get("side", "")).upper() != "BUY":
                continue
            if str(tr.get("outcome", "")) != mk["res"]:
                continue
            px = float(tr.get("price", 1))
            if px > PX_CHEAP:
                continue
            sz = float(tr.get("size", 0))
            w = tr.get("proxyWallet") or tr.get("maker") or "?"
            d = whales[w]
            d["cost"] += px * sz
            d["sz"] += sz
            d["n"] += 1
            d["pmin"] = min(d["pmin"], px)
            d["tmin"] = min(d["tmin"], rel)
            d["tmax"] = max(d["tmax"], rel)
        except Exception:
            continue
    big = {w: d for w, d in whales.items() if d["cost"] >= SEUIL_WHALE}
    rec = dict(start=start, hhmm=time.strftime("%d/%m %H:%M", time.gmtime(start)),
               move=round(mx, 1), t_move=at, strike=round(strike, 1),
               res=mk["res"], whales=[])
    for w, d in sorted(big.items(), key=lambda x: -x[1]["cost"]):
        profit = d["sz"] * 1.0 - d["cost"]
        rec["whales"].append(dict(w=w[:10], cost=round(d["cost"]), sz=round(d["sz"]),
                                  avg=round(d["cost"] / d["sz"], 2), n=d["n"],
                                  t=f"{d['tmin']}-{d['tmax']}s", profit=round(profit)))
        wt = whale_totals[w]
        wt["n"] += 1
        wt["cost"] += d["cost"]
        wt["profit"] += profit
    hammered.append(rec)

print(f"\n=== RÉSULTAT : {len(hammered)} fenêtres MARTELÉES (retournement tardif ≥ {SEUIL_MOVE}$) sur {len(wins)} scannées ===")
with_wh = [h for h in hammered if h["whales"]]
print(f"dont PRÉ-POSITIONNÉES par ≥1 wallet ({SEUIL_WHALE}$+ du côté gagnant ≤ {PX_CHEAP}) : {len(with_wh)}")
for h in hammered:
    tag = " 🐋" if h["whales"] else ""
    print(f"\n{h['hhmm']} UTC · marteau {h['move']}$ à t+{h['t_move']}s · gagnant {h['res']}{tag}")
    for w in h["whales"]:
        print(f"   {w['w']}… coût {w['cost']}$ ({w['sz']} parts avg {w['avg']}, {w['n']} fills, t+{w['t']}) → profit +{w['profit']}$")
print("\n=== RÉCIDIVISTES ===")
for w, d in sorted(whale_totals.items(), key=lambda x: -x[1]["profit"])[:10]:
    print(f"{w} : {d['n']} fenêtres, coût total {d['cost']:.0f}$, profit total +{d['profit']:.0f}$")
