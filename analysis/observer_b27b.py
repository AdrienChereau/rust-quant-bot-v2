#!/usr/bin/env python3
"""Observatoire LONGUE DUREE du wallet 0xb27b… (le +774k$) sur btc-updown-5m.

Mode campagne (1 semaine par defaut, OBS_DAYS pour changer) :
  - persistant : summary.json est CUMULATIF (recharge au boot, dedoublonne,
    re-resout les fenetres en attente) -> survit aux crashs/redemarrages ;
  - concu pour launchd (KeepAlive) : relance automatique ;
  - fichiers par fenetre ALLEGES (fills reduits a t/side/px/sz) ~30-50 Ko.

Sorties (servies par http.server 8710) :
  observer/data/live.json      — fenetre en cours (~5 s)
  observer/data/summary.json   — toutes les fenetres de la campagne
  observer/data/window_<ts>.json — detail par fenetre
"""
import json, subprocess, time, collections, os

W = "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82"
BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "observer", "data")
os.makedirs(BASE, exist_ok=True)
DAYS = float(os.environ.get("OBS_DAYS", "7"))

def curl(u):
    return subprocess.run(["curl", "-s", "--max-time", "8", u], capture_output=True, text=True).stdout

def get(u):
    try:
        return json.loads(curl(u))
    except Exception:
        return None

def log(m):
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)

def write(name, obj):
    tmp = os.path.join(BASE, name + ".tmp")
    json.dump(obj, open(tmp, "w"))
    os.replace(tmp, os.path.join(BASE, name))

def market_of(start):
    ev = get(f"https://gamma-api.polymarket.com/events?slug=btc-updown-5m-{start}")
    if not ev or not isinstance(ev, list):
        return None
    m = ev[0]["markets"][0]
    outc = json.loads(m["outcomes"]); toks = json.loads(m["clobTokenIds"])
    pr = json.loads(m.get("outcomePrices", '["",""]'))
    res = None
    if pr and pr[0] in ("0", "1"):
        res = "Up" if pr[outc.index("Up")] == "1" else "Down"
    return dict(cid=m["conditionId"], up=toks[outc.index("Up")], dn=toks[outc.index("Down")], res=res)

def best(book):
    try:
        bids = [float(x["price"]) for x in book.get("bids", [])]
        asks = [float(x["price"]) for x in book.get("asks", [])]
        return (max(bids) if bids else 0.0, min(asks) if asks else 0.0)
    except Exception:
        return (0.0, 0.0)

# ── summary cumulatif : rechargement au boot ──
SUMPATH = os.path.join(BASE, "summary.json")
if os.path.exists(SUMPATH):
    try:
        summary = json.load(open(SUMPATH))
    except Exception:
        summary = dict(target=W, windows=[])
else:
    summary = dict(target=W, windows=[])
summary["target"] = W
summary["campaign"] = f"{DAYS:g} jours"
summary.pop("finished", None)
summary.setdefault("windows", [])
done = {w["start"] for w in summary["windows"]}
log(f"campagne {DAYS:g} j — summary rechargé : {len(done)} fenêtres existantes")

def save_summary():
    summary["windows"].sort(key=lambda x: x["start"])
    summary["updated"] = int(time.time())
    write("summary.json", summary)

def try_resolve_unresolved():
    for w in [w for w in summary["windows"] if not w.get("res")]:
        mk = market_of(w["start"])
        if mk and mk["res"]:
            w["res"] = mk["res"]
            settle = w["sh_up"] if mk["res"] == "Up" else w["sh_dn"]
            w["pnl_trading"] = round(settle - w["deployed"], 2)
            log(f"résolu {w['start']}: {mk['res']} → {w['pnl_trading']:+.2f}$")
            save_summary()

def observe_window(start):
    end = start + 300
    slug = f"btc-updown-5m-{start}"
    mk = None
    for _ in range(15):
        mk = market_of(start)
        if mk:
            break
        time.sleep(2)
    if not mk:
        log(f"marché introuvable {slug}")
        return None
    fills = {}
    merges = {}
    books = []
    other = collections.Counter()
    imb_path = []
    px_path = []  # [t, mid_up, mid_dn] pour le monitor de prix
    kl_open = get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s&startTime={start*1000}&endTime={start*1000+2000}&limit=2")
    strike = float(kl_open[0][1]) if kl_open else 0.0
    last_book = last_act = last_live = 0.0
    while time.time() < end + 8:
        t = time.time()
        if t - last_book >= 3:
            last_book = t
            bu = get(f"https://clob.polymarket.com/book?token_id={mk['up']}")
            bd = get(f"https://clob.polymarket.com/book?token_id={mk['dn']}")
            if bu and bd:
                b1, a1 = best(bu); b2, a2 = best(bd)
                books.append((int(t), b1, a1, b2, a2))
                if b1 and a1 and b2 and a2:
                    px_path.append([int(t - start), round((b1 + a1) / 2, 4), round((b2 + a2) / 2, 4)])
        if t - last_act >= 4:
            last_act = t
            acts = get(f"https://data-api.polymarket.com/activity?user={W}&limit=300")
            if isinstance(acts, list):
                for a in acts:
                    if not isinstance(a, dict) or int(a.get("timestamp", 0)) < start - 10:
                        continue
                    key = f"{a.get('transactionHash','')}-{a.get('asset','')}-{a.get('side','')}-{a.get('size','')}"
                    if a.get("type") == "TRADE":
                        if a.get("conditionId") == mk["cid"]:
                            fills.setdefault(key, a)
                        else:
                            sl = str(a.get("eventSlug", ""))
                            if "updown" in sl:
                                other[sl.split("-updown")[0] + "|" + key] = 1
                    elif a.get("type") == "MERGE" and a.get("conditionId") == mk["cid"]:
                        merges.setdefault(key, a)
        if t - last_live >= 5:
            last_live = t
            evs = sorted(fills.values(), key=lambda x: int(x["timestamp"]))
            sh = {"Up": 0.0, "Down": 0.0}; cost = {"Up": 0.0, "Down": 0.0}
            for a in evs:
                sh[a["outcome"]] += float(a["size"]); cost[a["outcome"]] += float(a["size"]) * float(a["price"])
            imb = sh["Up"] - sh["Down"]
            imb_path.append([int(t - start), round(imb)])
            au = cost["Up"] / sh["Up"] if sh["Up"] else 0
            ad = cost["Down"] / sh["Down"] if sh["Down"] else 0
            bb = books[-1] if books else (0, 0, 0, 0, 0)
            write("live.json", dict(
                slug=slug, start=start, now=int(t), elapsed=int(t - start), strike=strike,
                fills=len(evs), sh_up=round(sh["Up"]), sh_dn=round(sh["Down"]),
                imb=round(imb), deployed=round(cost["Up"] + cost["Down"], 2),
                avg_up=round(au, 4), avg_dn=round(ad, 4),
                pair_cost=round(au + ad, 4) if sh["Up"] and sh["Down"] else 0,
                merged=round(sum(float(m.get("usdcSize", 0)) for m in merges.values()), 2),
                n_merges=len(merges),
                rebate_est=round(sum(0.15 * 0.07 * float(a["price"]) * (1 - float(a["price"])) * float(a["size"]) for a in evs), 2),
                px_path=px_path[-100:],
                fills_slim=[dict(t=int(a["timestamp"]) - start, side=a["outcome"], px=float(a["price"])) for a in evs][-150:],
                merges_t=[int(m["timestamp"]) - start for m in merges.values()],
                book=dict(bb_up=bb[1], ba_up=bb[2], bb_dn=bb[3], ba_dn=bb[4]),
                last=[dict(t=int(a["timestamp"]) - start, side=a["outcome"],
                           px=float(a["price"]), sz=float(a["size"])) for a in evs[-8:]],
                other=dict(collections.Counter(k.split("|")[0] for k in other)),
                imb_path=imb_path[-60:],
            ))
        time.sleep(0.5)

    evs = sorted(fills.values(), key=lambda x: int(x["timestamp"]))
    sh = {"Up": 0.0, "Down": 0.0}; cost = {"Up": 0.0, "Down": 0.0}
    imb_max = 0.0; run = {"Up": 0.0, "Down": 0.0}
    at_touch = inside = behind_t = 0
    slim_fills = []
    for a in evs:
        o = a["outcome"]; s = float(a["size"]); p = float(a["price"]); ts = int(a["timestamp"])
        sh[o] += s; cost[o] += p * s
        run[o] += s
        if abs(run["Up"] - run["Down"]) > abs(imb_max):
            imb_max = run["Up"] - run["Down"]
        near = min(books, key=lambda x: abs(x[0] - ts)) if books else None
        if near and abs(near[0] - ts) <= 4:
            bb = near[1] if o == "Up" else near[3]
            ba = near[2] if o == "Up" else near[4]
            if abs(p - bb) < 0.0051: at_touch += 1
            elif bb < p < ba: inside += 1
            else: behind_t += 1
        slim_fills.append(dict(t=ts - start, side=o, px=p, sz=s))
    au = cost["Up"] / sh["Up"] if sh["Up"] else 0
    ad = cost["Down"] / sh["Down"] if sh["Down"] else 0
    kl = get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s&startTime={start*1000}&endTime={end*1000}&limit=1000")
    close = float(kl[-1][4]) if kl else 0.0
    rec = dict(
        start=start, slug=slug, strike=strike, close=close,
        fills=len(evs), sh_up=round(sh["Up"]), sh_dn=round(sh["Down"]),
        avg_up=round(au, 4), avg_dn=round(ad, 4),
        pair_cost=round(au + ad, 4) if sh["Up"] and sh["Down"] else 0,
        pairs=round(min(sh["Up"], sh["Down"])),
        imb_max=round(imb_max), imb_final=round(sh["Up"] - sh["Down"]),
        deployed=round(cost["Up"] + cost["Down"], 2),
        merged=round(sum(float(m.get("usdcSize", 0)) for m in merges.values()), 2),
        n_merges=len(merges),
        merge_times=[int(m["timestamp"]) - start for m in sorted(merges.values(), key=lambda x: int(x["timestamp"]))],
        touch=dict(at=at_touch, inside=inside, behind=behind_t),
        other=dict(collections.Counter(k.split("|")[0] for k in other)),
        rebate_est=round(sum(0.15 * 0.07 * f2["px"] * (1 - f2["px"]) * f2["sz"] for f2 in slim_fills), 2),
        res=None, pnl_trading=None,
    )
    json.dump(dict(rec=rec, fills=slim_fills, px_path=px_path,
                   merges=[dict(t=int(m["timestamp"]) - start, usd=float(m.get("usdcSize", 0)))
                           for m in merges.values()],
                   imb_path=imb_path),
              open(os.path.join(BASE, f"window_{start}.json"), "w"))
    return rec

def main():
    end_ts = time.time() + DAYS * 86400
    log(f"observatoire persistant — cible {W[:10]}… — fin de campagne dans {DAYS:g} j")
    save_summary()
    last_resolve = 0.0
    while time.time() < end_ts:
        now = int(time.time())
        cur = now // 300 * 300
        start = cur if (cur + 300 - now) >= 240 and cur not in done else cur + 300
        if start in done:
            start += 300
        while time.time() < start:
            if time.time() - last_resolve > 20:
                last_resolve = time.time()
                try_resolve_unresolved()
            time.sleep(2)
        log(f"fenêtre {len(done)+1} : {start}")
        try:
            rec = observe_window(start)
        except Exception as e:
            log(f"erreur fenêtre {start}: {e}")
            rec = None
        if rec:
            summary["windows"].append(rec)
            done.add(start)
            save_summary()
        try_resolve_unresolved()
    log("campagne terminée.")
    summary["finished"] = True
    save_summary()

if __name__ == "__main__":
    main()
