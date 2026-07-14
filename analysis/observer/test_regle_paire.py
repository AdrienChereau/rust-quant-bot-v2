import json, glob, statistics as st

# Teste : le flotteur est-il conditionné par le coût de paire COURANT ?
#  - pair_run > 1$ -> aligné avec le leader (compenser) ?
#  - pair_run < 1$ -> équilibré/contrarien (loterie du reverse) ?
al = {'>1': [0, 0], '<1': [0, 0]}      # [alignés, total] quand |imb|>=20
mag = {'>1': [], '<1': []}             # |imb| par régime (tous points)
flips_pair = []                        # pair_run au moment des flips ±40

for f in sorted(glob.glob('/home/ubuntu/rust-quant-bot-v2/analysis/observer/data/window_*.json')):
    d = json.load(open(f))
    rec, fills = d.get('rec', {}), d.get('fills') or []
    imb, pxp = d.get('imb_path') or [], d.get('px_path') or []
    if not fills or not imb or not pxp or len(fills) < 30:
        continue
    fills = sorted(fills, key=lambda x: x['t'])

    def pair_run(t):
        su = sd = cu = cd = 0.0
        for x in fills:
            if x['t'] > t:
                break
            if x['side'] == 'Up':
                su += x['sz']; cu += x['sz'] * x['px']
            else:
                sd += x['sz']; cd += x['sz'] * x['px']
        if su < 5 or sd < 5:
            return None
        return cu / su + cd / sd

    def upx(t):
        return min(pxp, key=lambda r: abs(r[0] - t))[1]

    state = 0
    for t, v in imb:
        if t < 60:
            # établissement : on saute, et on suit l'état pour les flips
            if v >= 40: state = 1
            elif v <= -40: state = -1
            continue
        pr = pair_run(t)
        if pr is None:
            continue
        reg = '>1' if pr > 1.005 else ('<1' if pr < 0.995 else None)
        # flips
        if v >= 40 and state <= 0:
            if state == -1:
                flips_pair.append(pr)
            state = 1
        elif v <= -40 and state >= 0:
            if state == 1:
                flips_pair.append(pr)
            state = -1
        if reg is None:
            continue
        mag[reg].append(abs(v))
        u = upx(t)
        leader = 1 if u > 0.52 else (-1 if u < 0.48 else 0)
        if leader != 0 and abs(v) >= 20:
            al[reg][1] += 1
            if (v > 0) == (leader > 0):
                al[reg][0] += 1

for reg in ('>1', '<1'):
    a, n = al[reg]
    m = mag[reg]
    print(f"paire courante {reg}$ : flotteur ALIGNÉ leader {100*a/max(n,1):.0f}% "
          f"({a}/{n} points) | |imb| moyen {st.mean(m):.0f} (médian {st.median(m):.0f})")
if flips_pair:
    print(f"flips : {len(flips_pair)} mesurés, paire courante au flip : "
          f"médiane {st.median(flips_pair)*100:.1f}c | >1$ dans {100*len([p for p in flips_pair if p>1])/len(flips_pair):.0f}% des cas")
