import json, glob, statistics as st

def pct(x): return f'{100*x:.0f}%'
wins = []
for f in sorted(glob.glob('/home/ubuntu/rust-quant-bot-v2/analysis/observer/data/window_*.json')):
    d = json.load(open(f))
    rec, fills = d.get('rec', {}), d.get('fills') or []
    imb, pxp = d.get('imb_path') or [], d.get('px_path') or []
    if not fills or not imb or len(fills) < 30 or 'close' not in rec or 'strike' not in rec:
        continue
    up_won = rec['close'] > rec['strike']
    su = sum(x['sz'] for x in fills if x['side'] == 'Up')
    sd = sum(x['sz'] for x in fills if x['side'] == 'Down')
    cu = sum(x['sz'] * x['px'] for x in fills if x['side'] == 'Up')
    cd = sum(x['sz'] * x['px'] for x in fills if x['side'] == 'Down')
    if su < 1 or sd < 1:
        continue
    pair = cu / su + cd / sd
    winner_sz = su if up_won else sd
    pnl = winner_sz - (cu + cd)
    absmax = max(abs(v) for _, v in imb)
    tmax = [t for t, v in imb if abs(v) == absmax][0]
    sgn_at_max = 1 if dict(imb)[tmax] > 0 else -1
    est = [(t, v) for t, v in imb if abs(v) >= 40]
    t_est, sgn_est = (est[0][0], 1 if est[0][1] > 0 else -1) if est else (None, 0)

    def upx(t):
        if not pxp:
            return None
        c = min(pxp, key=lambda r: abs(r[0] - t))
        return c[1]

    p_est = upx(t_est) if t_est is not None else None
    lead_px_est = (p_est if sgn_est > 0 else (1 - p_est)) if p_est is not None else None
    flips = []
    state = 0
    for t, v in imb:
        if v >= 40 and state <= 0:
            if state == -1:
                flips.append((t, upx(t)))
            state = 1
        elif v <= -40 and state >= 0:
            if state == 1:
                flips.append((t, upx(t)))
            state = -1
    correct = (sgn_at_max > 0) == up_won
    tail = [v for t, v in imb if t >= 235]
    mid = [v for t, v in imb if 200 <= t < 235]
    conv = (abs(mid[-1]) - abs(tail[-1])) if tail and mid else None
    finimb = imb[-1][1]
    resid_winner = ((finimb > 0) == up_won) if abs(finimb) > 5 else None
    vol = su + sd
    ext = sum(x['sz'] for x in fills if x['px'] >= 0.90 or x['px'] <= 0.10)
    late = sum(x['sz'] for x in fills if x['t'] >= 240)
    wins.append(dict(pair=pair, pnl=pnl, absmax=absmax, t_est=t_est, lead_px_est=lead_px_est,
                     nflips=len(flips), flips=flips, correct=correct, finimb=finimb,
                     resid_winner=resid_winner, conv=conv, extfrac=ext / vol,
                     latefrac=late / vol, ratio=absmax / max(su, sd)))

n = len(wins)
print('fenêtres analysées:', n)
cor = [w for w in wins if w['correct']]
inc = [w for w in wins if not w['correct']]
print(f"précision du flotteur (signe à |imb|max vs gagnant): {pct(len(cor)/n)}")
print(f"PnL moyen appel juste: {st.mean(w['pnl'] for w in cor):+.0f}$ | appel faux: {st.mean(w['pnl'] for w in inc):+.0f}$")
print(f"PnL total: {sum(w['pnl'] for w in wins):+.0f}$ sur {n} fen. | fen. gagnantes: {pct(len([w for w in wins if w['pnl']>0])/n)}")
q = st.quantiles([w['pair'] for w in wins], n=4)
print(f"coût de paire médian: {st.median(w['pair'] for w in wins)*100:.1f}c | p25-p75: {q[0]*100:.1f}-{q[2]*100:.1f}")
print(f"flotteur max |imb| médian: {st.median(w['absmax'] for w in wins):.0f} parts | ratio/volume côté: {pct(st.median(w['ratio'] for w in wins))}")
ests = [w['t_est'] for w in wins if w['t_est'] is not None]
print(f"établissement (|imb|>=40): {pct(len(ests)/n)} des fen., t médian: {st.median(ests):.0f}s")
lps = [w['lead_px_est'] for w in wins if w['lead_px_est']]
qq = st.quantiles(lps, n=4)
print(f"prix du leader à l'établissement: médian {st.median(lps)*100:.0f}c | p25-p75 {qq[0]*100:.0f}-{qq[2]*100:.0f}c")
nf = [w['nflips'] for w in wins]
print(f"flips (±40 croisés): 0 flip: {pct(nf.count(0)/n)} | 1: {pct(nf.count(1)/n)} | >=2: {pct(len([x for x in nf if x>=2])/n)}")
allf = [(t, p) for w in wins for t, p in w['flips']]
if allf:
    fps = [p for _, p in allf if p is not None]
    px_msg = f" | prix Up au flip médian: {st.median(fps)*100:.0f}c" if fps else ""
    print(f"  moment des flips: médian t={st.median([t for t,_ in allf]):.0f}s{px_msg}")
fw = [w for w in wins if w['nflips'] >= 1]
if fw:
    print(f"  PnL fen. avec flip: {st.mean(w['pnl'] for w in fw):+.0f}$ vs sans flip: {st.mean(w['pnl'] for w in wins if w['nflips']==0):+.0f}$")
rw = [w for w in wins if w['resid_winner'] is not None]
print(f"résidu final >5 parts: {pct(len(rw)/n)} des fen. ; du côté GAGNANT: {pct(len([w for w in rw if w['resid_winner']])/len(rw))}")
convs = [w['conv'] for w in wins if w['conv'] is not None]
print(f"conversion T-60: |imb| réduite en moyenne de {st.mean(convs):.0f} parts (médiane {st.median(convs):.0f})")
print(f"volume aux extrêmes (>=90c ou <=10c): médian {pct(st.median(w['extfrac'] for w in wins))} | volume après t+240: {pct(st.median(w['latefrac'] for w in wins))}")
print(f"|imb| finale médiane: {st.median(abs(w['finimb']) for w in wins):.0f} parts")
