// Copy Maker V8 — dashboard style observatoire, poll /state + /events.
const $ = (id) => document.getElementById(id);
const f = (n, d = 2) => (n == null || isNaN(n)) ? '–' : Number(n).toFixed(d);
const money = (n) => (n >= 0 ? '+' : '') + f(n, 2) + '$';
const cents = (n) => (n == null || isNaN(n) || n === 0) ? '–' : f(n * 100, 1) + '¢';
const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
const CH = { pxmon: 400 };
// Réécrit un innerHTML seulement s'il change : supprime le "rafraîchissement"
// visible toutes les 2 s (le DOM n'était pas sale, on le réécrivait quand même).
function setHtml(id, html) { const el = $(id); if (el.__last !== html) { el.__last = html; el.innerHTML = html; } }

let series = [], evData = [], winStart = 0;

function ctx(id) {
  const c = $(id); const h = CH[id]; const w = c.clientWidth || 600;
  const dpr = window.devicePixelRatio || 1;
  c.width = w * dpr; c.height = h * dpr;
  c.style.width = '100%'; c.style.height = h + 'px';
  const x = c.getContext('2d'); x.setTransform(dpr, 0, 0, dpr, 0, 0);
  return [x, w, h];
}
async function j(u) { try { const r = await fetch(u + '?t=' + Date.now()); return await r.json(); } catch (e) { return null; } }

function drawPxMonitor(s) {
  const [x, w, h] = ctx('pxmon');
  if (!winStart) return;
  const ws = winStart * 1000;
  const pts = series.filter((p) => p.t >= ws && p.up_mid > 0);
  const padL = 34, padR = 10, padT = 8, padB = 18;
  const X = (tS) => padL + (Math.min(Math.max(tS, 0), 300) / 300) * (w - padL - padR);
  const Y = (c) => h - padB - (c / 100) * (h - padT - padB);
  x.font = '10px ui-monospace,monospace';
  [0, 25, 50, 75, 100].forEach((c) => {
    const y = Y(c);
    x.strokeStyle = c === 50 ? '#3a3f4a' : '#22262e';
    x.beginPath(); x.moveTo(padL, y); x.lineTo(w - padR, y); x.stroke();
    x.fillStyle = '#8a919c'; x.fillText(c + '¢', 2, y + 3);
  });
  [60, 120, 180, 240].forEach((t) => {
    const xx = X(t);
    x.strokeStyle = '#1c2028'; x.beginPath(); x.moveTo(xx, padT); x.lineTo(xx, h - padB); x.stroke();
    x.fillStyle = '#8a919c'; x.fillText(t / 60 + ':00', xx - 10, h - 5);
  });
  if (pts.length >= 2) {
    const line = (key, color) => {
      x.strokeStyle = color; x.lineWidth = 2; x.beginPath();
      pts.forEach((p, i) => {
        const px = X((p.t - ws) / 1000), py = Y(p[key] * 100);
        i ? x.lineTo(px, py) : x.moveTo(px, py);
      });
      x.stroke();
    };
    line('up_mid', '#4caf6a');
    line('down_mid', '#e2604a');
  }
  (evData || []).forEach((e) => {
    const tS = Date.parse(e.ts) / 1000 - winStart;
    if (!(tS >= 0 && tS <= 300)) return;
    if (e.kind === 'buy') {
      const px = X(tS), py = Y(e.price * 100), up = e.side === 'up';
      x.fillStyle = up ? '#4caf6a' : '#e2604a';
      x.beginPath();
      if (up) { x.moveTo(px, py - 6); x.lineTo(px - 5, py + 4); x.lineTo(px + 5, py + 4); } // ▲ achat Up
      else { x.moveTo(px, py + 6); x.lineTo(px - 5, py - 4); x.lineTo(px + 5, py - 4); }     // ▼ achat Down
      x.closePath(); x.fill();
      x.strokeStyle = '#0d0f12'; x.lineWidth = 1; x.stroke();
    } else if (e.kind === 'merge') {
      const px = X(tS), py = Y(50);
      x.fillStyle = '#4aa3ff';
      x.beginPath(); x.arc(px, py, 5, 0, 7); x.fill();
      x.strokeStyle = '#0d0f12'; x.lineWidth = 1.5; x.stroke();
    }
  });
  // Overlay valeurs courantes (comme la capture) : prix Up + minuteur.
  const last = pts[pts.length - 1];
  if (last) {
    const up = (last.up_mid * 100);
    x.font = '600 15px ui-monospace,monospace'; x.textAlign = 'right';
    x.fillStyle = '#4caf6a'; x.fillText('Up ' + up.toFixed(1) + '¢', w - padR - 2, Y(up) - 6);
    x.textAlign = 'left';
  }
  if (window.__rem != null) {
    const mm = Math.floor(window.__rem / 60), ssv = window.__rem % 60;
    x.font = '600 16px ui-monospace,monospace'; x.textAlign = 'right';
    x.fillStyle = window.__rem < 30 ? '#e0a24a' : '#8a919c';
    x.fillText(mm + ':' + String(ssv).padStart(2, '0'), w - padR - 2, padT + 14);
    x.textAlign = 'left';
  }
}

async function tick() {
  const s = await j('/state');
  if (!s) return;
  series = s.series || [];
  winStart = s.window_start || 0;
  $('mode').textContent = s.dry_run ? 'PAPER' : 'LIVE';
  // Mode LIVE : le journal du bot remplace les panneaux stratégie.
  if (!window.__uiMode || window.__uiMode !== s.dry_run) {
    window.__uiMode = s.dry_run;
    document.querySelectorAll('.stratp').forEach((el) => el.style.display = s.dry_run ? '' : 'none');
    $('logspanel').style.display = s.dry_run ? 'none' : '';
    if (!s.dry_run && !window.__logsTimer) {
      const pollLogs = async () => {
        const lines = await j('/logs');
        if (!lines) return;
        const lc = { WARN: '#e0a24a', ERROR: '#ff8a8a', INFO: '#8a919c' };
        const html = lines.slice().reverse().map(([t, lvl, m]) =>
          `<div><span class="mut">${t}</span> <span style="color:${lc[lvl] || '#8a919c'}">${lvl}</span> ${m.replace(/</g, '&lt;')}</div>`
        ).join('');
        const el = $('botlog');
        if (el.__last !== html) { el.__last = html; el.innerHTML = html; }
      };
      pollLogs();
      window.__logsTimer = setInterval(pollLogs, 2000);
    }
  }
  if (typeof s.trading_enabled === 'boolean' && s.trading_enabled !== tradingOn) { tradingOn = s.trading_enabled; renderPower(); }
  $('clock').textContent = new Date().toLocaleTimeString();
  const ws = s.windows || [];
  $('hw').textContent = ws.length;
  const pnl = ws.reduce((a, w) => a + (w.pnl || 0), 0);
  const reb = ws.reduce((a, w) => a + (w.rebate || 0), 0) + (s.rebate_window || 0);
  const hp = $('hp');
  if (!s.dry_run && typeof s.live_wallet_pnl === 'number') {
    // LIVE : le PnL affiché = wallet RÉEL (collatéral − baseline), pas le miroir.
    // Note : les positions ouvertes/non-redeem comptent à 0 ici — c'est voulu
    // (pire cas), la valeur revient au wallet à chaque merge/redeem.
    $('hpl').textContent = 'PnL wallet (réel)';
    hp.textContent = money(s.live_wallet_pnl);
    hp.className = 'v ' + (s.live_wallet_pnl >= 0 ? 'pos' : 'neg');
  } else {
    hp.textContent = money(pnl);
    hp.className = 'v ' + (pnl >= 0 ? 'pos' : 'neg');
  }
  $('hr').textContent = money(reb); $('hr').className = 'v pos';
  const tot = pnl + reb;
  $('ht').textContent = money(tot); $('ht').className = 'v ' + (tot >= 0 ? 'pos' : 'neg');
  $('hwin').textContent = `${ws.filter((w) => (w.pnl || 0) > 0).length}/${ws.length}`;
  const dirOk = ws.filter((w) => ((w.imb_max > 0 ? 'Up' : 'Down') === w.res)).length;
  $('hdir').textContent = ws.length ? (dirOk / ws.length * 100).toFixed(0) + '%' : '–';

  // live
  $('lslug').textContent = s.market_slug || '…';
  $('lelapsed').textContent = winStart ? (300 - (s.remaining_s || 0)) + 's / 300s' : '';
  $('lstate').textContent = s.last_block_reason || (s.in_band ? 'quotes posées' : 'sans quote');
  $('lf').textContent = s.fills || 0;
  $('lud').textContent = `${f(s.up_bal, 0)}/${f(s.down_bal, 0)}`;
  const imbNow = (s.up_bal || 0) - (s.down_bal || 0);
  const li = $('li'); li.textContent = (imbNow > 0 ? '+' : '') + f(imbNow, 0);
  $('lpc').textContent = cents(s.pair_cost);

  // ── BANDEAU DE VALEURS EXPLOITABLES (au-dessus du grand graphique) ──
  window.__rem = s.remaining_s;
  const setIs = (id, txt, cls) => { const e = $(id); if (!e) return; e.textContent = txt; e.className = 'isv' + (cls ? ' ' + cls : ''); };
  const lat = s.signal_age_ms || 0;
  setIs('ilat', lat ? lat + ' ms' : '–', lat > 2000 ? 'neg' : lat > 500 ? 'warn' : 'pos');
  // RTT réel de la dernière requête CLOB (POST d'ordre ou poll 3 s) :
  // l'aller-retour Dublin ↔ serveur Polymarket. <120 ms sain, >500 ms suspect.
  const rtt = s.order_rtt_ms || 0;
  setIs('irtt', rtt ? rtt + ' ms' : '–', rtt > 500 ? 'neg' : rtt > 120 ? 'warn' : 'pos');
  setIs('iord', (s.open_orders != null ? s.open_orders : '–') + '/2', s.open_orders === 2 ? 'pos' : s.open_orders === 0 ? 'mut' : '');
  setIs('ifil', s.fills || 0);
  // merges de la FENÊTRE courante (paires ≈ $ recouvré), pas le cumul de toujours
  setIs('imrg', f(s.merged_window || 0, 0), (s.merged_window || 0) > 0 ? 'pos' : 'mut');
  setIs('iud', `${f(s.up_bal, 0)} / ${f(s.down_bal, 0)}`);
  setIs('iimb', (imbNow > 0 ? '+' : '') + f(imbNow, 0), Math.abs(imbNow) > 12 ? 'neg' : '');
  setIs('ipc', cents(s.pair_cost));
  const rem = s.remaining_s || 0;
  setIs('iclk', Math.floor(rem / 60) + ':' + String(rem % 60).padStart(2, '0'), rem < 30 ? 'warn' : '');
  const ldir = $('ldir');
  if (ldir) {
    const dt = s.dir_total || 0, dw = s.dir_wins || 0;
    // précision directionnelle de Tokyo : > 55-60% = vrai edge, ~50% = illusion
    ldir.textContent = dt ? `${Math.round(100*dw/dt)}% (${dw}/${dt})` : '– (0)';
    ldir.className = 'cv ' + (dt < 10 ? '' : dw/dt >= 0.57 ? 'pos' : dw/dt <= 0.5 ? 'neg' : '');
  }
  const ltf = $('ltf');
  if (ltf) {
    ltf.textContent = f(s.taker_fees_window || 0, 2) + '$';
    // cible : 0 — chaque centime ici est un cross d'ouverture ou un FAK
    ltf.className = 'cv ' + ((s.taker_fees_window || 0) > 0.005 ? 'neg' : '');
  }
  const mpa = $('lmpa');
  if (mpa) {
    mpa.textContent = s.merge_pair_avg ? cents(s.merge_pair_avg) : '–';
    // qualité d'exécution : vert ≤ 1,00$, rouge au-delà (le salaire = rebates,
    // la paire au merge doit coller à 1$)
    mpa.className = 'cv ' + (s.merge_pair_avg > 1.0 ? 'neg' : s.merge_pair_avg > 0 ? 'pos' : '');
  }
  $('ld').textContent = f(s.deployed, 2) + '$';
  $('lr').textContent = f(s.rebate_window, 2) + '$';
  $('lsf').textContent = '×' + f(s.size_factor, 2) + (s.loss_streak ? ` (${s.loss_streak}p)` : '');
  $('lc').textContent = s.dry_run ? f(s.cash, 0) + '$' : f(s.live_collateral || 0, 2) + '$ wallet';

  // bilan par jour
  const days = {};
  ws.forEach((w) => {
    const d = new Date(w.start * 1000).toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit' });
    (days[d] = days[d] || { n: 0, pnl: 0, reb: 0, dep: 0, win: 0 }).n++;
    days[d].pnl += w.pnl || 0; days[d].reb += w.rebate || 0; days[d].dep += w.deployed || 0;
    if ((w.pnl || 0) > 0) days[d].win++;
  });
  setHtml('daily', `<tr><th>jour</th><th>fenêtres</th><th>gagnantes</th><th>déployé$</th><th>PnL$</th><th>rebate$</th><th>total$</th></tr>` +
    Object.entries(days).map(([d, v]) => {
      const t = v.pnl + v.reb; const cls = t >= 0 ? 'pos' : 'neg';
      return `<tr><td>${d}</td><td>${v.n}</td><td>${v.win}</td><td>${f(v.dep, 0)}</td>` +
        `<td class="${v.pnl >= 0 ? 'pos' : 'neg'}">${money(v.pnl)}</td><td class="pos">${money(v.reb)}</td>` +
        `<td class="${cls}">${money(t)}</td></tr>`;
    }).join(''));

  // tableau fenêtres
  setHtml('tbl', `<tr><th>fenêtre</th><th>res</th><th>fills</th><th>Up@</th><th>Down@</th><th>paire</th><th>imb max</th><th>imb fin</th><th>mergé$</th><th>rebate$</th><th>PnL$</th></tr>` +
    ws.slice(-40).reverse().map((w) => {
      const hh = new Date(w.start * 1000).toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' });
      const cls = (w.pnl || 0) >= 0 ? 'pos' : 'neg';
      return `<tr><td>${hh}</td><td><span class="tag ${w.res === 'Up' ? 'up' : 'dn'}">${w.res}</span></td>` +
        `<td>${w.fills}</td><td>${cents(w.avg_up)}</td><td>${cents(w.avg_dn)}</td><td>${cents(w.pair_cost)}</td>` +
        `<td>${w.imb_max > 0 ? '+' : ''}${f(w.imb_max, 0)}</td><td>${w.imb_final > 0 ? '+' : ''}${f(w.imb_final, 0)}</td>` +
        `<td>${f(w.merged, 0)}</td><td class="pos">${f(w.rebate, 2)}</td><td class="${cls}">${money(w.pnl)}</td></tr>`;
    }).join('') || '<tr><td class="empty">en attente…</td></tr>');

  drawPxMonitor(s);
}

async function tickEvents() {
  const ev = await j('/events');
  if (!ev) return;
  evData = ev;
  const kc = { buy: '#4aa3ff', merge: '#a78bfa', resolve: '#6fe0a0', sell: '#e0a24a' };
  setHtml('feed', ev.slice().reverse().slice(0, 25).map((t) => {
    const time = (t.ts || '').slice(11, 19);
    const det = t.kind === 'merge' ? `${f(t.size, 0)} paires → +${f(t.size, 0)}$`
      : t.kind === 'resolve' ? `${t.side} gagne · payout ${f(t.size, 0)}$`
      : `${t.side} ${f(t.size, 0)} @ ${cents(t.price)}`;
    return `<div><span class="mut">${time}</span> <span style="color:${kc[t.kind] || '#fff'}">${t.kind}</span> ${det}</div>`;
  }).join(''));
}

// ── bouton ON/OFF (interrupteur manuel : /start | /stop) ──
let tradingOn = true;
function renderPower() {
  const b = $('power');
  b.className = 'pw ' + (tradingOn ? 'on' : 'off');
  b.textContent = tradingOn ? '● ON' : '■ OFF';
}
$('power').onclick = async () => {
  const target = tradingOn ? '/stop' : '/start';
  if (!tradingOn || confirm('Couper les prises d\'ordres ? (les ordres restants seront annulés, les positions vont à la résolution)')) {
    try { const r = await fetch(target, { method: 'POST' }); const d = await r.json(); tradingOn = d.trading_enabled; } catch (e) {}
    renderPower();
  }
};

tick(); tickEvents();
setInterval(tick, 1000);
setInterval(tickEvents, 2000);
window.addEventListener('resize', () => tick());
