// Copy Maker V8 — dashboard style observatoire, poll /state + /events.
const $ = (id) => document.getElementById(id);
const f = (n, d = 2) => (n == null || isNaN(n)) ? '–' : Number(n).toFixed(d);
const money = (n) => (n >= 0 ? '+' : '') + f(n, 2) + '$';
const cents = (n) => (n == null || isNaN(n) || n === 0) ? '–' : f(n * 100, 1) + '¢';
const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
const CH = { pxmon: 210, imb: 80, pc: 100, cum: 120 };
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
      const px = X(tS), py = Y(e.price * 100);
      x.fillStyle = e.side === 'up' ? '#4caf6a' : '#e2604a';
      x.beginPath(); x.moveTo(px, py - 5); x.lineTo(px - 4.5, py + 4); x.lineTo(px + 4.5, py + 4);
      x.closePath(); x.fill();
      x.strokeStyle = '#0d0f12'; x.lineWidth = 1; x.stroke();
    } else if (e.kind === 'merge') {
      const px = X(tS), py = Y(50);
      x.fillStyle = '#4aa3ff';
      x.beginPath(); x.arc(px, py, 4, 0, 7); x.fill();
      x.strokeStyle = '#0d0f12'; x.stroke();
    }
  });
}

function drawImb() {
  const [x, w, h] = ctx('imb');
  if (!winStart) return;
  const ws = winStart * 1000;
  const pts = series.filter((p) => p.t >= ws);
  if (pts.length < 2) return;
  const sv = pts.map((p) => symlog(p.imb || 0));
  const lim = Math.max(Math.abs(Math.min(...sv, 0)), Math.abs(Math.max(...sv, 0)), symlog(50));
  const X = (i) => 34 + ((pts[i].t - ws) / 300000) * (w - 42);
  const Y = (v) => h / 2 - (v / lim) * (h / 2 - 10);
  x.font = '9px ui-monospace,monospace';
  [0, 50, -50, 200, -200].forEach((t) => {
    const y = Y(symlog(t));
    if (y > 4 && y < h - 4) {
      x.strokeStyle = t === 0 ? '#555' : '#2a2f38';
      x.setLineDash(t === 0 ? [3, 3] : [2, 4]);
      x.beginPath(); x.moveTo(34, y); x.lineTo(w - 8, y); x.stroke(); x.setLineDash([]);
      x.fillStyle = '#8a919c'; x.fillText(t > 0 ? '+' + t : String(t), 2, y + 3);
    }
  });
  x.strokeStyle = '#a78bfa'; x.lineWidth = 2; x.beginPath();
  pts.forEach((p, i) => {
    const y = Y(symlog(p.imb || 0));
    i ? x.lineTo(X(i), y) : x.moveTo(X(i), y);
  });
  x.stroke();
}

function drawWindows(ws) {
  // barres coût de paire (60 dernières)
  let [x, w2, h2] = ctx('pc');
  const wsc = ws.slice(-60);
  if (wsc.length) {
    const bw = Math.max(3, Math.min(30, (w2 - 40) / wsc.length - 3));
    const vals = wsc.map((q) => (q.pair_cost || 0) * 100);
    const mx = Math.max(...vals, 110);
    const y100 = h2 - 12 - (100 / mx) * (h2 - 20);
    x.strokeStyle = '#555'; x.setLineDash([3, 3]);
    x.beginPath(); x.moveTo(30, y100); x.lineTo(w2 - 6, y100); x.stroke(); x.setLineDash([]);
    x.fillStyle = '#8a919c'; x.font = '10px ui-monospace,monospace'; x.fillText('100', 4, y100 + 3);
    wsc.forEach((q, i) => {
      const v = (q.pair_cost || 0) * 100;
      const bh = (v / mx) * (h2 - 20);
      x.fillStyle = v > 0 && v < 100 ? '#4caf6a' : '#e2604a';
      x.fillRect(30 + i * (bw + 3), h2 - 12 - bh, bw, bh);
    });
  }
  // cumul trading vs trading+rebate
  [x, w2, h2] = ctx('cum');
  if (ws.length) {
    let a = 0, b = 0;
    const t1 = ws.map((q) => a += (q.pnl || 0));
    const t2 = ws.map((q, i) => b += (q.pnl || 0) + (q.rebate || 0));
    const all = t1.concat(t2);
    const mn = Math.min(...all, 0), mx = Math.max(...all, 1);
    const X = (i) => 40 + (i / Math.max(ws.length - 1, 1)) * (w2 - 48);
    const Y = (v) => h2 - 14 - ((v - mn) / (mx - mn || 1)) * (h2 - 22);
    x.font = '10px ui-monospace,monospace';
    x.strokeStyle = '#555'; x.setLineDash([3, 3]);
    x.beginPath(); x.moveTo(40, Y(0)); x.lineTo(w2 - 6, Y(0)); x.stroke(); x.setLineDash([]);
    x.fillStyle = '#8a919c'; x.fillText(f(mx, 0), 2, Y(mx) + 8); x.fillText(f(mn, 0), 2, Y(mn) - 2);
    const line = (arr, color) => {
      x.strokeStyle = color; x.lineWidth = 2; x.beginPath();
      arr.forEach((v, i) => { i ? x.lineTo(X(i), Y(v)) : x.moveTo(X(i), Y(v)); });
      x.stroke();
    };
    line(t1, '#e0a24a');
    line(t2, '#a78bfa');
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
  const hp = $('hp'); hp.textContent = money(pnl); hp.className = 'v ' + (pnl >= 0 ? 'pos' : 'neg');
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
  li.className = 'cv ' + (Math.abs(imbNow) > 100 ? 'neg' : '');
  $('lpc').textContent = cents(s.pair_cost);
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

  drawPxMonitor(s); drawImb(); drawWindows(ws);
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
