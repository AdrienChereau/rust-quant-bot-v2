// TOKYO · SIGNAL — le signal EST la page. Un fleuve d'énergie traverse l'écran
// du cœur émetteur vers Dublin ; tout est piloté par les vraies données :
//   vitesse du flux  ∝ ticks/s émis        amplitude des ondes ∝ σ + |OBI|
//   teinte           ∝ signe du drift       KILL → flash rouge plein écran
//   feed mort        → ligne plate grise (électrocardiogramme à l'arrêt)
// Le HUD est éphémère : il apparaît au mouvement de souris, s'évanouit après 3,5 s ;
// les événements du journal surgissent en toasts qui se dissolvent.

const $ = (id) => document.getElementById(id);
const cv = $('scene');
const cx = cv.getContext('2d');
let W = 0, H = 0;
function resize() {
  const dpr = window.devicePixelRatio || 1;
  W = window.innerWidth; H = window.innerHeight;
  cv.width = W * dpr; cv.height = H * dpr;
  cx.setTransform(dpr, 0, 0, dpr, 0, 0);
}
resize(); window.addEventListener('resize', resize);

// ── HUD éphémère ──
let sleepTimer = null;
function awake() {
  document.body.classList.add('awake');
  clearTimeout(sleepTimer);
  sleepTimer = setTimeout(() => document.body.classList.remove('awake'), 3500);
}
window.addEventListener('mousemove', awake);
window.addEventListener('touchstart', awake);

// ── état réel (poll /state 1 Hz) ──
const S = {
  connected: false, micro: 0, obi: 0, ofi: 0, drift: 0, sigma: 0,
  seq: 0, kills: 0, rate: 0,
};
let lastSeq = 0, lastSeqT = Date.now(), lastKills = -1, lastLogLen = 0;
let shock = 0; // flash KILL

function toast(t, m) {
  const cls = m.includes('KILL') ? 'kill' : m.startsWith('✓') || m.startsWith('◉') ? 'ok' : 'warn';
  const el = document.createElement('div');
  el.className = 'toast';
  el.innerHTML = `<span class="t" style="color:var(--mut);margin-right:8px">${t}</span><span class="${cls}">${m}</span>`;
  $('toasts').appendChild(el);
  setTimeout(() => el.remove(), 7200);
}

async function poll() {
  try {
    const r = await fetch('/state?t=' + Date.now());
    const d = await r.json();
    const now = Date.now();
    const dt = (now - lastSeqT) / 1000;
    if (dt > 0.5) { S.rate = Math.max(0, (d.seq - lastSeq) / dt); lastSeq = d.seq; lastSeqT = now; }
    S.connected = d.binance_connected; S.micro = d.btc_micro;
    S.obi = d.obi || 0; S.ofi = d.ofi || 0; S.drift = d.drift || 0;
    S.sigma = d.sigma || 0; S.seq = d.seq || 0; S.kills = d.kills_emitted || 0;
    if (lastKills >= 0 && S.kills > lastKills) shock = 1;
    lastKills = S.kills;

    $('px').textContent = S.micro > 0 ? S.micro.toLocaleString('fr-FR', { maximumFractionDigits: 0 }) + ' $' : '–';
    $('status').className = S.connected ? 'on' : '';
    $('statustxt').textContent = S.connected ? 'FEED BINANCE ACTIF · ÉMISSION 10 HZ' : 'FEED PERDU · RECONNEXION';
    $('mdrift').textContent = (S.drift >= 0 ? '+' : '') + S.drift.toExponential(1);
    $('mdrift').style.color = S.drift >= 0 ? 'var(--gr)' : 'var(--rd)';
    $('mobi').textContent = (S.obi >= 0 ? '+' : '') + S.obi.toFixed(2);
    $('mofi').textContent = (S.ofi >= 0 ? '+' : '') + S.ofi.toFixed(2);
    $('msigma').textContent = (S.sigma * 100).toFixed(0) + ' %';
    $('rate').textContent = S.rate.toFixed(0) + ' ticks/s';
    $('seq').textContent = S.seq.toLocaleString('fr-FR');
    $('kills').textContent = S.kills;

    const logs = d.radar_log || [];
    const html = logs.slice().reverse().map(([t, m]) => {
      const cls = m.includes('KILL') ? 'kill' : m.startsWith('✓') || m.startsWith('◉') ? 'ok' : 'warn';
      return `<div><span class="t">${t}</span><span class="${cls}">${m}</span></div>`;
    }).join('');
    const el = $('log');
    if (el.__last !== html) { el.__last = html; el.innerHTML = html; }
    // nouveaux événements → toasts éphémères
    if (lastLogLen > 0 && logs.length > lastLogLen) {
      logs.slice(lastLogLen).forEach(([t, m]) => toast(t, m));
    }
    lastLogLen = logs.length;
  } catch (e) { S.connected = false; }
}
poll(); setInterval(poll, 1000);

// ── LE SIGNAL : fleuve de rubans ondulants + particules, blending additif ──
const RIBBONS = 7;
const ribbons = Array.from({ length: RIBBONS }, (_, i) => ({
  seed: Math.random() * 1000,
  f1: 0.8 + Math.random() * 1.4,   // fréquences spatiales
  f2: 2.2 + Math.random() * 2.6,
  off: (i - (RIBBONS - 1) / 2) / ((RIBBONS - 1) / 2), // -1..1 (écartement)
  w: 1 + Math.random() * 1.6,
}));
const parts = [];
let flow = 0, t0 = performance.now();

// lissage visuel des données (évite les sauts au poll)
const V = { rate: 0, sigma: 0, obi: 0, drift: 0, alive: 0 };

function yRiver(xn, rb, tphase, amp) {
  // xn ∈ 0..1 le long de l'écran ; enveloppe qui pince aux extrémités
  const env = Math.sin(Math.PI * Math.min(1, xn * 1.15)) ** 0.7;
  const wave =
    Math.sin(xn * Math.PI * 2 * rb.f1 + tphase + rb.seed) * 0.62 +
    Math.sin(xn * Math.PI * 2 * rb.f2 - tphase * 1.7 + rb.seed * 2) * 0.38;
  return H / 2 + env * (wave * amp + rb.off * amp * 0.55);
}

function frame(now) {
  const dt = Math.min(0.05, (now - t0) / 1000); t0 = now;
  const k = 1 - Math.exp(-dt * 2.2); // constante de lissage
  V.rate += (S.rate - V.rate) * k;
  V.sigma += (S.sigma - V.sigma) * k;
  V.obi += (S.obi - V.obi) * k;
  V.drift += (S.drift - V.drift) * k;
  V.alive += ((S.connected && S.rate > 0.5 ? 1 : 0) - V.alive) * k;

  // fond : traînée persistante (le fleuve laisse un sillage)
  cx.globalCompositeOperation = 'source-over';
  cx.fillStyle = 'rgba(3,5,9,0.32)';
  cx.fillRect(0, 0, W, H);

  const alive = V.alive;
  flow += dt * (0.6 + Math.min(2.2, V.rate / 6)) * (0.15 + alive);
  // amplitude : σ (40%→) + |OBI| ; morte → ligne plate
  const amp = alive * (H * 0.10) * (0.55 + Math.min(1.6, V.sigma) + Math.abs(V.obi) * 0.5) + (1 - alive) * 3;
  // teinte : cyan neutre, tirée vers le vert (drift+) ou le rouge (drift−)
  const dir = Math.tanh(V.drift * 6000); // -1..1
  const hue = 190 + dir * (dir > 0 ? -35 : -186 + 190); // 190→155 (vert) ou →4 (rouge)
  const baseHue = dir >= 0 ? 190 - dir * 35 : 190 + dir * 186;

  // rubans (additif : les croisements s'illuminent)
  cx.globalCompositeOperation = 'lighter';
  const steps = 90;
  for (const rb of ribbons) {
    cx.beginPath();
    for (let s = 0; s <= steps; s++) {
      const xn = s / steps;
      const x = xn * W;
      const y = yRiver(xn, rb, flow * (1 + rb.off * 0.18), amp);
      s === 0 ? cx.moveTo(x, y) : cx.lineTo(x, y);
    }
    const a = alive * 0.16 + 0.02;
    const grad = cx.createLinearGradient(0, 0, W, 0);
    grad.addColorStop(0, `hsla(${baseHue},90%,60%,0)`);
    grad.addColorStop(0.18, `hsla(${baseHue},90%,62%,${a})`);
    grad.addColorStop(0.72, `hsla(${(baseHue + 250) % 360},85%,66%,${a * 0.9})`);
    grad.addColorStop(1, `hsla(258,85%,70%,${a * 1.3})`);
    cx.strokeStyle = grad;
    cx.lineWidth = rb.w * (1 + alive);
    cx.stroke();
  }

  // cœur émetteur (gauche) : pulse au rythme des ticks
  const cxr = W * 0.06, cyr = H / 2;
  const pulse = 1 + 0.15 * Math.sin(now / 1000 * Math.PI * Math.max(1, Math.min(7, V.rate / 1.6))) * alive;
  const R = (26 + amp * 0.12) * pulse;
  const core = alive > 0.5 ? `${baseHue},95%,65%` : '215,10%,45%';
  const g1 = cx.createRadialGradient(cxr, cyr, 1, cxr, cyr, R * 4);
  g1.addColorStop(0, `hsla(${core},0.9)`);
  g1.addColorStop(0.3, `hsla(${core},0.25)`);
  g1.addColorStop(1, 'hsla(0,0%,0%,0)');
  cx.fillStyle = g1;
  cx.beginPath(); cx.arc(cxr, cyr, R * 4, 0, 7); cx.fill();

  // réception Dublin (droite) : halo violet qui bat en écho (léger retard)
  const g2 = cx.createRadialGradient(W - W * 0.03, cyr, 1, W - W * 0.03, cyr, R * 3.2);
  const echo = 1 + 0.15 * Math.sin((now - 480) / 1000 * Math.PI * Math.max(1, Math.min(7, V.rate / 1.6))) * alive;
  g2.addColorStop(0, `hsla(258,85%,70%,${0.55 * alive * echo})`);
  g2.addColorStop(1, 'hsla(0,0%,0%,0)');
  cx.fillStyle = g2;
  cx.beginPath(); cx.arc(W - W * 0.03, cyr, R * 3.2 * echo, 0, 7); cx.fill();

  // particules : des paquets de données qui remontent le fleuve
  if (alive > 0.4 && Math.random() < 0.35 + V.rate / 25) {
    const rb = ribbons[(Math.random() * RIBBONS) | 0];
    parts.push({ xn: 0.02, rb, sp: (0.10 + Math.random() * 0.10 + V.rate / 90) });
  }
  for (let i = parts.length - 1; i >= 0; i--) {
    const p = parts[i];
    p.xn += p.sp * dt * (1 + p.xn * 1.6); // accélère vers Dublin
    if (p.xn >= 1) { parts.splice(i, 1); continue; }
    const x = p.xn * W;
    const y = yRiver(p.xn, p.rb, flow * (1 + p.rb.off * 0.18), amp);
    const a = Math.sin(Math.PI * p.xn) * 0.9;
    cx.fillStyle = `hsla(${baseHue},95%,75%,${a})`;
    cx.beginPath(); cx.arc(x, y, 1.6 + p.xn * 1.8, 0, 7); cx.fill();
  }

  // flash KILL : le fleuve vire au rouge, voile plein écran
  if (shock > 0) {
    cx.globalCompositeOperation = 'source-over';
    cx.fillStyle = `rgba(255,60,60,${shock * 0.18})`;
    cx.fillRect(0, 0, W, H);
    shock = Math.max(0, shock - dt * 0.7);
  }

  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
