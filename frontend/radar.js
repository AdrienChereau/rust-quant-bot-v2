// TOKYO · RADAR — scène animée pilotée par les vraies données (/state à 1 Hz).
// Un cœur qui pulse au rythme des ticks émis, des anneaux radar, et un flux de
// particules propulsé vers Dublin. KILL = onde de choc rouge.
const $ = (id) => document.getElementById(id);
const cv = $('scene');
const cx = cv.getContext('2d');
let W = 0, H = 0, DPR = 1;
function resize() {
  DPR = window.devicePixelRatio || 1;
  W = window.innerWidth; H = window.innerHeight;
  cv.width = W * DPR; cv.height = H * DPR;
  cx.setTransform(DPR, 0, 0, DPR, 0, 0);
}
resize(); window.addEventListener('resize', resize);

// ── état alimenté par /state ──
const S = {
  connected: false, micro: 0, obi: 0, ofi: 0, drift: 0, sigma: 0,
  seq: 0, kills: 0, rate: 0, lastSeq: 0, lastSeqT: Date.now(),
};
let shock = 0;          // onde de choc KILL (0→1)
let lastKills = -1;

async function poll() {
  try {
    const r = await fetch('/state?t=' + Date.now());
    const d = await r.json();
    const now = Date.now();
    const dt = (now - S.lastSeqT) / 1000;
    if (dt > 0.5) {
      S.rate = Math.max(0, (d.seq - S.lastSeq) / dt);
      S.lastSeq = d.seq; S.lastSeqT = now;
    }
    S.connected = d.binance_connected; S.micro = d.btc_micro;
    S.obi = d.obi || 0; S.ofi = d.ofi || 0; S.drift = d.drift || 0;
    S.sigma = d.sigma || 0; S.seq = d.seq || 0; S.kills = d.kills_emitted || 0;
    if (lastKills >= 0 && S.kills > lastKills) shock = 1;
    lastKills = S.kills;

    // HUD
    $('px').textContent = S.micro > 0 ? S.micro.toLocaleString('fr-FR', { maximumFractionDigits: 0 }) + ' $' : '–';
    $('status').className = S.connected ? 'on' : '';
    $('statustxt').textContent = S.connected ? 'FEED BINANCE ACTIF — ÉMISSION 10 HZ' : 'FEED PERDU — RECONNEXION';
    $('mdrift').textContent = (S.drift >= 0 ? '+' : '') + S.drift.toExponential(2);
    $('mdrift').style.color = S.drift >= 0 ? 'var(--gr)' : 'var(--rd)';
    $('mobi').textContent = (S.obi >= 0 ? '+' : '') + S.obi.toFixed(2);
    $('mofi').textContent = (S.ofi >= 0 ? '+' : '') + S.ofi.toFixed(2);
    $('msigma').textContent = (S.sigma * 100).toFixed(0) + ' %';
    bar('bdrift', Math.tanh(S.drift * 4000));
    bar('bobi', S.obi); bar('bofi', S.ofi);
    $('rate').textContent = S.rate.toFixed(0);
    $('seq').textContent = S.seq.toLocaleString('fr-FR');
    $('kills').textContent = S.kills;

    // journal (réécrit seulement si changé)
    const html = (d.radar_log || []).slice().reverse().map(([t, m]) => {
      const cls = m.includes('KILL') ? 'kill' : m.startsWith('✓') || m.startsWith('◉') ? 'ok' : m.startsWith('⚠') || m.startsWith('✗') ? 'warn' : '';
      return `<div><span class="t">${t}</span><span class="${cls}">${m}</span></div>`;
    }).join('');
    const el = $('log');
    if (el.__last !== html) { el.__last = html; el.innerHTML = html; }
  } catch (e) { S.connected = false; }
}
function bar(id, v) {
  const i = $(id); const half = 60; // px
  const w = Math.min(1, Math.abs(v)) * half;
  i.style.width = w + 'px';
  i.style.left = v >= 0 ? '50%' : (60 - w) + 'px';
  i.style.background = v >= 0 ? 'var(--gr)' : 'var(--rd)';
}
poll(); setInterval(poll, 1000);

// ── particules : cœur → Dublin (droite) ──
const parts = [];
function spawn(n) {
  const cxr = W * 0.42, cyr = H * 0.5;
  for (let i = 0; i < n; i++) {
    const a = (Math.random() - 0.5) * 0.5; // cône vers la droite
    const sp = 2.2 + Math.random() * 2.8 + Math.abs(S.drift) * 3000;
    parts.push({
      x: cxr + Math.cos(a) * 34, y: cyr + Math.sin(a) * 34,
      vx: Math.cos(a) * sp, vy: Math.sin(a) * sp,
      life: 1, hue: S.drift >= 0 ? 152 : 4, // vert / rouge selon le drift
    });
  }
}

let t0 = performance.now();
let ringPhase = 0;
function frame(now) {
  const dt = Math.min(0.05, (now - t0) / 1000); t0 = now;
  cx.clearRect(0, 0, W, H);
  const cxr = W * 0.42, cyr = H * 0.5;
  const alive = S.connected && S.rate > 0.5;

  // fond : grille polaire discrète
  cx.save();
  cx.globalAlpha = 0.5;
  for (let r = 70; r < Math.max(W, H) * 0.75; r += 90) {
    cx.beginPath(); cx.arc(cxr, cyr, r, 0, 7);
    cx.strokeStyle = 'rgba(51,224,255,0.045)'; cx.lineWidth = 1; cx.stroke();
  }
  cx.restore();

  // anneaux radar qui se propagent (cadence liée au rate)
  ringPhase += dt * (alive ? 0.55 + Math.min(1.2, S.rate / 12) : 0.12);
  for (let k = 0; k < 3; k++) {
    const p = (ringPhase + k / 3) % 1;
    const r = 40 + p * 320;
    cx.beginPath(); cx.arc(cxr, cyr, r, 0, 7);
    cx.strokeStyle = `rgba(51,224,255,${(1 - p) * 0.22})`;
    cx.lineWidth = 1.5; cx.stroke();
  }

  // onde de choc KILL
  if (shock > 0) {
    const r = (1 - shock) * Math.max(W, H) * 0.8;
    cx.beginPath(); cx.arc(cxr, cyr, r, 0, 7);
    cx.strokeStyle = `rgba(255,82,82,${shock * 0.75})`;
    cx.lineWidth = 3 + shock * 5; cx.stroke();
    shock = Math.max(0, shock - dt * 0.8);
  }

  // cœur : orbe qui respire au rythme des ticks
  const pulse = alive ? 1 + 0.12 * Math.sin(now / 1000 * Math.PI * Math.max(1, Math.min(6, S.rate / 2))) : 1;
  const R = 30 * pulse;
  const grad = cx.createRadialGradient(cxr, cyr, 2, cxr, cyr, R * 3.2);
  const core = alive ? '51,224,255' : '92,104,120';
  grad.addColorStop(0, `rgba(${core},0.95)`);
  grad.addColorStop(0.25, `rgba(${core},0.35)`);
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  cx.fillStyle = grad;
  cx.beginPath(); cx.arc(cxr, cyr, R * 3.2, 0, 7); cx.fill();
  cx.fillStyle = `rgba(${core},0.9)`;
  cx.beginPath(); cx.arc(cxr, cyr, R * 0.45, 0, 7); cx.fill();

  // spawn de particules ∝ ticks/s (le flux EST le débit du signal)
  if (alive) spawn(Math.min(6, 1 + S.rate / 4));
  for (let i = parts.length - 1; i >= 0; i--) {
    const p = parts[i];
    p.x += p.vx; p.y += p.vy;
    p.vx *= 1.012; // propulsion : accélère vers Dublin
    p.life -= dt * 0.55;
    if (p.life <= 0 || p.x > W + 20) { parts.splice(i, 1); continue; }
    cx.globalAlpha = Math.max(0, p.life) * 0.85;
    cx.fillStyle = `hsl(${p.hue},85%,62%)`;
    cx.fillRect(p.x, p.y, 2.4 + p.life * 2, 1.6);
  }
  cx.globalAlpha = 1;

  // point d'arrivée « Dublin » : halo violet sur le bord droit
  const dg = cx.createRadialGradient(W - 4, cyr, 1, W - 4, cyr, 130);
  dg.addColorStop(0, 'rgba(139,124,247,0.5)');
  dg.addColorStop(1, 'rgba(0,0,0,0)');
  cx.fillStyle = dg;
  cx.fillRect(W - 140, cyr - 140, 140, 280);

  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
