// TOKYO · SIGNAL v3 — composition CENTRÉE qui raconte le pipeline :
//   gauche  → bruit brut du marché (particules chaotiques) qui converge vers…
//   centre  → le CŒUR Tokyo (calcul du signal), qui pulse au rythme d'émission
//   droite  → le signal MIS EN FORME (tresse ordonnée) propulsé vers Dublin
// Le chaos entre, l'ordre sort : c'est exactement le travail du radar.
// Données réelles : vitesse=ticks/s, amplitude=σ+|OBI|, teinte=drift, KILL=flash.

const $ = (id) => document.getElementById(id);
const cv = $('scene');
const cx = cv.getContext('2d');
let W = 0, H = 0, CX = 0, CY = 0;
function resize() {
  const dpr = window.devicePixelRatio || 1;
  W = window.innerWidth; H = window.innerHeight;
  CX = W / 2; CY = H / 2;
  cv.width = W * dpr; cv.height = H * dpr;
  cx.setTransform(dpr, 0, 0, dpr, 0, 0);
}
resize(); window.addEventListener('resize', resize);

// ── état réel ──
const S = { connected: false, micro: 0, obi: 0, ofi: 0, drift: 0, sigma: 0, seq: 0, kills: 0, rate: 0 };
let lastSeq = 0, lastSeqT = Date.now(), lastKills = -1, shock = 0;

// traductions humaines
function trendLabel(d) {
  const x = Math.tanh(d * 6000);
  if (x > 0.5) return ['▲ HAUSSIER', 'up'];
  if (x > 0.12) return ['↗ haussier léger', 'up'];
  if (x < -0.5) return ['▼ BAISSIER', 'dn'];
  if (x < -0.12) return ['↘ baissier léger', 'dn'];
  return ['— neutre', 'mid'];
}
function pressLabel(v) {
  if (v > 0.5) return ['ACHAT fort', 'up'];
  if (v > 0.15) return ['achat', 'up'];
  if (v < -0.5) return ['VENTE forte', 'dn'];
  if (v < -0.15) return ['vente', 'dn'];
  return ['équilibré', 'mid'];
}
function sigLabel(s) {
  const pc = s * 100;
  if (pc < 45) return [pc.toFixed(0) + '% · calme', 'mid'];
  if (pc < 90) return [pc.toFixed(0) + '% · actif', 'up'];
  return [pc.toFixed(0) + '% · NERVEUX', 'warn'];
}
function setRead(id, [txt, cls]) {
  const el = $(id);
  el.textContent = txt;
  el.style.color = cls === 'up' ? 'var(--gr)' : cls === 'dn' ? 'var(--rd)' : cls === 'warn' ? 'var(--amb)' : 'var(--mut)';
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
    $('statustxt').textContent = S.connected
      ? 'OPÉRATIONNEL — BINANCE ÉCOUTÉ, DUBLIN ALIMENTÉ'
      : 'FEED BINANCE PERDU — RECONNEXION EN COURS';
    $('rate').textContent = S.rate.toFixed(0);
    $('seq').textContent = S.seq.toLocaleString('fr-FR');
    $('kills').textContent = S.kills;
    setRead('rdrift', trendLabel(S.drift));
    setRead('robi', pressLabel(S.obi));
    setRead('rofi', pressLabel(S.ofi));
    setRead('rsig', sigLabel(S.sigma));

    const logs = d.radar_log || [];
    const html = logs.slice().reverse().map(([t, m]) => {
      const cls = m.includes('KILL') ? 'kill' : m.startsWith('✓') || m.startsWith('◉') ? 'ok' : 'warn';
      return `<div><span class="t">${t}</span><span class="${cls}">${m}</span></div>`;
    }).join('');
    const el = $('log');
    if (el.__last !== html) { el.__last = html; el.innerHTML = html; }
  } catch (e) { S.connected = false; }
}
poll(); setInterval(poll, 1000);

// ── scène : UNE onde, centrée, symétrique — pas de cœur, pas de battement ──
// La forme est un signal pur au milieu de l'écran :
//   amplitude ∝ σ + |OBI|   vitesse de défilement ∝ ticks/s émis
//   teinte    ∝ drift (cyan neutre → sauge en hausse, rosé en baisse)
//   feed mort → ligne plate grise   KILL → voile rosé
const V = { rate: 0, sigma: 0, obi: 0, drift: 0, alive: 0 };
let flow = 0, t0 = performance.now();

const HARM = 4; // harmoniques superposées (fines) qui composent l'onde
const harms = Array.from({ length: HARM }, (_, i) => ({
  f: 1.5 + i * 1.15 + Math.random() * 0.4, // fréquence spatiale
  sp: 1 + i * 0.35,                         // vitesse relative
  w: 1 - i * 0.16,                          // poids dans l'amplitude
  seed: Math.random() * 9,
}));
const packets = [];

function waveY(xn, amp, ph) {
  // enveloppe symétrique : l'onde naît et meurt aux bords, ample au CENTRE
  const env = Math.sin(Math.PI * xn) ** 1.4;
  let y = 0;
  for (const h of harms) y += Math.sin(xn * Math.PI * 2 * h.f - ph * h.sp + h.seed) * h.w;
  return CY + env * y * amp * 0.42;
}

function frame(now) {
  const dt = Math.min(0.05, (now - t0) / 1000); t0 = now;
  const k = 1 - Math.exp(-dt * 2.2);
  V.rate += (S.rate - V.rate) * k;
  V.sigma += (S.sigma - V.sigma) * k;
  V.obi += (S.obi - V.obi) * k;
  V.drift += (S.drift - V.drift) * k;
  V.alive += ((S.connected && S.rate > 0.5 ? 1 : 0) - V.alive) * k;
  const alive = V.alive;
  flow += dt * (0.5 + Math.min(1.6, V.rate / 8)) * (0.15 + alive);

  const dir = Math.tanh(V.drift * 6000);
  const hue = dir >= 0 ? 190 - dir * 35 : 190 + dir * 186;
  const amp = alive * H * 0.11 * (0.5 + Math.min(1.2, V.sigma) + Math.abs(V.obi) * 0.3) + (1 - alive) * 2;

  // sillage soyeux
  cx.globalCompositeOperation = 'source-over';
  cx.fillStyle = 'rgba(7,9,13,0.20)';
  cx.fillRect(0, 0, W, H);
  cx.globalCompositeOperation = 'lighter';

  const mL = W * 0.15, mR = W * 0.15; // le signal occupe le CENTRE (70 % de la largeur)
  const span = W - mL - mR;
  const steps = 160;

  // ligne médiane, à peine là (l'axe du signal)
  cx.strokeStyle = 'rgba(255,255,255,0.045)';
  cx.lineWidth = 1;
  cx.beginPath(); cx.moveTo(mL, CY); cx.lineTo(W - mR, CY); cx.stroke();

  // l'onde : 3 passes du même tracé (halo large → cœur fin) pour un glow doux
  const passes = [
    { lw: 7, a: 0.05 },
    { lw: 2.6, a: 0.14 },
    { lw: 1.1, a: 0.5 },
  ];
  for (const pass of passes) {
    cx.beginPath();
    for (let i = 0; i <= steps; i++) {
      const xn = i / steps;
      const x = mL + xn * span;
      const y = waveY(xn, amp, flow * 2);
      i === 0 ? cx.moveTo(x, y) : cx.lineTo(x, y);
    }
    const sat = 30 + alive * 25;
    cx.strokeStyle = `hsla(${hue},${sat}%,70%,${pass.a * (0.25 + alive * 0.75)})`;
    cx.lineWidth = pass.lw;
    cx.stroke();
  }
  // onde miroir, fantôme (profondeur, symétrie verticale)
  cx.beginPath();
  for (let i = 0; i <= steps; i++) {
    const xn = i / steps;
    const x = mL + xn * span;
    const y = 2 * CY - waveY(xn, amp * 0.8, flow * 2 + 1.3);
    i === 0 ? cx.moveTo(x, y) : cx.lineTo(x, y);
  }
  cx.strokeStyle = `hsla(${hue},30%,70%,${0.06 * (0.3 + alive)})`;
  cx.lineWidth = 1;
  cx.stroke();

  // paquets de données qui parcourent l'onde vers la droite (Dublin)
  if (alive > 0.4 && Math.random() < 0.12 + V.rate / 45) {
    packets.push({ xn: 0, sp: 0.10 + Math.random() * 0.06 + V.rate / 120 });
  }
  for (let i = packets.length - 1; i >= 0; i--) {
    const p = packets[i];
    p.xn += p.sp * dt * 1.6;
    if (p.xn >= 1) { packets.splice(i, 1); continue; }
    const x = mL + p.xn * span;
    const y = waveY(p.xn, amp, flow * 2);
    cx.fillStyle = `hsla(${hue},55%,82%,${0.7 * alive * Math.sin(Math.PI * p.xn)})`;
    cx.beginPath(); cx.arc(x, y, 1.7, 0, 7); cx.fill();
  }

  // KILL : voile rosé
  if (shock > 0) {
    cx.globalCompositeOperation = 'source-over';
    cx.fillStyle = `rgba(217,120,120,${shock * 0.12})`;
    cx.fillRect(0, 0, W, H);
    shock = Math.max(0, shock - dt * 0.7);
  }

  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
