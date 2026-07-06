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

// ── scène ──
const V = { rate: 0, sigma: 0, obi: 0, drift: 0, alive: 0 }; // valeurs lissées
let flow = 0, t0 = performance.now();

// particules chaos (entrée, gauche) et tresse (sortie, droite)
const chaos = [];
const RIB = 5;
const ribbons = Array.from({ length: RIB }, (_, i) => ({
  seed: Math.random() * 9,
  f: 1.6 + Math.random() * 2.2,
  off: (i - (RIB - 1) / 2) / ((RIB - 1) / 2),
}));
const packets = [];

function braidY(xn, rb, amp) {
  // xn ∈ 0..1 entre le cœur et le bord droit ; la tresse naît serrée et s'ouvre
  const open = Math.sin(Math.PI * Math.min(1, xn * 1.06)) ** 0.8;
  const wave = Math.sin(xn * Math.PI * 2 * rb.f - flow * 2 + rb.seed);
  return CY + open * (wave * amp * 0.5 + rb.off * amp * 0.75);
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
  flow += dt * (0.35 + Math.min(1.2, V.rate / 10)) * (0.15 + alive);

  const dir = Math.tanh(V.drift * 6000);
  const hue = dir >= 0 ? 190 - dir * 35 : 190 + dir * 186; // cyan→vert / cyan→rouge
  const amp = alive * H * 0.07 * (0.5 + Math.min(1.2, V.sigma) + Math.abs(V.obi) * 0.3) + (1 - alive) * 2;

  // sillage persistant
  cx.globalCompositeOperation = 'source-over';
  cx.fillStyle = 'rgba(7,9,13,0.22)';
  cx.fillRect(0, 0, W, H);
  cx.globalCompositeOperation = 'lighter';

  // ── ① CHAOS ENTRANT (gauche → cœur) : bruit de marché ──
  if (alive > 0.3 && Math.random() < 0.22 + V.sigma * 0.5) {
    chaos.push({
      x: -5, y: CY + (Math.random() - 0.5) * H * 0.7,
      vx: 1.4 + Math.random() * 1.8, vy: (Math.random() - 0.5) * 1.6,
      j: 0.4 + Math.random() * 1.2, // nervosité
    });
  }
  for (let i = chaos.length - 1; i >= 0; i--) {
    const p = chaos[i];
    // attiré par le cœur, avec du jitter (c'est du bruit)
    const dx = CX - p.x, dy = CY - p.y;
    const dist = Math.hypot(dx, dy) + 1;
    p.vx += (dx / dist) * 0.10 * (1 + 200 / dist);
    p.vy += (dy / dist) * 0.10 * (1 + 200 / dist) + (Math.random() - 0.5) * p.j * (0.5 + V.sigma);
    p.x += p.vx; p.y += p.vy;
    if (dist < 30 || p.x > CX) { chaos.splice(i, 1); continue; }
    const a = Math.min(0.3, 30 / dist + 0.07) * alive;
    cx.fillStyle = `hsla(215,18%,72%,${a})`;
    cx.fillRect(p.x, p.y, 1.8, 1.8);
  }

  // ── ② LE CŒUR (centre) : pulse au rythme d'émission ──
  const pulse = 1 + 0.09 * Math.sin(now / 1000 * Math.PI * Math.max(1, Math.min(7, V.rate / 1.6))) * alive;
  const R = (24 + amp * 0.12) * pulse;
  const core = alive > 0.5 ? `${hue},52%,68%` : '215,8%,42%';
  const g1 = cx.createRadialGradient(CX, CY, 1, CX, CY, R * 4.2);
  g1.addColorStop(0, `hsla(${core},0.75)`);
  g1.addColorStop(0.25, `hsla(${core},0.16)`);
  g1.addColorStop(1, 'hsla(0,0%,0%,0)');
  cx.fillStyle = g1;
  cx.beginPath(); cx.arc(CX, CY, R * 4.2, 0, 7); cx.fill();
  // anneau de traitement qui tourne
  cx.strokeStyle = `hsla(${core},${0.28 * alive})`;
  cx.lineWidth = 1;
  cx.beginPath();
  cx.arc(CX, CY, R * 1.7, flow * 0.9, flow * 0.9 + 4.2);
  cx.stroke();

  // ── ③ TRESSE SORTANTE (cœur → droite) : le signal mis en forme ──
  const steps = 70;
  for (const rb of ribbons) {
    cx.beginPath();
    for (let s = 0; s <= steps; s++) {
      const xn = s / steps;
      const x = CX + xn * (W - CX);
      const y = braidY(xn, rb, amp);
      s === 0 ? cx.moveTo(x, y) : cx.lineTo(x, y);
    }
    const a = alive * 0.13 + 0.015;
    const grad = cx.createLinearGradient(CX, 0, W, 0);
    grad.addColorStop(0, `hsla(${hue},55%,66%,${a})`);
    grad.addColorStop(1, `hsla(255,45%,72%,${a * 1.2})`);
    cx.strokeStyle = grad;
    cx.lineWidth = 0.9 + alive * 0.6;
    cx.stroke();
  }
  // paquets UDP qui filent vers Dublin
  if (alive > 0.4 && Math.random() < 0.16 + V.rate / 40) {
    packets.push({ xn: 0.01, rb: ribbons[(Math.random() * RIB) | 0], sp: 0.24 + Math.random() * 0.14 + V.rate / 70 });
  }
  for (let i = packets.length - 1; i >= 0; i--) {
    const p = packets[i];
    p.xn += p.sp * dt * (1 + p.xn);
    if (p.xn >= 1) { packets.splice(i, 1); continue; }
    const x = CX + p.xn * (W - CX);
    const y = braidY(p.xn, p.rb, amp);
    cx.fillStyle = `hsla(${hue},60%,80%,${0.65 * alive})`;
    cx.beginPath(); cx.arc(x, y, 1.8 + p.xn * 1.6, 0, 7); cx.fill();
  }
  // halo Dublin (bord droit) qui bat en écho
  const echo = 1 + 0.09 * Math.sin((now - 480) / 1000 * Math.PI * Math.max(1, Math.min(7, V.rate / 1.6))) * alive;
  const g2 = cx.createRadialGradient(W - 6, CY, 1, W - 6, CY, 110 * echo);
  g2.addColorStop(0, `hsla(255,45%,72%,${0.3 * alive})`);
  g2.addColorStop(1, 'hsla(0,0%,0%,0)');
  cx.fillStyle = g2;
  cx.fillRect(W - 220, CY - 220, 220, 440);

  // KILL : voile rouge
  if (shock > 0) {
    cx.globalCompositeOperation = 'source-over';
    cx.fillStyle = `rgba(217,120,120,${shock * 0.12})`;
    cx.fillRect(0, 0, W, H);
    shock = Math.max(0, shock - dt * 0.7);
  }

  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
