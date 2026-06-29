const $ = (id) => document.getElementById(id);
function fmt(n, d = 2) { return (n == null || Number.isNaN(n)) ? "—" : Number(n).toFixed(d); }

// Mode courant + type de nœud (dernier /state).
let currentMode = "PAPER";
let nodeKind = "mono";

// Endpoint de contrôle (POST) → feedback immédiat via le mode renvoyé, puis refresh complet.
async function ctl(path) {
  try {
    const r = await (await fetch(path, { method: "POST" })).json();
    if (r && r.mode) { const mb = $("mode"); mb.textContent = r.mode; mb.className = "badge mode " + r.mode.toLowerCase(); }
  } catch (e) {}
  refresh();
}
window.ctl = ctl;

// Start/Stop = pause logicielle. Si le nœud tourne (LIVE/PAPER) → Stop ; si en pause → Start.
function toggleRun() {
  const running = (currentMode === "LIVE" || currentMode === "PAPER");
  if (running) {
    ctl("/stop");
  } else {
    if (nodeKind === "live" && !confirm("Démarrer le nœud LIVE ? Le sizing utilisera la bankroll réelle (CLOB).")) return;
    ctl("/start");
  }
}
window.toggleRun = toggleRun;

// Met à jour le libellé/style du bouton Start/Stop selon l'état (en pause ou actif).
function renderToggle(mode) {
  const btn = $("mode-toggle");
  if (!btn) return;
  const running = (mode === "LIVE" || mode === "PAPER");
  if (running) {
    btn.textContent = "⏸ STOP";
    btn.className = "mode-toggle live";
  } else {
    btn.textContent = "▶ START";
    btn.className = "mode-toggle paper";
  }
}

function signed(el, n, d = 2) { el.textContent = fmt(n, d); el.classList.toggle("pos", n > 0); el.classList.toggle("neg", n < 0); }
function obi(el, v) { el.textContent = (v >= 0 ? "+" : "") + fmt(v, 3); el.classList.toggle("pos", v > 0); el.classList.toggle("neg", v < 0); }

async function refresh() {
  try {
    const s = await (await fetch("/state", { cache: "no-store" })).json();
    $("status").textContent = "✓ connecté"; $("status").className = "ok";

    const dry = $("dry"); dry.textContent = s.dry_run ? "PAPER" : "LIVE"; dry.className = "badge " + (s.dry_run ? "paper" : "live");

    // Type de nœud — pilote l'affichage (un nœud = une vue). Fallback heuristique si absent.
    nodeKind = s.node_kind || "";
    let isOrder, isKiller, isLiveNode;
    if (nodeKind === "radar") { isOrder = true;  isKiller = false; isLiveNode = false; }
    else if (nodeKind === "live")  { isOrder = false; isKiller = true;  isLiveNode = true;  }
    else if (nodeKind === "paper") { isOrder = false; isKiller = true;  isLiveNode = false; }
    else if (nodeKind === "mono")  { isOrder = true;  isKiller = true;  isLiveNode = (s.mode === "LIVE"); }
    else {
        // Fallback legacy : détection par contenu.
        isOrder = (s.btc_spot > 0) || (s.lat_binance_ms != null) || (s.obi_binance !== 0);
        isKiller = (s.market_slug !== "") || (s.lat_polymarket_ms != null) || (s.cash > 0);
        isLiveNode = (s.mode === "LIVE");
    }

    const titles = { radar: "ORDER TERMINAL (TOKYO)", live: "LIVE TERMINAL (DUBLIN)", paper: "PAPER TERMINAL", mono: "MONO TERMINAL" };
    $("app-name").textContent = titles[nodeKind] || (isOrder && isKiller ? "MONO TERMINAL" : isOrder ? "ORDER TERMINAL" : "KILLER TERMINAL");
    $("order-terminal").style.display = isOrder ? "grid" : "none";
    $("killer-terminal").style.display = isKiller ? "block" : "none";
    // Carte latence totale : nœud live/mono uniquement (le paper n'envoie pas d'ordre réel).
    const cardTotal = $("card-lat-total");
    if (cardTotal) cardTotal.style.display = isLiveNode ? "block" : "none";

    if (isOrder) {
        $("binance").innerHTML = s.binance_connected ? '<span class="ok">connecté</span>' : '<span class="ko">—</span>';
        $("okx").innerHTML = s.okx_connected ? '<span class="ok">connecté</span>' : '<span class="ko">—</span>';
        $("spot").textContent = fmt(s.btc_spot, 1);
        obi($("obib"), s.obi_binance); obi($("obio"), s.obi_okx); obi($("obic"), s.obi_consolidated);
        $("agree").innerHTML = s.agreement ? '<span class="ok">oui ✓</span>' : '<span class="ko">non ✗</span>';
        $("vel").textContent = (s.velocity >= 0 ? "+" : "") + (s.velocity * 100).toFixed(3) + "%";

        const chk = (el, v) => { $(el).innerHTML = v ? '<span class="ok">✓</span>' : '<span class="ko">✗</span>'; };
        chk("c_agree", s.cond_agreement); chk("c_persist", s.cond_persist); chk("c_vel", s.cond_velocity);
        chk("c_gap", s.cond_gap); chk("c_ready", s.cond_ready);
        $("c_all").innerHTML = s.all_conditions ? '<span class="ok">🔥 FEU</span>' : '<span class="muted">en attente</span>';

        // Compartiment Maths — valeurs vivantes du signal stack.
        const plain = (id, v, d = 3) => { $(id).textContent = fmt(v, d); };
        obi($("m_obib"), s.obi_binance); obi($("m_obio"), s.obi_okx);
        obi($("m_tfi"), s.tfi); obi($("m_velnorm"), s.vel_norm); obi($("m_basis"), s.basis_norm);
        plain("m_basisunc", s.basis_unc, 2);
        obi($("m_score"), s.score);
        plain("m_scoresigma", s.score_sigma, 3);
        plain("m_sigreal", s.sigma_realized, 3); plain("m_sigewma", s.sigma_ewma, 3); plain("m_sigblend", s.sigma_blended, 3);
        plain("m_strike", s.strike, 1); plain("m_micro", s.microprice, 1); plain("m_kvel", s.kalman_velocity, 2);
        signed($("m_d2base"), s.d2_base, 3); signed($("m_d2adj"), s.d2_adj, 3);
        plain("m_ic", s.ic, 3);
    }

    if (isKiller) {
        // Contrôle d'exécution + circuit breaker
        const mode = s.mode || "—";
        currentMode = mode;
        const mb = $("mode"); mb.textContent = mode; mb.className = "badge mode " + mode.toLowerCase();
        $("ctl_mode").textContent = mode;
        renderToggle(mode);
        $("ctl_bankroll").innerHTML = s.live_bankroll != null
          ? `<span class="ok">${fmt(s.live_bankroll, 2)} USDC</span>`
          : '<span class="ko">— (non lue)</span>';
        $("ctl_paper_bk").innerHTML = `<span class="muted">${fmt(s.equity, 2)} $ fictif</span>`;
        const isLive = mode === "LIVE";
        $("ctl_live_pnl").innerHTML = (isLive && s.live_pnl != null)
          ? `<span class="${s.live_pnl >= 0 ? "pos" : "neg"}">${s.live_pnl >= 0 ? "+" : ""}${fmt(s.live_pnl, 2)} USDC</span>`
          : '<span class="muted">— (live off)</span>';
        $("ctl_live_shots").textContent = isLive ? (s.live_shots ?? 0) : "—";
        $("ctl_armed").innerHTML = s.live_armed ? '<span class="ko">ARMÉ ⚠</span>' : '<span class="ok">non (sûr)</span>';
        $("ctl_sizing").innerHTML = (s.fixed_order_usd > 0)
          ? `<span class="ko">Fixe ${s.fixed_order_usd}$ ⚠</span>`
          : s.live_force_min
          ? '<span class="ko">MIN forcé ⚠</span>'
          : '<span class="ok">Kelly</span>';
        const ddv = s.initial_capital != null ? (s.initial_capital - (s.equity ?? s.initial_capital)) : null;
        $("ctl_dd").textContent = ddv != null ? `${fmt(ddv, 2)} / ${fmt(s.max_drawdown, 0)} $` : "—";
        const banner = $("breaker-banner");
        if (s.breaker_tripped) { banner.hidden = false; banner.classList.add("pulse"); }
        else { banner.hidden = true; banner.classList.remove("pulse"); }

        const fsm = $("fsm"); fsm.textContent = s.fsm_state || "—";
        fsm.className = s.fsm_state === "ARMING" ? "warn" : (s.fsm_state === "COOLDOWN" ? "muted" : "");
        $("slug").textContent = s.market_slug || "—";
        $("rem").textContent = s.remaining_s != null ? s.remaining_s + "s" : "—";
        $("fair").textContent = fmt(s.fair_up, 3);
        $("real").textContent = fmt(s.real_up, 3);
        signed($("gap"), s.gap, 3);
        $("vacuum").innerHTML = s.liquidity_vacuum ? '<span class="ko">⚠ VIDE</span>' : '<span class="ok">non</span>';
        $("kelly").textContent = fmt(s.kelly_size, 0) + " tk";

        if (s.in_position) {
          $("pos").innerHTML = `<span class="warn">${s.pos_side.toUpperCase()} ouverte</span>`;
          $("ets").textContent = `${fmt(s.pos_entry,2)} / ${fmt(s.pos_tp,2)} / ${fmt(s.pos_sl,2)}`;
        } else { $("pos").textContent = "à plat"; $("ets").textContent = "—"; }
        $("cash").textContent = fmt(s.cash, 2);
        $("equity").textContent = fmt(s.equity, 2);
        $("dd").textContent = fmt(s.drawdown, 2);
        $("shots").textContent = `${s.shots ?? 0} (${s.wins ?? 0}/${s.losses ?? 0})`;
        $("hr").textContent = ((s.hit_rate ?? 0) * 100).toFixed(1) + "%";
        
        // Giant PNL — nœud LIVE : PnL réel (Δ bankroll) ; nœud PAPER : PnL paper.
        const showLivePnl = isLiveNode && s.live_pnl != null;
        const pnlVal = showLivePnl ? s.live_pnl : s.realized_pnl;
        $("pnl-label").textContent = showLivePnl ? "REALIZED PNL — LIVE (réel, USDC)" : "REALIZED PNL — PAPER (USDC)";
        const giantPnl = $("giant-pnl");
        giantPnl.textContent = (pnlVal >= 0 ? "+" : "") + fmt(pnlVal, 2);
        giantPnl.className = "giant-pnl " + (pnlVal > 0 ? "pos" : (pnlVal < 0 ? "neg" : ""));
    }

    // Latences TCP — max affiché = 500 ms (Binance dépasse souvent, on sature la barre)
    const MAX_MS = 500;
    function latColor(ms) {
      if (ms == null) return "var(--muted)";
      if (ms < 60)  return "var(--green)";
      if (ms < 150) return "var(--amber)";
      return "var(--red)";
    }
    function renderLat(valId, barId, ms) {
      const el = $(valId), bar = $(barId);
      if (ms == null) { el.textContent = "—"; el.style.color = "var(--muted)"; bar.style.width = "0%"; return; }
      el.textContent = ms.toFixed(0) + " ms";
      el.style.color = latColor(ms);
      bar.style.width = Math.min(100, ms / MAX_MS * 100).toFixed(1) + "%";
      bar.style.background = latColor(ms);
    }
    
    if (isOrder) {
        renderLat("lat_b", "latbar_b", s.lat_binance_ms);
        renderLat("lat_o", "latbar_o", s.lat_okx_ms);
        // Avantage relatif Binance vs OKX
        const adv = $("lat_adv");
        if (s.lat_binance_ms != null && s.lat_okx_ms != null) {
          const diff = s.lat_binance_ms - s.lat_okx_ms;
          adv.textContent = (diff >= 0 ? "OKX +lead " : "Binance +lead ") + Math.abs(diff).toFixed(0) + " ms";
          adv.style.color = diff >= 0 ? "var(--green)" : "var(--red)";
        } else { adv.textContent = "—"; }
        $("lat_age").textContent = "mis à jour il y a < 5 s";
    }
    
    if (isKiller) {
        renderLat("lat_p", "latbar_p", s.lat_polymarket_ms);
    }

    if (isLiveNode) {
        const ms = (v) => (v == null ? "—" : v.toFixed(0) + " ms");
        const totColor = (v) => v == null ? "var(--muted)" : (v < 150 ? "var(--green)" : v < 400 ? "var(--amber)" : "var(--red)");
        const totEl = $("lat_total");
        if (totEl) { totEl.textContent = ms(s.lat_total_ms); totEl.style.color = totColor(s.lat_total_ms); }
        if ($("lat_transport")) $("lat_transport").textContent = ms(s.lat_transport_ms);
        if ($("lat_decide")) $("lat_decide").textContent = ms(s.lat_decide_ms);
        if ($("lat_post")) $("lat_post").textContent = ms(s.lat_post_ms);
    }

  } catch (e) {
    $("status").textContent = "✗ backend injoignable"; $("status").className = "ko";
  }
}
// ── Console de tuning à chaud ─────────────────────────────────────────────────
// Le panneau se construit dynamiquement depuis /params (aucune valeur en dur) ; il ne se
// reconstruit qu'après un Apply pour ne jamais écraser ce que l'opérateur est en train de saisir.
let paramsBounds = {}, paramsLoaded = {};

async function loadParams() {
  try {
    const p = await (await fetch("/params", { cache: "no-store" })).json();
    if (!p.enabled) { $("tuning-section").style.display = "none"; return; }
    $("tuning-section").style.display = "block";
    paramsBounds = p.bounds || {};
    paramsLoaded = p.params || {};
    const sel = $("scenario-select");
    if (sel.options.length === 0) {
      sel.innerHTML = '<option value="">— choisir —</option>' +
        (p.scenarios || []).map((n) => `<option value="${n}">${n}</option>`).join("");
    }
    buildGrid();
  } catch (e) {}
}

function stepFor(b) { const r = b.max - b.min; return r <= 1 ? 0.01 : (r <= 100 ? 1 : 10); }

function buildGrid() {
  const grid = $("tuning-grid");
  grid.innerHTML = Object.keys(paramsBounds).map((k) => {
    const b = paramsBounds[k], v = paramsLoaded[k];
    return `<div class="tuning-row">
      <label>${k}</label>
      <input type="number" data-key="${k}" min="${b.min}" max="${b.max}" step="${stepFor(b)}" value="${v}" />
      <span class="muted t-range">[${b.min} … ${b.max}]</span>
    </div>`;
  }).join("");
  grid.querySelectorAll("input").forEach((inp) => {
    inp.addEventListener("input", () => {
      const b = paramsBounds[inp.dataset.key], val = parseFloat(inp.value);
      inp.classList.toggle("invalid", Number.isNaN(val) || val < b.min || val > b.max);
    });
  });
}

async function postTuning(path, body) {
  try {
    const r = await (await fetch(path, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
    })).json();
    if (r.ok) { paramsLoaded = r.params; buildGrid(); tStatus("✓ appliqué", true); }
    else { tStatus("✗ " + (r.errors || ["erreur"]).join(" · "), false); }
  } catch (e) { tStatus("✗ backend injoignable", false); }
}

function applyParams() {
  const updates = {};
  $("tuning-grid").querySelectorAll("input").forEach((inp) => {
    const k = inp.dataset.key, val = parseFloat(inp.value);
    if (!Number.isNaN(val) && val !== paramsLoaded[k]) updates[k] = val;
  });
  if (Object.keys(updates).length === 0) { tStatus("aucun changement", true); return; }
  postTuning("/params", updates);
}
function applyScenario() {
  const name = $("scenario-select").value;
  if (!name) { tStatus("choisis un scénario", false); return; }
  postTuning("/scenario", { name });
}
function resetParams() {
  const updates = {};
  Object.keys(paramsBounds).forEach((k) => { updates[k] = paramsBounds[k].default; });
  postTuning("/params", updates);
}
window.applyParams = applyParams;
window.applyScenario = applyScenario;
window.resetParams = resetParams;
function tStatus(msg, ok) { const el = $("tuning-status"); el.textContent = msg; el.className = ok ? "ok" : "ko"; }

// ── Logs en direct (tous les nœuds) ───────────────────────────────────────────
async function pollLogs() {
  const sec = $("logs-section");
  if (!sec) return;
  try {
    const r = await (await fetch("/logs", { cache: "no-store" })).json();
    const lines = r.lines || [];
    sec.style.display = "block";
    $("log-node").textContent = "· " + (nodeKind || "");
    const view = $("log-view");
    const atBottom = view.scrollTop + view.clientHeight >= view.scrollHeight - 30;
    view.innerHTML = lines.slice(-300).map((l) => {
      let cls = "lvl-info";
      if (l.includes("ERROR")) cls = "lvl-error";
      else if (l.includes("WARN")) cls = "lvl-warn";
      return `<span class="${cls}">${escapeHtml(l)}</span>`;
    }).join("\n");
    if (atBottom) view.scrollTop = view.scrollHeight;
  } catch (e) {}
}
function escapeHtml(s) { return s.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c])); }

// ── Graphe entrées / sorties (mono / paper) ───────────────────────────────────
function cssVar(n) { return getComputedStyle(document.documentElement).getPropertyValue(n).trim(); }

async function pollChart() {
  const sec = $("chart-section");
  if (!sec) return;
  if (!(nodeKind === "mono" || nodeKind === "paper")) { sec.style.display = "none"; return; }
  sec.style.display = "block";
  try {
    const [series, trades] = await Promise.all([
      (await fetch("/series", { cache: "no-store" })).json(),
      (await fetch("/trades", { cache: "no-store" })).json(),
    ]);
    drawChart(series || [], trades || []);
  } catch (e) {}
}

function drawChart(series, trades) {
  const cv = $("trade-chart"); if (!cv) return;
  const ctx = cv.getContext("2d");
  const W = cv.width, H = cv.height;
  ctx.clearRect(0, 0, W, H);
  const padL = 52, padR = 16, padT = 14, padB = 26;

  let tradePts = trades.map((t) => ({ t: Date.parse(t.ts), price: t.price, kind: t.kind, side: t.side, pnl: t.pnl }))
    .filter((t) => !Number.isNaN(t.t) && t.price > 0);
  if (series.length < 2 && tradePts.length < 1) {
    ctx.fillStyle = cssVar("--muted"); ctx.font = "13px sans-serif";
    ctx.fillText("En attente de données…", padL, H / 2);
    return;
  }
  // Fenêtre temporelle = celle de la série (live) ; on ne garde que les trades dedans.
  const t0 = series.length ? series[0].t : Math.min(...tradePts.map((t) => t.t));
  const t1 = series.length ? series[series.length - 1].t : Math.max(...tradePts.map((t) => t.t));
  tradePts = tradePts.filter((t) => t.t >= t0 && t.t <= t1);
  const allP = series.flatMap((p) => [p.real, p.fair]).concat(tradePts.map((t) => t.price)).filter((v) => v > 0);
  if (allP.length < 1) {
    ctx.fillStyle = cssVar("--muted"); ctx.font = "13px sans-serif";
    ctx.fillText("En attente de données…", padL, H / 2);
    return;
  }
  let pmin = Math.min(...allP), pmax = Math.max(...allP);
  const pad = Math.max((pmax - pmin) * 0.12, 0.02); pmin -= pad; pmax += pad;
  const xs = (t) => padL + (t1 === t0 ? 0.5 : (t - t0) / (t1 - t0)) * (W - padL - padR);
  const ys = (p) => padT + (1 - (p - pmin) / (pmax - pmin || 1)) * (H - padT - padB);

  // Grille + axes
  ctx.strokeStyle = cssVar("--border-soft"); ctx.fillStyle = cssVar("--muted");
  ctx.font = "11px -apple-system, sans-serif"; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const p = pmin + (pmax - pmin) * i / 4, y = ys(p);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.fillText(p.toFixed(2), 8, y + 3);
  }
  for (let i = 0; i <= 4; i++) {
    const t = t0 + (t1 - t0) * i / 4, x = xs(t);
    ctx.fillText(new Date(t).toLocaleTimeString().slice(0, 5), x - 14, H - 8);
  }

  // Lignes real / fair
  const line = (key, color, dash) => {
    ctx.strokeStyle = color; ctx.lineWidth = 1.6; ctx.setLineDash(dash || []);
    ctx.beginPath(); let started = false;
    series.forEach((p) => { const v = p[key]; if (v <= 0) return; const x = xs(p.t), y = ys(v);
      if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y); });
    ctx.stroke(); ctx.setLineDash([]);
  };
  line("real", cssVar("--blue"), []);
  line("fair", cssVar("--accent"), [5, 4]);

  // Marqueurs entrées / sorties
  tradePts.forEach((t) => {
    const x = xs(t.t), y = ys(t.price);
    if (t.kind === "fire") {
      ctx.fillStyle = t.side === "up" ? cssVar("--green") : cssVar("--red");
      ctx.beginPath();
      if (t.side === "up") { ctx.moveTo(x, y - 6); ctx.lineTo(x - 5, y + 4); ctx.lineTo(x + 5, y + 4); }
      else { ctx.moveTo(x, y + 6); ctx.lineTo(x - 5, y - 4); ctx.lineTo(x + 5, y - 4); }
      ctx.closePath(); ctx.fill();
    } else {
      ctx.fillStyle = (t.pnl >= 0) ? cssVar("--green") : cssVar("--red");
      ctx.beginPath(); ctx.arc(x, y, 4, 0, 2 * Math.PI); ctx.fill();
      ctx.strokeStyle = cssVar("--panel"); ctx.lineWidth = 1.5; ctx.stroke();
    }
  });
}

setInterval(() => { $("clock").textContent = new Date().toLocaleTimeString(); }, 1000);
setInterval(refresh, 1000);
setInterval(pollLogs, 2000);
setInterval(pollChart, 2000);
refresh();
loadParams();
pollLogs();
pollChart();
