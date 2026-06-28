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
        $("ctl_sizing").innerHTML = s.live_force_min
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
setInterval(() => { $("clock").textContent = new Date().toLocaleTimeString(); }, 1000);
setInterval(refresh, 1000);
refresh();
