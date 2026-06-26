const $ = (id) => document.getElementById(id);
function fmt(n, d = 2) { return (n == null || Number.isNaN(n)) ? "—" : Number(n).toFixed(d); }
function signed(el, n, d = 2) { el.textContent = fmt(n, d); el.classList.toggle("pos", n > 0); el.classList.toggle("neg", n < 0); }
function obi(el, v) { el.textContent = (v >= 0 ? "+" : "") + fmt(v, 3); el.classList.toggle("pos", v > 0); el.classList.toggle("neg", v < 0); }

async function refresh() {
  try {
    const s = await (await fetch("/state", { cache: "no-store" })).json();
    $("status").textContent = "✓ connecté"; $("status").className = "ok";

    const dry = $("dry"); dry.textContent = s.dry_run ? "PAPER" : "LIVE"; dry.className = "badge " + (s.dry_run ? "paper" : "live");

    $("binance").innerHTML = s.binance_connected ? '<span class="ok">connecté</span>' : '<span class="ko">—</span>';
    $("okx").innerHTML = s.okx_connected ? '<span class="ok">connecté</span>' : '<span class="ko">—</span>';
    $("spot").textContent = fmt(s.btc_spot, 1);
    obi($("obib"), s.obi_binance); obi($("obio"), s.obi_okx); obi($("obic"), s.obi_consolidated);
    $("agree").innerHTML = s.agreement ? '<span class="ok">oui ✓</span>' : '<span class="ko">non ✗</span>';
    $("vel").textContent = (s.velocity >= 0 ? "+" : "") + (s.velocity * 100).toFixed(3) + "%";

    const fsm = $("fsm"); fsm.textContent = s.fsm_state || "—";
    fsm.className = s.fsm_state === "ARMING" ? "warn" : (s.fsm_state === "COOLDOWN" ? "muted" : "");
    $("slug").textContent = s.market_slug || "—";
    $("rem").textContent = s.remaining_s != null ? s.remaining_s + "s" : "—";
    $("fair").textContent = fmt(s.fair_up, 3);
    $("real").textContent = fmt(s.real_up, 3);
    signed($("gap"), s.gap, 3);
    $("vacuum").innerHTML = s.liquidity_vacuum ? '<span class="ko">⚠ VIDE</span>' : '<span class="ok">non</span>';
    $("kelly").textContent = fmt(s.kelly_size, 0) + " tk";

    const chk = (el, v) => { $(el).innerHTML = v ? '<span class="ok">✓</span>' : '<span class="ko">✗</span>'; };
    chk("c_agree", s.cond_agreement); chk("c_persist", s.cond_persist); chk("c_vel", s.cond_velocity);
    chk("c_gap", s.cond_gap); chk("c_ready", s.cond_ready);
    $("c_all").innerHTML = s.all_conditions ? '<span class="ok">🔥 FEU</span>' : '<span class="muted">en attente</span>';

    if (s.in_position) {
      $("pos").innerHTML = `<span class="warn">${s.pos_side.toUpperCase()} ouverte</span>`;
      $("ets").textContent = `${fmt(s.pos_entry,2)} / ${fmt(s.pos_tp,2)} / ${fmt(s.pos_sl,2)}`;
    } else { $("pos").textContent = "à plat"; $("ets").textContent = "—"; }
    $("cash").textContent = fmt(s.cash, 2);
    $("equity").textContent = fmt(s.equity, 2);
    signed($("pnl"), s.realized_pnl, 2);
    $("dd").textContent = fmt(s.drawdown, 2);
    $("shots").textContent = `${s.shots ?? 0} (${s.wins ?? 0}/${s.losses ?? 0})`;
    $("hr").textContent = ((s.hit_rate ?? 0) * 100).toFixed(1) + "%";

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
    renderLat("lat_b", "latbar_b", s.lat_binance_ms);
    renderLat("lat_o", "latbar_o", s.lat_okx_ms);
    renderLat("lat_p", "latbar_p", s.lat_polymarket_ms);

    // Avantage relatif Binance vs OKX
    const adv = $("lat_adv");
    if (s.lat_binance_ms != null && s.lat_okx_ms != null) {
      const diff = s.lat_binance_ms - s.lat_okx_ms;
      adv.textContent = (diff >= 0 ? "OKX +lead " : "Binance +lead ") + Math.abs(diff).toFixed(0) + " ms";
      adv.style.color = diff >= 0 ? "var(--green)" : "var(--red)";
    } else { adv.textContent = "—"; }

    // Âge de la dernière sonde (pas de timestamp côté Rust → affichage statique)
    $("lat_age").textContent = "mis à jour il y a < 5 s";
  } catch (e) {
    $("status").textContent = "✗ backend injoignable"; $("status").className = "ko";
  }
}
setInterval(() => { $("clock").textContent = new Date().toLocaleTimeString(); }, 1000);
setInterval(refresh, 1000);
refresh();
