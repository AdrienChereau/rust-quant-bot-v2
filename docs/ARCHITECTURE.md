# Architecture — rust-quant-bot (v3 split paper/live)

## Pourquoi le split

Avant (v2), le nœud Exécuteur (Dublin) exécutait paper ET live dans **la même hot-loop**, même
process, même runtime tokio. Le paper taxait le live :
- `paper.fire()` = walk VWAP synchrone + écriture disque dans le chemin d'exécution ;
- `paper.manage()` à chaque tick 50 ms ;
- dashboard `RwLock` en écriture toutes les 50 ms sur le thread des ordres ;
- sizing live couplé à l'instance `PaperEngine`.

v3 sépare en **deux nœuds maigres sur deux machines** : le live n'a aucun code paper, le paper aucun
code live. Isolation OS totale → 0 contention.

## Topologie

```
   Tokyo (radar)  ──UDP 14o──►  Dublin (live)   [--features live]  dashboard :8769
        │  (1) live d'abord                       OrderEngine → CLOB
        └──UDP 14o──►  Box paper (paper)  [build défaut]          dashboard :8768
           (2) paper ensuite                       PaperEngine (sim VWAP)
```

Un seul binaire, sélection du rôle par sous-commande CLI (`src/main.rs`) :
`radar` · `live` · `paper` · `executor` (alias live) · `mono` · `poly` (outils).

## Hot-path (nœud live, `src/roles/live.rs`)

1. `udp::listen` reçoit un `WireSignal` (14 octets, `src/net/wire.rs`).
2. Handler `Attack` (immédiat, hors tick) : calcul `gap`, checks de rejet (breaker, pause, marché,
   fin de fenêtre, cooldown, gap_min), sizing `KellyParams::kelly_size_for`, soumission non-bloquante
   à l'`OrderEngine` (`src/polymarket/order_engine.rs`) via mpsc → POST CLOB **hors hot-loop**.
3. Tick 50 ms : snapshot `pm` (Mutex), drain résultats OrderEngine (oneshot non-bloquant), gestion de
   position live (TP/SL/max-hold → SELL), circuit breaker, écriture dashboard.

Le paper (`src/roles/paper.rs`) suit la même structure mais avec `PaperEngine` (fills VWAP simulés) et
sans aucune credential/OrderEngine/POST.

## Locks & atomics

| Donnée | Type | Hot-path | Background | Risque |
|--------|------|----------|------------|--------|
| `RuntimeControls` (paper_paused, live_paused, live_enabled, breaker) | `AtomicBool` Relaxed | lecture | dashboard POST | nul |
| `bankroll` (live) | `watch::channel` | borrow lock-free | poll CLOB 10 s | nul |
| `PmShared` (marché + carnets) | `Arc<Mutex>` | snapshot/tick | pm_poller + pm_ws | faible (tick) |
| `DashState` | `Arc<RwLock>` | write/tick 50 ms | HTTP /state | faible, hors process critique |

Isolation v3 : paper et live ne partagent **plus aucune** de ces structures (process distincts).

## Protocole UDP (`src/net/wire.rs`, v2 = 14 octets)

`[0]` kind (0x00 DOWN / 0x01 UP / 0xFF KILL) · `[1]` size u8 · `[2..6]` price f32 LE ·
`[6..14]` `sent_ms` u64 LE (horloge radar — mesure de latence transport, requiert NTP).

## Fichiers clés

- `src/main.rs` — CLI + `run_mono`.
- `src/roles/{live,paper,radar}.rs` — les 3 nœuds.
- `src/net/{wire,udp}.rs` — transport UDP.
- `src/strategy/{bankroll,live_position,sniper}.rs` — sizing/FSM/exécution.
- `src/polymarket/{order_engine,live_executor,pm_*}.rs` — CLOB + flux marché.
- `src/dashboard.rs` + `frontend/` — observabilité (un nœud = une vue, piloté par `node_kind`).
