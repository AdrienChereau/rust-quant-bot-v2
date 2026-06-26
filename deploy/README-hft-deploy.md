# Déploiement HFT multi-nœuds — Radar (Tokyo) / Exécuteur (Dublin)

Architecture : le **Radar** (AWS `ap-northeast-1`, Tokyo, près de Binance) calcule l'OBI consolidé
Binance+OKX et **tire un signal UDP de 6 octets** vers l'**Exécuteur** (AWS `eu-west-1`, Dublin),
qui exécute en **paper** sur Polymarket. Un seul binaire, deux sous-commandes.

```
[Binance/OKX] → RADAR (Tokyo) ──UDP 6 octets, port 8080──▶ EXÉCUTEUR (Dublin) → Polymarket (paper)
```

> ⚠️ **Paper only.** `DRY_RUN=true` est imposé tant que le signing EIP-712 n'est pas implémenté.
> L'exécuteur *simule* les ordres (`PaperEngine`). Ne pas retirer `DRY_RUN` (Step 5.3 du spec) :
> aucun ordre réel ne peut partir, et c'est une règle de sécurité du projet.

## Protocole UDP (6 octets, Little Endian)

| Octet | Champ   | Valeurs                                            |
|-------|---------|----------------------------------------------------|
| 0     | kind    | `0x00` DOWN · `0x01` UP · `0xFF` KILL              |
| 1     | size    | taille indicative u8 (l'exécuteur recalcule Kelly) |
| 2..5  | price   | `f32` LE = `fair_up` calculé côté radar            |

Fire-and-forget, sans ACK ni crypto : la sécurité repose sur le **Security Group AWS** (cf. infra).

## 1. Build (sur chaque EC2, ou cross-compile)

```bash
# Dépendances Ubuntu
sudo apt-get update && sudo apt-get install -y build-essential pkg-config libssl-dev
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
# Build release (lto=fat) — idéalement RUSTFLAGS natif sur l'EC2 cible
cd ~/rust-quant-bot && RUSTFLAGS="-C target-cpu=native" cargo build --release
```

## 2. Security Group (CRITIQUE)

Sur l'EC2 **Dublin** (exécuteur), autoriser l'entrée **UDP 8080 depuis l'IP publique de Tokyo**
uniquement :

```
Type: Custom UDP · Port: 8080 · Source: <IP_PUBLIQUE_TOKYO>/32
```

(Optionnel) ouvrir le port dashboard `8768` en TCP depuis ton IP / via Tailscale seulement.

## 3. systemd

```bash
# Radar (Tokyo) — éditer TARGET_EXECUTOR_IP = IP publique Dublin
sudo cp deploy/rust-quant-bot-radar.service /etc/systemd/system/
sudo nano /etc/systemd/system/rust-quant-bot-radar.service   # renseigner l'IP Dublin
sudo systemctl daemon-reload && sudo systemctl enable --now rust-quant-bot-radar

# Exécuteur (Dublin)
sudo cp deploy/rust-quant-bot-executor.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now rust-quant-bot-executor
```

Ajuster `User=` et les chemins `/home/<user>/...` dans les units selon l'AMI (souvent `ubuntu`).

## 4. Vérification (= Step 5.2 du spec, dry-run)

```bash
# Côté Dublin : les paquets arrivent
tail -f ~/rust-quant-bot/data/executor.log | grep -E "signal reçu|KILL reçu|SNIPE"
# Côté Tokyo : les signaux partent
tail -f ~/rust-quant-bot/data/radar.log | grep -E "signal UDP envoyé|strike fenêtre"
```

Succès : sur OBI soutenu, Tokyo logge `🚀 signal UDP envoyé` et Dublin `🎯 signal reçu de Tokyo`
puis un `🎯 SNIPE` simulé (paper). **`DRY_RUN` reste `true`** — on ne passe pas en live.

## Test loopback local (= Step 5.1, sans AWS, ex. sur cloudy)

Les deux rôles sur une seule machine (PORT différents pour éviter le conflit de dashboard) :

```bash
# Terminal A — exécuteur
PORT=8768 cargo run --release -- executor --listen-port 8080
# Terminal B — radar (cible loopback)
PORT=8769 cargo run --release -- radar --target-ip 127.0.0.1 --target-port 8080
```

## Mode mono (historique, inchangé)

`cargo run --release` (ou `... -- mono`) lance radar+exécuteur dans un seul processus — c'est le
déploiement actuel sur cloudy, non impacté par ce refactor.

---

# Live testing (paper → réel) — handoff

> ⚠️ **Deux verrous indépendants, plus le breaker.** Aucun ordre réel ne part sans les DEUX :
> 1. **feature `live`** à la compilation (sinon `sign_order_eip712` renvoie une erreur) ;
> 2. **`LIVE_ARMED=true`** au runtime (sinon l'ordre est signé + loggé mais **non envoyé**).
> Le circuit breaker (`MAX_DRAWDOWN`) coupe toute exécution si `equity ≤ START_CASH − MAX_DRAWDOWN`.

### Pré-requis toolchain
`alloy 2.1` exige **rustc ≥ 1.91**. Utiliser rustup (`rustup update stable`), pas un rust Homebrew
ancien. Le build **paper par défaut** n'a pas cette contrainte.

### Build + vérification du signing (sur box rustup à jour)
```bash
cargo test  --features live          # round-trip signature (hash→sign→recover == signer)
cargo build --release --features live
```

### Étapes de validation (ordre strict, cf. spec)
1. **Dry-Run Live** : `--features live`, `LIVE_ARMED=false`, credentials `POLY_*` dans `.env`.
   Activer le mode live au dashboard (Live ON + ▶ Live). Vérifier dans les logs la ligne
   `LIVE order signé` contenant `"orderType":"FAK"` — **rien n'est envoyé**.
2. **Test breaker** : mettre `MAX_DRAWDOWN=1`, forcer une perte (ou POST `/breaker/trip`).
   Vérifier l'arrêt de toute exécution + la bannière rouge clignotante au dashboard.
3. **Micro-Test Live** : `LIVE_ARMED=true`, taille minimale (≥ `min_order_size`). UN seul ordre
   réel pour valider le workflow L2 + la **parité** de la signature (le code seul ne la prouve pas).

### ⚠️ À faire AVANT d'armer le live
Le dashboard (et ses endpoints POST `/live/enable`…) écoute sur `0.0.0.0`. **Le binder sur
localhost/Tailscale ou le protéger** avant `LIVE_ARMED=true` — sinon n'importe qui sur le réseau
peut basculer les modes. Confirmer aussi l'`EXCHANGE_CTF` (neg-risk = adresse différente) et le
`signatureType` (3) pour ton compte.

### Credentials
`POLY_API_KEY/SECRET/PASSPHRASE/FUNDER_ADDRESS/PRIVATE_KEY` générés EN AMONT (flow L1 hors bot),
dans `.env` (jamais commité). Voir `.env.example`.
