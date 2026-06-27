# MetaMask → rust-quant-bot — prochaines étapes

Guide après dépôt des fonds sur Polymarket avec MetaMask connecté.

---

## 1. Vérifier ton `POLY_SIG_TYPE`

Compare les deux adresses :

| Source | Adresse |
|--------|---------|
| MetaMask → ton compte → copier l'adresse | EOA signataire |
| [polymarket.com/settings](https://polymarket.com/settings) → adresse du profil | Funder (collatéral) |

**Même adresse** → `POLY_SIG_TYPE=0` (EOA pur — ton cas probable si tu as déposé via l'UI sans proxy distinct).

**Adresses différentes** → `POLY_SIG_TYPE=2` (Gnosis Safe proxy). Dans ce cas :
- `POLY_FUNDER_ADDRESS` = adresse du profil Polymarket (proxy)
- `POLY_SIGNER_ADDRESS` = adresse MetaMask (EOA)

> Le bot supporte `sig_type` 0, 1 et 2. **Pas** le `sig_type=3` (deposit wallet Magic).

---

## 2. Exporter la clé privée MetaMask

1. MetaMask → compte → ⋮ → **Détails du compte**
2. **Exporter la clé privée** (mot de passe MetaMask requis)
3. Format attendu : `0x` + 64 caractères hex

**Ne jamais** committer cette clé. Elle reste dans `.env` / `.env.local` (gitignored).

---

## 3. Régénérer les credentials API L2

Les anciennes clés (compte Magic) ne fonctionnent pas. Il faut en créer de nouvelles liées à MetaMask.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r scripts/requirements.txt
python scripts/derive_poly_creds.py   # lit .env / .env.local automatiquement
```

Ou avec export manuel :

```bash
export POLY_PRIVATE_KEY=0x<VOTRE_CLE>
export POLY_FUNDER_ADDRESS=0x<PROXY_POLymarket>
export POLY_SIG_TYPE=2
python scripts/derive_poly_creds.py
```

Le script affiche `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_PASSPHRASE` à copier dans `.env`.

---

## 4. Remplir `.env` (ou `.env.local`)

Exemple pour **sig_type=0** (MetaMask = funder) :

```env
POLY_PRIVATE_KEY=0x...
POLY_FUNDER_ADDRESS=0x...        # même adresse que MetaMask
POLY_SIGNER_ADDRESS=0x...        # optionnel (défaut = funder)
POLY_SIG_TYPE=0

POLY_API_KEY=...
POLY_API_SECRET=...
POLY_PASSPHRASE=...

LIVE_ARMED=false
```

Exemple pour **sig_type=2** (proxy Polymarket) :

```env
POLY_PRIVATE_KEY=0x...           # clé MetaMask
POLY_FUNDER_ADDRESS=0x...        # proxy (settings Polymarket)
POLY_SIGNER_ADDRESS=0x...        # adresse MetaMask EOA
POLY_SIG_TYPE=2
```

---

## 5. Build avec la feature `live`

```bash
rustup update stable   # alloy exige rustc ≥ 1.91
cargo test --features live
cargo build --release --features live
```

---

## 6. Validation (ordre strict)

### Étape A — Vérifier le solde CLOB

```bash
cargo run --release --features live
```

Active **Live ON** au dashboard (`http://localhost:8768`). Le champ **live bankroll** doit afficher ton solde déposé (~5 $). Si erreur auth → revoir credentials ou `POLY_SIG_TYPE`.

### Étape B — Dry-Run Live (`LIVE_ARMED=false`)

1. Garde `LIVE_ARMED=false`
2. Active Live ON + ▶ Live au dashboard
3. Dans les logs, cherche `LIVE order signé` avec `"orderType":"FAK"` et `"signatureType":0`
4. **Aucun ordre n'est envoyé** — c'est normal

### Étape C — Micro-test réel (`LIVE_ARMED=true`)

1. Protège le dashboard (localhost / Tailscale uniquement)
2. Passe `LIVE_ARMED=true` dans `.env`
3. Redémarre le bot
4. **Un seul** ordre réel, taille minimale
5. Vérifie dans les logs : `✅ ordre LIVE accepté`

### Étape D — Circuit breaker

Teste `MAX_DRAWDOWN=1` ou le bouton breaker du dashboard pour confirmer l'arrêt d'urgence.

---

## Checklist rapide

- [ ] Fonds visibles sur polymarket.com
- [ ] `POLY_SIG_TYPE` confirmé (0 ou 2)
- [ ] Clé privée MetaMask exportée → `POLY_PRIVATE_KEY`
- [ ] Credentials API régénérés via `scripts/derive_poly_creds.py`
- [ ] `.env` mis à jour (7 variables `POLY_*`)
- [ ] `cargo build --release --features live` OK
- [ ] Solde CLOB visible au dashboard
- [ ] Dry-run live validé (log `LIVE order signé`, rien envoyé)
- [ ] Micro-test live avec `LIVE_ARMED=true`

---

## Déploiement AWS

Sur le serveur, copie `.env` (pas `.env.local`) avec les 7 variables `POLY_*`.
Rebuild avec `--features live` :

```bash
cargo build --release --features live
sudo systemctl restart rust-quant-bot-executor
tail -f ~/rust-quant-bot/data/executor.log | grep -iE "bankroll|balance"
```

Succès : `💰 bankroll réelle CLOB` avec ton solde. Erreur `401 Invalid api key` → credentials
régénérés pour le mauvais wallet ou `POLY_SIG_TYPE` incorrect.


| Symptôme | Cause probable | Fix |
|----------|----------------|-----|
| `401 Unauthorized` | Credentials obsolètes ou mauvais wallet | Régénérer via le script |
| `INVALID_SIGNATURE` | Mauvais `POLY_SIG_TYPE` | Revoir comparaison adresses (§1) |
| `Invalid Funder Address` | Funder = MetaMask au lieu du proxy | Utiliser l'adresse settings Polymarket |
| Solde CLOB = 0 | Mauvais `signature_type` dans la requête balance | Aligner `POLY_SIG_TYPE` |
| `not enough balance` | Fonds sur mauvaise adresse | Vérifier funder vs proxy |
