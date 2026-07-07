# Déploiement Copy Maker V8.4 — split Tokyo/Dublin (PAPER)

## 0. Supprimer les anciens services (une fois par serveur)
```bash
systemctl list-units --type=service --all | grep -iE 'quant|poly|bot'   # repérer les noms
sudo systemctl disable --now <ancien-service>
sudo rm /etc/systemd/system/<ancien-service>.service && sudo systemctl daemon-reload
```

## 1. Remplacer le code (une fois par serveur)
```bash
cd ~ && mv rust-quant-bot-v2 rust-quant-bot-v2.old   # l'ancien reste sur disque
git clone git@github.com:AdrienChereau/rust-quant-bot-v2.git
cp rust-quant-bot-v2.old/.env rust-quant-bot-v2/backend/.env   # récupérer vos creds
```
Puis compléter `backend/.env` depuis `deploy/tokyo.env.example` (Tokyo)
ou `deploy/dublin.env.example` (Dublin). Sur Tokyo, mettre l'IP de Dublin
dans `SIGNAL_TARGET` (port 8080).

## 2. Déployer
```bash
# Dublin D'ABORD (il doit écouter avant que Tokyo n'émette)
cd ~/rust-quant-bot-v2/deploy && ./deploy.sh executor
# Puis Tokyo
cd ~/rust-quant-bot-v2/deploy && ./deploy.sh radar
```

## 3. Vérifier (dans l'ordre)
```bash
# Tokyo émet : log "radar obi=… micro=…" chaque seconde
journalctl -u poly-radar -f | head
# Dublin reçoit : drift non-nul + AUCUN "signal Tokyo PÉRIMÉ"
journalctl -u poly-executor -f | head
# Dashboard (depuis votre machine) :
ssh -L 8767:localhost:8767 <dublin>   # puis http://localhost:8767
```
Santé : la tuile « fenêtre en cours » vit, les résolutions tombent toutes les
5 min, et le tableau vérifie contre gamma-api. Si « signal Tokyo PÉRIMÉ »
apparaît : liaison UDP coupée → vérifier security group 8080/udp et l'IP dans
SIGNAL_TARGET (le bot retire ses quotes tout seul en attendant, aucun risque).

## Étape B — LIVE (paper et live en PARALLÈLE sur Dublin)

Le live est un 2e process : même binaire, feature `live`, `.env.live` séparé,
état `data/live_*`, dashboard :8771, UDP :8081. Le paper continue sur :8767/:8080.

```bash
# 1. Tokyo : ajouter la 2e cible UDP puis restart
echo "SIGNAL_TARGET2=<ip-dublin>:8081" >> ~/rust-quant-bot-v2/backend/.env
sudo systemctl restart poly-radar

# 2. Dublin : créer backend/.env.live depuis deploy/dublin-live.env.example
#    (recopier les POLY_* du .env existant ; LIVE_ARMED=false pour commencer)
# 3. Déployer le service live
cd ~/rust-quant-bot-v2/deploy && ./deploy.sh live
```

Validation (ordre STRICT) :
1. `journalctl -u poly-live -f` : « credentials POLY chargées », « client POLY_1271
   authentifié », « LIVE démarré — collatéral USDC réel », « user_ws: connecté »,
   puis des « ordre LIVE signé » SANS « ✅ accepté » (= dry-run armé correctement).
2. 1 h propre → `LIVE_ARMED=true` dans .env.live + `sudo systemctl restart poly-live`.
3. Surveiller les 1ers « ✅ ordre LIVE accepté » + fills WS + le portfolio Polymarket.
4. Heartbeat = dead-man : si le process meurt, le CLOB annule nos ordres en ~15 s.

## Ancienne étape B (mémo)
Gate : 3 sessions paper consécutives positives sur le serveur. Puis :
DRY_RUN=false, POLY_SIG_TYPE=3, capital 30$, clips micro — et la boucle
d'ordres GTC réels reste à porter (voir checklist du dashboard, section 3).
