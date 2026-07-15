# LA LOI — Stratégie de référence (grand modèle 0xb, v2 « flotteur grand-livre »)

*Figée le 14 juillet 2026, après l'étude de 7 fenêtres au microscope et la passe de
mesure sur 56 fenêtres complètes de l'observatoire 0xb27b (fills, imbalance, prix,
merges). Ce document est LA référence : toute modification du bot doit se justifier
par rapport à lui, ou le mettre à jour.*

---

## 1. Les deux étages de la machine

### Étage 1 — LE CHÂSSIS (tourne toujours, sans opinion)

Cotation bilatérale continue en maker, **achats uniquement** :

- **Zéro vente, jamais** (`SC_ALLOW_FLATTEN=false`). Poussière comprise. Les résidus
  courent jusqu'à la résolution : le gagnant paie au redeem, le perdant expire.
  0xb mesuré : 100 % d'achats sur 2 383 fills.
- Grille d'ouverture échelonnée qui **suit le touch marche par marche** (chasse dès
  1 pas d'échelle + 1 tick d'écart, cooldown 4 s pour préserver la file sur les
  oscillations courtes).
- **Extrêmes ouverts** : paires d'ouverture jusqu'à `SC_OPEN_PAIR_TARGET=0.99`
  (0xb fait les 2/3 de son volume de fenêtre décidée à 96-99¢ + 1-3¢).
- **Merge en continu** pour recycler le capital (0xb : 3-4 merges/fenêtre, capital
  retourné ~1×/fenêtre). On merge quand on a besoin de se replacer.
- **Jamais unijambiste** : le côté déficitaire (par rapport à la CIBLE, cf. étage 2)
  porte toujours un achat exécutable ; l'assurance qui RÉDUIT l'écart ne connaît
  aucun délai.
- Discipline de paire **souple** : complétion ordinaire jusqu'à ~`1.02`
  (médiane 0xb mesurée : 100,5¢, p25-p75 : 97,8-103,4). L'inventaire plein prime
  sur la marge par paire. Les escalades au-delà restent gouvernées par la rampe
  de confiance du sauvetage (borne EV, dur 0,95 de room).

Rôle économique : transformer « avoir tort » en une perte de 1-4¢ par paire au lieu
d'une jambe nue, et produire le volume.

### Étage 2 — LE FLOTTEUR : TOUJOURS du côté GAGNANT (doctrine ferme)

Le bot vise en permanence une **imbalance cible** `T` (signée, en parts) au lieu de
la symétrie. Tout le moteur (complétion, retraits, room, assurance) travaille vers
`imbalance = T` et non plus vers zéro.

**Règle utilisateur, non négociable : on a plus du côté gagnant que du côté
perdant, tout le temps.** La direction de T = le gagnant du moment :

1. **Tokyo d'abord** (tilt confirmé ±0,5) — il voit le renversement 1-3 s avant le
   carnet, il établit avant le prix et flippe avant le recroisement.
2. **Le leader du prix ensuite** (côté > 52¢, bande morte 48-52 — 0xb flippe à
   47¢ Up médian).
3. Prix hésitant et Tokyo muet → on garde le camp courant (pas de churn).

- **Taille : PROPORTIONNELLE** — `SC_FLOAT_PCT` (15 %) × volume moyen d'un côté
  sur la fenêtre, plafonnée à `SC_FLOAT_SHARES` (12). 0xb flotte à ~14 % de son
  volume (138 parts pour ~950/côté) — un RATIO, pas une constante : ±12 fixe dès
  la première minute = 80-100 % du livre (la violence du 15 juil.). La cible
  grandit avec la fenêtre : ~0 au départ, pleine taille quand le livre est gros.
- **Établissement tôt** : 0xb à t médian 50 s, leader à 55¢.
- **Flip = geste défensif, jamais un second pari** : le camp change quand le
  GAGNANT change (Tokyo ou recroisement), avec temporisation (dwell) anti-churn.
  Mesuré : les fenêtres avec flip ne rapportent pas plus (−6$ vs −3$) — le flip
  limite les dégâts, il ne cherche pas le gain.
- La paire chère n'excuse rien : quand nos paires sont sous l'eau, le flotteur
  gagnant est aussi ce qui les rembourse (le mode « compensation » mesuré chez
  0xb, aligné 56 % des points quand sa paire dépasse 1$).
- **DISJONCTEUR DE FENÊTRE HACHÉE** (« 3 retournements dans les 2 dernières
  minutes », `SC_CHOP_FLIPS=3` / `SC_CHOP_WINDOW_S=120`) : collée au strike
  (±10$), une fenêtre n'a PAS de gagnant — le leader PM flippe au moindre
  frémissement et Tokyo ne voit pas des écarts de 10$ (son rôle reste de blinder
  les creux). 3 retournements du leader dans la fenêtre glissante : cible → 0 et
  « urgence prix » désarmée — on sauve les meubles : quoting symétrique (farmer
  l'oscillation, le meilleur régime du châssis), complétions maker, merges et
  assurance de fin de fenêtre continuent. Les retournements VIEILLISSENT : si le
  zigzag s'apaise et qu'une vraie tendance s'installe, le directionnel se réarme
  tout seul. (15 juil. 00:00 : 5 flips en 2 min, sommets payés en FAK ≥ 0.69, −25$.)

**Ce qu'on ne copie PAS de 0xb : son mode « convexité »** (mesuré : flotteur
contrarien 60 % des points quand ses paires gagnent — un ticket de loterie sur le
côté pas cher). C'est un luxe financé par ses rebates, que nous n'avons pas, et il
contredit la règle « plus de gagnant, tout le temps ». Chez nous : jamais de
flotteur côté perdant.

### Fin de fenêtre — LA CONVERSION

Quand la poussière du côté opposé au flotteur cote ≤ `SC_CONV_DUST` (~5-6¢) dans la
dernière minute : **T revient à 0** — la complétion avale la poussière et convertit
le flotteur en paires certaines (95¢ probables + 3¢ = 1,00$ garanti). Si la
poussière est plus chère, le flotteur (gagnant) court au redeem. On vise 0, jamais
l'autre bord : à aucun moment on ne détient plus de perdant que de gagnant.

---

## 2. Les chiffres qui fondent la loi (passe de mesure, 56 fenêtres)

- Flotteur présent dans **100 %** des fenêtres ; max médian **138 parts = 14 %** du
  volume d'un côté ; établi à **t=50 s** médian, leader à **55¢**.
- Flips (±40 croisés) : 0 flip 39 % / 1 flip 50 % / ≥2 11 % ; moment médian
  **t=171 s** ; prix Up au flip **47¢** ; paire courante au flip **100,7¢**.
- Alignement du flotteur 0xb : **56 %** avec le leader quand paire > 1$ ;
  **40 %** (donc 60 % contrarien) quand paire < 1$ — son grand livre pilote sa
  direction. NOUS ne copions que la moitié gagnante (cf. étage 2).
- Coût de paire médian **100,5¢** (p25-p75 : 97,8-103,4).
- « Précision directionnelle » du flotteur 0xb : **48 %** — ce n'est pas un pari
  chez lui : la moitié de ses flotteurs sont délibérément contrariens.
- PnL trading 0xb sur l'échantillon : **−294$ / 56 fenêtres** (+25$ quand aligné
  juste, −34$ quand faux ; 46 % de fenêtres vertes). L'échantillon précédent de
  19 fenêtres : +599$. Vérité : **son trading oscille autour de zéro ; son profit
  est le REBATE sur ~2 000-2 300$ de volume/fenêtre** (2-5$/fenêtre, 318k$/110 j).
- Volume aux extrêmes (≥90¢ ou ≤10¢) : 12 % médian ; résidu final >5 parts dans
  91 % des fenêtres.

## 3. L'équation économique — la sienne et la nôtre

`PnL_0xb = paires (≈0 en moyenne) + convexité (jackpots de reverse) + REBATES (le salaire)`

**Nous n'avons pas son salaire** (à notre taille le rebate se compte en centimes).
Copier sa machine à l'identique = copier un break-even sans la rente. Donc :

`PnL_nous = paires (≈0) + flotteur MEILLEUR que sa pièce de monnaie (Tokyo) + rebates (bonus)`

## 4. Notre supplément : Tokyo arbitre le flotteur

Ce que 0xb décide en regardant son propre carnet, nous le décidons avec Binance
**1 à 3 s avant le carnet** :

- **Établissement** : si Tokyo crie avant que le prix ait choisi (50/50), le
  flotteur s'établit sur l'appel Tokyo, pas sur le leader.
- **Flip** : Tokyo retourne le flotteur PLUS TÔT que le recroisement du prix.
- Le chemin chaud (impulsion, FAK d'accumulation, assurance sans délai) reste
  intact — il sert le flotteur au lieu de la symétrie.

**Gate de vérité** : le compteur `dir_wins/dir_total` (précision Tokyo, live) et la
ventilation PnL par intention. Si notre précision s'installe ≥ ~55-58 %, la machine
est rentable par le trading ; en dessous, elle ne l'est pas — chiffres, pas
impressions. Revue à ~50 fenêtres jouées.

## 5. Invariants non négociables (rappel des doctrines)

1. **ZÉRO VENTE** — flatten/coupe/sortie éclair inclus. Se couper avant la fin =
   acheter l'autre côté (conversion), jamais vendre.
2. Achat du perdant < 0,40 comme PARI : uniquement Tokyo qui crie. Jamais de
   flotteur côté perdant.
3. **JAMAIS UNIJAMBISTE — la règle tient, intégralement** : le côté déficitaire
   porte toujours un achat exécutable, et un achat qui réduit l'écart n'attend
   jamais. Ce qui a changé avec le flotteur : l'écart se mesure à la CIBLE
   (détenir le surplus gagnant VOULU n'est pas être unijambiste — être en manque
   du côté gagnant, si). Toute la chaîne d'assurance (impulsion, urgence prix,
   borne EV, tranches) est intacte.
   **Et la frontière pari/assurance est ABSOLUE** (15 juil. 00:35, −8$) : le
   taker n'a le droit que de RÉDUIRE |imbalance| vers 0. CONSTRUIRE ou
   maintenir le flotteur (s'éloigner de 0 vers la cible) = un pari → maker
   uniquement, dans les creux — c'est le rôle de Tokyo. Le sauvetage qui
   achetait 21 Down @0.79-0.80 au sommet pour « atteindre la cible » −12
   fabriquait des paires >1$ en boucle contre sa propre aspiration.
4. **Plus de gagnant que de perdant, tout le temps** : le flotteur suit le
   gagnant du moment ; la conversion de fin vise 0, jamais l'autre bord.
5. Tailles : GTC ≥ 5 parts, FAK entier (notionnel 2 décimales), LOT_SIZE 2 déc.
6. Les fills WS se comptent en `size_matched` absolu par ordre (jamais les
   montants des events trade).
7. Jamais de `rm` sur les états/logs.

## 6. Mapping configuration

| Env | Défaut | Rôle |
|---|---|---|
| `SC_FLOAT_PCT` | 0.15 | flotteur = 15 % du volume moyen d'un côté (ratio 0xb mesuré) |
| `SC_FLOAT_SHARES` | 12 | PLAFOND absolu du flotteur (parts) |
| `SC_FLOAT_DWELL_S` | 10 | temporisation entre deux changements de cible (anti-churn) |
| `SC_CONV_DUST` | 0.06 | seuil poussière de la conversion de fin (T→0 sous T−60) |
| `SC_CHOP_FLIPS` | 3 | fenêtre hachée : retournements du leader dans la fenêtre glissante |
| `SC_CHOP_WINDOW_S` | 120 | largeur de la fenêtre glissante du disjoncteur (réarmement auto) |
| `SC_OPEN_PAIR_TARGET` | 0.99 | somme des paires d'ouverture (extrêmes ouverts) |
| `SC_COMPLETION_MAX_PAIR` | 1.02 | plafond de la complétion ordinaire (paire souple) |
| `SC_MAX_IMBALANCE` | 30 | cap DUR de l'écart au-delà de la cible |
| `SC_ALLOW_FLATTEN` | false | ventes de flatten — INTERDITES |

Le reste (impulsion, tilt Binance, rampe de sauvetage, borne EV, échelle 3×3,
chasse +1 tick, budget fenêtre recyclé au merge) : inchangé, au service de la cible.

## 7. Ce qu'on mesure pour juger (après ~50 fenêtres jouées)

1. `dir_wins/dir_total` (précision Tokyo à l'établissement du flotteur).
2. PnL par intention : les paires (symmetric_open + completion) doivent être ≈ 0
   ou mieux ; le flotteur (skew_accumulation + rescue) doit être > 0 si Tokyo > 55 %.
3. Coût de paire médian ≤ ~101¢ ; volume/fenêtre en hausse (la grille 0.99 doit
   se voir immédiatement).
4. Résidus finaux : majoritairement côté poussière, jamais > cap.
