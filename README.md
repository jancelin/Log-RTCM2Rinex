# Log-RTCM2Rinex — Centipede-RTK logger + archives RINEX

Ce projet exécute dans Docker les outils **RTKLIBExplorer v2.5.0** pour :
- **logger** des flux RTCM3 depuis un caster NTRIP (`str2str`) en continu (24/7),
- **convertir** les logs RTCM en **RINEX** (`convbin`),
- produire des fichiers **Hatanaka** (`rnx2crx`) puis **gzip** pour obtenir des `*.crx.gz`.

Tous les temps / noms de fichiers / planifications sont en **UTC** (`TZ=UTC`).

---

## Produits générés

### 1) RINEX journalier 30s (après minuit UTC + 20 min)
- **Quand :** tous les jours à **00:20 UTC**
- **Quoi :** conversion de la **journée précédente** (00:00–23:59 UTC)
- **Sortie :**

```
data/pub/centipede_30s/YYYY/DOY/
<RINEX_ID>_S_YYYYDOY0000_01D_30S_MO.crx.gz
```

Exemple :

```
A00100FRA_S_20260010000_01D_30S_MO.crx.gz
```

### 2) RINEX horaire 1s (toutes les heures UTC + 3 min)
- **Quand :** toutes les heures à **HH:03 UTC**
- **Quoi :** conversion de **l’heure précédente** (HH-1:00–HH-1:59 UTC) pour avoir une heure complète
- **Sortie :**

```
data/pub/centipede_1s/YYYY/DOY/
<RINEX_ID>_S_YYYYDOYHH00_01H_01S_MO.crx.gz
```

Exemple (heure 00:00–00:59) :

```
A00100FRA_S_20260010000_01H_01S_MO.crx.gz
```

---

## Logs RTCM bruts

Les fichiers RTCM sont stockés par station et par jour (DOY) :

```
data/pub/rtcm_raw/YYYY/DOY/<MOUNTPOINT>/ <MOUNTPOINT>*YYYY-MM-DD_hh-mm-ss*<SUFFIX>.rtcm
```

La rotation RTCM est configurable (par défaut **horaire**) via `RTCM_ROTATE_HOURS`.

---

## Structure du projet

```
.
├── docker-compose.yml
├── Dockerfile
├── config/
│   ├── .env
│   └── stations.list
├── scripts/
│   ├── entrypoint.sh
│   ├── lib.sh
│   ├── logger-manager.sh
│   ├── station-logger.sh
│   ├── rinex-converter.sh
│   ├── converter1s.sh
│   └── converter30s.sh
└── data/
    └── pub/
        ├── centipede_30s/
        ├── centipede_1s/
        ├── rtcm_raw/
        └── logs/
        ├── events/
        └── traces/
````

> Important : on monte **`./config/` en répertoire** dans le conteneur (`/config`) pour éviter les problèmes d’“atomic save” (inode) avec les bind-mounts de fichiers.

---

## Prérequis

- Docker + Docker Compose v2
- Accès réseau au caster NTRIP
- Espace disque suffisant (RTCM + RINEX)
- Identifiants NTRIP (si nécessaires)

---

## Démarrage rapide

1) Configure `.env` :
- `PUID` / `PGID` (pour que les fichiers créés sur l’hôte appartiennent à ton user)
- `NTRIP_HOST`, `NTRIP_PORT`, `NTRIP_USER`, `NTRIP_PASS`
- options de robustesse `str2str`
- options de conversion RINEX

2) Configure `stations.list` :
- **1 ligne par station**
- format : `MOUNTPOINT  RINEX_ID`

Exemple :
```txt
# mountpoint   rinex_id
BENGLA1        BENGL01BGD
CT02           A61300FRA
````

3. Lancer :

```bash
docker compose up -d --build
```

4. Suivre les logs :

```bash
docker compose logs -f logger
docker compose logs -f converter1s
docker compose logs -f converter30s
```


---

## Architecture des convertisseurs (converter1s / converter30s)

On sépare la conversion en **deux services Docker** indépendants :

- **`converter1s`** : génère les **RINEX horaires 1s** (heure précédente, typiquement à `HH:03` UTC).
- **`converter30s`** : génère les **RINEX journaliers 30s** (jour précédent, typiquement à `00:20` UTC).

### Pourquoi cette séparation ?

Dans un design “monolithique”, si la conversion horaire prend longtemps (beaucoup de stations, charge CPU/IO),
la boucle principale peut **rater l’instant exact** du déclenchement journalier (ex: `00:20`) et le daily ne part pas.
Avec deux conteneurs :
- chaque planification est **indépendante** (pas de blocage mutuel),
- tu peux ajuster **le parallélisme** et les ressources par service,
- tu simplifies les redémarrages/debug (un converter n’impacte pas l’autre),
- tu peux **multiplier** les converters plus tard (ex: un `converter1s_bis` avec d’autres stations ou un autre planning).

### Zéro duplication de logique

La logique est **unique** dans `scripts/rinex-converter.sh`.  
Les scripts `converter1s.sh` et `converter30s.sh` ne font que poser des variables d’environnement (nom/role/tmp)
puis `exec` le script commun.

### Identité et fichiers de monitoring

Chaque converter définit `CONVERTER_NAME`, ce qui **sépare** :

- logs : `data/pub/logs/events/<CONVERTER_NAME>.log`
- heartbeat : `data/pub/logs/events/<CONVERTER_NAME>.heartbeat`
- status : `data/pub/logs/events/<CONVERTER_NAME>.status.json`

Le **cleanup** (rétention) est activé uniquement sur `converter30s` (`FORCE_CLEANUP_ENABLE=true`) par défaut.

---

## Ajouter / enlever des stations “à chaud”

Tu modifies simplement `config/stations.list` :

* **ajout** : ajoute une ligne, sauvegarde → le logger démarre le `str2str` correspondant
* **suppression** : retire la ligne → le logger stoppe uniquement ce mountpoint

Vérification (conteneur voit bien le fichier) :

```bash
docker compose exec logger bash -lc 'cat -n /config/stations.list'
```

Logs manager :

```
data/pub/logs/events/logger-manager.log
```

---

## Variables principales (`config/.env`)

### Identité / droits

* `TZ=UTC`
* `PUID=1000`
* `PGID=1000`

### NTRIP

* `NTRIP_HOST=crtk.net`
* `NTRIP_PORT=2101`
* `NTRIP_USER=...`
* `NTRIP_PASS=...`

### Arborescence

* `PUB_ROOT=/data/pub`
* `RINEX_OUT_ROOT_DAILY=/data/pub/centipede_30s`
* `RINEX_OUT_ROOT_HOURLY=/data/pub/centipede_1s`

### Logging RTCM / robustesse

* `RTCM_ROTATE_HOURS=1` (rotation des fichiers RTCM)
* `RTCM_SWAP_MARGIN_S=10` (option `-f` de `str2str`)
* `STR2STR_TIMEOUT_MS=10000`
* `STR2STR_RECONNECT_MS=10000`
* `STR2STR_TRACE_LEVEL=2`
* `RTCM_SUFFIX=GNSS-1`
* watchdog “base down” :

  * `STALE_CHECK_EVERY_SEC=60`
  * `STALE_AFTER_SEC=300`

### Conversion RINEX / compression

* `RINEX_VERSION=3.04`
* Daily 30s :

  * `RINEX_DAILY_ENABLE=true`
  * `RINEX_DAILY_INTERVAL_S=30`
* Hourly 1s :

  * `RINEX_HOURLY_ENABLE=true`
  * `RINEX_HOURLY_INTERVAL_S=1`
* Compression :

  * `RINEX_HATANAKA=true`
  * `RINEX_GZIP=true`

### Planification UTC

* Horaire 1s à **HH:03** :

  * `RINEX_HOURLY_AT_MINUTE=3`
* Journalier 30s à **00:20** :

  * `RINEX_DAILY_AT=00:20`

---

## Logs & diagnostic

### Politique de logs (optimisation stockage)

Le gros volume de logs venait des **messages console RTKLIB** de `str2str` (une ligne d'état toutes les ~5s). Dans l’ancienne version, ces messages étaient capturés dans `data/pub/logs/events/<MP>.log` (ex: `CT02.log`) ce qui grossit très vite et n’apporte pas de monitoring utile en production.

Nouvelle logique :

* **Monitoring persistant** via `data/pub/logs/events/<MP>.events.log` (événements: base down, redémarrages, etc.).
* **Console RTKLIB (très verbeuse)** désactivée par défaut : le fichier `data/pub/logs/events/<MP>.log` n’est généré que si tu passes en mode debug.

Variables dans `config/.env` :

* `LOG_LEVEL=ERROR|WARN|INFO|DEBUG` : niveau global (logger-manager + converter).
* `STATION_EVENTS_LEVEL=WARN` : niveau écrit dans `<MP>.events.log` (recommandé en prod: WARN).
* `STATION_CAPTURE_LOG=auto|true|false` : capture de la console RTKLIB dans `<MP>.log` (**très verbeux**). `auto` = activé uniquement si `LOG_LEVEL=DEBUG`.

Recommandation production : `LOG_LEVEL=INFO`, `STATION_EVENTS_LEVEL=WARN`, `STATION_CAPTURE_LOG=auto`.

### Traces RTKLIB (`str2str`)

Un fichier de trace par station :

```
data/pub/logs/traces/<MOUNTPOINT>/str2str.trace
```

### Événements (pannes, station “stale”, démarrages/arrêts)

* par station :

```
data/pub/logs/events/<MOUNTPOINT>.events.log
```

* manager :

```
data/pub/logs/events/logger-manager.log
```

* converter :

```
data/pub/logs/events/converter.log
```

## Lancer la fabrication des rinexs hourly à la machine

```
docker compose exec -u 1000:1000 converter bash -lc '/opt/scripts/rinex-backfill-hourly.sh 2026-02-12'
```

---

## Notes de scalabilité (vers ~1000 stations)

Ce design fonctionne très bien pour quelques stations à quelques dizaines.s
Pour **~1000 stations**, attention :

* 1000 processus `str2str` sur une seule machine = très lourd (CPU, IO, fichiers ouverts, réseau).
* stratégie recommandée : **sharding** (plusieurs hôtes / plusieurs stacks compose) et répartition des stations.
* surveiller : IO disque, inode, limites `ulimit`, bande passante, stabilité réseau.
