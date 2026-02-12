
# Logging RTCM (NTRIP) avec `str2str` + conversion en RINEX journalier (RTKLIBExplorer v2.5.0)

Ce guide décrit une procédure simple (manuel + `tmux`) pour :
1. Installer **RTKLIBExplorer v2.5.0** (`str2str`, `convbin`)
2. Logger un flux **RTCM3** depuis un caster **NTRIP** avec rotation horaire
3. Convertir les logs RTCM (segments horaires) en **1 RINEX `.obs` par jour**

---

## Pré-requis

- Système : Debian/Ubuntu (apt)
- Accès internet
- Droits `root` (ou `sudo`)
- Identifiants NTRIP et mountpoint

---

## 1) Installation de `str2str` et `convbin` (RTKLIBExplorer v2.5.0)

```bash
RTKLIB_RELEASE='RTKLIB-2.5.0'

apt-get update
apt-get install -y git build-essential wget

wget -qO - https://github.com/rtklibexplorer/RTKLIB/archive/refs/tags/v2.5.0.tar.gz | tar -xvz

make --directory="${RTKLIB_RELEASE}"/app/consapp/str2str/gcc
make --directory="${RTKLIB_RELEASE}"/app/consapp/str2str/gcc install

make --directory="${RTKLIB_RELEASE}"/app/consapp/convbin/gcc
make --directory="${RTKLIB_RELEASE}"/app/consapp/convbin/gcc install
````

Vérifier que les binaires sont disponibles :

```bash
which str2str
which convbin
str2str -h | head
convbin -h | head
```

---

## 2) Installation de `tmux`

`tmux` permet de laisser `str2str` tourner en tâche de fond, même si la session SSH est coupée.

```bash
apt-get install -y tmux
```

---

## 3) Démarrer une session `tmux` pour la collecte RTCM

Créer une session dédiée :

```bash
tmux new -s str2str
```


Créer un nouveau dossier pour stocker les logs rtcm:

```bash
mkdir /mondossier/rtcm
```

Se placer dans le dossier qui servira de stockage des logs :

```bash
cd /mondossier/rtcm
```


Lancer la collecte du flux RTCM3 via NTRIP avec rotation **horaire** (`S=1`) :

```bash
str2str \
  -in  ntrip://centipede:centipede@crtk.net:2101/BENGLA1 \
  -out file://BENGLA1_%Y-%m-%d_%h-%M-%S_GNSS-1.rtcm::T::S=1 \
  -f 10
```

### Notes

* `-in ntrip://user:pass@host:port/MOUNTPOINT` : connexion au caster NTRIP.
* `-out file://...` : écriture dans un fichier.
* `%Y-%m-%d_%h-%M-%S` : horodatage dans le nom du fichier.
* `::S=1` : rotation toutes les **1 heure** (mettre `S=24` pour 24h).
* `-f 10` : marge liée au swap/rotation (secondes).

---

## 4) Sortir / revenir dans `tmux`

Sortir de la session (laisser tourner en arrière-plan) :

* `Ctrl` + `b`, puis `d`

Revenir dans la session :

```bash
tmux attach -t str2str
```

---

## 5) Conversion : RTCM segmentés → RINEX journalier

Objectif : si ton dossier contient des logs RTCM découpés (ex: 1 fichier/h), produire **un seul** fichier RINEX `.obs` par jour, et **ne pas reconvertir** les jours déjà convertis.

### 5.1 Arborescence recommandée

Dans `/mondossier/` créer un dossier de sortie `rinex` :

```bash
cd /mondossier
mkdir -p rinex
```

### 5.2 Créer le script `rtcm2rinex_daily.sh`

Dans `/mondossier`, créer le fichier de programme :

```bash
nano rtcm2rinex_daily.sh
```

Copier-coller le contenu suivant :

```bash
#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

IN_DIR="${1:-.}"                # dossier des .rtcm
OUT_DIR="${2:-$IN_DIR}"         # dossier de sortie des .obs
STATION="${3:-BENGLA1}"         # station / préfixe
MARKER="${4:-BENGLA1}"          # convbin -hm
ANT_HGT="${5:-2.7}"             # convbin -hc
INTERVAL="${6:-30}"             # convbin -ti (s)

mkdir -p "$OUT_DIR"

# Liste des jours présents (extrait YYYY-MM-DD depuis les noms de fichiers)
mapfile -t DAYS < <(
  find "$IN_DIR" -maxdepth 1 -type f -name "*.rtcm" -printf "%f\n" \
  | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' \
  | sort -u
)

if ((${#DAYS[@]}==0)); then
  echo "Aucun .rtcm trouvé dans $IN_DIR"
  exit 0
fi

for day in "${DAYS[@]}"; do
  out_obs="$OUT_DIR/${STATION}_GNSS-${day}.obs"

  # skip si déjà converti (ou compressé)
  if [[ -s "$out_obs" || -s "$out_obs.gz" ]]; then
    echo "[SKIP] $day -> $(basename "$out_obs") (déjà présent)"
    continue
  fi

  # Fichiers RTCM de ce jour (tri lexical = OK si timestamp dans le nom)
  mapfile -t files < <(
    find "$IN_DIR" -maxdepth 1 -type f -name "*${day}*.rtcm" -printf "%p\n" | sort
  )

  if ((${#files[@]}==0)); then
    echo "[WARN] aucun fichier pour $day (pourtant détecté dans la liste) — skip"
    continue
  fi

  tmp_rtcm="$(mktemp --tmpdir "${STATION}_${day}_XXXX.rtcm")"
  tmp_obs="${out_obs}.tmp"
  trap 'rm -f "$tmp_rtcm" "$tmp_obs"' RETURN

  echo "[CAT ] $day -> $(basename "$tmp_rtcm") (${#files[@]} fichiers)"
  cat "${files[@]}" > "$tmp_rtcm"

  # convbin attend -ts/-te au format YYYY/MM/DD hh:mm:ss
  day_slash="${day//-/\/}"

  echo "[RUN ] convbin -> $(basename "$out_obs")"
  convbin "$tmp_rtcm" \
    -v 3.04 -r rtcm3 \
    -hc "$ANT_HGT" -hm "$MARKER" \
    -f 2 -y J -y S -y C -y I \
    -od -os -oi -ot -ti "$INTERVAL" -tt 0 \
    -ts "$day_slash" 00:00:00 -te "$day_slash" 23:59:59 \
    -o "$tmp_obs"

  mv -f "$tmp_obs" "$out_obs"
  rm -f "$tmp_rtcm"
  trap - RETURN

  echo "[OK  ] $out_obs"
done
```

### 5.3 Rendre le script exécutable

```bash
chmod +x rtcm2rinex_daily.sh
```

### 5.4 Lancer la conversion

Exemple (logs dans `./rtcm` et sortie dans `./rinex`) :

```bash
./rtcm2rinex_daily.sh ./rtcm ./rinex BENGLA1 BENGLA1 2.7 30
```

* `./rtcm` : dossier d’entrée contenant les `.rtcm`
* `./rinex` : dossier de sortie des `.obs`
* `BENGLA1` : nom station (préfixe de sortie)
* `BENGLA1` : marker name RINEX (`-hm`)
* `2.7` : hauteur antenne (`-hc`)
* `30` : intervalle d’échantillonnage (s) (`-ti`)

---

## 6) Contrôles rapides

Lister les logs RTCM :

```bash
ls -lh /mondossier/rtcm/*.rtcm | tail
```

Lister les RINEX générés :

```bash
ls -lh /mondossier/rinex/*.obs | tail
```

Vérifier qu’un jour déjà converti est bien “skip” :

```bash
./rtcm2rinex_daily.sh /mondossier/rtcm /mondossier/rinex BENGLA1 BENGLA1 2.7 30
```

---

## 7) Remarques / limites de cette méthode (manuel + tmux)

* Cette procédure est idéale pour démarrer vite.
* Pour plusieurs bases GNSS (~6) et une exécution 24/7, il sera plus robuste de passer à une gestion par services (`systemd`) avec redémarrage automatique, logs centralisés, et 1 instance par mountpoint.

