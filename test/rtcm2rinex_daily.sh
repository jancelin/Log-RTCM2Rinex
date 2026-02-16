#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

IN_DIR="${1:-.}"                # dossier des .rtcm
OUT_DIR="${2:-$IN_DIR}"         # dossier de sortie des .obs
STATION="${3:-BENGLA1}"         # station / préfixe
MARKER="${4:-BENGLA1}"          # convbin -hm
ANT_HGT="${5:-0.0}"             # convbin -hc
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

  # convbin attend -ts/-te au format YYYY/MM/DD hh:mm:ss :contentReference[oaicite:1]{index=1}
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

