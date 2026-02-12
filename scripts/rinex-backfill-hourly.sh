#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

# Optionnel: helpers (station_var, rate_token...)
if [[ -f /opt/scripts/lib.sh ]]; then
  # shellcheck disable=SC1091
  source /opt/scripts/lib.sh
fi

# Charge env
if [[ -f /config/.env ]]; then
  set -a; source /config/.env; set +a
fi

PUB_ROOT="${PUB_ROOT:-/data/pub}"
STATIONS_FILE="/config/stations.list"
DAY="${1:?Usage: rinex-backfill-hourly.sh YYYY-MM-DD}"

read_stations() {
  awk '
    /^[[:space:]]*#/ {next}
    /^[[:space:]]*$/ {next}
    { mp=$1; rid=$2; if (rid=="") rid=mp; print mp, rid }
  ' "$STATIONS_FILE"
}

# fallback if rate_token not provided by lib.sh
rate_token_fallback() {
  case "$1" in
    1) echo "01S" ;;
    30) echo "30S" ;;
    5) echo "05S" ;;
    10) echo "10S" ;;
    *) printf "%02dS" "$1" ;;
  esac
}

log() { echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] backfill $*"; }

pick_tmp_root() {
  local candidates=()
  [[ -n "${TMP_ROOT:-}" ]] && candidates+=("$TMP_ROOT")
  candidates+=("$PUB_ROOT/tmp")
  candidates+=("/tmp")
  local d
  for d in "${candidates[@]}"; do
    mkdir -p "$d" 2>/dev/null || true
    if [[ -d "$d" && -w "$d" ]]; then
      echo "$d"; return 0
    fi
  done
  return 1
}

atomic_write_gzip() {
  local src="${1:?}"
  local dst="${2:?}"
  local tmp="${dst}.tmp"
  rm -f "$tmp"
  gzip -c "$src" > "$tmp"
  mv -f "$tmp" "$dst"
}

patch_rinex_header() {
  local rnx="$1"
  local log_hint="${2:-}"

  local pgm="${RINEX_PGM:-CONVBIN EX 2.5.0}"
  local runby="${RINEX_RUNBY:-CentipedeLogger}"
  local dt="$(date -u +'%Y%m%d %H%M%S UTC')"

  awk -v pgm="$pgm" -v runby="$runby" -v dt="$dt" -v log_hint="$log_hint" '
    function fmt_pgm() { return sprintf("%-20s%-20s%-20s%-20s", pgm, runby, dt, "PGM / RUN BY / DATE") }

    $0 ~ /PGM \/ RUN BY \/ DATE$/ { print fmt_pgm(); next }

    $0 ~ /END OF HEADER$/ {
      print sprintf("%-60s%-20s","format: RTCM 3","COMMENT")
      if (log_hint != "") print sprintf("%-60s%-20s","log: " log_hint,"COMMENT")
      print $0
      next
    }

    { print $0 }
  ' "$rnx" > "${rnx}.patched"

  mv -f "${rnx}.patched" "$rnx"
}

# Deduplicate RINEX epochs (keeps first occurrence, drops subsequent duplicate blocks).
dedup_rinex_epochs() {
  local f="$1"
  [[ "${RINEX_DEDUP_EPOCHS:-true}" == "true" ]] || return 0
  [[ -s "$f" ]] || return 0

  local dup
  dup="$(awk 'BEGIN{d=0}
            /^>/{k=$2" "$3" "$4" "$5" "$6" "$7; if(seen[k]++){d++}}
            END{print d}' "$f" 2>/dev/null || echo 0)"
  [[ "${dup:-0}" =~ ^[0-9]+$ ]] || dup=0

  if (( dup > 0 )); then
    log "WARN duplicate epochs detected in $(basename "$f") (count=$dup) -> dedup"
    awk 'BEGIN{keep=1}
         /^>/{k=$2" "$3" "$4" "$5" "$6" "$7; if(seen[k]++){keep=0}else{keep=1}}
         {if(keep)print}
        ' "$f" > "${f}.dedup" && mv -f "${f}.dedup" "$f"
  fi
}

collect_files_for_hour() {
  local mp="$1"
  local hour_start_epoch="$2"      # exact HH:00:00 epoch
  local edge_hours="${3:-1}"       # include +/- edge_hours

  local -a epochs=()
  local i
  for ((i=edge_hours; i>=1; i--)); do epochs+=( $((hour_start_epoch - i*3600)) ); done
  epochs+=( "$hour_start_epoch" )
  for ((i=1; i<=edge_hours; i++)); do epochs+=( $((hour_start_epoch + i*3600)) ); done

  local -a files=()
  local ep y doy ymd hh dir

  for ep in "${epochs[@]}"; do
    y="$(date -u -d "@$ep" +%Y)"
    doy="$(date -u -d "@$ep" +%j)"
    ymd="$(date -u -d "@$ep" +%Y-%m-%d)"
    hh="$(date -u -d "@$ep" +%H)"
    dir="$PUB_ROOT/rtcm_raw/$y/$doy/$mp"
    [[ -d "$dir" ]] || continue
    files+=( "$dir"/*_"$ymd"_"$hh"-*.rtcm )
  done

  IFS=$'\n' files=($(printf '%s\n' "${files[@]}" | sed '/^$/d' | sort -u)); unset IFS
  ((${#files[@]} > 0)) || return 0
  printf '%s\n' "${files[@]}"
}

INTERVAL="${RINEX_HOURLY_INTERVAL_S:-1}"
if command -v rate_token >/dev/null 2>&1; then
  RATE="$(rate_token "$INTERVAL")"
else
  RATE="$(rate_token_fallback "$INTERVAL")"
fi

RINEX_VER="${RINEX_VERSION:-3.04}"
OUT_ROOT="${RINEX_OUT_ROOT_HOURLY:-$PUB_ROOT/centipede_1s}"
FREQ="${CONVBIN_FREQ:-4}"
EDGE="${RINEX_HOURLY_EDGE_HOURS:-1}"

TODAY="$(date -u +%F)"
NOWH="$(date -u +%H)"

# boucle 00..23
for HH in $(seq -w 00 23); do
  # si DAY == aujourd’hui, ne pas traiter l’heure courante/future (sauf override)
  if [[ "$DAY" == "$TODAY" && "${ALLOW_PARTIAL_TODAY:-false}" != "true" ]]; then
    if (( 10#$HH >= 10#$NOWH )); then
      continue
    fi
  fi

  hour_start_epoch="$(date -u -d "${DAY} ${HH}:00:00" +%s)"
  year="$(date -u -d "@$hour_start_epoch" +%Y)"
  doy="$(date -u -d "@$hour_start_epoch" +%j)"
  ymd_slash="$(date -u -d "@$hour_start_epoch" +%Y/%m/%d)"

  mapfile -t stations < <(read_stations)

  for line in "${stations[@]}"; do
    mp="$(awk '{print $1}' <<<"$line")"
    rid="$(awk '{print $2}' <<<"$line")"

    out_dir="$OUT_ROOT/$year/$doy"
    mkdir -p "$out_dir"

    base="${rid}_S_${year}${doy}${HH}00_01H_${RATE}_MO"
    out="$out_dir/${base}.crx.gz"
    [[ -s "$out" ]] && continue

    mapfile -t files < <(collect_files_for_hour "$mp" "$hour_start_epoch" "$EDGE" || true)
    ((${#files[@]} > 0)) || continue

    tmp_root="$(pick_tmp_root)" || { log "ERR  $mp hour=$HH -> no writable tmp dir"; continue; }

    tmp_rtcm="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${HH}_XXXX.rtcm")"
    tmp_rnx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${HH}_XXXX.rnx")"
    tmp_crx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${HH}_XXXX.crx")"
    trap 'rm -f "$tmp_rtcm" "$tmp_rnx" "$tmp_crx"' RETURN

    log "CAT  $mp $DAY hour=$HH (${#files[@]} files)"
    cat "${files[@]}" > "$tmp_rtcm"

    # RENAG-like: MARKER NAME = rid ; MARKER NUMBER = mp
    ant_hgt="${RINEX_ANT_HGT_DEFAULT:-2.7}"
    hd_args=()
    [[ -n "${ant_hgt:-}" ]] && hd_args=(-hd "${ant_hgt}/0/0")

    log "RUN  convbin $mp $DAY hour=$HH -> $base"
    if ! convbin "$tmp_rtcm" \
      -v "$RINEX_VER" -r rtcm3 \
      -hm "$rid" -hn "$mp" \
      -f "$FREQ" \
      "${hd_args[@]}" \
      -os \
      -ti "$INTERVAL" -tt 0 \
      -ts "$ymd_slash" "${HH}:00:00" -te "$ymd_slash" "${HH}:59:59" \
      -o "$tmp_rnx"
    then
      log "WARN $mp hour=$HH -> convbin failed — skip"
      continue
    fi

    if [[ ! -s "$tmp_rnx" ]] || ! grep -q "END OF HEADER" "$tmp_rnx" || ! grep -qE '^>' "$tmp_rnx"; then
      log "WARN $mp hour=$HH -> empty/no epochs — skip"
      continue
    fi

    patch_rinex_header "$tmp_rnx" "$PUB_ROOT/rtcm_raw/$year/$doy/$mp"

    dedup_rinex_epochs "$tmp_rnx"

    rnx2crx < "$tmp_rnx" > "$tmp_crx" || { log "WARN $mp hour=$HH -> rnx2crx failed"; continue; }
    [[ -s "$tmp_crx" ]] || { log "WARN $mp hour=$HH -> empty CRX"; continue; }

    atomic_write_gzip "$tmp_crx" "$out"
    log "OK   $mp hour=$HH -> $(basename "$out")"
  done
done

