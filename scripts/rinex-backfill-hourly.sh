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
  # shellcheck disable=SC1091
  set -a; source /config/.env; set +a
fi

PUB_ROOT="${PUB_ROOT:-/data/pub}"
STATIONS_FILE="/config/stations.list"
DAY="${1:?Usage: rinex-backfill-hourly.sh YYYY-MM-DD}"

# Pool global de parallélisme (sur toutes les tâches station×heure)
BACKFILL_PARALLEL="${BACKFILL_PARALLEL:-${CONVERT_PARALLEL:-2}}"
if ! [[ "$BACKFILL_PARALLEL" =~ ^[0-9]+$ ]]; then BACKFILL_PARALLEL=2; fi
if (( BACKFILL_PARALLEL < 1 )); then BACKFILL_PARALLEL=1; fi

# Decode stations.list tokens where internal spaces were encoded as '|'
decode_ws_token() {
  local s="${1:-}"
  printf '%s' "${s//|/ }"
}

# Sanitize free-form values for RINEX header fields passed to RTKLIB convbin
# (convbin expects fields separated by '/'). RINEX header fields are fixed width.
sanitize_field() {
  local s="${1:-}"
  s="${s//$'\t'/ }"
  while [[ "$s" == *"  "* ]]; do s="${s//  / }"; done
  s="${s#"${s%%[! ]*}"}"  # ltrim spaces
  s="${s%"${s##*[! ]}"}"  # rtrim spaces
  s="${s//\//-}"
  if (( ${#s} > 20 )); then
    s="${s:0:20}"
  fi
  printf '%s' "$s"
}

read_stations() {
  awk '
    /^[[:space:]]*#/ {next}
    /^[[:space:]]*$/ {next}
    {
      mp=$1; rid=$2; if (rid=="") rid=mp;
      x=$3; y=$4; z=$5;
      rec_type=$6; rec_ver=$7; ant_type=$8;
      ant_h=$9; ant_e=$10; ant_n=$11;
      print mp, rid, x, y, z, rec_type, rec_ver, ant_type, ant_h, ant_e, ant_n
    }
  ' "$STATIONS_FILE"
}

# fallback si rate_token non fourni
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
  local dt
  dt="$(date -u +'%Y%m%d %H%M%S UTC')"

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

# Dédup epochs RINEX (garde le 1er bloc, supprime les suivants)
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
  local hour_start_epoch="$2"
  local edge_hours="${3:-1}"

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

  if ((${#files[@]}==0)); then
    return 0
  fi
  IFS=$'\n' files=($(printf '%s\n' "${files[@]}" | sed '/^$/d' | sort -u)); unset IFS
  printf '%s\n' "${files[@]}"
}

# ---- Inputs & defaults ----

OUT_ROOT="${RINEX_OUT_ROOT_HOURLY:-$PUB_ROOT/centipede_1s}"
RINEX_VER="${RINEX_VERSION:-3.04}"
INTERVAL="${RINEX_HOURLY_INTERVAL_S:-1}"
EDGE="${RINEX_HOURLY_EDGE_HOURS:-${RTCM_EDGE_HOURS:-1}}"

# Rate token
if command -v rate_token >/dev/null 2>&1; then
  RATE="$(rate_token "$INTERVAL")"
else
  RATE="$(rate_token_fallback "$INTERVAL")"
fi

# Frequency mask (RTKLIB convbin -f)
FREQ="${CONVBIN_FREQ:-${CONVBIN_FREQ_MASK:-4}}"

TODAY="$(date -u +%Y-%m-%d)"
NOWH="$(date -u +%H)"

process_station_hour() {
  local mp="$1"
  local rid="$2"
  local x="$3" y="$4" z="$5"
  local rec_type_in="$6" rec_ver_in="$7" ant_type_in="$8"
  local ant_h_in="$9" ant_e_in="${10}" ant_n_in="${11}"
  local HH="${12}"
  local hour_start_epoch="${13}"
  local year="${14}"
  local doy="${15}"
  local ymd_slash="${16}"

  local out_dir="$OUT_ROOT/$year/$doy"
  mkdir -p "$out_dir"

  local base out
  base="${rid}_S_${year}${doy}${HH}00_01H_${RATE}_MO"
  out="$out_dir/${base}.crx.gz"
  [[ -s "$out" ]] && return 0

  local -a files=()
  mapfile -t files < <(collect_files_for_hour "$mp" "$hour_start_epoch" "$EDGE" || true)
  ((${#files[@]} > 0)) || return 0

  local tmp_root
  tmp_root="$(pick_tmp_root)" || { log "ERR  $mp hour=$HH -> no writable tmp dir"; return 0; }

  local tmp_rtcm tmp_rnx tmp_crx
  tmp_rtcm="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${HH}_XXXX.rtcm")"
  tmp_rnx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${HH}_XXXX.rnx")"
  tmp_crx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${HH}_XXXX.crx")"

  cleanup() { rm -f "$tmp_rtcm" "$tmp_rnx" "$tmp_crx" 2>/dev/null || true; }
  trap cleanup EXIT

  log "CAT  $mp $DAY hour=$HH (${#files[@]} files)"
  cat "${files[@]}" > "$tmp_rtcm"

  # Decode metadata
  local rec_num ant_num rec_type rec_ver ant_type
  rec_num="${RINEX_REC_NUM_DEFAULT:-UNKNOWN}"
  ant_num="${RINEX_ANT_NUM_DEFAULT:-UNKNOWN}"

  rec_type="$(decode_ws_token "${rec_type_in:-UNKNOWN}")"
  rec_ver="$(decode_ws_token "${rec_ver_in:-UNKNOWN}")"
  ant_type="$(decode_ws_token "${ant_type_in:-NONE|NONE}")"

  local ant_h ant_e ant_n
  ant_h="${ant_h_in:-${RINEX_ANT_HGT_DEFAULT:-0.0}}"
  ant_e="${ant_e_in:-0.0}"
  ant_n="${ant_n_in:-0.0}"

  local -a hp_args=() hr_args=() ha_args=() hd_args=()
  if [[ -n "${x:-}" && -n "${y:-}" && -n "${z:-}" ]]; then
    hp_args=(-hp "${x}/${y}/${z}")
  fi

  hr_args=(-hr "$(sanitize_field "$rec_num")/$(sanitize_field "$rec_type")/$(sanitize_field "$rec_ver")")
  ha_args=(-ha "$(sanitize_field "$ant_num")/$(sanitize_field "${ant_type:-NONE NONE}")")
  hd_args=(-hd "${ant_h}/${ant_e}/${ant_n}")

  log "RUN  convbin $mp $DAY hour=$HH -> $base"
  if ! convbin "$tmp_rtcm" \
      -v "$RINEX_VER" -r rtcm3 \
      -hm "$rid" -hn "$mp" \
      "${hp_args[@]}" \
      "${hr_args[@]}" \
      "${ha_args[@]}" \
      "${hd_args[@]}" \
      -f "$FREQ" \
      -os \
      -ti "$INTERVAL" -tt 0 \
      -ts "$ymd_slash" "${HH}:00:00" -te "$ymd_slash" "${HH}:59:59" \
      -o "$tmp_rnx"
  then
    log "WARN $mp hour=$HH -> convbin failed — skip"
    return 0
  fi

  if [[ ! -s "$tmp_rnx" ]] || ! grep -q "END OF HEADER" "$tmp_rnx" || ! grep -qE '^>' "$tmp_rnx"; then
    log "WARN $mp hour=$HH -> empty/no epochs — skip"
    return 0
  fi

  patch_rinex_header "$tmp_rnx" "$PUB_ROOT/rtcm_raw/$year/$doy/$mp"
  dedup_rinex_epochs "$tmp_rnx"

  rnx2crx < "$tmp_rnx" > "$tmp_crx" || { log "WARN $mp hour=$HH -> rnx2crx failed"; return 0; }
  [[ -s "$tmp_crx" ]] || { log "WARN $mp hour=$HH -> empty CRX"; return 0; }

  atomic_write_gzip "$tmp_crx" "$out"
  log "OK   $mp hour=$HH -> $(basename "$out")"
}

# ---- Backfill global pool station×heure ----
mapfile -t stations < <(read_stations)
log "TASKPOOL day=${DAY} stations=${#stations[@]} parallel=${BACKFILL_PARALLEL}"

pids=()
tasks=0

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

  for line in "${stations[@]}"; do
    # stations.list is whitespace-separated; REC_TYPE/REC_VER/ANT_TYPE may contain '|'
    read -r mp rid x y z rec_type rec_ver ant_type ant_h ant_e ant_n <<<"$line"
    [[ -n "${mp:-}" ]] || continue
    [[ -n "${rid:-}" ]] || rid="$mp"

    process_station_hour \
      "$mp" "$rid" \
      "${x:-}" "${y:-}" "${z:-}" \
      "${rec_type:-}" "${rec_ver:-}" "${ant_type:-}" \
      "${ant_h:-}" "${ant_e:-}" "${ant_n:-}" \
      "$HH" "$hour_start_epoch" "$year" "$doy" "$ymd_slash" &

    pids+=("$!")
    ((tasks++)) || true

    # throttle pool
    while (( ${#pids[@]} >= BACKFILL_PARALLEL )); do
      if wait -n 2>/dev/null; then :; else wait "${pids[0]}" || true; fi
      alive=()
      for pid in "${pids[@]}"; do
        kill -0 "$pid" 2>/dev/null && alive+=("$pid")
      done
      pids=("${alive[@]}")
    done
  done

done

for pid in "${pids[@]}"; do
  wait "$pid" || true

done

log "DONE tasks=${tasks}"
