#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

# shellcheck disable=SC1091
source /opt/scripts/lib.sh

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
  # Keep it reasonably short for the fixed-width RINEX header fields.
  if (( ${#s} > 20 )); then
    s="${s:0:20}"
  fi
  printf '%s' "$s"
}
PUB_ROOT="${PUB_ROOT:-/data/pub}"
EVENTS_DIR="$PUB_ROOT/logs/events"
mkdir -p "$EVENTS_DIR"

# Converter instance identity (allows multiple converters running in parallel)
CONVERTER_NAME="${CONVERTER_NAME:-converter}"
LOG_FILE="$EVENTS_DIR/${CONVERTER_NAME}.log"
HB_FILE="$EVENTS_DIR/${CONVERTER_NAME}.heartbeat"
STATUS_FILE="$EVENTS_DIR/${CONVERTER_NAME}.status.json"

logc() {
  local lvl="INFO"
  local first=""
  if (( $# > 0 )); then
    first="${1%% *}"
  fi

  case "$first" in
    0|ERROR|ERR)
      lvl="ERROR"
      if [[ "$1" == "$first" ]]; then
        shift || true
      else
        set -- "${1#${first} }" "${@:2}"
      fi
      ;;
    1|WARN|WARNING)
      lvl="WARN"
      if [[ "$1" == "$first" ]]; then
        shift || true
      else
        set -- "${1#${first} }" "${@:2}"
      fi
      ;;
    2|INFO)
      lvl="INFO"
      if [[ "$1" == "$first" ]]; then
        shift || true
      else
        set -- "${1#${first} }" "${@:2}"
      fi
      ;;
    3|DEBUG)
      lvl="DEBUG"
      if [[ "$1" == "$first" ]]; then
        shift || true
      else
        set -- "${1#${first} }" "${@:2}"
      fi
      ;;
  esac
  local threshold="${LOG_LEVEL:-INFO}"
  log_should "$lvl" "$threshold" || return 0
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] ${CONVERTER_NAME} $lvl $*" | tee -a "$LOG_FILE"
}

heartbeat() {
  local now
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf "%s\n" "$now" > "$HB_FILE.tmp" 2>/dev/null || true
  mv -f "$HB_FILE.tmp" "$HB_FILE" 2>/dev/null || true
  # lightweight JSON status for external monitoring
  role="auto"
  if [[ "${RINEX_HOURLY_ENABLE:-false}" == "true" && "${RINEX_DAILY_ENABLE:-false}" == "true" ]]; then role="both";
  elif [[ "${RINEX_HOURLY_ENABLE:-false}" == "true" ]]; then role="hourly";
  elif [[ "${RINEX_DAILY_ENABLE:-false}" == "true" ]]; then role="daily";
  else role="disabled"; fi
  printf '{"ts":"%s","name":"%s","role":"%s","last_hour":"%s","last_day":"%s"}\n' "$now" "$CONVERTER_NAME" "$role" "${last_hour_key:-}" "${last_day_key:-}" > "$STATUS_FILE.tmp" 2>/dev/null || true
  mv -f "$STATUS_FILE.tmp" "$STATUS_FILE" 2>/dev/null || true
}

run_station_jobs() {
  local mode="$1"; shift
  local max="${CONVERT_PARALLEL:-2}"
  if ! [[ "$max" =~ ^[0-9]+$ ]]; then max=2; fi
  (( max < 1 )) && max=1

  local -a pids=()
  local line mp rid x y z rec_type rec_ver ant_type ant_h ant_e ant_n

  for line in "$@"; do
    read -r mp rid x y z rec_type rec_ver ant_type ant_h ant_e ant_n <<<"$line"
    [[ -z "$mp" ]] && continue
    [[ -z "${rid:-}" ]] && rid="$mp"

    if [[ "$mode" == "hourly" ]]; then
      ( convert_hourly_for_station "$mp" "$rid" "$x" "$y" "$z" "$rec_type" "$rec_ver" "$ant_type" "$ant_h" "$ant_e" "$ant_n" || true ) &
    else
      ( convert_daily_for_station "$mp" "$rid" "$x" "$y" "$z" "$rec_type" "$rec_ver" "$ant_type" "$ant_h" "$ant_e" "$ant_n" || true ) &
    fi

    pids+=("$!")

    while (( ${#pids[@]} >= max )); do
      wait -n || true
      # prune finished
      local -a alive=()
      local pid
      for pid in "${pids[@]}"; do
        kill -0 "$pid" 2>/dev/null && alive+=("$pid")
      done
      pids=("${alive[@]}")
    done
  done

  local pid
  for pid in "${pids[@]}"; do
    wait "$pid" || true
  done
}

cleanup_old() {
  local rtcm_days="${RTCM_RETENTION_DAYS:-0}"
  local trace_days="${TRACE_RETENTION_DAYS:-0}"
  local events_days="${EVENTS_RETENTION_DAYS:-0}"
  local tmp_days="${TMP_RETENTION_DAYS:-0}"

  if [[ "$rtcm_days" =~ ^[0-9]+$ ]] && (( rtcm_days > 0 )); then
    local n
    n="$(find "$PUB_ROOT/rtcm_raw" -type f -name '*.rtcm' -mtime +"$rtcm_days" -print -delete 2>/dev/null | wc -l | tr -d ' ')" || n=0
    logc INFO "CLEAN rtcm_raw >${rtcm_days}d -> deleted=${n}"
    find "$PUB_ROOT/rtcm_raw" -type d -empty -delete 2>/dev/null || true
  fi

  if [[ "$trace_days" =~ ^[0-9]+$ ]] && (( trace_days > 0 )); then
    local n
    n="$(find "$PUB_ROOT/logs/traces" -type f -mtime +"$trace_days" -print -delete 2>/dev/null | wc -l | tr -d ' ')" || n=0
    logc INFO "CLEAN traces >${trace_days}d -> deleted=${n}"
  fi

  if [[ "$events_days" =~ ^[0-9]+$ ]] && (( events_days > 0 )); then
    local n
    n="$(find "$PUB_ROOT/logs/events" -type f -name '*.log' -mtime +"$events_days" -print -delete 2>/dev/null | wc -l | tr -d ' ')" || n=0
    logc INFO "CLEAN events >${events_days}d -> deleted=${n}"
  fi

  if [[ "$tmp_days" =~ ^[0-9]+$ ]] && (( tmp_days > 0 )); then
    local n
    n="$(find "$PUB_ROOT/tmp" -type f -mtime +"$tmp_days" -print -delete 2>/dev/null | wc -l | tr -d ' ')" || n=0
    logc INFO "CLEAN tmp >${tmp_days}d -> deleted=${n}"
  fi
}

load_cfg() {
  if [[ -f /config/.env ]]; then
    # shellcheck disable=SC1091
    set -a; source /config/.env; set +a
    # Per-container overrides (docker-compose `environment:`) — keep them stable even if /config/.env changes
    [[ -n "${FORCE_RINEX_HOURLY_ENABLE:-}" ]] && RINEX_HOURLY_ENABLE="$FORCE_RINEX_HOURLY_ENABLE"
    [[ -n "${FORCE_RINEX_DAILY_ENABLE:-}"  ]] && RINEX_DAILY_ENABLE="$FORCE_RINEX_DAILY_ENABLE"
    [[ -n "${FORCE_CLEANUP_ENABLE:-}"    ]] && CLEANUP_ENABLE="$FORCE_CLEANUP_ENABLE"
    [[ -n "${FORCE_TMP_ROOT:-}"          ]] && TMP_ROOT="$FORCE_TMP_ROOT"
  fi
}

STATIONS_FILE="${STATIONS_FILE:-/config/stations.list}"

read_stations() {
  if [[ -r "$STATIONS_FILE" ]]; then
    awk '
      /^[[:space:]]*#/ {next}
      /^[[:space:]]*$/ {next}
      {
        mp=$1; rid=$2;
        if (rid=="") rid=mp;
        # Optional extended fields (RENAG-like metadata)
        x=$3; y=$4; z=$5;
        rec_type=$6; rec_ver=$7;
        ant_type=$8;
        ant_h=$9; ant_e=$10; ant_n=$11;
        print mp, rid, x, y, z, rec_type, rec_ver, ant_type, ant_h, ant_e, ant_n
      }
    ' "$STATIONS_FILE"
    return
  fi

  # fallback STATIONS env
  local s="${STATIONS:-}"
  s="${s//,/ }"
  for mp in $s; do
    [[ -n "$mp" ]] && echo "$mp $mp"
  done
}

# Pick a writable temp directory (important on NFS/root_squash cases)
pick_tmp_root() {
  local rtcm_dir="${1:-}"
  local candidates=()

  [[ -n "${TMP_ROOT:-}" ]] && candidates+=("$TMP_ROOT")
  candidates+=("$PUB_ROOT/tmp")
  [[ -n "$rtcm_dir" ]] && candidates+=("$rtcm_dir/.tmp")
  candidates+=("/tmp")

  local d
  for d in "${candidates[@]}"; do
    mkdir -p "$d" 2>/dev/null || true
    if [[ -d "$d" && -w "$d" ]]; then
      echo "$d"
      return 0
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

atomic_write_move() {
  local src="${1:?}"
  local dst="${2:?}"
  local tmp="${dst}.tmp"
  rm -f "$tmp"
  mv -f "$src" "$tmp"
  mv -f "$tmp" "$dst"
}

patch_rinex_header() {
  local rnx="$1"
  local log_hint="${2:-}"

  local pgm="${RINEX_PGM:-CONVBIN EX 2.5.0}"
  local runby="${RINEX_RUNBY:-CentipedeLogger}"
  local dt="$(date -u +'%Y%m%d %H%M%S UTC')"

  local observer="${RINEX_OBSERVER:-}"
  local agency="${RINEX_AGENCY:-}"
  local marker_number="${RINEX_MARKER_NUMBER:-}"   # vide = ne pas écraser
  local add_comments="${RINEX_ADD_COMMENTS:-1}"

  awk -v pgm="$pgm" -v runby="$runby" -v dt="$dt" \
      -v observer="$observer" -v agency="$agency" \
      -v marker_number="$marker_number" \
      -v add_comments="$add_comments" \
      -v log_hint="$log_hint" '
    function fmt_pgm()    { return sprintf("%-20s%-20s%-20s%-20s", pgm, runby, dt, "PGM / RUN BY / DATE") }
    function fmt_obs()    { return sprintf("%-20s%-40s%-20s", observer, agency, "OBSERVER / AGENCY") }
    function fmt_markno() { return sprintf("%-20s%-40s%-20s", marker_number, "", "MARKER NUMBER") }

    BEGIN { injected=0 }

    $0 ~ /PGM \/ RUN BY \/ DATE$/ { print fmt_pgm(); next }

    $0 ~ /OBSERVER \/ AGENCY$/ {
      if (observer != "" || agency != "") print fmt_obs(); else print $0
      next
    }

    $0 ~ /MARKER NUMBER$/ {
      if (marker_number != "") print fmt_markno(); else print $0
      next
    }

    $0 ~ /END OF HEADER$/ {
      if (!injected && add_comments == 1) {
        print sprintf("%-60s%-20s","format: RTCM 3","COMMENT")
        if (log_hint != "") print sprintf("%-60s%-20s","log: " log_hint,"COMMENT")
        injected=1
      }
      print $0
      next
    }

    { print $0 }
  ' "$rnx" > "${rnx}.patched"

  mv -f "${rnx}.patched" "$rnx"
}

# Deduplicate RINEX epochs (keeps first occurrence, drops subsequent duplicate blocks).
# This is a safety net for upstream overlaps, e.g. RTKLIB str2str "swap margin" (-f)
# or any RTCM concatenation that repeats the same epoch.
dedup_rinex_epochs() {
  local f="$1"
  [[ "${RINEX_DEDUP_EPOCHS:-true}" == "true" ]] || return 0
  [[ -s "$f" ]] || return 0

  # Count duplicate epoch headers ('>' records). Key = epoch timestamp only.
  local dup
  dup="$(awk 'BEGIN{d=0}
            /^>/{k=$2" "$3" "$4" "$5" "$6" "$7; if(seen[k]++){d++}}
            END{print d}' "$f" 2>/dev/null || echo 0)"
  [[ "${dup:-0}" =~ ^[0-9]+$ ]] || dup=0

  if (( dup > 0 )); then
    logc "WARN duplicate epochs detected in $(basename "$f") (count=$dup) -> dedup"
    awk 'BEGIN{keep=1}
         /^>/{k=$2" "$3" "$4" "$5" "$6" "$7; if(seen[k]++){keep=0}else{keep=1}}
         {if(keep)print}
        ' "$f" > "${f}.dedup" && mv -f "${f}.dedup" "$f"
  fi
}

# Collect RTCM files around a time window using filename hour buckets
collect_rtcm_files_window() {
  local mp="$1"
  local center_epoch="$2"        # epoch within target hour/day
  local edge_hours="$3"          # include +/- edge_hours around window

  local -a epochs=()
  local i

  for ((i=edge_hours; i>=1; i--)); do epochs+=( $((center_epoch - i*3600)) ); done
  epochs+=( "$center_epoch" )
  for ((i=1; i<=edge_hours; i++)); do epochs+=( $((center_epoch + i*3600)) ); done

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

  # de-dup & sort
  if ((${#files[@]}==0)); then
    return 0
  fi
  IFS=$'\n' files=($(printf '%s\n' "${files[@]}" | sed '/^$/d' | sort -u)); unset IFS
  printf '%s\n' "${files[@]}"
}

# Convert DAILY (yesterday) at 00:20 UTC
convert_daily_for_station() {
  local mp="$1"
  local rinex_id="$2"
  local x="${3:-}" y="${4:-}" z="${5:-}"
  local rec_type_in="${6:-}" rec_ver_in="${7:-}"
  local ant_type_in="${8:-}"
  local ant_h_in="${9:-}" ant_e_in="${10:-}" ant_n_in="${11:-}"

  local day_epoch year doy day_slash
  day_epoch="$(date -u -d '1 day ago 12:00:00' +%s)"    # stable inside the day
  year="$(date -u -d "@$day_epoch" +%Y)"
  doy="$(date -u -d "@$day_epoch" +%j)"
  day_slash="$(date -u -d "@$day_epoch" +%Y/%m/%d)"

  local interval="${RINEX_DAILY_INTERVAL_S:-${RINEX_INTERVAL_S:-30}}"
  local rate; rate="$(rate_token "$interval")"

  local out_root="${RINEX_OUT_ROOT_DAILY:-$PUB_ROOT/centipede_30s}"
  local out_dir="$out_root/$year/$doy"
  mkdir -p "$out_dir"

  # RENAG-like: MARKER NAME = rinex_id ; MARKER NUMBER = mountpoint
  local marker_name marker_number
  marker_name="$(station_var "$mp" "MARKER_NAME")"; marker_name="${marker_name:-$rinex_id}"
  marker_number="$(station_var "$mp" "MARKER_NUMBER")"; marker_number="${marker_number:-$mp}"

  # Optional station metadata (from stations.list extended columns or env overrides)
  local rec_num rec_type rec_ver ant_num ant_type ant_h ant_e ant_n
  rec_num="$(station_var "$mp" "REC_NUM")"; rec_num="${rec_num:-${RINEX_REC_NUM_DEFAULT:-UNKNOWN}}"
  rec_type="$(station_var "$mp" "REC_TYPE")"; rec_type="${rec_type:-$rec_type_in}"
  rec_ver="$(station_var "$mp" "REC_VER")";   rec_ver="${rec_ver:-$rec_ver_in}"

  ant_num="$(station_var "$mp" "ANT_NUM")";   ant_num="${ant_num:-${RINEX_ANT_NUM_DEFAULT:-UNKNOWN}}"
  ant_type="$(station_var "$mp" "ANT_TYPE")"; ant_type="${ant_type:-$ant_type_in}"

  # Decode internal spaces encoded as | in stations.list tokens
  rec_type="$(decode_ws_token "${rec_type:-UNKNOWN}")"
  rec_ver="$(decode_ws_token "${rec_ver:-UNKNOWN}")"
  ant_type="$(decode_ws_token "${ant_type:-NONE|NONE}")"

  # Backward compat: ANT_HGT previously meant DELTA H (meters)
  local ant_hgt_legacy
  ant_hgt_legacy="$(station_var "$mp" "ANT_HGT")"

  ant_h="$(station_var "$mp" "ANT_H")"; ant_h="${ant_h:-$ant_h_in}"; ant_h="${ant_h:-$ant_hgt_legacy}"; ant_h="${ant_h:-${RINEX_ANT_HGT_DEFAULT:-0.0}}"
  ant_e="$(station_var "$mp" "ANT_E")"; ant_e="${ant_e:-$ant_e_in}"; ant_e="${ant_e:-0.0}"
  ant_n="$(station_var "$mp" "ANT_N")"; ant_n="${ant_n:-$ant_n_in}"; ant_n="${ant_n:-0.0}"

  local approx_xyz
  approx_xyz="$(station_var "$mp" "APPROX_XYZ")"
  if [[ -n "$approx_xyz" ]]; then
    IFS=/ read -r x y z <<<"$approx_xyz"
  fi

  local base out_crx_gz
  base="${rinex_id}_S_${year}${doy}0000_01D_${rate}_MO"
  out_crx_gz="$out_dir/${base}.crx.gz"

  [[ -s "$out_crx_gz" ]] && { logc "SKIP $mp daily -> exists $(basename "$out_crx_gz")"; return 0; }

  # pick some RTCM around the day (yesterday +/- edge hours)
  local edge="${RINEX_DAILY_EDGE_HOURS:-2}"   # include +/- 2h around midnight
  local center_epoch="$day_epoch"
  local -a files=()
  mapfile -t files < <(collect_rtcm_files_window "$mp" "$center_epoch" "$edge" || true)

  # Also include all RTCM of the target DOY dir (fast path when hourly-rotated)
  local rtcm_dir="$PUB_ROOT/rtcm_raw/$year/$doy/$mp"
  if [[ -d "$rtcm_dir" ]]; then
    files+=( "$rtcm_dir"/*.rtcm )
  fi

  IFS=$'\n' files=($(printf '%s\n' "${files[@]}" | sed '/^$/d' | sort -u)); unset IFS
  ((${#files[@]} > 0)) || return 0

  local tmp_root
  tmp_root="$(pick_tmp_root "$rtcm_dir")" || { logc "ERR  $mp daily -> no writable tmp dir (check permissions on $PUB_ROOT)"; return 1; }

  local tmp_rtcm tmp_rnx tmp_crx
  tmp_rtcm="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_DAILY_XXXX.rtcm")"
  tmp_rnx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_DAILY_XXXX.rnx")"
  tmp_crx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_DAILY_XXXX.crx")"
  trap 'rm -f "$tmp_rtcm" "$tmp_rnx" "$tmp_crx"' RETURN

  logc "CAT  $mp daily $year/$doy (${#files[@]} files) tmp=$tmp_root"
  cat "${files[@]}" > "$tmp_rtcm"

  local rinex_ver="${RINEX_VERSION:-3.04}"
  local freq="${CONVBIN_FREQ:-4}"
  local hp_args=() hr_args=() ha_args=() hd_args=()
  if [[ -n "${x:-}" && -n "${y:-}" && -n "${z:-}" ]]; then
    hp_args=(-hp "${x}/${y}/${z}")
  fi
  if [[ -n "${rec_type:-}" || -n "${rec_ver:-}" || -n "${rec_num:-}" ]]; then
    hr_args=(-hr "$(sanitize_field "$rec_num")/$(sanitize_field "${rec_type:-UNKNOWN}")/$(sanitize_field "${rec_ver:-UNKNOWN}")")
  fi
  if [[ -n "${ant_type:-}" || -n "${ant_num:-}" ]]; then
    ha_args=(-ha "$(sanitize_field "$ant_num")/$(sanitize_field "${ant_type:-NONE NONE}")")
  fi
  hd_args=(-hd "${ant_h}/${ant_e}/${ant_n}")

  logc "RUN  convbin $mp daily -> ${base}.crx.gz"
  if ! convbin "$tmp_rtcm" \
    -v "$rinex_ver" -r rtcm3 \
    -hm "$marker_name" -hn "$marker_number" \
    "${hp_args[@]}" "${hr_args[@]}" "${ha_args[@]}" \
    -f "$freq" \
    "${hd_args[@]}" \
    -os \
    -ti "$interval" -tt 0 \
    -ts "$day_slash" 00:00:00 -te "$day_slash" 23:59:59 \
    -o "$tmp_rnx"
  then
    logc "WARN $mp daily -> convbin failed — skip"
    return 0
  fi

  # reject empty/header-only
  if [[ ! -s "$tmp_rnx" ]] || ! grep -q "END OF HEADER" "$tmp_rnx" || ! grep -qE '^>' "$tmp_rnx"; then
    logc "WARN $mp daily -> empty/no epochs — skip"
    return 0
  fi

  patch_rinex_header "$tmp_rnx" "$rtcm_dir"
  dedup_rinex_epochs "$tmp_rnx"

  if [[ "${RINEX_HATANAKA:-true}" == "true" ]]; then
    rnx2crx < "$tmp_rnx" > "$tmp_crx"
    [[ -s "$tmp_crx" ]] || { logc "WARN $mp daily -> rnx2crx empty — skip"; return 0; }
  else
    mv -f "$tmp_rnx" "$tmp_crx"
  fi

  if [[ "${RINEX_GZIP:-true}" == "true" ]]; then
    atomic_write_gzip "$tmp_crx" "$out_crx_gz"
  else
    atomic_write_move "$tmp_crx" "$out_dir/${base}.crx"
  fi

  [[ -s "$out_crx_gz" || "${RINEX_GZIP:-true}" != "true" ]] || { logc "ERR  $mp daily -> output empty"; return 1; }
  logc "OK   $mp daily -> $(basename "$out_crx_gz")"

  trap - RETURN
  rm -f "$tmp_rtcm" "$tmp_rnx" "$tmp_crx" 2>/dev/null || true
}

# Convert HOURLY (previous hour) at HH:03 UTC
convert_hourly_for_station() {
  local mp="$1"
  local rinex_id="$2"
  local x="${3:-}" y="${4:-}" z="${5:-}"
  local rec_type_in="${6:-}" rec_ver_in="${7:-}"
  local ant_type_in="${8:-}"
  local ant_h_in="${9:-}" ant_e_in="${10:-}" ant_n_in="${11:-}"

  local target_epoch year doy ymd_slash ymd_hyphen hh
  target_epoch="$(date -u -d '1 hour ago' +%s)"           # within target hour
  year="$(date -u -d "@$target_epoch" +%Y)"
  doy="$(date -u -d "@$target_epoch" +%j)"
  ymd_slash="$(date -u -d "@$target_epoch" +%Y/%m/%d)"
  ymd_hyphen="$(date -u -d "@$target_epoch" +%Y-%m-%d)"
  hh="$(date -u -d "@$target_epoch" +%H)"

  local interval="${RINEX_HOURLY_INTERVAL_S:-1}"
  local rate; rate="$(rate_token "$interval")"

  local out_root="${RINEX_OUT_ROOT_HOURLY:-$PUB_ROOT/centipede_1s}"
  local out_dir="$out_root/$year/$doy"
  mkdir -p "$out_dir"

  local marker_name marker_number
  marker_name="$(station_var "$mp" "MARKER_NAME")"; marker_name="${marker_name:-$rinex_id}"
  marker_number="$(station_var "$mp" "MARKER_NUMBER")"; marker_number="${marker_number:-$mp}"

  local rec_num rec_type rec_ver ant_num ant_type ant_h ant_e ant_n
  rec_num="$(station_var "$mp" "REC_NUM")"; rec_num="${rec_num:-${RINEX_REC_NUM_DEFAULT:-UNKNOWN}}"
  rec_type="$(station_var "$mp" "REC_TYPE")"; rec_type="${rec_type:-$rec_type_in}"
  rec_ver="$(station_var "$mp" "REC_VER")";   rec_ver="${rec_ver:-$rec_ver_in}"

  ant_num="$(station_var "$mp" "ANT_NUM")";   ant_num="${ant_num:-${RINEX_ANT_NUM_DEFAULT:-UNKNOWN}}"
  ant_type="$(station_var "$mp" "ANT_TYPE")"; ant_type="${ant_type:-$ant_type_in}"

  # Decode internal spaces encoded as | in stations.list tokens
  rec_type="$(decode_ws_token "${rec_type:-UNKNOWN}")"
  rec_ver="$(decode_ws_token "${rec_ver:-UNKNOWN}")"
  ant_type="$(decode_ws_token "${ant_type:-NONE|NONE}")"

  local ant_hgt_legacy
  ant_hgt_legacy="$(station_var "$mp" "ANT_HGT")"
  ant_h="$(station_var "$mp" "ANT_H")"; ant_h="${ant_h:-$ant_h_in}"; ant_h="${ant_h:-$ant_hgt_legacy}"; ant_h="${ant_h:-${RINEX_ANT_HGT_DEFAULT:-0.0}}"
  ant_e="$(station_var "$mp" "ANT_E")"; ant_e="${ant_e:-$ant_e_in}"; ant_e="${ant_e:-0.0}"
  ant_n="$(station_var "$mp" "ANT_N")"; ant_n="${ant_n:-$ant_n_in}"; ant_n="${ant_n:-0.0}"

  local approx_xyz
  approx_xyz="$(station_var "$mp" "APPROX_XYZ")"
  if [[ -n "$approx_xyz" ]]; then
    IFS=/ read -r x y z <<<"$approx_xyz"
  fi

  local base out_crx_gz
  base="${rinex_id}_S_${year}${doy}${hh}00_01H_${rate}_MO"
  out_crx_gz="$out_dir/${base}.crx.gz"

  [[ -s "$out_crx_gz" ]] && { logc "SKIP $mp hour=$hh -> exists $(basename "$out_crx_gz")"; return 0; }

  local edge="${RINEX_HOURLY_EDGE_HOURS:-1}"
  local -a files=()
  mapfile -t files < <(collect_rtcm_files_window "$mp" "$target_epoch" "$edge" || true)
  ((${#files[@]} > 0)) || return 0

  local rtcm_dir="$PUB_ROOT/rtcm_raw/$year/$doy/$mp"

  local tmp_root
  tmp_root="$(pick_tmp_root "$rtcm_dir")" || { logc "ERR  $mp hour=$hh -> no writable tmp dir (check permissions on $PUB_ROOT)"; return 1; }

  local tmp_rtcm tmp_rnx tmp_crx
  tmp_rtcm="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${hh}_XXXX.rtcm")"
  tmp_rnx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${hh}_XXXX.rnx")"
  tmp_crx="$(mktemp -p "$tmp_root" "${mp}_${year}${doy}_${hh}_XXXX.crx")"
  trap 'rm -f "$tmp_rtcm" "$tmp_rnx" "$tmp_crx"' RETURN

  logc "CAT  $mp hour=$hh $year/$doy (${#files[@]} files) tmp=$tmp_root"
  cat "${files[@]}" > "$tmp_rtcm"

  local rinex_ver="${RINEX_VERSION:-3.04}"
  local freq="${CONVBIN_FREQ:-4}"
  local hp_args=() hr_args=() ha_args=() hd_args=()
  if [[ -n "${x:-}" && -n "${y:-}" && -n "${z:-}" ]]; then
    hp_args=(-hp "${x}/${y}/${z}")
  fi
  if [[ -n "${rec_type:-}" || -n "${rec_ver:-}" || -n "${rec_num:-}" ]]; then
    hr_args=(-hr "$(sanitize_field "$rec_num")/$(sanitize_field "${rec_type:-UNKNOWN}")/$(sanitize_field "${rec_ver:-UNKNOWN}")")
  fi
  if [[ -n "${ant_type:-}" || -n "${ant_num:-}" ]]; then
    ha_args=(-ha "$(sanitize_field "$ant_num")/$(sanitize_field "${ant_type:-NONE NONE}")")
  fi
  hd_args=(-hd "${ant_h}/${ant_e}/${ant_n}")

  logc "RUN  convbin $mp hour=$hh -> ${base}.crx.gz"
  if ! convbin "$tmp_rtcm" \
    -v "$rinex_ver" -r rtcm3 \
    -hm "$marker_name" -hn "$marker_number" \
    "${hp_args[@]}" "${hr_args[@]}" "${ha_args[@]}" \
    -f "$freq" \
    "${hd_args[@]}" \
    -os \
    -ti "$interval" -tt 0 \
    -ts "$ymd_slash" "${hh}:00:00" -te "$ymd_slash" "${hh}:59:59" \
    -o "$tmp_rnx"
  then
    logc "WARN $mp hour=$hh -> convbin failed — skip"
    return 0
  fi

  if [[ ! -s "$tmp_rnx" ]] || ! grep -q "END OF HEADER" "$tmp_rnx" || ! grep -qE '^>' "$tmp_rnx"; then
    logc "WARN $mp hour=$hh -> empty/no epochs — skip"
    return 0
  fi

  patch_rinex_header "$tmp_rnx" "$rtcm_dir"
  dedup_rinex_epochs "$tmp_rnx"

  if [[ "${RINEX_HATANAKA:-true}" == "true" ]]; then
    rnx2crx < "$tmp_rnx" > "$tmp_crx"
    [[ -s "$tmp_crx" ]] || { logc "WARN $mp hour=$hh -> rnx2crx empty — skip"; return 0; }
  else
    mv -f "$tmp_rnx" "$tmp_crx"
  fi

  if [[ "${RINEX_GZIP:-true}" == "true" ]]; then
    atomic_write_gzip "$tmp_crx" "$out_crx_gz"
  else
    atomic_write_move "$tmp_crx" "$out_dir/${base}.crx"
  fi

  [[ -s "$out_crx_gz" || "${RINEX_GZIP:-true}" != "true" ]] || { logc "ERR  $mp hour=$hh -> output empty"; return 1; }
  logc "OK   $mp hour=$hh -> $(basename "$out_crx_gz")"

  trap - RETURN
  rm -f "$tmp_rtcm" "$tmp_rnx" "$tmp_crx" 2>/dev/null || true
}

logc "started (schedule: hourly @ HH:${RINEX_HOURLY_AT_MINUTE:-03} UTC; daily @ ${RINEX_DAILY_AT:-00:20} UTC)"

last_hour_key=""
last_day_key=""

last_cleanup_key=""

while true; do
  load_cfg
  mapfile -t stations < <(read_stations)

  # ---- HOURLY trigger ----
  if [[ "${RINEX_HOURLY_ENABLE:-false}" == "true" ]]; then
    at_min="${RINEX_HOURLY_AT_MINUTE:-3}"
    now_min="$(date -u +%M)"; now_min=$((10#$now_min))
    # Robust trigger: if we missed the exact minute (load/restart), run once as soon as now_min >= at_min.
    if (( now_min >= at_min )); then
      hour_key="$(date -u -d '1 hour ago' +%Y%m%d%H)"
      if [[ "$hour_key" != "$last_hour_key" ]]; then
        logc "tick hourly: target_hour=$hour_key"
        run_station_jobs hourly "${stations[@]}"
        last_hour_key="$hour_key"
      fi
    fi
  fi

  # ---- DAILY trigger ----
  if [[ "${RINEX_DAILY_ENABLE:-false}" == "true" ]]; then
    daily_at="${RINEX_DAILY_AT:-00:20}"
    now_hm="$(date -u +%H:%M)"
    # Robust trigger: if we missed the exact minute (load/restart), run once as soon as now_hm >= daily_at.
    if [[ "$now_hm" == "$daily_at" || "$now_hm" > "$daily_at" ]]; then
      day_key="$(date -u -d '1 day ago' +%Y%j)"
      if [[ "$day_key" != "$last_day_key" ]]; then
        logc "tick daily: target_day=$day_key"
        run_station_jobs daily "${stations[@]}"
        last_day_key="$day_key"
      fi
    fi
  fi

  # heartbeat for healthcheck
  heartbeat

  # optional cleanup (runs once per UTC day at CLEANUP_AT)
  if [[ "${CLEANUP_ENABLE:-true}" == "true" ]]; then
    cleanup_at="${CLEANUP_AT:-01:10}"
    now_hm="$(date -u +%H:%M)"
    if [[ "$now_hm" == "$cleanup_at" ]]; then
      cleanup_key="$(date -u +%Y%m%d)"
      if [[ "$cleanup_key" != "$last_cleanup_key" ]]; then
        cleanup_old || true
        last_cleanup_key="$cleanup_key"
      fi
    fi
  fi

  sleep 60
done

