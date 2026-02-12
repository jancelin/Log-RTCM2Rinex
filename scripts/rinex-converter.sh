#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

# shellcheck disable=SC1091
source /opt/scripts/lib.sh

PUB_ROOT="${PUB_ROOT:-/data/pub}"
EVENTS_DIR="$PUB_ROOT/logs/events"
mkdir -p "$EVENTS_DIR"

logc() { echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] converter $*" | tee -a "$EVENTS_DIR/converter.log"; }

load_cfg() {
  if [[ -f /config/.env ]]; then
    # shellcheck disable=SC1091
    set -a; source /config/.env; set +a
  fi
}

STATIONS_FILE="/config/stations.list"

read_stations() {
  if [[ -r "$STATIONS_FILE" ]]; then
    awk '
      /^[[:space:]]*#/ {next}
      /^[[:space:]]*$/ {next}
      { mp=$1; rid=$2; if (rid=="") rid=mp; print mp, rid }
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
  local marker_name marker_number ant_hgt
  marker_name="$(station_var "$mp" "MARKER_NAME")"; marker_name="${marker_name:-$rinex_id}"
  marker_number="$(station_var "$mp" "MARKER_NUMBER")"; marker_number="${marker_number:-$mp}"
  ant_hgt="$(station_var "$mp" "ANT_HGT")"; ant_hgt="${ant_hgt:-${RINEX_ANT_HGT_DEFAULT:-2.7}}"

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
  local hd_args=()
  [[ -n "${ant_hgt:-}" ]] && hd_args=(-hd "${ant_hgt}/0/0")

  logc "RUN  convbin $mp daily -> ${base}.crx.gz"
  if ! convbin "$tmp_rtcm" \
    -v "$rinex_ver" -r rtcm3 \
    -hm "$marker_name" -hn "$marker_number" \
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

  local marker_name marker_number ant_hgt
  marker_name="$(station_var "$mp" "MARKER_NAME")"; marker_name="${marker_name:-$rinex_id}"
  marker_number="$(station_var "$mp" "MARKER_NUMBER")"; marker_number="${marker_number:-$mp}"
  ant_hgt="$(station_var "$mp" "ANT_HGT")"; ant_hgt="${ant_hgt:-${RINEX_ANT_HGT_DEFAULT:-2.7}}"

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
  local hd_args=()
  [[ -n "${ant_hgt:-}" ]] && hd_args=(-hd "${ant_hgt}/0/0")

  logc "RUN  convbin $mp hour=$hh -> ${base}.crx.gz"
  if ! convbin "$tmp_rtcm" \
    -v "$rinex_ver" -r rtcm3 \
    -hm "$marker_name" -hn "$marker_number" \
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

while true; do
  load_cfg
  mapfile -t stations < <(read_stations)

  # ---- HOURLY trigger ----
  if [[ "${RINEX_HOURLY_ENABLE:-false}" == "true" ]]; then
    at_min="${RINEX_HOURLY_AT_MINUTE:-3}"
    now_min="$(date -u +%M)"; now_min=$((10#$now_min))
    if (( now_min == at_min )); then
      hour_key="$(date -u -d '1 hour ago' +%Y%m%d%H)"
      if [[ "$hour_key" != "$last_hour_key" ]]; then
        logc "tick hourly: target_hour=$hour_key"
        for line in "${stations[@]}"; do
          mp="$(awk '{print $1}' <<<"$line")"
          rid="$(awk '{print $2}' <<<"$line")"
          convert_hourly_for_station "$mp" "$rid" || true
        done
        last_hour_key="$hour_key"
      fi
    fi
  fi

  # ---- DAILY trigger ----
  if [[ "${RINEX_DAILY_ENABLE:-false}" == "true" ]]; then
    daily_at="${RINEX_DAILY_AT:-00:20}"
    now_hm="$(date -u +%H:%M)"
    if [[ "$now_hm" == "$daily_at" ]]; then
      day_key="$(date -u -d '1 day ago' +%Y%j)"
      if [[ "$day_key" != "$last_day_key" ]]; then
        logc "tick daily: target_day=$day_key"
        for line in "${stations[@]}"; do
          mp="$(awk '{print $1}' <<<"$line")"
          rid="$(awk '{print $2}' <<<"$line")"
          convert_daily_for_station "$mp" "$rid" || true
        done
        last_day_key="$day_key"
      fi
    fi
  fi

  sleep 60
done

