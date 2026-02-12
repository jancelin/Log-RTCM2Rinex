#!/usr/bin/env bash
set -euo pipefail

utc_now_hm() { date -u +"%H:%M"; }
utc_epoch() { date -u +%s; }

day_year() { date -u -d "${1} day" +"%Y"; }
day_doy()  { date -u -d "${1} day" +"%j"; }
day_ymd()  { date -u -d "${1} day" +"%Y-%m-%d"; }
day_ymd_slash() { date -u -d "${1} day" +"%Y/%m/%d"; }

in_window() {
  local now start end
  now="$(utc_now_hm)"
  start="${CONVERT_WINDOW_START:-00:00}"
  end="${CONVERT_WINDOW_END:-03:00}"
  [[ "$now" >="$start" && "$now" <"$end" ]]
}

on_tick() {
  local n="${CONVERT_EVERY_MIN:-30}"
  local mm
  mm="$(date -u +%M)"
  mm=$((10#$mm))
  (( mm % n == 0 ))
}

rate_token() {
  local s="$1"
  if (( s % 3600 == 0 )); then
    printf "%02dH" $((s/3600))
  elif (( s % 60 == 0 )); then
    printf "%02dM" $((s/60))
  else
    printf "%02dS" "$s"
  fi
}

# sanitize mountpoint to env var key
mp_key() {
  echo "$1" | tr '[:lower:]' '[:upper:]' | sed -E 's/[^A-Z0-9]+/_/g'
}

# read per-station override: STATION_<MP>_<FIELD>
station_var() {
  local mp="$1" field="$2"
  local key; key="$(mp_key "$mp")"
  local var="STATION_${key}_${field}"
  # indirect expansion
  echo "${!var:-}"
}
