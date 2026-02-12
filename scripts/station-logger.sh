#!/usr/bin/env bash
set -euo pipefail

MP="${1:?usage: station-logger.sh <MOUNTPOINT>}"

# shellcheck disable=SC1091
source /opt/scripts/lib.sh

# Load latest config each loop (mounted .env)
load_cfg() {
  if [[ -f /config/.env ]]; then
    # shellcheck disable=SC1091
    set -a; source /config/.env; set +a
  fi
}

evt() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $MP $*" | tee -a "${EVENTS_FILE:-/dev/stderr}"
}

child_pid=""

cleanup() {
  if [[ -n "${child_pid:-}" ]] && kill -0 "$child_pid" 2>/dev/null; then
    evt "cleanup: stopping str2str (pid=$child_pid)"
    kill -INT "$child_pid" 2>/dev/null || true
    sleep 1
    kill -TERM "$child_pid" 2>/dev/null || true
    sleep 1
    kill -KILL "$child_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

while true; do
  load_cfg

  PUB_ROOT="${PUB_ROOT:-/data/pub}"
  RTCM_ROOT="$PUB_ROOT/rtcm_raw"
  TRACE_ROOT="$PUB_ROOT/logs/traces"
  EVENTS_DIR="$PUB_ROOT/logs/events"
  mkdir -p "$EVENTS_DIR"
  EVENTS_FILE="$EVENTS_DIR/${MP}.events.log"

  # per-station overrides
  NTRIP_HOST_E="$(station_var "$MP" "NTRIP_HOST")"; NTRIP_HOST="${NTRIP_HOST_E:-${NTRIP_HOST:-crtk.net}}"
  NTRIP_PORT_E="$(station_var "$MP" "NTRIP_PORT")"; NTRIP_PORT="${NTRIP_PORT_E:-${NTRIP_PORT:-2101}}"
  NTRIP_USER_E="$(station_var "$MP" "NTRIP_USER")"; NTRIP_USER="${NTRIP_USER_E:-${NTRIP_USER:-centipede}}"
  NTRIP_PASS_E="$(station_var "$MP" "NTRIP_PASS")"; NTRIP_PASS="${NTRIP_PASS_E:-${NTRIP_PASS:-centipede}}"

  STR2STR_TIMEOUT_MS="${STR2STR_TIMEOUT_MS:-10000}"
  STR2STR_RECONNECT_MS="${STR2STR_RECONNECT_MS:-10000}"
  STR2STR_TRACE_LEVEL="${STR2STR_TRACE_LEVEL:-2}"
  RTCM_ROTATE_HOURS="${RTCM_ROTATE_HOURS:-1}"
  RTCM_SWAP_MARGIN_S="${RTCM_SWAP_MARGIN_S:-10}"
  RTCM_SUFFIX="${RTCM_SUFFIX:-GNSS-1}"

  STALE_CHECK_EVERY_SEC="${STALE_CHECK_EVERY_SEC:-60}"
  STALE_AFTER_SEC="${STALE_AFTER_SEC:-300}"

  YEAR="$(date -u +%Y)"
  DOY="$(date -u +%j)"
  DAYDIR="$RTCM_ROOT/$YEAR/$DOY/$MP"
  mkdir -p "$DAYDIR"

  # separate working dir per station so RTKLIB writes str2str.trace there
  WORKDIR="$TRACE_ROOT/$MP"
  mkdir -p "$WORKDIR"
  cd "$WORKDIR"

  OUT_PATH="file://${DAYDIR}/${MP}_%Y-%m-%d_%h-%M-%S_${RTCM_SUFFIX}.rtcm::T::S=${RTCM_ROTATE_HOURS}"

  next_midnight_epoch="$(date -u -d 'tomorrow 00:00:00' +%s)"

  evt "RUN str2str -> $OUT_PATH"
  str2str \
    -in "ntrip://${NTRIP_USER}:${NTRIP_PASS}@${NTRIP_HOST}:${NTRIP_PORT}/${MP}" \
    -out "$OUT_PATH" \
    -s "$STR2STR_TIMEOUT_MS" \
    -r "$STR2STR_RECONNECT_MS" \
    -f "$RTCM_SWAP_MARGIN_S" \
    -t "$STR2STR_TRACE_LEVEL" &
  child_pid=$!
  pid=$child_pid

  last_warn=0

  while kill -0 "$pid" 2>/dev/null; do
    now="$(date -u +%s)"

    # restart cleanly at 00:00 UTC to switch YYYY/DOY directories
    if (( now >= next_midnight_epoch )); then
      evt "MIDNIGHT UTC -> restart str2str"
      kill -INT "$pid" 2>/dev/null || true
      wait "$pid" || true
      break
    fi

    # watchdog: detect station outage by missing/old files
    latest="$(ls -1t "$DAYDIR"/*.rtcm 2>/dev/null | head -n 1 || true)"
    if [[ -n "$latest" ]]; then
      mtime="$(stat -c %Y "$latest" 2>/dev/null || echo 0)"
      age=$(( now - mtime ))
      if (( age > STALE_AFTER_SEC )) && (( now - last_warn > STALE_AFTER_SEC )); then
        evt "STALE no new RTCM for ${age}s (base down?) latest=$(basename "$latest")"
        last_warn="$now"
      fi
    else
      if (( now - last_warn > STALE_AFTER_SEC )); then
        evt "STALE no RTCM files yet (base down?)"
        last_warn="$now"
      fi
    fi

    sleep "$STALE_CHECK_EVERY_SEC"
  done

  wait "$pid" || true
  evt "str2str exited -> restart in 5s"
  sleep 5
done
