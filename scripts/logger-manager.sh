#!/usr/bin/env bash
set -euo pipefail

# shellcheck disable=SC1091
source /opt/scripts/lib.sh

STATIONS_FILE="/config/stations.list"
POLL_SEC="${POLL_SEC:-5}"

load_cfg() {
  if [[ -f /config/.env ]]; then
    # shellcheck disable=SC1091
    set -a; source /config/.env; set +a
  fi
}

init_paths() {
  PUB_ROOT="${PUB_ROOT:-/data/pub}"
  EVENTS_DIR="$PUB_ROOT/logs/events"
  mkdir -p "$EVENTS_DIR"
  HB_FILE="$EVENTS_DIR/logger-manager.heartbeat"
}

logm() {
  local lvl="${1:-INFO}"; shift || true
  local threshold="${LOG_LEVEL:-INFO}"
  log_should "$lvl" "$threshold" || return 0
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] logger-manager $lvl $*" | tee -a "$EVENTS_DIR/logger-manager.log"
}

declare -A PIDS=()

start_station() {
  local mp="$1"
  if [[ -n "${PIDS[$mp]:-}" ]] && kill -0 "${PIDS[$mp]}" 2>/dev/null; then
    return
  fi
  logm INFO "START $mp"

  # Extremely verbose RTKLIB console output (connect status every ~5s) must NOT be stored by default.
  # We only capture it in DEBUG mode, or if explicitly forced.
  local capture="${STATION_CAPTURE_LOG:-}"
  if [[ -z "$capture" || "$capture" == "auto" ]]; then
    if (( $(log_level_num "${LOG_LEVEL:-INFO}") >= 3 )); then
      capture=true
    else
      capture=false
    fi
  fi

  if bool_is_true "$capture"; then
    /opt/scripts/station-logger.sh "$mp" >>"$EVENTS_DIR/${mp}.log" 2>&1 &
  else
    /opt/scripts/station-logger.sh "$mp" >/dev/null 2>&1 &
  fi
  PIDS[$mp]=$!
}

stop_station() {
  local mp="$1"
  local pid="${PIDS[$mp]:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    logm INFO "STOP  $mp (pid=$pid)"
    kill -TERM "$pid" 2>/dev/null || true
    sleep 2
    kill -KILL "$pid" 2>/dev/null || true
  fi
  unset "PIDS[$mp]" || true
}

parse_mountpoints() {
  if [[ -r "$STATIONS_FILE" ]]; then
    awk '
      /^[[:space:]]*#/ {next}
      /^[[:space:]]*$/ {next}
      {print $1}
    ' "$STATIONS_FILE"
    return
  fi

  # Fallback: STATIONS in /config/.env (si stations.list absent)
  if [[ -f /config/.env ]]; then
    # shellcheck disable=SC1091
    set -a; source /config/.env; set +a
  fi
  local s="${STATIONS:-}"
  s="${s//,/ }"
  for mp in $s; do
    [[ -n "$mp" ]] && echo "$mp"
  done
}

reconcile() {
  mapfile -t desired < <(parse_mountpoints | sort -u)

  declare -A WANT=()
  local mp
  for mp in "${desired[@]}"; do
    [[ -n "$mp" ]] && WANT["$mp"]=1
  done

  # stop removed
  for mp in "${!PIDS[@]}"; do
    if [[ -z "${WANT[$mp]:-}" ]]; then
      stop_station "$mp"
    fi
  done

  # start new
  for mp in "${!WANT[@]}"; do
    if [[ -z "${PIDS[$mp]:-}" ]]; then
      start_station "$mp"
    fi
  done
}

sigterm() {
  logm WARN "SIGTERM: stopping all stations"
  for mp in "${!PIDS[@]}"; do stop_station "$mp"; done
  exit 0
}
trap sigterm TERM INT

load_cfg
init_paths

logm INFO "boot"
reconcile

# Polling robuste (marche même si inotify ne voit pas les bind mounts)
prev_sig=""
while true; do
  load_cfg
  init_paths

  # heartbeat (atomic write, safe on NFS)
  printf "%s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$HB_FILE.tmp" 2>/dev/null || true
  mv -f "$HB_FILE.tmp" "$HB_FILE" 2>/dev/null || true
  sig=""
  if [[ -r "$STATIONS_FILE" ]]; then
    sig="$(cksum "$STATIONS_FILE" | awk '{print $1 ":" $2}')"
  else
    # fallback on .env signature
    if [[ -r /config/.env ]]; then
      sig="$(cksum /config/.env | awk '{print $1 ":" $2}')"
    fi
  fi

  if [[ "$sig" != "$prev_sig" ]]; then
    logm INFO "config changed -> reconcile"
    reconcile
    prev_sig="$sig"
  fi

  sleep "$POLL_SEC"
done

