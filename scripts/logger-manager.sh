#!/usr/bin/env bash
set -euo pipefail

PUB_ROOT="${PUB_ROOT:-/data/pub}"
EVENTS_DIR="$PUB_ROOT/logs/events"
mkdir -p "$EVENTS_DIR"
HB_FILE="$EVENTS_DIR/logger-manager.heartbeat"

STATIONS_FILE="/config/stations.list"
POLL_SEC="${POLL_SEC:-5}"

logm() { echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] logger-manager $*" | tee -a "$EVENTS_DIR/logger-manager.log"; }

declare -A PIDS=()

start_station() {
  local mp="$1"
  if [[ -n "${PIDS[$mp]:-}" ]] && kill -0 "${PIDS[$mp]}" 2>/dev/null; then
    return
  fi
  logm "START $mp"
  /opt/scripts/station-logger.sh "$mp" >>"$EVENTS_DIR/${mp}.log" 2>&1 &
  PIDS[$mp]=$!
}

stop_station() {
  local mp="$1"
  local pid="${PIDS[$mp]:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    logm "STOP  $mp (pid=$pid)"
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
  logm "SIGTERM: stopping all stations"
  for mp in "${!PIDS[@]}"; do stop_station "$mp"; done
  exit 0
}
trap sigterm TERM INT

logm "boot"
reconcile

# Polling robuste (marche même si inotify ne voit pas les bind mounts)
prev_sig=""
while true; do
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
    logm "config changed -> reconcile"
    reconcile
    prev_sig="$sig"
  fi

  sleep "$POLL_SEC"
done

