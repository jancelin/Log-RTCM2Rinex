#!/usr/bin/env bash
set -euo pipefail

# Load config (if mounted)
if [[ -f /config/.env ]]; then
  # shellcheck disable=SC1091
  set -a; source /config/.env; set +a
fi

PUB_ROOT="${PUB_ROOT:-/data/pub}"
EVENTS_DIR="$PUB_ROOT/logs/events"
HB_FILE="$EVENTS_DIR/logger-manager.heartbeat"
MAX_AGE_SEC="${LOGGER_HEALTH_MAX_AGE_SEC:-120}"

# manager heartbeat
if [[ ! -f "$HB_FILE" ]]; then
  echo "missing heartbeat: $HB_FILE"
  exit 1
fi

now="$(date -u +%s)"
mtime="$(stat -c %Y "$HB_FILE" 2>/dev/null || echo 0)"
if (( now - mtime > MAX_AGE_SEC )); then
  echo "stale heartbeat: age=$((now-mtime))s > ${MAX_AGE_SEC}s"
  exit 1
fi

# stations list -> ensure each has a running station-logger
STATIONS_FILE="/config/stations.list"
mountpoints=()
if [[ -r "$STATIONS_FILE" ]]; then
  while read -r mp _; do
    [[ -z "$mp" ]] && continue
    [[ "$mp" =~ ^# ]] && continue
    mountpoints+=("$mp")
  done < <(awk 'NF && $1!~ /^#/ {print $1" "$2}' "$STATIONS_FILE")
else
  s="${STATIONS:-}"
  s="${s//,/ }"
  for mp in $s; do
    [[ -n "$mp" ]] && mountpoints+=("$mp")
  done
fi

# If no stations configured, container is still healthy
if ((${#mountpoints[@]}==0)); then
  exit 0
fi

missing=0
for mp in "${mountpoints[@]}"; do
  if ! pgrep -f "/opt/scripts/station-logger.sh[[:space:]]+$mp" >/dev/null 2>&1; then
    echo "missing station-logger for $mp"
    missing=1
  fi
done

(( missing == 0 ))
