#!/usr/bin/env bash
set -euo pipefail

if [[ -f /config/.env ]]; then
  # shellcheck disable=SC1091
  set -a; source /config/.env; set +a
fi

PUB_ROOT="${PUB_ROOT:-/data/pub}"
EVENTS_DIR="$PUB_ROOT/logs/events"
HB_FILE="$EVENTS_DIR/converter.heartbeat"
MAX_AGE_SEC="${CONVERTER_HEALTH_MAX_AGE_SEC:-180}"

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

# process exists (best effort)
pgrep -f "/opt/scripts/rinex-converter.sh" >/dev/null 2>&1 || {
  echo "rinex-converter.sh not running"
  exit 1
}

exit 0
