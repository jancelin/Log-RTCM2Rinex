#!/usr/bin/env bash
set -euo pipefail

# Load dynamic config if present (so mounted .env can be used by scripts too)
if [[ -f /config/.env ]]; then
  # shellcheck disable=SC1091
  set -a; source /config/.env; set +a
fi

: "${TZ:=UTC}"
export TZ

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

# If PGID already exists, DO NOT groupadd (common: GID 1000 = users)
if ! getent group "$PGID" >/dev/null 2>&1; then
  groupadd -g "$PGID" app
fi

# If PUID already exists, DO NOT useradd
if ! getent passwd "$PUID" >/dev/null 2>&1; then
  useradd -u "$PUID" -g "$PGID" -m -s /bin/bash app
fi

PUB_ROOT="${PUB_ROOT:-/data/pub}"

mkdir -p \
  "$PUB_ROOT"/centipede_30s \
  "$PUB_ROOT"/centipede_1s \
  "$PUB_ROOT"/rtcm_raw \
  "$PUB_ROOT"/logs/events \
  "$PUB_ROOT"/logs/traces \
  "$PUB_ROOT"/tmp \
  /config

# Best-effort ownership on bind mounts
chown -R "$PUID:$PGID" /data /config 2>/dev/null || true

# Run with numeric IDs (no dependency on user/group names)
exec gosu "$PUID:$PGID" "$@"

