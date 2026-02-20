#!/usr/bin/env bash
set -euo pipefail

# Thin wrapper for the daily (30s) RINEX converter.
# No code duplication: the whole logic lives in rinex-converter.sh.

export CONVERTER_NAME="${CONVERTER_NAME:-converter30s}"

# Per-role overrides (do NOT depend on /config/.env values)
export FORCE_RINEX_HOURLY_ENABLE="${FORCE_RINEX_HOURLY_ENABLE:-false}"
export FORCE_RINEX_DAILY_ENABLE="${FORCE_RINEX_DAILY_ENABLE:-true}"
export FORCE_CLEANUP_ENABLE="${FORCE_CLEANUP_ENABLE:-true}"

# Tmp sur /dev/shm (tmpfs RAM) pour éliminer la contention I/O disque/NFS.
# Taille shm_size allouée dans docker-compose.yml (4g pour converter30s).
# Fallback automatique vers /data/pub/tmp si /dev/shm est indisponible ou saturé
# (géré dans pick_tmp_root() de rinex-converter.sh).
export FORCE_TMP_ROOT="${FORCE_TMP_ROOT:-/dev/shm/${CONVERTER_NAME}}"

exec /opt/scripts/rinex-converter.sh
