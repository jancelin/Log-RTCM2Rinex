#!/usr/bin/env bash
set -euo pipefail

# Thin wrapper for the daily (30s) RINEX converter.
# No code duplication: the whole logic lives in rinex-converter.sh.

export CONVERTER_NAME="${CONVERTER_NAME:-converter30s}"

# Per-role overrides (do NOT depend on /config/.env values)
export FORCE_RINEX_HOURLY_ENABLE="${FORCE_RINEX_HOURLY_ENABLE:-false}"
export FORCE_RINEX_DAILY_ENABLE="${FORCE_RINEX_DAILY_ENABLE:-true}"
export FORCE_CLEANUP_ENABLE="${FORCE_CLEANUP_ENABLE:-true}"

# Dedicated tmp root per converter service (helps debugging and avoids tmp contention)
export FORCE_TMP_ROOT="${FORCE_TMP_ROOT:-/data/pub/tmp/${CONVERTER_NAME}}"

exec /opt/scripts/rinex-converter.sh
