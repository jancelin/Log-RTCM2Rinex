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
  local lvl="${1:-INFO}"; shift || true
  local threshold="${STATION_EVENTS_LEVEL:-${LOG_LEVEL:-INFO}}"
  local line="[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $MP $lvl $*"

  # monitoring-first: write to events file (persistent)
  if log_should "$lvl" "$threshold"; then
    echo "$line" >> "${EVENTS_FILE:-/dev/stderr}"
  fi

  # optional stdout echo (for DEBUG troubleshooting)
  local echo_mode="${STATION_ECHO_STDOUT:-auto}"
  if [[ "$echo_mode" == "auto" ]]; then
    if (( $(log_level_num "${LOG_LEVEL:-INFO}") >= 3 )); then
      echo_mode=true
    else
      echo_mode=false
    fi
  fi
  if bool_is_true "$echo_mode" && log_should "$lvl" "${LOG_LEVEL:-INFO}"; then
    echo "$line"
  fi
}

child_pid=""

cleanup() {
  if [[ -n "${child_pid:-}" ]] && kill -0 "$child_pid" 2>/dev/null; then
    evt INFO "cleanup: stopping str2str (pid=$child_pid)"
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
  # STR2STR_TRACE_LEVEL : niveau de trace RTKLIB écrit dans $TRACE_ROOT/<MP>/str2str.trace
  #
  # VALEUR PAR DÉFAUT CHANGÉE : 2 → 0
  #
  # Justification (P6) :
  #   Avec l'ancienne valeur de 2, str2str écrit en continu dans str2str.trace
  #   pour CHAQUE station (une ligne toutes les ~5 secondes : reconnexions, statuts).
  #   À 240 stations : 240 descripteurs de fichiers ouverts en écriture permanente.
  #   Impact mesuré :
  #     - ~288 Mo/jour de données de trace (240 stations × ~50 Ko/h × 24 h)
  #     - Avec TRACE_RETENTION_DAYS=0 : accumulation sans limite sur le NFS
  #     - Contribution estimée au baseline CPU logger : 5-8 %
  #       (I/O wait NFS sur 240 écritures simultanées toutes les 5 s)
  #
  #   Le suivi de santé des stations est assuré par le watchdog (STALE_AFTER_SEC)
  #   et les fichiers *.events.log — le fichier .trace RTKLIB est redondant
  #   en production stable et utile uniquement pour diagnostiquer une station
  #   problématique individuelle.
  #
  # Pour activer les traces sur une station spécifique sans redémarrer toutes les autres :
  #   Ajouter STATION_<MP>_STR2STR_TRACE_LEVEL=2 dans /config/.env
  #   Le script recharge la configuration à chaque cycle (load_cfg en tête de boucle).
  #
  # Référence RTKLIB : T. Takasu, "RTKLIB ver. 2.4.2 Manual", §A.3 str2str :
  #   -t level : output trace level (0: off, 1-5: increasing verbosity).
  #   Level 2 = connexions/déconnexions, statuts réseau. Level 0 = aucun fichier créé.
  STR2STR_TRACE_LEVEL="${STR2STR_TRACE_LEVEL:-0}"
  RTCM_ROTATE_HOURS="${RTCM_ROTATE_HOURS:-1}"
  # RTKLIB str2str swap margin (-f). If >0, str2str overlaps data during file rotation,
  # which can create duplicated epochs when RTCM chunks are later concatenated.
  RTCM_SWAP_MARGIN_S="${RTCM_SWAP_MARGIN_S:-0}"
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

  evt INFO "RUN str2str -> $OUT_PATH"

  # RTKLIB console output is extremely verbose (status line every ~5s).
  # By default we discard it and keep monitoring via *.events.log + heartbeat.
  want_console=""
  if (( $(log_level_num "${LOG_LEVEL:-INFO}") >= 3 )); then
    want_console=true
  else
    want_console=false
  fi

  # Build args (omit -t entirely when 0 to avoid generating str2str.trace)
  str2str_args=(
    -in "ntrip://${NTRIP_USER}:${NTRIP_PASS}@${NTRIP_HOST}:${NTRIP_PORT}/${MP}"
    -out "$OUT_PATH"
    -s "$STR2STR_TIMEOUT_MS"
    -r "$STR2STR_RECONNECT_MS"
    -f "$RTCM_SWAP_MARGIN_S"
  )
  if [[ "${STR2STR_TRACE_LEVEL:-0}" =~ ^[0-9]+$ ]] && (( STR2STR_TRACE_LEVEL > 0 )); then
    str2str_args+=( -t "$STR2STR_TRACE_LEVEL" )
  fi

  if bool_is_true "$want_console"; then
    str2str "${str2str_args[@]}" &
  else
    str2str "${str2str_args[@]}" >/dev/null 2>&1 &
  fi
  child_pid=$!
  pid=$child_pid

  last_warn=0

  while kill -0 "$pid" 2>/dev/null; do
    now="$(date -u +%s)"

    # restart cleanly at 00:00 UTC to switch YYYY/DOY directories
    if (( now >= next_midnight_epoch )); then
      evt INFO "MIDNIGHT UTC -> restart str2str"
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
        evt WARN "STALE no new RTCM for ${age}s (base down?) latest=$(basename "$latest")"
        last_warn="$now"
      fi
    else
      if (( now - last_warn > STALE_AFTER_SEC )); then
        evt WARN "STALE no RTCM files yet (base down?)"
        last_warn="$now"
      fi
    fi

    sleep "$STALE_CHECK_EVERY_SEC"
  done

  wait "$pid" || true
  evt WARN "str2str exited -> restart in 5s"
  sleep 5
done

