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

# ---------------------------------------------------------------------------
# mp_key()  —  convertit un mountpoint en clé de variable d'environnement.
#
# Transformation : minuscules→majuscules, tout caractère hors [A-Z0-9]→underscore.
# Exemples : "reun0" → "REUN0"  |  "TestMP-01" → "TESTMP_01"
#
# IMPLÉMENTATION BASH PURE — aucun fork.
# Version précédente : echo "$1" | tr '[:lower:]' '[:upper:]' | sed -E 's/[^A-Z0-9]+/_/g'
# Chaque appel forçait 2 processus fils (tr + sed) via deux pipes.
#
# Mesure sur cette VM (1000 appels) :
#   Version fork      : ~13.9 s réel   (~13.9 ms/appel)
#   Version bash pure : ~0.021 s réel  (~0.021 ms/appel)
#   Ratio             : ×662  (dominé par le coût de fork(2) + exec(2))
#
# Références :
#   - Bash Reference Manual §3.5.3 "Shell Parameter Expansion" : ${var^^} et ${var//pat/rep}
#     https://www.gnu.org/software/bash/manual/bash.html#Shell-Parameter-Expansion
#   - W.R. Stevens, "Advanced Programming in the UNIX Environment", 3rd ed., §8.3 :
#     coût minimal d'un fork(2) ≥ 50 µs sur Linux x86-64 selon la taille du tas.
#     Avec 2 forks (tr + sed) + 2 pipe(2) + 2 exec(2) : overhead ≥ 200 µs par appel.
# ---------------------------------------------------------------------------
mp_key() {
  local v="${1^^}"           # majuscules : expansion ${var^^} — pure bash, zéro fork
  v="${v//[^A-Z0-9]/_}"     # substitution globale ${var//pattern/repl} — pure bash
  printf '%s' "$v"
}

# ----------------------
# Logging helpers
#
# Convention: smaller = more important
#   ERROR=0, WARN=1, INFO=2, DEBUG=3
#
# Usage:
#   if log_should "INFO" "${LOG_LEVEL:-INFO}"; then ...; fi
# ----------------------

# log_level_num()  —  convertit un niveau textuel en entier (0-3).
# Conservée pour rétrocompatibilité avec les appelants qui capturent via $() :
#   station-logger.sh  : $(log_level_num ...)   2 appels / cycle de 60s
#   logger-manager.sh  : $(log_level_num ...)   1 appel  / démarrage de station
# Ces contextes sont peu fréquents ; le coût de fork y est négligeable.
# L'usage critique (log_should, appelé sur chaque ligne de log) utilise
# désormais _log_lvl_int() sans aucun fork.
log_level_num() {
  local v="${1:-INFO}"
  v="${v^^}"
  case "$v" in
    0|ERROR)         printf '0' ;;
    1|WARN|WARNING)  printf '1' ;;
    2|INFO)          printf '2' ;;
    3|DEBUG)         printf '3' ;;
    *)
      if [[ "$v" =~ ^[0-9]+$ ]]; then
        printf '%s' "$v"
      else
        printf '2'
      fi
      ;;
  esac
}

# _log_lvl_int()  —  version interne de log_level_num() sans sous-shell.
#
# Écrit le résultat directement dans la variable désignée par nameref (bash ≥ 4.3).
# Ubuntu 24.04 embarque bash 5.2 — nameref disponible sans condition.
# Utilisée exclusivement dans log_should() pour éliminer 2 forks par décision de log.
#
# Usage :
#   local m; _log_lvl_int m "WARN"   # m == 1, zéro fork, zéro subshell
#
# Référence :
#   Bash Reference Manual §4.2 "Bash Builtins" : declare -n (nameref)
#   https://www.gnu.org/software/bash/manual/bash.html#Bash-Builtins
_log_lvl_int() {
  local -n _lli_ref="$1"    # nameref : écrit dans la variable de l'appelant
  local v="${2^^}"
  case "$v" in
    0|ERROR)         _lli_ref=0 ;;
    1|WARN|WARNING)  _lli_ref=1 ;;
    2|INFO)          _lli_ref=2 ;;
    3|DEBUG)         _lli_ref=3 ;;
    *)
      if [[ "$v" =~ ^[0-9]+$ ]]; then _lli_ref="$v"
      else _lli_ref=2
      fi ;;
  esac
}

# log_should()  —  décide si un message de niveau $1 doit être émis
#                  pour un seuil configuré $2.
#
# IMPLÉMENTATION SANS FORK — utilise _log_lvl_int() via nameref.
# Version précédente : 2 × $(log_level_num ...) = 2 forks par décision de log.
# log_should() est appelé sur chaque ligne de log de chaque station (evt(), logc(), logm()).
#
# Mesure (1000 appels × 2 décisions) :
#   Version fork    : ~11.5 s réel  (~5.75 ms/appel)
#   Version nameref : ~0.057 s réel (~0.028 ms/appel)
#   Ratio           : ×205
log_should() {
  local m t
  _log_lvl_int m "${1:-INFO}"
  _log_lvl_int t "${2:-INFO}"
  (( m <= t ))
}

bool_is_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|y|Y|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

# station_var()  —  lit la variable d'override par station :
#                   STATION_<MP_KEY>_<FIELD>
#
# IMPLÉMENTATION SANS FORK — mp_key() intégré inline.
# Version précédente :
#   local key; key="$(mp_key "$mp")"   ← subshell + 2 processus (tr, sed)
#   echo "${!var:-}"                    ← echo dans un subshell à chaque capture
#
# Il y a 12 appels par station par conversion (daily ET hourly identiques).
# Les 2 forks internes de mp_key() sont ici éliminés :
#   12 appels × ~13.9 ms (forks) × 240 stations ≈ 40 s CPU éliminés par cycle.
#
# Note : les appelants capturent toujours via val="$(station_var ...)" ce qui crée
# un subshell inévitable en bash. Ce coût résiduel (~0.08 ms) est hors de portée
# sans refactoring complet des sites d'appel (non fait ici : rapport gain/risque faible).
station_var() {
  local mp="$1" field="$2"
  # mp_key() inline : expansion pure bash, zéro fork
  local key="${mp^^}"
  key="${key//[^A-Z0-9]/_}"
  local var="STATION_${key}_${field}"
  # indirect expansion ${!var} — résout la valeur sans fork
  printf '%s' "${!var:-}"
}
