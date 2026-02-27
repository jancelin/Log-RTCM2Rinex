#!/usr/bin/env bash
set -euo pipefail

: "${PUB_ROOT:=/srv/pub}"
: "${STATS_DIR:=/srv/pub/stats}"
: "${SCAN_DAYS:=60}"           # fenêtre glissante recalculée à chaque run
: "${SLEEP_SECONDS:=1800}"     # 30 minutes par défaut

mkdir -p "$STATS_DIR"

echo "[stat] ============================================"
echo "[stat] PUB_ROOT      = $PUB_ROOT"
echo "[stat] STATS_DIR     = $STATS_DIR"
echo "[stat] SCAN_DAYS     = $SCAN_DAYS"
echo "[stat] SLEEP_SECONDS = $SLEEP_SECONDS"
echo "[stat] ============================================"
echo "[stat] Premier scan immédiat au démarrage…"

# Premier run immédiat (pas d'attente au démarrage)
python /app/stat.py \
  --pub-root  "$PUB_ROOT" \
  --stats-dir "$STATS_DIR" \
  --scan-days "$SCAN_DAYS" \
  || echo "[stat] WARN: premier scan échoué (on continue)"

echo "[stat] Boucle : un scan toutes les ${SLEEP_SECONDS}s"

while true; do
  sleep "$SLEEP_SECONDS"
  python /app/stat.py \
    --pub-root  "$PUB_ROOT" \
    --stats-dir "$STATS_DIR" \
    --scan-days "$SCAN_DAYS" \
    || echo "[stat] WARN: stat.py a échoué (retry au prochain cycle)"
done
