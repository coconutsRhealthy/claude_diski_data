#!/bin/bash
set -euo pipefail

MARKET="${1:?usage: run-pipeline.sh <market>}"
LOG="/var/log/diski/pipeline-${MARKET}.log"

echo "=== $(date -Is) starting pipeline for ${MARKET} ===" >> "$LOG"
docker run --rm \
  --env-file /srv/diski/.env \
  -v /srv/diski/data:/app/data \
  -v /srv/diski/inputs:/app/inputs \
  -v /srv/diski/output:/app/output \
  diski-pipeline --market "$MARKET" \
  >> "$LOG" 2>&1
echo "=== $(date -Is) finished pipeline for ${MARKET} ===" >> "$LOG"
