#!/usr/bin/env bash
set -euo pipefail

TARGET_DATE="${1:-$(TZ=Asia/Shanghai date +%F)}"

cd /home/investmentofficehku/.openclaw/workspace

python3 scripts/duanxianxia_batch.py dailyline --target-date "$TARGET_DATE"
python3 scripts/duanxianxia_review_backfill.py --target-date "$TARGET_DATE"
