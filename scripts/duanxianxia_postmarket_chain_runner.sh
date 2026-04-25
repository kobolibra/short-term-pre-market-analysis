#!/usr/bin/env bash
set -euo pipefail

TARGET_DATE="${1:-$(TZ=Asia/Shanghai date +%F)}"
TMP_FETCH_JSON="$(mktemp)"
trap 'rm -f "$TMP_FETCH_JSON"' EXIT

# 正确链路：先抓盘后原始数据形成 official report/capture，供 dailyline 建池；
# 然后补齐日线；最后基于这份已抓到的盘后 report 重建分析并正式输出。
sleep "$(( RANDOM % 31 ))"

cd /home/investmentofficehku/.openclaw/workspace

echo "[duanxianxia] postmarket_cashflow capture_only"
python3 scripts/duanxianxia_batch.py postmarket_cashflow --capture-only --webhook-url '' --json > "$TMP_FETCH_JSON"
REPORT_PATH="$(python3 - <<'PY' "$TMP_FETCH_JSON"
import json, sys
with open(sys.argv[1], encoding='utf-8') as f:
    data = json.load(f)
print(data.get('report_path', ''))
PY
)"

if [[ -z "$REPORT_PATH" ]]; then
  echo "failed to resolve capture_only report_path" >&2
  exit 1
fi

echo "[duanxianxia] capture report_path=${REPORT_PATH}"
echo

echo "[duanxianxia] dailyline target_date=${TARGET_DATE}"
python3 scripts/duanxianxia_batch.py dailyline --target-date "$TARGET_DATE"

echo
echo "[duanxianxia] postmarket_cashflow analysis_only source_report=${REPORT_PATH}"
python3 scripts/duanxianxia_batch.py postmarket_cashflow --report-path "$REPORT_PATH" --save-analysis-copy
