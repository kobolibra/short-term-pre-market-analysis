#!/usr/bin/env bash
set -euo pipefail

ARG1="${1:-}"
STAGE="full"
TARGET_DATE="$(TZ=Asia/Shanghai date +%F)"

case "$ARG1" in
  postmarket_capture|postmarket_dailyline|postmarket_analysis)
    STAGE="$ARG1"
    ;;
  "")
    ;;
  *)
    TARGET_DATE="$ARG1"
    ;;
esac

TMP_FETCH_JSON="$(mktemp)"
trap 'rm -f "$TMP_FETCH_JSON"' EXIT

# 正确链路：先抓盘后原始数据形成 official report/capture，供 dailyline 建池；
# 然后补齐日线；最后基于这份已抓到的盘后 report 重建分析并正式输出。
sleep "$(( RANDOM % 31 ))"

cd /home/investmentofficehku/.openclaw/workspace

resolve_latest_capture_report() {
  python3 - <<'PY'
import pathlib
base = pathlib.Path('/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/reports')
paths = sorted(base.rglob('postmarket_cashflow/*.json'))
print(paths[-1] if paths else '')
PY
}

run_capture() {
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
}

run_dailyline() {
  echo "[duanxianxia] dailyline target_date=${TARGET_DATE}"
  python3 scripts/duanxianxia_batch.py dailyline --target-date "$TARGET_DATE"
}

run_analysis() {
  local report_path="${1:-}"
  if [[ -z "$report_path" ]]; then
    report_path="$(resolve_latest_capture_report)"
  fi
  if [[ -z "$report_path" ]]; then
    echo "failed to resolve source report for analysis" >&2
    exit 1
  fi

  echo "[duanxianxia] postmarket_cashflow analysis_only source_report=${report_path}"
  python3 scripts/duanxianxia_batch.py postmarket_cashflow --report-path "$report_path" --save-analysis-copy
}

case "$STAGE" in
  postmarket_capture)
    run_capture
    ;;
  postmarket_dailyline)
    run_dailyline
    ;;
  postmarket_analysis)
    run_analysis
    ;;
  full)
    run_capture
    echo
    run_dailyline
    echo
    run_analysis "$REPORT_PATH"
    ;;
esac
