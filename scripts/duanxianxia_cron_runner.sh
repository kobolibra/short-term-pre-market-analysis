#!/usr/bin/env bash
set -euo pipefail

GROUP="${1:-}"
if [[ -z "$GROUP" ]]; then
  echo "usage: duanxianxia_cron_runner.sh <premarket|postmarket|postmarket_cashflow|cashflow|intraday_cashflow>" >&2
  exit 2
fi

case "$GROUP" in
  premarket)
    # 15-25 秒随机延迟
    sleep "$(( RANDOM % 11 + 15 ))"
    ;;
  postmarket|postmarket_cashflow)
    # 0-2 分钟随机延迟，避免仅随机 sleep 就把 5 分钟 cron 超时预算吃满
    sleep "$(( RANDOM % 121 ))"
    ;;
  cashflow)
    # 保留兼容：纯资金流向固定执行，不额外随机延迟
    ;;
  intraday_cashflow)
    # 10:01 任务：盘中表 + 资金流向，延迟 0-45 秒
    sleep "$(( RANDOM % 46 ))"
    ;;
  *)
    echo "unsupported group: $GROUP" >&2
    exit 2
    ;;
esac

cd /home/investmentofficehku/.openclaw/workspace

if [[ "$GROUP" == "premarket" ]]; then
  # v7: run setup-classifier instead of v5 inline analysis. The runner imports
  # duanxianxia_batch as a module, monkey-patches build_premarket_analysis to
  # v7, then dispatches main() with the same argv. No double-run — v5 path is
  # entirely bypassed for premarket.
  python3 scripts/duanxianxia_premarket_v7_runner.py "$GROUP"
else
  python3 scripts/duanxianxia_batch.py "$GROUP"
fi

# v7 intraday validator: after intraday_cashflow capture, validate premarket
# anchors against fresh intraday data and emit reports/<date>/intraday_validation.json.
# Failure of validator MUST NOT mask success of the capture (capture is the
# critical part of the cron). Use `|| true` so cron exit stays 0 if validator
# bails out (e.g. before premarket anchors exist on a non-trading day).
if [[ "$GROUP" == "intraday_cashflow" ]]; then
  python3 scripts/duanxianxia_intraday_validator.py \
    --project-root /home/investmentofficehku/.openclaw/workspace/projects/duanxianxia \
    --quiet || echo "[duanxianxia] v7 intraday validator returned non-zero (non-fatal)" >&2
fi
