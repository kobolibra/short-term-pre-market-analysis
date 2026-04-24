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
python3 scripts/duanxianxia_batch.py "$GROUP"
