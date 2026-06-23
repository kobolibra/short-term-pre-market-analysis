#!/usr/bin/env bash
# agent_job_runner.sh — cron entry: 同步 git main 后运行 job worker。
#
# 一次性安装（在 VM 上执行一次，之后全自动）：
#   chmod +x /home/investmentofficehku/.openclaw/workspace/scripts/agent_job_runner.sh
#   ( crontab -l 2>/dev/null; echo '*/10 * * * * /usr/bin/flock -n /tmp/dxx_agent_job.lock bash /home/investmentofficehku/.openclaw/workspace/scripts/agent_job_runner.sh' ) | crontab -
#
# 说明：
# - git reset --hard origin/main 使工作树严格跟随远端 main（丢弃本地未推送的代码改动）。
#   captures/reports/dailyline 等数据如在 .gitignore 内不受影响。
# - flock 防止上一次还没跑完又被叠加。
set -uo pipefail
WS=/home/investmentofficehku/.openclaw/workspace
LOG="$WS/projects/duanxianxia/reports/_audit/agent_jobs/runner.log"
mkdir -p "$(dirname "$LOG")"
cd "$WS" || { echo "$(date -Is) cd failed" >> "$LOG"; exit 1; }
{
  echo "=== $(date -Is) sync+run ==="
  if git fetch origin main --quiet && git reset --hard origin/main --quiet; then
    echo "git synced to $(git rev-parse --short HEAD)"
  else
    echo "git sync FAILED (check remote/credentials)"
  fi
  /usr/bin/python3 scripts/agent_job_worker.py
  echo "=== done $(date -Is) ==="
} >> "$LOG" 2>&1
