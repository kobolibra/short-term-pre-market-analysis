#!/usr/bin/env bash
# agent_job_runner.sh — cron entry: 同步 git main 后运行 job worker, 并把结果发布到 agent-results 分支。
#
# 一次性安装（在 VM 上执行一次，之后全自动）：
#   chmod +x /home/investmentofficehku/.openclaw/workspace/scripts/agent_job_runner.sh
#   ( crontab -l 2>/dev/null; echo '*/10 * * * * /usr/bin/flock -n /tmp/dxx_agent_job.lock bash /home/investmentofficehku/.openclaw/workspace/scripts/agent_job_runner.sh' ) | crontab -
#
# 说明：
# - git reset --hard origin/main 使工作树严格跟随远端 main（丢弃本地未推送的代码改动）。
#   captures/reports/dailyline 等数据如在 .gitignore 内不受影响。未跟踪的队列文件(daily_*.json)不被 reset 删除。
# - 每轮先调用 agent_daily_refresh.py 把当日分析套件入队(幂等，每天每脚本一次)，再跑 worker。
# - 跑完后把 reports/_audit 下的小结果文件快照发布到独立分支 agent-results（force-push，
#   仅本机写，永不与 main 代码推送冲突），供 AI 直接经 GitHub 读取，避开外部隐道网址。
#   原始大数据（*_analysis_v9.json、captures、dailyline）从不上传，只由服务器脚本本地读。
# - flock 防止上一次还没跑完又被叠加。
set -uo pipefail
WS=/home/investmentofficehku/.openclaw/workspace
RESULTS_REL=projects/duanxianxia/reports/_audit
RESULTS_BRANCH=agent-results
LOG="$WS/$RESULTS_REL/agent_jobs/runner.log"
mkdir -p "$(dirname "$LOG")"
cd "$WS" || { echo "$(date -Is) cd failed" >> "$LOG"; exit 1; }

publish_results() {
  command -v git >/dev/null 2>&1 || { echo "git missing"; return 0; }
  [ -d "$RESULTS_REL" ] || { echo "no results dir"; return 0; }
  local TMPIDX TREE COMMIT
  local -a RFILES
  TMPIDX="$(mktemp -u)"
  mapfile -t RFILES < <(find "$RESULTS_REL" -type f ! -name '*.lock' 2>/dev/null)
  if [ "${#RFILES[@]}" -eq 0 ]; then echo "no result files"; return 0; fi
  GIT_INDEX_FILE="$TMPIDX" git add -f -- "${RFILES[@]}" 2>/dev/null
  if ! GIT_INDEX_FILE="$TMPIDX" git ls-files --cached | grep -q .; then
    rm -f "$TMPIDX"; echo "nothing staged"; return 0
  fi
  TREE="$(GIT_INDEX_FILE="$TMPIDX" git write-tree)"
  rm -f "$TMPIDX"
  [ -n "$TREE" ] || { echo "write-tree failed"; return 0; }
  COMMIT="$(git commit-tree "$TREE" -m "agent results $(date -Is)")"
  [ -n "$COMMIT" ] || { echo "commit-tree failed"; return 0; }
  if git push --force origin "$COMMIT:refs/heads/$RESULTS_BRANCH" 2>&1; then
    echo "results published to $RESULTS_BRANCH @ ${COMMIT:0:8}"
  else
    echo "results push FAILED (check push credentials)"
  fi
}

{
  echo "=== $(date -Is) sync+run ==="
  if git fetch origin main --quiet && git reset --hard origin/main --quiet; then
    echo "git synced to $(git rev-parse --short HEAD)"
  else
    echo "git sync FAILED (check remote/credentials)"
  fi
  echo "--- daily refresh enqueue ---"
  /usr/bin/python3 scripts/agent_daily_refresh.py
  /usr/bin/python3 scripts/agent_job_worker.py
  echo "--- publish results ---"
  publish_results
  echo "=== done $(date -Is) ==="
} >> "$LOG" 2>&1
