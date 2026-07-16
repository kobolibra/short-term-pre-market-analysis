#!/usr/bin/env bash
# agent_job_runner.sh — cron entry: 同步 git main 后运行 job worker, 并把结果发布到 agent-results 分支。
set -uo pipefail
WS=/home/investmentofficehku/.openclaw/workspace
RESULTS_REL=projects/duanxianxia/reports/_audit
REPORTS_REL=projects/duanxianxia/reports
IPO_RESULTS_REL=projects/ipo_calendar/reports/_audit
RESULTS_BRANCH=agent-results
LOG="$WS/$RESULTS_REL/agent_jobs/runner.log"
mkdir -p "$(dirname "$LOG")"
cd "$WS" || { echo "$(date -Is) cd failed" >> "$LOG"; exit 1; }

publish_results() {
  command -v git >/dev/null 2>&1 || { echo "git missing"; return 0; }
  local TMPIDX TREE COMMIT
  local -a RFILES
  TMPIDX="$(mktemp -u)"
  RFILES=()
  if [ -d "$RESULTS_REL" ]; then
    while IFS= read -r f; do RFILES+=("$f"); done < <(find "$RESULTS_REL" -type f ! -name '*.lock' 2>/dev/null)
  fi
  if [ -d "$REPORTS_REL" ]; then
    while IFS= read -r f; do RFILES+=("$f"); done < <(find "$REPORTS_REL" -maxdepth 3 -type f -name "*.json" ! -path "*/_audit/*" 2>/dev/null)
  fi
  if [ -d "$IPO_RESULTS_REL" ]; then
    while IFS= read -r f; do RFILES+=("$f"); done < <(find "$IPO_RESULTS_REL" -type f ! -name '*.lock' 2>/dev/null)
  fi
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
  echo "--- capture datestamp self-heal ---"
  /usr/bin/python3 scripts/duanxianxia_capture_datestamp_selfheal.py --recent 6 --apply
  echo "--- publish results ---"
  publish_results
  echo "=== done $(date -Is) ==="
} >> "$LOG" 2>&1
