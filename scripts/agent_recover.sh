#!/usr/bin/env bash
#
# One-shot recovery + diagnostic for the agent job pipeline.
#
# The autonomous runner (agent_job_runner.sh via */10 cron) stopped publishing
# after 2026-07-03 23:50 and did NOT self-heal. This script surfaces WHY and can
# force one foreground run to drain the backlog (0137/0138 + the midnight daily
# batch).
#
# Usage:
#   bash scripts/agent_recover.sh          # diagnose only (safe, read-only-ish)
#   bash scripts/agent_recover.sh --run    # diagnose, then force one runner pass
#
# NOTE: the server working copy is likely stuck at an old commit (the dead runner
# never did its git fetch/reset), so this file may not exist on disk yet. If so:
#   cd "$WS" && git fetch origin main && git reset --hard origin/main
# then re-run this script.

set -uo pipefail

WS="${WS:-/home/investmentofficehku/.openclaw/workspace}"
RUN=0
[ "${1:-}" = "--run" ] && RUN=1

hr() { printf '\n=== %s ===\n' "$1"; }

hr "context"
date -Is
echo "host=$(hostname 2>/dev/null)  user=$(whoami 2>/dev/null)"
echo "WS=$WS"
cd "$WS" 2>/dev/null || { echo "!! cannot cd into WS=$WS"; exit 2; }

hr "cron alive?"
( systemctl status cron 2>/dev/null || systemctl status crond 2>/dev/null \
    || service cron status 2>/dev/null || service crond status 2>/dev/null \
    || echo "!! no systemd/service cron status available" ) | head -n 15
echo "--- crontab entries mentioning the runner ---"
crontab -l 2>/dev/null | grep -E 'agent_job_runner|duanxianxia' || echo "(none in crontab -l)"

hr "stuck processes"
ps aux 2>/dev/null | grep -E 'agent_job_runner|agent_job_worker|publish_results|duanxianxia|git ' | grep -v grep \
    || echo "(no matching processes running)"

hr "lock files (delete only if the owning PID above is dead)"
find . -maxdepth 3 -name '*.lock' 2>/dev/null | while read -r f; do
    printf '%s  ->  ' "$f"; cat "$f" 2>/dev/null; echo
done
ls -la /tmp/*agent*.lock /tmp/*duanxianxia*.lock 2>/dev/null || true

hr "recent logs (tail)"
for lg in runner.log scripts/runner.log logs/runner.log \
          agent_job_runner.log scripts/agent_jobs/runner.log \
          worker.log scripts/agent_jobs/worker.log; do
    if [ -f "$lg" ]; then
        echo "--- $lg (last 40) ---"; tail -n 40 "$lg"; echo
    fi
done

hr "git state of workspace"
echo "branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null)"
echo "local HEAD:  $(git rev-parse HEAD 2>/dev/null)"
git fetch origin main --quiet 2>/dev/null && \
  echo "origin/main: $(git rev-parse origin/main 2>/dev/null)"
echo "--- status --short ---"; git status --short 2>/dev/null | head -n 20
echo "--- can we push to agent-results? (dry-run) ---"
git push --dry-run origin HEAD:refs/heads/agent-results 2>&1 | head -n 8 \
    || echo "!! push dry-run failed -- likely auth/token or non-fast-forward"

if [ "$RUN" -eq 1 ]; then
    hr "FORCE RUN: syncing main then running runner in foreground"
    git fetch origin main && git reset --hard origin/main
    echo "now at: $(git rev-parse HEAD)"
    if [ -f scripts/agent_job_runner.sh ]; then
        bash scripts/agent_job_runner.sh
        echo "runner exit code: $?"
    else
        echo "!! scripts/agent_job_runner.sh not found after sync"
    fi
else
    hr "next step"
    echo "Diagnosis only. To force one pass and drain the backlog, run:"
    echo "  bash scripts/agent_recover.sh --run"
fi
