#!/usr/bin/env bash
# ipo_calendar_cron_runner.sh — standalone IPO calendar cron entry.
#
# Runs independently from duanxianxia:
#   - syncs latest main
#   - fetches 9fzt IPO calendar
#   - filters events for today
#   - sends Feishu message if events exist
#   - writes local data/reports under projects/ipo_calendar/
#
# One-time install example:
#   ( crontab -l 2>/dev/null; echo '0 8 * * 1-5 /usr/bin/flock -n /tmp/ipo_calendar.lock bash /home/investmentofficehku/.openclaw/workspace/scripts/ipo_calendar_cron_runner.sh' ) | crontab -
set -uo pipefail

WS=/home/investmentofficehku/.openclaw/workspace
LOG="$WS/projects/ipo_calendar/reports/_audit/cron.log"
mkdir -p "$(dirname "$LOG")"
cd "$WS" || { echo "$(date -Is) cd failed" >> "$LOG"; exit 1; }

{
  echo "=== $(date -Is) ipo calendar run ==="
  if git fetch origin main --quiet && git reset --hard origin/main --quiet; then
    echo "git synced to $(git rev-parse --short HEAD)"
  else
    echo "git sync FAILED (continue with local code)"
  fi
  /usr/bin/python3 scripts/ipo_calendar_notify.py
  rc=$?
  echo "ipo_calendar_notify rc=$rc"
  echo "=== done $(date -Is) ==="
  exit "$rc"
} >> "$LOG" 2>&1
