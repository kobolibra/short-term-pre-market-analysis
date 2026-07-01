#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0102 install duanxianxia capture crons (premarket/intraday/postmarket).

ROOT CAUSE: the live crontab only had the */10 analysis runner + 0 8 IPO cron.
The three capture crons documented in HANDOFF section 2.5 (premarket 25 9 /
intraday 1 10 / postmarket 20 17) were NOT installed, so no premarket data was
ever downloaded -> downstream analysis / stock-selection / push ran on empty or
stale data.

This job idempotently (re)installs the three capture crons WITHOUT touching the
existing analysis (agent_job_runner.sh) or IPO (ipo_calendar_cron_runner.sh)
cron lines. Mirrors the proven install_ipo_calendar_cron.py pattern.
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
WS = Path("/home/investmentofficehku/.openclaw/workspace")
SCRIPTS = WS / "scripts"
AUDIT = WS / "projects" / "duanxianxia" / "reports" / "_audit"

PREMARKET_RUNNER = SCRIPTS / "duanxianxia_cron_runner.sh"
POSTMARKET_RUNNER = SCRIPTS / "duanxianxia_postmarket_chain_runner.sh"

# HANDOFF 2.5 schedule. flock-guarded, matching the existing crontab style.
CAPTURE_LINES = [
    f"25 9 * * 1-5 /usr/bin/flock -n /tmp/dxx_premarket.lock bash {PREMARKET_RUNNER} premarket",
    f"1 10 * * 1-5 /usr/bin/flock -n /tmp/dxx_intraday.lock bash {PREMARKET_RUNNER} intraday_cashflow",
    f"20 17 * * 1-5 /usr/bin/flock -n /tmp/dxx_postmarket.lock bash {POSTMARKET_RUNNER}",
]

# Markers that identify OUR capture cron lines (for idempotent re-install).
# Deliberately does NOT match agent_job_runner.sh (analysis) or
# ipo_calendar_cron_runner.sh (IPO), so those lines are preserved untouched.
CAPTURE_MARKERS = [
    "duanxianxia_cron_runner.sh",
    "duanxianxia_postmarket_chain_runner.sh",
    "/tmp/dxx_premarket.lock",
    "/tmp/dxx_intraday.lock",
    "/tmp/dxx_postmarket.lock",
]


def sh(cmd, input_text=None):
    return subprocess.run(cmd, input=input_text, text=True, capture_output=True)


def main() -> int:
    AUDIT.mkdir(parents=True, exist_ok=True)

    missing_runners = [str(p) for p in (PREMARKET_RUNNER, POSTMARKET_RUNNER) if not p.exists()]
    for runner in (PREMARKET_RUNNER, POSTMARKET_RUNNER):
        try:
            runner.chmod(runner.stat().st_mode | 0o111)
        except Exception:
            pass

    cur = sh(["crontab", "-l"])
    existing = cur.stdout if cur.returncode == 0 else ""
    prev_lines = [ln.rstrip() for ln in existing.splitlines() if ln.strip()]

    # Keep everything that is not one of our capture lines, then append fresh ones.
    kept = [ln for ln in prev_lines if not any(m in ln for m in CAPTURE_MARKERS)]
    new_lines = kept + CAPTURE_LINES
    new_cron = "\n".join(new_lines) + "\n"

    proc = sh(["crontab", "-"], input_text=new_cron)

    verify = sh(["crontab", "-l"])
    final_lines = [ln.rstrip() for ln in (verify.stdout or "").splitlines() if ln.strip()]

    rec = {
        "job": "0102_install_capture_cron",
        "generated_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "ok": proc.returncode == 0 and not missing_runners,
        "missing_runners": missing_runners,
        "previous_crontab": prev_lines,
        "installed_lines": CAPTURE_LINES,
        "final_crontab": final_lines,
        "write_stderr": (proc.stderr or "")[-1000:],
    }
    out = AUDIT / "install_duanxianxia_capture_cron.json"
    out.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(rec, ensure_ascii=False, indent=2))

    if missing_runners:
        return 3
    return 0 if proc.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
