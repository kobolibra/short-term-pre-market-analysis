#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0161 install duanxianxia auction premarket self-check cron.

Adds a trading-day 09:30 cron that runs duanxianxia_auction_selfcheck.py, which
verifies the 09:25 premarket capture produced the auction.jjyd.* / auction.jjlive.*
tables and, if not, runs a best-effort fetch_retry backfill + writes an alert.

Idempotent & additive: preserves ALL existing crontab lines (the */10 analysis
runner, the IPO cron, and the 0102 capture crons); only (re)writes OUR self-check
line, identified by its unique markers. Mirrors the proven 0102 install pattern.
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

SELFCHECK = SCRIPTS / "duanxianxia_auction_selfcheck.py"

# 09:30 = ~5 min after the `25 9` premarket capture (its 15-25s sleep + runtime).
# Auction summary tables are typically still queryable after the 09:15-09:25
# window, so a backfill here can still recover a transient 09:25 miss.
SELFCHECK_LINE = (
    f"30 9 * * 1-5 /usr/bin/flock -n /tmp/dxx_auction_selfcheck.lock "
    f"bash -lc 'cd {WS} && /usr/bin/python3 {SELFCHECK}'"
)

# Markers unique to OUR self-check line. Deliberately do NOT match any existing
# cron line (analysis / IPO / 0102 capture), so those are preserved untouched.
SELFCHECK_MARKERS = [
    "duanxianxia_auction_selfcheck.py",
    "/tmp/dxx_auction_selfcheck.lock",
]


def sh(cmd, input_text=None):
    return subprocess.run(cmd, input=input_text, text=True, capture_output=True)


def main() -> int:
    AUDIT.mkdir(parents=True, exist_ok=True)

    missing_script = [] if SELFCHECK.exists() else [str(SELFCHECK)]
    try:
        SELFCHECK.chmod(SELFCHECK.stat().st_mode | 0o111)
    except Exception:
        pass

    cur = sh(["crontab", "-l"])
    existing = cur.stdout if cur.returncode == 0 else ""
    prev_lines = [ln.rstrip() for ln in existing.splitlines() if ln.strip()]

    kept = [ln for ln in prev_lines if not any(m in ln for m in SELFCHECK_MARKERS)]
    new_lines = kept + [SELFCHECK_LINE]
    new_cron = "\n".join(new_lines) + "\n"

    proc = sh(["crontab", "-"], input_text=new_cron)

    verify = sh(["crontab", "-l"])
    final_lines = [ln.rstrip() for ln in (verify.stdout or "").splitlines() if ln.strip()]

    rec = {
        "job": "0161_install_auction_selfcheck_cron",
        "generated_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "ok": proc.returncode == 0 and not missing_script,
        "missing_script": missing_script,
        "previous_crontab": prev_lines,
        "installed_line": SELFCHECK_LINE,
        "final_crontab": final_lines,
        "write_stderr": (proc.stderr or "")[-1000:],
    }
    out = AUDIT / "install_auction_selfcheck_cron.json"
    out.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(rec, ensure_ascii=False, indent=2))

    if missing_script:
        return 3
    return 0 if proc.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
