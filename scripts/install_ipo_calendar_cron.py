#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""install_ipo_calendar_cron.py — install standalone IPO calendar 8am cron on VM.

Installs an idempotent crontab entry:
  0 8 * * 1-5 /usr/bin/flock -n /tmp/ipo_calendar.lock bash .../scripts/ipo_calendar_cron_runner.sh

This is intentionally separate from duanxianxia. It only runs the standalone
scripts/ipo_calendar_notify.py through scripts/ipo_calendar_cron_runner.sh.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
WS = Path("/home/investmentofficehku/.openclaw/workspace")
RUNNER = WS / "scripts" / "ipo_calendar_cron_runner.sh"
AUDIT = WS / "projects" / "ipo_calendar" / "reports" / "_audit"
CRON_MARK = "ipo_calendar_cron_runner.sh"
CRON_LINE = f"0 8 * * 1-5 /usr/bin/flock -n /tmp/ipo_calendar.lock bash {RUNNER}"


def sh(cmd: list[str], input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, input=input_text, text=True, capture_output=True)


def main() -> int:
    AUDIT.mkdir(parents=True, exist_ok=True)
    if not RUNNER.exists():
        raise RuntimeError(f"runner not found: {RUNNER}")
    try:
        RUNNER.chmod(RUNNER.stat().st_mode | 0o111)
    except Exception:
        pass

    cur = sh(["crontab", "-l"])
    existing = cur.stdout if cur.returncode == 0 else ""
    lines = [ln.rstrip() for ln in existing.splitlines() if ln.strip()]
    # Remove any old IPO calendar entries, keep unrelated cron lines intact.
    lines = [ln for ln in lines if CRON_MARK not in ln and "/tmp/ipo_calendar.lock" not in ln]
    lines.append(CRON_LINE)
    new_cron = "\n".join(lines) + "\n"
    proc = sh(["crontab", "-"], input_text=new_cron)

    rec = {
        "generated_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "ok": proc.returncode == 0,
        "cron_line": CRON_LINE,
        "stdout": proc.stdout[-2000:],
        "stderr": proc.stderr[-2000:],
        "previous_count": len(existing.splitlines()),
        "new_count": len(lines),
    }
    out = AUDIT / "install_ipo_calendar_cron.json"
    out.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(rec, ensure_ascii=False, indent=2))
    return 0 if proc.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
