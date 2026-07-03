#!/usr/bin/env python3
"""0130 cron dump (read-only diagnostic).

The 0129 audit proved a nightly ~01:20 job re-fetches review.daily / review.fupan.plate /
review.ltgd.range into the NEXT day's folder. Find WHAT schedules it: dump the live
crontab + systemd timers, and list candidate cron/review/backfill scripts.
No writes.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

WS = Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace"))


def run(cmd):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return {"rc": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def main():
    result = {"task": "0130_cron_dump", "ok": True}
    result["crontab"] = run(["crontab", "-l"])
    result["systemd_timers"] = run([
        "bash", "-lc",
        "systemctl list-timers --all --no-pager 2>/dev/null | head -60",
    ])
    result["cron_d_grep"] = run([
        "bash", "-lc",
        "grep -rEl 'duanxianxia|review|backfill' /etc/cron* 2>/dev/null | head -40",
    ])
    scripts_dir = WS / "scripts"
    names = []
    if scripts_dir.exists():
        for f in sorted(scripts_dir.iterdir()):
            n = f.name
            if any(k in n for k in ("cron", "review", "backfill", "runner", "postmarket", "install", "timer")):
                names.append(n)
    result["candidate_scripts"] = names
    print("=== CRON DUMP 0130 ===")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
