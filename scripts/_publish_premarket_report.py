#!/usr/bin/env python3
"""把指定日期的盘前报告复制到 _audit/premarket_reports/ 供 git 发布."""
import sys, json, shutil
from pathlib import Path
from datetime import datetime

WS = Path("/home/investmentofficehku/.openclaw/workspace")
REPORTS = WS / "projects" / "duanxianxia" / "reports"
AUDIT = REPORTS / "_audit" / "premarket_reports"

def main():
    today = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    src_dir = REPORTS / today / "premarket"
    if not src_dir.is_dir():
        print(f"ERROR: no premarket dir at {src_dir}")
        return 1
    candidates = sorted(src_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        print(f"ERROR: no json files in {src_dir}")
        return 1
    src = candidates[0]
    AUDIT.mkdir(parents=True, exist_ok=True)
    dst = AUDIT / f"{today}.json"
    shutil.copy2(src, dst)
    print(f"OK: {src} -> {dst} ({src.stat().st_size} bytes)")
    return 0

if __name__ == "__main__":
    sys.exit(main())
