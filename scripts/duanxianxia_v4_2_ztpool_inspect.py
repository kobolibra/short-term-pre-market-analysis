#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_ztpool_inspect.py  --  检查 ztpool 数据实际字段
"""

from __future__ import annotations

import json
import sys
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
CAPTURES_DIR = PROJECT_ROOT / "captures"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit"
ZT_POOL_DS = "home.ztpool"

_HHMMSS_FILE_PATTERN = re.compile(r"^(\d{6})\.json$")


def _list_capture_files(dir_path: Path) -> List[tuple]:
    if not dir_path.exists() or not dir_path.is_dir():
        return []
    out = []
    for p in sorted(dir_path.iterdir()):
        m = _HHMMSS_FILE_PATTERN.match(p.name)
        if m:
            out.append((m.group(1), p))
    out.sort(key=lambda x: x[0])
    return out


def main() -> int:
    if not CAPTURES_DIR.exists():
        print(f"ERROR: captures dir not found: {CAPTURES_DIR}")
        return 1

    date_dirs = sorted([d for d in CAPTURES_DIR.iterdir() if d.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", d.name)])
    print(f"Scanning {len(date_dirs)} date directories for ztpool data...")

    found = 0
    for dd in date_dirs:
        ds = dd.name
        ztpool_dir = dd / ZT_POOL_DS
        if not ztpool_dir.exists():
            continue

        files = _list_capture_files(ztpool_dir)
        if not files:
            continue

        found += 1
        hhmmss, path = files[-1]  # 取最新快照
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  {ds} READ ERROR: {e}")
            continue

        if isinstance(data, dict):
            rows = data.get("rows", [])
            meta = data.get("meta", {})
            print(f"\n=== {ds} @ {hhmmss} ===")
            print(f"  meta keys: {list(meta.keys())}")
            print(f"  n_rows: {len(rows)}")
            if rows and isinstance(rows, list):
                r0 = rows[0]
                if isinstance(r0, dict):
                    print(f"  row[0] keys: {list(r0.keys())}")
                    print(f"  row[0] sample: {json.dumps(r0, ensure_ascii=False, default=str)[:500]}")
                else:
                    print(f"  row[0] type: {type(r0).__name__}, value: {str(r0)[:300]}")
            if len(rows) > 0:
                print(f"  --- all rows ---")
                for r in rows:
                    if isinstance(r, dict):
                        ladder = r.get("ladder_group") or r.get("分组名称") or r.get("group") or "?"
                        promo = r.get("promo_rate") or r.get("晋级率") or r.get("rate") or "N/A"
                        name = r.get("code") or r.get("name") or r.get("名称") or "?"
                        print(f"    {name} | ladder={ladder} | promo={promo}")
                    else:
                        print(f"    {str(r)[:200]}")
        else:
            print(f"  {ds}: data is not dict, type={type(data).__name__}")

        if found >= 5:
            break

    if found == 0:
        print(f"\nNo ztpool data found in any date directory!")
        # List all ds directories
        print(f"\nAll dataset directories in {date_dirs[0] if date_dirs else 'N/A'}:")
        if date_dirs:
            for d in sorted(date_dirs[0].iterdir()):
                if d.is_dir():
                    print(f"  {d.name}")

    return 0


if __name__ == "__main__":
    sys.exit(main())