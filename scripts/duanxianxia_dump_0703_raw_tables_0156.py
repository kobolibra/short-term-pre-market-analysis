#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_dump_0703_raw_tables_0156.py — Task 0156.

把服务器本地磁盘上指定交易日(默认 2026-07-03)的 weimai(auction.jjyd.weimai)
和 vratio(auction.jjyd.vratio) 两张原始 capture 表全部字段、全部行原样 dump
出来，写入审计报告文件，供人工核对 item9 语义与 vratio 竞价成交额口径。
若 captures/<date>/ 目录不存在或为空，如实报告目录状态，不臆造数据。
用法: python3 scripts/duanxianxia_dump_0703_raw_tables_0156.py [YYYY-MM-DD]
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import DEFAULT_PROJECT_ROOT

DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-07-03"
DATASETS = ["auction.jjyd.weimai", "auction.jjyd.vratio"]


def load_all_files(date_dir: Path, dsid: str):
    d = date_dir / dsid
    if not d.is_dir():
        return {"dir_exists": False, "file_names": [], "captures": []}
    files = sorted(d.glob("*.json"))
    captures = []
    for fp in files:
        try:
            payload = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            captures.append({"file": fp.name, "error": str(e)})
            continue
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            rows = payload.get("rows")
        else:
            rows = None
        captures.append({
            "file": fp.name,
            "row_count": len(rows) if isinstance(rows, list) else None,
            "rows": rows if isinstance(rows, list) else payload,
        })
    return {"dir_exists": True, "file_names": [f.name for f in files], "captures": captures}


def main():
    root = Path(DEFAULT_PROJECT_ROOT)
    cap_root = root / "captures"
    date_dir = cap_root / DATE
    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0156_dump_0703_raw_tables",
        "date": DATE,
        "captures_root_exists": cap_root.is_dir(),
        "captures_root_listing": sorted(p.name for p in cap_root.iterdir())[-40:] if cap_root.is_dir() else [],
        "date_dir_exists": date_dir.is_dir(),
        "date_dir_listing": sorted(p.name for p in date_dir.iterdir()) if date_dir.is_dir() else [],
        "datasets": {},
    }
    for ds in DATASETS:
        out["datasets"][ds] = load_all_files(date_dir, ds)

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    out_path = audit / "raw_dump_0703_weimai_vratio_0156.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    summary = {
        "date": DATE,
        "captures_root_exists": out["captures_root_exists"],
        "date_dir_exists": out["date_dir_exists"],
        "date_dir_listing": out["date_dir_listing"],
        "captures_root_listing_tail": out["captures_root_listing"],
        "weimai_files": out["datasets"]["auction.jjyd.weimai"].get("file_names"),
        "vratio_files": out["datasets"]["auction.jjyd.vratio"].get("file_names"),
        "weimai_row_counts": [c.get("row_count") for c in out["datasets"]["auction.jjyd.weimai"].get("captures", [])],
        "vratio_row_counts": [c.get("row_count") for c in out["datasets"]["auction.jjyd.vratio"].get("captures", [])],
        "output_file": str(out_path),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
