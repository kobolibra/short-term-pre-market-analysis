#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v41_capture_timing_audit.py — job: 扫描所有 captures 的 fetched_at, 确认每个数据集到底盘前还是盘中抓取.

动机: GitHub 仓库只提交了 4 个日期, 无法逐日确认 pool/cashflow 的抓取时点. 本作业在服务器端
(拥有完整多天)扫描每个数据集每份快照的 fetched_at(HH:MM:SS), 统计最早/最晚抓取时间、
盘前(<=09:30)快照数 vs 盘中/后数, 以及每日首次抓取时间. 纯只读.
输出 reports/_audit/capture_timing_audit_v41.{json,md}
用法: python3 scripts/v41_capture_timing_audit.py
"""
from __future__ import annotations
import json
import sys
import traceback
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10

PREOPEN = "09:30:00"
YES = "\u662f"
NO = "\u5426"


def hhmmss(fetched_at):
    try:
        return fetched_at.split("T")[1][:8]
    except Exception:
        return None


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []
    datasets = {}
    for dd in date_dirs:
        for ds_dir in sorted(p for p in dd.iterdir() if p.is_dir()):
            ds = ds_dir.name
            info = datasets.setdefault(ds, {"n_files": 0, "preopen": 0, "intraday": 0, "times": [], "per_date": {}})
            day_times = []
            for f in sorted(ds_dir.glob("*.json")):
                try:
                    payload = json.loads(f.read_text(encoding="utf-8"))
                except Exception:
                    continue
                fa = payload.get("fetched_at") if isinstance(payload, dict) else None
                t = hhmmss(fa) if fa else None
                if t is None:
                    stem = f.stem
                    if len(stem) == 6 and stem.isdigit():
                        t = stem[:2] + ":" + stem[2:4] + ":" + stem[4:]
                if t is None:
                    continue
                info["n_files"] += 1
                info["times"].append(t)
                day_times.append(t)
                if t <= PREOPEN:
                    info["preopen"] += 1
                else:
                    info["intraday"] += 1
            if day_times:
                info["per_date"][dd.name] = sorted(day_times)
    out = {}
    for ds, info in datasets.items():
        times = sorted(info["times"])
        out[ds] = {
            "n_files": info["n_files"],
            "preopen_files": info["preopen"],
            "intraday_files": info["intraday"],
            "earliest": times[0] if times else None,
            "latest": times[-1] if times else None,
            "is_premarket_dataset": info["preopen"] > 0 and info["intraday"] == 0,
            "per_date_first": {d: ts[0] for d, ts in sorted(info["per_date"].items())},
        }
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "capture_timing_audit_v41",
        "preopen_cutoff": PREOPEN,
        "n_dates": len(date_dirs),
        "datasets": out,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "capture_timing_audit_v41.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# \u6293\u53d6\u65f6\u70b9\u5ba1\u8ba1 v41", "",
         "- \u751f\u6210: " + report["generated_at"] + " \uff5c\u65e5\u671f\u6570: " + str(len(date_dirs)) + " \uff5c\u76d8\u524d\u9608\u503c: " + PREOPEN, "",
         "| \u6570\u636e\u96c6 | \u6587\u4ef6\u6570 | \u76d8\u524d(<=9:30) | \u76d8\u4e2d/\u540e | \u6700\u65e9 | \u6700\u665a | \u7eaf\u76d8\u524d? |",
         "|---|---|---|---|---|---|---|"]
    for ds in sorted(out):
        o = out[ds]
        flag = YES if o["is_premarket_dataset"] else NO
        L.append("| " + ds + " | " + str(o["n_files"]) + " | " + str(o["preopen_files"]) + " | " + str(o["intraday_files"]) + " | " + str(o["earliest"]) + " | " + str(o["latest"]) + " | " + flag + " |")
    (audit / "capture_timing_audit_v41.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
