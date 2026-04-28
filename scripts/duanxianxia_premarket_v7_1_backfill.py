#!/usr/bin/env python3
"""
duanxianxia_premarket_v7_1_backfill.py — v7.1 历史回放 harness

批量调用 v7.1 runner,汇总 setup_stats / none_ratio / top1。
D7 用该输出调阈值,暂不触碰线上 cron。
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from duanxianxia_premarket_v7_1_runner import run_v7_1, DEFAULT_PROJECT_ROOT

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _date_range(start: str, end: str) -> List[str]:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    out: List[str] = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _capture_date_exists(project_root: Path, d: str) -> bool:
    return (project_root / "captures" / d).exists()


def run_backfill(dates: List[str], project_root: Path, write_each: bool = False) -> Dict[str, Any]:
    runs: List[Dict[str, Any]] = []
    aggregate_stats: Dict[str, int] = {}
    errors: List[Dict[str, str]] = []

    for d in dates:
        if not _capture_date_exists(project_root, d):
            errors.append({"date": d, "error": "capture date missing"})
            continue
        try:
            result = run_v7_1(d, project_root, no_write=not write_each)
            stats = result.get("setup_stats") or {}
            for k, v in stats.items():
                if k == "none_ratio":
                    continue
                aggregate_stats[k] = aggregate_stats.get(k, 0) + int(v or 0)
            top = result.get("top_candidates") or []
            runs.append({
                "date": d,
                "candidate_count": result.get("meta", {}).get("candidate_count"),
                "setup_stats": stats,
                "top1": top[0] if top else None,
                "paths": result.get("paths"),
                "warnings": result.get("meta", {}).get("warnings", []),
            })
        except Exception as exc:  # noqa: BLE001
            errors.append({"date": d, "error": f"{type(exc).__name__}: {exc}"})

    total_candidates = sum(int(r.get("candidate_count") or 0) for r in runs)
    total_none = aggregate_stats.get("none", 0)
    none_ratio = round(total_none / total_candidates, 4) if total_candidates else 0
    non_empty_days = sum(1 for r in runs if r.get("top1"))

    return {
        "version": "premarket_v7_1_backfill",
        "generated_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
        "project_root": str(project_root),
        "dates_requested": dates,
        "run_count": len(runs),
        "error_count": len(errors),
        "total_candidates": total_candidates,
        "aggregate_setup_stats": aggregate_stats,
        "aggregate_none_ratio": none_ratio,
        "non_empty_days": non_empty_days,
        "runs": runs,
        "errors": errors,
        "acceptance_probe": {
            "none_ratio_le_0_80": none_ratio <= 0.80 if total_candidates else False,
            "has_A_or_A_ice": (aggregate_stats.get("A", 0) + aggregate_stats.get("A_ice", 0)) > 0,
            "has_B": aggregate_stats.get("B", 0) > 0,
            "has_E": aggregate_stats.get("E", 0) > 0,
            "all_days_have_top1": non_empty_days == len(runs) if runs else False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill v7.1 premarket analysis")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--dates", default="", help="comma-separated explicit dates")
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument("--write-each", action="store_true", help="also write per-day v7.1 analysis outputs")
    parser.add_argument("--output", default="", help="output json path; default reports/<end>/premarket_v7_1_backfill/<HHMMSS>.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    project_root = Path(args.project_root)
    if args.dates:
        dates = [x.strip() for x in args.dates.split(",") if x.strip()]
    elif args.start and args.end:
        dates = _date_range(args.start, args.end)
    else:
        raise SystemExit("Need --dates or --start/--end")

    summary = run_backfill(dates, project_root, write_each=args.write_each)

    if args.output:
        out_path = Path(args.output)
    else:
        end_date = dates[-1]
        out_dir = project_root / "reports" / end_date / "premarket_v7_1_backfill"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{datetime.now(TZ_SHANGHAI).strftime('%H%M%S')}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    summary["output_path"] = str(out_path)

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"v7.1 backfill done runs={summary['run_count']} errors={summary['error_count']} none_ratio={summary['aggregate_none_ratio']} output={out_path}")
        print(f"acceptance_probe={summary['acceptance_probe']}")
    return 0 if not summary["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
