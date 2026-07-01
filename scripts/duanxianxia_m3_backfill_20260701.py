#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_m3_backfill_20260701.py  --  v11 M3 历史回溯重导 + 重生成 CSV

走完 captures/<date>/ 所有日期，对每个有竞价数据的日期用 feature_builder
(canonical-first) 重派生特征行，汇聚成:
  1. projects/duanxianxia/_all_candidates_flat_v11.csv   (code/date/所有特征列)
  2. projects/duanxianxia/reports/_audit/m3_backfill_20260701.json (运行摘要)

禁止 sed 改历史，只从 raw 经 canonical 重导。pool.hot 无 raw → 无条目（不构造）。
本脚本 READ-ONLY except for writing the two output files. rc=0 = pass.
"""
from __future__ import annotations
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duanxianxia_feature_builder as FB

WS = Path.cwd()
PROJECT = WS / "projects" / "duanxianxia"
CAPTURES = PROJECT / "captures"
OUT_CSV = PROJECT / "_all_candidates_flat_v11.csv"
OUT_JSON = PROJECT / "reports" / "_audit" / "m3_backfill_20260701.json"

TZ = ZoneInfo("Asia/Shanghai")


def _date_dirs():
    if not CAPTURES.is_dir():
        return []
    return sorted(
        [d for d in CAPTURES.iterdir() if d.is_dir() and d.name[:4].isdigit()],
        key=lambda d: d.name,
    )


def _has_auction_data(date_dir: Path) -> bool:
    for dsid in FB.AUCTION_DATASETS:
        if (date_dir / dsid).is_dir():
            return True
    return False


# All output columns (ordered)
CSV_COLS = [
    "date", "code", "name",
    "free_float_mktcap", "free_float_mktcap_caliber",
    "bidAmount", "bidStrength",
    "volumeRatio", "grabStrength",
    "changeRate", "latestChangePct", "turnoverRate",
    "mainNetInflow", "mainNetInflowFull",
    "superLargeOrder", "largeOrder",
    "sealAmount", "boardLabel", "price", "concept",
    "source_hit_count", "source_hits",
]


def _safe_str(v):
    if v is None:
        return ""
    if isinstance(v, list):
        return "|".join(str(x) for x in v)
    return str(v)


def main():
    now = datetime.now(TZ).isoformat(timespec="seconds")
    all_dates = _date_dirs()
    eligible = [d for d in all_dates if _has_auction_data(d)]

    print(f"[M3] captures root: {CAPTURES}")
    print(f"[M3] total date dirs: {len(all_dates)}, with auction data: {len(eligible)}")

    if not eligible:
        summary = {
            "status": "NO_CAPTURES",
            "run_at": now,
            "n_dates": 0,
            "n_rows": 0,
        }
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
                            encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    all_rows = []
    per_date = []
    total_canonical_errors = 0

    for date_dir in eligible:
        date = date_dir.name
        try:
            res = FB.build_feature_table(date_dir)
        except Exception as e:
            per_date.append({"date": date, "status": "ERROR", "error": str(e)})
            print(f"[M3] {date} ERROR: {e}", file=sys.stderr)
            continue

        feats = res.get("features", [])
        coverage = res.get("coverage", {})
        date_errors = sum(c.get("canonical_error", 0) for c in coverage.values())
        total_canonical_errors += date_errors

        for f in feats:
            row = {col: _safe_str(f.get(col)) for col in CSV_COLS}
            row["date"] = date
            all_rows.append(row)

        per_date.append({
            "date": date,
            "status": "OK",
            "n_features": len(feats),
            "canonical_errors": date_errors,
            "coverage": {k: {"rows_in": v["rows_in"], "ok": v["canonical_ok"]}
                         for k, v in coverage.items()},
        })
        print(f"[M3] {date}: {len(feats)} features, {date_errors} canonical errors")

    # Write CSV
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLS)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"[M3] wrote {len(all_rows)} rows to {OUT_CSV}")

    # Write JSON summary
    summary = {
        "status": "OK",
        "run_at": now,
        "version": FB.VERSION,
        "n_dates_eligible": len(eligible),
        "n_dates_ok": sum(1 for p in per_date if p["status"] == "OK"),
        "n_dates_error": sum(1 for p in per_date if p["status"] == "ERROR"),
        "n_rows_total": len(all_rows),
        "total_canonical_errors": total_canonical_errors,
        "out_csv": str(OUT_CSV),
        "per_date": per_date,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    assert total_canonical_errors == 0 or total_canonical_errors < len(all_rows) * 0.05, \
        f"Too many canonical errors: {total_canonical_errors}/{len(all_rows)}"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
