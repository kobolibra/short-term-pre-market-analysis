#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_indicator_listing_0157.py -- Task 0157.

用指定交易日(默认今天, Asia/Shanghai)的盘前竞价原始 capture,
经 canonical -> feature_builder -> indicator_builder, 列出 D1-D6 各维度指标。
真实数据驱动; 若 captures/<date>/ 不存在或为空, 如实报告目录状态, 不臆造。
用法: python3 scripts/duanxianxia_indicator_listing_0157.py [YYYY-MM-DD] [TOPN]
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
import duanxianxia_feature_builder as fb
import duanxianxia_indicator_builder as ib


def _today_shanghai() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _f(x, scale=1.0, nd=3):
    if x is None:
        return "-"
    try:
        return format(x / scale, "." + str(nd) + "f")
    except Exception:
        return str(x)


def _pct(x, nd=3):
    if x is None:
        return "-"
    try:
        return format(x * 100.0, "." + str(nd) + "f")
    except Exception:
        return str(x)


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else _today_shanghai()
    topn = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    root = Path(DEFAULT_PROJECT_ROOT)
    cap_root = root / "captures"
    date_dir = cap_root / date

    diag = {
        "job": "0157_indicator_listing",
        "date": date,
        "captures_root_exists": cap_root.is_dir(),
        "date_dir_exists": date_dir.is_dir(),
        "date_dir_listing": sorted(p.name for p in date_dir.iterdir()) if date_dir.is_dir() else [],
        "captures_root_tail": sorted(p.name for p in cap_root.iterdir())[-15:] if cap_root.is_dir() else [],
    }

    result = None
    if date_dir.is_dir():
        try:
            ft = fb.build_feature_table(date_dir)
            result = ib.build_indicators(ft)
            diag["coverage_by_source"] = ft.get("coverage")
            diag["capture_meta"] = ft.get("capture_meta")
        except Exception as e:
            diag["build_error"] = repr(e)

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    out_path = audit / ("indicator_listing_" + date + "_0157.json")
    out_path.write_text(json.dumps({"diag": diag, "result": result},
                                   ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")

    # ---- compact output LAST (worker keeps stdout tail) ----
    lines = []
    if not result or not result.get("indicators"):
        lines.append("NO_FEATURES date=" + date + " date_dir_exists=" + str(diag["date_dir_exists"]))
        if diag.get("build_error"):
            lines.append("build_error=" + diag["build_error"])
        lines.append("date_dir_listing=" + json.dumps(diag["date_dir_listing"], ensure_ascii=False))
        lines.append("captures_root_tail=" + json.dumps(diag["captures_root_tail"], ensure_ascii=False))
        print(chr(10).join(lines))
        return 0

    rows = sorted(
        result["indicators"],
        key=lambda r: (r.get("d1_auction_amount_pct") is not None, r.get("d1_auction_amount_pct") or 0.0),
        reverse=True,
    )
    cov = result["coverage"]
    lines.append("=== D1-D6 指标清单 " + date + " (top " + str(topn) + " by 竞价量能占比) ===")
    lines.append("列: code name | D1 量能占比% 量比 | D2 换手% | D3 资金合计亿 主力净额亿 | D4 承接% 真封单亿 | D5 竞价涨幅% 委买强度%")
    for r in rows[:topn]:
        cell = [
            str(r.get("code")) + " " + (str(r.get("name") or ""))[:6],
            "D1 " + _pct(r.get("d1_auction_amount_pct")) + " " + _f(r.get("d1_volume_ratio"), nd=2),
            "D2 " + _f(r.get("d2_turnover_rate"), nd=2),
            "D3 " + _f(r.get("d3_money"), 1e8) + " " + _f(r.get("d3_main_net_inflow"), 1e8),
            "D4 " + _pct(r.get("d4_pressure_score")) + " " + _f(r.get("d4_true_seal"), 1e8),
            "D5 " + _f(r.get("d5_auction_change_pct"), nd=2) + " " + _pct(r.get("d5_weimai_strength")),
        ]
        lines.append(" | ".join(cell))
    lines.append("=== coverage missing_rate ===")
    lines.append(json.dumps({k: cov[k]["missing_rate"] for k in cov}, ensure_ascii=False))
    if result.get("warnings"):
        lines.append("=== warnings === " + json.dumps(result["warnings"], ensure_ascii=False))
    lines.append(
        "n_rows=" + str(result["n_rows"]) + " date=" + date
        + " ver=" + str(result["version"]) + "/" + str(result.get("feature_version"))
        + " out=" + out_path.name
    )
    print(chr(10).join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
