#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_ztpool_full_inspect.py  --  全面检查 home.ztpool 和 review.daily 的晋级率数据结构
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

_HHMMSS_FILE_PATTERN = re.compile(r"^(\d{6})\.json$")


def _list_files(dir_path: Path) -> List[tuple]:
    if not dir_path.exists() or not dir_path.is_dir():
        return []
    out = []
    for p in sorted(dir_path.iterdir()):
        m = _HHMMSS_FILE_PATTERN.match(p.name)
        if m:
            out.append((m.group(1), p))
    out.sort(key=lambda x: x[0])
    return out


def inspect_ztpool(date_dirs: List[Path]) -> dict:
    """检查 home.ztpool 中每个日期的晋级率分层"""
    results = {}
    for dd in date_dirs:
        ds = dd.name
        ztpool_dir = dd / "home.ztpool"
        files = _list_files(ztpool_dir)
        if not files:
            continue
        hhmmss, path = files[-1]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        rows = data.get("rows", []) if isinstance(data, dict) else []
        meta = data.get("meta", {}) if isinstance(data, dict) else {}

        # 从 rows 中提取 ladder_group + promo_rate
        ladder_rates = {}
        for row in (rows or []):
            if not isinstance(row, dict):
                continue
            ladder = str(row.get("ladder_group") or row.get("分组名称") or "").strip()
            promo = row.get("promo_rate") or row.get("晋级率")
            if not ladder or promo is None:
                continue
            try:
                val = float(str(promo).replace("%", "").strip())
            except (ValueError, TypeError):
                continue
            if ladder not in ladder_rates:
                ladder_rates[ladder] = val

        # 从 meta.groups 中提取（如果存在）
        meta_groups = meta.get("groups", [])
        meta_rates = {}
        for g in (meta_groups or []):
            if not isinstance(g, dict):
                continue
            name = str(g.get("name") or g.get("group") or g.get("分组名称") or "").strip()
            promo = g.get("promo_rate") or g.get("晋级率") or g.get("rate")
            if not name or promo is None:
                continue
            try:
                val = float(str(promo).replace("%", "").strip())
            except (ValueError, TypeError):
                continue
            meta_rates[name] = val

        results[ds] = {
            "capture_time": hhmmss,
            "n_rows": len(rows) if rows else 0,
            "rows_keys": list(rows[0].keys()) if rows and isinstance(rows[0], dict) else [],
            "ladder_rates_from_rows": ladder_rates,
            "meta_keys": list(meta.keys()),
            "meta_groups": meta_rates,
        }

    return results


def inspect_review_daily(date_dirs: List[Path]) -> dict:
    """检查 review.daily.top_metrics 中每个日期的晋级率分层"""
    results = {}
    for dd in date_dirs:
        ds = dd.name
        rd_dir = dd / "review.daily.top_metrics"
        files = _list_files(rd_dir)
        if not files:
            continue
        hhmmss, path = files[-1]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        rows = data.get("rows", []) if isinstance(data, dict) else []
        meta = data.get("meta", {}) if isinstance(data, dict) else {}

        # 提取 PBBX 相关行（晋级率）
        pbbx_rows = []
        for row in (rows or []):
            if not isinstance(row, dict):
                continue
            mk = str(row.get("metric_key") or "").strip()
            if "PBBX" in mk or "晋级" in mk or "连板" in str(row.get("metric_label", "")):
                pbbx_rows.append(row)

        results[ds] = {
            "n_rows": len(rows) if rows else 0,
            "pbbx_rows": pbbx_rows,
            "meta_keys": list(meta.keys()),
        }

    return results


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not CAPTURES_DIR.exists():
        print(f"ERROR: captures dir not found: {CAPTURES_DIR}")
        return 1

    date_dirs = sorted([d for d in CAPTURES_DIR.iterdir() if d.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", d.name)])
    print(f"Found {len(date_dirs)} date directories")

    # ====== 1. 检查 home.ztpool ======
    print("\n" + "=" * 80)
    print("=== home.ztpool 晋级率数据结构 ===")
    print("=" * 80)
    ztpool_results = inspect_ztpool(date_dirs)
    for ds, info in sorted(ztpool_results.items()):
        print(f"\n--- {ds} @ {info['capture_time']} ---")
        print(f"  rows keys: {info['rows_keys']}")
        print(f"  meta keys: {info['meta_keys']}")
        lr = info['ladder_rates_from_rows']
        mr = info['meta_groups']
        if lr:
            print(f"  rows 晋级率: {lr}")
        if mr:
            print(f"  meta 晋级率: {mr}")
        if not lr and not mr:
            print(f"  ⚠️ NO 晋级率 DATA FOUND!")

    # ====== 2. 检查 review.daily.top_metrics ======
    print("\n" + "=" * 80)
    print("=== review.daily.top_metrics 晋级率数据结构 ===")
    print("=" * 80)
    rd_results = inspect_review_daily(date_dirs)
    for ds, info in sorted(rd_results.items()):
        print(f"\n--- {ds} ---")
        print(f"  n_rows: {info['n_rows']}")
        print(f"  meta_keys: {info['meta_keys']}")
        if info['pbbx_rows']:
            for r in info['pbbx_rows']:
                print(f"    {r.get('metric_key','')}: {r.get('value','')} | {r.get('metric_label','')}")
        else:
            print(f"  ⚠️ NO PBBX/晋级率 rows found!")

    # ====== 3. 统计汇总 ======
    print("\n" + "=" * 80)
    print("=== 汇总统计 ===")
    print("=" * 80)

    # 统计 ztpool 中有哪些 ladder_group
    all_ladders = set()
    days_with_data = 0
    days_with_1j2 = 0
    days_with_2j3 = 0
    days_with_3j4 = 0
    days_with_4p = 0
    days_with_shouban = 0

    for ds, info in sorted(ztpool_results.items()):
        lr = info['ladder_rates_from_rows']
        mr = info['meta_groups']
        merged = {**mr, **lr}  # meta 优先
        if not merged:
            continue
        days_with_data += 1
        for ladder in merged:
            all_ladders.add(ladder)
        if any("1进2" in k for k in merged):
            days_with_1j2 += 1
        if any("2进3" in k for k in merged):
            days_with_2j3 += 1
        if any("3进4" in k for k in merged):
            days_with_3j4 += 1
        if any("4" in k and ("进" in k or "板" in k) for k in merged):
            days_with_4p += 1
        if any("首板" in k or "1板" in k for k in merged):
            days_with_shouban += 1

    print(f"总日期数: {len(date_dirs)}")
    print(f"有 ztpool 晋级率数据的天数: {days_with_data}")
    print(f"所有 ladder_group 名称: {sorted(all_ladders)}")
    print(f"含 1进2 的天数: {days_with_1j2}")
    print(f"含 2进3 的天数: {days_with_2j3}")
    print(f"含 3进4 的天数: {days_with_3j4}")
    print(f"含 4板+ 的天数: {days_with_4p}")
    print(f"含 首板 的天数: {days_with_shouban}")

    # 统计 review.daily 中有 PBBX 的天数
    rd_days = 0
    for ds, info in sorted(rd_results.items()):
        if info['pbbx_rows']:
            rd_days += 1
    print(f"review.daily 有 PBBX 晋级率的天数: {rd_days}")

    return 0


if __name__ == "__main__":
    sys.exit(main())