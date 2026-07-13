#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_kqxy_scan.py  --  扫描所有历史交易日的 KQXY 值

扫描 captures 目录下所有日期的 home.qxlive.top_metrics 早盘快照，
提取 KQXY 指标值，输出到 reports/_audit/kqxy_scan.json。
"""

from __future__ import annotations

import json
import sys
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
CAPTURES_DIR = PROJECT_ROOT / "captures"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit"
QXLIVE_DS = "home.qxlive.top_metrics"
QXLIVE_CUTOFF = "093300"  # 只取 <=09:33 的早盘快照

_HHMMSS_FILE_PATTERN = re.compile(r"^(\d{6})\.json$")


def _extract_metric(rows: List[Dict[str, Any]], metric_key: str) -> Optional[float]:
    """从 qxlive top_metrics 行中提取指定指标值"""
    for row in (rows or []):
        key = str(row.get("metric_key") or "").strip()
        label = str(row.get("metric_label") or row.get("指标名称") or "").strip()
        if key == metric_key or metric_key in label:
            for vk in ("raw_chart_tail_value", "raw_value", "value", "指标值"):
                if vk in row:
                    try:
                        v = row.get(vk)
                        if v in (None, "", "-"):
                            return None
                        return float(str(v).replace("%", "").replace("亿", "").replace(",", "").strip())
                    except (ValueError, TypeError):
                        continue
    return None


def _list_capture_files(dir_path: Path) -> List[tuple]:
    """列出所有 capture 文件 (hhmmss, path)"""
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
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {
        "scan_time": date.today().isoformat(),
        "metric": "KQXY",
        "source": QXLIVE_DS,
        "cutoff": QXLIVE_CUTOFF,
        "days": [],
    }

    if not CAPTURES_DIR.exists():
        print(f"ERROR: captures dir not found: {CAPTURES_DIR}")
        return 1

    # 遍历所有日期目录
    date_dirs = sorted([d for d in CAPTURES_DIR.iterdir() if d.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", d.name)])
    print(f"Scanning {len(date_dirs)} date directories...")

    for dd in date_dirs:
        ds = dd.name
        qxlive_dir = dd / QXLIVE_DS
        files = _list_capture_files(qxlive_dir)

        if not files:
            continue

        # 取 <=09:33 的最早快照
        eligible = [(t, p) for (t, p) in files if t <= QXLIVE_CUTOFF]
        if not eligible:
            continue

        hhmmss, path = eligible[0]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        rows = data.get("rows", []) if isinstance(data, dict) else []
        if not isinstance(rows, list):
            rows = []

        kqxy = _extract_metric(rows, "KQXY")

        # 也提取 ZTBX, LBBX 作为参考
        ztbx = _extract_metric(rows, "ZTBX")
        lbbx = _extract_metric(rows, "LBBX")

        results["days"].append({
            "date": ds,
            "capture_time": hhmmss,
            "KQXY": kqxy,
            "ZTBX": ztbx,
            "LBBX": lbbx,
        })

        print(f"  {ds} @ {hhmmss}: KQXY={kqxy}, ZTBX={ztbx}, LBBX={lbbx}")

    # 统计
    kqxy_vals = [d["KQXY"] for d in results["days"] if d["KQXY"] is not None]
    if kqxy_vals:
        sorted_vals = sorted(kqxy_vals)
        n = len(sorted_vals)
        results["stats"] = {
            "n_days": len(results["days"]),
            "n_with_kqxy": len(kqxy_vals),
            "min": sorted_vals[0],
            "max": sorted_vals[-1],
            "mean": round(sum(kqxy_vals) / n, 2),
            "median": sorted_vals[n // 2] if n % 2 == 1 else round((sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2, 2),
            "p95": sorted_vals[int(n * 0.95)],
            "p80": sorted_vals[int(n * 0.80)],
            "p50": sorted_vals[int(n * 0.50)],
            "p20": sorted_vals[int(n * 0.20)],
            "distribution": {
                "<=0": sum(1 for v in kqxy_vals if v <= 0),
                "0-5": sum(1 for v in kqxy_vals if 0 < v <= 5),
                "5-10": sum(1 for v in kqxy_vals if 5 < v <= 10),
                "10-20": sum(1 for v in kqxy_vals if 10 < v <= 20),
                "20-30": sum(1 for v in kqxy_vals if 20 < v <= 30),
                "30-50": sum(1 for v in kqxy_vals if 30 < v <= 50),
                ">50": sum(1 for v in kqxy_vals if v > 50),
            }
        }
        print(f"\n=== KQXY Stats (n={len(kqxy_vals)}) ===")
        print(f"  min={sorted_vals[0]}, max={sorted_vals[-1]}, mean={round(sum(kqxy_vals)/n,2)}")
        print(f"  median={results['stats']['median']}, p80={results['stats']['p80']}, p95={results['stats']['p95']}")
        print(f"  distribution: {results['stats']['distribution']}")

    out_path = OUTPUT_DIR / "kqxy_scan.json"
    out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\nDone. Output: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())