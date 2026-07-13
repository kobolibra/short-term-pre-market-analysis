#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_all_metrics_scan.py  --  扫描所有历史交易日的 qxlive top_metrics 全部 12 个指标
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
QXLIVE_CUTOFF = "093300"

_HHMMSS_FILE_PATTERN = re.compile(r"^(\d{6})\.json$")

METRIC_DEFS = [
    ("QX", "number", "情绪指标"),
    ("ZT", "number", "涨停家数"),
    ("DT", "number", "跌停家数"),
    ("KQXY", "number", "亏钱效应"),
    ("HSLN", "signed", "主力流入"),
    ("LBGD", "number", "连板高度"),
    ("SZ", "number", "上涨家数"),
    ("XD", "number", "下跌家数"),
    ("PB", "percent", "今日封板率"),
    ("ZTBX", "percent", "昨涨停表现"),
    ("LBBX", "percent", "昨连板表现"),
    ("PBBX", "number", "沪深5分钟量能"),
]


def _extract_metric(rows: List[Dict[str, Any]], metric_key: str) -> Optional[float]:
    for row in (rows or []):
        key = str(row.get("metric_key") or "").strip()
        if key == metric_key:
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
    if not dir_path.exists() or not dir_path.is_dir():
        return []
    out = []
    for p in sorted(dir_path.iterdir()):
        m = _HHMMSS_FILE_PATTERN.match(p.name)
        if m:
            out.append((m.group(1), p))
    out.sort(key=lambda x: x[0])
    return out


def stats(values: List[float]) -> dict:
    if not values:
        return {"n": 0}
    sv = sorted(values)
    n = len(sv)
    return {
        "n": n,
        "min": sv[0],
        "max": sv[-1],
        "mean": round(sum(sv) / n, 2),
        "median": sv[n // 2] if n % 2 else round((sv[n // 2 - 1] + sv[n // 2]) / 2, 2),
        "zero_count": sum(1 for v in sv if v == 0),
        "nonzero_count": sum(1 for v in sv if v != 0),
        "p80": sv[int(n * 0.80)],
        "p95": sv[int(n * 0.95)],
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Any] = {
        "scan_time": date.today().isoformat(),
        "source": QXLIVE_DS,
        "cutoff": QXLIVE_CUTOFF,
        "metrics": {},
    }

    if not CAPTURES_DIR.exists():
        print(f"ERROR: captures dir not found: {CAPTURES_DIR}")
        return 1

    date_dirs = sorted([d for d in CAPTURES_DIR.iterdir() if d.is_dir() and re.match(r"^\d{4}-\d{2}-\d{2}$", d.name)])
    print(f"Scanning {len(date_dirs)} date directories...")

    # Collect all metrics per day
    all_metrics: Dict[str, Dict[str, List[float]]] = {}
    days = []

    for dd in date_dirs:
        ds = dd.name
        qxlive_dir = dd / QXLIVE_DS
        files = _list_capture_files(qxlive_dir)
        if not files:
            continue
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

        day_vals = {}
        for mk, vt, ml in METRIC_DEFS:
            val = _extract_metric(rows, mk)
            day_vals[mk] = val
            if mk not in all_metrics:
                all_metrics[mk] = {"label": ml, "type": vt, "values": []}
            if val is not None:
                all_metrics[mk]["values"].append(val)
        days.append({"date": ds, "capture_time": hhmmss, **day_vals})

    # Print summary
    print(f"\n{'='*80}")
    print(f"{'Metric':12s} {'Label':14s} {'Type':8s} {'N':>5s} {'Zero%':>8s} {'Min':>10s} {'Max':>10s} {'Mean':>10s} {'Median':>10s} {'P80':>10s} {'P95':>10s}")
    print(f"{'='*80}")

    for mk, meta in all_metrics.items():
        vals = meta["values"]
        s = stats(vals)
        zero_pct = f"{s['zero_count']/s['n']*100:.0f}%" if s["n"] > 0 else "N/A"
        print(f"{mk:12s} {meta['label']:14s} {meta['type']:8s} {s['n']:>5d} {zero_pct:>8s} {s['min']:>10.2f} {s['max']:>10.2f} {s['mean']:>10.2f} {s['median']:>10.2f} {s['p80']:>10.2f} {s['p95']:>10.2f}")

    # Print daily table
    print(f"\n{'='*120}")
    header = f"{'Date':>12s} {'Time':6s} " + " ".join(f"{mk:>8s}" for mk, _, _ in METRIC_DEFS)
    print(header)
    print("-" * 120)
    for d in days:
        row = f"{d['date']:>12s} {d['capture_time']:6s} "
        for mk, _, _ in METRIC_DEFS:
            v = d.get(mk)
            if v is None:
                row += f"{'null':>8s}"
            else:
                row += f"{v:>8.2f}"
        print(row)

    # Save
    results["metrics"] = {mk: {**meta, "stats": stats(meta["values"])} for mk, meta in all_metrics.items()}
    out_path = OUTPUT_DIR / "all_metrics_scan.json"
    out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\nDone. Output: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())