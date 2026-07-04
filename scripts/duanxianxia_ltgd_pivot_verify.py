#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_ltgd_pivot_verify.py -- Item A verify (job 0142, additive, read-only).

验证 pit_panel._load_map 对 review.ltgd.range 的多窗口 pivot 修复。
0141 实测: 同一张 capture raw 80 行 / 55 codes, 旧 last-wins 丢 25 行、幸存窗口随机。
本探针在同一张 capture 上跑【新】_load_map, 断言:
  - merged code 数 == raw distinct codes (55)
  - 窗口字段总数 == raw 行数 (80) => rows_lost == 0
  - base range_period == 每 code 最短可用窗口 (确定性)
只读; 不写 git。
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WS = HERE.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from duanxianxia_canonical_routing import canonicalize_row
from duanxianxia_master_indicators import _rows_of, _row_code, _norm_code
from duanxianxia_pit_panel import _load_map, LTGD_DS, LTGD_WINDOW_ORDER

PERIOD_KEY = "周期"
CODE_KEY = "代码"


def _latest_capture(root):
    files = sorted(root.glob("20*-*-*/review.ltgd.range/*.json"),
                   key=lambda p: (p.parent.parent.name, p.stem))
    return files[-1] if files else None


def main():
    root = WS / "projects" / "duanxianxia" / "captures"
    out = {"task": "0142_ltgd_pivot_verify", "dataset": LTGD_DS,
           "captures_root": str(root)}
    chosen = _latest_capture(root)
    if chosen is None:
        out["error"] = "no_capture"
        print(json.dumps(out, ensure_ascii=False, indent=1))
        return 1
    out["chosen_capture"] = str(chosen.relative_to(root))

    payload = json.loads(chosen.read_text(encoding="utf-8"))
    raw_rows = [r for r in _rows_of(payload) if isinstance(r, dict)]
    raw_codes = set()
    raw_period_dist = {}
    for r in raw_rows:
        c = _row_code(r) or _norm_code(r.get(CODE_KEY))
        if c:
            raw_codes.add(c)
        p = str(r.get(PERIOD_KEY))
        raw_period_dist[p] = raw_period_dist.get(p, 0) + 1
    out["raw_row_count"] = len(raw_rows)
    out["raw_distinct_codes"] = len(raw_codes)
    out["raw_period_distribution"] = raw_period_dist

    merged, errs = _load_map(chosen.parent, LTGD_DS, chosen, canonicalize_row)
    out["canonical_err"] = errs
    out["merged_code_count"] = len(merged)

    total_window_fields = 0
    prim_dist = {}
    win_n_dist = {}
    base_ok = True
    for code, m in merged.items():
        wins = m.get("range_windows", [])
        total_window_fields += len(wins)
        win_n_dist[str(len(wins))] = win_n_dist.get(str(len(wins)), 0) + 1
        prim = m.get("range_primary_window")
        prim_dist[str(prim)] = prim_dist.get(str(prim), 0) + 1
        exp = next((w for w in LTGD_WINDOW_ORDER if w in wins), None)
        if prim != exp:
            base_ok = False
    out["total_window_fields"] = total_window_fields
    out["rows_lost"] = len(raw_rows) - total_window_fields
    out["per_code_window_count_dist"] = win_n_dist
    out["primary_window_dist"] = prim_dist
    out["base_is_shortest_window"] = base_ok

    sample = {}
    for code, m in merged.items():
        if m.get("range_windows_n", 0) >= 3:
            sample[code] = {k: v for k, v in m.items()
                            if k.startswith("range_") or k == "name"}
        if len(sample) >= 3:
            break
    out["sample_multiwindow"] = sample

    out["collapse_fixed"] = bool(
        out["rows_lost"] == 0
        and out["merged_code_count"] == out["raw_distinct_codes"]
        and base_ok
    )
    print(json.dumps(out, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
