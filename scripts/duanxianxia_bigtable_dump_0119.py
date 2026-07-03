#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_bigtable_dump_0119.py -- Task 0119 (read-only).

用途: 把“大表”(PIT 面板)的表头字段 + 今天真实样例行打出来供人工验证。
  - 表头 = DATASETS(19 张源表, 带 scope/canonical) + INDICATORS(列头)。
  - 今天两个视角: premarket(决策时, 无未来函数) + postmarket(当日全量)。
  - 每个视角取字段最齐的 2 行真实数据, 并附最优行的逐字段来源(src/lag/batch)。
只读; 不写 git。用法: python3 scripts/duanxianxia_bigtable_dump_0119.py [YYYY-MM-DD]
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import duanxianxia_master_indicators as M  # noqa: E402
from duanxianxia_pit_panel import build_pit_panel  # noqa: E402

WS = Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace"))
CAP = WS / "projects" / "duanxianxia" / "captures"


def _dates():
    if not CAP.is_dir():
        return []
    return sorted(p.name for p in CAP.iterdir()
                  if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-")


def _completeness(row, headers):
    return sum(1 for h in headers if row.get(h) is not None)


def dump_panel(date, as_of, headers):
    res = build_pit_panel(CAP, date, as_of_slot=as_of, cutoff="09:29")
    summ = res.get("summary") or {}
    panel = res.get("panel") or {}
    rows = sorted(panel.values(), key=lambda r: _completeness(r, headers), reverse=True)
    top = rows[:2]
    samples = []
    for r in top:
        vals = {"code": r.get("code"), "name": r.get("name"),
                "_nonnull_fields": _completeness(r, headers)}
        for h in headers:
            v = r.get(h)
            if v is not None:
                vals[h] = v
        samples.append(vals)
    prov = {}
    if top:
        best = top[0]
        for h in headers:
            if best.get(h) is not None:
                prov[h] = {"value": best.get(h), "src": best.get(h + "__src"),
                           "lag": best.get(h + "__lag"), "batch": best.get(h + "__batch")}
    return {
        "as_of": as_of,
        "universe_size": summ.get("universe_size"),
        "today_live_tables": summ.get("today_live_tables"),
        "t1_eod_tables": summ.get("t1_eod_tables"),
        "coverage_pct": summ.get("coverage_pct"),
        "sample_rows_values": samples,
        "best_row_provenance": prov,
    }


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    dates = _dates()
    date = args[0] if args else (dates[-1] if dates else None)
    headers = list(M.INDICATORS.keys())
    out = {
        "job": "0119_bigtable_dump",
        "captures_root": str(CAP),
        "available_dates": dates[-8:],
        "date": date,
        "n_datasets": len(M.DATASETS),
        "datasets": {ds: {"scope": m.get("scope"), "canonical": m.get("canonical")}
                     for ds, m in M.DATASETS.items()},
        "n_indicator_headers": len(headers),
        "indicator_headers": headers,
    }
    if not date:
        out["error"] = "no captures dates found"
        print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
        return 1
    out["panels"] = {}
    for as_of in ("postmarket", "premarket"):
        try:
            out["panels"][as_of] = dump_panel(date, as_of, headers)
        except Exception as e:  # noqa: BLE001
            out["panels"][as_of] = {"error": "%s: %s" % (type(e).__name__, e)}
    out["ok"] = any(isinstance(p, dict) and p.get("sample_rows_values")
                    for p in out["panels"].values())
    print("=== DUANXIANXIA BIG-TABLE DUMP (Task 0119) ===")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    return 0 if out["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
