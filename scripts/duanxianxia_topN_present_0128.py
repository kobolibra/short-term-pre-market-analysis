#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_topN_present_0128.py -- Task 0128 (final corrected Top5, read-only).

After Fix B (auction_change_pct raw[4]->raw[8] fallback) + Fix C (qiangchou
grab/qiangchou split), emit the corrected user-facing Top5 tables straight from
master.build_master_panel so ordering is evidence-based, not hand-stitched:
  1) baoliang  : auction.jjyd.vratio  Top5 by volume_ratio (+ auction_change_pct)
  2) qiangchou grab group   Top5 by grab_strength (+ auction_change_pct)
  3) qiangchou qiangchou grp Top5 by grab_strength_qiangchou (+ auction_change_pct)
Writes nothing to git; prints JSON.
"""
from __future__ import annotations
import json
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WS = Path(os.environ.get("DXX_WS", HERE.parent))
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

out = {"task": "0128_topN_present", "ok": True, "errors": []}
try:
    import duanxianxia_master_indicators as M
except Exception as e:  # noqa: BLE001
    out["ok"] = False
    out["errors"].append(f"import: {type(e).__name__}: {e}")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    sys.exit(0)

cap_root = WS / "projects" / "duanxianxia" / "captures"
target = os.environ.get("DXX_TARGET")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
if target:
    date_dir = cap_root / target
else:
    dates = sorted(p.name for p in cap_root.iterdir()
                   if p.is_dir() and DATE_RE.match(p.name)) if cap_root.is_dir() else []
    date_dir = cap_root / (dates[-1] if dates else "NONE")
out["date_dir"] = str(date_dir)


def _num(x):
    try:
        return float(x)
    except Exception:  # noqa: BLE001
        return float("-inf")


try:
    res = M.build_master_panel(date_dir)
    rows = list(res["panel"].values())

    def top5(metric):
        have = [r for r in rows if r.get(metric) is not None]
        have.sort(key=lambda r: _num(r.get(metric)), reverse=True)
        return [{"code": r.get("code"), "name": r.get("name"),
                 metric: r.get(metric),
                 "auction_change_pct": r.get("auction_change_pct"),
                 "latest_change_pct": r.get("latest_change_pct")} for r in have[:5]]

    out["baoliang_volume_ratio_top5"] = top5("volume_ratio")
    out["qiangchou_grab_top5"] = top5("grab_strength")
    out["qiangchou_qiangchou_top5"] = top5("grab_strength_qiangchou")
    out["n_codes"] = res["summary"]["n_codes"]
    out["coverage"] = {
        "volume_ratio": res["summary"]["coverage_pct"].get("volume_ratio"),
        "grab_strength": res["summary"]["coverage_pct"].get("grab_strength"),
        "grab_strength_qiangchou": res["summary"]["coverage_pct"].get("grab_strength_qiangchou"),
        "auction_change_pct": res["summary"]["coverage_pct"].get("auction_change_pct"),
    }
except Exception as e:  # noqa: BLE001
    out["ok"] = False
    out["errors"].append(f"panel: {type(e).__name__}: {e}")

print("=== TOP5 PRESENT 0128 ===")
print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
