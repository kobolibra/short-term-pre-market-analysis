#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_pit_fix_validate_0123.py -- Task 0123 (read-only).

验证 0120 off-by-one 修复: 盘前 as-of 下, 隔夜复盘表(fupan/ltgd) 的内容日期
应等于 T-1(prior), 而不是旧逻辑的 T-2。
同时对比 OLD(按文件夹 prior 取最后快照) vs NEW(按内容日期解析) 的 streak。
只读。
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from duanxianxia_pit_panel import (  # noqa: E402
    build_pit_panel, _all_folders, _prior_trading_day,
    _resolve_overnight, _content_date_of, _pick_snapshot,
)
from duanxianxia_master_indicators import _rows_of, _row_code, _norm_code  # noqa: E402
from duanxianxia_canonical_routing import canonicalize_row  # noqa: E402

WS = Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace"))
CAP = WS / "projects" / "duanxianxia" / "captures"
CHECK = ["605488", "605189", "600641", "603137", "002979", "002396"]


def _streak_from(chosen, ds):
    if chosen is None:
        return {}
    try:
        payload = json.loads(chosen.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        return {"_err": str(e)}
    out = {}
    for row in _rows_of(payload):
        rc = _row_code(row)
        if not rc:
            continue
        code = _norm_code(rc)
        if code in CHECK:
            c = canonicalize_row(ds, row) or {}
            out[code] = {"streak": c.get("streak"), "interval_period": c.get("interval_period"),
                         "interval_change": c.get("interval_change")}
    return out


def main():
    folders = _all_folders(CAP)
    if not folders:
        print(json.dumps({"error": "no folders"}))
        return 0
    today = folders[-1]
    prior = _prior_trading_day(CAP, today)
    out = {"job": "0123_pit_fix_validate", "today": today, "prior(T-1)": prior}

    res = build_pit_panel(CAP, today, as_of_slot="premarket", cutoff="09:29")
    plan = res["summary"]["plan"]
    panel = res["panel"]
    out["NEW_plan"] = {ds: plan.get(ds) for ds in ("review.fupan.plate", "review.ltgd.range")}

    # NEW content date via resolver
    fx, fb, fcd = _resolve_overnight(CAP, "review.fupan.plate", prior, folders)
    lx, lb, lcd = _resolve_overnight(CAP, "review.ltgd.range", prior, folders)
    out["NEW_resolved"] = {
        "fupan": {"batch": fb, "content_date": fcd, "streak": _streak_from(fx, "review.fupan.plate")},
        "ltgd": {"batch": lb, "content_date": lcd, "streak": _streak_from(lx, "review.ltgd.range")},
    }

    # OLD logic: folder=prior, last snapshot
    old_fu, _ = _pick_snapshot(CAP / prior / "review.fupan.plate", None)
    old_lt, _ = _pick_snapshot(CAP / prior / "review.ltgd.range", None)
    old_fu_cd = _content_date_of(json.loads(old_fu.read_text(encoding="utf-8"))) if old_fu else None
    out["OLD_logic"] = {
        "fupan": {"batch": f"{prior} {old_fu.stem}" if old_fu else None,
                  "content_date": old_fu_cd, "streak": _streak_from(old_fu, "review.fupan.plate")},
        "ltgd": {"batch": f"{prior} {old_lt.stem}" if old_lt else None,
                 "streak": _streak_from(old_lt, "review.ltgd.range")},
    }

    # panel streak for check codes (final)
    out["panel_streak"] = {c: {"name": panel.get(c, {}).get("name"),
                               "streak": panel.get(c, {}).get("streak"),
                               "streak__batch": panel.get(c, {}).get("streak__batch"),
                               "streak__lag": panel.get(c, {}).get("streak__lag"),
                               "board_label": panel.get(c, {}).get("board_label"),
                               "board_label__batch": panel.get(c, {}).get("board_label__batch")}
                          for c in CHECK}

    out["_summary"] = {
        "fupan_new_content_date": fcd, "fupan_old_content_date": old_fu_cd,
        "fix_ok(new==T-1)": fcd == prior,
        "old_was_stale(old<T-1)": (old_fu_cd is not None and prior is not None and old_fu_cd < prior),
        "fupan_used": plan.get("review.fupan.plate", {}).get("used"),
        "ltgd_used": plan.get("review.ltgd.range", {}).get("used"),
    }
    print("=== PIT FIX VALIDATE (Task 0123) ===")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
