#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_overnight_timing_diag_0120.py -- Task 0120 (read-only).

回答: 隔夜/盘后表(fupan/cashflow/ltgd/daily/ztpool)到底什么时间落盘,
以及它们内部描述的是哪个交易日的状态(vs 它们被归档到的日期文件夹)。
证据: 每张表各日快照的 HHMMSS + 内部日期字段 + 目标股原始行 +
  日线涨停序列, 让“昨收盘态 vs 今竞价态”一目了然。
只读; 不写 git。
"""
from __future__ import annotations
import csv
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from duanxianxia_master_indicators import _rows_of, _row_code, _norm_code  # noqa: E402

WS = Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace"))
ROOT = WS / "projects" / "duanxianxia"
CAP = ROOT / "captures"
DAILY = ROOT / "dailyline" / "stocks"

OVERNIGHT = ["review.fupan.plate", "review.ltgd.range", "review.daily.top_metrics",
             "cashflow.stock.today", "cashflow.stock.3day", "cashflow.stock.5day",
             "cashflow.stock.10day", "home.ztpool"]
ROW_TABLES = ["review.fupan.plate", "cashflow.stock.today", "home.ztpool", "review.ltgd.range"]
DATE_HINT_KEYS = ["date", "trade_date", "tradedate", "tradingday", "day", "asof", "as_of",
                  "datetime", "update_time", "updatetime", "日期", "交易日", "更新时间", "时间"]


def _dates():
    if not CAP.is_dir():
        return []
    return sorted(p.name for p in CAP.iterdir()
                  if p.is_dir() and len(p.name) == 10 and p.name[4] == "-")


def _snaps(dd):
    return sorted(p.name for p in dd.glob("*.json")) if dd.is_dir() else []


def _snap_summary(dd):
    s = _snaps(dd)
    if not s:
        return {"count": 0}
    return {"count": len(s), "first": s[0], "last": s[-1]}


def _scan_hints(obj, depth=0, found=None):
    if found is None:
        found = {}
    if depth > 3 or len(found) > 8:
        return found
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (str, int, float)) and str(k).lower() in DATE_HINT_KEYS:
                found.setdefault(str(k), v)
            elif isinstance(v, (dict, list)):
                _scan_hints(v, depth + 1, found)
    elif isinstance(obj, list):
        for it in obj[:3]:
            _scan_hints(it, depth + 1, found)
    return found


def _find_row(payload, code):
    tgt = _norm_code(code)
    try:
        for row in _rows_of(payload):
            rc = _row_code(row)
            if rc == tgt or (rc and _norm_code(rc) == tgt):
                return row
    except Exception as e:  # noqa: BLE001
        return {"_iter_error": str(e)}
    return None


def _last_payload(dd):
    s = _snaps(dd)
    if not s:
        return None, None
    f = dd / s[-1]
    try:
        return json.loads(f.read_text(encoding="utf-8")), s[-1]
    except Exception as e:  # noqa: BLE001
        return {"_read_error": str(e)}, s[-1]


def daily_rows(code, dates):
    f = DAILY / (str(code).zfill(6) + ".csv")
    if not f.exists():
        return {"error": "no dailyline csv", "path": str(f)}
    res = {}
    try:
        with open(f, newline="") as fh:
            for r in csv.DictReader(fh):
                if r.get("date") in dates:
                    res[r["date"]] = {k: r.get(k) for k in
                                      ("open", "high", "low", "close", "preclose",
                                       "pctChg", "turn", "tradestatus") if k in r}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}
    return res


def main():
    dates = _dates()
    tail = dates[-3:]
    code = "002979"
    out = {"job": "0120_overnight_timing_diag", "today": dates[-1] if dates else None,
           "recent_date_folders": tail, "captures_root": str(CAP),
           "explain": "snapshot HHMMSS=落盘时刻; hints=文件内部日期字段; "
                      "target_row=该股原始行(看 streak/状态到底是哪天)"}
    # 1) 落盘时刻 + 内部日期 hint
    snap = {}
    for ds in OVERNIGHT:
        per = {}
        for d in tail:
            dd = CAP / d / ds
            summ = _snap_summary(dd)
            if summ.get("count"):
                payload, fn = _last_payload(dd)
                summ["internal_date_hints"] = _scan_hints(payload) if isinstance(payload, (dict, list)) else {}
            per[d] = summ
        snap[ds] = per
    out["landing_time_and_hints"] = snap
    # 2) 目标股原始行 (跨最近日期文件夹)
    rowdump = {}
    for ds in ROW_TABLES:
        per = {}
        for d in tail:
            payload, fn = _last_payload(CAP / d / ds)
            if isinstance(payload, (dict, list)):
                r = _find_row(payload, code)
                if r is not None:
                    per[d] = {"file": fn, "raw_row": r}
        rowdump[ds] = per
    out["target_%s_raw" % code] = rowdump
    # 3) 今日竞价 weimai 原始行 (board_label 来源)
    if dates:
        payload, fn = _last_payload(CAP / dates[-1] / "auction.jjyd.weimai")
        if isinstance(payload, (dict, list)):
            out["weimai_today_%s" % code] = {"file": fn, "raw_row": _find_row(payload, code)}
    # 4) 日线涨停序列 (决定性证据: 昨收盘 vs 前天)
    dl_dates = sorted(set(tail + ([dates[-1]] if dates else [])))
    out["dailyline_%s" % code] = daily_rows(code, dl_dates)
    out["dailyline_002396"] = daily_rows("002396", dl_dates)
    print("=== OVERNIGHT TIMING DIAG (Task 0120) ===")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
