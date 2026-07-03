#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_hardcase_validation_0121.py -- Task 0121 (read-only).

自动挖出今日真实数据中最难验证的样本, 专打尚未坐实的不确定点:
 (A) fupan 文件夹日期 vs 内容日期错位 (streak 跨文件夹变化的股)
 (B) cashflow.today 盘中实时 vs 昨日终值 (首快照 vs 末快照 vs 昨文件夹)
 (C) 今日竞价 board_label vs 昨收 streak 冲突
 (D) ltgd 多窗口覆盖 (同一 code 多个 range_period)
 (E) fengdan 封单竞价额为负/缺失
只读。
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
from duanxianxia_canonical_routing import canonicalize_row  # noqa: E402

WS = Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace"))
ROOT = WS / "projects" / "duanxianxia"
CAP = ROOT / "captures"
DAILY = ROOT / "dailyline" / "stocks"


def _dates():
    if not CAP.is_dir():
        return []
    return sorted(p.name for p in CAP.iterdir()
                  if p.is_dir() and len(p.name) == 10 and p.name[4] == "-")


def _snaps(dd):
    return sorted(p.name for p in dd.glob("*.json")) if dd.is_dir() else []


def _load(dd, which="last"):
    s = _snaps(dd)
    if not s:
        return None, None
    fn = s[-1] if which == "last" else s[0]
    try:
        return json.loads((dd / fn).read_text(encoding="utf-8")), fn
    except Exception as e:  # noqa: BLE001
        return {"_err": str(e)}, fn


def _canon(ds, row):
    try:
        return canonicalize_row(ds, row) or {}
    except Exception as e:  # noqa: BLE001
        return {"_canon_err": str(e)}


def _index(payload, ds):
    out = {}
    if not isinstance(payload, (dict, list)):
        return out
    try:
        for row in _rows_of(payload):
            rc = _row_code(row)
            if not rc:
                continue
            out[_norm_code(rc)] = _canon(ds, row)
    except Exception:  # noqa: BLE001
        pass
    return out


def _index_multi(payload, ds):
    out = {}
    if not isinstance(payload, (dict, list)):
        return out
    try:
        for row in _rows_of(payload):
            rc = _row_code(row)
            if not rc:
                continue
            out.setdefault(_norm_code(rc), []).append(_canon(ds, row))
    except Exception:  # noqa: BLE001
        pass
    return out


def _daily(code, dates):
    f = DAILY / (str(code).zfill(6) + ".csv")
    if not f.exists():
        return {}
    res = {}
    try:
        with open(f, newline="") as fh:
            for r in csv.DictReader(fh):
                if r.get("date") in dates:
                    res[r["date"]] = {k: r.get(k) for k in
                                      ("close", "preclose", "pctChg", "tradestatus") if k in r}
    except Exception:  # noqa: BLE001
        pass
    return res


def _num(x):
    try:
        return float(x)
    except Exception:  # noqa: BLE001
        return None


def main():
    D = _dates()
    if len(D) < 2:
        print(json.dumps({"error": "need >=2 date folders", "dates": D}))
        return 0
    today, prev = D[-1], D[-2]
    dl = D[-2:]
    out = {"job": "0121_hardcase_validation", "today": today, "prev_folder": prev, "dl_dates": dl}

    weimai_t = _index(_load(CAP / today / "auction.jjyd.weimai")[0], "auction.jjyd.weimai")
    net_t = _index(_load(CAP / today / "auction.jjyd.net_amount")[0], "auction.jjyd.net_amount")
    fp_t_raw, fpt_fn = _load(CAP / today / "review.fupan.plate")
    fp_p_raw, fpp_fn = _load(CAP / prev / "review.fupan.plate")
    fupan_t = _index(fp_t_raw, "review.fupan.plate")
    fupan_p = _index(fp_p_raw, "review.fupan.plate")

    # (E) fengdan negative/missing
    fd = _index(_load(CAP / today / "auction.jjlive.fengdan")[0], "auction.jjlive.fengdan")
    neg = []
    for c, v in fd.items():
        vals = [_num(v.get(k)) for k in ("seal_bid_915", "seal_bid_920", "seal_bid_925")]
        if any(x is not None and x < 0 for x in vals):
            neg.append({"code": c, "name": v.get("name"),
                        "seal_bid_915": vals[0], "seal_bid_920": vals[1], "seal_bid_925": vals[2]})
    out["E_fengdan_negative"] = {"count": len(neg), "examples": neg[:4]}

    # (D) ltgd multiwindow
    ltgd_raw = _load(CAP / today / "review.ltgd.range")[0]
    if not isinstance(ltgd_raw, (dict, list)):
        ltgd_raw = _load(CAP / prev / "review.ltgd.range")[0]
    ltgd_multi = _index_multi(ltgd_raw, "review.ltgd.range")
    multi = []
    multi_count = 0
    for c, lst in ltgd_multi.items():
        pers = [str(x.get("range_period")) for x in lst if isinstance(x, dict) and x.get("range_period") is not None]
        if len(set(pers)) >= 2:
            multi_count += 1
            if len(multi) < 5:
                multi.append({"code": c, "name": (lst[0] or {}).get("name"),
                              "windows": [{"range_period": x.get("range_period"),
                                           "range_return": x.get("range_return"),
                                           "range_rank": x.get("range_rank")} for x in lst]})
    out["D_ltgd_multiwindow"] = {"note": "panel dedup keeps only LAST row/code -> other windows lost; interval_period records survivor",
                                "total_multiwindow_codes": multi_count, "examples": multi}

    # (C) board(today auction) vs streak(T-1 close)
    conflict = []
    for c, w in weimai_t.items():
        bl = w.get("board_label")
        if not bl:
            continue
        conflict.append({"code": c, "name": w.get("name"),
                         "today_auction_board_label": bl,
                         "today_auction_pct": (net_t.get(c, {}) or {}).get("auction_change_pct"),
                         "fupan_streak_prev_close": _num(fupan_t.get(c, {}).get("streak"))})
    out["C_board_vs_streak"] = {"count": len(conflict), "examples": conflict[:6]}

    # (B) cashflow.today intraday test
    ci_first = _index(_load(CAP / today / "cashflow.stock.today", "first")[0], "cashflow.stock.today")
    cf_last_p, cfl = _load(CAP / today / "cashflow.stock.today", "last")
    ci_last = _index(cf_last_p, "cashflow.stock.today")
    cf_first_fn = _snaps(CAP / today / "cashflow.stock.today")
    cff = cf_first_fn[0] if cf_first_fn else None
    cf_prev_p, cfp = _load(CAP / prev / "cashflow.stock.today", "last")
    ci_prev = _index(cf_prev_p, "cashflow.stock.today")
    changed = total = 0
    cf_cmp = []
    for c, lastv in ci_last.items():
        a = _num(ci_first.get(c, {}).get("main_net"))
        b = _num(lastv.get("main_net"))
        if a is None or b is None:
            continue
        total += 1
        if abs(a - b) > 1:
            changed += 1
        if len(cf_cmp) < 5:
            cf_cmp.append({"code": c, "name": lastv.get("name"),
                           "main_net_first": a, "main_net_last": b,
                           "main_net_prev_folder": _num(ci_prev.get(c, {}).get("main_net"))})
    out["B_cashflow_intraday_test"] = {
        "first_file": cff, "last_file": cfl, "prev_folder_last_file": cfp,
        "n_compared": total, "n_changed_within_day": changed,
        "verdict_hint": "changed>0 => today INTRADAY live (NOT premarket-usable); changed==0 & ==prev_folder => yesterday FINAL",
        "samples": cf_cmp}

    # (A) fupan folder-shift (streak changed across folders) -- placed last (most important)
    shift = []
    shift_count = 0
    for c, cur in fupan_t.items():
        pv = fupan_p.get(c)
        if not pv:
            continue
        s_t = _num(cur.get("streak"))
        s_p = _num(pv.get("streak"))
        if s_t is None or s_p is None or s_t == s_p:
            continue
        shift_count += 1
        if len(shift) < 5:
            shift.append({"code": c, "name": cur.get("name"),
                          "streak_in_folder_%s" % today: s_t,
                          "streak_in_folder_%s" % prev: s_p,
                          "dailyline": _daily(c, dl)})
    out["A_fupan_folder_shift"] = {
        "fupan_today_file": fpt_fn, "fupan_prev_file": fpp_fn,
        "hint": "if folder-D content=D-1 close, streak in folder_%s should equal the ladder at dailyline[%s]" % (today, prev),
        "total_shift_codes": shift_count, "examples": shift}

    out["_summary"] = {"fupan_shift_codes": shift_count,
                       "cashflow_changed_within_day": changed, "cashflow_compared": total,
                       "board_conflict_rows": len(conflict),
                       "ltgd_multiwindow_codes": multi_count,
                       "fengdan_negative": len(neg)}
    print("=== HARDCASE VALIDATION (Task 0121) ===")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
