#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_weimai_columns_probe_0165.py -- Task 0165 (READ-ONLY).

以 fengdan f925(已交叉验证=真封单) 为基准, 逐列查明 weimai 原始列含义
(raw4 未剔成交委托 / raw8 / raw17 seal_amount / 主力净额),
并统计 fengdan 与竞价四表 / FF 重叠率(回答为何缺 FF)。不改任何线上逻辑。
用法: python3 scripts/duanxianxia_weimai_columns_probe_0165.py [YYYY-MM-DD]
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from datetime import datetime
from statistics import median

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from v10_optimize import DEFAULT_PROJECT_ROOT
import duanxianxia_feature_builder as fb
from duanxianxia_canonical_routing import canonicalize_rows_by_id


def _today():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _yi(x, nd=3):
    if x is None:
        return "-"
    try:
        return format(x / 1e8, "." + str(nd) + "f")
    except Exception:
        return str(x)


def _med(xs):
    xs = [x for x in xs if x is not None]
    return median(xs) if xs else None


def _r(a, b):
    if a is None or b in (None, 0):
        return None
    return a / b


def _load_map(date_dir, dsid, cutoff_secs):
    d = date_dir / dsid
    if not d.is_dir():
        return {}
    picked, _meta = fb._pick_capture_file(d, cutoff_secs)
    m = {}
    if picked is not None:
        for r in canonicalize_rows_by_id(dsid, fb._rows_of(picked[1])):
            if isinstance(r, dict) and r.get("code") and not r.get("_canonical_error"):
                m[fb._norm_code(r["code"])] = r
    return m


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else _today()
    root = Path(DEFAULT_PROJECT_ROOT)
    date_dir = root / "captures" / date
    cs = fb._cutoff_seconds(fb.T0_DEFAULT_CUTOFF)

    if not date_dir.is_dir():
        print("NO_DATE_DIR " + date)
        return 0

    fmap = _load_map(date_dir, "auction.jjlive.fengdan", cs)
    wmap = _load_map(date_dir, "auction.jjyd.weimai", cs)
    vmap = _load_map(date_dir, "auction.jjyd.vratio", cs)
    nmap = _load_map(date_dir, "auction.jjyd.net_amount", cs)
    qmap = _load_map(date_dir, "auction.jjyd.qiangchou", cs)

    def has_ff(code):
        for m in (wmap, vmap, nmap, qmap):
            r = m.get(code)
            if r and r.get("free_float_mktcap") is not None:
                return True
        return False

    def has_bid(code):
        for m in (vmap, nmap, wmap, qmap):
            r = m.get(code)
            if r and r.get("auction_turnover") is not None:
                return True
        return False

    fcodes = list(fmap)
    cov = {
        "n_fengdan": len(fmap),
        "n_weimai": len(wmap),
        "n_vratio": len(vmap),
        "n_net_amount": len(nmap),
        "n_qiangchou": len(qmap),
        "fengdan_925_nonzero": sum(1 for c in fcodes if (fmap[c].get("seal_bid_925") or 0) > 0),
        "in_weimai": sum(1 for c in fcodes if c in wmap),
        "in_vratio": sum(1 for c in fcodes if c in vmap),
        "in_net_amount": sum(1 for c in fcodes if c in nmap),
        "in_qiangchou": sum(1 for c in fcodes if c in qmap),
        "in_any_of_4": sum(1 for c in fcodes if (c in wmap or c in vmap or c in nmap or c in qmap)),
        "fengdan_with_FF": sum(1 for c in fcodes if has_ff(c)),
        "fengdan_with_bid": sum(1 for c in fcodes if has_bid(c)),
    }

    rows = []
    raw8_over_A, vbid_over_A, raw8_over_vbid = [], [], []
    raw17_over_f925, raw17_over_A, raw4m8_over_f925 = [], [], []
    f920_over_raw4, f920_over_f925plusbid = [], []
    for c in fcodes:
        fr = fmap[c]
        f915, f920, f925 = fr.get("seal_bid_915"), fr.get("seal_bid_920"), fr.get("seal_bid_925")
        wm = wmap.get(c)
        if not wm or not f925:
            continue
        raw4 = wm.get("seal_amount_wan_raw")
        raw8 = wm.get("auction_turnover")
        raw17 = wm.get("seal_amount")
        mnet = wm.get("main_net_inflow")
        ff = wm.get("free_float_mktcap")
        vbid = (vmap.get(c) or {}).get("auction_turnover")
        A = (raw4 - f925) if (raw4 is not None) else None
        rows.append({
            "code": c, "name": fr.get("name"),
            "f915": f915, "f920": f920, "f925": f925,
            "raw4_wtuo": raw4, "raw8": raw8, "raw17_seal": raw17,
            "main_net": mnet, "vbid": vbid, "A_raw4_minus_f925": A, "ff": ff,
        })
        if A is not None:
            raw8_over_A.append(_r(raw8, A))
            vbid_over_A.append(_r(vbid, A))
            raw17_over_A.append(_r(raw17, A))
        raw8_over_vbid.append(_r(raw8, vbid))
        raw17_over_f925.append(_r(raw17, f925))
        if raw4 is not None and raw8 is not None:
            raw4m8_over_f925.append(_r(raw4 - raw8, f925))
        f920_over_raw4.append(_r(f920, raw4))
        if f925 is not None and vbid is not None:
            f920_over_f925plusbid.append(_r(f920, f925 + vbid))

    stats = {
        "n_col_compare": len(rows),
        "median_raw8_div_A(raw4-f925)": _med(raw8_over_A),
        "median_vbid_div_A": _med(vbid_over_A),
        "median_raw8_div_vbid": _med(raw8_over_vbid),
        "median_(raw4-raw8)_div_f925": _med(raw4m8_over_f925),
        "median_raw17_div_f925": _med(raw17_over_f925),
        "median_raw17_div_A": _med(raw17_over_A),
        "median_f920_div_raw4": _med(f920_over_raw4),
        "median_f920_div_(f925+vbid)": _med(f920_over_f925plusbid),
    }

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    out_path = audit / ("weimai_columns_" + date + "_0165.json")
    out_path.write_text(json.dumps({"coverage": cov, "stats": stats, "rows": rows},
                                   ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")

    lines = ["=== weimai 列含义查明 + fengdan 覆盖 " + date + " ==="]
    lines.append("coverage=" + json.dumps(cov, ensure_ascii=False))
    lines.append("列(亿): code name | f915 f920 f925 | raw4未剔委托 | raw8 | raw17 | vbid竞价成交 | A=raw4-f925 | FF")
    rows.sort(key=lambda r: (r["f925"] or 0), reverse=True)
    for r in rows[:12]:
        lines.append(" | ".join([
            str(r["code"]) + " " + (str(r["name"] or ""))[:6],
            _yi(r["f915"]) + " " + _yi(r["f920"]) + " " + _yi(r["f925"]),
            _yi(r["raw4_wtuo"]), _yi(r["raw8"]), _yi(r["raw17_seal"]),
            _yi(r["vbid"]), _yi(r["A_raw4_minus_f925"]), _yi(r["ff"]),
        ]))
    lines.append("=== stats (比值~1 即恒等; A=raw4-f925=真封单口径下的竞价成交) ===")
    lines.append(json.dumps(stats, ensure_ascii=False, default=str))
    lines.append("out=" + out_path.name)
    print(chr(10).join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
