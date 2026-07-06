#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_fengdan_relation_probe_0164.py -- Task 0164 (READ-ONLY probe).

用真实 captures/<date> 数据, 实证 fengdan 封单时间序列 (amount_915/920/925)
与竞价成交额 (bidAmount) 及 weimai 封单/委买口径的关系。不改动任何线上逻辑。

待验证恒等式:
  H1: amount_925 ~= 委买额(weimai raw4) - 竞价成交额(bid)   [== weimai raw17 网站封单]
  H2: amount_920 ~= amount_925 + bid                       [920 委托含将成交部分]

用法: python3 scripts/duanxianxia_fengdan_relation_probe_0164.py [YYYY-MM-DD]
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

FENGDAN_DSID = "auction.jjlive.fengdan"


def _today_shanghai() -> str:
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


def _ratio(a, b):
    if a is None or b in (None, 0):
        return None
    return a / b


def main():
    date = sys.argv[1] if len(sys.argv) > 1 else _today_shanghai()
    root = Path(DEFAULT_PROJECT_ROOT)
    date_dir = root / "captures" / date
    cutoff_secs = fb._cutoff_seconds(fb.T0_DEFAULT_CUTOFF)

    diag = {
        "job": "0164_fengdan_relation_probe",
        "date": date,
        "date_dir_exists": date_dir.is_dir(),
        "date_dir_listing": sorted(p.name for p in date_dir.iterdir()) if date_dir.is_dir() else [],
    }

    fengdan_rows = []
    if date_dir.is_dir():
        fdir = date_dir / FENGDAN_DSID
        if not fdir.is_dir():
            cand = [p for p in date_dir.iterdir() if p.is_dir() and "fengdan" in p.name.lower()]
            fdir = cand[0] if cand else None
        if fdir and fdir.is_dir():
            picked, meta = fb._pick_capture_file(fdir, cutoff_secs)
            diag["fengdan_dir"] = fdir.name
            diag["fengdan_capture_meta"] = meta
            if picked is not None:
                fengdan_rows = canonicalize_rows_by_id(FENGDAN_DSID, fb._rows_of(picked[1]))

    feats_by_code = {}
    if date_dir.is_dir():
        try:
            ft = fb.build_feature_table(date_dir)
            feats_by_code = {f["code"]: f for f in ft.get("features", [])}
            diag["n_features"] = len(feats_by_code)
            diag["coverage"] = ft.get("coverage")
        except Exception as e:
            diag["feature_build_error"] = repr(e)

    fmap = {}
    for r in fengdan_rows:
        if isinstance(r, dict) and r.get("code"):
            fmap[fb._norm_code(r.get("code"))] = r

    rows = []
    for code, fr in fmap.items():
        feat = feats_by_code.get(code)
        rows.append({
            "code": code, "name": fr.get("name"),
            "f915": fr.get("seal_bid_915"), "f920": fr.get("seal_bid_920"),
            "f925": fr.get("seal_bid_925"),
            "bid": feat.get("bidAmount") if feat else None,
            "wm_seal": feat.get("sealAmount") if feat else None,
            "wm_raw4": feat.get("sealAmountRaw") if feat else None,
            "ff": feat.get("free_float_mktcap") if feat else None,
        })

    r_925_bid, r_920_bid, r_925_wmseal, r_925_raw4, r_925_raw4mbid = [], [], [], [], []
    resid_H2, resid_H1 = [], []
    matched = 0
    for r in rows:
        if r["f925"] is None or r["bid"] is None:
            continue
        matched += 1
        r_925_bid.append(_ratio(r["f925"], r["bid"]))
        if r["f920"] is not None:
            r_920_bid.append(_ratio(r["f920"], r["bid"]))
            if r["f920"]:
                resid_H2.append((r["f920"] - (r["f925"] + r["bid"])) / r["f920"])
        if r["wm_seal"]:
            r_925_wmseal.append(_ratio(r["f925"], r["wm_seal"]))
        if r["wm_raw4"]:
            r_925_raw4.append(_ratio(r["f925"], r["wm_raw4"]))
            denom = r["wm_raw4"] - r["bid"]
            r_925_raw4mbid.append(_ratio(r["f925"], denom))
            if r["f925"]:
                resid_H1.append((r["f925"] - denom) / r["f925"])

    stats = {
        "n_fengdan": len(fmap),
        "n_features": len(feats_by_code),
        "n_matched_f925_and_bid": matched,
        "median_f925_div_bid": _med(r_925_bid),
        "median_f920_div_bid": _med(r_920_bid),
        "median_f925_div_wmSeal_raw17": _med(r_925_wmseal),
        "median_f925_div_wmRaw4_weimai": _med(r_925_raw4),
        "median_f925_div_(raw4-bid)_H1": _med(r_925_raw4mbid),
        "median_resid_H2_(f920-(f925+bid))/f920": _med(resid_H2),
        "median_resid_H1_(f925-(raw4-bid))/f925": _med(resid_H1),
    }

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    out_path = audit / ("fengdan_relation_" + date + "_0164.json")
    out_path.write_text(json.dumps({"diag": diag, "stats": stats, "rows": rows},
                                   ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")

    lines = ["=== fengdan 关系比对 " + date + " ==="]
    if not fmap:
        lines.append("NO_FENGDAN date_dir_exists=" + str(diag["date_dir_exists"]))
        lines.append("listing=" + json.dumps(diag["date_dir_listing"], ensure_ascii=False))
        print(chr(10).join(lines))
        return 0
    lines.append("列: code name | f915 f920 f925(亿) | 竞价成交(亿) | wm封单raw17(亿) | wm委买raw4(亿) | FF(亿)")
    show = [r for r in rows if r["f925"] is not None and r["bid"] is not None]
    show.sort(key=lambda r: (r["f925"] or 0), reverse=True)
    for r in show[:15]:
        lines.append(" | ".join([
            str(r["code"]) + " " + (str(r["name"] or ""))[:6],
            _yi(r["f915"]) + " " + _yi(r["f920"]) + " " + _yi(r["f925"]),
            _yi(r["bid"]), _yi(r["wm_seal"]), _yi(r["wm_raw4"]), _yi(r["ff"]),
        ]))
    lines.append("=== stats (H1: f925~=raw4-bid; H2: f920~=f925+bid; resid越接近0越成立) ===")
    lines.append(json.dumps(stats, ensure_ascii=False, default=str))
    lines.append("out=" + out_path.name)
    print(chr(10).join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
