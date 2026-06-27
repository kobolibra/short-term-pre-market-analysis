#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v42_clean_survivor_composite.py

目标: 在 0050(v39) 选出的干净盘前幸存因子上, 回答两个问题:
  (1) 这些因子彼此是否高度重复(两两 Spearman 相关) — 真正独立的有几个?
  (2) 干净组合(等权 z) 能否超过单个最强因子(抢筹成交额 IC~0.16)?
只用盘前(<=09:30)的最晚一份快照. excess=(close-open)/preclose*100.
输出 reports/_audit/clean_survivor_composite_v42.{json,md}
用法: python3 scripts/v42_clean_survivor_composite.py
"""
from __future__ import annotations
import json
import sys
import math
import traceback
from collections import defaultdict
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10

PREOPEN = "093000"
CODE_KEYS = ["code", "\u4ee3\u7801"]

# (dataset_id, field, short_name)
FACTORS = [
    ("auction.jjyd.qiangchou", "auction_turnover_wan", "QC.turnover"),
    ("auction.jjyd.qiangchou", "turnover_rate_pct", "QC.turnrate"),
    ("auction.jjyd.qiangchou", "latest_change_pct", "QC.chg"),
    ("auction.jjyd.vratio", "auction_turnover_wan", "VR.turnover"),
    ("auction.jjyd.vratio", "latest_change_pct", "VR.chg"),
    ("auction.jjyd.weimai", "main_net_inflow_full", "WM.mainflow"),
    ("auction.jjyd.weimai", "super_large_net_inflow", "WM.xlflow"),
]


def pnum(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("%", "").replace("+", "")
    if s in ("", "-", "--", "None"):
        return None
    mult = 1.0
    if s.endswith("\u4ebf"):
        mult, s = 1e4, s[:-1]
    elif s.endswith("\u4e07"):
        mult, s = 1.0, s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return None


def _norm(v):
    s = str(v or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:].zfill(6) if len(s) >= 6 else s


def code_of(r):
    for k in CODE_KEYS:
        if r.get(k) not in (None, ""):
            return _norm(r.get(k))
    return ""


def latest_preopen_rows(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return []
    pre = []
    for f in d.glob("*.json"):
        stem = f.stem
        if len(stem) == 6 and stem.isdigit() and stem <= PREOPEN:
            pre.append(f)
    if not pre:
        return []
    f = sorted(pre)[-1]
    try:
        payload = json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def zscore(valmap, codes):
    vals = [valmap[c] for c in codes if c in valmap]
    if len(vals) < 3:
        return {}
    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / len(vals)
    sd = math.sqrt(var)
    if sd == 0:
        return {}
    return {c: (valmap[c] - m) / sd for c in codes if c in valmap}


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []

    ic_daily = defaultdict(list)
    comp_daily = []
    pair_corr = defaultdict(list)
    cov = defaultdict(int)

    for dd in date_dirs:
        fvals = {}
        for (ds, field, short) in FACTORS:
            rows = latest_preopen_rows(dd, ds)
            m = {}
            for r in rows:
                c = code_of(r)
                v = pnum(r.get(field))
                if c and v is not None:
                    m[c] = v
            fvals[short] = m
        all_codes = set()
        for short in fvals:
            all_codes |= set(fvals[short].keys())
        ex = {}
        for c in all_codes:
            e = daily.excess(c, dd.name)
            if e is not None:
                ex[c] = e
        if len(ex) < 8:
            continue
        # per-factor daily IC
        for short in fvals:
            xs, ys = [], []
            for c, v in fvals[short].items():
                if c in ex:
                    xs.append(v)
                    ys.append(ex[c])
            if len(xs) >= 8:
                ic = v10.spearman(xs, ys)
                if ic is not None:
                    ic_daily[short].append(ic)
                    cov[short] += len(xs)
        # pairwise correlation among factors (on shared codes)
        shorts = [s for (_, _, s) in FACTORS]
        for i in range(len(shorts)):
            for j in range(i + 1, len(shorts)):
                a, b = shorts[i], shorts[j]
                xs, ys = [], []
                for c in fvals[a]:
                    if c in fvals[b]:
                        xs.append(fvals[a][c])
                        ys.append(fvals[b][c])
                if len(xs) >= 8:
                    r = v10.spearman(xs, ys)
                    if r is not None:
                        pair_corr[a + " ~ " + b].append(r)
        # equal-weight z composite over codes in ex
        zmaps = {short: zscore(fvals[short], list(ex.keys())) for short in fvals}
        comp = {}
        for c in ex:
            zs = [zmaps[short][c] for short in zmaps if c in zmaps[short]]
            if zs:
                comp[c] = sum(zs) / len(zs)
        xs, ys = [], []
        for c, v in comp.items():
            xs.append(v)
            ys.append(ex[c])
        if len(xs) >= 8:
            ic = v10.spearman(xs, ys)
            if ic is not None:
                comp_daily.append(ic)

    factor_summary = []
    for (_, _, short) in FACTORS:
        m, icir, nd = v10.mean_icir(ic_daily.get(short, []))
        factor_summary.append({"factor": short, "mean_ic": m, "icir": icir, "n_days": nd, "avg_rows": (cov[short] // nd) if nd else 0})
    factor_summary.sort(key=lambda x: abs(x["mean_ic"]) if x["mean_ic"] is not None else -1, reverse=True)
    cm, cicir, cnd = v10.mean_icir(comp_daily)
    corr_summary = []
    for k, lst in pair_corr.items():
        avg = sum(lst) / len(lst) if lst else None
        corr_summary.append({"pair": k, "avg_spearman": round(avg, 3) if avg is not None else None, "n_days": len(lst)})
    corr_summary.sort(key=lambda x: abs(x["avg_spearman"]) if x["avg_spearman"] is not None else -1, reverse=True)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "clean_survivor_composite_v42",
        "factors": factor_summary,
        "composite_equalweight_z": {"mean_ic": cm, "icir": cicir, "n_days": cnd},
        "pairwise_correlation": corr_summary,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "clean_survivor_composite_v42.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# \u5e72\u51c0\u5e78\u5b58\u56e0\u5b50: \u76f8\u5173\u6027 + \u7ec4\u5408 IC v42", "",
         "- \u751f\u6210: " + report["generated_at"], "",
         "## \u5355\u56e0\u5b50", "",
         "| \u56e0\u5b50 | mean_ic | icir | n_days | avg_rows |", "|---|---|---|---|---|"]
    for r in factor_summary:
        L.append("| " + r["factor"] + " | " + str(r["mean_ic"]) + " | " + str(r["icir"]) + " | " + str(r["n_days"]) + " | " + str(r["avg_rows"]) + " |")
    L += ["", "## \u7b49\u6743 z \u7ec4\u5408", "", "- mean_ic=" + str(cm) + " icir=" + str(cicir) + " n_days=" + str(cnd), "",
          "## \u4e24\u4e24\u76f8\u5173(Spearman, \u8d8a\u9ad8\u8d8a\u91cd\u590d)", "", "| \u56e0\u5b50\u5bf9 | avg_spearman | n_days |", "|---|---|---|"]
    for r in corr_summary:
        L.append("| " + r["pair"] + " | " + str(r["avg_spearman"]) + " | " + str(r["n_days"]) + " |")
    (audit / "clean_survivor_composite_v42.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
