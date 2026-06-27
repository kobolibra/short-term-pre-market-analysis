#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v44_decorrelated_composite.py

0053 证实竞价因子高度重复(成交额~超大单~主力净流入 corr 0.6-1.0), 等权堆 7 个 IC 反而
低于单个最强(0.151<0.163). 0054 发现 T-1 review.fupan.plate 活跃度(成交额/换手率) IC~0.10
且与当日竞价多半独立, home.ztpool 晋级率是反向(-0.07).
本作业: 每个独立簇取一个代表 + T-1 复盘信号, 算交叉相关 + 多种去相关组合 IC,
与单因子 0.163 对比, 看加入独立的 T-1 复盘活跃度能否突破天花板.
excess=(close-open)/preclose*100. 同日因子用<=09:30快照; 滞后因子用<D的最近抓取日.
输出 reports/_audit/decorrelated_composite_v44.{json,md}
用法: python3 scripts/v44_decorrelated_composite.py
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

# key, source(sameday|lagged), dataset, field, sign
FACTORS = [
    ("A_turnover", "sameday", "auction.jjyd.qiangchou", "auction_turnover_wan", 1),
    ("B_turnrate", "sameday", "auction.jjyd.qiangchou", "turnover_rate_pct", 1),
    ("C_gap", "sameday", "auction.jjyd.qiangchou", "latest_change_pct", 1),
    ("D_fp_amount", "lagged", "review.fupan.plate", "\u6210\u4ea4\u989d", 1),
    ("E_fp_turnrate", "lagged", "review.fupan.plate", "\u6362\u624b\u7387", 1),
    ("F_ztpool_promo", "lagged", "home.ztpool", "\u664b\u7ea7\u7387", -1),
]
LAG_DATASETS = ["review.fupan.plate", "home.ztpool"]

COMPOSITES = {
    "comp_SD": ["A_turnover", "B_turnrate", "C_gap"],
    "comp_SD_FP": ["A_turnover", "B_turnrate", "C_gap", "D_fp_amount", "E_fp_turnrate"],
    "comp_ALL": ["A_turnover", "B_turnrate", "C_gap", "D_fp_amount", "E_fp_turnrate", "F_ztpool_promo"],
    "comp_AB_D": ["A_turnover", "B_turnrate", "D_fp_amount"],
    "comp_A_D": ["A_turnover", "D_fp_amount"],
}


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


def index_rows(rows):
    idx = {}
    for r in rows:
        c = code_of(r)
        if c:
            idx.setdefault(c, r)
    return idx


def latest_preopen_rows(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return []
    pre = [f for f in d.glob("*.json") if len(f.stem) == 6 and f.stem.isdigit() and f.stem <= PREOPEN]
    if not pre:
        return []
    f = sorted(pre)[-1]
    try:
        payload = json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def latest_rows(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return []
    files = sorted(d.glob("*.json"))
    if not files:
        return []
    try:
        payload = json.loads(files[-1].read_text(encoding="utf-8"))
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
    names = [d.name for d in date_dirs]
    by_name = {d.name: d for d in date_dirs}
    lag_dates = {ds: [n for n in names if (by_name[n] / ds).is_dir() and any((by_name[n] / ds).glob("*.json"))] for ds in LAG_DATASETS}
    sign = {k: s for (k, _, _, _, s) in FACTORS}

    ic_daily = defaultdict(list)
    comp_daily = defaultdict(list)
    pair_corr = defaultdict(list)

    for D in names:
        d_dir = by_name[D]
        # build per-factor raw value maps
        fvals = {}
        sd_idx = index_rows(latest_preopen_rows(d_dir, "auction.jjyd.qiangchou"))
        lag_idx = {}
        for ds in LAG_DATASETS:
            priors = [n for n in lag_dates[ds] if n < D]
            lag_idx[ds] = index_rows(latest_rows(by_name[priors[-1]], ds)) if priors else {}
        for (k, src, ds, field, _s) in FACTORS:
            idx = sd_idx if src == "sameday" else lag_idx.get(ds, {})
            m = {}
            for c, r in idx.items():
                v = pnum(r.get(field))
                if v is not None:
                    m[c] = v
            fvals[k] = m
        all_codes = set()
        for k in fvals:
            all_codes |= set(fvals[k].keys())
        ex = {}
        for c in all_codes:
            e = daily.excess(c, D)
            if e is not None:
                ex[c] = e
        if len(ex) < 8:
            continue
        # single-factor IC
        for k in fvals:
            xs, ys = [], []
            for c, v in fvals[k].items():
                if c in ex:
                    xs.append(v)
                    ys.append(ex[c])
            if len(xs) >= 8:
                ic = v10.spearman(xs, ys)
                if ic is not None:
                    ic_daily[k].append(ic)
        # pairwise correlation
        keys = [k for (k, _, _, _, _) in FACTORS]
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                a, b = keys[i], keys[j]
                xs, ys = [], []
                for c in fvals[a]:
                    if c in fvals[b]:
                        xs.append(fvals[a][c])
                        ys.append(fvals[b][c])
                if len(xs) >= 8:
                    r = v10.spearman(xs, ys)
                    if r is not None:
                        pair_corr[a + " ~ " + b].append(r)
        # composites (signed z)
        zmaps = {k: zscore(fvals[k], list(ex.keys())) for k in fvals}
        for cname, keylist in COMPOSITES.items():
            comp = {}
            for c in ex:
                zs = []
                for k in keylist:
                    if c in zmaps[k]:
                        zs.append(sign[k] * zmaps[k][c])
                if zs:
                    comp[c] = sum(zs) / len(zs)
            xs, ys = [], []
            for c, v in comp.items():
                xs.append(v)
                ys.append(ex[c])
            if len(xs) >= 8:
                ic = v10.spearman(xs, ys)
                if ic is not None:
                    comp_daily[cname].append(ic)

    factors_out = []
    for (k, _, _, _, _) in FACTORS:
        m, icir, nd = v10.mean_icir(ic_daily.get(k, []))
        factors_out.append({"factor": k, "mean_ic": m, "icir": icir, "n_days": nd})
    comp_out = []
    for cname in COMPOSITES:
        m, icir, nd = v10.mean_icir(comp_daily.get(cname, []))
        comp_out.append({"composite": cname, "members": COMPOSITES[cname], "mean_ic": m, "icir": icir, "n_days": nd})
    comp_out.sort(key=lambda x: (x["mean_ic"] if x["mean_ic"] is not None else -1), reverse=True)
    corr_out = []
    for k, lst in pair_corr.items():
        avg = sum(lst) / len(lst) if lst else None
        corr_out.append({"pair": k, "avg_spearman": round(avg, 3) if avg is not None else None, "n_days": len(lst)})
    corr_out.sort(key=lambda x: abs(x["avg_spearman"]) if x["avg_spearman"] is not None else -1, reverse=True)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "decorrelated_composite_v44",
        "single_best_reference": {"factor": "A_turnover", "mean_ic": 0.1629},
        "factors": factors_out,
        "composites": comp_out,
        "pairwise_correlation": corr_out,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "decorrelated_composite_v44.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# \u53bb\u76f8\u5173\u591a\u6e90\u7ec4\u5408 v44", "", "- \u751f\u6210: " + report["generated_at"], "",
         "## \u5355\u56e0\u5b50", "", "| \u56e0\u5b50 | mean_ic | icir | n_days |", "|---|---|---|---|"]
    for r in factors_out:
        L.append("| " + r["factor"] + " | " + str(r["mean_ic"]) + " | " + str(r["icir"]) + " | " + str(r["n_days"]) + " |")
    L += ["", "## \u7ec4\u5408(vs \u5355\u56e0\u5b50 0.163)", "", "| \u7ec4\u5408 | mean_ic | icir | n_days | \u6210\u5458 |", "|---|---|---|---|---|"]
    for r in comp_out:
        L.append("| " + r["composite"] + " | " + str(r["mean_ic"]) + " | " + str(r["icir"]) + " | " + str(r["n_days"]) + " | " + ",".join(r["members"]) + " |")
    L += ["", "## \u4ea4\u53c9\u76f8\u5173", "", "| \u56e0\u5b50\u5bf9 | avg_spearman | n_days |", "|---|---|---|"]
    for r in corr_out:
        L.append("| " + r["pair"] + " | " + str(r["avg_spearman"]) + " | " + str(r["n_days"]) + " |")
    (audit / "decorrelated_composite_v44.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
