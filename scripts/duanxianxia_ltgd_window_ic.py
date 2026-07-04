#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""job 0143: ltgd multi-window (5/10/20/50d) IC vs same-day excess.

Uses the FIXED pit_panel._load_map dense pivot (Item A) so per-code merged rows
carry range_return_{5,10,20,50}d / range_rank_{5,10,20,50}d (old last-wins
collapse lost 25/80 window observations).

review.ltgd.range is an overnight review dataset with uncertain capture timing,
so to avoid look-ahead leakage we compute TWO leakage-safe variants per field:
  - asof_preopen: the folder==D ltgd capture whose stem<=093000 (genuinely
    pre-open on day D -> correct alignment, no leakage).
  - lag_prev: the latest ltgd capture folder strictly before D (mirrors v44
    review.fupan.plate; maximally conservative, no leakage).
excess=(close-open)/preclose*100 (same target as v39/v44/v10).

Each window field reports single-factor daily cross-sectional Spearman IC/ICIR
and the average cross-sectional correlation vs the current single-best premarket
factor A_turnover (auction.jjyd.qiangchou.auction_turnover_wan, 0055
mean_ic~0.163) to test whether ltgd momentum adds INDEPENDENT signal before
touching v9 weights.

Output reports/_audit/ltgd_window_ic_0143.{json,md}
Usage: python3 scripts/duanxianxia_ltgd_window_ic.py
"""
from __future__ import annotations
import json
import sys
import traceback
from collections import defaultdict
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
from duanxianxia_canonical_routing import canonicalize_row
from duanxianxia_pit_panel import _load_map, LTGD_DS

PREOPEN = "093000"
CODE_KEYS = ["code", "\u4ee3\u7801"]
REF_DS = "auction.jjyd.qiangchou"
REF_FIELD = "auction_turnover_wan"

WINDOW_FIELDS = [
    ("ret_5d", "range_return_5d"), ("rank_5d", "range_rank_5d"),
    ("ret_10d", "range_return_10d"), ("rank_10d", "range_rank_10d"),
    ("ret_20d", "range_return_20d"), ("rank_20d", "range_rank_20d"),
    ("ret_50d", "range_return_50d"), ("rank_50d", "range_rank_50d"),
    ("ret_base", "range_return"), ("rank_base", "range_rank"),
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


def ltgd_merged(date_dir, preopen_only=False):
    d = date_dir / LTGD_DS
    if not d.is_dir():
        return {}
    fs = [f for f in d.glob("*.json")]
    if preopen_only:
        fs = [f for f in fs if len(f.stem) == 6 and f.stem.isdigit() and f.stem <= PREOPEN]
    fs = sorted(fs)
    if not fs:
        return {}
    try:
        by_code, _errs = _load_map(date_dir, LTGD_DS, fs[-1], canonicalize_row)
    except Exception:
        return {}
    return by_code or {}


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []
    names = [d.name for d in date_dirs]
    by_name = {d.name: d for d in date_dirs}
    ltgd_dates = [n for n in names if (by_name[n] / LTGD_DS).is_dir() and any((by_name[n] / LTGD_DS).glob("*.json"))]

    ic_daily = defaultdict(list)
    corr_daily = defaultdict(list)
    merge_days = defaultdict(int)
    eval_days = 0

    for D in names:
        d_dir = by_name[D]
        variants = {}
        priors = [n for n in ltgd_dates if n < D]
        if priors:
            mp = ltgd_merged(by_name[priors[-1]])
            if mp:
                variants["lag_prev"] = mp
        ma = ltgd_merged(d_dir, preopen_only=True)
        if ma:
            variants["asof_preopen"] = ma
        if not variants:
            continue
        ref_idx = index_rows(latest_preopen_rows(d_dir, REF_DS))
        ref_map = {}
        for c, r in ref_idx.items():
            v = pnum(r.get(REF_FIELD))
            if v is not None:
                ref_map[c] = v
        codes = set(ref_map.keys())
        for m in variants.values():
            codes |= set(m.keys())
        ex = {}
        for c in codes:
            e = daily.excess(c, D)
            if e is not None:
                ex[c] = e
        if len(ex) < 8:
            continue
        eval_days += 1
        for vname, merged in variants.items():
            merge_days[vname] += 1
            for (fkey, field) in WINDOW_FIELDS:
                m = {}
                for c, r in merged.items():
                    v = pnum(r.get(field))
                    if v is not None:
                        m[c] = v
                xs, ys = [], []
                for c, v in m.items():
                    if c in ex:
                        xs.append(v)
                        ys.append(ex[c])
                if len(xs) >= 8:
                    ic = v10.spearman(xs, ys)
                    if ic is not None:
                        ic_daily[(vname, fkey)].append(ic)
                    xs2, ys2 = [], []
                    for c, v in m.items():
                        if c in ref_map:
                            xs2.append(v)
                            ys2.append(ref_map[c])
                    if len(xs2) >= 8:
                        rr = v10.spearman(xs2, ys2)
                        if rr is not None:
                            corr_daily[(vname, fkey)].append(rr)

    factors_out = []
    for vname in ("asof_preopen", "lag_prev"):
        for (fkey, field) in WINDOW_FIELDS:
            m, icir, nd = v10.mean_icir(ic_daily.get((vname, fkey), []))
            cc = corr_daily.get((vname, fkey), [])
            avgcorr = round(sum(cc) / len(cc), 3) if cc else None
            factors_out.append({"variant": vname, "factor": fkey, "field": field,
                                "mean_ic": m, "icir": icir, "n_days": nd,
                                "corr_days": len(cc),
                                "avg_corr_vs_A_turnover": avgcorr})
    factors_out.sort(key=lambda x: abs(x["mean_ic"]) if x["mean_ic"] is not None else -1, reverse=True)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0143_ltgd_window_ic",
        "dataset": LTGD_DS,
        "variants": {
            "asof_preopen": "folder==D ltgd capture with stem<=093000 (pre-open, aligned, no leakage)",
            "lag_prev": "latest ltgd capture folder strictly before D (mirrors v44 review.fupan.plate)",
        },
        "ref_factor": {"dataset": REF_DS, "field": REF_FIELD, "single_best_mean_ic": 0.1629},
        "ltgd_merge_days": dict(merge_days),
        "eval_days": eval_days,
        "factors": factors_out,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "ltgd_window_ic_0143.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# ltgd \u591a\u7a97\u53e3 IC v0143 (Item B)", "",
         f"- \u751f\u6210: {report['generated_at']} \uff5c\u8bc4\u4f30\u5929: {eval_days} \uff5cmerge_days: {dict(merge_days)}",
         f"- \u72ec\u7acb\u6027\u53c2\u7167: {REF_DS}.{REF_FIELD} (\u5355\u56e0\u5b50\u6700\u5f3a mean_ic~0.163)", "",
         "## \u5404\u7a97\u53e3\u56e0\u5b50 IC (\u6309 |IC| \u6392\u5e8f)", "",
         "| variant | \u56e0\u5b50 | \u5b57\u6bb5 | mean_ic | icir | n_days | corr_vs_A |",
         "|---|---|---|---|---|---|---|"]
    for r in factors_out:
        L.append(f"| {r['variant']} | {r['factor']} | {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} | {r['avg_corr_vs_A_turnover']} |")
    (audit / "ltgd_window_ic_0143.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"ltgd_merge_days": dict(merge_days), "eval_days": eval_days,
                      "factors": factors_out}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
