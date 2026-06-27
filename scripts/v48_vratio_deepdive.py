#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v48_vratio_deepdive.py
Deep-dive on premarket vratio table (auction.jjyd.vratio) = jingjia baoliang.
Key question: redundant with qiangchou, or unique value?
Unique vs qiangchou: volume_ratio_multiple (=today/yesterday auction turnover,
fangliang multiple) and POPULATED yesterday_auction_turnover_wan (null in qiangchou).
Computes:
  1) coverage: yesterday_auction_turnover_wan, seal_amount_wan, volume_ratio_multiple
  2) field IC (own n): auction_turnover_wan, turnover_rate_pct, auction_change_pct,
     auction_volume_ratio, volume_ratio_multiple, latest_change_pct
  3) today/yesterday turnover ratio IC (recomputed from raw fields)
  4) pairwise redundancy (avg daily spearman)
  5) cross-table overlap with qiangchou + in-qc vs vratio-only bucket excess
Premarket snapshot only (HHMMSS<=093000). excess=(close-open)/preclose*100.
Output reports/_audit/vratio_deepdive_v48.json
"""
from __future__ import annotations
import json
import sys
import traceback
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10

DSID = "auction.jjyd.vratio"
QC_DSID = "auction.jjyd.qiangchou"
PREOPEN = "093000"
CODE_KEYS = ["code", "\u4ee3\u7801"]
YI = "\u4ebf"
WAN = "\u4e07"

FIELDS = {
    "auction_turnover_wan": "auction_turnover_wan",
    "turnover_rate_pct": "turnover_rate_pct",
    "auction_change_pct": "auction_change_pct",
    "auction_volume_ratio": "auction_volume_ratio",
    "volume_ratio_multiple": "volume_ratio_multiple",
    "latest_change_pct": "latest_change_pct",
}
COVER = ["yesterday_auction_turnover_wan", "seal_amount_wan", "volume_ratio_multiple"]


def pnum(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("%", "").replace("+", "")
    if s in ("", "-", "--", "None"):
        return None
    mult = 1.0
    if s.endswith(YI):
        mult, s = 1e4, s[:-1]
    elif s.endswith(WAN):
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


def load_rows(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return []
    pre = [f for f in d.glob("*.json") if len(f.stem) == 6 and f.stem.isdigit() and f.stem <= PREOPEN]
    if not pre:
        return []
    try:
        payload = json.loads(sorted(pre)[-1].read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []

    cov = {f: [0, 0] for f in COVER}
    ic_fields = defaultdict(list)
    ratio_ic = []
    pair_corr = defaultdict(list)
    overlap_stats = []
    grp_raw = defaultdict(list)
    grp_dm = defaultdict(list)
    n_dates = 0

    for dd in date_dirs:
        D = dd.name
        rows = load_rows(dd, DSID)
        if not rows:
            continue
        byc = {}
        for r in rows:
            c = code_of(r)
            if not c:
                continue
            if c not in byc:
                byc[c] = r
        exrows = []
        for c, r in byc.items():
            e = daily.excess(c, D)
            if e is None:
                continue
            exrows.append((c, r, e))
        if len(exrows) < 8:
            continue
        n_dates += 1
        day_mean_ex = mean([e for _, _, e in exrows])
        for _, r, _ in exrows:
            for f in COVER:
                cov[f][1] += 1
                if pnum(r.get(f)) is not None:
                    cov[f][0] += 1
        for key, col in FIELDS.items():
            xs, ys = [], []
            for c, r, e in exrows:
                v = pnum(r.get(col))
                if v is not None:
                    xs.append(v)
                    ys.append(e)
            if len(xs) >= 8:
                ic = v10.spearman(xs, ys)
                if ic is not None:
                    ic_fields[key].append(ic)
        rxs, rys = [], []
        for c, r, e in exrows:
            t = pnum(r.get("auction_turnover_wan"))
            y = pnum(r.get("yesterday_auction_turnover_wan"))
            if t is not None and y not in (None, 0):
                rxs.append(t / y)
                rys.append(e)
        if len(rxs) >= 8:
            ic = v10.spearman(rxs, rys)
            if ic is not None:
                ratio_ic.append(ic)
        keys = list(FIELDS.keys())
        colvals = {}
        for key in keys:
            col = FIELDS[key]
            colvals[key] = [pnum(r.get(col)) for c, r, e in exrows]
        for a, b in combinations(keys, 2):
            xs, ys = [], []
            for va, vb in zip(colvals[a], colvals[b]):
                if va is not None and vb is not None:
                    xs.append(va)
                    ys.append(vb)
            if len(xs) >= 8:
                cc = v10.spearman(xs, ys)
                if cc is not None:
                    pair_corr[a + " ~ " + b].append(cc)
        qc_rows = load_rows(dd, QC_DSID)
        qc_codes = set()
        for r in qc_rows:
            c = code_of(r)
            if c:
                qc_codes.add(c)
        vr_codes = set(c for c, _, _ in exrows)
        if qc_codes:
            inter = vr_codes & qc_codes
            overlap_stats.append({
                "date": D,
                "n_vratio": len(vr_codes),
                "n_qiangchou": len(qc_codes),
                "n_overlap": len(inter),
                "vratio_in_qc_pct": round(len(inter) / len(vr_codes), 3) if vr_codes else None,
            })
            for c, r, e in exrows:
                b = "in_qiangchou" if c in qc_codes else "vratio_only"
                grp_raw[b].append(e)
                grp_dm[b].append(e - day_mean_ex)

    field_out = []
    for k in FIELDS:
        m, icir, nd = v10.mean_icir(ic_fields.get(k, []))
        field_out.append({"field": k, "mean_ic": m, "icir": icir, "n_days": nd})
    field_out.sort(key=lambda x: (x["mean_ic"] if x["mean_ic"] is not None else -9), reverse=True)
    rm, ricir, rnd = v10.mean_icir(ratio_ic)
    cov_out = {f: {"present": cov[f][0], "total": cov[f][1],
                   "pct": round(cov[f][0] / cov[f][1], 3) if cov[f][1] else None} for f in COVER}
    pair_out = []
    for k, v in pair_corr.items():
        if v:
            pair_out.append({"pair": k, "avg_spearman": round(sum(v) / len(v), 3), "n_days": len(v)})
    pair_out.sort(key=lambda x: abs(x["avg_spearman"]), reverse=True)
    grp_out = []
    for b in grp_raw:
        grp_out.append({"bucket": b, "n": len(grp_raw[b]),
                        "mean_excess": round(mean(grp_raw[b]), 3) if grp_raw[b] else None,
                        "mean_excess_demeaned": round(mean(grp_dm[b]), 3) if grp_dm[b] else None})
    grp_out.sort(key=lambda x: x["n"], reverse=True)
    ov_sum = {}
    if overlap_stats:
        ov_sum = {
            "n_days": len(overlap_stats),
            "avg_n_vratio": round(mean([o["n_vratio"] for o in overlap_stats]), 1),
            "avg_n_qiangchou": round(mean([o["n_qiangchou"] for o in overlap_stats]), 1),
            "avg_n_overlap": round(mean([o["n_overlap"] for o in overlap_stats]), 1),
            "avg_vratio_in_qc_pct": round(mean([o["vratio_in_qc_pct"] for o in overlap_stats if o["vratio_in_qc_pct"] is not None]), 3),
        }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "vratio_deepdive_v48",
        "dataset": DSID,
        "n_dates": n_dates,
        "coverage": cov_out,
        "field_ic": field_out,
        "today_over_yesterday_turnover_ic": {"mean_ic": rm, "icir": ricir, "n_days": rnd},
        "pairwise_redundancy": pair_out,
        "overlap_with_qiangchou": ov_sum,
        "bucket_in_qc_vs_vratio_only": grp_out,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "vratio_deepdive_v48.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
