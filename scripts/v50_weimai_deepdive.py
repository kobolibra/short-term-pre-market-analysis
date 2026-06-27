#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v50_weimai_deepdive.py
Deep-dive on premarket weimai table (auction.jjyd.weimai = jingjia yidong/zhangting weimai,
limit-up bid-to-buy). ~150 rows/day. Unique vs qiangchou/vratio: premarket money-flow
decomposition (main_net_inflow_full / super_large_net_inflow / large_order_net_inflow),
market_cap, seal_volume.
Priors (0050): main_net_inflow_full IC 0.103/0.554, super_large_net_inflow 0.094/0.548.

Key questions:
  1) coverage of inflow/mktcap/seal fields
  2) field IC (own n) for all numeric fields
  3) NORMALIZED inflow-intensity: is inflow a real signal or just a size proxy?
     net/turnover, superlarge/turnover, net/mktcap, superlarge/mktcap
  4) redundancy (avg daily spearman) among turnover/inflow/mktcap/turnoverrate
  5) INTERACTION turnover x main_net_inflow_full: quadrant + conditional IC + gates
     (applying table-3 lesson: regime matters, test non-linear)
  6) inflow SIGN bucket (positive vs negative net inflow despite being on the list)
  7) board_label categorical buckets (yesterday-board vs today-sealed)
  8) overlap with qiangchou universe
excess=(close-open)/preclose*100. Premarket snapshot only (HHMMSS<=093000).
Output reports/_audit/weimai_deepdive_v50.json
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

DSID = "auction.jjyd.weimai"
QC_DSID = "auction.jjyd.qiangchou"
PREOPEN = "093000"
CODE_KEYS = ["code", "\u4ee3\u7801"]
YI = "\u4ebf"
WAN = "\u4e07"

FIELDS = {
    "auction_turnover_wan": "auction_turnover_wan",
    "turnover_rate_pct": "turnover_rate_pct",
    "auction_change_pct": "auction_change_pct",
    "latest_change_pct": "latest_change_pct",
    "main_net_inflow_full": "main_net_inflow_full",
    "main_net_inflow_wan": "main_net_inflow_wan",
    "super_large_net_inflow": "super_large_net_inflow",
    "large_order_net_inflow": "large_order_net_inflow",
    "seal_volume": "seal_volume",
    "auction_amount_wan": "auction_amount_wan",
    "market_cap_yi": "market_cap_yi",
    "seal_amount_wan": "seal_amount_wan",
}
COVER = ["main_net_inflow_full", "super_large_net_inflow", "large_order_net_inflow",
         "seal_amount_wan", "market_cap_yi", "auction_amount_wan"]
PAIR_FIELDS = ["auction_turnover_wan", "main_net_inflow_full", "super_large_net_inflow",
               "large_order_net_inflow", "turnover_rate_pct", "market_cap_yi", "auction_change_pct"]


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


def pranks(vals):
    n = len(vals)
    if n == 0:
        return []
    if n == 1:
        return [0.5]
    rk = v10.rankdata(vals)
    return [(r - 1.0) / (n - 1.0) for r in rk]


def safe_div(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []

    cov = {f: [0, 0] for f in COVER}
    ic_fields = defaultdict(list)
    norm_ic = defaultdict(list)
    pair_corr = defaultdict(list)
    quad_raw = defaultdict(list)
    quad_dm = defaultdict(list)
    gate_dm = defaultdict(list)
    cond_ic_inflow_in_highturn = []
    cond_ic_turn_in_highinflow = []
    sign_raw = defaultdict(list)
    sign_dm = defaultdict(list)
    board_raw = defaultdict(list)
    board_dm = defaultdict(list)
    overlap_stats = []
    n_dates = 0

    for dd in date_dirs:
        D = dd.name
        rows = load_rows(dd, DSID)
        if not rows:
            continue
        byc = {}
        for r in rows:
            c = code_of(r)
            if c and c not in byc:
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
        dme = mean([e for _, _, e in exrows])
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
        # normalized inflow-intensity factors
        norm_defs = {
            "main_net_over_turnover": ("main_net_inflow_full", "auction_turnover"),
            "superlarge_over_turnover": ("super_large_net_inflow", "auction_turnover"),
            "main_net_over_mktcap": ("main_net_inflow_full", "market_cap"),
            "superlarge_over_mktcap": ("super_large_net_inflow", "market_cap"),
        }
        for nm, (numf, denf) in norm_defs.items():
            xs, ys = [], []
            for c, r, e in exrows:
                num = pnum(r.get(numf))
                den = pnum(r.get(denf))
                v = safe_div(num, den)
                if v is not None:
                    xs.append(v)
                    ys.append(e)
            if len(xs) >= 8:
                ic = v10.spearman(xs, ys)
                if ic is not None:
                    norm_ic[nm].append(ic)
        # pairwise redundancy
        colvals = {}
        for key in PAIR_FIELDS:
            colvals[key] = [pnum(r.get(key)) for c, r, e in exrows]
        for a, b in combinations(PAIR_FIELDS, 2):
            xs, ys = [], []
            for va, vb in zip(colvals[a], colvals[b]):
                if va is not None and vb is not None:
                    xs.append(va)
                    ys.append(vb)
            if len(xs) >= 8:
                cc = v10.spearman(xs, ys)
                if cc is not None:
                    pair_corr[a + " ~ " + b].append(cc)
        # interaction turnover x main_net_inflow_full
        sub = []
        for c, r, e in exrows:
            tv = pnum(r.get("auction_turnover_wan"))
            iv = pnum(r.get("main_net_inflow_full"))
            if tv is not None and iv is not None:
                sub.append((tv, iv, e))
        if len(sub) >= 8:
            tr = pranks([s[0] for s in sub])
            ir = pranks([s[1] for s in sub])
            for i in range(len(sub)):
                b = ("H" if tr[i] >= 0.5 else "L") + ("H" if ir[i] >= 0.5 else "L")
                quad_raw[b].append(sub[i][2])
                quad_dm[b].append(sub[i][2] - dme)
                if tr[i] >= 2.0 / 3.0 and ir[i] >= 2.0 / 3.0:
                    gate_dm["both_top33"].append(sub[i][2] - dme)
                if tr[i] >= 0.8 and ir[i] >= 0.8:
                    gate_dm["both_top20"].append(sub[i][2] - dme)
            xr, yr = [], []
            for i in range(len(sub)):
                if tr[i] >= 0.5:
                    xr.append(sub[i][1]); yr.append(sub[i][2])
            if len(xr) >= 8:
                ic = v10.spearman(xr, yr)
                if ic is not None:
                    cond_ic_inflow_in_highturn.append(ic)
            xt, yt = [], []
            for i in range(len(sub)):
                if ir[i] >= 0.5:
                    xt.append(sub[i][0]); yt.append(sub[i][2])
            if len(xt) >= 8:
                ic = v10.spearman(xt, yt)
                if ic is not None:
                    cond_ic_turn_in_highinflow.append(ic)
        # inflow sign bucket
        for c, r, e in exrows:
            iv = pnum(r.get("main_net_inflow_full"))
            if iv is None:
                continue
            b = "net_inflow_pos" if iv > 0 else "net_inflow_neg"
            sign_raw[b].append(e)
            sign_dm[b].append(e - dme)
        # board_label bucket
        for c, r, e in exrows:
            lbl = str(r.get("board_label") or "").strip()
            if not lbl:
                lbl = "(none)"
            board_raw[lbl].append(e)
            board_dm[lbl].append(e - dme)
        # overlap with qiangchou
        qc_rows = load_rows(dd, QC_DSID)
        qc_codes = set(code_of(r) for r in qc_rows if code_of(r))
        wm_codes = set(c for c, _, _ in exrows)
        if qc_codes:
            inter = wm_codes & qc_codes
            overlap_stats.append({
                "date": D,
                "n_weimai": len(wm_codes),
                "n_qiangchou": len(qc_codes),
                "n_overlap": len(inter),
                "weimai_in_qc_pct": round(len(inter) / len(wm_codes), 3) if wm_codes else None,
            })

    field_out = []
    for k in FIELDS:
        m, icir, nd = v10.mean_icir(ic_fields.get(k, []))
        field_out.append({"field": k, "mean_ic": m, "icir": icir, "n_days": nd})
    field_out.sort(key=lambda x: (x["mean_ic"] if x["mean_ic"] is not None else -9), reverse=True)
    norm_out = []
    for k in ["main_net_over_turnover", "superlarge_over_turnover", "main_net_over_mktcap", "superlarge_over_mktcap"]:
        m, icir, nd = v10.mean_icir(norm_ic.get(k, []))
        norm_out.append({"factor": k, "mean_ic": m, "icir": icir, "n_days": nd})
    norm_out.sort(key=lambda x: (x["mean_ic"] if x["mean_ic"] is not None else -9), reverse=True)
    cov_out = {f: {"present": cov[f][0], "total": cov[f][1],
                   "pct": round(cov[f][0] / cov[f][1], 3) if cov[f][1] else None} for f in COVER}
    pair_out = []
    for k, v in pair_corr.items():
        if v:
            pair_out.append({"pair": k, "avg_spearman": round(sum(v) / len(v), 3), "n_days": len(v)})
    pair_out.sort(key=lambda x: abs(x["avg_spearman"]), reverse=True)
    quad = []
    for b in ["HH", "HL", "LH", "LL"]:
        rr = quad_raw.get(b, [])
        dd2 = quad_dm.get(b, [])
        quad.append({"bucket": b, "n": len(rr),
                     "mean_excess": round(mean(rr), 3) if rr else None,
                     "mean_excess_demeaned": round(mean(dd2), 3) if dd2 else None})
    gates = {}
    for g in ["both_top33", "both_top20"]:
        dm = gate_dm.get(g, [])
        gates[g] = {"n": len(dm), "mean_excess_demeaned": round(mean(dm), 3) if dm else None}
    cir = v10.mean_icir(cond_ic_inflow_in_highturn)
    cit = v10.mean_icir(cond_ic_turn_in_highinflow)
    sign_out = []
    for b in sign_raw:
        sign_out.append({"bucket": b, "n": len(sign_raw[b]),
                         "mean_excess": round(mean(sign_raw[b]), 3) if sign_raw[b] else None,
                         "mean_excess_demeaned": round(mean(sign_dm[b]), 3) if sign_dm[b] else None})
    sign_out.sort(key=lambda x: x["n"], reverse=True)
    board_out = []
    for b in board_raw:
        if len(board_raw[b]) >= 10:
            board_out.append({"label": b, "n": len(board_raw[b]),
                              "mean_excess": round(mean(board_raw[b]), 3),
                              "mean_excess_demeaned": round(mean(board_dm[b]), 3)})
    board_out.sort(key=lambda x: x["mean_excess_demeaned"], reverse=True)
    ov_sum = {}
    if overlap_stats:
        ov_sum = {
            "n_days": len(overlap_stats),
            "avg_n_weimai": round(mean([o["n_weimai"] for o in overlap_stats]), 1),
            "avg_n_qiangchou": round(mean([o["n_qiangchou"] for o in overlap_stats]), 1),
            "avg_n_overlap": round(mean([o["n_overlap"] for o in overlap_stats]), 1),
            "avg_weimai_in_qc_pct": round(mean([o["weimai_in_qc_pct"] for o in overlap_stats if o["weimai_in_qc_pct"] is not None]), 3),
        }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "weimai_deepdive_v50",
        "dataset": DSID,
        "n_dates": n_dates,
        "coverage": cov_out,
        "field_ic": field_out,
        "normalized_inflow_intensity_ic": norm_out,
        "pairwise_redundancy": pair_out,
        "interaction_turnover_x_main_net_inflow": {
            "quadrant_median_split": quad,
            "double_high_gates": gates,
            "cond_ic_inflow_within_high_turnover": {"mean_ic": cir[0], "icir": cir[1], "n_days": cir[2]},
            "cond_ic_turnover_within_high_inflow": {"mean_ic": cit[0], "icir": cit[1], "n_days": cit[2]},
        },
        "inflow_sign_bucket": sign_out,
        "board_label_bucket": board_out,
        "overlap_with_qiangchou": ov_sum,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "weimai_deepdive_v50.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
