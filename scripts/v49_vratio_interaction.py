#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v49_vratio_interaction.py
Non-linear / conditional follow-up on vratio (auction.jjyd.vratio).
User hypothesis: volume_ratio_multiple (fangliang) and auction_volume_ratio (liangbi)
may add value NOT as standalone linear factors, but in INTERACTION with absolute
auction turnover. Requiring absolute turnover to ALSO be large filters out the
small-base contamination (tiny yesterday turnover -> huge ratio = micro junk).

For each premarket date (snapshot HHMMSS<=093000), within vratio universe:
  baseline pooled excess (raw + day-demeaned)
  For pair (absolute turnover) x (ratio in {volume_ratio_multiple, auction_volume_ratio}):
    - 2x2 quadrant by within-day median rank: HH/HL/LH/LL excess
    - double-high gates: both top 1/3, top 20%, top 10% -> demeaned excess
    - conditional IC: ratio IC within high-turnover half; turnover IC within high-ratio half
    - double-high monotone factor min(rank_turn, rank_ratio) IC
    - product interaction factor (rank_turn-0.5)*(rank_ratio-0.5) IC
excess=(close-open)/preclose*100. Output reports/_audit/vratio_interaction_v49.json
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

DSID = "auction.jjyd.vratio"
PREOPEN = "093000"
CODE_KEYS = ["code", "\u4ee3\u7801"]
YI = "\u4ebf"
WAN = "\u4e07"
TURN = "auction_turnover_wan"
RATIOS = ["volume_ratio_multiple", "auction_volume_ratio"]


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


def summ(raw, dm):
    return {
        "n": len(raw),
        "mean_excess": round(mean(raw), 3) if raw else None,
        "mean_excess_demeaned": round(mean(dm), 3) if dm else None,
    }


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []

    base_raw, base_dm = [], []
    n_dates = 0

    pairs_out = {}
    for ratio in RATIOS:
        quad_raw = defaultdict(list)
        quad_dm = defaultdict(list)
        gate_dm = defaultdict(list)
        cond_ic_ratio_in_highturn = []
        cond_ic_turn_in_highratio = []
        minrank_ic = []
        prod_ic = []
        pairs_out[ratio] = {
            "_quad_raw": quad_raw, "_quad_dm": quad_dm, "_gate_dm": gate_dm,
            "_cir": cond_ic_ratio_in_highturn, "_cit": cond_ic_turn_in_highratio,
            "_mr": minrank_ic, "_pr": prod_ic,
        }

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
        for _, _, e in exrows:
            base_raw.append(e)
            base_dm.append(e - dme)

        for ratio in RATIOS:
            sub = []
            for c, r, e in exrows:
                tv = pnum(r.get(TURN))
                rv = pnum(r.get(ratio))
                if tv is not None and rv is not None:
                    sub.append((tv, rv, e))
            if len(sub) < 8:
                continue
            tvals = [s[0] for s in sub]
            rvals = [s[1] for s in sub]
            evals = [s[2] for s in sub]
            tr = pranks(tvals)
            rr = pranks(rvals)
            P = pairs_out[ratio]
            for i in range(len(sub)):
                hi_t = tr[i] >= 0.5
                hi_r = rr[i] >= 0.5
                b = ("H" if hi_t else "L") + ("H" if hi_r else "L")
                P["_quad_raw"][b].append(evals[i])
                P["_quad_dm"][b].append(evals[i] - dme)
                if tr[i] >= 2.0 / 3.0 and rr[i] >= 2.0 / 3.0:
                    P["_gate_dm"]["both_top33"].append(evals[i] - dme)
                if tr[i] >= 0.8 and rr[i] >= 0.8:
                    P["_gate_dm"]["both_top20"].append(evals[i] - dme)
                if tr[i] >= 0.9 and rr[i] >= 0.9:
                    P["_gate_dm"]["both_top10"].append(evals[i] - dme)
            # conditional IC: ratio within high-turnover half
            xr, yr = [], []
            for i in range(len(sub)):
                if tr[i] >= 0.5:
                    xr.append(rvals[i]); yr.append(evals[i])
            if len(xr) >= 8:
                ic = v10.spearman(xr, yr)
                if ic is not None:
                    P["_cir"].append(ic)
            # conditional IC: turnover within high-ratio half
            xt, yt = [], []
            for i in range(len(sub)):
                if rr[i] >= 0.5:
                    xt.append(tvals[i]); yt.append(evals[i])
            if len(xt) >= 8:
                ic = v10.spearman(xt, yt)
                if ic is not None:
                    P["_cit"].append(ic)
            # double-high monotone factor min(rank)
            mr = [min(tr[i], rr[i]) for i in range(len(sub))]
            ic = v10.spearman(mr, evals)
            if ic is not None:
                P["_mr"].append(ic)
            # product interaction factor
            pr = [(tr[i] - 0.5) * (rr[i] - 0.5) for i in range(len(sub))]
            ic = v10.spearman(pr, evals)
            if ic is not None:
                P["_pr"].append(ic)

    out_pairs = {}
    for ratio in RATIOS:
        P = pairs_out[ratio]
        quad = []
        for b in ["HH", "HL", "LH", "LL"]:
            quad.append({"bucket": b, **summ(P["_quad_raw"].get(b, []), P["_quad_dm"].get(b, []))})
        gates = {}
        for g in ["both_top33", "both_top20", "both_top10"]:
            dm = P["_gate_dm"].get(g, [])
            gates[g] = {"n": len(dm), "mean_excess_demeaned": round(mean(dm), 3) if dm else None}
        cir = v10.mean_icir(P["_cir"])
        cit = v10.mean_icir(P["_cit"])
        mr = v10.mean_icir(P["_mr"])
        pr = v10.mean_icir(P["_pr"])
        out_pairs["turnover_x_" + ratio] = {
            "quadrant_median_split": quad,
            "double_high_gates": gates,
            "cond_ic_ratio_within_high_turnover": {"mean_ic": cir[0], "icir": cir[1], "n_days": cir[2]},
            "cond_ic_turnover_within_high_ratio": {"mean_ic": cit[0], "icir": cit[1], "n_days": cit[2]},
            "double_high_minrank_ic": {"mean_ic": mr[0], "icir": mr[1], "n_days": mr[2]},
            "product_interaction_ic": {"mean_ic": pr[0], "icir": pr[1], "n_days": pr[2]},
        }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "vratio_interaction_v49",
        "dataset": DSID,
        "n_dates": n_dates,
        "baseline": {"n": len(base_raw),
                      "mean_excess": round(mean(base_raw), 3) if base_raw else None,
                      "mean_excess_demeaned": round(mean(base_dm), 3) if base_dm else None},
        "pairs": out_pairs,
        "note": "HH=both high vs market-demeaned; gates require turnover AND ratio jointly in top tier; cond IC tests incremental ranking power inside the other factor's high regime.",
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "vratio_interaction_v49.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
