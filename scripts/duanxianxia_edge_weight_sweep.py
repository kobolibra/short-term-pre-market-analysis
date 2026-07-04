#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0152: edge weight A/B sweep on the FULL candidate universe.

用 compute_edge_v9 的 params 覆盖 7 个权重, 对每个候选日(~400 只 x 20 天)
重算 edge_score, 量化每组权重的日内横截面 rank-IC + 分位单调性(vs 次日超额)。
纯重配现有因子, 不加新数据; production 口径, 真持久化输入。
"""
from __future__ import annotations
import json, math, statistics, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import Daily, DEFAULT_PROJECT_ROOT
from duanxianxia_v9_edge import compute_edge_v9
from duanxianxia_v9_output import _regime_label

WEIGHTS = {
    "baseline": {},
    "ic_prop": {"edge_w_amt": 0.16, "edge_w_auction": 0.26, "edge_w_liquidity": 0.23, "edge_w_money": 0.16, "edge_w_pressure": 0.07, "edge_w_weimai": 0.07, "edge_w_orderbook": 0.05},
    "auction_heavy": {"edge_w_amt": 0.08, "edge_w_auction": 0.34, "edge_w_liquidity": 0.28, "edge_w_money": 0.15, "edge_w_pressure": 0.07, "edge_w_weimai": 0.04, "edge_w_orderbook": 0.04},
    "full_cov": {"edge_w_amt": 0.05, "edge_w_auction": 0.38, "edge_w_liquidity": 0.30, "edge_w_money": 0.17, "edge_w_pressure": 0.05, "edge_w_weimai": 0.03, "edge_w_orderbook": 0.02},
    "drop_dead": {"edge_w_amt": 0.05, "edge_w_auction": 0.40, "edge_w_liquidity": 0.32, "edge_w_money": 0.18, "edge_w_pressure": 0.0, "edge_w_weimai": 0.03, "edge_w_orderbook": 0.02},
}


def _rankdata(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _spearman(xs, ys):
    if len(xs) < 8:
        return None
    rx = _rankdata(xs)
    ry = _rankdata(ys)
    mx = statistics.mean(rx)
    my = statistics.mean(ry)
    num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    dx = math.sqrt(sum((a - mx) ** 2 for a in rx))
    dy = math.sqrt(sum((b - my) ** 2 for b in ry))
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def _decision(cand):
    full = cand.get("full") or {}
    return {
        "code": cand.get("code"),
        "auction_strength": cand.get("auction_strength"),
        "theme_strength_t0": cand.get("theme_strength_t0"),
        "auction_pct": cand.get("auction_pct"),
        "auction_detail": full.get("auction_detail") or cand.get("auction_detail") or {},
        "weimai_detail": full.get("weimai_detail") or {},
        "theme_detail": full.get("theme_detail") or {},
        "context_detail": full.get("context_detail") or {},
    }


def main():
    root = Path(DEFAULT_PROJECT_ROOT)
    daily = Daily(root)
    rep = root / "reports"
    day_files = []
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        fs = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if fs:
            day_files.append((dd.name, fs[-1]))

    agg = {name: {"ic": [], "cold": [], "ctw": [], "dec": []} for name in WEIGHTS}
    n_days = 0
    regime_days = {}
    for date, f in day_files:
        try:
            analysis = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list) or not cands:
            continue
        meta = analysis.get("meta") or {}
        market_env = meta.get("market_env") or analysis.get("market_env") or {}
        regime = _regime_label(market_env, meta) or "(unknown)"
        rows = []
        for c in cands:
            if not isinstance(c, dict) or not c.get("code"):
                continue
            ex = daily.excess(c.get("code"), date)
            if ex is None:
                continue
            rows.append((ex, _decision(c)))
        if len(rows) < 20:
            continue
        n_days += 1
        regime_days[regime] = regime_days.get(regime, 0) + 1
        for name, W in WEIGHTS.items():
            scored = []
            for ex, dcn in rows:
                es = compute_edge_v9(dcn, market_env, W)["edge_score"]
                scored.append((es, ex))
            ic = _spearman([s for s, _ in scored], [e for _, e in scored])
            if ic is not None:
                agg[name]["ic"].append(ic)
                if regime == "cold":
                    agg[name]["cold"].append(ic)
                elif regime == "cold_to_warming":
                    agg[name]["ctw"].append(ic)
            scored.sort(key=lambda t: t[0])
            m = len(scored)
            decl = []
            for q in range(10):
                a = m * q // 10
                b = m * (q + 1) // 10
                seg = [t[1] for t in scored[a:b]]
                decl.append(statistics.mean(seg) if seg else None)
            agg[name]["dec"].append(decl)

    def mean(xs):
        return statistics.mean(xs) if xs else None

    def avg_decile(lst):
        out = []
        for q in range(10):
            vals = [d[q] for d in lst if d[q] is not None]
            out.append(round(statistics.mean(vals), 3) if vals else None)
        return out

    report = {"job": "0152_edge_weight_sweep", "n_days": n_days, "regime_days": regime_days, "variants": {}}
    for name in WEIGHTS:
        ics = agg[name]["ic"]
        sd = statistics.pstdev(ics) if len(ics) > 1 else 0.0
        dec = avg_decile(agg[name]["dec"])
        top = dec[9]
        bot = dec[0]
        report["variants"][name] = {
            "mean_ic": round(mean(ics), 4) if ics else None,
            "ir": round(mean(ics) / sd, 3) if ics and sd > 0 else None,
            "pos_day_frac": round(sum(1 for x in ics if x > 0) / len(ics), 3) if ics else None,
            "cold_ic": round(mean(agg[name]["cold"]), 4) if agg[name]["cold"] else None,
            "ctw_ic": round(mean(agg[name]["ctw"]), 4) if agg[name]["ctw"] else None,
            "decile_top": top,
            "decile_bottom": bot,
            "top_minus_bottom": round(top - bot, 3) if (top is not None and bot is not None) else None,
            "decile": dec,
            "weights": WEIGHTS[name] if WEIGHTS[name] else "current-defaults",
        }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "edge_weight_sweep_0152.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = ["n_days=%s regime_days=%s" % (n_days, regime_days),
             "variant            mean_ic   ir      posd    cold_ic   ctw_ic   topDec   botDec   spread"]
    for name in WEIGHTS:
        v = report["variants"][name]
        lines.append("%-16s  %+.4f  %-6s  %-5s  %+.4f  %-7s  %-6s  %-6s  %s" % (
            name, v["mean_ic"], v["ir"], v["pos_day_frac"],
            v["cold_ic"] if v["cold_ic"] is not None else 0.0,
            v["ctw_ic"], v["decile_top"], v["decile_bottom"], v["top_minus_bottom"]))
    lines.append("baseline_decile=%s" % (report["variants"]["baseline"]["decile"],))
    lines.append("best_by_spread=%s" % (max(WEIGHTS, key=lambda nm: (report["variants"][nm]["top_minus_bottom"] or -9)),))
    print(chr(10).join(lines))


if __name__ == "__main__":
    main()
