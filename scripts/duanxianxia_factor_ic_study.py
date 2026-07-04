#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0151: full-universe factor IC study on EXISTING data.

充分挖掘现有数据: 每天 all_candidates(~400) x 20天 ≈ 8000 条
「打分 + 次日真实超额」样本。对每个候选日:
 - target = daily.excess(code,date) = (close-open)/preclose*100
 - 自动发现 full.{auction_detail,weimai_detail,theme_detail,context_detail,
   edge_components,risk_detail} + 顶层标量 中所有数值字段
逐日做横截面 Spearman rank-IC, 再跨日平均(mean_ic / IR / 胜日率 / 覆盖率)。
另对 edge_score 做分位单调性检验(整体 + 分 regime)。
不加任何新数据, 纯粹把已有数据吃透。
"""
from __future__ import annotations
import json, math, statistics, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import Daily, DEFAULT_PROJECT_ROOT
from duanxianxia_v9_output import _regime_label

SUBDICTS = ["auction_detail", "weimai_detail", "theme_detail", "context_detail", "edge_components", "risk_detail"]
TOP_SCALARS = ["edge_score", "auction_strength", "theme_strength_t0", "auction_amount_wan", "auction_pct", "auction_amount_pct"]


def _isnum(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and not (isinstance(v, float) and math.isnan(v))


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


def _flatten(cand):
    out = {}
    for k in TOP_SCALARS:
        v = cand.get(k)
        if _isnum(v):
            out[k] = v
    full = cand.get("full") or {}
    for sd in SUBDICTS:
        d = full.get(sd)
        if not isinstance(d, dict):
            d = cand.get(sd)
        if isinstance(d, dict):
            for k, v in d.items():
                if _isnum(v):
                    out[sd + "." + k] = v
    return out


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

    fac_daily = {}
    fac_cov = {}
    edge_decile_all = []
    edge_decile_by_regime = {}
    regime_days = {}
    n_days = 0
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
            rows.append((ex, _flatten(c)))
        if len(rows) < 20:
            continue
        n_days += 1
        regime_days[regime] = regime_days.get(regime, 0) + 1
        n = len(rows)
        keys = set()
        for _, feats in rows:
            keys.update(feats.keys())
        for k in keys:
            pair = [(feats[k], ex) for ex, feats in rows if k in feats]
            if len(pair) >= 15:
                ic = _spearman([p[0] for p in pair], [p[1] for p in pair])
                if ic is not None:
                    fac_daily.setdefault(k, []).append(ic)
                    fac_cov.setdefault(k, []).append(len(pair) / n)
        edge_rows = [(feats["edge_score"], ex) for ex, feats in rows if "edge_score" in feats]
        if len(edge_rows) >= 20:
            edge_rows.sort(key=lambda t: t[0])
            m = len(edge_rows)
            dec = []
            for q in range(10):
                a = m * q // 10
                b = m * (q + 1) // 10
                seg = [t[1] for t in edge_rows[a:b]]
                dec.append(statistics.mean(seg) if seg else None)
            edge_decile_all.append(dec)
            edge_decile_by_regime.setdefault(regime, []).append(dec)

    def summarize(k):
        ics = fac_daily.get(k, [])
        if len(ics) < 5:
            return None
        mic = statistics.mean(ics)
        sd = statistics.pstdev(ics) if len(ics) > 1 else 0.0
        ir = (mic / sd) if sd > 0 else None
        pos = sum(1 for x in ics if x > 0) / len(ics)
        return {"n_days": len(ics), "avg_cov": round(statistics.mean(fac_cov[k]), 3),
                "mean_ic": round(mic, 4), "ir": round(ir, 3) if ir is not None else None,
                "pos_day_frac": round(pos, 3)}

    facs = {}
    for k in fac_daily:
        s = summarize(k)
        if s is not None:
            facs[k] = s
    ranked = sorted(facs.items(), key=lambda kv: abs(kv[1]["mean_ic"]), reverse=True)

    def avg_decile(lst):
        if not lst:
            return None
        out = []
        for q in range(10):
            vals = [d[q] for d in lst if d[q] is not None]
            out.append(round(statistics.mean(vals), 3) if vals else None)
        return out

    report = {
        "job": "0151_factor_ic_study",
        "method": "daily cross-sectional Spearman rank-IC vs next-day excess; mean across days",
        "n_days": n_days,
        "regime_days": regime_days,
        "edge_score_ic": facs.get("edge_score"),
        "edge_decile_mean_excess_all": avg_decile(edge_decile_all),
        "edge_decile_by_regime": {r: avg_decile(v) for r, v in edge_decile_by_regime.items()},
        "factors": {k: v for k, v in ranked},
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "factor_ic_study_0151.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("n_days=%s regime_days=%s" % (n_days, regime_days))
    lines.append("edge_score_ic=%s" % (facs.get("edge_score"),))
    lines.append("edge_decile_all=%s" % (avg_decile(edge_decile_all),))
    for r, v in edge_decile_by_regime.items():
        lines.append("edge_decile[%s]=%s" % (r, avg_decile(v)))
    lines.append("TOP_FACTORS_by_absIC (mean_ic ir posd nd cov factor):")
    for k, v in ranked[:22]:
        lines.append("  %+.4f  ir=%s  posd=%s  nd=%s  cov=%s  %s" % (v["mean_ic"], v["ir"], v["pos_day_frac"], v["n_days"], v["avg_cov"], k))
    print(chr(10).join(lines))


if __name__ == "__main__":
    main()
