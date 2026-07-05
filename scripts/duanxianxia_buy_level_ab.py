#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0154: 买入级 A/B —— 当前代码口径下, 对比 baseline 买入闸门
与 0153 画像启发的 3 个变体的真实 top-N 胜率/赔率/回撤。

0151-0153 已证: 因子空间(线性重配/交互/去相关)榚干, 增量在"选股+风险层"。
0153 顶档画像: 赢家=高流动+高资金+中档竞价; 顶档 59% 带 risk_flag。
本 job 在买入档(实际就买这些)验证:
  baseline    = 当前 REGIME_ACTION_GATE (_assign_actions) 选出的 BUY
  risk_strict = 先剔除 risk_flag 再走同一闸门
  profile     = 先要求 liquidity>=当日中位 且 money>=当日中位 再走闸门
  cap_auction = 先剔除 auction_strength 当日 top10% 极值 再走闸门
口径: 当前 compute_edge_v9 重算 edge/risk (无历史污染); excess=次日(收盘-竞价)/preclose。
纯现有数据; 加法; 可回退。
"""
from __future__ import annotations
import json, statistics, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import Daily, DEFAULT_PROJECT_ROOT
from duanxianxia_v9_edge import compute_edge_v9
from duanxianxia_v9_output import _assign_actions, _regime_label


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


def _median(xs):
    return statistics.median(xs) if xs else 0.0


def _pct_threshold(xs, q):
    if not xs:
        return None
    s = sorted(xs)
    idx = min(len(s) - 1, int(q * len(s)))
    return s[idx]


def _agg(exs):
    if not exs:
        return {"n": 0, "mean_excess": None, "win_rate": None, "avg_win": None, "avg_loss": None, "payoff": None}
    wins = [e for e in exs if e > 0]
    losses = [e for e in exs if e <= 0]
    aw = statistics.mean(wins) if wins else None
    al = statistics.mean(losses) if losses else None
    payoff = (aw / abs(al)) if (aw is not None and al is not None and al != 0) else None
    return {
        "n": len(exs),
        "mean_excess": round(statistics.mean(exs), 3),
        "win_rate": round(len(wins) / len(exs), 3),
        "avg_win": round(aw, 3) if aw is not None else None,
        "avg_loss": round(al, 3) if al is not None else None,
        "payoff": round(payoff, 3) if payoff is not None else None,
    }


def _buy_exs(rows, market_env, meta):
    ranked = sorted(rows, key=lambda r: r["edge_score"], reverse=True)
    work = [{"edge_score": r["edge_score"], "risk_flag": r["risk_flag"], "alpha_type": r["alpha_type"], "_ex": r["_ex"]} for r in ranked]
    _assign_actions(work, market_env, meta)
    return [r["_ex"] for r in work if r.get("action_type") == "BUY"]


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

    variants = ["baseline", "risk_strict", "profile", "cap_auction"]
    acc = {v: {"all": [], "cold": [], "ctw": []} for v in variants}
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
            out = compute_edge_v9(_decision(c), market_env, {})
            sub = out.get("edge_components", {}).get("sub", {})
            rows.append({
                "code": c.get("code"),
                "edge_score": float(out.get("edge_score") or 0.0),
                "risk_flag": bool(out.get("risk_flag")),
                "alpha_type": out.get("alpha_type"),
                "liq": float(sub.get("liquidity", 0.0) or 0.0),
                "money": float(sub.get("money", 0.0) or 0.0),
                "auc": float(sub.get("auction_strength", 0.0) or 0.0),
                "_ex": ex,
            })
        if len(rows) < 20:
            continue
        n_days += 1
        regime_days[regime] = regime_days.get(regime, 0) + 1
        med_liq = _median([r["liq"] for r in rows])
        med_money = _median([r["money"] for r in rows])
        auc_cap = _pct_threshold([r["auc"] for r in rows], 0.90)
        subsets = {
            "baseline": rows,
            "risk_strict": [r for r in rows if not r["risk_flag"]],
            "profile": [r for r in rows if r["liq"] >= med_liq and r["money"] >= med_money],
            "cap_auction": [r for r in rows if auc_cap is None or r["auc"] < auc_cap],
        }
        for v in variants:
            exs = _buy_exs(subsets[v], market_env, meta)
            acc[v]["all"].extend(exs)
            if regime == "cold":
                acc[v]["cold"].extend(exs)
            elif regime == "cold_to_warming":
                acc[v]["ctw"].extend(exs)

    report = {"job": "0154_buy_level_ab", "n_days": n_days, "regime_days": regime_days, "variants": {}}
    for v in variants:
        report["variants"][v] = {
            "overall": _agg(acc[v]["all"]),
            "cold": _agg(acc[v]["cold"]),
            "cold_to_warming": _agg(acc[v]["ctw"]),
        }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "buy_level_ab_0154.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = ["n_days=%s regime_days=%s" % (n_days, regime_days),
             "variant       scope            n    mean_ex  win    avg_win  avg_loss  payoff"]
    for v in variants:
        for scope in ("overall", "cold", "cold_to_warming"):
            a = report["variants"][v][scope]
            lines.append("%-12s %-15s %-4s %-8s %-6s %-8s %-9s %s" % (
                v, scope, a["n"], a["mean_excess"], a["win_rate"], a["avg_win"], a["avg_loss"], a["payoff"]))
    print(chr(10).join(lines))


if __name__ == "__main__":
    main()
