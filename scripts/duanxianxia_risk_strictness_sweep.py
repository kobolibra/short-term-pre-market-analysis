#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0155: 风险剔除严格度 参数敏感性 A/B —— 0154 冠军 risk_strict 的后续验证。

0154 定论: risk_strict(过闸门前剔除任何 risk_flag 行)在买入档全面胜出
  overall win 0.615->0.769 / payoff 1.744->2.322(不塌) / avg_loss -3.69->-2.97;
  代价是买入样本减半(26->13, cold 17->6)。profile=no-op; cap_auction 有害。
本 job 做参数敏感性: 不再"一刀切剔除任何 risk_flag", 而是按 risk_penalty 阈值
渐进剔除, 并按风险类别单独剔除, 回答:
  1) 胜率提升是否随严格度单调 (还是被少数硬否决行主导)?
  2) 温和阈值能否在保留更多买入样本的同时保住提升?
  3) 哪类风险 (low_liquidity / high_board / high_open_cost / hard_veto) 贡献最大?
口径完全对齐 0154(复用其 helpers), 当前 compute_edge_v9 重算, excess=次日(收盘-竞价)/preclose。
纯现有数据; 加法; 可回退; 不改口径/字段名。
"""
from __future__ import annotations
import json, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import Daily, DEFAULT_PROJECT_ROOT
from duanxianxia_v9_edge import compute_edge_v9
from duanxianxia_v9_output import _regime_label
# 复用 0154 的口径/闸门/汇总 helpers, 保证与 baseline A/B 完全同口径
from duanxianxia_buy_level_ab import _decision, _agg, _buy_exs

PEN_THRESHOLDS = [10.0, 20.0, 30.0, 45.0]


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

    variants = ["baseline", "risk_strict"]
    variants += ["pen_ge_%d" % int(t) for t in PEN_THRESHOLDS]
    variants += ["veto_only", "drop_lowliq", "drop_highboard", "drop_highopen"]
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
            rd = out.get("risk_detail") or {}
            rows.append({
                "code": c.get("code"),
                "edge_score": float(out.get("edge_score") or 0.0),
                "risk_flag": bool(out.get("risk_flag")),
                "alpha_type": out.get("alpha_type"),
                "risk_pen": float(out.get("edge_components", {}).get("risk_penalty", 0.0) or 0.0),
                "rd_veto": bool(rd.get("hard_veto")),
                "rd_lowliq": "low_liquidity" in rd,
                "rd_highboard": "high_board_position" in rd,
                "rd_highopen": "high_open_cost" in rd,
                "_ex": ex,
            })
        if len(rows) < 20:
            continue
        n_days += 1
        regime_days[regime] = regime_days.get(regime, 0) + 1

        subsets = {
            "baseline": rows,
            "risk_strict": [r for r in rows if not r["risk_flag"]],
            "veto_only": [r for r in rows if not r["rd_veto"]],
            "drop_lowliq": [r for r in rows if not r["rd_lowliq"]],
            "drop_highboard": [r for r in rows if not r["rd_highboard"]],
            "drop_highopen": [r for r in rows if not r["rd_highopen"]],
        }
        for t in PEN_THRESHOLDS:
            subsets["pen_ge_%d" % int(t)] = [r for r in rows if r["risk_pen"] < t]

        for v in variants:
            exs = _buy_exs(subsets[v], market_env, meta)
            acc[v]["all"].extend(exs)
            if regime == "cold":
                acc[v]["cold"].extend(exs)
            elif regime == "cold_to_warming":
                acc[v]["ctw"].extend(exs)

    report = {"job": "0155_risk_strictness_sweep", "n_days": n_days, "regime_days": regime_days,
              "pen_thresholds": PEN_THRESHOLDS, "variants": {}}
    for v in variants:
        report["variants"][v] = {
            "overall": _agg(acc[v]["all"]),
            "cold": _agg(acc[v]["cold"]),
            "cold_to_warming": _agg(acc[v]["ctw"]),
        }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "risk_strictness_sweep_0155.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = ["n_days=%s regime_days=%s" % (n_days, regime_days),
             "variant         scope            n    mean_ex  win    avg_win  avg_loss  payoff"]
    for v in variants:
        for scope in ("overall", "cold", "cold_to_warming"):
            a = report["variants"][v][scope]
            lines.append("%-15s %-15s %-4s %-8s %-6s %-8s %-9s %s" % (
                v, scope, a["n"], a["mean_excess"], a["win_rate"], a["avg_win"], a["avg_loss"], a["payoff"]))
    print(chr(10).join(lines))


if __name__ == "__main__":
    main()
