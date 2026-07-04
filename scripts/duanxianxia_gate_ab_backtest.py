#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_gate_ab_backtest.py — Task 0144.

历史 A/B 回测:把 OLD vs NEW REGIME_ACTION_GATE 分别套到已有历史 analysis_v9.json,
用真实 excess(收盘涨幅-竞价涨幅)对比两套门控选出的 BUY 名单的实盘表现。
不依赖周一实盘 —— 直接在过去的数据上量化 835dd9c 门控改动的效果。

复用 v10_optimize.Daily / DEFAULT_PROJECT_ROOT;复现 v9_output._assign_actions 的 BUY 判定。
输出 reports/_audit/gate_ab_backtest_0144.json + 紧凑 stdout 摘要(置尾防截断)。
"""
from __future__ import annotations
import json, statistics, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from v10_optimize import Daily, DEFAULT_PROJECT_ROOT

RISK_EXTRA_MARGIN = 8.0

OLD_GATE = {
    "cold":            {"buy_top_frac": 0.015, "buy_floor": 50.0, "max_buys": 1},
    "cold_to_warming": {"buy_top_frac": 0.030, "buy_floor": 48.0, "max_buys": 3},
    "warming":         {"buy_top_frac": 0.030, "buy_floor": 48.0, "max_buys": 3},
    "normal":          {"buy_top_frac": 0.050, "buy_floor": 45.0, "max_buys": 4},
    "hot":             {"buy_top_frac": 0.080, "buy_floor": 42.0, "max_buys": 5},
}
NEW_GATE = {
    "cold":            {"buy_top_frac": 0.020, "buy_floor": 50.0, "max_buys": 2},
    "cold_to_warming": {"buy_top_frac": 0.030, "buy_floor": 50.0, "max_buys": 2},
    "warming":         {"buy_top_frac": 0.030, "buy_floor": 50.0, "max_buys": 3},
    "normal":          {"buy_top_frac": 0.050, "buy_floor": 45.0, "max_buys": 4},
    "hot":             {"buy_top_frac": 0.080, "buy_floor": 42.0, "max_buys": 5},
}
DEFAULT_GATE = {"buy_top_frac": 0.030, "buy_floor": 48.0, "max_buys": 3}


def _edge(r):
    try:
        return float(r.get("edge_score") or 0)
    except Exception:
        return 0.0


def buys_for_gate(ranked, regime, gate_map):
    gate = gate_map.get(regime, DEFAULT_GATE)
    n = len(ranked)
    buy_floor = float(gate["buy_floor"])
    max_buys = int(gate["max_buys"])
    buy_rank_cap = max(1, int(round(n * float(gate["buy_top_frac"])))) if n else 0
    buys = []
    cnt = 0
    for idx, row in enumerate(ranked):
        edge = _edge(row)
        risk = bool(row.get("risk_flag"))
        floor = buy_floor + (RISK_EXTRA_MARGIN if risk else 0.0)
        if (idx < buy_rank_cap) and (edge >= floor) and (cnt < max_buys):
            buys.append(row)
            cnt += 1
    return buys


def regime_of(analysis):
    meta = analysis.get("meta") or {}
    ag = meta.get("action_gate") or {}
    reg = ag.get("regime")
    if isinstance(reg, str) and reg and reg != "(unknown)":
        return reg
    r = meta.get("regime")
    if isinstance(r, dict):
        return str(r.get("regime") or r.get("label") or "")
    if isinstance(r, str):
        return r
    return ""


def main():
    root = Path(DEFAULT_PROJECT_ROOT)
    daily = Daily(root)
    rep = root / "reports"
    days = []
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if not files:
            continue
        try:
            analysis = json.loads(files[-1].read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list) or not cands:
            continue
        regime = regime_of(analysis)
        ranked = sorted([c for c in cands if isinstance(c, dict) and c.get("code")],
                        key=_edge, reverse=True)
        days.append({"date": dd.name, "regime": regime, "ranked": ranked})

    def agg(xs):
        if not xs:
            return {"n": 0, "mean_excess": None, "win_rate": None}
        return {"n": len(xs), "mean_excess": round(statistics.mean(xs), 3),
                "win_rate": round(sum(1 for x in xs if x > 0) / len(xs), 3)}

    def eval_gate(gate_map):
        per_day = []
        all_ex = []
        regime_ex = {}
        n_days_with_buys = 0
        total_buys = 0
        for d in days:
            buys = buys_for_gate(d["ranked"], d["regime"], gate_map)
            total_buys += len(buys)
            exs = []
            for b in buys:
                ex = daily.excess(b.get("code"), d["date"])
                if ex is not None:
                    exs.append(ex)
            if exs:
                n_days_with_buys += 1
                all_ex.extend(exs)
                regime_ex.setdefault(d["regime"] or "(unknown)", []).extend(exs)
            per_day.append({"date": d["date"], "regime": d["regime"] or "(unknown)",
                            "n_buys": len(buys), "n_scored": len(exs),
                            "mean_excess": round(statistics.mean(exs), 3) if exs else None})
        return {
            "overall": agg(all_ex),
            "by_regime": {k: agg(v) for k, v in sorted(regime_ex.items())},
            "n_days": len(days),
            "n_days_with_buys": n_days_with_buys,
            "total_buys": total_buys,
            "per_day": per_day,
        }

    old = eval_gate(OLD_GATE)
    new = eval_gate(NEW_GATE)
    report = {
        "job": "0144_gate_ab_backtest",
        "target": "excess = (close-open)/preclose*100",
        "n_days": len(days),
        "days": [d["date"] for d in days],
        "OLD_gate": old,
        "NEW_gate": new,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "gate_ab_backtest_0144.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    summ = {
        "n_days": len(days),
        "OLD_overall": old["overall"], "OLD_total_buys": old["total_buys"],
        "NEW_overall": new["overall"], "NEW_total_buys": new["total_buys"],
        "OLD_by_regime": old["by_regime"],
        "NEW_by_regime": new["by_regime"],
    }
    print(json.dumps(summ, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
