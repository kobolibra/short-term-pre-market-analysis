#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_replay_current_code.py — Task 0146.

用【当前 HEAD 代码】(compute_edge_v9 + REGIME_ACTION_GATE/_assign_actions) 对最近 N 天
历史盘前候选做整体 replay，量化【胜率】(win_rate) 与【赔率】(payoff/expectancy)，
作为迭代高胜率高赔率框架的基线。

做法(feature 级 replay, C 方案):
  - 读每天 analysis_v9.json 的 all_candidates(每行含 full 明细: auction/weimai/theme/context detail,
    以及 assemble_v9 注入并持久化的 auction_amount_pct)。
  - 用这些【当天真实特征】+【当前 compute_edge_v9 公式/风控闸门】重算 edge_score/risk_flag/alpha_type。
  - 用【当前 REGIME_ACTION_GATE】(_assign_actions) 重新标 BUY/WATCH/DROP。
  - 用真实 excess=(收盘涨幅-竞价涨幅) 评估:
      CURRENT (当前代码选出的 BUY) vs STORED (磁盘上旧代码当天实际标的 BUY) vs 纯排名 topK。
  - 指标: n, mean_excess, win_rate, payoff=avg_win/|avg_loss|, avg_win/avg_loss, by_regime, per_day。

注:这是 feature 级 replay —— 复用当天已持久化特征,只换【打分+风控+门控】即当前模型。
    特征【提取】层的改动(pit_panel 等)不在此口径内(需 raw capture 重跑, C2, 本 job 附带可用性自检)。

复用 v10_optimize.Daily/DEFAULT_PROJECT_ROOT; duanxianxia_v9_edge.compute_edge_v9;
duanxianxia_v9_output._assign_actions/_regime_label。
输出 reports/_audit/replay_current_code_0146.json + 紧凑 stdout 摘要(置尾防截断)。
用法: python3 duanxianxia_replay_current_code.py [N=10]
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


def _decision_from_candidate(cand):
    """从持久化候选(_compact,含 full)重建 compute_edge_v9 需要的 decision。"""
    full = cand.get("full") or {}
    dec = {
        "code": cand.get("code"),
        "name": cand.get("name"),
        "auction_detail": full.get("auction_detail") or cand.get("auction_detail") or {},
        "weimai_detail": full.get("weimai_detail") or {},
        "theme_detail": full.get("theme_detail") or {},
        "context_detail": full.get("context_detail") or {},
    }
    if cand.get("auction_strength") is not None:
        dec["auction_strength"] = cand.get("auction_strength")
    if cand.get("theme_strength_t0") is not None:
        dec["theme_strength_t0"] = cand.get("theme_strength_t0")
    return dec


def _agg(xs):
    if not xs:
        return {"n": 0, "mean_excess": None, "win_rate": None,
                "avg_win": None, "avg_loss": None, "payoff": None}
    wins = [x for x in xs if x > 0]
    losses = [x for x in xs if x < 0]
    avg_win = statistics.mean(wins) if wins else 0.0
    avg_loss = statistics.mean(losses) if losses else 0.0
    payoff = (avg_win / abs(avg_loss)) if losses else None
    return {
        "n": len(xs),
        "mean_excess": round(statistics.mean(xs), 3),
        "win_rate": round(len(wins) / len(xs), 3),
        "avg_win": round(avg_win, 3),
        "avg_loss": round(avg_loss, 3),
        "payoff": round(payoff, 3) if payoff is not None else None,
    }


def main():
    n_days = 10
    if len(sys.argv) > 1:
        try:
            n_days = int(sys.argv[1])
        except Exception:
            pass

    root = Path(DEFAULT_PROJECT_ROOT)
    daily = Daily(root)
    rep = root / "reports"

    day_dirs = []
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if files:
            day_dirs.append((dd.name, files[-1]))
    day_dirs = day_dirs[-n_days:]

    days = []
    raw_capture_days = 0
    amt_pct_present = 0
    amt_pct_total = 0
    for date, f in day_dirs:
        try:
            analysis = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list) or not cands:
            continue
        meta = analysis.get("meta") or {}
        market_env = meta.get("market_env") or analysis.get("market_env") or {}
        pm_dir = rep / date / "premarket"
        if pm_dir.is_dir() and (any(pm_dir.glob("*raw*")) or any(pm_dir.glob("*capture*")) or (pm_dir / "raw").is_dir()):
            raw_capture_days += 1

        rebuilt = []
        for cand in cands:
            if not isinstance(cand, dict) or not cand.get("code"):
                continue
            dec = _decision_from_candidate(cand)
            amt_pct_total += 1
            if (dec.get("auction_detail") or {}).get("auction_amount_pct") is not None:
                amt_pct_present += 1
            out = compute_edge_v9(dec, market_env, {})
            rebuilt.append({
                "code": cand.get("code"),
                "name": cand.get("name"),
                "edge_score": out["edge_score"],
                "alpha_type": out["alpha_type"],
                "risk_flag": out["risk_flag"],
                "stored_action": cand.get("action_type"),
                "stored_edge": cand.get("edge_score"),
            })

        ranked = sorted(rebuilt, key=lambda r: float(r.get("edge_score") or 0), reverse=True)
        _assign_actions(ranked, market_env, meta)
        regime = _regime_label(market_env, meta)
        days.append({"date": date, "regime": regime or "(unknown)", "ranked": ranked})

    def excess_list(rows, date):
        xs = []
        for r in rows:
            ex = daily.excess(r.get("code"), date)
            if ex is not None:
                xs.append(ex)
        return xs

    cur_all, sto_all = [], []
    cur_regime, sto_regime = {}, {}
    top1, top3, top5 = [], [], []
    per_day = []
    for d in days:
        date = d["date"]; regime = d["regime"]; ranked = d["ranked"]
        cur_buys = [r for r in ranked if r.get("action_type") == "BUY"]
        sto_buys = [r for r in ranked if r.get("stored_action") == "BUY"]
        cur_ex = excess_list(cur_buys, date)
        sto_ex = excess_list(sto_buys, date)
        cur_all += cur_ex; sto_all += sto_ex
        cur_regime.setdefault(regime, []).extend(cur_ex)
        sto_regime.setdefault(regime, []).extend(sto_ex)
        top1 += excess_list(ranked[:1], date)
        top3 += excess_list(ranked[:3], date)
        top5 += excess_list(ranked[:5], date)
        per_day.append({
            "date": date, "regime": regime,
            "cur_n_buys": len(cur_buys), "cur_scored": len(cur_ex),
            "cur_mean": round(statistics.mean(cur_ex), 3) if cur_ex else None,
            "sto_n_buys": len(sto_buys), "sto_scored": len(sto_ex),
            "sto_mean": round(statistics.mean(sto_ex), 3) if sto_ex else None,
            "top1": ranked[0].get("code") if ranked else None,
        })

    report = {
        "job": "0146_replay_current_code",
        "mode": "feature-level replay (reuse persisted features, current edge+gate)",
        "target": "excess = (close-open)/preclose*100",
        "n_days": len(days),
        "days": [d["date"] for d in days],
        "CURRENT": {"overall": _agg(cur_all),
                    "by_regime": {k: _agg(v) for k, v in sorted(cur_regime.items())}},
        "STORED": {"overall": _agg(sto_all),
                   "by_regime": {k: _agg(v) for k, v in sorted(sto_regime.items())}},
        "ranking_quality": {"top1": _agg(top1), "top3": _agg(top3), "top5": _agg(top5)},
        "faithfulness": {
            "auction_amount_pct_coverage": (round(amt_pct_present / amt_pct_total, 3) if amt_pct_total else None),
            "raw_capture_days_available": raw_capture_days,
            "note": "auction_amount_pct 覆盖率低则 feature replay 失真; raw_capture 可用则可做 C2 全链路重跑",
        },
        "per_day": per_day,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "replay_current_code_0146.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    summ = {
        "n_days": len(days), "days": report["days"],
        "CURRENT_overall": report["CURRENT"]["overall"],
        "STORED_overall": report["STORED"]["overall"],
        "CURRENT_by_regime": report["CURRENT"]["by_regime"],
        "ranking_quality": report["ranking_quality"],
        "faithfulness": report["faithfulness"],
    }
    print(json.dumps(summ, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
