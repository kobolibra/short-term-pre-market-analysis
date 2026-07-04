#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_replay_current_code.py — Task 0146/0148.

用【当前 HEAD 代码】(compute_edge_v9 + REGIME_ACTION_GATE/_assign_actions) 对最近 N 天
历史盘前候选做整体 replay，量化【胜率】(win_rate) 与【赔率】(payoff)。

0148 修正(关键保真):auction_amount_pct(edge 最大权重 0.323)在旧分析文件里未必持久化,
  0147 实测覆盖率仅 49% → 一半候选回退中性 50, replay 失真。
  本版改为【每天按 auction_amount_wan 横截面重算百分位】(与生产 assemble_v9 同口径),
  auction_amount_wan 每行均已持久化 → 覆盖率拉到 100%, 消除污染。

做法(feature 级 replay):
  - 读每天 analysis_v9.json 的 all_candidates(每行含 full 明细)。
  - 按当天 auction_amount_wan 横截面重算 auction_amount_pct 注入。
  - 用当天真实特征 + 当前 compute_edge_v9 重算 edge/risk/alpha。
  - 用当前 REGIME_ACTION_GATE 重标 BUY/WATCH/DROP。
  - 用真实 excess=(收盘涨幅-竞价涨幅) 评估 CURRENT vs STORED vs topK, 含 by_regime。

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


def _wan(cand):
    full = cand.get("full") or {}
    ad = full.get("auction_detail") or {}
    v = cand.get("auction_amount_wan")
    if v is None:
        v = ad.get("auction_amount_wan")
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except Exception:
        return None


def _decision_from_candidate(cand, amt_pct=None):
    """从持久化候选(_compact,含 full)重建 compute_edge_v9 需要的 decision。"""
    full = cand.get("full") or {}
    ad = dict(full.get("auction_detail") or cand.get("auction_detail") or {})
    if amt_pct is not None:
        ad["auction_amount_pct"] = amt_pct   # 横截面重算值覆盖
    dec = {
        "code": cand.get("code"),
        "name": cand.get("name"),
        "auction_detail": ad,
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
    amt_wan_present = 0
    amt_wan_total = 0
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

        valid = [c for c in cands if isinstance(c, dict) and c.get("code")]
        wan_vals = sorted(w for w in (_wan(c) for c in valid) if w is not None)
        n_wan = len(wan_vals)
        amt_wan_present += n_wan
        amt_wan_total += len(valid)

        def pctl(v):
            if v is None or n_wan == 0:
                return None
            lt = sum(1 for x in wan_vals if x < v)
            eq = sum(1 for x in wan_vals if x == v)
            return 100.0 * (lt + 0.5 * eq) / n_wan

        rebuilt = []
        for cand in valid:
            amt_pct = pctl(_wan(cand))
            dec = _decision_from_candidate(cand, amt_pct)
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
        "job": "0148_replay_current_code",
        "mode": "feature-level replay; auction_amount_pct recomputed from daily cross-section",
        "target": "excess = (close-open)/preclose*100",
        "n_days": len(days),
        "days": [d["date"] for d in days],
        "CURRENT": {"overall": _agg(cur_all),
                    "by_regime": {k: _agg(v) for k, v in sorted(cur_regime.items())}},
        "STORED": {"overall": _agg(sto_all),
                   "by_regime": {k: _agg(v) for k, v in sorted(sto_regime.items())}},
        "ranking_quality": {"top1": _agg(top1), "top3": _agg(top3), "top5": _agg(top5)},
        "faithfulness": {
            "auction_amount_wan_coverage": (round(amt_wan_present / amt_wan_total, 3) if amt_wan_total else None),
            "auction_amount_pct": "recomputed from daily auction_amount_wan cross-section (production-equivalent)",
            "raw_capture_days_available": 0,
        },
        "per_day": per_day,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "replay_current_code_0148.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    summ = {
        "n_days": len(days), "days": report["days"],
        "CURRENT_overall": report["CURRENT"]["overall"],
        "STORED_overall": report["STORED"]["overall"],
        "CURRENT_by_regime": report["CURRENT"]["by_regime"],
        "STORED_by_regime": report["STORED"]["by_regime"],
        "ranking_quality": report["ranking_quality"],
        "faithfulness": report["faithfulness"],
    }
    print(json.dumps(summ, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
