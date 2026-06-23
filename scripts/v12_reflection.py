#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v12_reflection.py — 全历史盘前选股“反思迭代”主报告(只读)。

覆盖已下载的所有有 v9 分析的交易日, 回答四件事:
  1) 每日当日超额最高的票是谁(Top-N 赢家)。
  2) 这些赢家为什么没被选出来(action / edge 排名 / 风险位 / 抢筹信号),
     并区分“排名问题(模型没排上)” vs “门控问题(排上了但被 regime/risk 拦掉)”。
  3) 选出来的票(BUY/各 action)当日实际表现如何(命中率/均值/跌停率)。
  4) 哪些盘前竞价字段有预测力(每日横截面 Spearman IC),
     对照现行 v10_amt 权重给出加权/降权/剔除建议。

excess_ret = 收盘涨幅 - 竞价涨幅 = (close - open)/preclose*100  (唯一正确的盘前选股口径)

输出: reports/_audit/premarket_reflection_report.{json,md}
用法: python3 scripts/v12_reflection.py [--top-n 30]
"""
from __future__ import annotations
import argparse
import json
import statistics
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10  # 复用 Daily/extract/derived/pctl/daily_ic/mean_icir/权重常量

RAW_FLDS = ["amt_pct", "auction_strength", "liquidity", "money", "pressure_score",
            "weimai_strength", "orderbook", "low_cost", "theme_strength_t0",
            "market_env_score", "cashflow_continuity_score", "longtou_score",
            "net_pressure", "latest_change_pct", "source_evidence_score",
            "auction_amount_wan", "net_amount_rank", "qiangchou_920_925_rank",
            "qiangchou_last_second_rank"]
DERIV_FLDS = ["deriv.auc_minus_8xopen", "deriv.lowopen_strength", "deriv.money_x_liq", "deriv.amt_x_auc"]


def _stat(xs):
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "mean_excess": round(statistics.mean(xs), 3),
            "median_excess": round(statistics.median(xs), 3),
            "win_rate": round(sum(1 for e in xs if e > 0) / len(xs), 3),
            "limitdown_rate": round(sum(1 for e in xs if e <= -9.5) / len(xs), 3)}


def load_days_plus(root, daily):
    out = []
    rep = root / "reports"
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
        regime = (analysis.get("regime") or analysis.get("market_regime")
                  or (analysis.get("meta") or {}).get("regime")
                  or (analysis.get("market_env") or {}).get("regime") or "")
        rows = []
        for rec in cands:
            if not isinstance(rec, dict) or not rec.get("code"):
                continue
            ex = daily.excess(rec["code"], dd.name)
            if ex is None:
                continue
            e = v10.extract(rec)
            e["excess"] = ex
            full = rec.get("full") if isinstance(rec.get("full"), dict) else {}
            ad = full.get("auction_detail") or {}
            rd = full.get("risk_detail") or {}
            rf = full.get("risk_flag")
            if rf is None:
                rf = rec.get("risk_flag")
            e["action"] = str(rec.get("action_type") or full.get("action_type") or "").strip().upper()
            e["risk_flag"] = bool(rf)
            e["weak_breadth"] = "weak_breadth" in (rd.get("market_env_flags") or [])
            e["primary_signal"] = str(ad.get("qiangchou_primary_signal") or "").strip()
            e["setup"] = str(ad.get("auction_setup_type") or "").strip()
            e["alpha"] = str(rec.get("alpha_pattern") or full.get("alpha_pattern") or "").strip()
            rows.append(e)
        if len(rows) < 30:
            continue
        amtp = [(i, rows[i]["f"]["auction_amount_wan"]) for i in range(len(rows))
                if rows[i]["f"]["auction_amount_wan"] is not None]
        amap = v10.pctl(amtp)
        for i, r in enumerate(rows):
            r["amt"] = amap.get(i, 50.0)
            r["d"] = v10.derived(r["f"], r["amt"])
        eorder = sorted(range(len(rows)),
                        key=lambda i: (rows[i]["edge_old"] if rows[i]["edge_old"] is not None else -1.0),
                        reverse=True)
        for rank, i in enumerate(eorder, 1):
            rows[i]["edge_rank"] = rank
        out.append({"date": dd.name, "regime": regime, "rows": rows})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    topN = args.top_n
    days = load_days_plus(root, daily)
    n_samples = sum(len(d["rows"]) for d in days)

    by_action = defaultdict(list)
    for d in days:
        for r in d["rows"]:
            by_action[r["action"] or "UNKNOWN"].append(r["excess"])
    pick_perf = {a: _stat(v) for a, v in sorted(by_action.items())}

    decile_ex = defaultdict(list)
    for d in days:
        rows = [r for r in d["rows"] if r["edge_old"] is not None]
        n = len(rows)
        if n < 10:
            continue
        order = sorted(rows, key=lambda r: r["edge_old"], reverse=True)
        for idx, r in enumerate(order):
            decile_ex[min(int(idx / n * 10), 9)].append(r["excess"])
    edge_decile = [{"decile_top": k, **_stat(v)} for k, v in sorted(decile_ex.items())]

    cap_edgeN, cap_buy = [], []
    winner_actions = defaultdict(int)
    missed = []
    bought_perf = []
    for d in days:
        rows = d["rows"]
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = order[:topN]
        edge_top = set(sorted(range(len(rows)),
                              key=lambda i: (rows[i]["edge_old"] if rows[i]["edge_old"] is not None else -1.0),
                              reverse=True)[:topN])
        denom = float(min(topN, len(winners))) or 1.0
        cap_edgeN.append(len(set(winners) & edge_top) / denom)
        cap_buy.append(sum(1 for i in winners if rows[i]["action"] == "BUY") / denom)
        for i in winners:
            r = rows[i]
            winner_actions[r["action"] or "UNKNOWN"] += 1
            if r["action"] != "BUY":
                missed.append({"date": d["date"], "code": r["code"], "excess": round(r["excess"], 2),
                               "action": r["action"], "edge_score": r["edge_old"], "edge_rank": r.get("edge_rank"),
                               "final": r["final"], "risk_flag": r["risk_flag"], "weak_breadth": r["weak_breadth"],
                               "primary_signal": r["primary_signal"], "setup": r["setup"], "alpha": r["alpha"]})
        for r in rows:
            if r["action"] == "BUY":
                bought_perf.append({"date": d["date"], "code": r["code"], "excess": round(r["excess"], 2),
                                    "edge_score": r["edge_old"], "regime": d["regime"], "risk_flag": r["risk_flag"]})
    missed.sort(key=lambda x: x["excess"], reverse=True)
    missed_gating = sum(1 for m in missed if m["edge_rank"] and m["edge_rank"] <= topN)
    missed_ranking = sum(1 for m in missed if m["edge_rank"] and m["edge_rank"] > topN)

    field_ic = []
    for fld in RAW_FLDS + DERIV_FLDS:
        di = [v10.daily_ic(d["rows"], fld) for d in days]
        m, icir, nd = v10.mean_icir(di)
        if m is not None:
            field_ic.append({"field": fld, "mean_ic": m, "icir": icir, "n_days": nd})
    field_ic.sort(key=lambda x: abs(x["mean_ic"]), reverse=True)
    ic_map = {r["field"]: r["mean_ic"] for r in field_ic}
    pos = {k: max(ic_map.get(k, 0.0), 0.0) for k in v10.CORE_FIELDS}
    tot = sum(pos.values()) or 1.0
    adj = []
    for k in v10.CORE_FIELDS:
        cur = v10.V10AMT_W[k]
        tgt = pos[k] / tot
        delta = tgt - cur
        ic = ic_map.get(k)
        if ic is None or ic <= 0.0:
            act = "降权/剔除 (IC<=0)"
        elif delta > 0.05:
            act = "加权"
        elif delta < -0.05:
            act = "降权"
        else:
            act = "维持"
        adj.append({"field": k, "current_w": cur, "ic": round(ic, 4) if ic is not None else None,
                    "ic_share": round(tgt, 3), "delta": round(delta, 3), "rec": act})
    adj.sort(key=lambda x: x["delta"])

    wc = {
        "edge_topN_capture": round(statistics.mean(cap_edgeN), 3) if cap_edgeN else None,
        "buy_capture": round(statistics.mean(cap_buy), 3) if cap_buy else None,
        "winner_action_dist": dict(winner_actions),
        "missed_gating_problem": missed_gating,
        "missed_ranking_problem": missed_ranking,
    }
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "top_n": topN, "n_days": len(days), "days": [d["date"] for d in days],
        "n_samples": n_samples,
        "pick_performance_by_action": pick_perf,
        "edge_decile_lift": edge_decile,
        "winner_capture": wc,
        "top_missed_winners": missed[:40],
        "bought_performance": bought_perf,
        "field_ic": field_ic,
        "weight_adjustment": adj,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_reflection_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前选股反思迭代主报告", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples} ｜Top-N: {topN}",
         f"- 覆盖日期: {', '.join(report['days'])}", ""]
    L += ["## 1. 选出来的票当日表现 (按 action)", "",
          "| action | n | mean_excess | median | win_rate | 跌停率 |", "|---|---|---|---|---|---|"]
    for a, s in pick_perf.items():
        if s.get("n"):
            L.append(f"| {a} | {s['n']} | {s['mean_excess']} | {s['median_excess']} | {s['win_rate']} | {s['limitdown_rate']} |")
    L += ["", "### 历次 BUY 明细", "", "| 日期 | 代码 | excess | edge | regime | risk |", "|---|---|---|---|---|---|"]
    for b in bought_perf:
        L.append(f"| {b['date']} | {b['code']} | {b['excess']} | {b['edge_score']} | {b['regime']} | {b['risk_flag']} |")
    L += ["", "## 2. edge_score 分层有效性 (每日排名十分位, 0=最高 edge)", "",
          "| 十分位(0=最高) | n | mean_excess | win_rate |", "|---|---|---|---|"]
    for e in edge_decile:
        L.append(f"| {e['decile_top']} | {e['n']} | {e['mean_excess']} | {e['win_rate']} |")
    L += ["", f"## 3. 赢家捕获 (每日 Top-{topN} 真实赢家)", "",
          f"- edge 排名 Top-{topN} 捕获率: **{wc['edge_topN_capture']}** (模型*排序*能力)",
          f"- 实际 BUY 捕获率: **{wc['buy_capture']}** (模型*最终选股*能力)",
          f"- 赢家 action 分布: {wc['winner_action_dist']}",
          f"- 门控问题(排进 Top-{topN} 却没买): **{wc['missed_gating_problem']}** 例",
          f"- 排名问题(模型根本没排上): **{wc['missed_ranking_problem']}** 例", ""]
    L += ["### 最大遗漏赢家 Top-40 (为什么没选出来)", "",
          "| 日期 | 代码 | excess | action | edge | edge_rank | risk | 抢筹信号 | setup |",
          "|---|---|---|---|---|---|---|---|---|"]
    for m in report["top_missed_winners"]:
        L.append(f"| {m['date']} | {m['code']} | {m['excess']} | {m['action']} | {m['edge_score']} | {m['edge_rank']} | {'!' if m['risk_flag'] else ''} | {m['primary_signal']} | {m['setup']} |")
    L += ["", "## 4. 字段预测力 (每日横截面 Spearman IC, 主口径 excess)", "",
          "| 字段 | mean_ic | icir | n_days |", "|---|---|---|---|"]
    for r in field_ic:
        L.append(f"| {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} |")
    L += ["", "## 5. 权重调整建议 (现行 v10_amt vs IC 实证)", "",
          "| 字段 | 现权重 | IC | IC占比 | Δ(目标-现) | 建议 |", "|---|---|---|---|---|---|"]
    for a in adj:
        L.append(f"| {a['field']} | {a['current_w']} | {a['ic']} | {a['ic_share']} | {a['delta']} | {a['rec']} |")
    L += ["", "> 注: *_rank 字段已做方向翻转(秩越小越好), 故正 IC 才算有效; IC<=0 的核心字段建议降权或剔除。",
          "> 调整需经 walk-forward 出样本验证(见 v10_optimize)后再改线上 edge 公式, 勿凭单日改模型。"]
    (audit / "premarket_reflection_report.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"n_days": len(days), "n_samples": n_samples,
                      "edge_topN_capture": wc["edge_topN_capture"], "buy_capture": wc["buy_capture"],
                      "pick_performance_by_action": pick_perf,
                      "top_field_ic": field_ic[:6], "weight_adjustment": adj}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
