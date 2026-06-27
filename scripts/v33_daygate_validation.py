#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v33_daygate_validation.py — job 0042: 验证「风险闸门」是否是个可落地的择日器 (只读)。

背景: job 0041 发现 risk-gated 篮子信息比高得离谱(Top3 IR=5.16), 但 used_days 从 15 掉到 6,
且 k=1..10 的 used_days 都是 6 —— 说明有 9 天整个票池几乎全被 risk_flag 标记。那个 5.16
是在挑出来的 6 天子样本上算的, 不能直接上线。本作业把「空仓的 9 天机会成本」算进去,
公平比较「始终做」 vs 「择日做」。

锁定生产排序 score = v10.score(CORE,V10AMT_W) - risk_penalty (不动公式)。
excess_ret = (close - open)/preclose*100

策略(均等权篮子, 空仓日超额计 0, 全 15 天口径才能公平比):
  - always_top1/2/3 : 每天从全票池选 Top-k (现状基准)
  - daygate_top1/2/3: 只在「清洁日」(非风险名额数 >= k) 从清洁名额选 Top-k, 否则空仓(0)
诊断: 被跳过那几天如果照常做 always_top3 到底赚还是亏(avoided_pnl), 以及 risk_flag 是否日级。
输出: reports/_audit/premarket_daygate_validation_v33.{json,md}
用法: python3 scripts/v33_daygate_validation.py
"""
from __future__ import annotations
import argparse
import json
import statistics
import sys
import traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12

KS = [1, 2, 3]


def prod_score(r):
    return v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r["risk"] or 0.0)


def topk_excess(rows, k):
    if len(rows) < k:
        return None
    picks = sorted(rows, key=prod_score, reverse=True)[:k]
    return statistics.mean([r["excess"] for r in picks])


def series_stats(series15, traded_mask):
    """series15: 全 15 天超额(空仓=0); traded_mask: 该天是否真交易。"""
    n = len(series15)
    mean = statistics.mean(series15) if n else 0.0
    sd = statistics.pstdev(series15) if n > 1 else 0.0
    traded = [s for s, m in zip(series15, traded_mask) if m]
    return {
        "n_total_days": n,
        "n_traded": sum(1 for m in traded_mask if m),
        "mean_daily_all": round(mean, 3),
        "std_daily_all": round(sd, 3),
        "info_ratio_all": round(mean / sd, 3) if sd > 0 else None,
        "cumulative": round(sum(series15), 3),
        "win_rate_all_days": round(sum(1 for s in series15 if s > 0) / n, 3) if n else None,
        "worst_day": round(min(series15), 3) if n else None,
        "best_day": round(max(series15), 3) if n else None,
        "mean_traded_only": round(statistics.mean(traded), 3) if traded else None,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    n_samples = sum(len(d["rows"]) for d in days)

    perday = []
    for d in days:
        rows = d["rows"]
        clean = [r for r in rows if not r["risk_flag"]]
        perday.append({
            "date": d["date"],
            "regime": d.get("regime", ""),
            "n": len(rows),
            "n_clean": len(clean),
            "n_risky": len(rows) - len(clean),
            "all_top3": topk_excess(rows, 3),
            "all_top1": topk_excess(rows, 1),
            "clean_top3": topk_excess(clean, 3),
            "clean_top1": topk_excess(clean, 1),
            "_rows": rows, "_clean": clean,
        })

    strategies = {}
    for k in KS:
        # always: 全票池 Top-k, 每天都做
        s_all, m_all = [], []
        for p in perday:
            v = topk_excess(p["_rows"], k)
            if v is None:
                s_all.append(0.0); m_all.append(False)
            else:
                s_all.append(v); m_all.append(True)
        strategies[f"always_top{k}"] = series_stats(s_all, m_all)
        # daygate: 只在清洁名额数>=k 的天从清洁名额选 Top-k
        s_g, m_g = [], []
        for p in perday:
            if len(p["_clean"]) >= k:
                s_g.append(topk_excess(p["_clean"], k)); m_g.append(True)
            else:
                s_g.append(0.0); m_g.append(False)
        strategies[f"daygate_top{k}"] = series_stats(s_g, m_g)

    # 诊断: 被闸门跳过的天(清洁<3), always_top3 本来会赚/亏多少
    satout_days = [p for p in perday if len(p["_clean"]) < 3]
    traded_days = [p for p in perday if len(p["_clean"]) >= 3]
    avoided = [p["all_top3"] for p in satout_days if p["all_top3"] is not None]
    on_days = [p["all_top3"] for p in traded_days if p["all_top3"] is not None]
    fully_flagged = [p["date"] for p in satout_days if p["n_clean"] == 0]

    verdict = {
        "frozen_ranking": "v10_amt: score(CORE,V10AMT_W) - risk_penalty",
        "n_days": len(days),
        "n_satout_days(clean<3)": len(satout_days),
        "n_fully_flagged_days(clean==0)": len(fully_flagged),
        "fully_flagged_dates": fully_flagged,
        "risk_flag_is_effectively_daylevel": len(fully_flagged) == len(satout_days) and len(satout_days) > 0,
        "avoided_days_always_top3": {
            "n": len(avoided),
            "sum_excess": round(sum(avoided), 3) if avoided else 0.0,
            "mean_excess": round(statistics.mean(avoided), 3) if avoided else None,
            "n_negative": sum(1 for x in avoided if x < 0),
        },
        "traded_days_always_top3": {
            "n": len(on_days),
            "mean_excess": round(statistics.mean(on_days), 3) if on_days else None,
        },
        "always_top3": strategies["always_top3"],
        "daygate_top3": strategies["daygate_top3"],
        "daygate_beats_always_top3": {
            "by_cumulative": strategies["daygate_top3"]["cumulative"] > strategies["always_top3"]["cumulative"],
            "by_info_ratio_all": (strategies["daygate_top3"]["info_ratio_all"] or -9) > (strategies["always_top3"]["info_ratio_all"] or -9),
        },
    }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0042_premarket_daygate_validation_v33",
        "n_days": len(days), "n_samples": n_samples,
        "per_day": [{kk: vv for kk, vv in p.items() if not kk.startswith("_")} for p in perday],
        "strategies": strategies,
        "verdict": verdict,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_daygate_validation_v33.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前选股 v33 — 风险闸门作为择日器的验证 (job 0042)", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples}",
         f"- 锁定排序: v10_amt; 只验证「是否择日」", "",
         "## 逐日明细", "",
         "| 日期 | regime | 清洁/总 | always_top3 | always_top1 | 闸门是否交易 |",
         "|---|---|---|---|---|---|"]
    for p in perday:
        traded = "交易" if p["n_clean"] >= 3 else "**空仓**"
        L.append(f"| {p['date']} | {p['regime']} | {p['n_clean']}/{p['n']} | {p['all_top3']} | {p['all_top1']} | {traded} |")
    L += ["", "## 策略对比 (全 15 天口径, 空仓日计 0)", "",
          "| 策略 | 交易天 | 日均(全) | 信息比(全) | 累计 | 胜日率 | 最差日 | 仅交易日均 |",
          "|---|---|---|---|---|---|---|---|"]
    for name, s in strategies.items():
        L.append(f"| {name} | {s['n_traded']} | {s['mean_daily_all']} | {s['info_ratio_all']} | "
                 f"{s['cumulative']} | {s['win_rate_all_days']} | {s['worst_day']} | {s['mean_traded_only']} |")
    av = verdict["avoided_days_always_top3"]
    L += ["", "## 结论", "",
          f"- risk_flag 是否实质上日级(空仓日都是全票池被标): **{verdict['risk_flag_is_effectively_daylevel']}**; 全标记天数 {verdict['n_fully_flagged_days(clean==0)']}/{verdict['n_satout_days(clean<3)']}",
          f"- 被闸门跳过的 {av['n']} 天, 若照常做 always_top3: 合计超额 **{av['sum_excess']}** (均 {av['mean_excess']}, 其中 {av['n_negative']} 天为负)",
          f"  → 这个数为负 = 闸门帮你避亏; 为正 = 闸门让你错过赚钱天",
          f"- daygate_top3 vs always_top3: 累计胜出={verdict['daygate_beats_always_top3']['by_cumulative']}, 信息比胜出={verdict['daygate_beats_always_top3']['by_info_ratio_all']}", "",
          "> 信息比(全) = 15天日均 / 15天波动(空仓计0), 已含空仓机会成本, 可直接比。小样本仍谨慎。"]
    (audit / "premarket_daygate_validation_v33.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"verdict": verdict}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
