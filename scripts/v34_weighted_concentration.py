#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v34_weighted_concentration.py — job 0043: 加权篮子 vs 等权 (只读)。

问题: job 0041/0042 显示等权下 Top1/2 收益最高, 但持 1-2 只太少(单票黑天鹅/容量/滑点)。
原因是 v10_amt 信号在榜首最强、往下衰减快, 等权加票 = 用没信号的票稀释榜首超额。
本作业验证: 持足够只数(Top5/8/10) 但按分数/排名加权(权重向榜首倾斜),
能否在保留集中度 alpha 的同时分散风险、逼近 Top2 的收益/信息比。

锁定排序 score = v10.score(CORE,V10AMT_W) - risk_penalty (不动公式)。
excess_ret = (close - open)/preclose*100
加权方案(只动组合权重, 不动排序):
  - equal:  w_i = 1/k
  - rank:   w_i ∝ (k - i)  (线性递减, 榜首最重)
  - score:  w_i ∝ softmax((s_i - max)/T), T=该日 top-k 分数标准差 (尺度无关)
有效持仓数 eff_n = 1/Σ w_i^2 (衡量真实分散度, 回答「太少」)。
输出: reports/_audit/premarket_weighted_concentration_v34.{json,md}
用法: python3 scripts/v34_weighted_concentration.py
"""
from __future__ import annotations
import argparse
import json
import math
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

KS = [3, 5, 8, 10]
SCHEMES = ["equal", "rank", "score"]


def prod_score(r):
    return v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r["risk"] or 0.0)


def weights(scheme, scores):
    k = len(scores)
    if scheme == "equal":
        w = [1.0 / k] * k
    elif scheme == "rank":
        raw = [float(k - i) for i in range(k)]  # scores already sorted desc
        tot = sum(raw)
        w = [x / tot for x in raw]
    else:  # score softmax
        mx = max(scores)
        sd = statistics.pstdev(scores) if k > 1 else 0.0
        T = sd if sd > 1e-9 else 1.0
        ex = [math.exp((s - mx) / T) for s in scores]
        tot = sum(ex)
        w = [x / tot for x in ex]
    return w


def basket_series(days, k, scheme):
    daily, eff_ns = [], []
    for d in days:
        rows = d["rows"]
        if len(rows) < k:
            continue
        picks = sorted(rows, key=prod_score, reverse=True)[:k]
        sc = [prod_score(r) for r in picks]
        w = weights(scheme, sc)
        daily.append(sum(wi * r["excess"] for wi, r in zip(w, picks)))
        eff_ns.append(1.0 / sum(wi * wi for wi in w))
    return daily, eff_ns


def stats(daily, eff_ns):
    if not daily:
        return {"n_days": 0}
    mean = statistics.mean(daily)
    sd = statistics.pstdev(daily) if len(daily) > 1 else 0.0
    return {
        "n_days": len(daily),
        "mean_daily": round(mean, 3),
        "median_daily": round(statistics.median(daily), 3),
        "std_daily": round(sd, 3),
        "info_ratio": round(mean / sd, 3) if sd > 0 else None,
        "cumulative": round(sum(daily), 3),
        "win_rate_days": round(sum(1 for x in daily if x > 0) / len(daily), 3),
        "worst_day": round(min(daily), 3),
        "best_day": round(max(daily), 3),
        "eff_n_names": round(statistics.mean(eff_ns), 2),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    n_samples = sum(len(d["rows"]) for d in days)

    summary = {}
    # 参考: 等权 Top1/Top2
    for k in (1, 2):
        dl, en = basket_series(days, k, "equal")
        summary[f"equal_top{k}"] = stats(dl, en)
    for k in KS:
        for sch in SCHEMES:
            dl, en = basket_series(days, k, sch)
            summary[f"{sch}_top{k}"] = stats(dl, en)

    ref_top2 = summary["equal_top2"]

    def ir(n):
        return summary[n]["info_ratio"]

    def cum(n):
        return summary[n]["cumulative"]

    # 在 分散(k>=5) 且 非等权 的方案里找最佳
    diversified = [f"{sch}_top{k}" for k in (5, 8, 10) for sch in ("rank", "score")]
    best_div_by_ir = max(diversified, key=lambda n: (ir(n) if ir(n) is not None else -9))
    best_div_by_cum = max(diversified, key=lambda n: cum(n))

    verdict = {
        "frozen_ranking": "v10_amt: score(CORE,V10AMT_W) - risk_penalty",
        "reference_equal_top2": {"cumulative": ref_top2["cumulative"], "info_ratio": ref_top2["info_ratio"],
                                 "worst_day": ref_top2["worst_day"], "eff_n_names": ref_top2["eff_n_names"]},
        "best_diversified_by_info_ratio": {"name": best_div_by_ir, **summary[best_div_by_ir]},
        "best_diversified_by_cumulative": {"name": best_div_by_cum, **summary[best_div_by_cum]},
        "weighting_recovers_topofbook": {
            "vs_equal_top5": {"equal": cum("equal_top5"), "rank": cum("rank_top5"), "score": cum("score_top5")},
            "vs_equal_top10": {"equal": cum("equal_top10"), "rank": cum("rank_top10"), "score": cum("score_top10")},
        },
    }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0043_premarket_weighted_concentration_v34",
        "n_days": len(days), "n_samples": n_samples,
        "schemes": SCHEMES, "ks": KS, "summary": summary, "verdict": verdict,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_weighted_concentration_v34.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前选股 v34 — 加权篮子 vs 等权 (job 0043)", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples}",
         f"- 锁定排序: v10_amt; 只变「持几只 + 怎么加权」", "",
         "## 各方案 (全 15 天)", "",
         "| 方案 | 有效持仓数 | 日均 | 信息比 | 累计 | 胜日率 | 最差日 |",
         "|---|---|---|---|---|---|---|"]
    order = ["equal_top1", "equal_top2"] + [f"{sch}_top{k}" for k in KS for sch in SCHEMES]
    for name in order:
        s = summary[name]
        L.append(f"| {name} | {s.get('eff_n_names')} | {s.get('mean_daily')} | {s.get('info_ratio')} | "
                 f"{s.get('cumulative')} | {s.get('win_rate_days')} | {s.get('worst_day')} |")
    v = verdict
    L += ["", "## 结论", "",
          f"- 参考 等权Top2: 累计 {v['reference_equal_top2']['cumulative']}, 信息比 {v['reference_equal_top2']['info_ratio']}, 最差日 {v['reference_equal_top2']['worst_day']}",
          f"- 分散(k>=5)中信息比最优: **{v['best_diversified_by_info_ratio']['name']}** "
          f"(持仓{v['best_diversified_by_info_ratio'].get('eff_n_names')}只, 累计{v['best_diversified_by_info_ratio'].get('cumulative')}, IR{v['best_diversified_by_info_ratio'].get('info_ratio')}, 最差日{v['best_diversified_by_info_ratio'].get('worst_day')})",
          f"- 分散(k>=5)中累计最高: **{v['best_diversified_by_cumulative']['name']}** (累计{v['best_diversified_by_cumulative'].get('cumulative')})",
          f"- 加权是否找回榜首超额 (Top5 累计): {v['weighting_recovers_topofbook']['vs_equal_top5']}",
          f"- 加权是否找回榜首超额 (Top10 累计): {v['weighting_recovers_topofbook']['vs_equal_top10']}", "",
          "> 目标: 用 score/rank 加权的 Top8/10(持足够只、有容量) 逼近等权Top2 的收益/信息比, 同时降低单票风险。小样本谨慎。"]
    (audit / "premarket_weighted_concentration_v34.md").write_text("\n".join(L), encoding="utf-8")

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
