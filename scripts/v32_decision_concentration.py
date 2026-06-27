#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v32_decision_concentration.py — job 0041: 决策层集中度/风险闸门研究 (只读)。

为什么: job 0038/0039/0040 三个独立实验收敛于同一结论——字段层面加权/加交互项/
加原始幅度 都不能稳健提升 v10_amt 的 OOS 排序能力(0040 的 Top3 暴涨是少数右尾
离群值伪阳性: IC 不动、capture 下降、median 下降)。按 HANDOFF §4.1: 停止凑字段,
把迭代重心转到决策层(仓位/集中度/闸门)。

本作业: 锁定生产排序 score = v10.score(CORE,V10AMT_W) - risk_penalty (不动公式),
只改变「选几只」与「是否剔除 risk_flag」, 比较不同集中度下的等权篮子表现。
因为排序公式固定(非拟合), 无需训练/出样本拆分, 直接用全部交易日评估策略。

excess_ret = (close - open)/preclose*100  (唯一正确盘前口径)
指标(逐日等权篮子日超额序列): 均值/中位/波动/信息比(均/波动)/胜日率/最差日;
池化个股: 均值/中位/胜率/跌停率。
输出: reports/_audit/premarket_decision_concentration_v32.{json,md}
用法: python3 scripts/v32_decision_concentration.py
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

KS = [1, 2, 3, 5, 10]


def prod_score(r):
    return v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r["risk"] or 0.0)


def basket(days, k, risk_gate):
    daily, pooled = [], []
    used_days = 0
    for d in days:
        cand = [r for r in d["rows"] if (not risk_gate or not r.get("risk_flag"))]
        if len(cand) < k:
            continue
        picks = sorted(cand, key=prod_score, reverse=True)[:k]
        exs = [r["excess"] for r in picks]
        daily.append(statistics.mean(exs))
        pooled.extend(exs)
        used_days += 1
    return daily, pooled, used_days


def daily_stats(dr):
    if not dr:
        return {"n_days": 0}
    mean = statistics.mean(dr)
    sd = statistics.pstdev(dr) if len(dr) > 1 else 0.0
    return {"n_days": len(dr),
            "mean_daily": round(mean, 3),
            "median_daily": round(statistics.median(dr), 3),
            "std_daily": round(sd, 3),
            "info_ratio": round(mean / sd, 3) if sd > 0 else None,
            "win_rate_days": round(sum(1 for x in dr if x > 0) / len(dr), 3),
            "worst_day": round(min(dr), 3),
            "best_day": round(max(dr), 3)}


def pool_stats(xs):
    if not xs:
        return {"n": 0}
    return {"n": len(xs),
            "mean_excess": round(statistics.mean(xs), 3),
            "median_excess": round(statistics.median(xs), 3),
            "win_rate": round(sum(1 for e in xs if e > 0) / len(xs), 3),
            "limitdown_rate": round(sum(1 for e in xs if e <= -9.5) / len(xs), 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    n_samples = sum(len(d["rows"]) for d in days)

    summary = {}
    for k in KS:
        for gate in (False, True):
            name = f"top{k}" + ("_riskgated" if gate else "")
            dr, pooled, ud = basket(days, k, gate)
            summary[name] = {"k": k, "risk_gated": gate, "used_days": ud,
                             "daily": daily_stats(dr), "pooled": pool_stats(pooled)}

    def ir(name):
        return summary[name]["daily"].get("info_ratio")

    def md(name):
        return summary[name]["daily"].get("mean_daily")

    ungated = [f"top{k}" for k in KS]
    best_by_ir = max(ungated, key=lambda n: (ir(n) if ir(n) is not None else -9))
    best_by_mean = max(ungated, key=lambda n: (md(n) if md(n) is not None else -9))

    def gating_helps(k):
        g = ir(f"top{k}_riskgated"); u = ir(f"top{k}")
        if g is None or u is None:
            return None
        return g > u

    verdict = {
        "frozen_ranking": "v10_amt: score(CORE,V10AMT_W) - risk_penalty",
        "best_concentration_by_info_ratio": best_by_ir,
        "best_concentration_by_mean_daily": best_by_mean,
        "production_top3": summary["top3"]["daily"],
        "production_top3_vs_best_ir": {
            "top3_info_ratio": ir("top3"),
            "best_info_ratio": ir(best_by_ir),
        },
        "risk_gating_helps": {f"top{k}": gating_helps(k) for k in KS},
    }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0041_premarket_decision_concentration_v32",
        "n_days": len(days), "days": [d["date"] for d in days], "n_samples": n_samples,
        "ks": KS, "summary": summary, "verdict": verdict,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_decision_concentration_v32.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前选股 v32 — 决策层集中度/风险闸门研究 (job 0041)", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples}",
         f"- 锁定排序: v10_amt (score(CORE,V10AMT_W) - risk_penalty), 只变集中度/风险闸门", "",
         "## 等权篮子表现 (逐日均值序列)", "",
         "| 策略 | 用日 | 日均 | 日中位 | 波动 | 信息比 | 胜日率 | 最差日 | 池化均/中位/胜率/跌停 |",
         "|---|---|---|---|---|---|---|---|---|"]
    for k in KS:
        for gate in (False, True):
            name = f"top{k}" + ("_riskgated" if gate else "")
            s = summary[name]; dd = s["daily"]; pp = s["pooled"]
            L.append(f"| {name} | {s['used_days']} | {dd.get('mean_daily')} | {dd.get('median_daily')} | "
                     f"{dd.get('std_daily')} | {dd.get('info_ratio')} | {dd.get('win_rate_days')} | {dd.get('worst_day')} | "
                     f"{pp.get('mean_excess')}/{pp.get('median_excess')}/{pp.get('win_rate')}/{pp.get('limitdown_rate')} |")
    L += ["", "## 结论", "",
          f"- 按信息比最优集中度: **{verdict['best_concentration_by_info_ratio']}** (IR={ir(verdict['best_concentration_by_info_ratio'])})",
          f"- 按日均超额最优集中度: **{verdict['best_concentration_by_mean_daily']}**",
          f"- 生产现用 Top3 信息比: {ir('top3')} (对比最优 {ir(verdict['best_concentration_by_info_ratio'])})",
          f"- 风险闸门是否改善(按 IR): {verdict['risk_gating_helps']}", "",
          "> 注: 排序公式冻结为生产 v10_amt; 本作业只评估决策策略(选几只/是否剔除 risk_flag)。",
          "> 信息比 = 日均超额 / 日超额波动, 对离群值比单纯均值更稳健; 小样本(n_days)仍需谨慎解读。"]
    (audit / "premarket_decision_concentration_v32.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"verdict": verdict, "summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
