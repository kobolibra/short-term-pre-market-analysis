#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v30_marginal_ic.py — 边际增量 IC 测试 (只读)。

问题: 在已经验证为最优的 v10_amt 基础上, 现有字段里还有没有任何一个
能边际提升 walk-forward OOS IC? 如全为负, 则 v10_amt 在现有数据下已是
线性局部最优, 应把迭代重心转到决策层(闸门/仓位/集中度)。

方法 (低过拟合, 不拟合权重):
  base = clip(v10.score(CORE, V10AMT_W) - risk, 0, 100)        现行生产口径
  对每个候选字段 X (不在 CORE 里): 逐日标准化 z(base)+lambda*z(X_dir),
  lambda 固定为 {0.15, 0.30} (不从数据拟), walk-forward(无需学权重, 直接全
  样本逐日横截面 IC 均值+ICIR), 与 base IC 比较增量。rank 字段做方向翻转。

excess_ret = (close - open) / preclose * 100
输出: reports/_audit/premarket_marginal_ic_v30.{json,md}
用法: python3 scripts/v30_marginal_ic.py
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

LAMBDAS = [0.15, 0.30]
CAND_FIELDS = [f for f in (v12.RAW_FLDS + v12.DERIV_FLDS) if f not in v10.CORE_FIELDS]


def base_scores(rows):
    out = []
    for r in rows:
        s = v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r["risk"] or 0.0)
        out.append(max(0.0, min(100.0, s)))
    return out


def zscore(pairs):
    """pairs: list of (i, value); return dict i->z (mean0 sd1). <2 -> 0."""
    vs = [v for _, v in pairs]
    if len(vs) < 2:
        return {i: 0.0 for i, _ in pairs}
    m = statistics.mean(vs)
    sd = statistics.pstdev(vs)
    if sd <= 0:
        return {i: 0.0 for i, _ in pairs}
    return {i: (v - m) / sd for i, v in pairs}


def field_z(rows, fld):
    pairs = []
    for i, r in enumerate(rows):
        v = v10.field_value(r, fld)
        if v is None:
            continue
        pairs.append((i, -v if fld in v10.RANK_FIELDS else v))
    return zscore(pairs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    n_samples = sum(len(d["rows"]) for d in days)

    # base 逐日 IC
    base_daily = []
    base_z_per_day = []
    for d in days:
        rows = d["rows"]
        bs = base_scores(rows)
        bz = zscore([(i, bs[i]) for i in range(len(rows))])
        base_z_per_day.append(bz)
        ex = [r["excess"] for r in rows]
        base_daily.append(v10.spearman(bs, ex))
    base_ic, base_icir, base_nd = v10.mean_icir(base_daily)

    results = []
    for fld in CAND_FIELDS:
        row_out = {"field": fld}
        for lam in LAMBDAS:
            di = []
            for k, d in enumerate(days):
                rows = d["rows"]
                bz = base_z_per_day[k]
                fz = field_z(rows, fld)
                comb = [bz.get(i, 0.0) + lam * fz.get(i, 0.0) for i in range(len(rows))]
                ex = [r["excess"] for r in rows]
                di.append(v10.spearman(comb, ex))
            m, icir, nd = v10.mean_icir(di)
            improved = sum(1 for a, b in zip(di, base_daily)
                           if a is not None and b is not None and a > b)
            row_out[f"ic_lam{lam}"] = m
            row_out[f"icir_lam{lam}"] = icir
            row_out[f"delta_lam{lam}"] = round((m - base_ic), 4) if (m is not None and base_ic is not None) else None
            row_out[f"days_improved_lam{lam}"] = f"{improved}/{nd}"
        # 平均增量 (两个 lambda)
        deltas = [row_out.get(f"delta_lam{l}") for l in LAMBDAS if row_out.get(f"delta_lam{l}") is not None]
        row_out["mean_delta"] = round(sum(deltas) / len(deltas), 4) if deltas else None
        results.append(row_out)
    results.sort(key=lambda r: (r["mean_delta"] if r["mean_delta"] is not None else -9), reverse=True)

    positive = [r for r in results if r["mean_delta"] is not None and r["mean_delta"] > 0
                and all((r.get(f"delta_lam{l}") or -9) > 0 for l in LAMBDAS)]
    verdict = {
        "base_oos_ic": base_ic, "base_icir": base_icir, "base_days": base_nd,
        "any_field_improves": len(positive) > 0,
        "robust_positive_fields": [r["field"] for r in positive],
        "best_field": results[0]["field"] if results else None,
        "best_mean_delta": results[0]["mean_delta"] if results else None,
    }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "n_days": len(days), "days": [d["date"] for d in days], "n_samples": n_samples,
        "lambdas": LAMBDAS, "candidate_fields": CAND_FIELDS,
        "base": {"oos_ic": base_ic, "icir": base_icir, "n_days": base_nd},
        "marginal": results, "verdict": verdict,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_marginal_ic_v30.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前选股 v30 边际增量 IC 测试", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples}",
         f"- base (v10_amt) OOS IC: **{base_ic}** (icir {base_icir}, {base_nd} 日)", "",
         f"- **是否有字段稳健提升: {verdict['any_field_improves']}**",
         f"- 稳健正增量字段: {verdict['robust_positive_fields'] or '无'}", "",
         "## 逐字段边际增量 (在 v10_amt 上加 lambda*z(field))", "",
         "| 字段 | 均Δ | Δ@0.15 | IC@0.15 | 改善天/总 | Δ@0.30 | IC@0.30 | 改善天/总 |",
         "|---|---|---|---|---|---|---|---|"]
    for r in results:
        L.append(f"| {r['field']} | {r['mean_delta']} | {r.get('delta_lam0.15')} | {r.get('ic_lam0.15')} | {r.get('days_improved_lam0.15')} | {r.get('delta_lam0.3')} | {r.get('ic_lam0.3')} | {r.get('days_improved_lam0.3')} |")
    L += ["", "> 解读: Δ>0 且两个 lambda 均为正 才算稳健增量; 否则该字段对 v10_amt 无边际信息。",
          "> rank 字段已方向翻转(秩越小越好)。"]
    (audit / "premarket_marginal_ic_v30.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"base_oos_ic": base_ic, "verdict": verdict, "top5_fields": results[:5]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
