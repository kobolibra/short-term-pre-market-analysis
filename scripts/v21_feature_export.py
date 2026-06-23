#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v21_feature_export.py — 重构第1步: 可复现特征矩阵导出器(只读)。

为 v22 学习排序模型准备干净训练集:
  - 特征: v12.RAW_FLDS + v12.DERIV_FLDS (23) 的原始值, 加每个特征的日内截面分位秩(xr.*)。
  - 标签: y_excess(当日超额), y_hold(竞买→次收持仓), y_t1close(次日收盘涨幅)。
  - 每个特征对 excess / hold 两个标签的每日横截面 Spearman IC 先验(模型重要性先验)。

复用真实 API: v10.field_value/pctl/spearman/mean_icir/RANK_FIELDS, v12.load_days_plus, v14.Line/t1_metrics。

输出:
  reports/_audit/feature_matrix_v21.csv      (训练矩阵)
  reports/_audit/premarket_features_v21.json (汇总)
  reports/_audit/premarket_features_v21.md   (IC先验 + 覆盖率)
用法: python3 scripts/v21_feature_export.py
"""
from __future__ import annotations
import argparse
import csv
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12
import v14_horizon as v14

FLDS = v12.RAW_FLDS + v12.DERIV_FLDS


def _r(x, nd=5):
    return round(x, nd) if isinstance(x, (int, float)) else ""


def ic_vs(days, fld, key):
    di = []
    for d in days:
        xs, ys = [], []
        for r in d["rows"]:
            v = v10.field_value(r, fld)
            y = r["excess"] if key == "excess" else r.get("_t1", {}).get(key)
            if v is None or y is None:
                continue
            xs.append(-v if fld in v10.RANK_FIELDS else v)
            ys.append(y)
        if len(xs) >= 8:
            di.append(v10.spearman(xs, ys))
    m, icir, nd = v10.mean_icir(di)
    return m, icir, nd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    line = v14.Line(root)
    days = v12.load_days_plus(root, daily)

    # attach T+1 labels + within-day percentile ranks
    for d in days:
        rows = d["rows"]
        for r in rows:
            r["_t1"] = v14.t1_metrics(line, r["code"], d["date"]) or {}
        d["_xr"] = {}
        for fld in FLDS:
            iv = [(i, v10.field_value(rows[i], fld)) for i in range(len(rows))
                  if v10.field_value(rows[i], fld) is not None]
            d["_xr"][fld] = v10.pctl(iv) if iv else {}

    feat_cols = list(FLDS) + [f"xr.{f}" for f in FLDS]
    meta_cols = ["date", "code", "regime", "action", "risk_flag", "amt", "edge_old", "final"]
    label_cols = ["y_excess", "y_hold", "y_t1close"]
    cols = meta_cols + label_cols + feat_cols

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    csv_path = audit / "feature_matrix_v21.csv"
    n_rows = 0
    nonnull = {c: 0 for c in feat_cols + label_cols}
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for d in days:
            rows = d["rows"]
            for i, r in enumerate(rows):
                rec = {
                    "date": d["date"], "code": r["code"], "regime": d.get("regime", ""),
                    "action": r.get("action", ""), "risk_flag": int(bool(r.get("risk_flag"))),
                    "amt": _r(r.get("amt")), "edge_old": _r(r.get("edge_old")), "final": _r(r.get("final")),
                    "y_excess": _r(r.get("excess")),
                    "y_hold": _r(r.get("_t1", {}).get("hold_open_t1close")),
                    "y_t1close": _r(r.get("_t1", {}).get("t1_close")),
                }
                for fld in FLDS:
                    rec[fld] = _r(v10.field_value(r, fld))
                    rec[f"xr.{fld}"] = _r(d["_xr"][fld].get(i))
                for c in feat_cols + label_cols:
                    if rec.get(c) != "":
                        nonnull[c] += 1
                w.writerow(rec)
                n_rows += 1

    # per-feature IC priors vs two labels
    ic_rows = []
    for fld in FLDS:
        me, ire, nde = ic_vs(days, fld, "excess")
        mh, irh, ndh = ic_vs(days, fld, "hold_open_t1close")
        ic_rows.append({"field": fld, "ic_excess": me, "icir_excess": ire,
                        "ic_hold": mh, "icir_hold": irh, "n_days": nde})
    ic_rows.sort(key=lambda x: abs(x["ic_excess"] if x["ic_excess"] is not None else 0), reverse=True)

    summary = {"generated_at": datetime.now().isoformat(timespec="seconds"),
               "n_days": len(days), "n_rows": n_rows,
               "n_features": len(feat_cols), "feature_cols": feat_cols,
               "label_cols": label_cols,
               "coverage": {c: (round(nonnull[c] / n_rows, 3) if n_rows else 0) for c in feat_cols + label_cols},
               "feature_ic": ic_rows, "csv": str(csv_path.relative_to(root))}
    (audit / "premarket_features_v21.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# v21 特征矩阵导出 (重构训练集地基)", "",
         f"- 生成: {summary['generated_at']}",
         f"- 训练日: {len(days)} ｜样本行: **{n_rows}** ｜特征数: {len(feat_cols)} (23原始+23截面秩)",
         f"- CSV: `{summary['csv']}`", "",
         "## 特征 IC 先验 (每日横截面 Spearman, 对两个标签)", "",
         "| 特征 | IC(当日超额) | ICIR | IC(次收持仓) | ICIR | n_days |",
         "|---|---|---|---|---|---|"]
    for r in ic_rows:
        L.append(f"| {r['field']} | {r['ic_excess']} | {r['icir_excess']} | {r['ic_hold']} | {r['icir_hold']} | {r['n_days']} |")
    L += ["", "## 关键特征覆盖率(非空占比)", ""]
    cov = summary["coverage"]
    for c in label_cols + FLDS:
        L.append(f"- `{c}`: {cov.get(c)}")
    L += ["", "> 用途: v22 直接读此 CSV 训练 torch 截面排序模型; IC先验用于特征筛选/正则先验。",
          "> 注: 样本仅 ~13 日, 模型须强正则 + walk-forward; 数据随每交易日自动累积。"]
    (audit / "premarket_features_v21.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"n_rows": n_rows, "n_features": len(feat_cols),
                      "top_ic": ic_rows[:6], "coverage_labels": {k: cov[k] for k in label_cols}},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
