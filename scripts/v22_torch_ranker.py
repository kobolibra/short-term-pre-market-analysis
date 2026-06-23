#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v22_torch_ranker.py — 重构第2步: torch 截面学习排序模型(只读回测)。

在进程内重建特征矩阵(不依赖上一轮 CSV 是否在 worker 保留), 用 v21 筛出的
15 个干净特征的日内截面分位秩, 训练 torch pairwise 排序模型(强 L2 + 早停),
walk-forward 出样本对比现行 v10_amt: capture@5 / capture@30 / IC / Top-5日均超额。

标签: 当日超额 excess = (close-open)/preclose*100。
复用: v10.score/field_value/pctl/spearman, v12.load_days_plus, v14.t1_metrics(仅取 hold 辅助)。

输出: reports/_audit/premarket_ranker_v22.{json,md}
用法: python3 scripts/v22_torch_ranker.py [--min-train 5] [--epochs 120]
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

# v21 筛选: 全覆盖 + 有横截面方差的特征(剔除无方差 theme/market_env/longtou 与高缺失 rank/net_pressure)
FEATURE_BASE = ["amt_pct", "auction_strength", "liquidity", "money", "pressure_score",
                "weimai_strength", "orderbook", "low_cost", "latest_change_pct",
                "source_evidence_score", "cashflow_continuity_score",
                "deriv.money_x_liq", "deriv.amt_x_auc", "deriv.auc_minus_8xopen",
                "deriv.lowopen_strength"]


def build_days(root):
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    out = []
    for d in days:
        rows = d["rows"]
        # within-day percentile rank per feature (impute missing -> 50)
        xr = {}
        for fld in FEATURE_BASE:
            iv = [(i, v10.field_value(rows[i], fld)) for i in range(len(rows))
                  if v10.field_value(rows[i], fld) is not None]
            xr[fld] = v10.pctl(iv) if iv else {}
        X, excess, amt_base, codes = [], [], [], []
        for i, r in enumerate(rows):
            X.append([xr[fld].get(i, 50.0) / 100.0 for fld in FEATURE_BASE])
            excess.append(r["excess"])
            amt_base.append(v10.score(r["f"], r["amt"], v10.V10AMT_W))
            codes.append(r["code"])
        out.append({"date": d["date"], "X": X, "excess": excess, "amt": amt_base, "codes": codes})
    return out


def capture(scores, excess, N):
    n = len(scores)
    N = min(N, n)
    if N <= 0:
        return None
    order = sorted(range(n), key=lambda i: scores[i], reverse=True)[:N]
    win = set(sorted(range(n), key=lambda i: excess[i], reverse=True)[:N])
    return len(set(order) & win) / float(N)


def topk_mean(scores, excess, k):
    n = len(scores)
    k = min(k, n)
    if k <= 0:
        return None
    order = sorted(range(n), key=lambda i: scores[i], reverse=True)[:k]
    return statistics.mean([excess[i] for i in order])


def run_torch(days, min_train, epochs):
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    torch.manual_seed(0)

    def make(kind, d):
        if kind == "linear":
            return nn.Linear(d, 1)
        return nn.Sequential(nn.Linear(d, 8), nn.ReLU(), nn.Dropout(0.3), nn.Linear(8, 1))

    def pair_loss(s, y):
        diff = s.unsqueeze(1) - s.unsqueeze(0)
        yd = y.unsqueeze(1) - y.unsqueeze(0)
        mask = (yd > 0).float()
        denom = mask.sum()
        if denom.item() <= 0:
            return None
        return -(F.logsigmoid(diff) * mask).sum() / denom

    def train(kind, train_days, d, lr=0.05, wd=1e-2):
        torch.manual_seed(0)
        model = make(kind, d)
        opt = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=wd)
        tens = [(torch.tensor(t["X"], dtype=torch.float32),
                 torch.tensor(t["excess"], dtype=torch.float32)) for t in train_days]
        model.train()
        for _ in range(epochs):
            for X, y in tens:
                if X.shape[0] < 3:
                    continue
                opt.zero_grad()
                s = model(X).squeeze(-1)
                loss = pair_loss(s, y)
                if loss is not None and torch.isfinite(loss):
                    loss.backward()
                    opt.step()
        model.eval()
        return model

    def predict(model, X):
        with torch.no_grad():
            return model(torch.tensor(X, dtype=torch.float32)).squeeze(-1).tolist()

    d = len(FEATURE_BASE)
    folds = []
    for ti in range(min_train, len(days)):
        train_days = days[:ti]
        test = days[ti]
        rec = {"date": test["date"], "n": len(test["excess"])}
        for kind in ("linear", "mlp"):
            try:
                model = train(kind, train_days, d)
                sc = predict(model, test["X"])
            except Exception as e:
                rec[kind] = {"err": f"{type(e).__name__}: {e}"[:120]}
                continue
            rec[kind] = {"cap5": capture(sc, test["excess"], 5),
                         "cap30": capture(sc, test["excess"], 30),
                         "ic": v10.spearman(sc, test["excess"]),
                         "top5": topk_mean(sc, test["excess"], 5),
                         "top30": topk_mean(sc, test["excess"], 30)}
        # baseline v10_amt
        rec["amt"] = {"cap5": capture(test["amt"], test["excess"], 5),
                      "cap30": capture(test["amt"], test["excess"], 30),
                      "ic": v10.spearman(test["amt"], test["excess"]),
                      "top5": topk_mean(test["amt"], test["excess"], 5),
                      "top30": topk_mean(test["amt"], test["excess"], 30)}
        folds.append(rec)
    return folds


def agg(folds, model, key):
    vals = [f[model][key] for f in folds if isinstance(f.get(model), dict) and f[model].get(key) is not None]
    return round(statistics.mean(vals), 4) if vals else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--min-train", type=int, default=5)
    ap.add_argument("--epochs", type=int, default=120)
    args = ap.parse_args()
    root = Path(args.project_root)
    days = build_days(root)

    torch_err = None
    folds = []
    try:
        folds = run_torch(days, args.min_train, args.epochs)
    except Exception as e:
        torch_err = f"{type(e).__name__}: {e}"[:200]

    models = ["linear", "mlp", "amt"]
    summary = {m: {k: agg(folds, m, k) for k in ("cap5", "cap30", "ic", "top5", "top30")} for m in models}

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "n_days": len(days), "oos_days": len(folds), "min_train": args.min_train,
              "epochs": args.epochs, "n_features": len(FEATURE_BASE), "features": FEATURE_BASE,
              "torch_error": torch_err, "oos_summary": summary, "folds": folds}
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_ranker_v22.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def vrow(m, label):
        s = summary[m]
        return f"| {label} | {s['cap5']} | {s['cap30']} | {s['ic']} | {s['top5']} | {s['top30']} |"

    L = ["# v22 torch 截面学习排序 vs v10_amt (出样本)", "",
         f"- 生成: {report['generated_at']} ｜训练日: {len(days)} ｜出样本天: {len(folds)} ｜特征: {len(FEATURE_BASE)} ｜epochs: {args.epochs}"]
    if torch_err:
        L += ["", f"> ⚠ torch 运行出错: {torch_err}"]
    L += ["", "## walk-forward 出样本均值 (标签=当日超额)", "",
          "| 模型 | capture@5 | capture@30 | IC | Top-5日均超额 | Top-30日均超额 |",
          "|---|---|---|---|---|---|",
          vrow("linear", "torch 线性排序"),
          vrow("mlp", "torch MLP"),
          vrow("amt", "v10_amt(现行)")]
    L += ["", "## 逐日出样本", "", "| 日期 | n | 线性 cap30 | 线性 Top5 | amt cap30 | amt Top5 |", "|---|---|---|---|---|---|"]
    for f in folds:
        ln = f.get("linear", {})
        am = f.get("amt", {})
        L.append(f"| {f['date']} | {f['n']} | {ln.get('cap30')} | {ln.get('top5')} | {am.get('cap30')} | {am.get('top5')} |")
    L += ["", "> 门槛: torch 排序只有在出样本 capture@30 与 Top-5日均超额 同时不输于 v10_amt 才值得上线;",
          "> 样本仅 13 日, 出样本差异大概率在噪声内 — 若不能稳定超越则诚实结论是「现阶段保持 v10_amt + Top-5 集中」, 等数据累积再训。"]
    (audit / "premarket_ranker_v22.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"oos_days": len(folds), "torch_error": torch_err, "oos_summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
