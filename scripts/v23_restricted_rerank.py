#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v23_restricted_rerank.py — 受限头部重排器(只读回测)。

v22 结论: torch 全市场排序不稳, 但 MLP Top-5 日均可能有非线性增量。
本脚本把 ML 降维成更可上线的形态: 仅在 v10_amt Top-M 内二次排序, 防止全市场乱选。
同时测试一个可解释的 IC-prior 稀疏公式。

比较:
  - v10_amt 原始 Top-K (K=3/5/10)
  - torch MLP 全市场 Top-K
  - v10_amt Top-M(M=10/20/30) 内用 torch MLP 二次排序 Top-K
  - sparse_ic 公式 Top-K: 只用 v21 稳定正 IC 特征的截面秩加权

输出: reports/_audit/premarket_rerank_v23.{json,md}
用法: python3 scripts/v23_restricted_rerank.py [--min-train 5] [--epochs 80]
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

FEATURE_BASE = ["amt_pct", "auction_strength", "liquidity", "money", "pressure_score",
                "weimai_strength", "orderbook", "low_cost", "latest_change_pct",
                "source_evidence_score", "cashflow_continuity_score",
                "deriv.money_x_liq", "deriv.amt_x_auc", "deriv.auc_minus_8xopen",
                "deriv.lowopen_strength"]
# v21 IC 先验: 稳定正向 + 高覆盖; 权重手工稀疏化, 避免 13天样本过拟合
SPARSE_W = {"deriv.amt_x_auc": 0.24, "auction_strength": 0.22, "liquidity": 0.18,
            "pressure_score": 0.13, "deriv.money_x_liq": 0.12, "money": 0.11}
KS = [3, 5, 10]
MS = [10, 20, 30]


def build_days(root):
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    out = []
    for d in days:
        rows = d["rows"]
        xr = {}
        for fld in FEATURE_BASE:
            iv = [(i, v10.field_value(rows[i], fld)) for i in range(len(rows))
                  if v10.field_value(rows[i], fld) is not None]
            xr[fld] = v10.pctl(iv) if iv else {}
        X, excess, amt, sparse, codes = [], [], [], [], []
        for i, r in enumerate(rows):
            X.append([xr[fld].get(i, 50.0) / 100.0 for fld in FEATURE_BASE])
            excess.append(r["excess"])
            amt.append(v10.score(r["f"], r["amt"], v10.V10AMT_W))
            sparse.append(sum(SPARSE_W.get(f, 0.0) * xr[f].get(i, 50.0) for f in SPARSE_W))
            codes.append(r["code"])
        out.append({"date": d["date"], "X": X, "excess": excess, "amt": amt, "sparse": sparse, "codes": codes})
    return out


def mean_top(indices, y, K):
    sel = indices[:min(K, len(indices))]
    return statistics.mean([y[i] for i in sel]) if sel else None


def cap_top(indices, y, K):
    K = min(K, len(indices), len(y))
    if K <= 0:
        return None
    sel = set(indices[:K])
    win = set(sorted(range(len(y)), key=lambda i: y[i], reverse=True)[:K])
    return len(sel & win) / float(K)


def eval_order(order, y):
    return {f"top{K}": mean_top(order, y, K) for K in KS} | {f"cap{K}": cap_top(order, y, K) for K in KS}


def train_predict_mlp(days, min_train, epochs):
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    torch.manual_seed(1)

    def pair_loss(s, y):
        diff = s.unsqueeze(1) - s.unsqueeze(0)
        yd = y.unsqueeze(1) - y.unsqueeze(0)
        mask = (yd > 0).float()
        denom = mask.sum()
        if denom.item() <= 0:
            return None
        return -(F.logsigmoid(diff) * mask).sum() / denom

    def train(train_days):
        torch.manual_seed(1)
        model = nn.Sequential(nn.Linear(len(FEATURE_BASE), 8), nn.Tanh(), nn.Dropout(0.35), nn.Linear(8, 1))
        opt = torch.optim.Adam(model.parameters(), lr=0.03, weight_decay=2e-2)
        tens = [(torch.tensor(t["X"], dtype=torch.float32), torch.tensor(t["excess"], dtype=torch.float32)) for t in train_days]
        model.train()
        for _ in range(epochs):
            for X, y in tens:
                opt.zero_grad()
                s = model(X).squeeze(-1)
                loss = pair_loss(s, y)
                if loss is not None and torch.isfinite(loss):
                    loss.backward(); opt.step()
        model.eval()
        return model

    folds = []
    for ti in range(min_train, len(days)):
        test = days[ti]
        try:
            model = train(days[:ti])
            with torch.no_grad():
                mlp = model(torch.tensor(test["X"], dtype=torch.float32)).squeeze(-1).tolist()
        except Exception as e:
            mlp = [0.0] * len(test["excess"])
            test["err"] = f"{type(e).__name__}: {e}"[:120]
        folds.append((test, mlp))
    return folds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--min-train", type=int, default=5)
    ap.add_argument("--epochs", type=int, default=80)
    args = ap.parse_args()
    root = Path(args.project_root)
    days = build_days(root)
    folds_raw = train_predict_mlp(days, args.min_train, args.epochs)

    per_day = []
    buckets = {}
    def add(name, stat):
        buckets.setdefault(name, {k: [] for k in [f"top{K}" for K in KS] + [f"cap{K}" for K in KS]})
        for k, v in stat.items():
            if v is not None:
                buckets[name][k].append(v)

    for test, mlp in folds_raw:
        n = len(test["excess"]); y = test["excess"]
        ord_amt = sorted(range(n), key=lambda i: test["amt"][i], reverse=True)
        ord_sparse = sorted(range(n), key=lambda i: test["sparse"][i], reverse=True)
        ord_mlp_all = sorted(range(n), key=lambda i: mlp[i], reverse=True)
        dayrec = {"date": test["date"], "n": n}
        stats = {"amt": eval_order(ord_amt, y), "sparse": eval_order(ord_sparse, y), "mlp_all": eval_order(ord_mlp_all, y)}
        for name, st in stats.items():
            add(name, st); dayrec[name] = st
        # restricted MLP rerank inside v10_amt Top-M
        for M in MS:
            pool = ord_amt[:min(M, n)]
            ord_rm = sorted(pool, key=lambda i: mlp[i], reverse=True)
            name = f"amtTop{M}_mlp"
            st = eval_order(ord_rm, y)
            add(name, st); dayrec[name] = st
        per_day.append(dayrec)

    summary = {name: {k: round(statistics.mean(v), 4) if v else None for k, v in vals.items()} for name, vals in buckets.items()}
    # rank by Top5 mean, then cap5
    ranked = sorted(summary.items(), key=lambda kv: ((kv[1].get("top5") or -999), (kv[1].get("cap5") or -999)), reverse=True)

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "n_days": len(days), "oos_days": len(per_day), "min_train": args.min_train,
              "epochs": args.epochs, "features": FEATURE_BASE, "sparse_weights": SPARSE_W,
              "summary": summary, "ranked_by_top5": ranked, "per_day": per_day}
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_rerank_v23.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# v23 受限头部重排器 / 稀疏 IC 公式", "",
         f"- 生成: {report['generated_at']} ｜训练日: {len(days)} ｜出样本天: {len(per_day)} ｜epochs: {args.epochs}",
         "- 目标: 只优化当日 Top-3/5 超额; ML 仅限 v10_amt 头部池内重排, 不允许全市场乱选。", "",
         "## 出样本均值", "",
         "| 策略 | Top3均值 | Top5均值 | Top10均值 | cap3 | cap5 | cap10 |",
         "|---|---|---|---|---|---|---|"]
    for name, s in ranked:
        L.append(f"| {name} | {s.get('top3')} | {s.get('top5')} | {s.get('top10')} | {s.get('cap3')} | {s.get('cap5')} | {s.get('cap10')} |")
    L += ["", "## 逐日关键策略 Top5", "", "| 日期 | amt | sparse | mlp_all | amtTop10_mlp | amtTop20_mlp | amtTop30_mlp |",
          "|---|---|---|---|---|---|---|"]
    for d in per_day:
        L.append(f"| {d['date']} | {round(d['amt']['top5'],3)} | {round(d['sparse']['top5'],3)} | {round(d['mlp_all']['top5'],3)} | {round(d['amtTop10_mlp']['top5'],3)} | {round(d['amtTop20_mlp']['top5'],3)} | {round(d['amtTop30_mlp']['top5'],3)} |")
    L += ["", "> 门槛: 若受限重排 Top5 均值高于 v10_amt, 且 cap5/逐日亏损不恶化, 才有上线价值;",
          "> 若 sparse_ic 接近或超越 v10_amt, 优先考虑可解释公式而非黑盒。"]
    (audit / "premarket_rerank_v23.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"oos_days": len(per_day), "ranked_by_top5": ranked[:6]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
