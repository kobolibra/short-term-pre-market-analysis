#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v25_sparse_validation.py — sparse_ic 可部署公式的全面验证(只读)。

v23 发现 sparse_ic 在 Top5 当日超额上超过 v10_amt, 且比 ML 黑盒稳定。
本脚本验证它是否可作为新生产公式或 Top5 专用公式:
  1) 全样本: 同日 excess Top3/5/10/30 与 T+1 hold Top5/10/30。
  2) walk-forward(OOS): 从 min_train 后逐日评估同日 Top3/5/10/30。
  3) blend: v10_amt 与 sparse_ic 按截面分位秩 70/30, 50/50, 30/70 融合。

跳过日不做过滤; 纯排序比较。
输出: reports/_audit/premarket_sparse_v25.{json,md}
"""
from __future__ import annotations
import argparse, json, statistics, sys, traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12
import v14_horizon as v14

SPARSE_W = {"deriv.amt_x_auc": 0.24, "auction_strength": 0.22, "liquidity": 0.18,
            "pressure_score": 0.13, "deriv.money_x_liq": 0.12, "money": 0.11}
KS = [3, 5, 10, 30]


def q(xs, p):
    xs = sorted([x for x in xs if x is not None])
    if not xs: return None
    return xs[int(p * (len(xs)-1))]


def stat(xs):
    xs = [x for x in xs if x is not None]
    if not xs: return {"n": 0}
    pos = [x for x in xs if x > 0]; neg = [x for x in xs if x < 0]
    ap = statistics.mean(pos) if pos else 0.0; an = statistics.mean(neg) if neg else 0.0
    return {"n": len(xs), "mean": round(statistics.mean(xs), 4), "median": round(statistics.median(xs), 4),
            "win_rate": round(len(pos)/len(xs), 4), "odds": round(ap/abs(an), 3) if neg and an else None,
            "sum": round(sum(xs), 4), "p10": round(q(xs,0.1),4), "p90": round(q(xs,0.9),4)}


def build_days(root):
    daily = v10.Daily(root); line = v14.Line(root)
    days = v12.load_days_plus(root, daily)
    out = []
    for d in days:
        rows = d["rows"]
        # base scores
        amt_raw = [v10.score(r["f"], r["amt"], v10.V10AMT_W) for r in rows]
        amt_pct = v10.pctl([(i, amt_raw[i]) for i in range(len(rows))])
        # sparse feature percentiles
        xr = {}
        for fld in SPARSE_W:
            iv = [(i, v10.field_value(rows[i], fld)) for i in range(len(rows)) if v10.field_value(rows[i], fld) is not None]
            xr[fld] = v10.pctl(iv) if iv else {}
        sparse = [sum(SPARSE_W[f]*xr[f].get(i,50.0) for f in SPARSE_W) for i in range(len(rows))]
        sparse_pct = v10.pctl([(i, sparse[i]) for i in range(len(rows))])
        labels_hold = []
        for r in rows:
            m = v14.t1_metrics(line, r["code"], d["date"]) or {}
            labels_hold.append(m.get("hold_open_t1close"))
        scores = {
            "v10_amt": amt_raw,
            "sparse_ic": sparse,
            "blend70_amt": [0.7*amt_pct.get(i,50.0)+0.3*sparse_pct.get(i,50.0) for i in range(len(rows))],
            "blend50": [0.5*amt_pct.get(i,50.0)+0.5*sparse_pct.get(i,50.0) for i in range(len(rows))],
            "blend70_sparse": [0.3*amt_pct.get(i,50.0)+0.7*sparse_pct.get(i,50.0) for i in range(len(rows))],
        }
        out.append({"date": d["date"], "scores": scores, "excess": [r["excess"] for r in rows],
                    "hold": labels_hold, "codes": [r["code"] for r in rows]})
    return out


def mean_top(scores, y, K):
    idx = [i for i,v in enumerate(y) if v is not None]
    order = sorted(idx, key=lambda i: scores[i], reverse=True)[:min(K,len(idx))]
    return statistics.mean([y[i] for i in order]) if order else None


def cap(scores, y, K):
    idx = [i for i,v in enumerate(y) if v is not None]
    K = min(K, len(idx))
    if K <= 0: return None
    pick = set(sorted(idx, key=lambda i: scores[i], reverse=True)[:K])
    win = set(sorted(idx, key=lambda i: y[i], reverse=True)[:K])
    return len(pick & win)/float(K)


def eval_series(days, model, label):
    res = {}
    for K in KS:
        vals = [mean_top(d["scores"][model], d[label], K) for d in days]
        caps = [cap(d["scores"][model], d[label], K) for d in days]
        res[f"top{K}"] = stat(vals)
        res[f"cap{K}"] = round(statistics.mean([c for c in caps if c is not None]),4) if any(c is not None for c in caps) else None
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--min-train", type=int, default=5)
    args = ap.parse_args()
    root = Path(args.project_root)
    days = build_days(root)
    models = list(days[0]["scores"].keys()) if days else []
    all_sample = {m: {"excess": eval_series(days, m, "excess"), "hold": eval_series(days, m, "hold")} for m in models}
    oos_days = days[args.min_train:]
    oos = {m: {"excess": eval_series(oos_days, m, "excess"), "hold": eval_series(oos_days, m, "hold")} for m in models}
    ranked_oos_top5 = sorted(models, key=lambda m: oos[m]["excess"]["top5"].get("mean", -999), reverse=True)

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"), "n_days": len(days),
              "oos_days": len(oos_days), "min_train": args.min_train, "sparse_weights": SPARSE_W,
              "all_sample": all_sample, "oos": oos, "ranked_oos_top5": ranked_oos_top5}
    audit = root/"reports"/"_audit"; audit.mkdir(parents=True, exist_ok=True)
    (audit/"premarket_sparse_v25.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def table(label, block):
        L = ["", f"## {label}", "", "| 模型 | Top3均值 | Top5均值 | Top10均值 | Top30均值 | cap5 | cap30 |",
             "|---|---|---|---|---|---|---|"]
        order = sorted(models, key=lambda m: block[m]["excess"]["top5"].get("mean", -999), reverse=True)
        for m in order:
            e = block[m]["excess"]
            L.append(f"| {m} | {e['top3'].get('mean')} | {e['top5'].get('mean')} | {e['top10'].get('mean')} | {e['top30'].get('mean')} | {e.get('cap5')} | {e.get('cap30')} |")
        return L

    L = ["# v25 sparse_ic 可部署公式全面验证", "", f"- 生成: {report['generated_at']} ｜总天数: {len(days)} ｜OOS天: {len(oos_days)}",
         "- sparse_ic = 0.24*amt_x_auc + 0.22*auction_strength + 0.18*liquidity + 0.13*pressure_score + 0.12*money_x_liq + 0.11*money (均用日内截面分位秩)"]
    L += table("OOS 同日 excess 排序", oos)
    L += table("全样本 同日 excess 排序", all_sample)
    L += ["", "## OOS 次日持仓 hold_open_t1close", "", "| 模型 | Top5均值 | Top10均值 | Top30均值 | cap30 |", "|---|---|---|---|---|"]
    order = sorted(models, key=lambda m: oos[m]["hold"]["top30"].get("mean", -999), reverse=True)
    for m in order:
        h=oos[m]["hold"]
        L.append(f"| {m} | {h['top5'].get('mean')} | {h['top10'].get('mean')} | {h['top30'].get('mean')} | {h.get('cap30')} |")
    L += ["", "> 判定: 只有 OOS Top5 均值、cap、逐层稳定性不差于 v10_amt 的公式才可替代;",
          "> 若 sparse 只赢 Top5 不赢 Top3/hold, 则作为当日 Top5 专用公式, 次日继续用 v10_amt Top30。"]
    (audit/"premarket_sparse_v25.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"oos_days": len(oos_days), "ranked_oos_top5": ranked_oos_top5,
                      "oos_top5": {m: oos[m]["excess"]["top5"] for m in models}}, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    try: sys.exit(main())
    except SystemExit: raise
    except Exception:
        traceback.print_exc(); sys.exit(1)
