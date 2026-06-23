#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v24_day_gate.py — Top-5 同日策略的日级空仓门控(只读回测)。

v19 发现: v10_amt Top-3/5 同日 alpha 明显, 但有坏日。排序之外, 组合收益提升的
关键可能是“今天做不做”。本脚本只使用盘前可见的日级特征, walk-forward 学习简单门槛规则,
决定当日是否交易 Top-5; 跳过日按 0 收益计入, 防止选择性统计。

日级特征(盘前可见): 候选数、Top分数强度、Top5/Top30分数均值、Top5分数集中度、
Top5低开/高开分布、risk_flag占比、weak_breadth占比等。

输出: reports/_audit/premarket_day_gate_v24.{json,md}
用法: python3 scripts/v24_day_gate.py [--min-train 6]
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


def q(xs, p):
    xs = sorted([x for x in xs if x is not None])
    if not xs:
        return None
    return xs[int(p * (len(xs) - 1))]


def stat(xs):
    if not xs:
        return {"n": 0}
    pos = [x for x in xs if x > 0]
    neg = [x for x in xs if x < 0]
    ap = statistics.mean(pos) if pos else 0.0
    an = statistics.mean(neg) if neg else 0.0
    return {"n": len(xs), "mean": round(statistics.mean(xs), 3), "median": round(statistics.median(xs), 3),
            "win_rate": round(len(pos)/len(xs), 3), "odds": round(ap/abs(an), 2) if neg and an else None,
            "sum": round(sum(xs), 3), "p10": round(q(xs, 0.1), 3), "p90": round(q(xs, 0.9), 3)}


def score_row(r):
    return v10.score(r["f"], r["amt"], v10.V10AMT_W)


def build_days(root):
    days = v12.load_days_plus(root, v10.Daily(root))
    out = []
    for d in days:
        rows = d["rows"]
        for r in rows:
            r["_amt_score"] = score_row(r)
        order = sorted(range(len(rows)), key=lambda i: rows[i]["_amt_score"], reverse=True)
        top5 = [rows[i] for i in order[:5]]
        top10 = [rows[i] for i in order[:10]]
        top30 = [rows[i] for i in order[:30]]
        s5 = [r["_amt_score"] for r in top5]
        s10 = [r["_amt_score"] for r in top10]
        s30 = [r["_amt_score"] for r in top30]
        lcp5 = [v10.field_value(r, "latest_change_pct") for r in top5]
        ex5 = [r["excess"] for r in top5]
        ret5 = statistics.mean(ex5) if ex5 else 0.0
        feat = {
            "n_candidates": len(rows),
            "top1_score": s5[0] if s5 else 0,
            "top5_score_mean": statistics.mean(s5) if s5 else 0,
            "top10_score_mean": statistics.mean(s10) if s10 else 0,
            "top30_score_mean": statistics.mean(s30) if s30 else 0,
            "score_gap_1_5": (s5[0] - s5[-1]) if len(s5) >= 5 else 0,
            "score_gap_5_30": (statistics.mean(s5) - statistics.mean(s30)) if s5 and s30 else 0,
            "top5_lowopen_frac": sum(1 for x in lcp5 if x is not None and x < 2.0) / len(lcp5) if lcp5 else 0,
            "top5_highopen_frac": sum(1 for x in lcp5 if x is not None and x >= 5.0) / len(lcp5) if lcp5 else 0,
            "top5_open_mean": statistics.mean([x for x in lcp5 if x is not None]) if any(x is not None for x in lcp5) else 0,
            "top30_risk_frac": sum(1 for r in top30 if r.get("risk_flag")) / len(top30) if top30 else 0,
            "top30_weak_frac": sum(1 for r in top30 if r.get("weak_breadth")) / len(top30) if top30 else 0,
        }
        out.append({"date": d["date"], "feat": feat, "top5_ret": ret5,
                    "top5_codes": [r["code"] for r in top5],
                    "top5_excess": [round(r["excess"], 2) for r in top5]})
    return out


def rules_from_train(train):
    keys = list(train[0]["feat"].keys()) if train else []
    rules = []
    # one-sided threshold rules: x >= threshold or x <= threshold. Thresholds from train quartiles.
    for k in keys:
        vals = [d["feat"][k] for d in train]
        ths = sorted(set([q(vals, p) for p in (0.25, 0.5, 0.75)]))
        for th in ths:
            if th is None:
                continue
            for op in (">=", "<="):
                sel = [d["top5_ret"] for d in train if (d["feat"][k] >= th if op == ">=" else d["feat"][k] <= th)]
                if len(sel) < max(3, int(len(train)*0.35)):
                    continue
                # objective: skipped days count as zero, so rule_ret is sum(selected)/all_days
                trade_series = [(d["top5_ret"] if (d["feat"][k] >= th if op == ">=" else d["feat"][k] <= th) else 0.0) for d in train]
                base_series = [d["top5_ret"] for d in train]
                gain = statistics.mean(trade_series) - statistics.mean(base_series)
                win = sum(1 for x in trade_series if x > 0) / len(trade_series)
                rules.append({"k": k, "op": op, "th": th, "train_gain": gain,
                              "train_mean": statistics.mean(trade_series), "train_win": win,
                              "trade_frac": sum(1 for x in trade_series if x != 0.0)/len(trade_series)})
    rules.sort(key=lambda r: (r["train_gain"], r["train_mean"], r["train_win"]), reverse=True)
    return rules


def apply_rule(rule, feat):
    if rule is None:
        return True
    return feat[rule["k"]] >= rule["th"] if rule["op"] == ">=" else feat[rule["k"]] <= rule["th"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--min-train", type=int, default=6)
    args = ap.parse_args()
    root = Path(args.project_root)
    days = build_days(root)

    folds = []
    for ti in range(args.min_train, len(days)):
        train = days[:ti]
        test = days[ti]
        rules = rules_from_train(train)
        best = rules[0] if rules else None
        do_trade = apply_rule(best, test["feat"])
        folds.append({"date": test["date"], "base_ret": test["top5_ret"],
                      "gated_ret": test["top5_ret"] if do_trade else 0.0,
                      "trade": bool(do_trade), "rule": best,
                      "feat": test["feat"], "codes": test["top5_codes"], "excess": test["top5_excess"]})

    base = [f["base_ret"] for f in folds]
    gated = [f["gated_ret"] for f in folds]
    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "n_days": len(days), "oos_days": len(folds), "min_train": args.min_train,
              "baseline_top5": stat(base), "gated_top5": stat(gated), "folds": folds}

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_day_gate_v24.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# v24 Top-5 日级空仓门控", "",
         f"- 生成: {report['generated_at']} ｜总天数: {len(days)} ｜出样本天: {len(folds)} ｜min_train: {args.min_train}",
         "- 跳过日按 0 收益计入; 只用盘前可见日级特征。", "",
         "## 出样本表现", "",
         "| 策略 | n | 均值 | 中位 | 胜率(含空仓0) | 赔率 | 累计 | p10 | p90 |",
         "|---|---|---|---|---|---|---|---|---|",
         f"| 每天做 v10_amt Top-5 | {report['baseline_top5']['n']} | {report['baseline_top5']['mean']} | {report['baseline_top5']['median']} | {report['baseline_top5']['win_rate']} | {report['baseline_top5']['odds']} | {report['baseline_top5']['sum']} | {report['baseline_top5']['p10']} | {report['baseline_top5']['p90']} |",
         f"| 日级门控后 | {report['gated_top5']['n']} | {report['gated_top5']['mean']} | {report['gated_top5']['median']} | {report['gated_top5']['win_rate']} | {report['gated_top5']['odds']} | {report['gated_top5']['sum']} | {report['gated_top5']['p10']} | {report['gated_top5']['p90']} |",
         "", "## 逐日出样本", "", "| 日期 | 交易? | 原Top5 | 门控后 | 规则 | Top5代码 | Top5超额 |", "|---|---|---|---|---|---|---|"]
    for f in folds:
        r = f.get("rule") or {}
        rule_txt = f"{r.get('k')} {r.get('op')} {round(r.get('th'),3) if isinstance(r.get('th'), (int,float)) else r.get('th')}" if r else "none"
        L.append(f"| {f['date']} | {'Y' if f['trade'] else 'N'} | {round(f['base_ret'],3)} | {round(f['gated_ret'],3)} | {rule_txt} | {','.join(f['codes'])} | {f['excess']} |")
    L += ["", "> 门槛: 门控只有在跳过日计0后仍抬升均值/累计且不明显牺牲胜率时才有上线价值;",
          "> 若不能通过, 说明坏日目前无法由盘前日级统计稳定识别, 维持每日 Top-5。"]
    (audit / "premarket_day_gate_v24.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"baseline_top5": report["baseline_top5"], "gated_top5": report["gated_top5"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
