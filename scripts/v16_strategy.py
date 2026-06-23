#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v16_strategy.py — 基于回测结论的两件落地实验(只读):

A) 排序融合: v10_amt 打分 与 stored edge 的 z-score 融合(walk-forward 选 α),
   看能否兼得 capture(广度) 与 选中均超额(精度)。参照: 纯edge(α=0)/纯amt(α=1)。
B) 次日冲高高抛出场规则回测: 对每日 Top-30 候选(按 edge / 按 amt),
   模拟“竞价开盘买入, 次日盘中触及目标位 T% 即卖, 否则持到次日收盘”,
   扫描 T, 给出均收益/胜率/赔率/分位, 把 v14 的 +8.58%% 上限变成可执行目标位。

只读, 不改线上模型; 结论供上线决策。
输出: reports/_audit/premarket_strategy_v16.{json,md}
用法: python3 scripts/v16_strategy.py [--top-n 30] [--min-train 5]
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
import v14_horizon as v14

ALPHAS = [0.0, 0.25, 0.5, 0.75, 1.0]
TARGETS = [3.0, 5.0, 8.0, 10.0, 12.0]


def _zparams(vals):
    xs = [v for v in vals if v is not None]
    if len(xs) < 2:
        return None
    sd = statistics.pstdev(xs)
    if sd == 0:
        return None
    return statistics.mean(xs), sd


def attach_scores(day):
    rows = day["rows"]
    amt_scores = []
    for r in rows:
        try:
            a = v10.score(r["f"], r["amt"], v10.V10AMT_W)
        except Exception:
            a = None
        r["_amt_score"] = a
        amt_scores.append(a)
    za = _zparams(amt_scores)
    ze = _zparams([r["edge_old"] for r in rows])
    for r in rows:
        r["_za"] = ((r["_amt_score"] - za[0]) / za[1]) if (za and r["_amt_score"] is not None) else 0.0
        r["_ze"] = ((r["edge_old"] - ze[0]) / ze[1]) if (ze and r["edge_old"] is not None) else 0.0
    return day


def winners_idx(rows, topN):
    return set(sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)[:topN])


def select_by(rows, key, topN):
    return sorted(range(len(rows)),
                  key=lambda i: (key(rows[i]) if key(rows[i]) is not None else -1e9),
                  reverse=True)[:topN]


def day_metrics(rows, sel_idx, topN):
    winners = winners_idx(rows, topN)
    sel = set(sel_idx)
    cap = len(winners & sel) / float(min(topN, len(winners)) or 1)
    exs = [rows[i]["excess"] for i in sel_idx]
    highs = [rows[i].get("hold_open_t1high") for i in sel_idx if rows[i].get("hold_open_t1high") is not None]
    return {"capture": cap,
            "mean_excess": statistics.mean(exs) if exs else None,
            "win_rate": (sum(1 for e in exs if e > 0) / len(exs)) if exs else None,
            "mean_t1high": statistics.mean(highs) if highs else None}


def _xstat(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    s = sorted(xs)
    pos = [x for x in xs if x > 0]
    neg = [x for x in xs if x < 0]
    avg_pos = statistics.mean(pos) if pos else 0.0
    avg_neg = statistics.mean(neg) if neg else 0.0
    odds = round(avg_pos / abs(avg_neg), 2) if (neg and avg_neg != 0) else None
    return {"n": len(xs), "mean": round(statistics.mean(xs), 3), "median": round(statistics.median(xs), 3),
            "win_rate": round(len(pos) / len(xs), 3), "odds": odds,
            "p90": round(s[int(0.9 * (len(s) - 1))], 2), "p10": round(s[int(0.1 * (len(s) - 1))], 2)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--min-train", type=int, default=5)
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    line = v14.Line(root)
    topN = args.top_n
    days = v12.load_days_plus(root, daily)

    for d in days:
        for r in d["rows"]:
            m = v14.t1_metrics(line, r["code"], d["date"])
            r["hold_open_t1high"] = m["hold_open_t1high"] if m else None
            r["hold_open_t1close"] = m["hold_open_t1close"] if m else None
        attach_scores(d)

    elig = [d for d in days if len(d["rows"]) >= topN]

    def blend_key(alpha):
        return lambda r: alpha * r["_za"] + (1 - alpha) * r["_ze"]

    # Part A: fixed-alpha full-sample sweep
    fixed = {}
    for a in ALPHAS:
        caps, exs, wins, highs = [], [], [], []
        for d in elig:
            mt = day_metrics(d["rows"], select_by(d["rows"], blend_key(a), topN), topN)
            caps.append(mt["capture"])
            if mt["mean_excess"] is not None:
                exs.append(mt["mean_excess"])
            if mt["win_rate"] is not None:
                wins.append(mt["win_rate"])
            if mt["mean_t1high"] is not None:
                highs.append(mt["mean_t1high"])
        fixed[str(a)] = {"capture": round(statistics.mean(caps), 3) if caps else None,
                         "mean_excess": round(statistics.mean(exs), 3) if exs else None,
                         "win_rate": round(statistics.mean(wins), 3) if wins else None,
                         "mean_t1high": round(statistics.mean(highs), 3) if highs else None}

    # Part A: walk-forward best-alpha (train by capture)
    wf = {"alpha": [], "capture": [], "mean_excess": [], "win_rate": [], "mean_t1high": []}
    for t in range(len(elig)):
        if t < args.min_train:
            continue
        train = elig[:t]
        best_a, best_c = ALPHAS[0], -1.0
        for a in ALPHAS:
            cs = [day_metrics(d["rows"], select_by(d["rows"], blend_key(a), topN), topN)["capture"] for d in train]
            mc = statistics.mean(cs) if cs else 0.0
            if mc > best_c:
                best_c, best_a = mc, a
        test = elig[t]
        mt = day_metrics(test["rows"], select_by(test["rows"], blend_key(best_a), topN), topN)
        wf["alpha"].append(best_a)
        wf["capture"].append(mt["capture"])
        for k in ("mean_excess", "win_rate", "mean_t1high"):
            if mt[k] is not None:
                wf[k].append(mt[k])
    wf_summary = {"n_test_days": len(wf["capture"]),
                  "alpha_usage": {str(a): wf["alpha"].count(a) for a in ALPHAS},
                  "capture": round(statistics.mean(wf["capture"]), 3) if wf["capture"] else None,
                  "mean_excess": round(statistics.mean(wf["mean_excess"]), 3) if wf["mean_excess"] else None,
                  "win_rate": round(statistics.mean(wf["win_rate"]), 3) if wf["win_rate"] else None,
                  "mean_t1high": round(statistics.mean(wf["mean_t1high"]), 3) if wf["mean_t1high"] else None}

    # Part B: exit-rule backtest
    def exit_backtest(rank_key):
        per_T = {T: [] for T in TARGETS}
        base_close, ceil_high = [], []
        for d in elig:
            for i in select_by(d["rows"], rank_key, topN):
                r = d["rows"][i]
                hr, cr = r.get("hold_open_t1high"), r.get("hold_open_t1close")
                if hr is None or cr is None:
                    continue
                base_close.append(cr)
                ceil_high.append(hr)
                for T in TARGETS:
                    per_T[T].append(T if hr >= T else cr)
        return {"baseline_hold_t1close": _xstat(base_close),
                "ceiling_t1high": _xstat(ceil_high),
                "target_exit": {str(T): _xstat(v) for T, v in per_T.items()}}

    exit_edge = exit_backtest(lambda r: r["edge_old"])
    exit_amt = exit_backtest(lambda r: r["_amt_score"])

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "top_n": topN, "min_train": args.min_train,
        "n_days": len(days), "n_eligible_days": len(elig),
        "partA_ensemble": {"note": "alpha=1 纯 v10_amt; alpha=0 纯 stored edge; 中间为 z-score 融合",
                            "fixed_alpha_full_sample": fixed,
                            "walk_forward_best_alpha": wf_summary},
        "partB_exit_rule": {"note": "竞价开盘买入; 次日最高>=T%% 则按 T%% 卖出, 否则持到次日收盘",
                            "select_by_edge_top30": exit_edge,
                            "select_by_amt_top30": exit_amt},
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_strategy_v16.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# v16 策略落地回测 (排序融合 + 次日高抛出场)", "",
         f"- 生成: {report['generated_at']} ｜交易日: {len(days)} ｜参与日: {len(elig)} ｜Top-N: {topN}", "",
         "## A. 排序融合 (α=1 纯amt, α=0 纯edge)", "",
         "### 全样本固定-α 扫描", "",
         "| α | capture@30 | 选中均excess | 选中胜率 | 选中次日高赔率 |", "|---|---|---|---|---|"]
    for a in ALPHAS:
        s = fixed[str(a)]
        L.append(f"| {a} | {s['capture']} | {s['mean_excess']} | {s['win_rate']} | {s['mean_t1high']} |")
    L += ["", f"### walk-forward 选 α (出样本, {wf_summary['n_test_days']} 天)", "",
          f"- α 使用分布: {wf_summary['alpha_usage']}",
          f"- capture@30: **{wf_summary['capture']}** ｜选中均excess: **{wf_summary['mean_excess']}** ｜胜率: {wf_summary['win_rate']} ｜次日高赔率: {wf_summary['mean_t1high']}", ""]

    def exit_block(title, ex):
        b = ex["baseline_hold_t1close"]
        c = ex["ceiling_t1high"]
        rows = ["", f"### {title}", "",
                "| 出场规则 | n | 均收益 | 中位 | 胜率 | 赔率 | p90 | p10 |", "|---|---|---|---|---|---|---|---|",
                f"| 持到次日收盘(baseline) | {b.get('n')} | {b.get('mean')} | {b.get('median')} | {b.get('win_rate')} | {b.get('odds')} | {b.get('p90')} | {b.get('p10')} |"]
        for T in TARGETS:
            s = ex["target_exit"][str(T)]
            rows.append(f"| 目标位 {T}%% 高抛 | {s.get('n')} | {s.get('mean')} | {s.get('median')} | {s.get('win_rate')} | {s.get('odds')} | {s.get('p90')} | {s.get('p10')} |")
        rows.append(f"| 次日最高(上限,不可执行) | {c.get('n')} | {c.get('mean')} | {c.get('median')} | {c.get('win_rate')} | {c.get('odds')} | {c.get('p90')} | {c.get('p10')} |")
        return rows

    L += ["## B. 次日冲高高抛出场规则回测"]
    L += exit_block("选股=按 edge Top-30", exit_edge)
    L += exit_block("选股=按 v10_amt Top-30", exit_amt)
    L += ["", "> 结论门槛: 融合仅在出样本 capture 且均excess 同时不劣于 α=1/α=0 才值得上线;",
          "> 出场规则选能明显括升均收益/赔率的最低目标位。"]
    (audit / "premarket_strategy_v16.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"n_eligible_days": len(elig),
                      "partA_fixed": fixed, "partA_walk_forward": wf_summary,
                      "partB_exit_edge_target": exit_edge["target_exit"],
                      "partB_baseline_edge": exit_edge["baseline_hold_t1close"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
