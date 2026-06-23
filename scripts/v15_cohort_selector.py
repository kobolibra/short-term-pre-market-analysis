#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v15_cohort_selector.py - cohort-aware selector + walk-forward OOS validation.

Motivation (from v12/v13/v14):
  - 88% of daily true Top-30 winners are risk_flag, 62% are low-open; the current
    edge buries them via a blanket risk_penalty -> capture@30 only ~0.15-0.19.
  - net_pressure has negative IC inside the low-open cohort -> drop it.

Approach: split candidates each day into low-open vs high-open cohorts, rank each
cohort with cohort-specific positive-IC weights (net_pressure excluded), and
allocate the 30 slots by the train-set low-open winner share. Compare OUT-OF-SAMPLE
against the stored edge and the fixed v10_amt weights on:
  - capture@30 of same-day excess winners
  - mean excess / win-rate of the 30 picks
  - mean T+1 payoff (hold_open_t1high) of the 30 picks

This is a validation gate: only promote the cohort selector if it beats the current
edge out-of-sample. No live model change here.

excess = (close-open)/preclose*100 (same-day premarket metric).
Output: reports/_audit/premarket_cohort_selector.{json,md}
Usage: python3 scripts/v15_cohort_selector.py [--top-n 30] [--min-train 5] [--low-open-max 2.0]
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

# percentile-scale (0-100) fields only, so equal-fill=50 is sane; net_pressure excluded
SEL_FIELDS = ["auction_strength", "liquidity", "money", "pressure_score",
              "weimai_strength", "orderbook", "amt_pct"]
LOW_MAX = 2.0


def is_low(r):
    v = r["f"].get("latest_change_pct")
    return (v is not None) and (v < LOW_MAX)


def score_row(r, w):
    s = 0.0
    for k, wk in w.items():
        v = v10.field_value(r, k)
        s += wk * (v if isinstance(v, (int, float)) else 50.0)
    return s


def cohort_ic_weights(train_days, want_low):
    w = {}
    for fld in SEL_FIELDS:
        ds = []
        for d in train_days:
            rows = [r for r in d["rows"] if is_low(r) == want_low]
            ds.append(v10.daily_ic(rows, fld))
        m, _, _ = v10.mean_icir(ds)
        w[fld] = max(m, 0.0) if m is not None else 0.0
    tot = sum(w.values())
    if tot <= 0:
        return {k: 1.0 / len(SEL_FIELDS) for k in SEL_FIELDS}
    return {k: w[k] / tot for k in SEL_FIELDS}


def low_share_train(train_days, topN):
    fr = []
    for d in train_days:
        rows = d["rows"]
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)[:topN]
        if not order:
            continue
        fr.append(sum(1 for i in order if is_low(rows[i])) / float(len(order)))
    return statistics.mean(fr) if fr else 0.5


def pick_cohort(rows, w_low, w_high, n_low, n_high, topN):
    low_idx = [i for i, r in enumerate(rows) if is_low(r)]
    high_idx = [i for i, r in enumerate(rows) if not is_low(r)]
    low_sorted = sorted(low_idx, key=lambda i: score_row(rows[i], w_low), reverse=True)
    high_sorted = sorted(high_idx, key=lambda i: score_row(rows[i], w_high), reverse=True)
    sel = low_sorted[:n_low] + high_sorted[:n_high]
    if len(sel) < topN:
        rest = low_sorted[n_low:] + high_sorted[n_high:]
        rest.sort(key=lambda i: score_row(rows[i], w_low if is_low(rows[i]) else w_high), reverse=True)
        sel += rest[:topN - len(sel)]
    return set(sel[:topN])


def pick_by_score(rows, key, topN):
    order = sorted(range(len(rows)),
                   key=lambda i: (rows[i][key] if rows[i].get(key) is not None else -1e9),
                   reverse=True)
    return set(order[:topN])


def pick_by_weights(rows, w, topN):
    order = sorted(range(len(rows)), key=lambda i: score_row(rows[i], w), reverse=True)
    return set(order[:topN])


def eval_sel(rows, sel, winners, topN):
    sel = list(sel)
    ex = [rows[i]["excess"] for i in sel]
    t1 = [rows[i]["t1"]["hold_open_t1high"] for i in sel
          if rows[i].get("t1") and rows[i]["t1"].get("hold_open_t1high") is not None]
    cap = len(set(sel) & set(winners)) / float(min(topN, len(winners)) or 1)
    return {"capture": cap,
            "mean_excess": (statistics.mean(ex) if ex else None),
            "win_rate": (sum(1 for e in ex if e > 0) / len(ex) if ex else None),
            "t1_high_payoff": (statistics.mean(t1) if t1 else None)}


def agg(rows_of_dicts, key):
    xs = [d[key] for d in rows_of_dicts if d.get(key) is not None]
    return round(statistics.mean(xs), 4) if xs else None


def main():
    global LOW_MAX
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--min-train", type=int, default=5)
    ap.add_argument("--low-open-max", type=float, default=2.0)
    args = ap.parse_args()
    LOW_MAX = args.low_open_max
    root = Path(args.project_root)
    daily = v10.Daily(root)
    line = v14.Line(root)
    topN = args.top_n
    days = v12.load_days_plus(root, daily)
    for d in days:
        for r in d["rows"]:
            r["t1"] = v14.t1_metrics(line, r["code"], d["date"])

    res = {"edge_stored": [], "v10_amt": [], "cohort": []}
    detail = []
    for ti in range(args.min_train, len(days)):
        train = days[:ti]
        test = days[ti]
        rows = test["rows"]
        if len(rows) < topN:
            continue
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = order[:topN]
        w_low = cohort_ic_weights(train, True)
        w_high = cohort_ic_weights(train, False)
        ls = low_share_train(train, topN)
        n_low = int(round(topN * ls))
        n_high = topN - n_low
        sel_edge = pick_by_score(rows, "edge_old", topN)
        sel_amt = pick_by_weights(rows, v10.V10AMT_W, topN)
        sel_coh = pick_cohort(rows, w_low, w_high, n_low, n_high, topN)
        e0 = eval_sel(rows, sel_edge, winners, topN)
        e1 = eval_sel(rows, sel_amt, winners, topN)
        e2 = eval_sel(rows, sel_coh, winners, topN)
        res["edge_stored"].append(e0)
        res["v10_amt"].append(e1)
        res["cohort"].append(e2)
        detail.append({"date": test["date"], "low_share": round(ls, 3), "n_low": n_low,
                       "cap_edge": round(e0["capture"], 3), "cap_amt": round(e1["capture"], 3),
                       "cap_cohort": round(e2["capture"], 3)})

    def summarize(name):
        r = res[name]
        return {"oos_days": len(r),
                "capture": agg(r, "capture"), "mean_excess": agg(r, "mean_excess"),
                "win_rate": agg(r, "win_rate"), "t1_high_payoff": agg(r, "t1_high_payoff")}
    summary = {m: summarize(m) for m in res}
    # full-sample cohort weights for reference
    ref_w_low = cohort_ic_weights(days, True)
    ref_w_high = cohort_ic_weights(days, False)
    ref_low_share = low_share_train(days, topN)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "top_n": topN, "min_train": args.min_train, "low_open_max": LOW_MAX,
        "n_days": len(days), "oos_days": summary["cohort"]["oos_days"],
        "oos_summary": summary,
        "per_day": detail,
        "fullsample_low_share": round(ref_low_share, 3),
        "fullsample_weights_low": {k: round(v, 4) for k, v in ref_w_low.items()},
        "fullsample_weights_high": {k: round(v, 4) for k, v in ref_w_high.items()},
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_cohort_selector.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# v15 cohort-aware 选择器 walk-forward 出样本验证", "",
         f"- 生成: {report['generated_at']} ｜总交易日: {len(days)} ｜出样本天数: {report['oos_days']} ｜Top-N: {topN}",
         f"- 低开阈值: <{LOW_MAX} ｜全样本低开赢家占比: {report['fullsample_low_share']}", "",
         "## 出样本对比 (越高越好)", "",
         "| 模型 | capture@30 | 选中均excess | 选中胜率 | 选中次日高招赔率 |",
         "|---|---|---|---|---|"]
    label = {"edge_stored": "现行 edge(stored)", "v10_amt": "v10_amt 固定权重", "cohort": "v15 cohort-aware"}
    for m in ["edge_stored", "v10_amt", "cohort"]:
        s = summary[m]
        L.append(f"| {label[m]} | {s['capture']} | {s['mean_excess']} | {s['win_rate']} | {s['t1_high_payoff']} |")
    L += ["", "## 逐日 capture@30", "",
          "| 日期 | 低开配额 | n_low | edge | v10_amt | cohort |", "|---|---|---|---|---|---|"]
    for d in detail:
        L.append(f"| {d['date']} | {d['low_share']} | {d['n_low']} | {d['cap_edge']} | {d['cap_amt']} | {d['cap_cohort']} |")
    L += ["", "## 全样本 cohort 权重 (供上线参考)", "",
          f"- 低开池权重: {report['fullsample_weights_low']}",
          f"- 高开池权重: {report['fullsample_weights_high']}",
          "", "> 结论门槛: 仅当 cohort capture@30 且均excess 出样本超过现行 edge 时, 才推荐上线。"]
    (audit / "premarket_cohort_selector.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"oos_days": report["oos_days"], "oos_summary": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
