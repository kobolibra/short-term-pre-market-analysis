#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v17_exit.py — 下行止损出场回测(只读)。

v16 发现: 固定目标位高抛封上不封下、反而变差; 杀伤来自左尾(次日 -9%% 的票)。
本脚本反过来: 只封下不封上 — 加止损, 让盈利奔跑。

规则: 竞价开盘(D.open)买入; 次日(D1) 若跌破 -S%% 则止损出场, 否则持到次日收盘。
  跳空穿透: 若 D1 开盘已 <= -S%%, 按开盘价成交(更保守);
  盘中触及: 若 D1 最低 <= -S%%, 按 -S%% 成交;
  未触发: 持到 D1 收盘。
上方不封顶(让盈利奔跑)。所有收益以买入价 D.open 为基准。

选股口径: 按 v10_amt Top-30 与 按 stored edge Top-30 各算一遍。
输出: reports/_audit/premarket_exit_v17.{json,md}
用法: python3 scripts/v17_exit.py [--top-n 30]
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

STOPS = [3.0, 5.0, 8.0]


def _trad(r):
    return bool(r) and str(r.get("tradestatus")) in ("1", "1.0")


def hold_metrics(line, code, d):
    d0 = line.row(code, d)
    d1 = line.next_row(code, d)
    if not _trad(d0) or not _trad(d1):
        return None
    o0 = v10.fnum(d0.get("open"))
    o1 = v10.fnum(d1.get("open"))
    h1 = v10.fnum(d1.get("high"))
    l1 = v10.fnum(d1.get("low"))
    c1 = v10.fnum(d1.get("close"))
    if not o0 or not c1:
        return None
    return {"ho_open": (o1 - o0) / o0 * 100 if o1 else None,
            "ho_low": (l1 - o0) / o0 * 100 if l1 else None,
            "ho_high": (h1 - o0) / o0 * 100 if h1 else None,
            "ho_close": (c1 - o0) / o0 * 100}


def realized_with_stop(m, S):
    op, lo, cl = m["ho_open"], m["ho_low"], m["ho_close"]
    if op is not None and op <= -S:
        return op
    if lo is not None and lo <= -S:
        return -S
    return cl


def select_by(rows, key, topN):
    return sorted(range(len(rows)),
                  key=lambda i: (key(rows[i]) if key(rows[i]) is not None else -1e9),
                  reverse=True)[:topN]


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
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    line = v14.Line(root)
    topN = args.top_n
    days = v12.load_days_plus(root, daily)

    for d in days:
        for r in d["rows"]:
            try:
                r["_amt_score"] = v10.score(r["f"], r["amt"], v10.V10AMT_W)
            except Exception:
                r["_amt_score"] = None
            r["_hm"] = hold_metrics(line, r["code"], d["date"])
    elig = [d for d in days if len(d["rows"]) >= topN]

    def exit_backtest(rank_key):
        base, ceil = [], []
        per_S = {S: [] for S in STOPS}
        for d in elig:
            for i in select_by(d["rows"], rank_key, topN):
                m = d["rows"][i].get("_hm")
                if not m or m["ho_close"] is None:
                    continue
                base.append(m["ho_close"])
                if m["ho_high"] is not None:
                    ceil.append(m["ho_high"])
                for S in STOPS:
                    per_S[S].append(realized_with_stop(m, S))
        return {"baseline_hold_close": _xstat(base),
                "ceiling_high": _xstat(ceil),
                "stop_then_hold": {str(S): _xstat(v) for S, v in per_S.items()}}

    res_amt = exit_backtest(lambda r: r["_amt_score"])
    res_edge = exit_backtest(lambda r: r["edge_old"])

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "top_n": topN, "n_days": len(days), "n_eligible_days": len(elig),
              "rule": "竞价开盘买入; 次日跌破 -S%% 止损(跳空按开盘), 否则持到次日收盘; 上方不封顶",
              "select_by_amt_top30": res_amt,
              "select_by_edge_top30": res_edge}
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_exit_v17.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def block(title, res):
        b = res["baseline_hold_close"]
        c = res["ceiling_high"]
        rows = ["", f"### {title}", "",
                "| 出场规则 | n | 均收益 | 中位 | 胜率 | 赔率 | p90 | p10 |", "|---|---|---|---|---|---|---|---|",
                f"| 持到次日收盘(baseline) | {b.get('n')} | {b.get('mean')} | {b.get('median')} | {b.get('win_rate')} | {b.get('odds')} | {b.get('p90')} | {b.get('p10')} |"]
        for S in STOPS:
            s = res["stop_then_hold"][str(S)]
            rows.append(f"| 止损 -{S}%% 后持到收盘 | {s.get('n')} | {s.get('mean')} | {s.get('median')} | {s.get('win_rate')} | {s.get('odds')} | {s.get('p90')} | {s.get('p10')} |")
        rows.append(f"| 次日最高(上限,不可执行) | {c.get('n')} | {c.get('mean')} | {c.get('median')} | {c.get('win_rate')} | {c.get('odds')} | {c.get('p90')} | {c.get('p10')} |")
        return rows

    L = ["# v17 下行止损出场回测 (只封下不封上)", "",
         f"- 生成: {report['generated_at']} ｜交易日: {len(days)} ｜参与日: {len(elig)} ｜Top-N: {topN}",
         f"- 规则: {report['rule']}"]
    L += ["", "## 选股=按 v10_amt Top-30"] + block("v10_amt 选股 + 止损", res_amt)
    L += ["", "## 选股=按 stored edge Top-30"] + block("edge 选股 + 止损", res_edge)
    L += ["", "> 门槛: 止损只有在 抬升均收益或赔率 且不明显牺牲胜率时才值得加; 若止损拉低均收益则说明左尾是假摇(次日低点多数会收回)。"]
    (audit / "premarket_exit_v17.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"n_eligible_days": len(elig),
                      "amt_baseline": res_amt["baseline_hold_close"], "amt_stops": res_amt["stop_then_hold"],
                      "edge_baseline": res_edge["baseline_hold_close"], "edge_stops": res_edge["stop_then_hold"]},
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
