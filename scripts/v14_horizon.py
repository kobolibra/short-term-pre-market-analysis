#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v14_horizon.py — 把评估周期从当日扩展到 T+1(次日)。(只读)

用 dailyline 日线计算每个候选票在信号日 D 之后的次日(D1)表现:
  t1_open   = (D1.open  - D1.preclose)/D1.preclose*100   次日开盘涨幅(隔夜跳空)
  t1_close  = (D1.close - D1.preclose)/D1.preclose*100   次日收盘涨幅
  t1_high   = (D1.high  - D1.preclose)/D1.preclose*100   次日最高涨幅
  hold_open_t1close = (D1.close - D.open)/D.open*100      竞价开盘买入持到次日收盘(真实策略收益)
  hold_open_t1high  = (D1.high  - D.open)/D.open*100      持有期最大可captured涨幅(赔率上限)

回答: 按 action 的 T+1 胜率/均值/赔率; 哪些字段对 T+1 有预测力(IC); 现行 edge 对 T+1 赢家的捕获率。
次日取该票严格大于 D 的下一个可交易日(tradestatus=1)。

输出: reports/_audit/premarket_horizon_t1.{json,md}
用法: python3 scripts/v14_horizon.py [--top-n 30]
"""
from __future__ import annotations
import argparse
import bisect
import csv
import json
import statistics
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12

TARGETS = ["t1_open", "t1_close", "t1_high", "hold_open_t1close", "hold_open_t1high"]
FLDS = v12.RAW_FLDS + v12.DERIV_FLDS


class Line:
    def __init__(self, root):
        self.dir = root / "dailyline" / "stocks"
        self.cache = {}

    def _load(self, code):
        code = str(code).zfill(6)
        if code not in self.cache:
            data, dates = {}, []
            f = self.dir / f"{code}.csv"
            if f.exists():
                with open(f, newline="") as fh:
                    for r in csv.DictReader(fh):
                        data[r["date"]] = r
                dates = sorted(data.keys())
            self.cache[code] = (data, dates)
        return self.cache[code]

    def row(self, code, d):
        return self._load(code)[0].get(d)

    def next_row(self, code, d):
        data, dates = self._load(code)
        i = bisect.bisect_right(dates, d)
        while i < len(dates):
            r = data[dates[i]]
            if str(r.get("tradestatus")) in ("1", "1.0"):
                return r
            i += 1
        return None


def _tradeable(r):
    return bool(r) and str(r.get("tradestatus")) in ("1", "1.0")


def t1_metrics(line, code, d):
    d0 = line.row(code, d)
    d1 = line.next_row(code, d)
    if not _tradeable(d0) or not _tradeable(d1):
        return None
    o0 = v10.fnum(d0.get("open"))
    o1, h1, c1, pc1 = (v10.fnum(d1.get("open")), v10.fnum(d1.get("high")),
                       v10.fnum(d1.get("close")), v10.fnum(d1.get("preclose")))
    if not o0 or not pc1 or not c1:
        return None
    return {"t1_open": (o1 - pc1) / pc1 * 100 if o1 else None,
            "t1_close": (c1 - pc1) / pc1 * 100,
            "t1_high": (h1 - pc1) / pc1 * 100 if h1 else None,
            "hold_open_t1close": (c1 - o0) / o0 * 100,
            "hold_open_t1high": (h1 - o0) / o0 * 100 if h1 else None}


def _stat(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    s = sorted(xs)
    return {"n": len(xs), "mean": round(statistics.mean(xs), 3), "median": round(statistics.median(xs), 3),
            "win_rate": round(sum(1 for e in xs if e > 0) / len(xs), 3),
            "p90": round(s[int(0.9 * (len(s) - 1))], 2),
            "p10": round(s[int(0.1 * (len(s) - 1))], 2)}


def daily_ic_t(rows, fld, key):
    xs, ys = [], []
    for r in rows:
        v = v10.field_value(r, fld)
        t = r.get(key)
        if v is None or t is None:
            continue
        xs.append(-v if fld in v10.RANK_FIELDS else v)
        ys.append(t)
    return v10.spearman(xs, ys) if len(xs) >= 8 else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    line = Line(root)
    topN = args.top_n
    days = v12.load_days_plus(root, daily)

    used_days = 0
    n_samples = 0
    by_action = {t: defaultdict(list) for t in TARGETS}
    universe = {t: [] for t in TARGETS}
    buy_detail = []
    for d in days:
        kept = []
        for r in d["rows"]:
            m = t1_metrics(line, r["code"], d["date"])
            if m is None:
                continue
            r.update(m)
            kept.append(r)
        d["rows1"] = kept
        if len(kept) < 20:
            continue
        used_days += 1
        n_samples += len(kept)
        for r in kept:
            a = r["action"] or "UNKNOWN"
            for t in TARGETS:
                by_action[t][a].append(r.get(t))
                universe[t].append(r.get(t))
            if r["action"] == "BUY":
                buy_detail.append({"date": d["date"], "code": r["code"], "excess_t0": round(r["excess"], 2),
                                   "t1_close": round(r["t1_close"], 2) if r.get("t1_close") is not None else None,
                                   "t1_high": round(r["t1_high"], 2) if r.get("t1_high") is not None else None,
                                   "hold_open_t1close": round(r["hold_open_t1close"], 2) if r.get("hold_open_t1close") is not None else None,
                                   "hold_open_t1high": round(r["hold_open_t1high"], 2) if r.get("hold_open_t1high") is not None else None})

    pick_perf = {t: {a: _stat(v) for a, v in sorted(by_action[t].items())} for t in TARGETS}
    universe_stat = {t: _stat(universe[t]) for t in TARGETS}

    def field_ic_for(key):
        out = []
        for fld in FLDS:
            di = [daily_ic_t(d.get("rows1", []), fld, key) for d in days if len(d.get("rows1", [])) >= 20]
            m, icir, nd = v10.mean_icir(di)
            if m is not None:
                out.append({"field": fld, "mean_ic": m, "icir": icir, "n_days": nd})
        out.sort(key=lambda x: abs(x["mean_ic"]), reverse=True)
        return out
    ic_t1close = field_ic_for("t1_close")
    ic_hold = field_ic_for("hold_open_t1close")

    cap_edge = []
    for d in days:
        rows = d.get("rows1", [])
        if len(rows) < topN:
            continue
        order = sorted(range(len(rows)), key=lambda i: (rows[i]["hold_open_t1close"]
                       if rows[i].get("hold_open_t1close") is not None else -1e9), reverse=True)
        winners = set(order[:topN])
        edge_top = set(sorted(range(len(rows)),
                       key=lambda i: (rows[i]["edge_old"] if rows[i]["edge_old"] is not None else -1.0),
                       reverse=True)[:topN])
        cap_edge.append(len(winners & edge_top) / float(min(topN, len(winners))))

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "top_n": topN, "n_days_with_t1": used_days, "n_samples": n_samples,
        "metric_defs": {
            "t1_open": "(D1.open-D1.preclose)/D1.preclose*100",
            "t1_close": "(D1.close-D1.preclose)/D1.preclose*100",
            "t1_high": "(D1.high-D1.preclose)/D1.preclose*100",
            "hold_open_t1close": "(D1.close-D.open)/D.open*100",
            "hold_open_t1high": "(D1.high-D.open)/D.open*100"},
        "universe_baseline": universe_stat,
        "pick_performance_by_action": pick_perf,
        "buy_detail": buy_detail,
        "field_ic_t1_close": ic_t1close,
        "field_ic_hold_open_t1close": ic_hold,
        "edge_capture_at_n_hold": round(statistics.mean(cap_edge), 3) if cap_edge else None,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_horizon_t1.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def perf_table(t):
        rows = ["", f"### {t}", "", "| action | n | mean | median | win_rate | p90 | p10 |", "|---|---|---|---|---|---|---|"]
        for a, s in pick_perf[t].items():
            if s.get("n"):
                rows.append(f"| {a} | {s['n']} | {s['mean']} | {s['median']} | {s['win_rate']} | {s['p90']} | {s['p10']} |")
        ub = universe_stat[t]
        if ub.get("n"):
            rows.append(f"| 全体baseline | {ub['n']} | {ub['mean']} | {ub['median']} | {ub['win_rate']} | {ub['p90']} | {ub['p10']} |")
        return rows
    L = ["# T+1 次日维度评估", "",
         f"- 生成: {report['generated_at']} ｜有 T+1 的交易日: {used_days} ｜样本: {n_samples} ｜Top-N: {topN}",
         f"- 现行 edge 对 hold_open_t1close 赢家捕获率: **{report['edge_capture_at_n_hold']}**", "",
         "## 1. 按 action 的 T+1 表现"]
    for t in ["hold_open_t1close", "hold_open_t1high", "t1_close", "t1_open", "t1_high"]:
        L += perf_table(t)
    L += ["", "## 2. 历次 BUY 的 T+1 明细", "",
          "| 日期 | 代码 | 当日excess | 次日收盘 | 次日最高 | 竞买→次收 | 竞买→次高 |",
          "|---|---|---|---|---|---|---|"]
    for b in buy_detail:
        L.append(f"| {b['date']} | {b['code']} | {b['excess_t0']} | {b['t1_close']} | {b['t1_high']} | {b['hold_open_t1close']} | {b['hold_open_t1high']} |")
    L += ["", "## 3. 字段对 hold_open_t1close 的 IC", "", "| 字段 | mean_ic | icir | n_days |", "|---|---|---|---|"]
    for r in ic_hold:
        L.append(f"| {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} |")
    L += ["", "## 4. 字段对 次日收盘涨幅(t1_close) 的 IC", "", "| 字段 | mean_ic | icir | n_days |", "|---|---|---|---|"]
    for r in ic_t1close:
        L.append(f"| {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} |")
    (audit / "premarket_horizon_t1.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"n_days_with_t1": used_days, "n_samples": n_samples,
                      "edge_capture_at_n_hold": report["edge_capture_at_n_hold"],
                      "pick_performance_by_action": {t: pick_perf[t] for t in ["hold_open_t1close", "t1_close"]},
                      "top_ic_hold": ic_hold[:6]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
