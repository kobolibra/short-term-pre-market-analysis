#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v18_concentration.py — 组合集中度扫描(只读)。

v14-v17 收敛: 排序(v10_amt)与择时(持到次日收盘)均到顶。本脚本换新维度:
集中度 — 既然排序到顶, 在头部下重注(收窄 Top-N) 能否把组合日均收益拉高?

对 N ∈ {5,10,15,20,30}, 按 v10_amt / stored edge 选 Top-N, 评估两个口径:
  - 当日超额 excess = (close-open)/preclose*100
  - 次日持仓 hold_open_t1close = (D1.close-D.open)/D.open*100
以 等权日组合 口径汇总(每日先取 Top-N 均值, 再跨日统计) — 贴近真实可交易回报。

输出: reports/_audit/premarket_concentration_v18.{json,md}
用法: python3 scripts/v18_concentration.py
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

NS = [5, 10, 15, 20, 30]


def _trad(r):
    return bool(r) and str(r.get("tradestatus")) in ("1", "1.0")


def ho_close(line, code, d):
    d0 = line.row(code, d)
    d1 = line.next_row(code, d)
    if not _trad(d0) or not _trad(d1):
        return None
    o0 = v10.fnum(d0.get("open"))
    c1 = v10.fnum(d1.get("close"))
    if not o0 or not c1:
        return None
    return (c1 - o0) / o0 * 100


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


def daily_portfolio(elig, rank_key, N, metric):
    series = []
    for d in elig:
        vals = []
        for i in select_by(d["rows"], rank_key, N):
            r = d["rows"][i]
            v = r["excess"] if metric == "excess" else r.get("_hc")
            if v is not None:
                vals.append(v)
        if vals:
            series.append(statistics.mean(vals))
    st = _xstat(series)
    st["n_days"] = len(series)
    return st


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    line = v14.Line(root)
    days = v12.load_days_plus(root, daily)
    for d in days:
        for r in d["rows"]:
            try:
                r["_amt_score"] = v10.score(r["f"], r["amt"], v10.V10AMT_W)
            except Exception:
                r["_amt_score"] = None
            r["_hc"] = ho_close(line, r["code"], d["date"])
    elig = [d for d in days if len(d["rows"]) >= max(NS)]

    rankers = {"v10_amt": lambda r: r["_amt_score"], "edge": lambda r: r["edge_old"]}
    out = {}
    for rn, rk in rankers.items():
        out[rn] = {}
        for metric in ("excess", "hold"):
            out[rn][metric] = {str(N): daily_portfolio(elig, rk, N, metric) for N in NS}

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "n_days": len(days), "n_eligible_days": len(elig), "Ns": NS,
              "metric_defs": {"excess": "当日(close-open)/preclose*100",
                              "hold": "次日(D1.close-D.open)/D.open*100",
                              "口径": "等权日组合: 每日先取Top-N均值再跨日统计; win_rate=上涨天占比"},
              "results": out}
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_concentration_v18.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def tbl(rn, metric, label):
        rows = ["", f"### {rn} 选股 · {label} (等权日组合)", "",
                "| Top-N | 有效天 | 日均收益 | 中位 | 上涨天占比 | 赔率 | 最好天(p90) | 最坏天(p10) |",
                "|---|---|---|---|---|---|---|---|"]
        for N in NS:
            s = out[rn][metric][str(N)]
            rows.append(f"| {N} | {s.get('n_days')} | {s.get('mean')} | {s.get('median')} | {s.get('win_rate')} | {s.get('odds')} | {s.get('p90')} | {s.get('p10')} |")
        return rows

    L = ["# v18 组合集中度扫描 (Top-N 收窄)", "",
         f"- 生成: {report['generated_at']} ｜交易日: {len(days)} ｜参与日: {len(elig)}",
         "- 口径: 等权日组合(每日取Top-N均值再跨日统计); 上涨天占比=组合日收益>0的天数占比"]
    L += ["", "## 按 v10_amt 选股"]
    L += tbl("v10_amt", "hold", "次日持仓收益")
    L += tbl("v10_amt", "excess", "当日超额")
    L += ["", "## 按 stored edge 选股"]
    L += tbl("edge", "hold", "次日持仓收益")
    L += tbl("edge", "excess", "当日超额")
    L += ["", "> 门槛: 若收窄 Top-N 能明显抬升日均收益且不把上涨天占比压太低, 则集中度是真增量;",
          "> 但需权衡样本数变少带来的不稳定(日数/最坏天)。"]
    (audit / "premarket_concentration_v18.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"n_eligible_days": len(elig), "results": out}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
