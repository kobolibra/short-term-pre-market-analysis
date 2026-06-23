#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v19_topk_robust.py — 验证 v18 发现(同日 alpha 集中于头部前5名)的稳健性(只读)。

1) 细扫 Top-K ∈ {3,5,7,8,10,12,15}, 按 v10_amt 选股, 看当日超额的衰减曲线(等权日组合)。
2) 逐日列出 Top-5 的实际票与超额, 确认不是一两天撑起来的。
3) 给出 Top-5 日组合超额的累计(简单叠加)与最坏/最好单日。

输出: reports/_audit/premarket_topk_v19.{json,md}
用法: python3 scripts/v19_topk_robust.py
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

KS = [3, 5, 7, 8, 10, 12, 15]


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
    ap = statistics.mean(pos) if pos else 0.0
    an = statistics.mean(neg) if neg else 0.0
    return {"n": len(xs), "mean": round(statistics.mean(xs), 3), "median": round(statistics.median(xs), 3),
            "win_rate": round(len(pos) / len(xs), 3),
            "odds": round(ap / abs(an), 2) if (neg and an != 0) else None,
            "p90": round(s[int(0.9 * (len(s) - 1))], 2), "p10": round(s[int(0.1 * (len(s) - 1))], 2),
            "sum": round(sum(xs), 2)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    for d in days:
        for r in d["rows"]:
            try:
                r["_amt_score"] = v10.score(r["f"], r["amt"], v10.V10AMT_W)
            except Exception:
                r["_amt_score"] = None
    elig = [d for d in days if len(d["rows"]) >= max(KS)]
    amt = lambda r: r["_amt_score"]

    decay = {}
    for K in KS:
        series = []
        for d in elig:
            vals = [d["rows"][i]["excess"] for i in select_by(d["rows"], amt, K)]
            vals = [v for v in vals if v is not None]
            if vals:
                series.append(statistics.mean(vals))
        decay[str(K)] = _xstat(series)

    per_day = []
    for d in elig:
        picks = select_by(d["rows"], amt, 5)
        items = [{"code": d["rows"][i]["code"], "excess": round(d["rows"][i]["excess"], 2)} for i in picks]
        mean5 = round(statistics.mean([it["excess"] for it in items]), 3) if items else None
        per_day.append({"date": d["date"], "picks": items, "port_excess": mean5})

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "n_days": len(days), "n_eligible_days": len(elig), "Ks": KS,
              "same_day_excess_decay": decay, "per_day_top5": per_day}
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_topk_v19.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# v19 Top-K 同日 alpha 衰减与稳健性", "",
         f"- 生成: {report['generated_at']} ｜交易日: {len(days)} ｜参与日: {len(elig)}", "",
         "## 1. 同日超额 随 Top-K 衰减 (按 v10_amt, 等权日组合)", "",
         "| Top-K | 有效天 | 日均超额 | 中位 | 上涨天占比 | 赔率 | 累计超额 | 最坏天 |",
         "|---|---|---|---|---|---|---|---|"]
    for K in KS:
        s = decay[str(K)]
        L.append(f"| {K} | {s.get('n')} | {s.get('mean')} | {s.get('median')} | {s.get('win_rate')} | {s.get('odds')} | {s.get('sum')} | {s.get('p10')} |")
    L += ["", "## 2. 逐日 Top-5 实际票与超额 (验证不是一两天撑起)", "",
          "| 日期 | Top-5 代码(超额%) | 组合超额 |", "|---|---|---|"]
    for p in per_day:
        codes = ", ".join(f"{it['code']}({it['excess']})" for it in p["picks"])
        L.append(f"| {p['date']} | {codes} | {p['port_excess']} |")
    L += ["", "> 若 Top-5 日均超额明显>宽 Top-N 且上涨天多数, 则集中度是真增量; 结合逐日表判断是否稳健。"]
    (audit / "premarket_topk_v19.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"n_eligible_days": len(elig), "same_day_excess_decay": decay}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
