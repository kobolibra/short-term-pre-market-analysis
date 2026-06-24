#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v27_shadow_outcome.py — v26 双模型影子组合真实结果评估(只读)。

盘后/次日运行:
  - 复现 v26 的当日 sparse_ic Top5 / v10_amt Top3 / v10_amt Top30。
  - 若 dailyline 已有当日数据, 计算 same-day excess=(close-open)/preclose*100。
  - 若已有下一交易日数据, 计算 hold_open_t1close; 否则标记 pending。
  - 单独比较 risk_flag=True vs False 的真实表现。

输出: reports/_audit/premarket_shadow_outcome_v27.{json,md}
用法: python3 scripts/v27_shadow_outcome.py [--date YYYY-MM-DD]
"""
from __future__ import annotations
import argparse, json, statistics, sys, traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v14_horizon as v14
import v26_shadow_strategy as v26


def stat(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0, "status": "pending"}
    pos = [x for x in xs if x > 0]
    neg = [x for x in xs if x < 0]
    ap = statistics.mean(pos) if pos else 0.0
    an = statistics.mean(neg) if neg else 0.0
    return {"n": len(xs), "mean": round(statistics.mean(xs), 4),
            "median": round(statistics.median(xs), 4),
            "win_rate": round(len(pos)/len(xs), 4),
            "odds": round(ap/abs(an), 3) if neg and an else None,
            "sum": round(sum(xs), 4), "min": round(min(xs), 4), "max": round(max(xs), 4)}


def attach_labels(root, date, rows):
    daily = v10.Daily(root)
    line = v14.Line(root)
    for r in rows:
        code = r["code"]
        r["_excess_real"] = daily.excess(code, date)
        m = v14.t1_metrics(line, code, date)
        r["_hold_real"] = (m or {}).get("hold_open_t1close")


def pick(rows, key, n):
    return sorted(rows, key=lambda r: r.get(key, -1e9), reverse=True)[:n]


def slim(r):
    return {"code": r["code"], "action": r.get("action"), "risk_flag": bool(r.get("risk_flag")),
            "sparse_ic": round(r.get("_sparse_ic", 0), 3), "v10_amt": round(r.get("_v10_amt", 0), 3),
            "excess": None if r.get("_excess_real") is None else round(r["_excess_real"], 4),
            "hold_open_t1close": None if r.get("_hold_real") is None else round(r["_hold_real"], 4),
            "latest_change_pct": r.get("f", {}).get("latest_change_pct")}


def group_eval(arr):
    return {"excess": stat([r.get("_excess_real") for r in arr]),
            "hold_open_t1close": stat([r.get("_hold_real") for r in arr]),
            "items": [slim(r) for r in arr]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    root = Path(args.project_root)
    date, path = v26.latest_analysis(root, args.date)
    if not date or not path:
        raise RuntimeError(f"no analysis file found: {args.date}")
    analysis = json.loads(path.read_text(encoding="utf-8"))
    rows = v26.extract_rows(analysis)
    v26.build_scores(rows)
    attach_labels(root, date, rows)

    sparse5 = pick(rows, "_sparse_ic", 5)
    v10top3 = pick(rows, "_v10_amt", 3)
    v10top30 = pick(rows, "_v10_amt", 30)
    risk_true = [r for r in sparse5 if r.get("risk_flag")]
    risk_false = [r for r in sparse5 if not r.get("risk_flag")]

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "date": date, "analysis_file": str(path.relative_to(root)),
              "n_candidates": len(rows),
              "same_day_sparse_top5": group_eval(sparse5),
              "aggressive_v10_top3": group_eval(v10top3),
              "t1_hold_v10_top30": group_eval(v10top30),
              "sparse_top5_risk_true": group_eval(risk_true),
              "sparse_top5_risk_false": group_eval(risk_false)}

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_shadow_outcome_v27.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def perf_row(name, block):
        e = block["excess"]
        h = block["hold_open_t1close"]
        return f"| {name} | {e.get('n')} | {e.get('mean')} | {e.get('win_rate')} | {e.get('min')} | {e.get('max')} | {h.get('n')} | {h.get('mean')} | {h.get('status','ok')} |"

    def item_table(title, items):
        L = ["", f"## {title}", "", "| 代码 | action | risk | sparse | v10_amt | 开盘涨幅 | excess | hold_t1close |",
             "|---|---|---|---|---|---|---|---|"]
        for it in items:
            L.append(f"| {it['code']} | {it['action']} | {'Y' if it['risk_flag'] else ''} | {it['sparse_ic']} | {it['v10_amt']} | {it['latest_change_pct']} | {it['excess']} | {it['hold_open_t1close']} |")
        return L

    L = ["# v27 v26影子组合真实结果评估", "",
         f"- 生成: {report['generated_at']} ｜日期: **{date}** ｜候选数: {len(rows)}",
         f"- 分析文件: `{report['analysis_file']}`",
         "- same-day excess 若为空表示 dailyline 尚未落地; hold_t1close 通常需下一交易日后才有。", "",
         "## 汇总", "",
         "| 组合 | excess n | excess均值 | excess胜率 | excess最小 | excess最大 | hold n | hold均值 | hold状态 |",
         "|---|---|---|---|---|---|---|---|---|",
         perf_row("sparse_ic Top5 当日", report["same_day_sparse_top5"]),
         perf_row("v10_amt Top3 激进", report["aggressive_v10_top3"]),
         perf_row("v10_amt Top30 次日", report["t1_hold_v10_top30"]),
         perf_row("sparse Top5 risk=Y", report["sparse_top5_risk_true"]),
         perf_row("sparse Top5 risk=False", report["sparse_top5_risk_false"])]
    L += item_table("A. sparse_ic Top5 明细", report["same_day_sparse_top5"]["items"])
    L += item_table("B. v10_amt Top3 明细", report["aggressive_v10_top3"]["items"])
    L += ["", "> 用途: 检查 v26 影子组合当日是否兑现; 若当天 dailyline 未更新, 下一轮自动重跑会补上。"]
    (audit / "premarket_shadow_outcome_v27.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"date": date,
                      "sparse_top5_excess": report["same_day_sparse_top5"]["excess"],
                      "v10_top3_excess": report["aggressive_v10_top3"]["excess"],
                      "v10_top30_excess": report["t1_hold_v10_top30"]["excess"]}, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
