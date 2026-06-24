#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v26_shadow_strategy.py — 双模型影子策略报告(只读, 不改生产)。

基于 v25 定论生成最新交易日的可执行影子组合:
  A) 当日超额策略: sparse_ic Top5 (竞价买入 -> 当日收盘)
  B) 激进当日策略: v10_amt Top3 (竞价买入 -> 当日收盘)
  C) 次日持仓策略: v10_amt Top30 (竞价买入 -> 次日收盘)
并列出重叠、冲突、risk/action 信息。

sparse_ic = 0.24*amt_x_auc + 0.22*auction_strength + 0.18*liquidity +
            0.13*pressure_score + 0.12*money_x_liq + 0.11*money
所有 sparse 特征使用当日日内截面分位秩。

输出: reports/_audit/premarket_shadow_strategy_v26.{json,md}
用法: python3 scripts/v26_shadow_strategy.py [--date YYYY-MM-DD]
"""
from __future__ import annotations
import argparse, json, sys, traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12

SPARSE_W = {"deriv.amt_x_auc": 0.24, "auction_strength": 0.22, "liquidity": 0.18,
            "pressure_score": 0.13, "deriv.money_x_liq": 0.12, "money": 0.11}


def latest_analysis_date(root, want=None):
    rep = root / "reports"
    dates = []
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        if pm.is_dir() and list(pm.glob("*_analysis_v9.json")):
            dates.append(dd.name)
    if want:
        return want if want in dates else None
    return dates[-1] if dates else None


def load_one_day(root, date):
    daily = v10.Daily(root)
    days = v12.load_days_plus(root, daily)
    for d in days:
        if d["date"] == date:
            return d
    return None


def build_scores(day):
    rows = day["rows"]
    # v10_amt raw score
    for r in rows:
        r["_v10_amt"] = v10.score(r["f"], r["amt"], v10.V10AMT_W)
    # sparse percentile score
    xr = {}
    for fld in SPARSE_W:
        iv = [(i, v10.field_value(rows[i], fld)) for i in range(len(rows)) if v10.field_value(rows[i], fld) is not None]
        xr[fld] = v10.pctl(iv) if iv else {}
    for i, r in enumerate(rows):
        r["_sparse_ic"] = sum(SPARSE_W[f] * xr[f].get(i, 50.0) for f in SPARSE_W)
        r["_sparse_parts"] = {f: round(xr[f].get(i, 50.0), 2) for f in SPARSE_W}


def pick(rows, key, n):
    return sorted(rows, key=lambda r: r.get(key, -1e9), reverse=True)[:n]


def slim(r):
    f = r.get("f", {})
    return {"code": r["code"], "action": r.get("action"), "risk_flag": bool(r.get("risk_flag")),
            "v10_amt": round(r.get("_v10_amt", 0), 3), "sparse_ic": round(r.get("_sparse_ic", 0), 3),
            "edge_old": r.get("edge_old"), "final": r.get("final"),
            "latest_change_pct": f.get("latest_change_pct"),
            "amt_pct": r.get("amt"), "auction_strength": f.get("auction_strength"),
            "liquidity": f.get("liquidity"), "money": f.get("money"),
            "pressure_score": f.get("pressure_score"), "sparse_parts": r.get("_sparse_parts")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    root = Path(args.project_root)
    date = latest_analysis_date(root, args.date)
    if not date:
        raise RuntimeError(f"no analysis date found: {args.date}")
    day = load_one_day(root, date)
    if not day:
        raise RuntimeError(f"date {date} has analysis but no trainable rows/excess; likely same-day dailyline missing")
    build_scores(day)
    rows = day["rows"]
    sparse_top5 = pick(rows, "_sparse_ic", 5)
    v10_top3 = pick(rows, "_v10_amt", 3)
    v10_top30 = pick(rows, "_v10_amt", 30)
    overlap_same = sorted(set(r["code"] for r in sparse_top5) & set(r["code"] for r in v10_top3))
    overlap_hold = sorted(set(r["code"] for r in sparse_top5) & set(r["code"] for r in v10_top30))
    report = {"generated_at": datetime.now().isoformat(timespec="seconds"), "date": date,
              "n_candidates": len(rows),
              "strategy_defs": {"same_day_top5": "sparse_ic Top5, buy auction open, sell same-day close",
                                "aggressive_same_day_top3": "v10_amt Top3, buy auction open, sell same-day close",
                                "t1_hold_top30": "v10_amt Top30, buy auction open, sell next trading day close"},
              "same_day_sparse_top5": [slim(r) for r in sparse_top5],
              "aggressive_v10_top3": [slim(r) for r in v10_top3],
              "t1_hold_v10_top30": [slim(r) for r in v10_top30],
              "overlap_sparse5_v10top3": overlap_same, "overlap_sparse5_v10top30": overlap_hold}
    audit = root / "reports" / "_audit"; audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_shadow_strategy_v26.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def md_table(title, arr):
        L = ["", f"## {title}", "", "| 排名 | 代码 | action | risk | sparse | v10_amt | edge_old | 开盘涨幅 | amt_pct | auc | liq | money | pressure |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for i, r in enumerate(arr, 1):
            L.append(f"| {i} | {r['code']} | {r['action']} | {'Y' if r['risk_flag'] else ''} | {r['sparse_ic']} | {r['v10_amt']} | {r['edge_old']} | {r['latest_change_pct']} | {r['amt_pct']} | {r['auction_strength']} | {r['liquidity']} | {r['money']} | {r['pressure_score']} |")
        return L

    L = ["# v26 双模型影子策略报告", "", f"- 生成: {report['generated_at']} ｜日期: **{date}** ｜候选数: {len(rows)}",
         "- 只读影子报告, 未改生产逻辑。", "",
         "## 策略定论", "",
         "- 当日 Top5: **sparse_ic**（v25 OOS Top5均值 1.491 / 胜率75% / p10 -2.02）",
         "- 激进 Top3: **v10_amt**（v25 OOS Top3最强）",
         "- 次日 Top30: **v10_amt**（v25 OOS 次日 Top30最强）",
         f"- sparse Top5 ∩ v10 Top3: {', '.join(overlap_same) if overlap_same else '无'}",
         f"- sparse Top5 ∩ v10 Top30: {', '.join(overlap_hold) if overlap_hold else '无'}"]
    L += md_table("A. 当日策略 sparse_ic Top5", report["same_day_sparse_top5"])
    L += md_table("B. 激进当日 v10_amt Top3", report["aggressive_v10_top3"])
    L += md_table("C. 次日持仓 v10_amt Top30", report["t1_hold_v10_top30"])
    L += ["", "> 执行解释: A 追求当日超额最大化; B 是更集中但波动更大的 Top3; C 是隔夜/次日扩散收益, 用 Top30 分散。",
          "> 生产更新仍需单独确认; 当前仅作为影子报告。"]
    (audit / "premarket_shadow_strategy_v26.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"date": date, "n_candidates": len(rows),
                      "same_day_sparse_top5": [r["code"] for r in report["same_day_sparse_top5"]],
                      "aggressive_v10_top3": [r["code"] for r in report["aggressive_v10_top3"]],
                      "overlap_sparse5_v10top30": overlap_hold}, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    try: sys.exit(main())
    except SystemExit: raise
    except Exception:
        traceback.print_exc(); sys.exit(1)
