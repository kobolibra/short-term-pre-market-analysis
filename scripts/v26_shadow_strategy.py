#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v26_shadow_strategy.py — 双模型影子策略报告(只读, 不改生产)。

基于 v25 定论生成最新交易日的可执行影子组合:
  A) 当日超额策略: sparse_ic Top5 (竞价买入 -> 当日收盘)
  B) 激进当日策略: v10_amt Top3 (竞价买入 -> 当日收盘)
  C) 次日持仓策略: v10_amt Top30 (竞价买入 -> 次日收盘)
并列出重叠、冲突、risk/action 信息。

重要: 影子策略报告必须能在盘前/盘中生成, 因此不能依赖 same-day dailyline/excess。
本脚本直接读取最新 reports/<date>/premarket/*_analysis_v9.json 候选池, 只使用盘前可见字段。

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

SPARSE_W = {"deriv.amt_x_auc": 0.24, "auction_strength": 0.22, "liquidity": 0.18,
            "pressure_score": 0.13, "deriv.money_x_liq": 0.12, "money": 0.11}


def latest_analysis(root, want=None):
    rep = root / "reports"
    candidates = []
    for dd in sorted(rep.glob("20*-*-*")):
        if want and dd.name != want:
            continue
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if files:
            candidates.append((dd.name, files[-1]))
    if not candidates:
        return None, None
    return candidates[-1]


def extract_rows(analysis):
    cands = analysis.get("all_candidates") or []
    rows = []
    for rec in cands:
        if not isinstance(rec, dict) or not rec.get("code"):
            continue
        e = v10.extract(rec)
        full = rec.get("full") if isinstance(rec.get("full"), dict) else {}
        ad = full.get("auction_detail") or {}
        rd = full.get("risk_detail") or {}
        rf = full.get("risk_flag")
        if rf is None:
            rf = rec.get("risk_flag")
        e["action"] = str(rec.get("action_type") or full.get("action_type") or "").strip().upper()
        e["risk_flag"] = bool(rf)
        e["weak_breadth"] = "weak_breadth" in (rd.get("market_env_flags") or [])
        e["primary_signal"] = str(ad.get("qiangchou_primary_signal") or "").strip()
        e["setup"] = str(ad.get("auction_setup_type") or "").strip()
        e["alpha"] = str(rec.get("alpha_pattern") or full.get("alpha_pattern") or "").strip()
        rows.append(e)
    # 竞价金额分位与衍生项, 与 v10/v12 保持一致
    amtp = [(i, rows[i]["f"].get("auction_amount_wan")) for i in range(len(rows))
            if rows[i]["f"].get("auction_amount_wan") is not None]
    amap = v10.pctl(amtp)
    for i, r in enumerate(rows):
        r["amt"] = amap.get(i, 50.0)
        r["d"] = v10.derived(r["f"], r["amt"])
    return rows


def field_value(r, fld):
    if fld == "amt_pct":
        return r.get("amt")
    if fld.startswith("deriv."):
        return r.get("d", {}).get(fld)
    return r.get("f", {}).get(fld)


def build_scores(rows):
    for r in rows:
        r["_v10_amt"] = v10.score(r["f"], r["amt"], v10.V10AMT_W)
    xr = {}
    for fld in SPARSE_W:
        iv = [(i, field_value(rows[i], fld)) for i in range(len(rows)) if field_value(rows[i], fld) is not None]
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
            "amt_pct": round(r.get("amt", 50.0), 3), "auction_strength": f.get("auction_strength"),
            "liquidity": f.get("liquidity"), "money": f.get("money"),
            "pressure_score": f.get("pressure_score"), "weimai_strength": f.get("weimai_strength"),
            "orderbook": f.get("orderbook"), "primary_signal": r.get("primary_signal"),
            "setup": r.get("setup"), "alpha": r.get("alpha"), "sparse_parts": r.get("_sparse_parts")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--date", default=None)
    args = ap.parse_args()
    root = Path(args.project_root)
    date, path = latest_analysis(root, args.date)
    if not date or not path:
        raise RuntimeError(f"no analysis file found: {args.date}")
    analysis = json.loads(path.read_text(encoding="utf-8"))
    rows = extract_rows(analysis)
    if len(rows) < 5:
        raise RuntimeError(f"date {date} has too few candidates: {len(rows)}")
    build_scores(rows)

    sparse_top5 = pick(rows, "_sparse_ic", 5)
    v10_top3 = pick(rows, "_v10_amt", 3)
    v10_top30 = pick(rows, "_v10_amt", 30)
    overlap_same = sorted(set(r["code"] for r in sparse_top5) & set(r["code"] for r in v10_top3))
    overlap_hold = sorted(set(r["code"] for r in sparse_top5) & set(r["code"] for r in v10_top30))
    report = {"generated_at": datetime.now().isoformat(timespec="seconds"), "date": date,
              "analysis_file": str(path.relative_to(root)), "n_candidates": len(rows),
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
         f"- 分析文件: `{report['analysis_file']}`",
         "- 只读影子报告, 未改生产逻辑; 不依赖 same-day 收盘/dailyline。", "",
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
