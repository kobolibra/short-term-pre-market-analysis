#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v11_failure_mode_backtest.py — 验证“高开 + 末秒抢筹 + 弱宽度环境”是否系统性负超额。

动机: 2026-06-23 唯一 BUY(000823 超声电子)竞价高开 +3.87% 后跌停。该票特征:
  - auction_pct 高开
  - qiangchou_primary_signal == last_second（末秒抢筹）
  - market_env_flags 含 weak_breadth / risk_flag=True
本脚本在全历史 v9 候选上按子集统计 excess_ret 分布，判断这是系统性失败模式
还是单日方差。若系统性为负，则支持在 cold/weak 环境下将该组合 BUY->WATCH 降级。

excess_ret = (close - open)/preclose*100 = 收盘涨幅 - 竞价涨幅
输出: reports/_audit/premarket_failure_mode_report.{json,md}
用法: python3 scripts/v11_failure_mode_backtest.py [--high-open 3.0]
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
import v10_optimize as v10  # 复用 Daily / fnum / DEFAULT_PROJECT_ROOT


def feat(rec: dict) -> dict:
    full = rec.get("full") if isinstance(rec.get("full"), dict) else {}
    ad = full.get("auction_detail") or {}
    rd = full.get("risk_detail") or rec.get("risk_detail") or {}
    flags = rd.get("market_env_flags") or []
    auction_pct = v10.fnum(rec.get("auction_pct"))
    if auction_pct is None:
        auction_pct = v10.fnum(ad.get("latest_change_pct"))
    prim = str(ad.get("qiangchou_primary_signal") or "").strip()
    last_sec_rank = v10.fnum(ad.get("qiangchou_last_second_rank"))
    early_rank = v10.fnum(ad.get("qiangchou_920_925_rank"))
    rf = full.get("risk_flag")
    if rf is None:
        rf = rec.get("risk_flag")
    rf = bool(rf)
    weak_breadth = "weak_breadth" in flags
    return {
        "code": str(rec.get("code") or "").strip(),
        "auction_pct": auction_pct,
        "primary_signal": prim,
        "last_second": (prim == "last_second") or (last_sec_rank is not None and early_rank is None),
        "risk_flag": rf,
        "weak_breadth": weak_breadth,
        "weak_env": bool(weak_breadth or rf),
        "edge": v10.fnum(rec.get("edge_score")),
        "action": str(rec.get("action_type") or "").strip(),
    }


def load(root: Path, daily, high_open_thr: float):
    rows = []
    days = set()
    rep = root / "reports"
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if not files:
            continue
        try:
            analysis = json.loads(files[-1].read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list):
            continue
        for rec in cands:
            if not isinstance(rec, dict) or not rec.get("code"):
                continue
            ex = daily.excess(rec["code"], dd.name)
            if ex is None:
                continue
            ft = feat(rec)
            ft["date"] = dd.name
            ft["excess"] = ex
            ft["high_open"] = (ft["auction_pct"] is not None and ft["auction_pct"] >= high_open_thr)
            rows.append(ft)
            days.add(dd.name)
    return rows, sorted(days)


def stat(rows: list) -> dict:
    ex = [r["excess"] for r in rows]
    if not ex:
        return {"n": 0}
    return {
        "n": len(ex),
        "mean_excess": round(statistics.mean(ex), 3),
        "median_excess": round(statistics.median(ex), 3),
        "win_rate": round(sum(1 for e in ex if e > 0) / len(ex), 3),
        "limitdown_rate": round(sum(1 for e in ex if e <= -9.5) / len(ex), 3),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--high-open", type=float, default=3.0)
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    rows, days = load(root, daily, args.high_open)

    subsets = {
        "ALL": rows,
        "high_open": [r for r in rows if r["high_open"]],
        "last_second": [r for r in rows if r["last_second"]],
        "weak_env": [r for r in rows if r["weak_env"]],
        "high_open+last_second": [r for r in rows if r["high_open"] and r["last_second"]],
        "high_open+weak_env": [r for r in rows if r["high_open"] and r["weak_env"]],
        "last_second+weak_env": [r for r in rows if r["last_second"] and r["weak_env"]],
        "TRIPLE": [r for r in rows if r["high_open"] and r["last_second"] and r["weak_env"]],
        "BUY_picks": [r for r in rows if r["action"] == "BUY"],
        "BUY_TRIPLE": [r for r in rows if r["action"] == "BUY" and r["high_open"] and r["last_second"] and r["weak_env"]],
    }
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "high_open_threshold": args.high_open,
        "n_days": len(days),
        "days": days,
        "n_samples": len(rows),
        "subsets": {k: stat(v) for k, v in subsets.items()},
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_failure_mode_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    L = [
        "# 失败模式回测: 高开 + 末秒抢筹 + 弱环境", "",
        f"- 生成: {report['generated_at']}",
        f"- 有效交易日: {len(days)} ｜样本: {len(rows)} ｜高开阈值: auction_pct>={args.high_open}", "",
        "| 子集 | n | mean_excess | median | win_rate | 跌停率 |",
        "|---|---|---|---|---|---|",
    ]
    for k, s in report["subsets"].items():
        if s.get("n"):
            L.append(f"| {k} | {s['n']} | {s['mean_excess']} | {s['median_excess']} | {s['win_rate']} | {s['limitdown_rate']} |")
        else:
            L.append(f"| {k} | 0 | - | - | - | - |")
    (audit / "premarket_failure_mode_report.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
