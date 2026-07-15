#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_backtest_daily.py  --  v4.2 每日回测

自动检测过去5个交易日，用滚动历史做每日情绪周期评估。
由 agent_daily_refresh.py 入队，agent_job_worker.py 执行。
结果写入 reports/_audit/v4_2_backtest/YYYY-MM-DD.json。

用法: python3 duanxianxia_v4_2_backtest_daily.py [--today 2026-07-14] [--days 5]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_runner import run_v4_2_pipeline, VERSION
from duanxianxia_v4_2_d6_emotion import D6History

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_backtest"


def _shanghai_today() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    except Exception:
        return date.today().isoformat()


def _past_trading_days(n: int = 5, end_date: str = None) -> List[str]:
    days = []
    if end_date:
        from datetime import datetime
        cur = datetime.fromisoformat(end_date).date()
    else:
        cur = date.today()
    cur -= timedelta(days=1)  # 从昨天开始
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur.isoformat())
        cur -= timedelta(days=1)
    return list(reversed(days))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--today", default=None, help="今天日期, 默认上海时间今天")
    p.add_argument("--days", type=int, default=5, help="回测天数")
    args = p.parse_args()
    today = args.today or _shanghai_today()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{today}.json"
    if out_path.exists():
        print(f"v4.2 backtest: overwriting existing {out_path} (re-run with fixes)")

    days = _past_trading_days(args.days, end_date=args.today)
    # 从过去回测结果加载历史, 用于滚动分位
    history = D6History()
    if OUTPUT_DIR.is_dir():
        for f in sorted(OUTPUT_DIR.glob("*.json")):
            try:
                d = json.loads(f.read_text(encoding="utf-8"))
                for r in d.get("results", []):
                    if r.get("status") == "ok" and r.get("ztbx_925") is not None:
                        history.add_day(
                            ztbx=r["ztbx_925"],
                            lbbx=r.get("lbbx_925"),
                            advance_share=r.get("advance_share"),
                            dt=r.get("dt_925"),
                            relay_health=r.get("relay_health"),
                        )
            except Exception:
                continue
    print(f"v4.2 backtest: loaded {history.history_days} history days from past results")
    summary: Dict[str, Any] = {"version": VERSION, "days": days, "results": []}

    for day in days:
        try:
            result = run_v4_2_pipeline(
                date_t0=day,
                project_root=str(PROJECT_ROOT),
                history=history,
                premarket_auction_cutoff="100000",  # 回测放宽截断时间，避免因数据采集时间差异导致数据缺失
            )
            if "error" in result:
                summary["results"].append({"date": day, "status": "error", "error": result["error"]})
                continue

            emo = result.get("emotion", {})
            ep = result.get("execution_plan", {})
            orders = ep.get("orders", [])
            summary["results"].append({
                "date": day,
                "status": "ok",
                "phase": emo.get("phase_label"),
                "risk_tier": emo.get("risk_tier"),
                "position_cap": emo.get("position_cap", 1.0),
                "jinji_weighted": emo.get("relay_health"),
                "ztbx_925": emo.get("ztbx_925"),
                "t0_impulse": emo.get("hard_veto"),
                "n_orders": len(orders),
                "orders": [
                    {"code": o["code"], "name": o.get("name", ""), "pool": o["pool"],
                     "position_pct": o["position_pct"]}
                    for o in orders
                ],
            })

            if emo.get("ztbx_925") is not None:
                history.add_day(
                    ztbx=emo["ztbx_925"],
                    lbbx=emo.get("lbbx_925"),
                    advance_share=emo.get("advance_share"),
                    dt=emo.get("dt_925"),
                    relay_health=emo.get("relay_health"),
                )
        except Exception as e:
            summary["results"].append({"date": day, "status": "error", "error": str(e)})

    # 写 JSON
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"v4.2 backtest done: {len(days)} days → {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())