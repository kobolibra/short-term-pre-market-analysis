#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_backtest_daily.py  --  v4.2 每日回测（零参数，自动检测过去5个交易日）

由 agent_daily_refresh.py 入队，agent_job_worker.py 执行。
结果写入 reports/_audit/v4_2_backtest/YYYY-MM-DD.json。
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_runner import run_v4_2_pipeline, VERSION
from duanxianxia_v4_2_d6_emotion import D6History

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_backtest"


def _past_trading_days(n: int = 5) -> List[str]:
    days = []
    cur = date.today() - timedelta(days=1)  # 从昨天开始
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur.isoformat())
        cur -= timedelta(days=1)
    return list(reversed(days))


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    out_path = OUTPUT_DIR / f"{today}.json"
    if out_path.exists():
        print(f"v4.2 backtest: overwriting existing {out_path} (re-run with fixes)")

    days = _past_trading_days(5)
    history = D6History()
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
                "jinji_weighted": emo.get("jinji_weighted"),
                "ztbx_925": emo.get("ztbx_925"),
                "t0_impulse": emo.get("t0_impulse"),
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
                    jinji_1_2=emo.get("jinji_1_2"),
                    jinji_2_3=emo.get("jinji_2_3"),
                )
        except Exception as e:
            summary["results"].append({"date": day, "status": "error", "error": str(e)})

    # 写 JSON
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"v4.2 backtest done: {len(days)} days → {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())