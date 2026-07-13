#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_premarket_daily.py  --  v4.2 盘前分析重跑（零参数，当天）

用已下载的 9:25 捕捉数据重跑 v4.2 盘前选股管线。
由 agent_daily_refresh.py 入队，agent_job_worker.py 执行。
结果写入 reports/_audit/v4_2_premarket/YYYY-MM-DD.json。
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_runner import run_v4_2_pipeline, VERSION
from duanxianxia_v4_2_d6_emotion import D6History

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_premarket"


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    out_path = OUTPUT_DIR / f"{today}.json"

    print(f"v4.2 premarket analysis: running for {today}...")

    result = run_v4_2_pipeline(
        date_t0=today,
        project_root=str(PROJECT_ROOT),
    )

    if "error" in result:
        summary: Dict[str, Any] = {
            "version": VERSION,
            "date": today,
            "status": "error",
            "error": result["error"],
        }
    else:
        emo = result.get("emotion", {})
        ep = result.get("execution_plan", {})
        orders = ep.get("orders", [])
        summary = {
            "version": VERSION,
            "date": today,
            "status": "ok",
            "emotion": emo.get("state"),
            "position_cap": emo.get("total_position_cap", 1.0),
            "buy_mode": emo.get("buy_mode"),
            "jinji_mean": emo.get("jinji_mean"),
            "ztbx_925": emo.get("ztbx_925"),
            "kqxy_925": emo.get("kqxy_925"),
            "red_rate": emo.get("red_rate"),
            "crisis_count": emo.get("crisis_count", 0),
            "t0_downgraded": emo.get("t0_downgraded", False),
            "t0_downgrade_reason": emo.get("t0_downgrade_reason", ""),
            "pool_enabled": emo.get("pool_enabled", {}),
            "pool_mult": emo.get("pool_mult", {}),
            "n_orders": len(orders),
            "orders": [
                {
                    "code": o["code"],
                    "name": o.get("name", ""),
                    "pool": o["pool"],
                    "position_pct": o["position_pct"],
                    "buy_strategy": o.get("buy_strategy", ""),
                    "height_mult": o.get("height_mult", 1.0),
                    "risk_mult": o.get("risk_mult", 1.0),
                }
                for o in orders
            ],
            "allocated_position": ep.get("allocated_position", 0),
            "reserve_position": ep.get("reserve_position", 0),
            "pools": {
                pn: {
                    "n_total": pd.get("n_total", 0),
                    "n_filtered": pd.get("n_filtered", 0),
                }
                for pn, pd in result.get("pools", {}).items()
            },
            "warnings": result.get("warnings", []),
            "diagnostics": result.get("diagnostics", {}),
        }

    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"v4.2 premarket done: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())