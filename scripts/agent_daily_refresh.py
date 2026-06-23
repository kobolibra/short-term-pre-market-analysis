#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""agent_daily_refresh.py — 每日自动把核心分析套件重新入队(只增不覆盖)。

让盘前选股分析“持续迭代”: 每天随新数据自动重跑
  v10_optimize / v12_reflection / v13_lowopen_reverse / v14_horizon,
结果由 runner 发布到 agent-results 分支。

幂等: 每天每脚本只入队一次(队列文件按日期命名; 已存在则跳过)。
worker 凭 <id>.result.json 是否存在决定是否执行, 故 id 含日期 → 每天跑一次。
新交易日的 v9 分析由现有盘前 cron 产生, 分析套件会自动把新增交易日纳入。
"""
from __future__ import annotations
import json
import sys
from datetime import datetime
from pathlib import Path

WS = Path(__file__).resolve().parent.parent
QUEUE_DIR = WS / "scripts" / "agent_jobs" / "queue"

SUITE = [
    ("v10_optimize.py", ["--no-regen", "--top-n", "30"]),
    ("v12_reflection.py", ["--top-n", "30"]),
    ("v13_lowopen_reverse.py", ["--low-open-max", "2.0", "--top-n", "30"]),
    ("v14_horizon.py", ["--top-n", "30"]),
]


def today_str():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def main():
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    today = today_str()
    created = []
    for script, args in SUITE:
        stem = script[:-3]
        job_id = f"daily_{today}_{stem}"
        qf = QUEUE_DIR / f"{job_id}.json"
        if qf.exists():
            continue
        qf.write_text(json.dumps({
            "id": job_id,
            "script": f"scripts/{script}",
            "args": args,
            "timeout": 2400,
            "created": datetime.now().isoformat(timespec="seconds"),
            "note": f"daily auto-refresh {today}",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        created.append(job_id)
    print(json.dumps({"today": today, "enqueued": created}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
