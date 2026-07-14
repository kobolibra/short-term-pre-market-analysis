#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""用最新代码重跑今天盘前分析并推送到飞书。

数据已经抓取过了（9:25 cron），这个脚本只重跑分析+推送，不重复抓取。
由 agent_jobs 队列触发。
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

SCRIPTS_DIR = Path(__file__).resolve().parent
WS = SCRIPTS_DIR.parent
PROJECT_ROOT = WS / "projects" / "duanxianxia"
REPORTS_DIR = PROJECT_ROOT / "reports"

sys.path.insert(0, str(SCRIPTS_DIR))


def _shanghai_today() -> str:
    if ZoneInfo:
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


def _find_latest_premarket_report(today: str) -> Optional[Path]:
    """找今天 9:25 cron 生成的最新 premarket 报告"""
    day_dir = REPORTS_DIR / today
    if not day_dir.is_dir():
        return None
    # 找 premarket_*.json 文件，按修改时间倒序
    candidates = sorted(
        day_dir.glob("premarket_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def main() -> int:
    today = _shanghai_today()
    report_path = _find_latest_premarket_report(today)

    if report_path is None:
        print(f"[feishu_rerun] ERROR: no premarket report found for {today}")
        return 1

    print(f"[feishu_rerun] using report: {report_path}")

    webhook_url = os.getenv("DUANXIANXIA_WEBHOOK_URL", "").strip()
    if not webhook_url:
        print("[feishu_rerun] ERROR: DUANXIANXIA_WEBHOOK_URL not set")
        return 1

    # 导入 batch 并 monkey-patch
    import duanxianxia_batch
    from duanxianxia_v4_2_runner import build_premarket_analysis_v4_2
    duanxianxia_batch.build_premarket_analysis = build_premarket_analysis_v4_2

    # 用 analysis_only 模式: 读已有报告, 重跑分析, 推飞书
    sys.argv = [
        "duanxianxia_batch.py",
        "premarket",
        "--report-path", str(report_path),
        "--webhook-url", webhook_url,
    ]
    return duanxianxia_batch.main()


if __name__ == "__main__":
    sys.exit(main())