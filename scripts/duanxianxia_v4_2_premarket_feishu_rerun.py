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
    # 报告在 reports/YYYY-MM-DD/premarket/HHMMSS.json 子目录里
    premarket_dir = day_dir / "premarket"
    if premarket_dir.is_dir():
        candidates = sorted(
            premarket_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return candidates[0]
    # fallback: 平铺文件
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

    # 从报告文件中读取 webhook URL (9:25 cron 存入的)
    webhook_url = ""
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            report_data = json.load(f)
        webhook_url = report_data.get("_webhook_url", "").strip()
    except Exception:
        pass

    if not webhook_url:
        webhook_url = os.getenv("DUANXIANXIA_WEBHOOK_URL", "").strip()
    if not webhook_url:
        print("[feishu_rerun] ERROR: no webhook URL found in report or env")
        return 1

    print(f"[feishu_rerun] webhook URL found, pushing...")

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