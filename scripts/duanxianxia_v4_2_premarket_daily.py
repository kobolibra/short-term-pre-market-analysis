#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_premarket_daily.py  --  v4.2 盘前分析重跑

从 9:25 cron 已生成的 premarket report 中读取数据，重跑 v4.2 管线。
由 agent_daily_refresh.py 入队，agent_job_worker.py 执行。
结果写入 reports/_audit/v4_2_premarket/YYYY-MM-DD.json。

用法: python3 duanxianxia_v4_2_premarket_daily.py --date 2026-07-14
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_runner import (
    run_v4_2_pipeline, VERSION,
    _build_bundle_from_report,
)
from duanxianxia_v4_2_d6_emotion import D6History

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_premarket"
BACKTEST_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_backtest"
REPORTS_DIR = PROJECT_ROOT / "reports"


def _shanghai_today() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    except Exception:
        return date.today().isoformat()


def _find_latest_premarket_report(today: str) -> Optional[Path]:
    """找今天 9:25 cron 生成的最新 premarket 报告 (与 feishu_rerun 同逻辑)"""
    day_dir = REPORTS_DIR / today
    if not day_dir.is_dir():
        return None
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


def _build_history_from_past_results(source_dir: Path, max_days: int = 60) -> D6History:
    """从过去分析结果构建 D6History, 用于滚动分位计算。"""
    history = D6History()
    if not source_dir.is_dir():
        return history
    records: List[Dict[str, Any]] = []
    for f in sorted(source_dir.glob("*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            if "results" in d:
                for r in d["results"]:
                    if r.get("status") == "ok":
                        records.append(r)
            elif d.get("status") == "ok":
                records.append(d)
        except Exception:
            continue
    records.sort(key=lambda r: r.get("date", ""))
    for r in records[-max_days:]:
        if r.get("ztbx_925") is not None:
            history.add_day(
                ztbx=r["ztbx_925"],
                lbbx=r.get("lbbx_925"),
                advance_share=r.get("advance_share"),
                dt=r.get("dt_925"),
                relay_health=r.get("relay_health"),
            )
    return history


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="分析日期, 默认上海时间今天")
    args = p.parse_args()
    today = args.date or _shanghai_today()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{today}.json"

    print(f"v4.2 premarket analysis: running for {today}...")

    # 从已有 premarket report 构建 bundle (不重新下载, 飞书推送同款逻辑)
    report_path = _find_latest_premarket_report(today)
    if report_path is None:
        summary: Dict[str, Any] = {
            "version": VERSION,
            "date": today,
            "status": "error",
            "error": f"找不到 {today} 的 premarket 报告, 9:25 cron 可能未运行",
        }
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"v4.2 premarket ERROR: no report found for {today}")
        return 1

    print(f"v4.2 premarket: using report {report_path}")

    try:
        with open(report_path, "r", encoding="utf-8") as f:
            report_data = json.load(f)
    except Exception as e:
        summary = {
            "version": VERSION, "date": today, "status": "error",
            "error": f"读取报告失败: {e}",
        }
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return 1

    # 从 report 构建 bundle (T0 数据从 report items 读取, T-1 从 captures 加载)
    bundle = _build_bundle_from_report(report_data, PROJECT_ROOT, today)

    # 从过去分析结果构建 D6History (滚动分位需要历史数据)
    history = _build_history_from_past_results(OUTPUT_DIR)
    # 如果 premarket 目录历史不够, 补充 backtest 数据
    if history.history_days < 20:
        bt_history = _build_history_from_past_results(BACKTEST_DIR)
        # 合并: 按日期序取, 不重复
        all_vals = {}
        for src in [bt_history, history]:
            for arr, key in [(src.ztbx_values, "ztbx"), (src.lbbx_values, "lbbx"),
                              (src.advance_share_values, "adv"), (src.dt_values, "dt"),
                              (src.relay_health_values, "relay")]:
                for i, v in enumerate(arr):
                    all_vals.setdefault((key, i), v)
        # 重建
        history = D6History()
        ztbx = sorted([v for (k, _), v in all_vals.items() if k == "ztbx"])
        lbbx = sorted([v for (k, _), v in all_vals.items() if k == "lbbx"])
        adv = sorted([v for (k, _), v in all_vals.items() if k == "adv"])
        dt = sorted([v for (k, _), v in all_vals.items() if k == "dt"])
        relay = sorted([v for (k, _), v in all_vals.items() if k == "relay"])
        for i in range(max(len(ztbx), len(lbbx), len(adv), len(dt), len(relay))):
            history.add_day(
                ztbx=ztbx[i] if i < len(ztbx) else None,
                lbbx=lbbx[i] if i < len(lbbx) else None,
                advance_share=adv[i] if i < len(adv) else None,
                dt=dt[i] if i < len(dt) else None,
                relay_health=relay[i] if i < len(relay) else None,
            )
    print(f"v4.2 premarket: history days = {history.history_days}")

    result = run_v4_2_pipeline(
        date_t0=today,
        project_root=str(PROJECT_ROOT),
        bundle=bundle,
        history=history,
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
            "phase": emo.get("phase"),
            "phase_label": emo.get("phase_label"),
            "level": emo.get("level"),
            "level_score": emo.get("level_score"),
            "direction": emo.get("direction"),
            "risk_tier": emo.get("risk_tier"),
            "position_cap": emo.get("position_cap", 1.0),
            "buy_mode": emo.get("buy_mode"),
            "relay_health": emo.get("relay_health"),
            "jinji_1_2": emo.get("jinji_1_2"),
            "jinji_2_3": emo.get("jinji_2_3"),
            "ztbx_925": emo.get("ztbx_925"),
            "lbbx_925": emo.get("lbbx_925"),
            "advance_share": emo.get("advance_share"),
            "dt_925": emo.get("dt_925"),
            "hard_veto": emo.get("hard_veto"),
            "profit_collapse": emo.get("profit_collapse", False),
            "breadth_panic": emo.get("breadth_panic", False),
            "profit_level": emo.get("profit_level"),
            "breadth_level": emo.get("breadth_level"),
            "relay_level": emo.get("relay_level"),
            "profit_delta": emo.get("profit_delta"),
            "breadth_delta": emo.get("breadth_delta"),
            "relay_delta": emo.get("relay_delta"),
            "height_preference": emo.get("height_preference"),
            "fenqi_priority": emo.get("fenqi_priority"),
            "pool_enabled": emo.get("pool_enabled", {}),
            "pool_mult": emo.get("pool_mult", {}),
            "phase_confidence": emo.get("phase_confidence"),
            "data_quality": emo.get("data_quality"),
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