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
from duanxianxia_v4_2_d6_emotion import (
    D6History, _extract_qxlive_metric,
    _extract_ztpool_pbbx, _smoothed_rate,
    RELAY_WEIGHT_1_2, RELAY_WEIGHT_2_3,
)
from duanxianxia_v7_1_data_loader import (
    load_capture_at_time, _extract_rows,
    DS_HOME_QXLIVE_TOP, DS_HOME_ZTPOOL,
    QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
)

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_backtest"
CAPTURES_DIR = PROJECT_ROOT / "captures"


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


def _build_history_from_raw_captures(project_root: Path, max_days: int = 60) -> D6History:
    """直接从 captures/ 原始数据构建 D6History, 不依赖过去分析结果。"""
    history = D6History()
    if not CAPTURES_DIR.is_dir():
        return history
    date_dirs = sorted(
        [d for d in CAPTURES_DIR.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )[-max_days:]
    for day_dir in date_dirs:
        date_str = day_dir.name
        if len(date_str) != 10 or date_str[4] != "-":
            continue
        qxlive = load_capture_at_time(
            project_root, date_str, DS_HOME_QXLIVE_TOP,
            max_hhmmss=QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
            pick="earliest_before", raise_if_missing=False,
        )
        qxlive_rows = _extract_rows(qxlive)
        ztbx = _extract_qxlive_metric(qxlive_rows, "ZTBX")
        lbbx = _extract_qxlive_metric(qxlive_rows, "LBBX")
        sz = _extract_qxlive_metric(qxlive_rows, "SZ")
        xd = _extract_qxlive_metric(qxlive_rows, "XD")
        dt = _extract_qxlive_metric(qxlive_rows, "DT")
        advance_share = None
        if sz is not None and xd is not None and (sz + xd) > 0:
            advance_share = round(sz / (sz + xd), 4)
        # KQXY 盘后 (取最新, 不限时间)
        kqxy = None
        kq_live = load_capture_at_time(
            project_root, date_str, DS_HOME_QXLIVE_TOP,
            pick="latest", raise_if_missing=False,
        )
        if kq_live:
            kq_rows = _extract_rows(kq_live)
            kq_val = _extract_qxlive_metric(kq_rows, "KQXY")
            if kq_val is not None and kq_val > 0:
                kqxy = kq_val
        ztpool = load_capture_at_time(
            project_root, date_str, DS_HOME_ZTPOOL,
            pick="latest", raise_if_missing=False,
        )
        ztpool_rows = _extract_rows(ztpool)
        pbbx = _extract_ztpool_pbbx(ztpool_rows)
        j12 = _smoothed_rate(
            pbbx.get("PBBX_1_2", {}).get("promoted"),
            pbbx.get("PBBX_1_2", {}).get("eligible"),
        )
        j23 = _smoothed_rate(
            pbbx.get("PBBX_2_3", {}).get("promoted"),
            pbbx.get("PBBX_2_3", {}).get("eligible"),
        )
        relay_health = None
        if j12 is not None and j23 is not None:
            relay_health = round(RELAY_WEIGHT_1_2 * j12 + RELAY_WEIGHT_2_3 * j23, 2)
        elif j12 is not None:
            relay_health = round(j12, 2)
        elif j23 is not None:
            relay_health = round(j23, 2)
        if ztbx is not None:
            history.add_day(
                ztbx=ztbx, lbbx=lbbx,
                advance_share=advance_share,
                dt=int(dt) if dt is not None else None,
                relay_health=relay_health,
                kqxy=kqxy,
            )
    return history


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
    if history.history_days < 20:
        raw_history = _build_history_from_raw_captures(PROJECT_ROOT)
        for v in raw_history.ztbx_values:
            history.add_day(ztbx=v)
        for v in raw_history.lbbx_values:
            history.add_day(lbbx=v)
        for v in raw_history.advance_share_values:
            history.add_day(advance_share=v)
        for v in raw_history.dt_values:
            history.add_day(dt=v)
        for v in raw_history.relay_health_values:
            history.add_day(relay_health=v)
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