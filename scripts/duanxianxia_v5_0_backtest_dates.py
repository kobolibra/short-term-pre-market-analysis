#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v5_0_backtest_dates.py  --  v5.0 指定日期回测

直接从 captures/ 加载数据, 运行 v5.0 管线, 输出结果到 _audit/v5_0_premarket/。

用法:
  python3 duanxianxia_v5_0_backtest_dates.py --dates 2026-07-16,2026-07-17
  python3 duanxianxia_v5_0_backtest_dates.py --dates 2026-07-16
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v7_1_data_loader import (
    load_premarket_bundle,
    DataLoaderError,
)
from duanxianxia_v5_0_runner import run_v5_0_pipeline, VERSION
from duanxianxia_v5_0_d6_profile import format_profile, ProfileHistory
from duanxianxia_v5_0_risk_exec import format_execution_plan

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v5_0_premarket"


def _build_history_from_captures(project_root: Path, max_days: int = 60) -> ProfileHistory:
    """直接从 captures/ 构建 ProfileHistory。"""
    from duanxianxia_v5_0_premarket_daily import _build_history_from_raw_captures
    return _build_history_from_raw_captures(project_root, max_days=max_days)


def run_single_date(trade_date: str, history: ProfileHistory) -> Dict[str, Any]:
    """对单个日期运行 v5.0 管线。"""
    print(f"\n{'='*60}")
    print(f"  运行 v5.0 回测: {trade_date}")
    print(f"  历史数据: close={history.close_days()}天, pre={history.pre_days()}天")
    print(f"{'='*60}")

    try:
        bundle = load_premarket_bundle(trade_date, str(PROJECT_ROOT))
    except DataLoaderError as e:
        return {
            "version": VERSION, "date": trade_date,
            "status": "error", "error": f"数据加载失败: {e}",
        }

    result = run_v5_0_pipeline(
        date_t0=trade_date,
        project_root=str(PROJECT_ROOT),
        bundle=bundle,
        history=history,
    )

    if "error" in result:
        return {
            "version": VERSION, "date": trade_date,
            "status": "error", "error": result["error"],
        }

    prof = result.get("profile", {})
    ep = result.get("execution_plan", {})
    orders = ep.get("orders", [])

    return {
        "version": VERSION,
        "date": trade_date,
        "status": "ok",
        "bottleneck": prof.get("bottleneck"),
        "bottleneck_name": prof.get("bottleneck_name"),
        "heat": prof.get("heat"),
        "divergence": prof.get("divergence"),
        "tilt": prof.get("tilt"),
        "direction_summary": prof.get("direction_summary"),
        "position": prof.get("position"),
        "buy_mode": prof.get("buy_mode"),
        "extreme_veto": prof.get("extreme_veto"),
        "veto_reason": prof.get("veto_reason"),
        "profit_collapse": prof.get("profit_collapse", False),
        "breadth_panic": prof.get("breadth_panic", False),
        "indicators": prof.get("indicators", {}),
        "pool_multipliers": prof.get("pool_multipliers", {}),
        "yizi_enabled": prof.get("yizi_enabled", True),
        "huanshou_enabled": prof.get("huanshou_enabled", True),
        "fenqi_enabled": prof.get("fenqi_enabled", True),
        "feiban_enabled": prof.get("feiban_enabled", True),
        "n_orders": len(orders),
        "orders": [
            {
                "code": o["code"], "name": o.get("name", ""),
                "pool": o["pool"], "position_pct": o["position_pct"],
                "buy_strategy": o.get("buy_strategy", ""),
                "height_mult": o.get("height_mult", 1.0),
                "risk_mult": o.get("risk_mult", 1.0),
                "profile_position": o.get("profile_position", 0.5),
                "pool_mult": o.get("pool_mult", 1.0),
            }
            for o in orders
        ],
        "allocated_position": ep.get("allocated_position", 0),
        "reserve_position": ep.get("reserve_position", 0),
        "pools": {
            pn: {
                "n_total": pd.get("n_total", 0),
                "n_filtered": pd.get("n_filtered", 0),
                "pool_mult": pd.get("pool_mult", 1.0),
            }
            for pn, pd in result.get("pools", {}).items()
        },
        "warnings": result.get("warnings", []),
        "diagnostics": result.get("diagnostics", {}),
    }


def main() -> int:
    p = argparse.ArgumentParser(description="v5.0 指定日期回测")
    p.add_argument("--dates", required=True,
                   help="日期列表, 逗号分隔, 如 2026-07-16,2026-07-17")
    p.add_argument("--text", action="store_true", help="同时输出可读文本")
    args = p.parse_args()

    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    if not dates:
        print("错误: 未提供有效日期")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 构建历史 (一次性, 所有日期共用)
    print("构建历史数据...")
    history = _build_history_from_captures(PROJECT_ROOT)
    print(f"  历史数据: close={history.close_days()}天, pre={history.pre_days()}天")

    all_results = []
    for trade_date in dates:
        result = run_single_date(trade_date, history)

        # 保存单个结果
        out_path = OUTPUT_DIR / f"{trade_date}.json"
        out_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"  结果已保存: {out_path}")

        # 可读文本
        if args.text:
            from duanxianxia_v5_0_d6_profile import calculate_profile, MarketProfile
            from duanxianxia_v5_0_risk_exec import ExecutionPlan
            # 重建 profile 对象来格式化
            print(f"\n{'─'*60}")
            print(f"  {trade_date} 快速摘要")
            print(f"  瓶颈: {result.get('bottleneck_name')} ({result.get('bottleneck', 0):.3f})")
            print(f"  温度: {result.get('heat', 0):.3f} | 分歧: {result.get('divergence', 0):.3f}")
            print(f"  仓位: {result.get('position', 0):.3f} | 买点: {result.get('buy_mode', '')}")
            if result.get("extreme_veto"):
                print(f"  🚨 极端否决: {result.get('veto_reason', '')}")
            orders = result.get("orders", [])
            if orders:
                print(f"  可下单: {len(orders)}只")
                for o in orders:
                    print(f"    {o['code']} {o.get('name','')} | {o['pool']} | 仓位={o['position_pct']}% | {o.get('buy_strategy','')}")
            else:
                print(f"  可下单: 0只")
            print(f"  池状态: {', '.join(f'{k}={v}' for k,v in result.get('pool_multipliers', {}).items())}")
            print(f"{'─'*60}")

        all_results.append(result)

    # 汇总
    print(f"\n{'='*60}")
    print(f"  汇总")
    print(f"{'='*60}")
    for r in all_results:
        status = "✅" if r.get("status") == "ok" else "❌"
        if r.get("status") == "ok":
            print(f"  {status} {r['date']}: 瓶颈={r['bottleneck_name']}({r['bottleneck']:.3f}) "
                  f"温度={r['heat']:.3f} 分歧={r['divergence']:.3f} "
                  f"仓位={r['position']:.3f} 订单={r['n_orders']}只 "
                  f"否决={r['extreme_veto']}")
        else:
            print(f"  {status} {r['date']}: {r.get('error', 'unknown')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())