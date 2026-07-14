#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_batch.py  --  v4.2 批量回测/批量分析脚本

对指定的日期范围，逐日运行 v4.2 完整决策链路，输出汇总结果。

用法:
  # 回测上周5天
  python3 duanxianxia_v4_2_batch.py --start 2026-07-07 --end 2026-07-11 --project-root /path/to/project

  # 指定输出目录
  python3 duanxianxia_v4_2_batch.py --start 2026-07-07 --end 2026-07-11 \\
      --project-root /path/to/project --output-dir ./v4_2_results

  # 同时生成文本和 JSON
  python3 duanxianxia_v4_2_batch.py --start 2026-07-07 --end 2026-07-11 \\
      --project-root /path/to/project --text --json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_runner import run_v4_2_pipeline, VERSION
from duanxianxia_v4_2_d6_emotion import D6History
from duanxianxia_v4_2_risk_exec import format_execution_plan


def _trading_days(start: date, end: date) -> List[str]:
    """生成日期范围内的所有交易日（周一至周五，排除周末）"""
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:  # 0=Mon, 4=Fri
            days.append(cur.isoformat())
        cur += timedelta(days=1)
    return days


def _main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=f"盘前竞价短线选股系统 {VERSION} — 批量回测"
    )
    ap.add_argument("--start", required=True, help="起始日期 (YYYY-MM-DD)")
    ap.add_argument("--end", required=True, help="结束日期 (YYYY-MM-DD)")
    ap.add_argument("--project-root", required=True, help="项目根目录路径（含 captures/ 目录）")
    ap.add_argument("--output-dir", "-o", default="./v4_2_results",
                    help="输出目录 (默认: ./v4_2_results)")
    ap.add_argument("--text", action="store_true", help="输出可读文本格式")
    ap.add_argument("--json", action="store_true", help="输出 JSON 格式")
    ap.add_argument("--top-n", type=int, default=3, help="每池取 Top N (默认: 3)")
    args = ap.parse_args(argv)

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    days = _trading_days(start_date, end_date)

    if not days:
        print(f"日期范围 {args.start}~{args.end} 内无交易日")
        return 1

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    project_root = Path(args.project_root)
    if not project_root.exists():
        print(f"错误: project_root 不存在: {project_root}")
        return 1

    captures_dir = project_root / "captures"
    if not captures_dir.exists():
        print(f"错误: captures 目录不存在: {captures_dir}")
        return 1

    print(f"v4.2 批量回测")
    print(f"日期范围: {args.start} ~ {args.end} ({len(days)} 个交易日)")
    print(f"项目根目录: {project_root}")
    print(f"输出目录: {output_dir}")
    print(f"=" * 60)

    # 历史数据（用于 D6 滚动分位数，跨日累积）
    history = D6History()

    # 汇总
    all_results: List[Dict[str, Any]] = []
    summary = {
        "version": VERSION,
        "date_range": f"{args.start}~{args.end}",
        "n_days": len(days),
        "n_success": 0,
        "n_error": 0,
        "daily_results": [],  # type: List[Dict[str, Any]]
    }

    for i, day in enumerate(days):
        print(f"\n{'='*60}")
        print(f"  [{i+1}/{len(days)}] {day}")
        print(f"{'='*60}")

        try:
            result = run_v4_2_pipeline(
                date_t0=day,
                project_root=str(project_root),
                history=history,
                top_n_per_pool=args.top_n,
            )

            if "error" in result:
                print(f"  ❌ 加载失败: {result['error']}")
                summary["daily_results"].append({
                    "date": day, "status": "error", "error": result["error"],
                })
                summary["n_error"] += 1
                continue

            all_results.append(result)
            emo = result["emotion"]
            ep = result["execution_plan"]
            n_orders = len(ep.get("orders", []))

            print(f"  周期: {emo['phase_label']} | 水位: {emo['level']} | 方向: {emo['direction']}")
            print(f"  风险: {emo['risk_tier']} | 仓位上限: {emo['position_cap']*100:.0f}%")
            print(f"  接力健康度: {emo.get('relay_health')}% | 1进2: {emo.get('jinji_1_2')}% | 2进3: {emo.get('jinji_2_3')}%")
            print(f"  ZTBX@9:25: {emo.get('ztbx_925')}% | advance_share: {emo.get('advance_share')} | DT: {emo.get('dt_925')}")
            print(f"  ZTBX塌方={emo['ztbx_collapse']} LBBX塌方={emo['lbbx_collapse']} 广度冲击={emo['breadth_shock']}")

            if emo.get("t0_impulse") == "NEGATIVE":
                print(f"  ⚠️ T0负向冲击: {emo.get('transition_reason', [])}")

            # 池详情
            for pool_name, pool_data in result["pools"].items():
                top_codes = [rk["code"] for rk in pool_data.get("top_n", [])]
                print(f"  {pool_name}: {pool_data['n_total']}只候选 "
                      f"→ Top{len(top_codes)}: {top_codes}")

            # 订单
            if n_orders > 0:
                print(f"  ✅ 可下单: {n_orders} 只")
                for o in ep["orders"]:
                    print(f"     {o['code']} {o['name']} | {o['pool']} "
                          f"| 仓位={o['position_pct']}% | {o['buy_strategy']}")
            else:
                print(f"  ⚠️ 今日无可下单股票")

            # 更新历史
            if emo.get("ztbx_925") is not None:
                history.add_day(
                    ztbx=emo["ztbx_925"],
                    lbbx=emo.get("lbbx_925"),
                    advance_share=emo.get("advance_share"),
                    dt=emo.get("dt_925"),
                    relay_health=emo.get("relay_health"),
                )

            summary["daily_results"].append({
                "date": day,
                "status": "success",
                "phase": emo["phase_label"],
                "risk_tier": emo["risk_tier"],
                "position_cap": emo["position_cap"],
                "n_orders": n_orders,
                "orders": [
                    {"code": o["code"], "name": o["name"], "pool": o["pool"],
                     "position_pct": o["position_pct"]}
                    for o in ep.get("orders", [])
                ],
            })
            summary["n_success"] += 1

        except Exception as e:
            print(f"  ❌ 异常: {e}")
            import traceback
            traceback.print_exc()
            summary["daily_results"].append({
                "date": day, "status": "error", "error": str(e),
            })
            summary["n_error"] += 1

    # ========================================================================
    # 汇总输出
    # ========================================================================
    print(f"\n{'='*60}")
    print(f"  汇总")
    print(f"{'='*60}")
    print(f"  成功: {summary['n_success']} 天 | 失败: {summary['n_error']} 天")

    # 写 JSON
    if args.json or not args.text:
        json_path = output_dir / "v4_2_batch_summary.json"
        json_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8"
        )
        print(f"  汇总 JSON: {json_path}")

    # 写文本
    if args.text:
        text_path = output_dir / "v4_2_batch_summary.txt"
        lines = []
        lines.append(f"盘前竞价选股系统 {VERSION} — 批量回测汇总")
        lines.append(f"日期范围: {args.start} ~ {args.end} ({len(days)} 个交易日)")
        lines.append(f"成功: {summary['n_success']} 天 | 失败: {summary['n_error']} 天")
        lines.append("=" * 60)

        for dr in summary["daily_results"]:
            lines.append(f"\n--- {dr['date']} ---")
            if dr["status"] == "error":
                lines.append(f"  ❌ 错误: {dr.get('error', 'unknown')}")
                continue
            lines.append(f"  周期: {dr['phase']} | 风险: {dr['risk_tier']} | 仓位上限: {dr['position_cap']*100:.0f}%")
            lines.append(f"  可下单: {dr['n_orders']} 只")
            for o in dr.get("orders", []):
                lines.append(f"    {o['code']} {o['name']} | {o['pool']} | {o['position_pct']}%")

        text_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"  汇总文本: {text_path}")

    return 0


if __name__ == "__main__":
    sys.exit(_main())