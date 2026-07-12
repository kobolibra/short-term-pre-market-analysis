#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_backtest.py  --  v4.2 回测脚本

用已有的 captures 数据，对指定日期范围逐日运行 v4.2 完整决策链路。

用法（在服务器上执行）:
  python3 duanxianxia_v4_2_backtest.py --start 2026-07-07 --end 2026-07-11

项目根目录自动使用服务器默认路径，也可手动指定:
  python3 duanxianxia_v4_2_backtest.py --start 2026-07-07 --end 2026-07-11 \\
      --project-root /path/to/project
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

# 服务器默认路径
SERVER_PROJECT_ROOT = "/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia"


def _trading_days(start: date, end: date) -> List[str]:
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur.isoformat())
        cur += timedelta(days=1)
    return days


def _main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=f"盘前竞价 v4.2 回测 — 用已有数据回溯分析"
    )
    ap.add_argument("--start", required=True, help="起始日期 YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="结束日期 YYYY-MM-DD")
    ap.add_argument("--project-root", default=SERVER_PROJECT_ROOT, help="项目根目录")
    ap.add_argument("--output", "-o", default="./v4_2_backtest.json", help="输出 JSON")
    ap.add_argument("--text", action="store_true", help="同时输出文本")
    ap.add_argument("--verbose", "-v", action="store_true", help="逐日详细输出")
    args = ap.parse_args(argv)

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    days = _trading_days(start_date, end_date)

    if not days:
        print(f"日期范围 {args.start}~{args.end} 内无交易日")
        return 1

    project_root = Path(args.project_root)
    captures_dir = project_root / "captures"
    if not captures_dir.exists():
        print(f"错误: captures 目录不存在: {captures_dir}")
        return 1

    print(f"v4.2 回测")
    print(f"日期: {args.start} ~ {args.end} ({len(days)} 个交易日)")
    print(f"数据目录: {captures_dir}")
    print(f"=" * 60)

    history = D6History()
    all_results: List[Dict[str, Any]] = []
    n_success = 0
    n_error = 0

    for i, day in enumerate(days):
        print(f"\n[{i+1}/{len(days)}] {day} ", end="", flush=True)

        try:
            result = run_v4_2_pipeline(
                date_t0=day,
                project_root=str(project_root),
                history=history,
            )

            if "error" in result:
                print(f"❌ {result['error']}")
                all_results.append({"date": day, "status": "error", "error": result["error"]})
                n_error += 1
                continue

            emo = result["emotion"]
            ep = result["execution_plan"]
            orders = ep.get("orders", [])
            all_results.append(result)
            n_success += 1

            order_codes = [f"{o['code']} {o.get('name','')}" for o in orders]
            print(f"情绪={emo['state']} | 仓位上限={emo['total_position_cap']*100:.0f}% | "
                  f"入选={len(orders)}只 {order_codes}")

            if args.verbose and orders:
                for o in orders:
                    print(f"    {o['code']} {o.get('name','')} | {o['pool']} "
                          f"| 仓位={o['position_pct']}% | {o['buy_strategy']}")
                if emo.get("t0_downgraded"):
                    print(f"    ⚠️ T0降级: {emo['t0_downgrade_reason']}")

            # 更新历史
            if emo.get("ztbx_925") is not None:
                history.add_day(
                    ztbx=emo["ztbx_925"],
                    jinji_mean=emo.get("jinji_mean"),
                    red_rate=emo.get("red_rate"),
                    kqxy=None,
                )

        except Exception as e:
            print(f"❌ 异常: {e}")
            import traceback
            traceback.print_exc()
            all_results.append({"date": day, "status": "error", "error": str(e)})
            n_error += 1

    # ====== 汇总 ======
    print(f"\n{'='*60}")
    print(f"汇总: 成功 {n_success} 天 | 失败 {n_error} 天")
    print(f"{'='*60}")

    # 逐日选股汇总表
    print(f"\n{'日期':<12} {'情绪':<8} {'仓位上限':>8} {'入选':>6} {'股票列表'}")
    print("-" * 80)
    for r in all_results:
        if r.get("status") == "error":
            print(f"{r['date']:<12} {'ERROR':<8} {'-':>8} {'-':>6} {r.get('error','')}")
            continue
        emo = r.get("emotion", {})
        ep = r.get("execution_plan", {})
        orders = ep.get("orders", [])
        codes = ", ".join(f"{o.get('code','')} {o.get('name','')}({o.get('pool','')})" for o in orders)
        if not codes:
            codes = "(无)"
        downgraded = " ⚠️降级" if emo.get("t0_downgraded") else ""
        print(f"{r['date']:<12} {emo.get('state','?'):<8} {emo.get('total_position_cap',0)*100:>7.0f}% {len(orders):>5}只 {codes}{downgraded}")

    # 输出 JSON
    summary = {
        "version": VERSION,
        "date_range": f"{args.start}~{args.end}",
        "n_success": n_success,
        "n_error": n_error,
        "results": all_results,
    }
    json_path = Path(args.output)
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\nJSON: {json_path}")

    if args.text:
        text_path = json_path.with_suffix(".txt")
        lines = []
        lines.append(f"盘前竞价 v4.2 回测汇总")
        lines.append(f"日期: {args.start} ~ {args.end}")
        lines.append(f"成功: {n_success} 天 | 失败: {n_error} 天")
        lines.append("=" * 60)
        for r in all_results:
            if r.get("status") == "error":
                lines.append(f"\n{r['date']} ❌ {r.get('error','')}")
                continue
            emo = r.get("emotion", {})
            ep = r.get("execution_plan", {})
            lines.append(f"\n--- {r['date']} ---")
            lines.append(f"情绪: {emo.get('state','?')} | 仓位上限: {emo.get('total_position_cap',0)*100:.0f}%")
            if emo.get("t0_downgraded"):
                lines.append(f"T0降级: {emo.get('t0_downgrade_reason','')}")
            for o in ep.get("orders", []):
                lines.append(f"  {o['code']} {o.get('name','')} | {o['pool']} | {o['position_pct']}% | {o['buy_strategy']}")
            if not ep.get("orders"):
                lines.append("  (无入选)")
        text_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"文本: {text_path}")

    return 0


if __name__ == "__main__":
    sys.exit(_main())