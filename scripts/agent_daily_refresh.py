#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""agent_daily_refresh.py — 每日自动把核心分析套件重新入队(只增不覆盖)。

让盘前选股分析“持续迭代”: 每天随新数据自动重跑核心回测/反思/重构套件,
结果由 runner 发布到 agent-results 分支。

幂等: 大多数脚本每天只入队一次(队列文件按日期命名; 已存在则跳过)。
worker 凭 <id>.result.json 是否存在决定是否执行, 故 id 含日期 → 每天跑一次。

例外: v27_shadow_outcome 是盘后标签评估。盘后 dailyline 可能较晚落地, 所以 v27
按小时入队重试, 直到后续某次拿到 same-day / T+1 标签。脚本本身 pending-safe。
"""
from __future__ import annotations
import json
import sys
from datetime import datetime
from pathlib import Path

WS = Path(__file__).resolve().parent.parent
QUEUE_DIR = WS / "scripts" / "agent_jobs" / "queue"

# 核心持续回测套件:
# v10-v15: 原有排序/反思/失败模式/低开/周期/cohort
# v16-v17: 持仓出场(目标位/止损)证伪与监控
# v18-v19: Top-K 集中度与逐日稳健性
# v20-v22: 环境/特征/torch 排序重构地基
# v23: 受限头部重排器
# v24: 日级空仓门控
# v25: sparse_ic 可部署公式全面验证
# v26: 双模型影子策略报告(最新候选组合输出)
SUITE_DAILY = [
    ("v10_optimize.py", ["--no-regen", "--top-n", "30"]),
    ("v12_reflection.py", ["--top-n", "30"]),
    ("v13_lowopen_reverse.py", ["--low-open-max", "2.0", "--top-n", "30"]),
    ("v14_horizon.py", ["--top-n", "30"]),
    ("v15_cohort_selector.py", ["--top-n", "30", "--min-train", "5", "--low-open-max", "2.0"]),
    ("v16_strategy.py", ["--top-n", "30", "--min-train", "5"]),
    ("v17_exit.py", ["--top-n", "30"]),
    ("v18_concentration.py", []),
    ("v19_topk_robust.py", []),
    ("v20_env_probe.py", []),
    ("v21_feature_export.py", []),
    ("v22_torch_ranker.py", ["--min-train", "5", "--epochs", "120"]),
    ("v23_restricted_rerank.py", ["--min-train", "5", "--epochs", "80"]),
    ("v24_day_gate.py", ["--min-train", "6"]),
    ("v25_sparse_validation.py", ["--min-train", "5"]),
    ("v26_shadow_strategy.py", []),
    ("duanxianxia_v4_2_backtest_daily.py", []),
    ("duanxianxia_v4_2_premarket_daily.py", []),
]

# pending-safe label evaluator: retry hourly because same-day dailyline / T+1 labels land later
SUITE_HOURLY = [
    ("v27_shadow_outcome.py", []),
]


def now_shanghai():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai"))
    except Exception:
        return datetime.now()


def main():
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    now = now_shanghai()
    today = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    created = []

    for script, args in SUITE_DAILY:
        stem = script[:-3]
        job_id = f"daily_{today}_{stem}"
        qf = QUEUE_DIR / f"{job_id}.json"
        if qf.exists():
            continue
        # premarket_daily 依赖 9:25 cron 生成的盘前报告, 9:30 前不入队
        if script == "duanxianxia_v4_2_premarket_daily.py" and now.hour < 9:
            continue
        # 对于每日脚本需要传入日期参数(解决时区问题)
        all_args = [*args]
        if script in ("duanxianxia_v4_2_premarket_daily.py", "duanxianxia_v4_2_backtest_daily.py"):
            all_args.extend(["--date" if "premarket" in script else "--today", today])
        qf.write_text(json.dumps({
            "id": job_id,
            "script": f"scripts/{script}",
            "args": all_args,
            "timeout": 2400,
            "created": now.isoformat(timespec="seconds"),
            "note": f"daily auto-refresh {today}",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        created.append(job_id)

    # 每日自动入队飞书推送重跑：用最新分析代码重新生成飞书消息
    # 必须在 daily premarket 跑完之后跑，所以单独入队但延迟处理(高优先级)
    # 同样依赖 9:25 盘前报告, 9:30 前不入队
    if now.hour >= 9:
        script = "duanxianxia_v4_2_premarket_feishu_rerun.py"
        job_id = f"daily_{today}_feishu_analysis_push"
        qf = QUEUE_DIR / f"{job_id}.json"
        if not qf.exists():
            qf.write_text(json.dumps({
                "id": job_id,
                "script": f"scripts/{script}",
                "args": [],
                "timeout": 2400,
                "created": now.isoformat(timespec="seconds"),
                "note": f"daily auto-refresh {today} - 用最新代码重跑盘前分析+飞书推送(含D6情绪周期+选股)",
            }, ensure_ascii=False, indent=2), encoding="utf-8")
            created.append(job_id)

    for script, args in SUITE_HOURLY:
        stem = script[:-3]
        job_id = f"hourly_{today}_{hour}_{stem}"
        qf = QUEUE_DIR / f"{job_id}.json"
        if qf.exists():
            continue
        qf.write_text(json.dumps({
            "id": job_id,
            "script": f"scripts/{script}",
            "args": args,
            "timeout": 1200,
            "created": now.isoformat(timespec="seconds"),
            "note": f"hourly retry {today} {hour}:00 for pending-safe shadow outcome labels",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        created.append(job_id)

    print(json.dumps({"today": today, "hour": hour, "enqueued": created}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
