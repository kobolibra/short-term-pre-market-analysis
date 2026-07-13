#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_runner.py  --  v4.2 主编排器

完整决策链路 (7.0):
  [9:25 数据就绪]
  → Layer 0: 数据加载 (复用 v7.1 data_loader + feature_builder)
  → Layer 1: D6 情绪周期 (duanxianxia_v4_2_d6_emotion)
  → Layer 2: D7 结构路由 (duanxianxia_v4_2_d7_router)
  → Layer 3: 池内排名   (duanxianxia_v4_2_pool_ranker)
  → Layer 4: 风控执行   (duanxianxia_v4_2_risk_exec)
  → 输出: 执行计划

用法:
  python3 duanxianxia_v4_2_runner.py --date 2026-07-12 --project-root /path/to/project
  python3 duanxianxia_v4_2_runner.py --date 2026-07-12 --project-root /path/to/project --output result.json

设计文档: dimension-design-v4/dimension-design-v4.html §7
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 确保脚本目录在 path 中
sys.path.insert(0, str(Path(__file__).parent))

# 复用现有基础设施
from duanxianxia_v7_1_data_loader import (
    load_premarket_bundle,
    PremarketDataBundle,
    DataLoaderError,
)
from duanxianxia_feature_builder import (
    build_from_datasets,
    AUCTION_DATASETS,
    FENGDAN_DATASET,
)
from duanxianxia_canonical import raw_to_canonical

# v4.2 核心模块
from duanxianxia_v4_2_d6_emotion import (
    determine_emotion_state,
    D6EmotionResult,
    D6History,
    EmotionState,
    BuyMode,
    _extract_qxlive_metric,
)
from duanxianxia_v4_2_d7_router import (
    route_all_stocks,
    build_review_plate_map,
    PoolType,
    RoutedStock,
)
from duanxianxia_v4_2_pool_ranker import (
    rank_all_pools,
    build_rank_maps,
    PoolRankResult,
    RankedStock,
)
from duanxianxia_v4_2_risk_exec import (
    build_execution_plan,
    format_execution_plan,
    execution_plan_to_dict,
    ExecutionPlan,
)


VERSION = "v4.2.0"


# ============================================================================
# 数据加载层
# ============================================================================

def _load_rank_data(
    bundle: PremarketDataBundle,
    premarket_auction_cutoff: str = "092900",
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    从 premarket bundle 中加载热度榜和飙升榜排名数据。

    注意: 热度榜和飙升榜数据来自 rank.rocket 和 rank.hot_stock_day 数据集。
    这些数据需要在 data_loader 中额外加载，或通过 feature table 合并。
    """
    hot_rank_map: Dict[str, int] = {}
    rocket_rank_map: Dict[str, int] = {}

    # 尝试从 captures 直接加载 rank 数据
    # rank.rocket 和 rank.hot_stock_day 不在 PremarketDataBundle 中
    # 这里通过读取 captures 目录来获取
    project_root = Path(bundle.project_root)
    date_t0 = bundle.date_t0

    for rank_ds, rank_map in [
        ("rank.rocket", rocket_rank_map),
        ("rank.hot_stock_day", hot_rank_map),
    ]:
        try:
            from duanxianxia_v7_1_data_loader import load_capture_at_time
            capture = load_capture_at_time(
                project_root, date_t0, rank_ds,
                max_hhmmss=premarket_auction_cutoff,
                pick="earliest_before",
                raise_if_missing=False,
            )
            if capture:
                rows = capture.get("rows", []) if isinstance(capture, dict) else []
                for row in (rows or []):
                    if isinstance(row, dict):
                        code = str(row.get("code", "")).strip().zfill(6)
                        rank_val = row.get("hot_rank") or row.get("rank")
                        if code and rank_val is not None:
                            try:
                                rank_map[code] = int(float(rank_val))
                            except (ValueError, TypeError):
                                pass
        except Exception:
            pass

    return hot_rank_map, rocket_rank_map


def _load_review_daily_data(
    bundle: PremarketDataBundle,
) -> List[Dict[str, Any]]:
    """
    加载 review_daily 数据（T-1 盘后 PBBX 晋级率）。
    优先从 ztpool 提取，因为 ztpool 包含 ladder_group + promo_rate 分层。
    """
    # ztpool 已经在 bundle 中
    if bundle.ztpool_t1:
        return bundle.ztpool_t1

    # 尝试加载 review.daily 数据
    try:
        from duanxianxia_v7_1_data_loader import load_capture_at_time
        project_root = Path(bundle.project_root)
        capture = load_capture_at_time(
            project_root, bundle.date_t1, "review.daily.top_metrics",
            pick="latest", raise_if_missing=False,
        )
        if capture:
            return capture.get("rows", []) if isinstance(capture, dict) else []
    except Exception:
        pass

    return []


# ============================================================================
# 主编排函数
# ============================================================================

def run_v4_2_pipeline(
    date_t0: str,
    project_root: str,
    history: Optional[D6History] = None,
    static_thresholds: Optional[Dict[str, float]] = None,
    top_n_per_pool: int = 3,
    premarket_auction_cutoff: str = "092900",
) -> Dict[str, Any]:
    """
    执行 v4.2 完整决策链路。

    Args:
        date_t0: T0 日期 (YYYY-MM-DD)
        project_root: 项目根目录路径
        history: D6 历史数据 (用于滚动分位数)
        static_thresholds: D6 静态阈值 (历史数据不足时回退)
        top_n_per_pool: 每池取 Top N
        premarket_auction_cutoff: 竞价数据截断时间 (默认 "092900"; 回测时可放宽至 "100000")

    Returns:
        {
            "version": "v4.2.0",
            "date": "2026-07-12",
            "emotion": {...},
            "pools": {...},
            "execution_plan": {...},
            "warnings": [...],
            "diagnostics": {...},
        }
    """
    warnings: List[str] = []
    diagnostics: Dict[str, Any] = {}

    # ========================================================================
    # Layer 0: 数据加载
    # ========================================================================
    try:
        bundle = load_premarket_bundle(date_t0, project_root, premarket_auction_cutoff=premarket_auction_cutoff)
    except DataLoaderError as e:
        return {
            "version": VERSION,
            "date": date_t0,
            "error": f"数据加载失败: {e}",
            "warnings": [],
        }

    warnings.extend(bundle.warnings)

    # 构建 T0 特征表
    datasets: Dict[str, Any] = {}
    for dsid in AUCTION_DATASETS:
        rows = getattr(bundle, {
            "auction.jjyd.vratio": "auction_vratio",
            "auction.jjyd.qiangchou": "auction_qiangchou",
            "auction.jjyd.net_amount": "auction_netamount",
            "auction.jjyd.weimai": "auction_weimai",
        }[dsid], [])
        datasets[dsid] = [{"code": r.get("code", ""), "raw": r} for r in rows]

    # fengdan
    datasets[FENGDAN_DATASET] = bundle.auction_fengdan

    feature_result = build_from_datasets(datasets, date=date_t0, cutoff="09:29")
    features = feature_result["features"]
    diagnostics["feature_table"] = {
        "n_features": feature_result["n_features"],
        "n_fengdan_merged": feature_result["n_fengdan_merged"],
        "coverage": feature_result["coverage"],
    }

    if not features:
        warnings.append("T0 特征表为空")

    # 加载 review_plate 数据
    fupan_canon = []
    for row in bundle.fupan_t1:
        try:
            c = raw_to_canonical("review.fupan.plate", row)
            fupan_canon.append(c)
        except Exception:
            pass
    review_plate_map = build_review_plate_map(fupan_canon)
    diagnostics["review_plate"] = {"n_rows": len(fupan_canon)}

    # 加载 review_daily 数据 (PBBX 晋级率)
    ztpool_t1 = _load_review_daily_data(bundle)
    diagnostics["ztpool_t1"] = {"n_rows": len(ztpool_t1)}

    # 加载 rank 数据
    hot_rank_map, rocket_rank_map = _load_rank_data(bundle, premarket_auction_cutoff=premarket_auction_cutoff)
    diagnostics["rank_data"] = {
        "n_hot_rank": len(hot_rank_map),
        "n_rocket_rank": len(rocket_rank_map),
    }

    # ========================================================================
    # Layer 1: D6 情绪周期
    # ========================================================================
    emotion_result = determine_emotion_state(
        ztpool_t1=ztpool_t1,
        qxlive_top_t0=bundle.qxlive_top_t0_rows,
        qxlive_top_t1=bundle.qxlive_top_t1_rows,
        features=features,
        history=history,
        static_thresholds=static_thresholds,
    )
    warnings.extend(emotion_result.warnings)

    # ========================================================================
    # Layer 2: D7 结构路由
    # ========================================================================
    pools = route_all_stocks(features, review_plate_map)
    pool_counts = {k.value: len(v) for k, v in pools.items()}
    diagnostics["routing"] = {"pool_counts": pool_counts}

    # ========================================================================
    # Layer 3: 池内排名
    # ========================================================================
    pool_results = rank_all_pools(pools, hot_rank_map, rocket_rank_map)
    pool_diag = {}
    for pt, pr in pool_results.items():
        pool_diag[pr.pool_label] = pr.diagnostics
    diagnostics["ranking"] = pool_diag

    # ========================================================================
    # Layer 4: 风控与执行
    # ========================================================================
    execution_plan = build_execution_plan(pool_results, emotion_result, date=date_t0)
    warnings.extend(execution_plan.warnings)

    # ========================================================================
    # 组装输出
    # ========================================================================
    return {
        "version": VERSION,
        "date": date_t0,
        "emotion": {
            "state": emotion_result.state_label,
            "total_position_cap": emotion_result.total_position_cap,
            "buy_mode": emotion_result.buy_mode.value,
            "jinji_mean": emotion_result.jinji_mean,
            "ztbx_925": emotion_result.ztbx_925,
            "red_rate": emotion_result.red_rate,
            "crisis_count": emotion_result.crisis_count,
            "crisis_detail": {
                "crisis_1_ztbx": emotion_result.crisis_1,
                "crisis_2_jinji": emotion_result.crisis_2,
                "crisis_3_red_rate": emotion_result.crisis_3,
            },
            "t0_downgraded": emotion_result.t0_downgraded,
            "t0_downgrade_reason": emotion_result.t0_downgrade_reason,
            "ztbx_collapse": emotion_result.ztbx_collapse,
            "lbbx_collapse": emotion_result.lbbx_collapse,
            "kqxy_spike": emotion_result.kqxy_spike,
            "pool_enabled": {
                "一字封": emotion_result.pool_yizi_enabled,
                "换手封": emotion_result.pool_huanshou_enabled,
                "分歧封": emotion_result.pool_fenqi_enabled,
                "非板": emotion_result.pool_feiban_enabled,
            },
            "pool_mult": {
                "一字封": emotion_result.pool_yizi_mult,
                "换手封": emotion_result.pool_huanshou_mult,
                "分歧封": emotion_result.pool_fenqi_mult,
                "非板": emotion_result.pool_feiban_mult,
            },
        },
        "pools": {
            pr.pool_label: {
                "n_total": len(pr.candidates),
                "n_filtered": len(pr.filtered_out),
                "top_n": [
                    {
                        "code": rk.code,
                        "name": rk.name,
                        "rank": rk.rank,
                        "score_primary": rk.score_primary,
                        "score_secondary": rk.score_secondary,
                        "bonus_applied": rk.bonus_applied,
                        "routed": {
                            "board_height": rk.routed.board_height if rk.routed else 0,
                            "open_num": rk.routed.open_num if rk.routed else 0,
                            "height_multiplier": rk.routed.height_multiplier if rk.routed else 1.0,
                            "risk_tags": [t.value for t in (rk.routed.risk_tags if rk.routed else [])],
                        } if rk.routed else {},
                    }
                    for rk in pr.top_n
                ],
            }
            for pt, pr in pool_results.items()
        },
        "execution_plan": execution_plan_to_dict(execution_plan),
        "warnings": warnings,
        "diagnostics": diagnostics,
    }


# ============================================================================
# batch.py 适配器 (供 cron 管线调用)
# ============================================================================

def build_premarket_analysis_v4_2(
    report: Dict[str, Any],
    project_root: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """batch.py 适配器: report → v4.2 决策。

    签名与 build_premarket_analysis_v9 / build_premarket_analysis_v7_3 一致。
    被 duanxianxia_premarket_v7_runner.py monkey-patch 到 batch.py 的
    build_premarket_analysis 函数上。

    数据流:
      cron → duanxianxia_cron_runner.sh premarket
      → duanxianxia_premarket_v7_runner.py (ACTIVE_ENGINE = 此函数)
      → duanxianxia_batch.py main() → build_premarket_analysis(report)
      → 此函数 → run_v4_2_pipeline() → 适配 batch 格式
    """
    # 推断交易日期
    trade_date = _infer_trade_date_from_report(report)

    # 确定 project_root
    from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT
    root = Path(project_root) if project_root is not None else DEFAULT_PROJECT_ROOT

    # 运行 v4.2 管线
    result = run_v4_2_pipeline(
        date_t0=trade_date,
        project_root=str(root),
    )

    if "error" in result:
        return {
            "enabled": False,
            "version": VERSION,
            "error": result["error"],
            "meta": {"engine": "premarket_v4_2", "error": result["error"]},
        }

    # 适配 batch 格式
    ep = result.get("execution_plan", {})
    orders = ep.get("orders", [])
    emo = result.get("emotion", {})

    # 买入候选 (所有订单)
    buy_rows = _v4_2_orders_to_batch_rows(orders, emo)
    buy_codes = {str(r.get("code")) for r in buy_rows}

    # 观察候选 (排名靠前但未入选的，从各池 top_n 中取)
    watch_rows = _v4_2_watch_candidates(result, buy_codes)

    # 闸门信息
    buy_gate = {
        "regime": emo.get("state", "UNKNOWN"),
        "selected": len(buy_rows),
        "emotion_state": emo.get("state"),
        "position_cap": emo.get("total_position_cap", 1.0),
        "buy_mode": emo.get("buy_mode"),
        "crisis_count": emo.get("crisis_count", 0),
        "t0_downgraded": emo.get("t0_downgraded", False),
    }

    total_candidates = sum(
        result.get("pools", {}).get(pn, {}).get("n_total", 0)
        for pn in ["一字封", "换手封", "分歧封", "非板"]
    )

    return {
        "enabled": True,
        "version": VERSION,
        "candidate_count": total_candidates,
        "top_candidates": buy_rows,
        "actionable_candidates": buy_rows,
        "watch_candidates": watch_rows,
        "buy_gate": buy_gate,
        "meta": {
            "engine": "premarket_v4_2",
            "emotion": emo,
            "buy_gate": buy_gate,
        },
        # 保留完整 v4.2 结果供调试
        "v4_2_raw": {
            "emotion": emo,
            "pools": result.get("pools", {}),
            "warnings": result.get("warnings", []),
            "diagnostics": result.get("diagnostics", {}),
        },
    }


def _infer_trade_date_from_report(report: Dict[str, Any]) -> str:
    """从 batch.py 的 report dict 中推断交易日期。

    优先从 items[0].capture_path 提取，其次用 generated_at。
    """
    # 尝试从 capture_path 提取
    for item in report.get("items", []) or []:
        capture_path = str((item or {}).get("capture_path") or "").strip()
        if not capture_path:
            continue
        parts = Path(capture_path).parts
        for idx, part in enumerate(parts):
            if part == "captures" and idx + 1 < len(parts):
                try:
                    from datetime import datetime
                    return datetime.fromisoformat(parts[idx + 1]).strftime("%Y-%m-%d")
                except Exception:
                    pass

    # 回退到 generated_at
    generated_at = str(report.get("generated_at") or "").strip()
    if len(generated_at) >= 10:
        try:
            from datetime import datetime
            return datetime.fromisoformat(generated_at[:10]).strftime("%Y-%m-%d")
        except Exception:
            pass

    # 最后回退到今天
    from datetime import date
    return date.today().isoformat()


def _v4_2_orders_to_batch_rows(
    orders: List[Dict[str, Any]],
    emo: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """将 v4.2 订单列表转换为 batch.py 兼容的候选行格式。"""
    rows: List[Dict[str, Any]] = []
    for i, order in enumerate(orders):
        pool = order.get("pool", "")
        reasons = _v4_2_reasons(order, emo)
        risks = _v4_2_risks(order)

        rows.append({
            "code": order.get("code", ""),
            "name": order.get("name", ""),
            "rank": order.get("pool_rank", i + 1),
            "action_type": "BUY",
            "pre_gate_action_type": "BUY",
            "score": order.get("position_pct", 0),
            "conviction_score": order.get("position_pct", 0),
            "expected_return_score": order.get("position_pct", 0),
            "action_reason": "；".join(reasons),
            "reasons": reasons,
            "risks": risks,
            "source_hit_count": 1,
            "pool": pool,
            "position_pct": order.get("position_pct", 0),
            "buy_strategy": order.get("buy_strategy", ""),
            "confirmation_threshold": order.get("confirmation_threshold", "正常"),
            "height_mult": order.get("height_mult", 1.0),
            "risk_mult": order.get("risk_mult", 1.0),
            "emotion_cap": order.get("emotion_cap", 1.0),
        })
    return rows


def _v4_2_watch_candidates(
    result: Dict[str, Any],
    buy_codes: set,
    max_watch: int = 15,
) -> List[Dict[str, Any]]:
    """从各池 top_n 中提取未入选的股票作为观察候选。"""
    watch_rows: List[Dict[str, Any]] = []
    for pool_name, pool_data in result.get("pools", {}).items():
        for rk in pool_data.get("top_n", []):
            if str(rk.get("code", "")) in buy_codes:
                continue
            routed = rk.get("routed", {})
            reasons = [
                f"{pool_name}池排名#{rk.get('rank', '?')}",
                f"板数={routed.get('board_height', 0)}",
            ]
            if rk.get("bonus_applied"):
                reasons.append(f"Bonus +{rk['bonus_applied']}")
            watch_rows.append({
                "code": rk.get("code", ""),
                "name": rk.get("name", ""),
                "rank": rk.get("rank", 0),
                "action_type": "WATCH",
                "pre_gate_action_type": "WATCH",
                "score": rk.get("score_primary") or 0,
                "action_reason": "；".join(reasons),
                "reasons": reasons,
                "risks": routed.get("risk_tags", []),
                "source_hit_count": 0,
                "pool": pool_name,
            })
    return watch_rows[:max_watch]


def _v4_2_reasons(order: Dict[str, Any], emo: Dict[str, Any]) -> List[str]:
    """生成 v4.2 订单的推荐理由。"""
    reasons = [f"{order.get('pool', '')}池排名#{order.get('pool_rank', '?')}"]
    pos = order.get("position_pct", 0)
    reasons.append(f"仓位{pos:.1f}%")
    buy = order.get("buy_strategy", "")
    if buy:
        reasons.append(buy)
    if order.get("height_mult", 1.0) < 1.0:
        reasons.append(f"高度调制×{order['height_mult']:.1f}")
    if order.get("risk_mult", 1.0) < 1.0:
        reasons.append(f"风险调制×{order['risk_mult']:.1f}")
    if emo.get("t0_downgraded"):
        reasons.append(f"T0降级: {emo.get('t0_downgrade_reason', '')}")
    return reasons


def _v4_2_risks(order: Dict[str, Any]) -> List[str]:
    """提取 v4.2 订单的风险标签。"""
    return order.get("risk_tags", [])


# ============================================================================
# CLI
# ============================================================================

def _main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=f"盘前竞价短线选股系统 {VERSION} — 完整决策链路"
    )
    ap.add_argument("--date", required=True, help="T0 日期 (YYYY-MM-DD)")
    ap.add_argument("--project-root", required=True, help="项目根目录路径")
    ap.add_argument("--output", "-o", help="输出 JSON 文件路径 (默认: stdout)")
    ap.add_argument("--top-n", type=int, default=3, help="每池取 Top N (默认: 3)")
    ap.add_argument("--text", action="store_true", help="输出可读文本格式")
    args = ap.parse_args(argv)

    result = run_v4_2_pipeline(
        date_t0=args.date,
        project_root=args.project_root,
        top_n_per_pool=args.top_n,
    )

    if args.text:
        # 文本格式输出
        if "error" in result:
            print(f"❌ 错误: {result['error']}")
            return 1

        emo = result["emotion"]
        print("=" * 60)
        print(f"  盘前竞价选股系统 {VERSION}")
        print(f"  日期: {result['date']}")
        print(f"  情绪: {emo['state']} | 总仓位: {emo['total_position_cap']*100:.0f}%")
        print("=" * 60)

        for pool_name, pool_data in result["pools"].items():
            print(f"\n📊 {pool_name}池 ({pool_data['n_total']}只候选, {pool_data['n_filtered']}只过滤):")
            if pool_data["top_n"]:
                for rk in pool_data["top_n"]:
                    routed = rk.get("routed", {})
                    print(f"  #{rk['rank']} {rk['code']} {rk['name']} "
                          f"| 板数={routed.get('board_height', 0)} "
                          f"| 开板={routed.get('open_num', 0)}")
            else:
                print("  (无入选)")

        ep = result["execution_plan"]
        if ep.get("orders"):
            print(f"\n✅ 可下单列表 ({len(ep['orders'])}只):")
            for o in ep["orders"]:
                print(f"  {o['code']} {o['name']} | {o['pool']} "
                      f"| 仓位={o['position_pct']}% | {o['buy_strategy']}")
        else:
            print("\n⚠️ 今日无可下单股票")

        print(f"\n仓位: 已分配 {ep['allocated_position']}% | 机动 {ep['reserve_position']}%")

        if result["warnings"]:
            print(f"\n⚠️ 警告:")
            for w in result["warnings"]:
                print(f"  - {w}")
    else:
        # JSON 输出
        payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        if args.output:
            Path(args.output).write_text(payload, encoding="utf-8")
            print(f"结果已写入: {args.output}")
        else:
            sys.stdout.write(payload + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(_main())