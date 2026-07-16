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
    EmotionPhase,
    RiskTier,
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
    bundle: Optional[PremarketDataBundle] = None,
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
        bundle: 可选, 预加载的 PremarketDataBundle (传入后跳过数据加载步骤)

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
    if bundle is not None:
        # 使用预加载的 bundle (从 report items 构建, 避免重新下载)
        pass
    else:
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
        # 直接传 capture rows; canonicalize_row → _row_source 会从 each row["raw"] 提取 positional array
        datasets[dsid] = rows

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
        qxlive_close_t1=getattr(bundle, "qxlive_close_t1_rows", None),  # v4 新增
        history=history,
        static_thresholds=static_thresholds,
        # kqxy_t1/kqxy_t2 不再显式传入, 由 determine_emotion_state 自动从 history 提取
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
            "phase": emotion_result.phase.value,
            "phase_label": emotion_result.phase_label,
            "level": emotion_result.level.value,
            "level_score": emotion_result.level_score,
            "direction": emotion_result.direction.value,
            "risk_tier": emotion_result.risk_tier.value,
            "position_cap": emotion_result.position_cap,
            "buy_mode": emotion_result.buy_mode.value,
            "relay_health": emotion_result.relay_health,
            "jinji_1_2": emotion_result.jinji_1_2,
            "jinji_2_3": emotion_result.jinji_2_3,
            "ztbx_925": emotion_result.ztbx_925,
            "lbbx_925": emotion_result.lbbx_925,
            "advance_share": emotion_result.advance_share,
            "dt_925": emotion_result.dt_925,
            "hard_veto": emotion_result.hard_veto,
            "profit_collapse": emotion_result.profit_collapse,
            "breadth_panic": emotion_result.breadth_panic,
            "kqxy_t1": emotion_result.kqxy_t1,
            "kqxy_t2": emotion_result.kqxy_t2,
            "loss_level": emotion_result.loss_level,
            "loss_direction": emotion_result.loss_direction,
            "loss_overlay": emotion_result.loss_overlay,
            "qx_925": emotion_result.qx_925,
            "qx_stats": emotion_result.qx_stats,
            "pool_enabled": {
                "一字封": emotion_result.yizi_enabled,
                "换手封": emotion_result.huanshou_enabled,
                "分歧封": emotion_result.fenqi_enabled,
                "非板": emotion_result.feiban_enabled,
            },
            "pool_mult": {
                "一字封": emotion_result.pool_yizi_mult,
                "换手封": emotion_result.pool_huanshou_mult,
                "分歧封": emotion_result.pool_fenqi_mult,
                "非板": emotion_result.pool_feiban_mult,
            },
            "phase_confidence": emotion_result.phase_confidence,
            "data_quality": emotion_result.data_quality,
            "profit_level": emotion_result.profit_level,
            "breadth_level": emotion_result.breadth_level,
            "relay_level": emotion_result.relay_level,
            "profit_delta": emotion_result.profit_delta,
            "breadth_delta": emotion_result.breadth_delta,
            "relay_delta": emotion_result.relay_delta,
            "height_preference": emotion_result.height_preference,
            "fenqi_priority": emotion_result.fenqi_priority,
            "auction_buy_enabled": emotion_result.auction_buy_enabled,
            # v4 双时间截面新增字段
            "close_level_score": emotion_result.close_level_score,
            "pre_level_score": emotion_result.pre_level_score,
            "level_source": emotion_result.level_source,
            "ztbx_close": emotion_result.ztbx_close,
            "lbbx_close": emotion_result.lbbx_close,
            "advance_share_close": emotion_result.advance_share_close,
            "dt_close": emotion_result.dt_close,
            "warnings": emotion_result.warnings,
            "diagnostics": emotion_result.diagnostics,
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


def _extract_t0_rows_from_item(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """从 report item 的 capture_path 读取实际行数据"""
    capture_path = item.get("capture_path", "")
    if not capture_path:
        return []
    try:
        capture = json.loads(Path(capture_path).read_text(encoding="utf-8"))
        return capture.get("rows", [])
    except Exception:
        return []


def _build_bundle_from_report(
    report: Dict[str, Any],
    project_root: Path,
    trade_date: str,
) -> PremarketDataBundle:
    """从已有 report 的 capture 数据构建 PremarketDataBundle, 避免重新下载。

    T0 竞价数据从 report items 的 capture_path 读取 (9:25 原始数据),
    T-1 数据直接从 captures 目录加载 (历史数据, 不会变)。

    如果 report 没有 items, 退回常规 load_premarket_bundle。

    注意: 不调用 load_premarket_bundle(trade_date), 因为 T0 captures
    可能只存在于 report 中而不在 captures/ 目录下。
    """
    from duanxianxia_v7_1_data_loader import (
        load_premarket_bundle as _load_bundle,
        load_capture_at_time, _extract_rows, _extract_meta,
        previous_trading_day,
        DS_HOME_QXLIVE_TOP, DS_HOME_ZTPOOL, DS_REVIEW_FUPAN, DS_REVIEW_DAILY, DS_REVIEW_LTGD,
        DS_HOME_KAIPAN, DS_CASHFLOW_TODAY, DS_CASHFLOW_3DAY,
        DS_CASHFLOW_5DAY, DS_CASHFLOW_10DAY,
        QXLIVE_PREMARKET_BOUNDARY_HHMMSS, DEFAULT_KAIPAN_HISTORY_DAYS,
    )

    items = report.get("items", [])
    if not items:
        return _load_bundle(trade_date, str(project_root),
                            premarket_auction_cutoff="092900")
    item_map: Dict[str, Dict[str, Any]] = {}
    for item in items:
        did = item.get("dataset_id", "")
        if did:
            item_map[did] = item

    # 从 report items 读取 T0 竞价数据 (9:25 原始快照)
    def _rows(ds_id: str) -> List[Dict[str, Any]]:
        return _extract_t0_rows_from_item(item_map.get(ds_id, {}))

    # 直接加载 T-1 数据 (不依赖 load_premarket_bundle, 避免 T0 缺失报错)
    d_t0 = date.fromisoformat(trade_date)
    d_t1 = previous_trading_day(project_root, d_t0, n=1)
    date_t1_str = d_t1.isoformat()
    try:
        d_t2 = previous_trading_day(project_root, d_t0, n=2)
        date_t2_str: Optional[str] = d_t2.isoformat()
    except Exception:
        date_t2_str = None

    t1_warnings: List[str] = []

    def _try(ds: str, d: str = date_t1_str, *, pick: str = "latest",
             max_hhmmss: Optional[str] = None) -> List[Dict[str, Any]]:
        cap = load_capture_at_time(
            project_root, d, ds, pick=pick,
            max_hhmmss=max_hhmmss, raise_if_missing=False,
        )
        if cap is None:
            t1_warnings.append(f"missing capture: {ds} {d}")
            return []
        return _extract_rows(cap)

    def _try_meta(ds: str, d: str = date_t1_str, *, pick: str = "latest",
                  max_hhmmss: Optional[str] = None) -> Dict[str, Any]:
        cap = load_capture_at_time(
            project_root, d, ds, pick=pick,
            max_hhmmss=max_hhmmss, raise_if_missing=False,
        )
        return _extract_meta(cap) if cap else {}

    # T-1 qxlive
    q1 = load_capture_at_time(
        project_root, date_t1_str, DS_HOME_QXLIVE_TOP,
        max_hhmmss=QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
        pick="earliest_before", raise_if_missing=False,
    )
    qxlive_top_t1_rows = _extract_rows(q1)
    qxlive_top_t1_meta = _extract_meta(q1) if q1 else {}
    if not qxlive_top_t1_rows:
        t1_warnings.append(f"missing_or_empty: {DS_HOME_QXLIVE_TOP} t1")

    # v4 新增: T-1 盘后 review_daily 收盘数据 (来自 postmarket cron, 用于水位计算)
    rc1 = load_capture_at_time(
        project_root, date_t1_str, DS_REVIEW_DAILY,
        pick="latest", raise_if_missing=False,
    )
    qxlive_close_t1_rows = _extract_rows(rc1)
    qxlive_close_t1_meta = _extract_meta(rc1) if rc1 else {}

    # T-2 qxlive
    if date_t2_str:
        q2 = load_capture_at_time(
            project_root, date_t2_str, DS_HOME_QXLIVE_TOP,
            max_hhmmss=QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
            pick="earliest_before", raise_if_missing=False,
        )
        qxlive_top_t2_rows = _extract_rows(q2)
        qxlive_top_t2_meta = _extract_meta(q2) if q2 else {}
    else:
        qxlive_top_t2_rows = []
        qxlive_top_t2_meta = {}

    # T-1 其他数据
    fupan_t1 = _try(DS_REVIEW_FUPAN)
    ztpool_t1 = _try(DS_HOME_ZTPOOL)
    ltgd_all = _try(DS_REVIEW_LTGD)
    ltgd_5day_t1 = [r for r in ltgd_all if str(r.get("周期", "") or "").strip() == "5日"]
    cashflow_today_t1 = _try(DS_CASHFLOW_TODAY)
    cashflow_3day_t1 = _try(DS_CASHFLOW_3DAY)
    cashflow_5day_t1 = _try(DS_CASHFLOW_5DAY)
    cashflow_10day_t1 = _try(DS_CASHFLOW_10DAY)

    # T-1 kaipan
    kaipan_t1 = load_capture_at_time(
        project_root, date_t1_str, DS_HOME_KAIPAN,
        pick="latest", raise_if_missing=False,
    )
    kaipan_t1_rows = _extract_rows(kaipan_t1)
    kaipan_t1_meta = _extract_meta(kaipan_t1)

    # kaipan history (T-2 ~ T-N)
    kaipan_history: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]] = []
    cur = d_t1
    for _ in range(DEFAULT_KAIPAN_HISTORY_DAYS):
        ds = cur.isoformat()
        cap = load_capture_at_time(
            project_root, ds, DS_HOME_KAIPAN,
            pick="latest", raise_if_missing=False,
        )
        if cap is not None:
            kaipan_history.append((ds, _extract_rows(cap), _extract_meta(cap)))
        try:
            cur = previous_trading_day(project_root, cur, n=1)
        except Exception:
            break

    # 加载 T-1/T-2 盘后 KQXY (不用 premarket 时间限制, 取最新 snapshot)
    from duanxianxia_v4_2_d6_emotion import _extract_qxlive_metric
    kqxy_t1 = None
    kqxy_t2 = None
    for date_str, attr in [(date_t1_str, "kqxy_t1"), (date_t2_str, "kqxy_t2")]:
        if date_str is None:
            continue
        kq_cap = load_capture_at_time(
            project_root, date_str, DS_HOME_QXLIVE_TOP,
            pick="latest", raise_if_missing=False,
        )
        if kq_cap:
            kq_rows = _extract_rows(kq_cap)
            kq_val = _extract_qxlive_metric(kq_rows, "KQXY")
            if kq_val is not None and kq_val > 0:
                if attr == "kqxy_t1":
                    kqxy_t1 = kq_val
                else:
                    kqxy_t2 = kq_val

    return PremarketDataBundle(
        date_t0=trade_date,
        date_t1=date_t1_str,
        date_t2=date_t2_str,
        project_root=str(project_root),
        # T0: 从 report items 读取原始竞价数据
        auction_vratio=_rows("auction.jjyd.vratio"),
        auction_qiangchou=_rows("auction.jjyd.qiangchou"),
        auction_netamount=_rows("auction.jjyd.net_amount"),
        auction_fengdan=_rows("auction.jjlive.fengdan"),
        auction_weimai=_rows("auction.jjyd.weimai"),
        kaipan_t0_rows=_rows("home.kaipan.plate.summary"),
        kaipan_t0_meta={},
        qxlive_top_t0_rows=_rows("home.qxlive.top_metrics"),
        qxlive_top_t0_meta={},
        # T-1: 直接从 captures 加载
        kaipan_t1_rows=kaipan_t1_rows,
        kaipan_t1_meta=kaipan_t1_meta,
        cashflow_today_t1=cashflow_today_t1,
        cashflow_3day_t1=cashflow_3day_t1,
        cashflow_5day_t1=cashflow_5day_t1,
        cashflow_10day_t1=cashflow_10day_t1,
        fupan_t1=fupan_t1,
        ltgd_5day_t1=ltgd_5day_t1,
        ztpool_t1=ztpool_t1,
        qxlive_top_t1_rows=qxlive_top_t1_rows,
        qxlive_top_t1_meta=qxlive_top_t1_meta,
        qxlive_top_t2_rows=qxlive_top_t2_rows,
        qxlive_top_t2_meta=qxlive_top_t2_meta,
        kaipan_history=kaipan_history,
        warnings=t1_warnings,
        qxlive_close_t1_rows=qxlive_close_t1_rows,
        qxlive_close_t1_meta=qxlive_close_t1_meta,
    )
    # KQXY 盘后值 (从 qxlive latest snapshot 提取, 非 9:25)
    # v4: determine_emotion_state 已自动从 history 提取, 此处保留用于其他用途
    bundle.kqxy_t1 = kqxy_t1
    bundle.kqxy_t2 = kqxy_t2
    return bundle

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

    # 从已有 report 的 capture 数据构建 bundle (T0用9:25原始数据, 不重新下载)
    bundle = _build_bundle_from_report(report, root, trade_date)

    # 构建历史数据 (从 captures 目录扫描 D6 历史, 用于水位分位计算)
    # 延迟导入避免循环依赖
    from duanxianxia_v4_2_premarket_daily import _build_history_from_raw_captures
    history = _build_history_from_raw_captures(root)

    # 运行 v4.2 管线
    result = run_v4_2_pipeline(
        date_t0=trade_date,
        project_root=str(root),
        bundle=bundle,
        history=history,
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
        "regime": emo.get("phase_label", "UNKNOWN"),
        "selected": len(buy_rows),
        "emotion_state": emo.get("phase_label"),
        "phase": emo.get("phase"),
        "risk_tier": emo.get("risk_tier"),
        "position_cap": emo.get("position_cap", 1.0),
        "buy_mode": emo.get("buy_mode"),
        "t0_impulse": emo.get("hard_veto"),
        "profit_collapse": emo.get("profit_collapse", False),
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
    if emo.get("hard_veto"):
        reasons.append("极端否决触发")
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
        print(f"  周期: {emo['phase_label']} | 水位: {emo['level']} | 方向: {emo['direction']}")
        print(f"  风险: {emo['risk_tier']} | 仓位上限: {emo['position_cap']*100:.0f}%")
        print(f"  接力健康度: {emo.get('relay_health')}% | advance_share: {emo.get('advance_share')}")
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