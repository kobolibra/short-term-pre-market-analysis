#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v5_0_runner.py  --  v5.0 主编排器

============================================================================
v4.2 → v5.0 核心变更
============================================================================

决策链路:
  [9:25 数据就绪]
  → Layer 0: 数据加载 (复用 v7.1 data_loader + feature_builder)
  → Layer 1: D6 统计剖面 (duanxianxia_v5_0_d6_profile)    ← 新
  → Layer 2: D7 结构路由 (duanxianxia_v4_2_d7_router)     ← 不变
  → Layer 3: 池内排名   (duanxianxia_v4_2_pool_ranker)    ← 不变
  → Layer 4: 风控执行   (duanxianxia_v5_0_risk_exec)       ← 新
  → 输出: 执行计划

用法:
  python3 duanxianxia_v5_0_runner.py --date 2026-07-19 --project-root /path/to/project
  python3 duanxianxia_v5_0_runner.py --date 2026-07-19 --project-root /path/to/project --output result.json

============================================================================
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

# v5.0 核心模块
from duanxianxia_v5_0_d6_profile import (
    calculate_profile,
    format_profile,
    profile_to_dict,
    ProfileHistory,
    MarketProfile,
)

# 复用 v4.2 的数据提取函数
from duanxianxia_v4_2_d6_emotion import (
    _extract_qxlive_metric,
    _extract_ztpool_pbbx,
    _smoothed_rate,
    RELAY_WEIGHT_1_2,
    RELAY_WEIGHT_2_3,
)

# D7 路由 + 池排名 (不变)
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

# v5.0 风控执行
from duanxianxia_v5_0_risk_exec import (
    build_execution_plan,
    format_execution_plan,
    execution_plan_to_dict,
    ExecutionPlan,
)


VERSION = "v5.0.0"


# ============================================================================
# 数据提取
# ============================================================================

def _extract_review_metric(rows: List[Dict[str, Any]], metric_key: str) -> Optional[float]:
    """从 review_daily rows 中提取指定 metric_key 的值。"""
    for row in rows:
        if str(row.get("metric_key", "")) == metric_key:
            try:
                v = row.get("value")
                return float(v) if v not in (None, "") else None
            except (ValueError, TypeError):
                return None
    return None


def _extract_close_data(review_daily_rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """
    从 review_daily (T-1 盘后) 数据中提取 close 指标值。

    返回:
        {
            "advance_share": float or None,
            "dt": float or None,
            "ztbx": float or None,
            "lbbx": float or None,
            "qx": float or None,
            "kqxy": float or None,
        }
    """
    sz = _extract_review_metric(review_daily_rows, "SZ")
    xd = _extract_review_metric(review_daily_rows, "XD")
    advance_share = None
    if sz is not None and xd is not None and (sz + xd) > 0:
        advance_share = round(sz / (sz + xd), 4)

    dt_raw = _extract_review_metric(review_daily_rows, "DT")
    dt = int(dt_raw) if dt_raw is not None else None

    return {
        "advance_share": advance_share,
        "dt": float(dt) if dt is not None else None,
        "ztbx": _extract_review_metric(review_daily_rows, "ZTBX"),
        "lbbx": _extract_review_metric(review_daily_rows, "LBBX"),
        "qx": _extract_review_metric(review_daily_rows, "QX"),
        "kqxy": _extract_review_metric(review_daily_rows, "KQXY"),
    }


def _extract_pre_data(qxlive_rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """
    从 qxlive (T0 9:25) 数据中提取 pre 指标值。

    返回:
        {
            "advance_share": float or None,
            "dt": float or None,
            "ztbx": float or None,
            "lbbx": float or None,
            "qx": float or None,
        }
    """
    sz = _extract_qxlive_metric(qxlive_rows, "SZ")
    xd = _extract_qxlive_metric(qxlive_rows, "XD")
    advance_share = None
    if sz is not None and xd is not None and (sz + xd) > 0:
        advance_share = round(sz / (sz + xd), 4)

    dt_raw = _extract_qxlive_metric(qxlive_rows, "DT")
    dt = int(dt_raw) if dt_raw is not None else None

    return {
        "advance_share": advance_share,
        "dt": float(dt) if dt is not None else None,
        "ztbx": _extract_qxlive_metric(qxlive_rows, "ZTBX"),
        "lbbx": _extract_qxlive_metric(qxlive_rows, "LBBX"),
        "qx": _extract_qxlive_metric(qxlive_rows, "QX"),
    }


def _calc_relay_health(ztpool_rows: List[Dict[str, Any]]) -> Optional[float]:
    """从 ztpool 数据计算接力健康度 (与 v4.2 一致)。"""
    pbbx = _extract_ztpool_pbbx(ztpool_rows)
    jinji_1_2_raw = pbbx.get("PBBX_1_2", {})
    jinji_2_3_raw = pbbx.get("PBBX_2_3", {})
    jinji_1_2 = _smoothed_rate(
        jinji_1_2_raw.get("promoted"), jinji_1_2_raw.get("eligible"),
    )
    jinji_2_3 = _smoothed_rate(
        jinji_2_3_raw.get("promoted"), jinji_2_3_raw.get("eligible"),
    )

    if jinji_1_2 is not None and jinji_2_3 is not None:
        return round(RELAY_WEIGHT_1_2 * jinji_1_2 + RELAY_WEIGHT_2_3 * jinji_2_3, 2)
    elif jinji_1_2 is not None:
        return round(jinji_1_2, 2)
    elif jinji_2_3 is not None:
        return round(jinji_2_3, 2)
    return None


# ============================================================================
# 主编排函数
# ============================================================================

def run_v5_0_pipeline(
    date_t0: str,
    project_root: str,
    history: Optional[ProfileHistory] = None,
    static_thresholds: Optional[Dict[str, float]] = None,
    top_n_per_pool: int = 3,
    premarket_auction_cutoff: str = "092900",
    bundle: Optional[PremarketDataBundle] = None,
) -> Dict[str, Any]:
    """
    执行 v5.0 完整决策链路。

    Args:
        date_t0: T0 日期 (YYYY-MM-DD)
        project_root: 项目根目录路径
        history: ProfileHistory 历史数据 (用于分位数计算)
        static_thresholds: 静态阈值 (历史数据不足时回退)
        top_n_per_pool: 每池取 Top N
        premarket_auction_cutoff: 竞价数据截断时间
        bundle: 可选, 预加载的 PremarketDataBundle

    Returns:
        {
            "version": "v5.0.0",
            "date": "...",
            "profile": {...},
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
        pass  # 使用预加载的 bundle
    else:
        try:
            bundle = load_premarket_bundle(
                date_t0, project_root,
                premarket_auction_cutoff=premarket_auction_cutoff,
            )
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
    dsid_to_attr = {
        "auction.jjyd.vratio": "auction_vratio",
        "auction.jjyd.qiangchou": "auction_qiangchou",
        "auction.jjyd.net_amount": "auction_netamount",
        "auction.jjyd.weimai": "auction_weimai",
    }
    for dsid in AUCTION_DATASETS:
        attr = dsid_to_attr.get(dsid, "")
        rows = getattr(bundle, attr, []) if attr else []
        datasets[dsid] = rows

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

    # 加载 ztpool 数据 (接力健康度)
    ztpool_t1 = bundle.ztpool_t1 if bundle.ztpool_t1 else []
    diagnostics["ztpool_t1"] = {"n_rows": len(ztpool_t1)}

    # 加载 rank 数据
    hot_rank_map, rocket_rank_map = _load_rank_data(bundle, premarket_auction_cutoff)
    diagnostics["rank_data"] = {
        "n_hot_rank": len(hot_rank_map),
        "n_rocket_rank": len(rocket_rank_map),
    }

    # ========================================================================
    # Layer 1: D6 统计剖面 (v5.0 核心)
    # ========================================================================

    # 提取 close 数据 (T-1 盘后, 来自 review_daily)
    review_daily_rows = getattr(bundle, "qxlive_close_t1_rows", None) or []
    close_data = _extract_close_data(review_daily_rows)

    # 提取 pre 数据 (T0 盘前 9:25, 来自 qxlive)
    pre_data = _extract_pre_data(bundle.qxlive_top_t0_rows)

    # 计算接力健康度
    relay_health = _calc_relay_health(ztpool_t1)

    # 提取 T-1 盘前 ZTBX/LBBX (用于极端否决)
    ztbx_pre_t1 = _extract_qxlive_metric(bundle.qxlive_top_t1_rows, "ZTBX")
    lbbx_pre_t1 = _extract_qxlive_metric(bundle.qxlive_top_t1_rows, "LBBX")

    # 计算统计剖面
    profile = calculate_profile(
        close_data=close_data,
        pre_data=pre_data,
        relay_health=relay_health,
        history=history,
        static_thresholds=static_thresholds,
        ztbx_pre_t1=ztbx_pre_t1,
        lbbx_pre_t1=lbbx_pre_t1,
    )
    profile.date = date_t0
    warnings.extend(profile.warnings)

    # ========================================================================
    # Layer 2: D7 结构路由 (不变)
    # ========================================================================
    pools = route_all_stocks(features, review_plate_map)
    pool_counts = {k.value: len(v) for k, v in pools.items()}
    diagnostics["routing"] = {"pool_counts": pool_counts}

    # ========================================================================
    # Layer 3: 池内排名 (不变)
    # ========================================================================
    pool_results = rank_all_pools(pools, hot_rank_map, rocket_rank_map)
    pool_diag = {}
    for pt, pr in pool_results.items():
        pool_diag[pr.pool_label] = pr.diagnostics
    diagnostics["ranking"] = pool_diag

    # ========================================================================
    # Layer 4: 风控执行 (v5.0)
    # ========================================================================
    execution_plan = build_execution_plan(pool_results, profile, date=date_t0)
    warnings.extend(execution_plan.warnings)

    # ========================================================================
    # 组装输出
    # ========================================================================
    return {
        "version": VERSION,
        "date": date_t0,
        "profile": profile_to_dict(profile),
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
# Rank 数据加载 (与 v4.2 一致)
# ============================================================================

def _load_rank_data(
    bundle: PremarketDataBundle,
    premarket_auction_cutoff: str = "092900",
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """从 premarket bundle 中加载热度榜和飙升榜排名数据。"""
    hot_rank_map: Dict[str, int] = {}
    rocket_rank_map: Dict[str, int] = {}

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


# ============================================================================
# batch.py 适配器
# ============================================================================

def _extract_t0_rows_from_item(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """从 report item 的 capture_path 读取实际行数据。"""
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
    """
    从已有 report 的 capture 数据构建 PremarketDataBundle, 避免重新下载。

    复用 v4.2 runner 的 _build_bundle_from_report 逻辑。
    """
    from duanxianxia_v7_1_data_loader import (
        load_premarket_bundle as _load_bundle,
        load_capture_at_time, _extract_rows, _extract_meta,
        previous_trading_day,
        DS_HOME_QXLIVE_TOP, DS_HOME_ZTPOOL, DS_REVIEW_FUPAN, DS_REVIEW_DAILY,
        DS_REVIEW_LTGD, DS_HOME_KAIPAN,
        DS_CASHFLOW_TODAY, DS_CASHFLOW_3DAY, DS_CASHFLOW_5DAY, DS_CASHFLOW_10DAY,
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

    def _rows(ds_id: str) -> List[Dict[str, Any]]:
        return _extract_t0_rows_from_item(item_map.get(ds_id, {}))

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

    # T-1 盘后 review_daily 收盘数据
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

    # kaipan history
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

    return PremarketDataBundle(
        date_t0=trade_date,
        date_t1=date_t1_str,
        date_t2=date_t2_str,
        project_root=str(project_root),
        auction_vratio=_rows("auction.jjyd.vratio"),
        auction_qiangchou=_rows("auction.jjyd.qiangchou"),
        auction_netamount=_rows("auction.jjyd.net_amount"),
        auction_fengdan=_rows("auction.jjlive.fengdan"),
        auction_weimai=_rows("auction.jjyd.weimai"),
        kaipan_t0_rows=_rows("home.kaipan.plate.summary"),
        kaipan_t0_meta={},
        qxlive_top_t0_rows=_rows("home.qxlive.top_metrics"),
        qxlive_top_t0_meta={},
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


def build_premarket_analysis_v5_0(
    report: Dict[str, Any],
    project_root: Optional[str | Path] = None,
) -> Dict[str, Any]:
    """
    batch.py 适配器: report → v5.0 决策。

    签名与 build_premarket_analysis_v4_2 一致。
    """
    trade_date = _infer_trade_date_from_report(report)

    from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT
    root = Path(project_root) if project_root is not None else DEFAULT_PROJECT_ROOT

    # 从已有 report 构建 bundle
    bundle = _build_bundle_from_report(report, root, trade_date)

    # 构建 ProfileHistory (延迟导入避免循环依赖)
    from duanxianxia_v5_0_premarket_daily import _build_history_from_raw_captures
    history = _build_history_from_raw_captures(root)

    # 运行 v5.0 管线
    result = run_v5_0_pipeline(
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
            "meta": {"engine": "premarket_v5_0", "error": result["error"]},
        }

    ep = result.get("execution_plan", {})
    orders = ep.get("orders", [])
    prof = result.get("profile", {})

    # 买入候选
    buy_rows = _v5_0_orders_to_batch_rows(orders, prof)
    buy_codes = {str(r.get("code")) for r in buy_rows}

    # 观察候选
    watch_rows = _v5_0_watch_candidates(result, buy_codes)

    # 闸门信息
    buy_gate = {
        "regime": prof.get("tilt", "UNKNOWN"),
        "selected": len(buy_rows),
        "bottleneck": prof.get("bottleneck_name"),
        "bottleneck_pct": prof.get("bottleneck"),
        "heat": prof.get("heat"),
        "divergence": prof.get("divergence"),
        "position": prof.get("position"),
        "buy_mode": prof.get("buy_mode"),
        "extreme_veto": prof.get("extreme_veto"),
        "profit_collapse": prof.get("profit_collapse", False),
        "breadth_panic": prof.get("breadth_panic", False),
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
            "engine": "premarket_v5_0",
            "profile": prof,
            "buy_gate": buy_gate,
        },
        "v5_0_raw": {
            "profile": prof,
            "pools": result.get("pools", {}),
            "warnings": result.get("warnings", []),
            "diagnostics": result.get("diagnostics", {}),
        },
    }


def _infer_trade_date_from_report(report: Dict[str, Any]) -> str:
    """从 batch.py 的 report dict 中推断交易日期。"""
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

    generated_at = str(report.get("generated_at") or "").strip()
    if len(generated_at) >= 10:
        try:
            from datetime import datetime
            return datetime.fromisoformat(generated_at[:10]).strftime("%Y-%m-%d")
        except Exception:
            pass

    from datetime import date
    return date.today().isoformat()


def _v5_0_orders_to_batch_rows(
    orders: List[Dict[str, Any]],
    prof: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """将 v5.0 订单列表转换为 batch.py 兼容的候选行格式。"""
    rows: List[Dict[str, Any]] = []
    for i, order in enumerate(orders):
        pool = order.get("pool", "")
        reasons = _v5_0_reasons(order, prof)
        risks = order.get("risk_tags", [])

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
            "profile_position": order.get("profile_position", 0.5),
            "pool_mult": order.get("pool_mult", 1.0),
        })
    return rows


def _v5_0_watch_candidates(
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


def _v5_0_reasons(order: Dict[str, Any], prof: Dict[str, Any]) -> List[str]:
    """生成 v5.0 订单的推荐理由。"""
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
    if prof.get("extreme_veto"):
        reasons.append("极端否决触发")
    return reasons


# ============================================================================
# CLI
# ============================================================================

def _main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=f"盘前竞价短线选股系统 {VERSION} — 统计剖面决策链路"
    )
    ap.add_argument("--date", required=True, help="T0 日期 (YYYY-MM-DD)")
    ap.add_argument("--project-root", required=True, help="项目根目录路径")
    ap.add_argument("--output", "-o", help="输出 JSON 文件路径 (默认: stdout)")
    ap.add_argument("--top-n", type=int, default=3, help="每池取 Top N (默认: 3)")
    ap.add_argument("--text", action="store_true", help="输出可读文本格式")
    args = ap.parse_args(argv)

    # 加载历史数据
    project_root = Path(args.project_root)
    history = None
    try:
        from duanxianxia_v5_0_premarket_daily import _build_history_from_raw_captures
        history = _build_history_from_raw_captures(project_root)
        print(f"历史数据: close={history.close_days()}天, pre={history.pre_days()}天")
    except Exception:
        pass

    result = run_v5_0_pipeline(
        date_t0=args.date,
        project_root=args.project_root,
        history=history,
        top_n_per_pool=args.top_n,
    )

    if args.text:
        if "error" in result:
            print(f"❌ 错误: {result['error']}")
            return 1

        prof = result["profile"]
        print("=" * 60)
        print(f"  盘前竞价选股系统 {VERSION}")
        print(f"  日期: {result['date']}")
        print(f"  瓶颈: {prof['bottleneck_name']} ({prof['bottleneck']:.3f}) "
              f"| 温度: {prof['heat']:.3f} | 分歧: {prof['divergence']:.3f}")
        print(f"  仓位系数: {prof['position']:.3f} | 买点: {prof['buy_mode']}")
        if prof.get("extreme_veto"):
            print(f"  🚨 极端否决: {prof.get('veto_reason', '')}")
        print("=" * 60)

        for pool_name, pool_data in result["pools"].items():
            print(f"\n📊 {pool_name}池 ({pool_data['n_total']}只候选, "
                  f"{pool_data['n_filtered']}只过滤):")
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

        print(f"\n仓位: 已分配 {ep['allocated_position']}% "
              f"| 机动 {ep['reserve_position']}%")

        if result["warnings"]:
            print(f"\n⚠️ 警告:")
            for w in result["warnings"]:
                print(f"  - {w}")
    else:
        payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        if args.output:
            Path(args.output).write_text(payload, encoding="utf-8")
            print(f"结果已写入: {args.output}")
        else:
            sys.stdout.write(payload + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(_main())