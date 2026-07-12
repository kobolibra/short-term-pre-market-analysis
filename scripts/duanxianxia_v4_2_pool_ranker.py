#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_pool_ranker.py  --  v4.2 池内排名模块

第三层：每个池独立排名，使用排名制（非加权打分）。
执行顺序: 风控过滤 → 核心排名 → Bonus 调制 → 取 Top 3。

四池排名逻辑:
  POOL_YIZI     一字封  — seal_amount_ratio + seal_strength + change_rate
  POOL_HUANSHOU  换手封  — turnover_rate + change_rate + net_amount
  POOL_FENQI     分歧封  — change_rate + turnover_rate + volume_ratio
  POOL_FEIBAN    非板    — change_rate + volume_ratio + bid_amount

Bonus 调制 (跨池统一):
  hot_rank ≤ 50       → +1 (所有池)
  rocket_rank ≤ 30    → +1 (非板/换手封)
  grab_strength > 0   → +1 (非板)
  net_amount > 0      → +1 (换手封/分歧封)
  最多 +2 位

设计文档: dimension-design-v4/dimension-design-v4.html §5
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

# 引用 D7 路由模块的类型
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_d7_router import (
    PoolType, RiskTag, RoutedStock, _calc_fill_ratio,
    _calc_seal_amount_ratio, _calc_seal_strength,
    _parse_first_seal_seconds,
)


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class RankedStock:
    """排名后的单只股票"""
    code: str
    name: str = ""
    pool: PoolType = PoolType.POOL_FEIBAN
    pool_label: str = "非板"

    # 排名信息
    rank: int = 0                           # 池内排名 (1-based)
    original_rank: int = 0                  # Bonus 调制前排名
    bonus_applied: int = 0                  # Bonus 移动位数

    # 核心指标值
    score_primary: Optional[float] = None   # 首要排序指标值
    score_secondary: Optional[float] = None # 次要排序指标值
    score_tertiary: Optional[float] = None  # 第三排序指标值

    # 路由信息
    routed: Optional[RoutedStock] = None

    # 是否被风控过滤
    filtered: bool = False
    filter_reason: str = ""

    # 诊断
    diagnostics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PoolRankResult:
    """单池排名结果"""
    pool_type: PoolType
    pool_label: str
    candidates: List[RankedStock] = field(default_factory=list)  # 全部排名
    top_n: List[RankedStock] = field(default_factory=list)       # Top N 入选
    filtered_out: List[RankedStock] = field(default_factory=list)
    pool_median_turnover: Optional[float] = None
    pool_median_bid_amount: Optional[float] = None
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 辅助函数
# ============================================================================

def _pool_median(values: List[Optional[float]]) -> Optional[float]:
    """计算池内中位数"""
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return statistics.median(clean)


def _pool_std(values: List[Optional[float]]) -> Optional[float]:
    """计算池内标准差"""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    return statistics.stdev(clean)


def _change_rate_score(change_rate: Optional[float], optimal_lo: float,
                       optimal_hi: float) -> float:
    """
    计算 change_rate 的排序得分。
    在最优区间内得分最高，偏离越远得分越低。
    返回: 得分 (越高越好)
    """
    if change_rate is None:
        return -999.0  # 缺失排最后

    optimal_center = (optimal_lo + optimal_hi) / 2.0
    optimal_range = (optimal_hi - optimal_lo) / 2.0

    if optimal_lo <= change_rate <= optimal_hi:
        # 在最优区间内: 越接近中心越好
        distance = abs(change_rate - optimal_center)
        return 100.0 - distance * 5.0  # 中心=100, 边缘=~75
    else:
        # 偏离最优区间: 距离越远越低
        if change_rate < optimal_lo:
            distance = optimal_lo - change_rate
        else:
            distance = change_rate - optimal_hi
        return max(0.0, 70.0 - distance * 10.0)


def _turnover_rate_score(turnover: Optional[float], median: Optional[float],
                         std: Optional[float]) -> float:
    """
    计算 turnover_rate 的排序得分。
    在池内中位数 ± 1σ 区间最优，太低了没人接，太高了分歧大。
    """
    if turnover is None:
        return -999.0
    if median is None:
        return 50.0  # 无参考，中性

    if std is None or std == 0:
        std = median * 0.3  # 默认 30% 波动

    lo = median - std
    hi = median + std

    if lo <= turnover <= hi:
        # 在最优区间
        return 100.0 - abs(turnover - median) / max(std, 0.01) * 20.0
    elif turnover < lo:
        return max(0.0, 60.0 - (lo - turnover) / max(median, 0.01) * 40.0)
    else:
        return max(0.0, 60.0 - (turnover - hi) / max(median, 0.01) * 40.0)


# ============================================================================
# 风控过滤
# ============================================================================

def _apply_pool_risk_filter(
    stocks: List[RoutedStock],
    pool_type: PoolType,
    pool_median_turnover: Optional[float],
    pool_median_bid_amount: Optional[float],
) -> Tuple[List[RoutedStock], List[RankedStock]]:
    """
    对池内股票应用风控过滤，返回 (通过列表, 被过滤列表)。

    风控规则:
    - 一字封: fill_ratio > 1.5 → 否决; high_rate + low_turnover → 否决
    - 换手封: fill_ratio > 1.5 → 否决; high_rate + high_turnover → 否决
    - 分歧封: change_rate < -2% → 否决; turnover < median × 0.8 → 否决
    - 非板: bid_amount < median × 0.3 → 否决; high_rate + low_turnover → 否决
    """
    passed: List[RoutedStock] = []
    filtered: List[RankedStock] = []

    for rs in stocks:
        feat = rs.feature or {}
        fill_ratio = _calc_fill_ratio(feat)
        change_rate = feat.get("changeRate")
        turnover = feat.get("turnoverRate")
        bid_amount = feat.get("bidAmount")

        filtered_out = False
        reason = ""

        if pool_type == PoolType.POOL_YIZI:
            # 封单失败: fill_ratio > 1.5
            if fill_ratio is not None and fill_ratio > 1.5:
                filtered_out = True
                reason = f"fill_ratio={fill_ratio:.2f}>1.5"
            # 高开无量诱多: change_rate > 7% and turnover < median × 0.5
            elif (change_rate is not None and change_rate > 7.0
                  and turnover is not None
                  and pool_median_turnover is not None
                  and turnover < pool_median_turnover * 0.5):
                filtered_out = True
                reason = f"高开无量诱多: change={change_rate}%, turnover={turnover}"

        elif pool_type == PoolType.POOL_HUANSHOU:
            # 封单失败: fill_ratio > 1.5
            if fill_ratio is not None and fill_ratio > 1.5:
                filtered_out = True
                reason = f"fill_ratio={fill_ratio:.2f}>1.5"
            # 高开放量滞涨: change_rate > 7% and turnover > median × 1.5
            elif (change_rate is not None and change_rate > 7.0
                  and turnover is not None
                  and pool_median_turnover is not None
                  and turnover > pool_median_turnover * 1.5):
                filtered_out = True
                reason = f"高开放量滞涨: change={change_rate}%, turnover={turnover}"

        elif pool_type == PoolType.POOL_FENQI:
            # 负开太多: change_rate < -2%
            if change_rate is not None and change_rate < -2.0:
                filtered_out = True
                reason = f"负开太多: change_rate={change_rate}%"
            # 修复无量: turnover < median × 0.8
            elif (turnover is not None
                  and pool_median_turnover is not None
                  and turnover < pool_median_turnover * 0.8):
                filtered_out = True
                reason = f"修复无量: turnover={turnover}<median×0.8"

        elif pool_type == PoolType.POOL_FEIBAN:
            # 流动性陷阱: bid_amount < median × 0.3
            if (bid_amount is not None
                and pool_median_bid_amount is not None
                and bid_amount < pool_median_bid_amount * 0.3):
                filtered_out = True
                reason = f"流动性陷阱: bid={bid_amount:.0f}<median×0.3"
            # 高开无量诱多: change_rate > 7% and turnover < median × 0.8
            elif (change_rate is not None and change_rate > 7.0
                  and turnover is not None
                  and pool_median_turnover is not None
                  and turnover < pool_median_turnover * 0.8):
                filtered_out = True
                reason = f"高开无量诱多: change={change_rate}%, turnover={turnover}"

        if filtered_out:
            rk = RankedStock(
                code=rs.code, name=rs.name, pool=rs.pool,
                pool_label=rs.pool_label, routed=rs,
                filtered=True, filter_reason=reason
            )
            filtered.append(rk)
        else:
            passed.append(rs)

    return passed, filtered


# ============================================================================
# 各池排名逻辑
# ============================================================================

def _rank_pool_yizi(
    stocks: List[RoutedStock],
    pool_median_turnover: Optional[float],
) -> List[RankedStock]:
    """
    一字封池排名。

    两层排序:
    Layer 1 (有封单数据): seal_amount_ratio ↓ → seal_strength ↓ → change_rate 3%-7% 最优
    Layer 2 (无封单数据): change_rate 3%-7% 最优 → turnover_rate 中位数以上最优
    """
    with_seal: List[RankedStock] = []
    without_seal: List[RankedStock] = []

    for rs in stocks:
        feat = rs.feature or {}
        seal_ratio = _calc_seal_amount_ratio(feat, rs.review_free_float_mktcap)
        seal_strength = _calc_seal_strength(feat)
        change_rate = feat.get("changeRate")
        turnover = feat.get("turnoverRate")

        rk = RankedStock(
            code=rs.code, name=rs.name, pool=rs.pool,
            pool_label=rs.pool_label, routed=rs,
        )

        if seal_ratio is not None:
            # Layer 1: 有封单数据
            rk.score_primary = seal_ratio
            rk.score_secondary = seal_strength or 0.0
            rk.score_tertiary = _change_rate_score(change_rate, 3.0, 7.0)
            rk.diagnostics["seal_layer"] = 1
            rk.diagnostics["seal_amount_ratio"] = seal_ratio
            rk.diagnostics["seal_strength"] = seal_strength
            with_seal.append(rk)
        else:
            # Layer 2: 无封单数据
            rk.score_primary = _change_rate_score(change_rate, 3.0, 7.0)
            rk.score_secondary = _turnover_rate_score(turnover, pool_median_turnover, None)
            rk.score_tertiary = 0.0
            rk.diagnostics["seal_layer"] = 2
            without_seal.append(rk)

    # 排序: Layer 1 优先
    with_seal.sort(key=lambda r: (-(r.score_primary or -999), -(r.score_secondary or -999), -(r.score_tertiary or -999)))
    without_seal.sort(key=lambda r: (-(r.score_primary or -999), -(r.score_secondary or -999)))

    return with_seal + without_seal


def _rank_pool_huanshou(
    stocks: List[RoutedStock],
    pool_median_turnover: Optional[float],
    pool_std_turnover: Optional[float],
) -> List[RankedStock]:
    """
    换手封池排名。

    turnover_rate 中位数 ± 1σ 最优 → change_rate 4%-7% 最优 → net_amount 正向加分
    first_seal_time < 09:35 → +1 位
    """
    ranked: List[RankedStock] = []

    for rs in stocks:
        feat = rs.feature or {}
        turnover = feat.get("turnoverRate")
        change_rate = feat.get("changeRate")
        net_amount = feat.get("mainNetInflow")

        rk = RankedStock(
            code=rs.code, name=rs.name, pool=rs.pool,
            pool_label=rs.pool_label, routed=rs,
        )
        rk.score_primary = _turnover_rate_score(turnover, pool_median_turnover, pool_std_turnover)
        rk.score_secondary = _change_rate_score(change_rate, 4.0, 7.0)
        # net_amount: 正 > 负 > 无数据
        if net_amount is not None:
            rk.score_tertiary = 1.0 if net_amount > 0 else (-1.0 if net_amount < 0 else 0.0)
        else:
            rk.score_tertiary = -999.0  # 无数据排最后

        rk.diagnostics["first_seal_time"] = rs.first_seal_time
        ranked.append(rk)

    # 排序
    ranked.sort(key=lambda r: (-(r.score_primary or -999), -(r.score_secondary or -999), -(r.score_tertiary or -999)))

    # first_seal_time < 09:35 → 前移 1 位
    cutoff_seconds = 9 * 3600 + 35 * 60  # 09:35:00
    for i, rk in enumerate(ranked):
        if rk.routed and rk.routed.first_seal_time:
            seal_seconds = _parse_first_seal_seconds(rk.routed.first_seal_time)
            if seal_seconds is not None and seal_seconds <= cutoff_seconds:
                rk.diagnostics["early_seal_bonus"] = True
                # 前移 1 位（与前一位置换）
                if i > 0:
                    ranked[i], ranked[i - 1] = ranked[i - 1], ranked[i]

    return ranked


def _rank_pool_fenqi(
    stocks: List[RoutedStock],
    pool_median_turnover: Optional[float],
) -> List[RankedStock]:
    """
    分歧封池排名。

    change_rate 0%-6% 最优 → turnover_rate 降序 → volume_ratio 正向加分
    """
    ranked: List[RankedStock] = []

    for rs in stocks:
        feat = rs.feature or {}
        change_rate = feat.get("changeRate")
        turnover = feat.get("turnoverRate")
        volume_ratio = feat.get("volumeRatio")

        rk = RankedStock(
            code=rs.code, name=rs.name, pool=rs.pool,
            pool_label=rs.pool_label, routed=rs,
        )
        # 分歧封: change_rate 正向优先 (0%-6% 最优)
        # 负值降权, >6% 降权
        if change_rate is not None:
            if 0.0 <= change_rate <= 6.0:
                rk.score_primary = 100.0 - abs(change_rate - 3.0) * 5.0
            elif change_rate > 6.0:
                rk.score_primary = 70.0 - (change_rate - 6.0) * 10.0
            else:
                rk.score_primary = 50.0 + change_rate * 5.0  # 负值越低得分越低
        else:
            rk.score_primary = -999.0

        # 修复必须放量: turnover_rate 降序
        rk.score_secondary = turnover if turnover is not None else -999.0

        # volume_ratio: 封顶 3 倍, 大于 1 越好
        if volume_ratio is not None:
            vr = min(volume_ratio, 3.0)
            rk.score_tertiary = vr
        else:
            rk.score_tertiary = -999.0

        ranked.append(rk)

    ranked.sort(key=lambda r: (-(r.score_primary or -999), -(r.score_secondary or -999), -(r.score_tertiary or -999)))
    return ranked


def _rank_pool_feiban(
    stocks: List[RoutedStock],
    pool_median_turnover: Optional[float],
    pool_median_bid_amount: Optional[float],
) -> List[RankedStock]:
    """
    非板池排名。

    change_rate 3%-7% 最优 → volume_ratio 降序 → bid_amount 降序
    """
    ranked: List[RankedStock] = []

    for rs in stocks:
        feat = rs.feature or {}
        change_rate = feat.get("changeRate")
        volume_ratio = feat.get("volumeRatio")
        bid_amount = feat.get("bidAmount")

        rk = RankedStock(
            code=rs.code, name=rs.name, pool=rs.pool,
            pool_label=rs.pool_label, routed=rs,
        )
        rk.score_primary = _change_rate_score(change_rate, 3.0, 7.0)

        # volume_ratio: 封顶 3 倍, 越大越好
        if volume_ratio is not None:
            rk.score_secondary = min(volume_ratio, 3.0)
        else:
            rk.score_secondary = -999.0

        # bid_amount: 越大越好
        rk.score_tertiary = bid_amount if bid_amount is not None else -999.0

        ranked.append(rk)

    ranked.sort(key=lambda r: (-(r.score_primary or -999), -(r.score_secondary or -999), -(r.score_tertiary or -999)))
    return ranked


# ============================================================================
# Bonus 调制
# ============================================================================

def _apply_bonus(
    ranked: List[RankedStock],
    hot_rank_map: Dict[str, int],
    rocket_rank_map: Dict[str, int],
) -> List[RankedStock]:
    """
    对已排名的股票列表应用 Bonus 调制。
    每只股票最多前移 2 位。

    Bonus 规则:
    - hot_rank ≤ 50 → +1 (所有池)
    - rocket_rank ≤ 30 → +1 (非板/换手封)
    - grab_strength > 0 → +1 (非板)
    - net_amount > 0 → +1 (换手封/分歧封)
    """
    for i, rk in enumerate(ranked):
        bonus = 0
        feat = rk.routed.feature if rk.routed else {}

        # hot_rank: 所有池
        hot_rank = hot_rank_map.get(rk.code, 999)
        if hot_rank <= 50:
            bonus += 1
            rk.diagnostics["bonus_hot"] = True

        # rocket_rank: 非板/换手封
        if rk.pool in (PoolType.POOL_FEIBAN, PoolType.POOL_HUANSHOU):
            rocket_rank = rocket_rank_map.get(rk.code, 999)
            if rocket_rank <= 30:
                bonus += 1
                rk.diagnostics["bonus_rocket"] = True

        # grab_strength: 非板
        if rk.pool == PoolType.POOL_FEIBAN:
            grab = feat.get("grabStrength")
            if grab is not None and grab > 0:
                bonus += 1
                rk.diagnostics["bonus_grab"] = True

        # net_amount: 换手封/分歧封
        if rk.pool in (PoolType.POOL_HUANSHOU, PoolType.POOL_FENQI):
            net = feat.get("mainNetInflow")
            if net is not None and net > 0:
                bonus += 1
                rk.diagnostics["bonus_net"] = True

        # 最多 +2 位
        bonus = min(bonus, 2)
        rk.bonus_applied = bonus
        rk.original_rank = i + 1

        if bonus > 0 and i - bonus >= 0:
            # 前移: 将当前元素插入到前面
            ranked.pop(i)
            ranked.insert(i - bonus, rk)

    # 重新编号
    for i, rk in enumerate(ranked):
        if rk.original_rank == 0:
            rk.original_rank = i + 1
        rk.rank = i + 1

    return ranked


# ============================================================================
# 主编排函数
# ============================================================================

def build_rank_maps(
    features: List[Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    从特征列表中构建 hot_rank 和 rocket_rank 映射。
    注意: 这些排名数据来自 rank.rocket 和 rank.hot_stock_day 数据集，
    不是 feature table 的一部分。这里提供一个接口供 runner 使用。
    """
    hot_rank_map: Dict[str, int] = {}
    rocket_rank_map: Dict[str, int] = {}

    for feat in features:
        code = feat.get("code", "")
        if not code:
            continue
        # 如果 feature 中包含 hot_rank/rocket_rank 字段（由 runner 合并）
        if "hot_rank" in feat:
            hot_rank_map[code] = int(feat["hot_rank"])
        if "rocket_rank" in feat:
            rocket_rank_map[code] = int(feat["rocket_rank"])

    return hot_rank_map, rocket_rank_map


def rank_pool(
    stocks: List[RoutedStock],
    pool_type: PoolType,
    hot_rank_map: Optional[Dict[str, int]] = None,
    rocket_rank_map: Optional[Dict[str, int]] = None,
) -> PoolRankResult:
    """
    对单个池执行完整排名流程: 风控过滤 → 核心排名 → Bonus 调制 → Top 3。

    Args:
        stocks: 路由后的股票列表
        pool_type: 池类型
        hot_rank_map: code → hot_rank 映射
        rocket_rank_map: code → rocket_rank 映射

    Returns:
        PoolRankResult 包含全部排名和 Top 3
    """
    if hot_rank_map is None:
        hot_rank_map = {}
    if rocket_rank_map is None:
        rocket_rank_map = {}

    pool_labels = {
        PoolType.POOL_YIZI: "一字封",
        PoolType.POOL_HUANSHOU: "换手封",
        PoolType.POOL_FENQI: "分歧封",
        PoolType.POOL_FEIBAN: "非板",
    }

    result = PoolRankResult(
        pool_type=pool_type,
        pool_label=pool_labels.get(pool_type, "未知"),
    )

    if not stocks:
        return result

    # 计算池内统计量
    turnovers = [rs.feature.get("turnoverRate") for rs in stocks if rs.feature]
    bid_amounts = [rs.feature.get("bidAmount") for rs in stocks if rs.feature]
    result.pool_median_turnover = _pool_median(turnovers)
    result.pool_median_bid_amount = _pool_median(bid_amounts)

    # Step 1: 风控过滤
    passed, filtered = _apply_pool_risk_filter(
        stocks, pool_type,
        result.pool_median_turnover,
        result.pool_median_bid_amount,
    )
    result.filtered_out = filtered

    # Step 2: 核心排名
    if pool_type == PoolType.POOL_YIZI:
        ranked = _rank_pool_yizi(passed, result.pool_median_turnover)
    elif pool_type == PoolType.POOL_HUANSHOU:
        pool_std = _pool_std([rs.feature.get("turnoverRate") for rs in passed if rs.feature])
        ranked = _rank_pool_huanshou(passed, result.pool_median_turnover, pool_std)
    elif pool_type == PoolType.POOL_FENQI:
        ranked = _rank_pool_fenqi(passed, result.pool_median_turnover)
    elif pool_type == PoolType.POOL_FEIBAN:
        ranked = _rank_pool_feiban(passed, result.pool_median_turnover, result.pool_median_bid_amount)
    else:
        ranked = []

    # Step 3: Bonus 调制
    ranked = _apply_bonus(ranked, hot_rank_map, rocket_rank_map)

    result.candidates = ranked
    result.top_n = ranked[:3]  # Top 3

    result.diagnostics = {
        "n_total": len(stocks),
        "n_passed": len(passed),
        "n_filtered": len(filtered),
        "n_ranked": len(ranked),
        "n_top": len(result.top_n),
        "pool_median_turnover": result.pool_median_turnover,
        "pool_median_bid_amount": result.pool_median_bid_amount,
    }

    return result


def rank_all_pools(
    pools: Dict[PoolType, List[RoutedStock]],
    hot_rank_map: Optional[Dict[str, int]] = None,
    rocket_rank_map: Optional[Dict[str, int]] = None,
) -> Dict[PoolType, PoolRankResult]:
    """
    对所有四个池执行完整排名。

    Args:
        pools: {PoolType: [RoutedStock]} 路由后的分池股票
        hot_rank_map: code → hot_rank 映射
        rocket_rank_map: code → rocket_rank 映射

    Returns:
        {PoolType: PoolRankResult} 各池排名结果
    """
    results: Dict[PoolType, PoolRankResult] = {}
    for pool_type in [PoolType.POOL_YIZI, PoolType.POOL_HUANSHOU,
                       PoolType.POOL_FENQI, PoolType.POOL_FEIBAN]:
        stocks = pools.get(pool_type, [])
        results[pool_type] = rank_pool(stocks, pool_type, hot_rank_map, rocket_rank_map)
    return results


# ============================================================================
# 自检
# ============================================================================

def _self_test() -> bool:
    """自检: 验证各池排名逻辑"""

    # 构造测试数据
    stocks = [
        # 一字板+开板=0 → 一字封
        RoutedStock(code="000001", name="A", pool=PoolType.POOL_YIZI, pool_label="一字封",
                    feature={"changeRate": 5.0, "turnoverRate": 0.5, "bidAmount": 1e8,
                             "sealBid925": 5e8, "free_float_mktcap": 50e8}),
        # 一字封无封单
        RoutedStock(code="000002", name="B", pool=PoolType.POOL_YIZI, pool_label="一字封",
                    feature={"changeRate": 4.0, "turnoverRate": 0.3, "bidAmount": 5e7}),
        # 换手封
        RoutedStock(code="000003", name="C", pool=PoolType.POOL_HUANSHOU, pool_label="换手封",
                    feature={"changeRate": 5.5, "turnoverRate": 0.8, "bidAmount": 2e8,
                             "mainNetInflow": 1e7},
                    first_seal_time="09:32:00"),
        # 分歧封
        RoutedStock(code="000004", name="D", pool=PoolType.POOL_FENQI, pool_label="分歧封",
                    feature={"changeRate": 2.0, "turnoverRate": 1.2, "bidAmount": 3e8,
                             "volumeRatio": 2.5}),
        # 非板
        RoutedStock(code="000005", name="E", pool=PoolType.POOL_FEIBAN, pool_label="非板",
                    feature={"changeRate": 5.0, "turnoverRate": 0.6, "bidAmount": 4e8,
                             "volumeRatio": 3.0, "grabStrength": 5.0}),
        # 非板被过滤 (bid_amount too low)
        RoutedStock(code="000006", name="F", pool=PoolType.POOL_FEIBAN, pool_label="非板",
                    feature={"changeRate": 3.0, "turnoverRate": 0.2, "bidAmount": 1e6}),
    ]

    # 构建 hot/rocket 排名
    hot_map = {"000001": 30, "000003": 45}
    rocket_map = {"000005": 20}

    # 测试 1: 一字封池排名
    yizi = [s for s in stocks if s.pool == PoolType.POOL_YIZI]
    result_yizi = rank_pool(yizi, PoolType.POOL_YIZI, hot_map, rocket_map)
    assert len(result_yizi.candidates) == 2, f"Expected 2 candidates, got {len(result_yizi.candidates)}"
    # 有封单数据的应排前面
    assert result_yizi.top_n[0].code == "000001", f"Expected 000001 first, got {result_yizi.top_n[0].code}"

    # 测试 2: 换手封池排名
    huanshou = [s for s in stocks if s.pool == PoolType.POOL_HUANSHOU]
    result_huanshou = rank_pool(huanshou, PoolType.POOL_HUANSHOU, hot_map, rocket_map)
    assert len(result_huanshou.candidates) == 1

    # 测试 3: 分歧封池排名
    fenqi = [s for s in stocks if s.pool == PoolType.POOL_FENQI]
    result_fenqi = rank_pool(fenqi, PoolType.POOL_FENQI, hot_map, rocket_map)
    assert len(result_fenqi.candidates) == 1

    # 测试 4: 非板池排名 + 风控过滤
    feiban = [s for s in stocks if s.pool == PoolType.POOL_FEIBAN]
    result_feiban = rank_pool(feiban, PoolType.POOL_FEIBAN, hot_map, rocket_map)
    assert len(result_feiban.filtered_out) == 1, f"Expected 1 filtered, got {len(result_feiban.filtered_out)}"
    assert result_feiban.filtered_out[0].code == "000006", f"Expected 000006 filtered, got {result_feiban.filtered_out[0].code}"
    assert len(result_feiban.candidates) == 1, f"Expected 1 candidate, got {len(result_feiban.candidates)}"
    # 非板有 grab_strength bonus
    if result_feiban.candidates:
        assert result_feiban.candidates[0].bonus_applied >= 1, f"Expected bonus for grab_strength"

    # 测试 5: rank_all_pools
    from duanxianxia_v4_2_d7_router import route_all_stocks, build_review_plate_map
    all_results = rank_all_pools(
        {PoolType.POOL_YIZI: yizi, PoolType.POOL_HUANSHOU: huanshou,
         PoolType.POOL_FENQI: fenqi, PoolType.POOL_FEIBAN: feiban},
        hot_map, rocket_map
    )
    assert len(all_results) == 4

    # 测试 6: change_rate_score
    assert _change_rate_score(5.0, 3.0, 7.0) > _change_rate_score(2.0, 3.0, 7.0)
    assert _change_rate_score(5.0, 3.0, 7.0) > _change_rate_score(9.0, 3.0, 7.0)

    return True


_self_test()


if __name__ == "__main__":
    print("duanxianxia_v4_2_pool_ranker self-test: PASS")