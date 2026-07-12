#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_d7_router.py  --  v4.2 D7 结构路由模块

D7 是第二层（分池主干），决定"做什么类型"。
路由主轴: 共识质量 = 涨停类型 + 开板次数。

四池定义:
  POOL_YIZI     一字封  — 一字板且开板=0（未经检验的共识）
  POOL_HUANSHOU  换手封  — 非一字板且开板≤1（经过检验的共识）
  POOL_FENQI     分歧封  — 开板≥2（被否定的共识）
  POOL_FEIBAN    非板    — 不在 review_plate 中（无共识）

高度调制: 板数越高仓位越低，不设硬否决。

设计文档: dimension-design-v4/dimension-design-v4.html §4
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# 枚举定义
# ============================================================================

class PoolType(Enum):
    """结构池类型"""
    POOL_YIZI = "一字封"           # 未经检验的共识
    POOL_HUANSHOU = "换手封"       # 经过检验的共识
    POOL_FENQI = "分歧封"          # 被否定的共识 / 逆转博弈
    POOL_FEIBAN = "非板"           # 无共识 / 从零到一


class LimitType(Enum):
    """涨停类型枚举（来自 review_plate.zt_type）"""
    YIZI = "一字板"
    T_ZI = "T字板"
    HUIFENG = "回封板"
    PUTONG = "普通板"
    UNKNOWN = "未知"


class RiskTag(Enum):
    """风险标签"""
    HIGH_LEVEL = "HIGH_LEVEL"              # 板数≥4
    HEAVY_DIVERGENCE = "HEAVY_DIVERGENCE"  # 开板≥3
    WEAK_SEAL = "WEAK_SEAL"                # fill_ratio > 1.2
    GAP_UP_WEAK = "GAP_UP_WEAK"            # 高开无量诱多


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class RoutedStock:
    """路由后的单只股票"""
    code: str
    name: str = ""

    # 池归属
    pool: PoolType = PoolType.POOL_FEIBAN
    pool_label: str = "非板"

    # D7 原始数据 (review_plate)
    limit_type: Optional[str] = None        # 涨停类型原始字符串
    limit_type_enum: LimitType = LimitType.UNKNOWN
    board_count_text: Optional[str] = None  # 板数文本 ("10天6板")
    board_height: int = 0                   # 解析后的板数
    streak: int = 0                         # 连板天数
    open_num: int = 0                       # 开板次数
    first_seal_time: Optional[str] = None   # 首次封板时间
    last_seal_time: Optional[str] = None    # 最后封板时间
    review_turnover_rate: Optional[float] = None  # 昨日换手率
    review_free_float_mktcap: Optional[float] = None  # 流通市值

    # 高度调制
    height_multiplier: float = 1.0          # 仓位乘子
    confirmation_threshold: str = "正常"     # 确认门槛

    # 风险标签
    risk_tags: List[RiskTag] = field(default_factory=list)

    # 特征数据引用 (T0)
    feature: Optional[Dict[str, Any]] = None

    # 诊断
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 辅助函数
# ============================================================================

# 涨停类型字符串 → 枚举映射
_LIMIT_TYPE_MAP: Dict[str, LimitType] = {
    "一字板": LimitType.YIZI,
    "T字板": LimitType.T_ZI,
    "回封板": LimitType.HUIFENG,
    "普通板": LimitType.PUTONG,
}


def _parse_limit_type(zt_type: Optional[str]) -> LimitType:
    """解析涨停类型字符串为枚举"""
    if zt_type is None:
        return LimitType.UNKNOWN
    return _LIMIT_TYPE_MAP.get(str(zt_type).strip(), LimitType.UNKNOWN)


def _parse_board_height(board_count_text: Optional[str]) -> int:
    """
    解析板数文本为高度数值。
    "10天6板" → 6
    "2天2板" → 2
    "5天4板" → 4
    "首板" → 1
    """
    if board_count_text is None:
        return 0
    text = str(board_count_text).strip()
    if not text:
        return 0

    # 匹配 "X天Y板" 模式
    m = re.search(r"(\d+)\s*天\s*(\d+)\s*板", text)
    if m:
        return int(m.group(2))

    # 匹配 "X板" 模式
    m = re.search(r"(\d+)\s*板", text)
    if m:
        return int(m.group(1))

    # 匹配 "首板"
    if "首板" in text:
        return 1

    return 0


def _parse_first_seal_seconds(first_seal_time: Optional[str]) -> Optional[int]:
    """将首次封板时间转换为秒数（用于比较早晚）"""
    if first_seal_time is None:
        return None
    text = str(first_seal_time).strip()
    m = re.match(r"(\d{1,2}):(\d{2}):(\d{2})", text)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
    return None


def _calc_fill_ratio(feature: Optional[Dict[str, Any]]) -> Optional[float]:
    """计算 fill_ratio = bidAmount / sealAmountRaw"""
    if feature is None:
        return None
    bid = feature.get("bidAmount")
    seal_raw = feature.get("sealAmountRaw")
    if bid is None or seal_raw in (None, 0):
        return None
    return bid / seal_raw


def _calc_seal_amount_ratio(
    feature: Optional[Dict[str, Any]],
    review_free_float_mktcap: Optional[float]
) -> Optional[float]:
    """
    计算 seal_amount_ratio = 封单金额 / 流通市值 × 100%
    封单金额优先取 fengdan sealBid925，其次取 weimai sealAmount。
    """
    if feature is None:
        return None
    seal = feature.get("sealBid925") or feature.get("sealAmount")
    ff = feature.get("free_float_mktcap") or review_free_float_mktcap
    if seal is None or ff in (None, 0):
        return None
    return seal / ff * 100.0


def _calc_seal_strength(feature: Optional[Dict[str, Any]]) -> Optional[float]:
    """计算 seal_strength = 封单金额 / 竞价成交额"""
    if feature is None:
        return None
    seal = feature.get("sealBid925") or feature.get("sealAmount")
    bid = feature.get("bidAmount")
    if seal is None or bid in (None, 0):
        return None
    return seal / bid


# ============================================================================
# 核心路由逻辑
# ============================================================================

def route_single_stock(
    code: str,
    feature: Optional[Dict[str, Any]],
    review_plate_row: Optional[Dict[str, Any]],
    pool_median_turnover: Optional[float] = None,
) -> RoutedStock:
    """
    对单只股票执行 D7 结构路由。

    Args:
        code: 股票代码
        feature: T0 竞价特征行 (来自 feature_builder)
        review_plate_row: T-1 review_plate 行 (来自 canonical)
        pool_median_turnover: 池内 turnover_rate 中位数 (用于 GAP_UP_WEAK 判定)

    Returns:
        RoutedStock 完整路由结果
    """
    rs = RoutedStock(code=code)
    rs.feature = feature

    if feature:
        rs.name = feature.get("name", "")

    # ========================================================================
    # Step 1: 查询是否在 review_plate 中
    # ========================================================================
    if review_plate_row is None:
        rs.pool = PoolType.POOL_FEIBAN
        rs.pool_label = "非板"
        rs.diagnostics["reason"] = "不在 review_plate 中"
        return rs

    # ========================================================================
    # Step 2: 提取 review_plate 字段
    # ========================================================================
    zt_type_str = review_plate_row.get("zt_type")
    rs.limit_type = zt_type_str
    rs.limit_type_enum = _parse_limit_type(zt_type_str)

    board_text = review_plate_row.get("board_count_text")
    rs.board_count_text = board_text
    rs.board_height = _parse_board_height(board_text)

    streak = review_plate_row.get("streak")
    rs.streak = int(streak) if streak is not None else 0

    open_num = review_plate_row.get("open_num")
    rs.open_num = int(open_num) if open_num is not None else 0

    rs.first_seal_time = review_plate_row.get("first_seal_time")
    rs.last_seal_time = review_plate_row.get("last_seal_time")
    rs.review_turnover_rate = review_plate_row.get("turnover_rate")
    rs.review_free_float_mktcap = review_plate_row.get("free_float_mktcap")

    # ========================================================================
    # Step 3: 共识质量判定 → 池归属
    # ========================================================================
    if rs.limit_type_enum == LimitType.YIZI and rs.open_num == 0:
        rs.pool = PoolType.POOL_YIZI
        rs.pool_label = "一字封"
        rs.diagnostics["consensus_quality"] = "未经检验"

    elif rs.limit_type_enum == LimitType.YIZI and rs.open_num >= 1:
        # 一字板但打开过 → 一字标签失效，本质是分歧
        rs.pool = PoolType.POOL_FENQI
        rs.pool_label = "分歧封"
        rs.diagnostics["consensus_quality"] = "一字标签失效→分歧"
        rs.diagnostics["reason"] = f"一字板但开板{rs.open_num}次→归入分歧封"

    elif rs.open_num >= 2:
        rs.pool = PoolType.POOL_FENQI
        rs.pool_label = "分歧封"
        rs.diagnostics["consensus_quality"] = "被否定"
        rs.diagnostics["reason"] = f"开板{rs.open_num}次→归入分歧封"

    elif rs.open_num <= 1:
        rs.pool = PoolType.POOL_HUANSHOU
        rs.pool_label = "换手封"
        if rs.limit_type_enum == LimitType.T_ZI:
            rs.diagnostics["consensus_quality"] = "轻度检验"
        else:
            rs.diagnostics["consensus_quality"] = "充分检验"

    else:
        # 不应该到达这里，但做安全回退
        rs.pool = PoolType.POOL_HUANSHOU
        rs.pool_label = "换手封"
        rs.diagnostics["consensus_quality"] = "未知→回退换手封"

    # ========================================================================
    # Step 4: 高度调制
    # ========================================================================
    height = rs.board_height
    if height >= 6:
        rs.height_multiplier = 0.3
        rs.confirmation_threshold = "极高"
    elif height >= 5:
        rs.height_multiplier = 0.5
        rs.confirmation_threshold = "高"
    elif height >= 4:
        rs.height_multiplier = 0.7
        rs.confirmation_threshold = "偏高"
    elif height >= 2:
        rs.height_multiplier = 0.85
        rs.confirmation_threshold = "正常"
    else:
        rs.height_multiplier = 1.0
        rs.confirmation_threshold = "正常"

    # ========================================================================
    # Step 5: 风险标签
    # ========================================================================
    if rs.board_height >= 4:
        rs.risk_tags.append(RiskTag.HIGH_LEVEL)

    if rs.open_num >= 3:
        rs.risk_tags.append(RiskTag.HEAVY_DIVERGENCE)

    # WEAK_SEAL: fill_ratio > 1.2
    fill_ratio = _calc_fill_ratio(feature)
    if fill_ratio is not None and fill_ratio > 1.2:
        rs.risk_tags.append(RiskTag.WEAK_SEAL)

    # GAP_UP_WEAK: change_rate > 8% 且 turnover < 池中位数 × 0.5
    if feature:
        change_rate = feature.get("changeRate")
        turnover = feature.get("turnoverRate")
        if (change_rate is not None and change_rate > 8.0
                and turnover is not None
                and pool_median_turnover is not None
                and turnover < pool_median_turnover * 0.5):
            rs.risk_tags.append(RiskTag.GAP_UP_WEAK)

    # 诊断信息
    rs.diagnostics.update({
        "board_height": height,
        "streak": rs.streak,
        "open_num": rs.open_num,
        "height_multiplier": rs.height_multiplier,
        "confirmation_threshold": rs.confirmation_threshold,
        "risk_tags": [t.value for t in rs.risk_tags],
        "fill_ratio": fill_ratio,
    })

    return rs


def route_all_stocks(
    features: List[Dict[str, Any]],
    review_plate_map: Dict[str, Dict[str, Any]],
) -> Dict[PoolType, List[RoutedStock]]:
    """
    对所有股票执行 D7 结构路由，返回按池分组的股票列表。

    Args:
        features: T0 竞价特征列表 (来自 feature_builder)
        review_plate_map: code → review_plate canonical row 的映射

    Returns:
        {PoolType: [RoutedStock, ...]} 四个池的股票列表
    """
    pools: Dict[PoolType, List[RoutedStock]] = {
        PoolType.POOL_YIZI: [],
        PoolType.POOL_HUANSHOU: [],
        PoolType.POOL_FENQI: [],
        PoolType.POOL_FEIBAN: [],
    }

    for feat in features:
        code = feat.get("code", "")
        if not code:
            continue

        rp_row = review_plate_map.get(code)

        # 计算池内 turnover 中位数 (用于 GAP_UP_WEAK 判定)
        # 这里先不计算，在 router 中设为 None，后续由 ranker 补充
        rs = route_single_stock(code, feat, rp_row, pool_median_turnover=None)
        pools[rs.pool].append(rs)

    return pools


def build_review_plate_map(
    fupan_t1: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """
    将 review_plate 数据转换为 code → row 映射。

    Args:
        fupan_t1: T-1 review_plate 数据行列表 (canonical 格式)

    Returns:
        code → row 的字典
    """
    plate_map: Dict[str, Dict[str, Any]] = {}
    for row in (fupan_t1 or []):
        code = str(row.get("code", "")).strip().zfill(6)
        if code:
            plate_map[code] = row
    return plate_map


# ============================================================================
# 自检
# ============================================================================

def _self_test() -> bool:
    """自检: 验证 D7 路由逻辑"""

    # 测试板数解析
    assert _parse_board_height("10天6板") == 6
    assert _parse_board_height("2天2板") == 2
    assert _parse_board_height("5天4板") == 4
    assert _parse_board_height("首板") == 1
    assert _parse_board_height(None) == 0
    assert _parse_board_height("") == 0

    # 测试涨停类型解析
    assert _parse_limit_type("一字板") == LimitType.YIZI
    assert _parse_limit_type("T字板") == LimitType.T_ZI
    assert _parse_limit_type("回封板") == LimitType.HUIFENG
    assert _parse_limit_type("普通板") == LimitType.PUTONG
    assert _parse_limit_type(None) == LimitType.UNKNOWN

    # 测试封板时间解析
    assert _parse_first_seal_seconds("09:35:00") == 9 * 3600 + 35 * 60
    assert _parse_first_seal_seconds("10:09:26") == 10 * 3600 + 9 * 60 + 26
    assert _parse_first_seal_seconds(None) is None

    # 测试路由: 一字板+开板=0 → 一字封
    rp1 = {"zt_type": "一字板", "board_count_text": "2天2板", "streak": 2, "open_num": 0}
    feat1 = {"code": "000001", "name": "测试", "bidAmount": 1e8, "sealAmountRaw": 2e8,
             "sealBid925": 3e8, "changeRate": 5.0, "turnoverRate": 0.5}
    rs1 = route_single_stock("000001", feat1, rp1)
    assert rs1.pool == PoolType.POOL_YIZI, f"Expected 一字封, got {rs1.pool}"
    assert rs1.board_height == 2
    assert rs1.height_multiplier == 0.85

    # 测试路由: 一字板+开板=1 → 分歧封 (一字标签失效)
    rp2 = {"zt_type": "一字板", "board_count_text": "3天3板", "streak": 3, "open_num": 1}
    rs2 = route_single_stock("000002", feat1, rp2)
    assert rs2.pool == PoolType.POOL_FENQI, f"Expected 分歧封, got {rs2.pool}, diag={rs2.diagnostics}"

    # 测试路由: 开板≥2 → 分歧封
    rp3 = {"zt_type": "普通板", "board_count_text": "2天2板", "streak": 2, "open_num": 3}
    rs3 = route_single_stock("000003", feat1, rp3)
    assert rs3.pool == PoolType.POOL_FENQI, f"Expected 分歧封, got {rs3.pool}"
    assert RiskTag.HEAVY_DIVERGENCE in rs3.risk_tags

    # 测试路由: 非板
    rs4 = route_single_stock("000004", feat1, None)
    assert rs4.pool == PoolType.POOL_FEIBAN, f"Expected 非板, got {rs4.pool}"

    # 测试路由: 换手封 (T字板+开板≤1)
    rp5 = {"zt_type": "T字板", "board_count_text": "首板", "streak": 1, "open_num": 1}
    rs5 = route_single_stock("000005", feat1, rp5)
    assert rs5.pool == PoolType.POOL_HUANSHOU, f"Expected 换手封, got {rs5.pool}"

    # 测试高度调制: 6板+
    rp6 = {"zt_type": "普通板", "board_count_text": "10天6板", "streak": 2, "open_num": 0}
    rs6 = route_single_stock("000006", feat1, rp6)
    assert rs6.height_multiplier == 0.3, f"Expected 0.3, got {rs6.height_multiplier}"
    assert RiskTag.HIGH_LEVEL in rs6.risk_tags

    # 测试 WEAK_SEAL: fill_ratio > 1.2
    feat_weak = {"code": "000007", "bidAmount": 2e8, "sealAmountRaw": 1e8}  # fill=2.0
    rp7 = {"zt_type": "普通板", "board_count_text": "首板", "streak": 1, "open_num": 0}
    rs7 = route_single_stock("000007", feat_weak, rp7)
    assert RiskTag.WEAK_SEAL in rs7.risk_tags, f"Expected WEAK_SEAL, got {rs7.risk_tags}"

    # 测试 build_review_plate_map
    fupan = [
        {"code": "000001", "zt_type": "一字板"},
        {"code": "000002", "zt_type": "普通板"},
    ]
    plate_map = build_review_plate_map(fupan)
    assert len(plate_map) == 2
    assert "000001" in plate_map

    # 测试 route_all_stocks
    features = [
        {"code": "000001", "name": "A", "changeRate": 5.0},
        {"code": "000002", "name": "B", "changeRate": 3.0},
        {"code": "000003", "name": "C", "changeRate": 7.0},
    ]
    rp_full = {
        "000001": {"zt_type": "一字板", "board_count_text": "2天2板", "streak": 2, "open_num": 0},
        "000002": {"zt_type": "普通板", "board_count_text": "首板", "streak": 1, "open_num": 0},
    }
    pools = route_all_stocks(features, rp_full)
    assert len(pools[PoolType.POOL_YIZI]) == 1
    assert len(pools[PoolType.POOL_HUANSHOU]) == 1
    assert len(pools[PoolType.POOL_FEIBAN]) == 1

    return True


_self_test()


if __name__ == "__main__":
    print("duanxianxia_v4_2_d7_router self-test: PASS")