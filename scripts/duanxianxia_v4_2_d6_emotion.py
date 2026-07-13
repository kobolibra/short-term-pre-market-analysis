#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_d6_emotion.py  --  v4.2 D6 两段式情绪周期模块

D6 是第一层（总指挥部），决定"今天做不做、做多少"。
采用两段式设计：T-1 盘后定计划 → T0 9:25 确认/否决。

数据来源:
  - review_daily (T-1 盘后): PBBX 晋级率 5 层分解 (pbbx_jinji)
  - qxlive 9:25 快照: ZTBX, LBBX, KQXY
  - 并集宽表统计: 竞价红盘率

输出:
  - 环境状态: NORMAL / WARNING / CRISIS
  - 总仓位上限
  - 结构优先级矩阵
  - 买点模式

设计文档: dimension-design-v4/dimension-design-v4.html §3
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# 枚举定义
# ============================================================================

class EmotionState(Enum):
    """情绪环境三级状态"""
    NORMAL = "NORMAL"      # 常态进攻
    WARNING = "WARNING"    # 预警防守
    CRISIS = "CRISIS"      # 危机收缩


class BuyMode(Enum):
    """买点模式"""
    AUCTION_AND_BOARD = "AUCTION_AND_BOARD"      # 竞价挂单 + 排板并行 (NORMAL)
    BOARD_ONLY = "BOARD_ONLY"                      # 排板/扫板为主 (WARNING)
    FENQI_LIGHT = "FENQI_LIGHT"                    # 仅分歧封轻仓试错 (CRISIS)


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class D6EmotionResult:
    """D6 情绪周期完整输出"""
    # 环境状态
    state: EmotionState
    state_label: str                          # "NORMAL" / "WARNING" / "CRISIS"

    # T-1 计划阶段指标
    jinji_mean: Optional[float] = None         # 晋级率均值 (PBBX_1_2 + PBBX_2_3)/2
    jinji_mean_pctile: Optional[float] = None  # 晋级率均值 60日分位

    # T0 确认阶段指标
    ztbx_925: Optional[float] = None           # ZTBX@9:25
    ztbx_pctile: Optional[float] = None        # ZTBX 60日分位
    red_rate: Optional[float] = None           # 竞价红盘率
    red_rate_pctile: Optional[float] = None    # 红盘率 60日分位

    # 危机指标明细
    crisis_1: bool = False                     # ZTBX < 20pct
    crisis_2: bool = False                     # 晋级率均值 < 15pct
    crisis_3: bool = False                     # 红盘率 < 25% 或 < 20pct
    crisis_count: int = 0

    # T0 确认闸门
    t0_downgraded: bool = False                # 是否被 T0 确认降级
    t0_downgrade_reason: str = ""              # 降级原因
    ztbx_collapse: bool = False                # ZTBX 塌方
    lbbx_collapse: bool = False                # LBBX 塌方
    kqxy_spike: bool = False                   # KQXY 飙升

    # 输出
    total_position_cap: float = 1.0            # 总仓位上限 (0.0~1.0)
    buy_mode: BuyMode = BuyMode.AUCTION_AND_BOARD

    # 结构优先级 (True=可参与, False=禁用)
    pool_yizi_enabled: bool = True
    pool_huanshou_enabled: bool = True
    pool_fenqi_enabled: bool = True
    pool_feiban_enabled: bool = True

    # 池级别仓位乘子
    pool_yizi_mult: float = 1.0
    pool_huanshou_mult: float = 1.0
    pool_fenqi_mult: float = 1.0
    pool_feiban_mult: float = 1.0

    # 诊断
    warnings: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 历史数据管理
# ============================================================================

@dataclass
class D6History:
    """D6 历史数据，用于计算滚动分位数"""
    ztbx_values: List[float] = field(default_factory=list)       # 近60日 ZTBX
    jinji_mean_values: List[float] = field(default_factory=list)  # 近60日 晋级率均值
    red_rate_values: List[float] = field(default_factory=list)    # 近60日 红盘率
    kqxy_values: List[float] = field(default_factory=list)        # 近60日 KQXY

    _WINDOW = 60  # 滚动窗口

    def add_day(self, ztbx: Optional[float], jinji_mean: Optional[float],
                red_rate: Optional[float], kqxy: Optional[float]) -> None:
        """记录一天的指标值"""
        if ztbx is not None:
            self.ztbx_values.append(ztbx)
        if jinji_mean is not None:
            self.jinji_mean_values.append(jinji_mean)
        if red_rate is not None:
            self.red_rate_values.append(red_rate)
        if kqxy is not None:
            self.kqxy_values.append(kqxy)

    def percentile(self, values: List[float], p: float) -> Optional[float]:
        """计算分位数（线性插值）"""
        if len(values) < 10:  # 数据不足时返回 None
            return None
        sorted_vals = sorted(values)
        k = (len(sorted_vals) - 1) * p
        f = int(k)
        c = k - f
        if f + 1 < len(sorted_vals):
            return sorted_vals[f] + c * (sorted_vals[f + 1] - sorted_vals[f])
        return sorted_vals[f]

    def ztbx_20pct(self) -> Optional[float]:
        return self.percentile(self.ztbx_values, 0.20)

    def jinji_15pct(self) -> Optional[float]:
        return self.percentile(self.jinji_mean_values, 0.15)

    def red_rate_20pct(self) -> Optional[float]:
        return self.percentile(self.red_rate_values, 0.20)

    def kqxy_80pct(self) -> Optional[float]:
        return self.percentile(self.kqxy_values, 0.80)


# ============================================================================
# 数据提取
# ============================================================================

def _extract_qxlive_metric(rows: List[Dict[str, Any]], metric_key: str) -> Optional[float]:
    """从 qxlive top_metrics 行中提取指定指标值"""
    for row in (rows or []):
        key = str(row.get("metric_key") or "").strip()
        label = str(row.get("metric_label") or row.get("指标名称") or "").strip()
        if key == metric_key or metric_key in label:
            for vk in ("raw_chart_tail_value", "raw_value", "value", "指标值"):
                if vk in row:
                    try:
                        v = row.get(vk)
                        if v in (None, "", "-"):
                            return None
                        return float(str(v).replace("%", "").replace("亿", "").replace(",", "").strip())
                    except (ValueError, TypeError):
                        continue
    return None


def _extract_review_daily_pbbx(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """
    从 review_daily 数据中提取 PBBX 5 层分解。
    返回: {"PBBX_1_2": ..., "PBBX_2_3": ..., "PBBX_3_4": ..., "PBBX_4P": ..., "PBBX_TOP": ...}
    """
    result: Dict[str, Optional[float]] = {
        "PBBX_1_2": None, "PBBX_2_3": None,
        "PBBX_3_4": None, "PBBX_4P": None, "PBBX_TOP": None
    }
    # review_daily 的 PBBX 数据来自 review.daily.top_metrics
    # 结构与 qxlive 类似，但 PBBX 包含分层数据
    # 也可能是 home.ztpool 中的晋级率数据
    for row in (rows or []):
        key = str(row.get("metric_key") or "").strip()
        label = str(row.get("metric_label") or row.get("指标名称") or "").strip()
        ladder = str(row.get("ladder_group") or row.get("分组名称") or "").strip()

        if "PBBX" in key or "晋级率" in label or "连板晋级率" in label:
            for vk in ("raw_chart_tail_value", "raw_value", "value", "指标值", "晋级率"):
                if vk in row:
                    try:
                        v = row.get(vk)
                        if v in (None, "", "-"):
                            continue
                        val = float(str(v).replace("%", "").strip())
                        # 根据 ladder_group 确定层级
                        if "1进2" in ladder or "1进2" in label:
                            result["PBBX_1_2"] = val
                        elif "2进3" in ladder or "2进3" in label:
                            result["PBBX_2_3"] = val
                        elif "3进4" in ladder or "3进4" in label:
                            result["PBBX_3_4"] = val
                        elif "4板" in ladder or "4板+" in ladder or "4进5" in ladder:
                            result["PBBX_4P"] = val
                        elif "TOP" in ladder or "总" in ladder:
                            result["PBBX_TOP"] = val
                        else:
                            # 如果没有分层信息，取第一个值作为 TOP
                            if result["PBBX_TOP"] is None:
                                result["PBBX_TOP"] = val
                    except (ValueError, TypeError):
                        continue
    return result


def _extract_review_daily_pbbx_from_ztpool(ztpool_rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """
    从 home.ztpool 数据中提取 PBBX 晋级率分层。
    ztpool 包含 ladder_group (分组名称) + promo_rate (晋级率) 字段。
    """
    # 注意: 代码中强制重命名为 pbbx_jinji, 与 qxlive 的 pbbx_volume 物理隔离
    result: Dict[str, Optional[float]] = {
        "PBBX_1_2": None, "PBBX_2_3": None,
        "PBBX_3_4": None, "PBBX_4P": None, "PBBX_TOP": None
    }
    for row in (ztpool_rows or []):
        ladder = str(row.get("ladder_group") or row.get("分组名称") or "").strip()
        promo = row.get("promo_rate") or row.get("晋级率")  # 回退到中文字段名
        if promo is None:
            continue
        try:
            val = float(promo)
        except (ValueError, TypeError):
            continue
        if "1进2" in ladder:
            result["PBBX_1_2"] = val
        elif "2进3" in ladder:
            result["PBBX_2_3"] = val
        elif "3进4" in ladder:
            result["PBBX_3_4"] = val
        elif "4板" in ladder or "4进5" in ladder:
            result["PBBX_4P"] = val
        elif "总" in ladder or "TOP" in ladder:
            result["PBBX_TOP"] = val
    return result


def _calc_red_rate(features: List[Dict[str, Any]]) -> Optional[float]:
    """从并集宽表计算竞价红盘率"""
    if not features:
        return None
    red = 0
    total = 0
    for feat in features:
        change = feat.get("changeRate")
        if change is not None:
            total += 1
            if change > 0:
                red += 1
    if total == 0:
        return None
    return red / total * 100.0


# ============================================================================
# 核心逻辑: 两段式情绪判定
# ============================================================================

def determine_emotion_state(
    # T-1 盘后数据 (review_daily)
    ztpool_t1: List[Dict[str, Any]],
    # T0 9:25 快照 (qxlive)
    qxlive_top_t0: List[Dict[str, Any]],
    qxlive_top_t1: List[Dict[str, Any]],       # T-1 qxlive (用于 ZTBX/LBBX 对比)
    # 并集宽表 (用于计算红盘率)
    features: List[Dict[str, Any]],
    # 历史数据 (用于滚动分位数)
    history: Optional[D6History] = None,
    # 静态阈值 (历史数据不足时的回退)
    static_thresholds: Optional[Dict[str, float]] = None,
) -> D6EmotionResult:
    """
    D6 两段式情绪判定:
    Stage 1 (T-1 计划): 基于 review_daily/ztpool 计算晋级率均值 → 初步状态
    Stage 2 (T0 确认): 基于 qxlive 9:25 快照确认/否决 → 最终状态

    Args:
        ztpool_t1: T-1 ztpool 数据 (home.ztpool rows), 含 PBBX 晋级率分层
        qxlive_top_t0: T0 qxlive 9:25 快照行
        qxlive_top_t1: T-1 qxlive 快照行 (用于 ZTBX/LBBX 对比)
        features: 并集宽表特征列表
        history: 近60日历史数据
        static_thresholds: 静态阈值 {"ztbx_20pct": ..., "jinji_15pct": ..., ...}

    Returns:
        D6EmotionResult 完整输出
    """
    result = D6EmotionResult(state=EmotionState.NORMAL, state_label="NORMAL")
    warnings: List[str] = []

    # ========================================================================
    # Stage 1: T-1 盘后 → 计划阶段
    # ========================================================================

    # 提取 PBBX 晋级率 (从 ztpool / review_daily)
    pbbx_jinji = _extract_review_daily_pbbx_from_ztpool(ztpool_t1)
    pbbx_1_2 = pbbx_jinji.get("PBBX_1_2")
    pbbx_2_3 = pbbx_jinji.get("PBBX_2_3")

    # 晋级率均值 = (1进2 + 2进3) / 2
    jinji_mean = None
    if pbbx_1_2 is not None and pbbx_2_3 is not None:
        jinji_mean = (pbbx_1_2 + pbbx_2_3) / 2.0
    elif pbbx_1_2 is not None:
        jinji_mean = pbbx_1_2
    elif pbbx_2_3 is not None:
        jinji_mean = pbbx_2_3
    result.jinji_mean = jinji_mean

    # ========================================================================
    # Stage 2: T0 9:25 → 确认/否决阶段
    # ========================================================================

    # 提取 qxlive 指标
    ztbx_925 = _extract_qxlive_metric(qxlive_top_t0, "ZTBX")
    lbbx_925 = _extract_qxlive_metric(qxlive_top_t0, "LBBX")
    kqxy_925 = _extract_qxlive_metric(qxlive_top_t0, "KQXY")
    ztbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "ZTBX")
    lbbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "LBBX")

    result.ztbx_925 = ztbx_925

    # 竞价红盘率
    red_rate = _calc_red_rate(features)
    result.red_rate = red_rate

    # ========================================================================
    # 危机判定 (使用滚动分位数或静态阈值)
    # ========================================================================

    if history and len(history.ztbx_values) >= 10:
        # 使用滚动分位数
        ztbx_20pct = history.ztbx_20pct()
        jinji_15pct = history.jinji_15pct()
        red_rate_20pct = history.red_rate_20pct()
        result.ztbx_pctile = _calc_pctile(history.ztbx_values, ztbx_925)
        result.jinji_mean_pctile = _calc_pctile(history.jinji_mean_values, jinji_mean)
        result.red_rate_pctile = _calc_pctile(history.red_rate_values, red_rate)
    else:
        # 使用静态阈值 (回退方案)
        defaults = static_thresholds or {}
        ztbx_20pct = defaults.get("ztbx_20pct", -2.0)
        jinji_15pct = defaults.get("jinji_15pct", 15.0)
        red_rate_20pct = defaults.get("red_rate_20pct", 25.0)
        warnings.append("历史数据不足(<10天), 使用静态阈值")

    # 三项危机指标
    crisis_1 = (ztbx_925 is not None and ztbx_20pct is not None and ztbx_925 < ztbx_20pct)
    crisis_2 = (jinji_mean is not None and jinji_15pct is not None and jinji_mean < jinji_15pct)
    crisis_3 = False
    if red_rate is not None:
        crisis_3 = (red_rate < 25.0) or (red_rate_20pct is not None and red_rate < red_rate_20pct)

    result.crisis_1 = crisis_1
    result.crisis_2 = crisis_2
    result.crisis_3 = crisis_3
    result.crisis_count = sum([crisis_1, crisis_2, crisis_3])

    # 判定环境状态
    if result.crisis_count >= 2:
        result.state = EmotionState.CRISIS
        result.state_label = "CRISIS"
    elif result.crisis_count == 1:
        result.state = EmotionState.WARNING
        result.state_label = "WARNING"
    else:
        result.state = EmotionState.NORMAL
        result.state_label = "NORMAL"

    # ========================================================================
    # T0 确认闸门: 计划 → 验证
    # ========================================================================

    # ZTBX 塌方: ZTBX@9:25 < -2% (绝对阈值), 或 ZTBX@9:25 < T-1 ZTBX × 0.5 (仅当 T-1 为正时)
    if ztbx_925 is not None:
        if ztbx_925 < -2.0 or (ztbx_t1 is not None and ztbx_t1 > 0 and ztbx_925 < ztbx_t1 * 0.5):
            result.ztbx_collapse = True
            if result.state == EmotionState.NORMAL:
                result.state = EmotionState.WARNING
                result.state_label = "WARNING"
                result.t0_downgraded = True
                result.t0_downgrade_reason = "ZTBX塌方: NORMAL→WARNING"
            elif result.state == EmotionState.WARNING:
                result.state = EmotionState.CRISIS
                result.state_label = "CRISIS"
                result.t0_downgraded = True
                result.t0_downgrade_reason = "ZTBX塌方: WARNING→CRISIS"

    # LBBX 塌方: LBBX@9:25 < T-1 LBBX × 0.5
    if lbbx_925 is not None and lbbx_t1 is not None:
        if lbbx_925 < lbbx_t1 * 0.5:
            result.lbbx_collapse = True
            # 一字封/换手封池降权 (仓位×0.5), 分歧封池不受影响
            result.pool_yizi_mult *= 0.5
            result.pool_huanshou_mult *= 0.5
            warnings.append("LBBX塌方: 一字封/换手封池降权×0.5")

    # KQXY 飙升: KQXY@9:25 > 60d 80pct
    if kqxy_925 is not None:
        if history and len(history.kqxy_values) >= 10:
            kqxy_80pct = history.kqxy_80pct()
            if kqxy_80pct is not None and kqxy_925 > kqxy_80pct:
                result.kqxy_spike = True
                # 全仓上限 × 0.7 (亏钱效应飙升 = 市场恐慌)
                warnings.append("KQXY飙升: 全仓上限×0.7")
        elif kqxy_925 > 30.0:  # 静态阈值
            result.kqxy_spike = True
            warnings.append("KQXY飙升(静态阈值>30): 全仓上限×0.7")

    # ========================================================================
    # 输出: 仓位上限 + 结构优先级 + 买点模式
    # ========================================================================

    if result.state == EmotionState.NORMAL:
        result.total_position_cap = 1.0
        result.buy_mode = BuyMode.AUCTION_AND_BOARD
        result.pool_yizi_enabled = True
        result.pool_huanshou_enabled = True
        result.pool_fenqi_enabled = True
        result.pool_feiban_enabled = True

    elif result.state == EmotionState.WARNING:
        result.total_position_cap = 0.6
        result.buy_mode = BuyMode.BOARD_ONLY
        result.pool_yizi_enabled = True
        result.pool_huanshou_enabled = True
        result.pool_fenqi_enabled = True   # WARNING 下分歧封优先级提升
        result.pool_feiban_enabled = True
        result.pool_yizi_mult *= 0.6
        result.pool_feiban_mult *= 0.6
        warnings.append("WARNING: 一字封/非板池降权×0.6, 分歧封优先级提升")

    elif result.state == EmotionState.CRISIS:
        result.total_position_cap = 0.2
        result.buy_mode = BuyMode.FENQI_LIGHT
        result.pool_yizi_enabled = False
        result.pool_huanshou_enabled = False
        result.pool_fenqi_enabled = True    # 仅分歧封可参与
        result.pool_feiban_enabled = False
        result.pool_fenqi_mult *= 0.3
        warnings.append("CRISIS: 仅分歧封轻仓试错(≤30%), 其余池硬禁用")

    # KQXY 飙升额外降仓
    if result.kqxy_spike:
        result.total_position_cap *= 0.7

    result.warnings = warnings
    result.diagnostics = {
        "jinji_mean": jinji_mean,
        "pbbx_jinji": pbbx_jinji,
        "ztbx_925": ztbx_925,
        "ztbx_t1": ztbx_t1,
        "lbbx_925": lbbx_925,
        "lbbx_t1": lbbx_t1,
        "kqxy_925": kqxy_925,
        "red_rate": red_rate,
        "crisis_1": crisis_1, "crisis_2": crisis_2, "crisis_3": crisis_3,
    }

    return result


def _calc_pctile(values: List[float], value: Optional[float]) -> Optional[float]:
    """计算 value 在 values 中的分位值"""
    if value is None or not values:
        return None
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    # 计算有多少个值小于等于 value
    count_le = sum(1 for v in sorted_vals if v <= value)
    return count_le / n


# ============================================================================
# 自检
# ============================================================================

def _self_test() -> bool:
    """自检: 验证 D6 情绪判定的基本逻辑"""
    # 构造测试数据
    ztpool_t1 = [
        {"ladder_group": "1进2", "promo_rate": 25.0},
        {"ladder_group": "2进3", "promo_rate": 20.0},
        {"ladder_group": "3进4", "promo_rate": 15.0},
        {"ladder_group": "4板+", "promo_rate": 10.0},
    ]
    qxlive_t0 = [
        {"metric_key": "ZTBX", "value": "2.5"},
        {"metric_key": "LBBX", "value": "3.0"},
        {"metric_key": "KQXY", "value": "5.0"},
    ]
    qxlive_t1 = [
        {"metric_key": "ZTBX", "value": "2.0"},
        {"metric_key": "LBBX", "value": "2.5"},
    ]
    features = [
        {"changeRate": 5.0}, {"changeRate": 3.0}, {"changeRate": -1.0},
        {"changeRate": 2.0}, {"changeRate": 6.0},
    ]

    # 测试 1: NORMAL 状态 (无危机)
    result = determine_emotion_state(
        ztpool_t1=ztpool_t1, qxlive_top_t0=qxlive_t0, qxlive_top_t1=qxlive_t1,
        features=features, history=None,
        static_thresholds={"ztbx_20pct": -3.0, "jinji_15pct": 10.0, "red_rate_20pct": 20.0}
    )
    assert result.state == EmotionState.NORMAL, f"Expected NORMAL, got {result.state}"
    assert result.total_position_cap == 1.0, f"Expected cap 1.0, got {result.total_position_cap}"
    assert result.jinji_mean == 22.5, f"Expected jinji_mean=22.5, got {result.jinji_mean}"
    assert result.red_rate == 80.0, f"Expected red_rate=80%, got {result.red_rate}"
    assert result.crisis_count == 0, f"Expected 0 crises, got {result.crisis_count}"

    # 测试 2: WARNING 状态 (1 crisis)
    result2 = determine_emotion_state(
        ztpool_t1=ztpool_t1, qxlive_top_t0=qxlive_t0, qxlive_top_t1=qxlive_t1,
        features=features, history=None,
        static_thresholds={"ztbx_20pct": 3.0, "jinji_15pct": 10.0, "red_rate_20pct": 20.0}
    )
    assert result2.state == EmotionState.WARNING, f"Expected WARNING, got {result2.state}"
    assert result2.crisis_1 is True, "ZTBX should be crisis"
    assert result2.total_position_cap == 0.6, f"Expected cap 0.6, got {result2.total_position_cap}"

    # 测试 3: ZTBX 塌方降级
    result3 = determine_emotion_state(
        ztpool_t1=ztpool_t1, qxlive_top_t0=[
            {"metric_key": "ZTBX", "value": "-3.0"},
            {"metric_key": "LBBX", "value": "1.0"},
        ], qxlive_top_t1=[
            {"metric_key": "ZTBX", "value": "2.0"},
            {"metric_key": "LBBX", "value": "2.0"},
        ],
        features=features, history=None,
        static_thresholds={"ztbx_20pct": -3.0, "jinji_15pct": 10.0, "red_rate_20pct": 20.0}
    )
    assert result3.ztbx_collapse is True, "ZTBX collapse should be detected"
    assert result3.t0_downgraded is True, "Should be downgraded"

    # 测试 4: CRISIS 状态
    result4 = determine_emotion_state(
        ztpool_t1=[{"ladder_group": "1进2", "promo_rate": 5.0}],
        qxlive_top_t0=[{"metric_key": "ZTBX", "value": "-5.0"}],
        qxlive_top_t1=[{"metric_key": "ZTBX", "value": "1.0"}],
        features=[{"changeRate": -3.0}, {"changeRate": -2.0}],
        history=None,
        static_thresholds={"ztbx_20pct": -1.0, "jinji_15pct": 15.0, "red_rate_20pct": 20.0}
    )
    assert result4.state == EmotionState.CRISIS, f"Expected CRISIS, got {result4.state}"
    assert result4.pool_yizi_enabled is False, "一字封 should be disabled in CRISIS"
    assert result4.pool_fenqi_enabled is True, "分歧封 should be enabled in CRISIS"

    return True


_self_test()


if __name__ == "__main__":
    print("duanxianxia_v4_2_d6_emotion self-test: PASS")