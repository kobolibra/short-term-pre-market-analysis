#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_d6_emotion.py  --  v4.2 D6 情绪周期状态机

============================================================================
设计哲学
============================================================================
D6 不只是风险预算层。D6 必须首先识别市场所处的情绪周期位置，再由周期位置
派生风险预算、进攻方向和结构偏好。

核心公式:  水位(Level) × 方向(Direction) → 九宫格周期相位(Phase)
          Phase + T0冲击 → 风险等级(RiskTier) + 仓位上限 + 结构偏好

============================================================================
三大家族
============================================================================
1. 强势股兑现(profit):  ZTBX + LBBX 分位取中位数
2. 市场广度(breadth):   advance_share + (1-DT分位) 取中位数
3. 接力生态(relay):   relay_health = 0.55×1进2 + 0.45×2进3, 分位

总水位 = weighted_median(profit, breadth, relay×2)
  → relay 2x 权重: 它是短线最核心信号(有没有人接盘)
方向 = 每个家族近3-5日稳健斜率取中位数, 至少2个家族同向

滞回区间: LOW→MID 需 0.40, MID→LOW 只需 0.30; HIGH→MID 需 0.60, MID→HIGH 只需 0.70

============================================================================
3×3 九宫格
============================================================================
              UP           FLAT          DOWN
HIGH   高潮加速      高位钝化      退潮初期
MID    发酵主升      震荡混沌      退潮扩散
LOW    冰点修复      冰点磨底      冰点下杀

============================================================================
接力健康度
============================================================================
单一指标 relay_health 贯穿全链路(展示/水位/方向统一口径):
  relay_health = 0.55 × smoothed_rate(1进2) + 0.45 × smoothed_rate(2进3)

1进2权重 0.55: 样本多(15-50只)/信号稳定/代表新接力形成
2进3权重 0.45: 接力持续确认, 是二阶信号

3进4以上样本太小(3-8只), 分位数无统计意义, 不纳入计算。
Laplace平滑: smoothed_rate = (promoted + 1) / (eligible + 2) × 100

============================================================================
状态迁移
============================================================================
慢升级, 快降级, 滞回区间防抖。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
# 枚举定义
# ============================================================================

class EmotionLevel(Enum):
    LOW = "LOW"
    MID = "MID"
    HIGH = "HIGH"

class EmotionDirection(Enum):
    UP = "UP"
    FLAT = "FLAT"
    DOWN = "DOWN"
    UNKNOWN = "UNKNOWN"

class EmotionPhase(Enum):
    # 低位
    ICE_FALL = "ICE_FALL"           # 冰点下杀: LOW+DOWN
    ICE_BASE = "ICE_BASE"           # 冰点磨底: LOW+FLAT
    REPAIR = "REPAIR"               # 冰点修复: LOW+UP
    # 中位
    CHOP = "CHOP"                   # 震荡混沌: MID+FLAT
    RETREAT = "RETREAT"             # 退潮扩散: MID+DOWN
    EXPANSION = "EXPANSION"         # 发酵主升: MID+UP
    # 高位
    RETREAT_EARLY = "RETREAT_EARLY" # 退潮初期: HIGH+DOWN
    HIGH_STAGNATION = "HIGH_STAGNATION"  # 高位钝化: HIGH+FLAT
    CLIMAX_ACCEL = "CLIMAX_ACCEL"   # 高潮加速: HIGH+UP
    # 数据不足
    UNKNOWN = "UNKNOWN"

class RiskTier(Enum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRISIS = "CRISIS"

class BuyMode(Enum):
    AUCTION_AND_BOARD = "auction_and_board"       # 竞价+排板
    BOARD_ONLY = "board_only"                     # 排板为主
    FENQI_ONLY = "fenqi_only"                     # 仅分歧轻仓
    OBSERVE_ONLY = "observe_only"                 # 仅观察
    EMPTY = "empty"                                # 空仓

class DataQuality(Enum):
    VALID = "VALID"
    DEGRADED = "DEGRADED"
    MISSING = "MISSING"

# ============================================================================
# 数据类
# ============================================================================

@dataclass
class FamilyScore:
    """单个信息家族的评分"""
    level: float          # 0-1 水位分位
    slope: Optional[float] = None  # 近3-5日中位数变化
    valid: bool = True

@dataclass
class D6History:
    """D6 历史数据，用于计算滚动分位数和斜率"""
    ztbx_values: List[float] = field(default_factory=list)
    lbbx_values: List[float] = field(default_factory=list)
    advance_share_values: List[float] = field(default_factory=list)
    dt_values: List[float] = field(default_factory=list)
    relay_health_values: List[float] = field(default_factory=list)

    _WINDOW = 60

    def add_day(self, ztbx: Optional[float] = None, lbbx: Optional[float] = None,
                advance_share: Optional[float] = None, dt: Optional[float] = None,
                relay_health: Optional[float] = None) -> None:
        if ztbx is not None: self.ztbx_values.append(ztbx)
        if lbbx is not None: self.lbbx_values.append(lbbx)
        if advance_share is not None: self.advance_share_values.append(advance_share)
        if dt is not None: self.dt_values.append(dt)
        if relay_health is not None: self.relay_health_values.append(relay_health)

    def _pctile(self, values: List[float], q: float) -> Optional[float]:
        if len(values) < 5:
            return None
        sv = sorted(values)
        idx = int(len(sv) * q)
        return sv[min(idx, len(sv) - 1)]

    def percentile(self, values: List[float], q: float) -> Optional[float]:
        return self._pctile(values, q)

    def ztbx_20pct(self) -> Optional[float]: return self._pctile(self.ztbx_values, 0.20)
    def ztbx_35pct(self) -> Optional[float]: return self._pctile(self.ztbx_values, 0.35)
    def lbbx_20pct(self) -> Optional[float]: return self._pctile(self.lbbx_values, 0.20)
    def advance_share_20pct(self) -> Optional[float]: return self._pctile(self.advance_share_values, 0.20)
    def dt_80pct(self) -> Optional[float]: return self._pctile(self.dt_values, 0.80)
    def relay_health_15pct(self) -> Optional[float]: return self._pctile(self.relay_health_values, 0.15)

    def robust_slope(self, values: List[float], n: int = 3) -> Optional[float]:
        """稳健斜率: 最近n日变化的中位数"""
        if len(values) < n + 1:
            return None
        diffs = [values[-1] - values[-2], values[-2] - values[-3], values[-3] - values[-4]]
        if len(values) >= 5:
            diffs.append(values[-4] - values[-5])
        diffs_sorted = sorted(diffs)
        mid = len(diffs_sorted) // 2
        return diffs_sorted[mid]

    @property
    def min_days(self) -> int:
        return min(len(self.ztbx_values), len(self.advance_share_values),
                   len(self.relay_health_values))

@dataclass
class D6EmotionResult:
    """D6 情绪周期完整输出"""
    # === 周期主状态 ===
    phase: EmotionPhase = EmotionPhase.UNKNOWN
    phase_label: str = "数据不足"

    # === 2D定位 ===
    level: EmotionLevel = EmotionLevel.MID
    level_score: float = 0.5
    direction: EmotionDirection = EmotionDirection.UNKNOWN
    momentum_score: float = 0.0

    # === 三个家族评分 ===
    profit_level: float = 0.5
    breadth_level: float = 0.5
    relay_level: float = 0.5
    profit_slope: Optional[float] = None
    breadth_slope: Optional[float] = None
    relay_slope: Optional[float] = None

    # === T0 冲击 ===
    t0_impulse: str = "NEUTRAL"           # POSITIVE / NEUTRAL / NEGATIVE
    ztbx_collapse: bool = False
    lbbx_collapse: bool = False
    breadth_shock: bool = False

    # === 风险 ===
    risk_tier: RiskTier = RiskTier.NORMAL
    position_cap: float = 1.0

    # === 结构偏好 ===
    height_preference: str = "MID"        # LOW / MID / HIGH
    fenqi_priority: str = "NORMAL"        # HIGH / NORMAL / LOW / DISABLED
    yizi_enabled: bool = True
    huanshou_enabled: bool = True
    fenqi_enabled: bool = True
    feiban_enabled: bool = True

    # === 池乘子(LBBX塌方时降权) ===
    pool_yizi_mult: float = 1.0
    pool_huanshou_mult: float = 1.0
    pool_fenqi_mult: float = 1.0
    pool_feiban_mult: float = 1.0

    # === 执行 ===
    buy_mode: BuyMode = BuyMode.AUCTION_AND_BOARD
    auction_buy_enabled: bool = True

    # === 质量 ===
    phase_confidence: float = 0.0
    data_quality: Dict[str, str] = field(default_factory=lambda: {
        "profit_family": "MISSING",
        "breadth_family": "MISSING",
        "relay_family": "MISSING",
    })
    transition_from: str = ""
    transition_reason: List[str] = field(default_factory=list)

    # === 原始指标 ===
    ztbx_925: Optional[float] = None
    lbbx_925: Optional[float] = None
    advance_share: Optional[float] = None
    dt_925: Optional[int] = None
    jinji_1_2: Optional[float] = None
    jinji_2_3: Optional[float] = None
    relay_health: Optional[float] = None

    # === 诊断 ===
    warnings: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 常数
# ============================================================================

# 水位阈值
LEVEL_LOW_THRESHOLD = 0.30
LEVEL_HIGH_THRESHOLD = 0.70
# 滞回区间
LEVEL_LOW_EXIT = 0.40
LEVEL_HIGH_EXIT = 0.60

# 方向死区
DIRECTION_DEADBAND = 0.03

# 晋级率收缩估计
ALPHA = 1.0
BETA = 1.0

# 接力健康度权重: 1进2(样本多信号稳) 0.55 + 2进3(接力持续) 0.45
RELAY_WEIGHT_1_2 = 0.55
RELAY_WEIGHT_2_3 = 0.45

# 静态阈值(历史不足时回退)
STATIC_DEFAULTS = {
    "ztbx_20pct": -2.0,
    "advance_share_20pct": 0.25,
    "dt_80pct": 15.0,
    "relay_health_15pct": 15.0,
}

# 周期相位→风险预算映射
PHASE_RISK_BUDGET = {
    EmotionPhase.ICE_FALL:          (RiskTier.CRISIS,  0.00, BuyMode.EMPTY),
    EmotionPhase.ICE_BASE:          (RiskTier.WARNING, 0.10, BuyMode.OBSERVE_ONLY),
    EmotionPhase.REPAIR:            (RiskTier.WARNING, 0.35, BuyMode.BOARD_ONLY),
    EmotionPhase.EXPANSION:         (RiskTier.NORMAL,  0.80, BuyMode.AUCTION_AND_BOARD),
    EmotionPhase.CLIMAX_ACCEL:      (RiskTier.WARNING, 0.50, BuyMode.BOARD_ONLY),
    EmotionPhase.HIGH_STAGNATION:   (RiskTier.WARNING, 0.30, BuyMode.BOARD_ONLY),
    EmotionPhase.RETREAT_EARLY:     (RiskTier.CRISIS,  0.15, BuyMode.FENQI_ONLY),
    EmotionPhase.RETREAT:           (RiskTier.CRISIS,  0.05, BuyMode.OBSERVE_ONLY),
    EmotionPhase.CHOP:              (RiskTier.WARNING, 0.30, BuyMode.BOARD_ONLY),
    EmotionPhase.UNKNOWN:           (RiskTier.WARNING, 0.10, BuyMode.OBSERVE_ONLY),
}

# 周期相位→结构偏好
PHASE_STRUCTURE = {
    EmotionPhase.ICE_FALL:          {"height": "LOW", "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": False, "feiban": False},
    EmotionPhase.ICE_BASE:          {"height": "LOW", "fenqi": "LOW", "yizi": False, "huanshou": False, "fenqi_enabled": True, "feiban": False},
    EmotionPhase.REPAIR:            {"height": "LOW", "fenqi": "HIGH", "yizi": False, "huanshou": True, "fenqi_enabled": True, "feiban": True},
    EmotionPhase.EXPANSION:         {"height": "MID", "fenqi": "NORMAL", "yizi": True, "huanshou": True, "fenqi_enabled": True, "feiban": True},
    EmotionPhase.CLIMAX_ACCEL:      {"height": "HIGH", "fenqi": "LOW", "yizi": True, "huanshou": True, "fenqi_enabled": True, "feiban": False},
    EmotionPhase.HIGH_STAGNATION:   {"height": "HIGH", "fenqi": "LOW", "yizi": True, "huanshou": True, "fenqi_enabled": False, "feiban": False},
    EmotionPhase.RETREAT_EARLY:     {"height": "LOW", "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": True, "feiban": False},
    EmotionPhase.RETREAT:           {"height": "LOW", "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": False, "feiban": False},
    EmotionPhase.CHOP:              {"height": "MID", "fenqi": "NORMAL", "yizi": False, "huanshou": True, "fenqi_enabled": True, "feiban": True},
    EmotionPhase.UNKNOWN:           {"height": "LOW", "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": False, "feiban": False},
}

# 状态迁移: 允许的升级路径
ALLOWED_UPGRADE = {
    EmotionPhase.ICE_FALL:          {EmotionPhase.ICE_BASE},
    EmotionPhase.ICE_BASE:          {EmotionPhase.REPAIR},
    EmotionPhase.REPAIR:            {EmotionPhase.EXPANSION},
    EmotionPhase.CHOP:              {EmotionPhase.EXPANSION},
    EmotionPhase.HIGH_STAGNATION:   {EmotionPhase.CLIMAX_ACCEL},
    EmotionPhase.UNKNOWN:           {EmotionPhase.ICE_BASE, EmotionPhase.REPAIR, EmotionPhase.EXPANSION,
                                     EmotionPhase.CHOP, EmotionPhase.HIGH_STAGNATION, EmotionPhase.CLIMAX_ACCEL,
                                     EmotionPhase.RETREAT, EmotionPhase.RETREAT_EARLY, EmotionPhase.ICE_FALL},
}

# 状态迁移: 允许的降级路径
ALLOWED_DOWNGRADE = {
    EmotionPhase.CLIMAX_ACCEL:      {EmotionPhase.RETREAT_EARLY, EmotionPhase.HIGH_STAGNATION},
    EmotionPhase.EXPANSION:         {EmotionPhase.RETREAT, EmotionPhase.CHOP},
    EmotionPhase.REPAIR:            {EmotionPhase.ICE_FALL, EmotionPhase.ICE_BASE},
    EmotionPhase.HIGH_STAGNATION:   {EmotionPhase.RETREAT_EARLY},
    EmotionPhase.RETREAT_EARLY:     {EmotionPhase.RETREAT},
    EmotionPhase.CHOP:              {EmotionPhase.RETREAT},
    EmotionPhase.ICE_BASE:          {EmotionPhase.ICE_FALL},
    EmotionPhase.UNKNOWN:           {EmotionPhase.ICE_FALL, EmotionPhase.ICE_BASE, EmotionPhase.REPAIR,
                                     EmotionPhase.EXPANSION, EmotionPhase.CHOP, EmotionPhase.HIGH_STAGNATION,
                                     EmotionPhase.CLIMAX_ACCEL, EmotionPhase.RETREAT, EmotionPhase.RETREAT_EARLY},
}

# 相位显示名
PHASE_LABELS = {
    EmotionPhase.ICE_FALL:          "冰点下杀",
    EmotionPhase.ICE_BASE:          "冰点磨底",
    EmotionPhase.REPAIR:            "冰点修复",
    EmotionPhase.EXPANSION:         "发酵主升",
    EmotionPhase.CLIMAX_ACCEL:      "高潮加速",
    EmotionPhase.HIGH_STAGNATION:   "高位钝化",
    EmotionPhase.RETREAT_EARLY:     "退潮初期",
    EmotionPhase.RETREAT:           "退潮扩散",
    EmotionPhase.CHOP:              "震荡混沌",
    EmotionPhase.UNKNOWN:           "数据不足",
}


# ============================================================================
# 核心逻辑
# ============================================================================

def _extract_ztpool_pbbx(ztpool_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    从 home.ztpool 数据中提取 PBBX 晋级率(1进2 + 2进3)。
    只提取样本量足够的两层, 3进4以上样本太小无统计意义。
    ztpool rows 每行含: 分组名称, 晋级率, 晋级数, 样本数
    """
    result: Dict[str, Dict[str, Any]] = {}
    seen: set = set()

    for row in (ztpool_rows or []):
        ladder = str(row.get("ladder_group") or row.get("分组名称") or "").strip()
        if not ladder or ladder in seen:
            continue
        seen.add(ladder)

        # 晋级率
        promo = row.get("promo_rate") or row.get("晋级率")
        rate = None
        if promo is not None:
            try:
                rate = float(str(promo).replace("%", "").strip())
            except (ValueError, TypeError):
                pass

        # 分子分母
        promoted = row.get("promoted_count") or row.get("晋级数")
        eligible = row.get("eligible_count") or row.get("样本数")
        prom_count = None
        elig_count = None
        if promoted is not None:
            try:
                prom_count = int(float(str(promoted).replace("%", "").strip()))
            except (ValueError, TypeError):
                pass
        if eligible is not None:
            try:
                elig_count = int(float(str(eligible).replace("%", "").strip()))
            except (ValueError, TypeError):
                pass

        key = None
        if "1进2" in ladder: key = "PBBX_1_2"
        elif "2进3" in ladder: key = "PBBX_2_3"

        if key:
            result[key] = {
                "rate": rate,
                "promoted": prom_count,
                "eligible": elig_count,
                "ladder": ladder,
            }

    return result


def _smoothed_rate(promoted: Optional[int], eligible: Optional[int],
                   alpha: float = ALPHA, beta: float = BETA) -> Optional[float]:
    """收缩估计: (promoted + alpha) / (eligible + alpha + beta)"""
    if eligible is None or eligible <= 0:
        return None
    p = promoted if promoted is not None else 0
    return round((p + alpha) / (eligible + alpha + beta) * 100, 2)


def _extract_qxlive_metric(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
    """从 qxlive top_metrics 中提取指标值"""
    for row in (rows or []):
        mk = str(row.get("metric_key") or "").strip()
        if mk == key:
            for vk in ("raw_chart_tail_value", "raw_value", "value"):
                if vk in row and row[vk] not in (None, "", "-"):
                    try:
                        return float(str(row[vk]).replace("%", "").replace("亿", "").replace(",", "").strip())
                    except (ValueError, TypeError):
                        continue
    return None


def _calc_pctile(values: List[float], value: Optional[float]) -> Optional[float]:
    """计算给定值在序列中的分位数"""
    if value is None or not values:
        return None
    sv = sorted(values)
    n = len(sv)
    rank = sum(1 for v in sv if v <= value)
    return round(rank / n, 4)


def _classify_level(score: float, prev_level: Optional[EmotionLevel] = None) -> EmotionLevel:
    """水位分类(含滞回区间)"""
    if prev_level == EmotionLevel.LOW:
        threshold = LEVEL_LOW_EXIT
    else:
        threshold = LEVEL_LOW_THRESHOLD

    if prev_level == EmotionLevel.HIGH:
        exit_threshold = LEVEL_HIGH_EXIT
    else:
        exit_threshold = LEVEL_HIGH_THRESHOLD

    if score < threshold:
        return EmotionLevel.LOW
    elif score > exit_threshold:
        return EmotionLevel.HIGH
    else:
        return EmotionLevel.MID


def _classify_direction(slope: float, deadband: float = DIRECTION_DEADBAND) -> EmotionDirection:
    """方向分类"""
    if abs(slope) < deadband:
        return EmotionDirection.FLAT
    elif slope > 0:
        return EmotionDirection.UP
    else:
        return EmotionDirection.DOWN


def _phase_to_level(phase: Optional[EmotionPhase]) -> Optional[EmotionLevel]:
    """从相位反推水位, 用于滞回区间"""
    if phase is None or phase == EmotionPhase.UNKNOWN:
        return None
    if phase in (EmotionPhase.ICE_FALL, EmotionPhase.ICE_BASE, EmotionPhase.REPAIR):
        return EmotionLevel.LOW
    if phase in (EmotionPhase.EXPANSION, EmotionPhase.CHOP, EmotionPhase.RETREAT):
        return EmotionLevel.MID
    return EmotionLevel.HIGH  # CLIMAX_ACCEL, HIGH_STAGNATION, RETREAT_EARLY


def _classify_phase(level: EmotionLevel, direction: EmotionDirection) -> EmotionPhase:
    """水位×方向 → 相位"""
    if level == EmotionLevel.LOW:
        if direction == EmotionDirection.UP: return EmotionPhase.REPAIR
        elif direction == EmotionDirection.DOWN: return EmotionPhase.ICE_FALL
        else: return EmotionPhase.ICE_BASE
    elif level == EmotionLevel.MID:
        if direction == EmotionDirection.UP: return EmotionPhase.EXPANSION
        elif direction == EmotionDirection.DOWN: return EmotionPhase.RETREAT
        else: return EmotionPhase.CHOP
    else:  # HIGH
        if direction == EmotionDirection.UP: return EmotionPhase.CLIMAX_ACCEL
        elif direction == EmotionDirection.DOWN: return EmotionPhase.RETREAT_EARLY
        else: return EmotionPhase.HIGH_STAGNATION


def _transition_phase(planned: EmotionPhase, prev: Optional[EmotionPhase],
                      t0_upgrade: bool, t0_downgrade: bool) -> Tuple[EmotionPhase, str, List[str]]:
    """状态迁移: 慢升级, 快降级"""
    reasons: List[str] = []
    if prev is None or prev == EmotionPhase.UNKNOWN:
        return planned, PHASE_LABELS[planned], [f"初始相位: {PHASE_LABELS[planned]}"]

    # T0 降级: 可以直接降级
    if t0_downgrade and planned != prev:
        if planned in ALLOWED_DOWNGRADE.get(prev, set()):
            reasons.append(f"T0负向冲击: {PHASE_LABELS[prev]} → {PHASE_LABELS[planned]}")
            return planned, PHASE_LABELS[planned], reasons
        # 如果计划降级不被允许, 至少降一级
        for candidate in sorted(ALLOWED_DOWNGRADE.get(prev, set()), key=lambda p: PHASE_RISK_BUDGET[p][1]):
            reasons.append(f"T0负向冲击(受限): {PHASE_LABELS[prev]} → {PHASE_LABELS[candidate]}")
            return candidate, PHASE_LABELS[candidate], reasons

    # T0 升级: 最多升一级
    if t0_upgrade and planned != prev:
        if planned in ALLOWED_UPGRADE.get(prev, set()):
            reasons.append(f"T0正向确认: {PHASE_LABELS[prev]} → {PHASE_LABELS[planned]}")
            return planned, PHASE_LABELS[planned], reasons

    # 无变化或变化不被允许
    if planned == prev:
        reasons.append(f"相位维持: {PHASE_LABELS[prev]}")
        return prev, PHASE_LABELS[prev], reasons

    # 计划变化但T0不确认, 保持不变
    reasons.append(f"相位计划变化但T0未确认, 维持: {PHASE_LABELS[prev]}")
    return prev, PHASE_LABELS[prev], reasons


# ============================================================================
# 主入口
# ============================================================================

def determine_emotion_state(
    ztpool_t1: List[Dict[str, Any]],
    qxlive_top_t0: List[Dict[str, Any]],
    qxlive_top_t1: List[Dict[str, Any]],
    history: Optional[D6History] = None,
    static_thresholds: Optional[Dict[str, float]] = None,
    prev_phase: Optional[EmotionPhase] = None,
) -> D6EmotionResult:
    """
    主入口: D6 情绪周期判定。

    Args:
        ztpool_t1: T-1 ztpool 数据(含 分组名称/晋级率/晋级数/样本数)
        qxlive_top_t0: T0 9:25 qxlive 指标
        qxlive_top_t1: T-1 qxlive 指标
        history: 滚动历史数据
        static_thresholds: 静态阈值覆盖
        prev_phase: 前一交易日相位

    Returns:
        D6EmotionResult: 完整情绪周期判定结果
    """
    warnings: List[str] = []
    thresh = static_thresholds or STATIC_DEFAULTS
    has_history = history is not None and history.min_days >= 5

    # ========================================================================
    # 阶段 1: 数据提取
    # ========================================================================

    # T0 qxlive 指标
    ztbx_925 = _extract_qxlive_metric(qxlive_top_t0, "ZTBX")
    lbbx_925 = _extract_qxlive_metric(qxlive_top_t0, "LBBX")
    sz_925 = _extract_qxlive_metric(qxlive_top_t0, "SZ")
    xd_925 = _extract_qxlive_metric(qxlive_top_t0, "XD")
    dt_925_raw = _extract_qxlive_metric(qxlive_top_t0, "DT")

    # T-1 qxlive 指标
    ztbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "ZTBX")
    lbbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "LBBX")

    # 上涨占比
    advance_share = None
    if sz_925 is not None and xd_925 is not None and (sz_925 + xd_925) > 0:
        advance_share = round(sz_925 / (sz_925 + xd_925), 4)
    dt_925 = int(dt_925_raw) if dt_925_raw is not None else None

    # 晋级率(含分子分母)
    pbbx = _extract_ztpool_pbbx(ztpool_t1)
    jinji_1_2_raw = pbbx.get("PBBX_1_2", {})
    jinji_2_3_raw = pbbx.get("PBBX_2_3", {})

    # 收缩估计晋级率
    jinji_1_2 = _smoothed_rate(jinji_1_2_raw.get("promoted"), jinji_1_2_raw.get("eligible"))
    jinji_2_3 = _smoothed_rate(jinji_2_3_raw.get("promoted"), jinji_2_3_raw.get("eligible"))

    # 接力健康度: 单一指标贯穿全链路
    # 1进2权重 0.55 (样本多/信号稳/新接力形成), 2进3权重 0.45 (接力持续确认)
    if jinji_1_2 is not None and jinji_2_3 is not None:
        relay_health = round(RELAY_WEIGHT_1_2 * jinji_1_2 + RELAY_WEIGHT_2_3 * jinji_2_3, 2)
    elif jinji_1_2 is not None:
        relay_health = jinji_1_2
    elif jinji_2_3 is not None:
        relay_health = jinji_2_3
    else:
        relay_health = None

    # ========================================================================
    # 阶段 2: 数据质量评估
    # ========================================================================

    data_quality = {
        "profit_family": "VALID" if ztbx_925 is not None else "MISSING",
        "breadth_family": "VALID" if advance_share is not None else "MISSING",
        "relay_family": "VALID" if relay_health is not None else "MISSING",
    }
    # 部分数据标记 DEGRADED
    if jinji_1_2 is None and jinji_2_3 is not None:
        data_quality["relay_family"] = "DEGRADED"
    if jinji_2_3 is None and jinji_1_2 is not None:
        data_quality["relay_family"] = "DEGRADED"
    if lbbx_925 is None:
        data_quality["profit_family"] = "DEGRADED"

    missing_core = sum(1 for v in data_quality.values() if v == "MISSING")
    phase_confidence = 1.0 if missing_core == 0 else (0.5 if missing_core == 1 else 0.2)

    # ========================================================================
    # 阶段 3: 三家族水位计算
    # ========================================================================

    # 家族1: 强势股兑现
    if has_history:
        ztbx_pct = _calc_pctile(history.ztbx_values, ztbx_925)
        ztbx_pct = ztbx_pct if ztbx_pct is not None else 0.5
        lbbx_pct = _calc_pctile(history.lbbx_values, lbbx_925)
        lbbx_pct = lbbx_pct if lbbx_pct is not None else 0.5
        profit_level = round((ztbx_pct + lbbx_pct) / 2, 4)
    else:
        profit_level = 0.5  # 默认中位

    # 家族2: 市场广度
    if has_history:
        adv_pct = _calc_pctile(history.advance_share_values, advance_share)
        adv_pct = adv_pct if adv_pct is not None else 0.5
        dt_pct = _calc_pctile(history.dt_values, dt_925)
        dt_pct = dt_pct if dt_pct is not None else 0.5
        breadth_level = round((adv_pct + (1 - dt_pct)) / 2, 4)
    else:
        # 静态回退
        adv_ok = advance_share is not None and advance_share > thresh.get("advance_share_20pct", 0.25)
        dt_ok = dt_925 is not None and dt_925 < thresh.get("dt_80pct", 15.0)
        breadth_level = 0.5
        if not adv_ok and not dt_ok:
            breadth_level = 0.15
        elif not adv_ok or not dt_ok:
            breadth_level = 0.30

    # 家族3: 接力生态
    if has_history and relay_health is not None:
        relay_level = _calc_pctile(history.relay_health_values, relay_health)
        relay_level = relay_level if relay_level is not None else 0.5
    elif relay_health is not None:
        relay_level = 0.15 if relay_health < thresh.get("relay_health_15pct", 15.0) else 0.50
    else:
        relay_level = 0.50

    # 总水位(加权中位数: 接力生态 2x 权重, 因为它是短线最核心信号)
    weighted = sorted([profit_level, breadth_level, relay_level, relay_level])
    level_score = round((weighted[1] + weighted[2]) / 2, 4)
    level = _classify_level(level_score, prev_level=_phase_to_level(prev_phase))

    # ========================================================================
    # 阶段 4: 三家族方向计算
    # ========================================================================

    profit_slope = None
    breadth_slope = None
    relay_slope = None

    if has_history:
        profit_slope = history.robust_slope(history.ztbx_values)
        breadth_slope = history.robust_slope(history.advance_share_values)
        if len(history.relay_health_values) >= 4:
            relay_slope = history.robust_slope(history.relay_health_values)

    # 方向一致性: 至少2个家族同向
    if profit_slope is not None and breadth_slope is not None and relay_slope is not None:
        up_count = sum([profit_slope > DIRECTION_DEADBAND, breadth_slope > DIRECTION_DEADBAND, relay_slope > DIRECTION_DEADBAND])
        down_count = sum([profit_slope < -DIRECTION_DEADBAND, breadth_slope < -DIRECTION_DEADBAND, relay_slope < -DIRECTION_DEADBAND])
        if up_count >= 2:
            direction = EmotionDirection.UP
            momentum_score = round(sorted([profit_slope, breadth_slope, relay_slope])[1], 4)
        elif down_count >= 2:
            direction = EmotionDirection.DOWN
            momentum_score = round(sorted([profit_slope, breadth_slope, relay_slope])[1], 4)
        else:
            direction = EmotionDirection.FLAT
            momentum_score = 0.0
    else:
        direction = EmotionDirection.UNKNOWN
        momentum_score = 0.0

    # ========================================================================
    # 阶段 5: 计划相位
    # ========================================================================
    planned_phase = _classify_phase(level, direction) if direction != EmotionDirection.UNKNOWN else EmotionPhase.UNKNOWN

    # ========================================================================
    # 阶段 6: T0 冲击检测
    # ========================================================================

    t0_upgrade = False
    t0_downgrade = False
    ztbx_collapse = False
    lbbx_collapse = False
    breadth_shock = False

    # ZTBX 塌方: sign_break(从正转负) 或 历史冲击+当前弱
    if ztbx_925 is not None and ztbx_t1 is not None:
        sign_break = (ztbx_t1 > 0 and ztbx_925 < 0)
        delta = ztbx_925 - ztbx_t1
        if has_history:
            delta_20pct = history.percentile(
                [history.ztbx_values[i] - history.ztbx_values[i-1] for i in range(1, len(history.ztbx_values))],
                0.20
            )
            ztbx_35pct = history.ztbx_35pct()
            historical_shock = (delta_20pct is not None and delta < delta_20pct)
            current_weak = (ztbx_35pct is not None and ztbx_925 < ztbx_35pct)
            ztbx_collapse = sign_break or (historical_shock and current_weak)
        else:
            ztbx_collapse = sign_break or ztbx_925 < -2.0

    if ztbx_collapse:
        t0_downgrade = True
        warnings.append("ZTBX塌方: 从正溢价转负或历史冲击+当前弱")

    # LBBX 塌方
    if lbbx_925 is not None and lbbx_t1 is not None:
        lbbx_sign_break = (lbbx_t1 > 0 and lbbx_925 < 0)
        if has_history:
            lbbx_delta = lbbx_925 - lbbx_t1
            lbbx_delta_20pct = history.percentile(
                [history.lbbx_values[i] - history.lbbx_values[i-1] for i in range(1, len(history.lbbx_values))],
                0.20
            )
            lbbx_collapse = lbbx_sign_break or (lbbx_delta_20pct is not None and lbbx_delta < lbbx_delta_20pct)
        else:
            lbbx_collapse = lbbx_sign_break

    if lbbx_collapse:
        t0_downgrade = True
        warnings.append("LBBX塌方: 连板溢价崩溃")

    # 广度冲击
    if advance_share is not None:
        if has_history:
            breadth_shock = advance_share < history.advance_share_20pct()
        else:
            breadth_shock = advance_share < thresh.get("advance_share_20pct", 0.25)

    if breadth_shock:
        t0_downgrade = True
        warnings.append("广度冲击: 上涨占比低于滚动20分位")

    # T0 正向确认
    if ztbx_925 is not None and ztbx_t1 is not None and ztbx_t1 < 0 and ztbx_925 > 0:
        t0_upgrade = True
        warnings.append("T0正向: ZTBX从负转正")

    # ========================================================================
    # 阶段 7: 状态迁移
    # ========================================================================
    final_phase, phase_label, transition_reasons = _transition_phase(
        planned_phase, prev_phase, t0_upgrade, t0_downgrade
    )

    # ========================================================================
    # 阶段 8: 风险预算 + 结构偏好
    # ========================================================================
    risk_tier, base_cap, buy_mode = PHASE_RISK_BUDGET[final_phase]
    struct = PHASE_STRUCTURE[final_phase]

    # T0 冲击降仓: 用 min() 而不是乘法
    shock_cap = 1.0
    if ztbx_collapse: shock_cap = min(shock_cap, 0.40)
    if lbbx_collapse: shock_cap = min(shock_cap, 0.50)
    if breadth_shock: shock_cap = min(shock_cap, 0.50)

    # 数据质量降仓
    data_cap = 1.0
    if missing_core >= 2:
        data_cap = 0.10
    elif missing_core == 1:
        data_cap = 0.30

    final_cap = min(base_cap, shock_cap, data_cap)
    final_cap = max(0.0, min(1.0, final_cap))

    # 降级时买点模式收紧
    if t0_downgrade and buy_mode == BuyMode.AUCTION_AND_BOARD:
        buy_mode = BuyMode.BOARD_ONLY

    # LBBX塌方: 额外池降权
    pool_yizi_mult = 0.5 if lbbx_collapse else 1.0
    pool_huanshou_mult = 0.5 if lbbx_collapse else 1.0
    pool_fenqi_mult = 0.7 if lbbx_collapse else 1.0
    pool_feiban_mult = 0.7 if lbbx_collapse else 1.0

    # ========================================================================
    # 阶段 9: 构建输出
    # ========================================================================
    result = D6EmotionResult(
        phase=final_phase,
        phase_label=phase_label,
        level=level,
        level_score=level_score,
        direction=direction,
        momentum_score=momentum_score,
        profit_level=profit_level,
        breadth_level=breadth_level,
        relay_level=relay_level,
        profit_slope=profit_slope,
        breadth_slope=breadth_slope,
        relay_slope=relay_slope,
        t0_impulse="POSITIVE" if t0_upgrade else ("NEGATIVE" if t0_downgrade else "NEUTRAL"),
        ztbx_collapse=ztbx_collapse,
        lbbx_collapse=lbbx_collapse,
        breadth_shock=breadth_shock,
        risk_tier=risk_tier,
        position_cap=final_cap,
        height_preference=struct["height"],
        fenqi_priority=struct["fenqi"],
        yizi_enabled=struct["yizi"],
        huanshou_enabled=struct["huanshou"],
        fenqi_enabled=struct["fenqi_enabled"],
        feiban_enabled=struct["feiban"],
        pool_yizi_mult=pool_yizi_mult,
        pool_huanshou_mult=pool_huanshou_mult,
        pool_fenqi_mult=pool_fenqi_mult,
        pool_feiban_mult=pool_feiban_mult,
        buy_mode=buy_mode,
        auction_buy_enabled=(buy_mode == BuyMode.AUCTION_AND_BOARD),
        phase_confidence=phase_confidence,
        data_quality=data_quality,
        transition_from=prev_phase.value if prev_phase else "",
        transition_reason=transition_reasons,
        ztbx_925=ztbx_925,
        lbbx_925=lbbx_925,
        advance_share=advance_share,
        dt_925=dt_925,
        jinji_1_2=jinji_1_2,
        jinji_2_3=jinji_2_3,
        relay_health=relay_health,
        warnings=warnings,
        diagnostics={
            "pbbx_raw": pbbx,
            "ztbx_t1": ztbx_t1,
            "lbbx_t1": lbbx_t1,
            "sz_925": sz_925,
            "xd_925": xd_925,
            "has_history": has_history,
            "history_days": history.min_days if history else 0,
            "planned_phase": planned_phase.value,
            "level_score": level_score,
            "momentum_score": momentum_score,
            "shock_cap": shock_cap,
            "data_cap": data_cap,
            "base_cap": base_cap,
            "final_cap": final_cap,
        },
    )

    return result


# ============================================================================
# 自检
# ============================================================================

def _self_test() -> bool:
    """自检: 验证 D6 情绪周期基本逻辑"""
    ztpool = [
        {"分组名称": "1进2", "晋级率": 25.0, "晋级数": 5, "样本数": 20},
        {"分组名称": "2进3", "晋级率": 20.0, "晋级数": 2, "样本数": 10},
        {"分组名称": "3进4", "晋级率": 33.3, "晋级数": 1, "样本数": 3},
        {"分组名称": "4进5", "晋级率": 50.0, "晋级数": 1, "样本数": 2},
    ]
    qxlive_t0 = [
        {"metric_key": "ZTBX", "value": "2.5"},
        {"metric_key": "LBBX", "value": "3.0"},
        {"metric_key": "SZ", "value": "2100"},
        {"metric_key": "XD", "value": "1500"},
        {"metric_key": "DT", "value": "3"},
    ]
    qxlive_t1 = [
        {"metric_key": "ZTBX", "value": "2.0"},
        {"metric_key": "LBBX", "value": "2.5"},
    ]

    # 测试1: 无历史, 默认MID+UNKNOWN→UNKNOWN
    r1 = determine_emotion_state(ztpool, qxlive_t0, qxlive_t1, history=None, prev_phase=None)
    assert r1.jinji_1_2 is not None, f"jinji_1_2 should not be None, got {r1.jinji_1_2}"
    assert r1.jinji_2_3 is not None
    assert r1.relay_health is not None, f"relay_health should not be None"
    assert abs(r1.advance_share - 2100/(2100+1500)) < 0.001, f"advance_share wrong: {r1.advance_share}"
    assert r1.phase_confidence > 0, f"confidence should be >0"
    print(f"  Test1 PASS: jinji_1_2={r1.jinji_1_2}, relay_health={r1.relay_health}, advance_share={r1.advance_share}, phase={r1.phase_label}")

    # 测试2: 收缩估计
    # promoted=5, eligible=20 → smoothed = (5+1)/(20+1+1) = 6/22 = 27.27%
    assert abs(r1.jinji_1_2 - 27.27) < 0.5, f"smoothed 1_2 wrong: {r1.jinji_1_2}"
    # promoted=2, eligible=10 → smoothed = (2+1)/(10+1+1) = 3/12 = 25.0%
    assert abs(r1.jinji_2_3 - 25.0) < 0.5, f"smoothed 2_3 wrong: {r1.jinji_2_3}"
    # relay_health = 0.55*27.27 + 0.45*25.0 = 14.9985 + 11.25 = 26.25
    assert abs(r1.relay_health - 26.25) < 1.0, f"relay_health wrong: {r1.relay_health}"
    print(f"  Test2 PASS: smoothed rates correct, relay_health={r1.relay_health}")

    # 测试3: ZTBX塌方 (sign_break: t1>0 and t0<0)
    r3 = determine_emotion_state(
        ztpool,
        [{"metric_key": "ZTBX", "value": "-1.0"}, {"metric_key": "LBBX", "value": "1.0"}],
        [{"metric_key": "ZTBX", "value": "2.0"}, {"metric_key": "LBBX", "value": "2.0"}],
        history=None, prev_phase=EmotionPhase.EXPANSION,
    )
    assert r3.ztbx_collapse, f"ZTBX collapse should be detected"
    assert r3.t0_impulse == "NEGATIVE"
    print(f"  Test3 PASS: ZTBX collapse detected, phase={r3.phase_label}")

    # 测试4: 有历史数据, 计算分位和方向
    hist = D6History()
    for i in range(10):
        hist.add_day(ztbx=2.0 + i * 0.1, lbbx=2.0 + i * 0.1,
                     advance_share=0.4 + i * 0.02, dt=5 - i * 0.2,
                     relay_health=25.0 + i * 0.75)
    r4 = determine_emotion_state(ztpool, qxlive_t0, qxlive_t1, history=hist, prev_phase=None)
    assert r4.direction != EmotionDirection.UNKNOWN, f"direction should be known with history"
    print(f"  Test4 PASS: direction={r4.direction.value}, level={r4.level.value}, phase={r4.phase_label}")

    # 测试5: 冰点下杀 (LOW+DOWN) — 加权中位数下 relay 2x权重
    hist_cold = D6History()
    for i in range(10):
        hist_cold.add_day(ztbx=-3.0 - i * 0.3, lbbx=-2.0 - i * 0.2,
                          advance_share=0.15 - i * 0.01, dt=20 + i * 2,
                          relay_health=30.0 - i * 1.5)
    r5 = determine_emotion_state(
        [{"分组名称": "1进2", "晋级率": 5.0, "晋级数": 1, "样本数": 20},
         {"分组名称": "2进3", "晋级率": 0.0, "晋级数": 0, "样本数": 5}],
        [{"metric_key": "ZTBX", "value": "-5.0"}, {"metric_key": "LBBX", "value": "-3.0"},
         {"metric_key": "SZ", "value": "300"}, {"metric_key": "XD", "value": "5000"},
         {"metric_key": "DT", "value": "25"}],
        [{"metric_key": "ZTBX", "value": "-4.0"}, {"metric_key": "LBBX", "value": "-2.0"}],
        history=hist_cold, prev_phase=None,
    )
    assert r5.phase == EmotionPhase.ICE_FALL, f"Expected ICE_FALL (LOW+DOWN), got {r5.phase}"
    assert r5.position_cap == 0.0, f"ICE_FALL should have cap=0, got {r5.position_cap}"
    print(f"  Test5 PASS: ICE_FALL detected, cap={r5.position_cap}, level={r5.level_score:.3f}")

    # 测试6: 晋级率缺失(eligible=0) → 该层为None, 不参与relay_health
    r6 = determine_emotion_state(
        [{"分组名称": "1进2", "晋级率": 25.0, "晋级数": 5, "样本数": 20}],
        qxlive_t0, qxlive_t1, history=None, prev_phase=None,
    )
    assert r6.jinji_1_2 is not None, "1进2 should exist"
    assert r6.jinji_2_3 is None, "2进3 should be None (no data)"
    assert r6.relay_health is not None, "relay_health should still work with 1 layer"
    assert r6.data_quality["relay_family"] == "DEGRADED"
    print(f"  Test6 PASS: single layer relay_health={r6.relay_health}, DQ={r6.data_quality['relay_family']}")

    print("\n=== ALL TESTS PASSED ===")
    return True


if __name__ == "__main__":
    _self_test()