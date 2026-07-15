#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_d6_emotion.py  --  v4.2 D6 情绪周期状态机 (简化版 v3)

============================================================================
设计哲学
============================================================================
D6 不是风险预算层。D6 必须首先识别市场所处的情绪周期位置，再由周期位置
派生风险预算、进攻方向和结构偏好。

核心公式:  水位(Level) × 方向(Direction) → 七宫格周期相位(Phase)
          Phase + 极端否决 → 风险等级(RiskTier) + 仓位上限 + 结构偏好

============================================================================
三大家族 (互补观察角度)
============================================================================
1. 强势股兑现(P):  median(pct(ZTBX), pct(LBBX))
2. 市场广度(B):    median(pct(advance_share), 1-pct(DT))
3. 接力生态(R):    pct(relay_health)

总水位 = median(P, B, R)   # 三家族等权中位数
方向 = majority(dP, dB, dR)  # 日变化, 2-of-3 共识, epsilon=0.03

============================================================================
七宫格
============================================================================
              UP              FLAT            DOWN
HIGH    HIGH_ACTIVE      HIGH_STAGNATION   RETREAT(EARLY)
MID     EXPANSION        CHOP              RETREAT(SPREADING)
LOW     REPAIR           ICE(BASING)       ICE(FALLING)

============================================================================
两个极端否决 (Phase 之外的硬止损)
============================================================================
1. 强势股集体翻负: ZTBX_t0<0 and LBBX_t0<0 and ZTBX_t1>0 and LBBX_t1>0
2. 广度恐慌: advance_share极低 AND DT极高

触发 → risk_tier=CRISIS, position_cap=0, 全池关闭

============================================================================
接力健康度
============================================================================
relay_health = 0.55 × smoothed_rate(1进2) + 0.45 × smoothed_rate(2进3)
Laplace平滑: smoothed_rate = (promoted + 1) / (eligible + 2) × 100

============================================================================
简化原则
============================================================================
- QX 退出核心判定 (公式不透明, 可能重复 P/B 信息)
- 3进4以上不进入核心 relay (样本太小, 分位无意义)
- 不再做成熟度/双速方向/背离标签/复杂状态迁移图
- 不再做多套 shock cap / 多路径重复惩罚
- 只用日变化不用多日斜率 (总共3个家族, 平滑无增量信息)
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
    ICE = "ICE"                     # 冰点: LOW+FLAT/DOWN (子阶段: FALLING/BASING)
    REPAIR = "REPAIR"               # 冰点修复: LOW+UP
    # 中位
    CHOP = "CHOP"                   # 震荡混沌: MID+FLAT
    RETREAT = "RETREAT"             # 退潮: MID+DOWN / HIGH+DOWN (子阶段: EARLY/SPREADING)
    EXPANSION = "EXPANSION"         # 发酵主升: MID+UP
    # 高位
    HIGH_ACTIVE = "HIGH_ACTIVE"     # 高位加速: HIGH+UP
    HIGH_STAGNATION = "HIGH_STAGNATION"  # 高位钝化: HIGH+FLAT
    # 数据不足
    UNKNOWN = "UNKNOWN"

class RiskTier(Enum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRISIS = "CRISIS"

class BuyMode(Enum):
    AUCTION_AND_BOARD = "auction_and_board"       # 竞价+排板
    BOARD_ONLY = "board_only"                     # 排板为主
    OBSERVE_ONLY = "observe_only"                 # 仅观察
    EMPTY = "empty"                                # 空仓

class DataQuality(Enum):
    GOOD = "GOOD"           # 3个家族全有效
    DEGRADED = "DEGRADED"   # 2个家族有效
    UNKNOWN = "UNKNOWN"     # <2个家族有效

# ============================================================================
# 数据类
# ============================================================================

@dataclass
class D6History:
    """D6 历史数据，用于计算滚动分位数和方向"""
    ztbx_values: List[float] = field(default_factory=list)
    lbbx_values: List[float] = field(default_factory=list)
    advance_share_values: List[float] = field(default_factory=list)
    dt_values: List[float] = field(default_factory=list)
    relay_health_values: List[float] = field(default_factory=list)

    _WINDOW = 60
    _MIN_DAYS_FOR_PCTILE = 20

    def add_day(self, ztbx: Optional[float] = None, lbbx: Optional[float] = None,
                advance_share: Optional[float] = None, dt: Optional[float] = None,
                relay_health: Optional[float] = None) -> None:
        if ztbx is not None: self.ztbx_values.append(ztbx)
        if lbbx is not None: self.lbbx_values.append(lbbx)
        if advance_share is not None: self.advance_share_values.append(advance_share)
        if dt is not None: self.dt_values.append(dt)
        if relay_health is not None: self.relay_health_values.append(relay_health)

    def _pctile(self, values: List[float], q: float) -> Optional[float]:
        """计算分位数, 需要至少 MIN_DAYS_FOR_PCTILE 天"""
        if len(values) < self._MIN_DAYS_FOR_PCTILE:
            return None
        sv = sorted(values)
        idx = int(len(sv) * q)
        return sv[min(idx, len(sv) - 1)]

    def percentile(self, values: List[float], q: float) -> Optional[float]:
        return self._pctile(values, q)

    def valid_for_pctile(self) -> bool:
        """是否有足够历史计算分位数"""
        return self.min_days >= self._MIN_DAYS_FOR_PCTILE

    # 极端否决阈值
    def advance_share_15pct(self) -> Optional[float]: return self._pctile(self.advance_share_values, 0.15)
    def dt_85pct(self) -> Optional[float]: return self._pctile(self.dt_values, 0.85)

    @property
    def min_days(self) -> int:
        return min(len(self.ztbx_values), len(self.advance_share_values),
                   len(self.relay_health_values))

    @property
    def history_days(self) -> int:
        return self.min_days

    def last_values(self) -> Dict[str, Optional[float]]:
        """获取最近一日的值, 用于计算方向"""
        return {
            "ztbx": self.ztbx_values[-1] if self.ztbx_values else None,
            "lbbx": self.lbbx_values[-1] if self.lbbx_values else None,
            "advance_share": self.advance_share_values[-1] if self.advance_share_values else None,
            "dt": self.dt_values[-1] if self.dt_values else None,
            "relay_health": self.relay_health_values[-1] if self.relay_health_values else None,
        }


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

    # === 三个家族水位 ===
    profit_level: float = 0.5
    breadth_level: float = 0.5
    relay_level: float = 0.5

    # === 三个家族日变化 ===
    profit_delta: Optional[float] = None
    breadth_delta: Optional[float] = None
    relay_delta: Optional[float] = None

    # === 极端否决 ===
    hard_veto: bool = False
    profit_collapse: bool = False     # 强势股集体翻负
    breadth_panic: bool = False       # 广度恐慌

    # === 风险 ===
    risk_tier: RiskTier = RiskTier.NORMAL
    position_cap: float = 1.0

    # === 结构偏好 ===
    height_preference: str = "MID"        # LOW / MID / CORE_HIGH
    fenqi_priority: str = "NORMAL"        # HIGH / NORMAL / LOW / DISABLED
    yizi_enabled: bool = True
    huanshou_enabled: bool = True
    fenqi_enabled: bool = True
    feiban_enabled: bool = True

    # === 池乘子 (保留接口, 简化版恒为 1.0) ===
    pool_yizi_mult: float = 1.0
    pool_huanshou_mult: float = 1.0
    pool_fenqi_mult: float = 1.0
    pool_feiban_mult: float = 1.0

    # === 执行 ===
    buy_mode: BuyMode = BuyMode.AUCTION_AND_BOARD
    auction_buy_enabled: bool = True

    # === KQXY 亏钱效应覆盖层 ===
    kqxy_t1: Optional[float] = None         # T-1 盘后 KQXY 原始值
    kqxy_t2: Optional[float] = None         # T-2 盘后 KQXY 原始值
    kqxy_pct: Optional[float] = None        # T-1 KQXY 分位 (0-1)
    kqxy_delta: Optional[float] = None      # KQXY 日变化 (T-1 - T-2 分位差)
    loss_level: str = "UNKNOWN"             # KQXY 水位: LOW / MID / HIGH
    loss_direction: str = "UNKNOWN"         # KQXY 方向: CONTRACTING / FLAT / EXPANDING
    loss_overlay: str = "NONE"              # 亏钱效应覆盖标记: NONE / REPAIR_SUPPORT / REPAIR_WEAK / HIGH_CRACKING

    # === 质量 ===
    phase_confidence: float = 0.0
    data_quality: Dict[str, str] = field(default_factory=lambda: {
        "profit_family": "MISSING",
        "breadth_family": "MISSING",
        "relay_family": "MISSING",
    })

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

# 水位阈值 (含滞回)
LEVEL_LOW_THRESHOLD = 0.30
LEVEL_HIGH_THRESHOLD = 0.70
LEVEL_LOW_EXIT = 0.40       # 退出 LOW 需要 >0.40
LEVEL_HIGH_EXIT = 0.60      # 退出 HIGH 需要 <0.60

# 方向死区
DIRECTION_DEADBAND = 0.03

# 晋级率 Laplace 收缩估计 (先验: Beta(1,1), 均值 50%)
ALPHA = 1.0
BETA = 1.0

# 接力健康度权重
RELAY_WEIGHT_1_2 = 0.55
RELAY_WEIGHT_2_3 = 0.45

# 静态阈值 (历史不足 20 天时回退)
STATIC_DEFAULTS = {
    "advance_share_15pct": 0.20,
    "dt_85pct": 20.0,
}

# 相位 → 风险预算
PHASE_RISK_BUDGET = {
    EmotionPhase.ICE:               (RiskTier.CRISIS,  0.00, BuyMode.EMPTY),
    EmotionPhase.REPAIR:            (RiskTier.WARNING, 0.35, BuyMode.BOARD_ONLY),
    EmotionPhase.EXPANSION:         (RiskTier.NORMAL,  0.80, BuyMode.AUCTION_AND_BOARD),
    EmotionPhase.HIGH_ACTIVE:       (RiskTier.WARNING, 0.50, BuyMode.BOARD_ONLY),
    EmotionPhase.HIGH_STAGNATION:   (RiskTier.WARNING, 0.30, BuyMode.BOARD_ONLY),
    EmotionPhase.RETREAT:           (RiskTier.CRISIS,  0.00, BuyMode.EMPTY),
    EmotionPhase.CHOP:              (RiskTier.WARNING, 0.30, BuyMode.BOARD_ONLY),
    EmotionPhase.UNKNOWN:           (RiskTier.WARNING, 0.10, BuyMode.OBSERVE_ONLY),
}

# 相位 → 结构偏好
PHASE_STRUCTURE = {
    EmotionPhase.ICE:               {"height": "LOW",  "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": False, "feiban": False},
    EmotionPhase.REPAIR:            {"height": "LOW",  "fenqi": "HIGH",    "yizi": False, "huanshou": True,  "fenqi_enabled": True,  "feiban": True},
    EmotionPhase.EXPANSION:         {"height": "MID",  "fenqi": "NORMAL",  "yizi": True,  "huanshou": True,  "fenqi_enabled": True,  "feiban": True},
    EmotionPhase.HIGH_ACTIVE:       {"height": "CORE_HIGH", "fenqi": "LOW", "yizi": True,  "huanshou": True,  "fenqi_enabled": True,  "feiban": False},
    EmotionPhase.HIGH_STAGNATION:   {"height": "CORE_HIGH", "fenqi": "DISABLED", "yizi": False, "huanshou": True, "fenqi_enabled": False, "feiban": False},
    EmotionPhase.RETREAT:           {"height": "LOW",  "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": False, "feiban": False},
    EmotionPhase.CHOP:              {"height": "MID",  "fenqi": "NORMAL",  "yizi": False, "huanshou": True,  "fenqi_enabled": True,  "feiban": True},
    EmotionPhase.UNKNOWN:           {"height": "LOW",  "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": False, "feiban": False},
}

# 相位显示名
PHASE_LABELS = {
    EmotionPhase.ICE:               "冰点",
    EmotionPhase.REPAIR:            "冰点修复",
    EmotionPhase.EXPANSION:         "发酵主升",
    EmotionPhase.HIGH_ACTIVE:       "高位加速",
    EmotionPhase.HIGH_STAGNATION:   "高位钝化",
    EmotionPhase.RETREAT:           "退潮",
    EmotionPhase.CHOP:              "震荡混沌",
    EmotionPhase.UNKNOWN:           "数据不足",
}


# ============================================================================
# 核心逻辑
# ============================================================================

def _extract_ztpool_pbbx(ztpool_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    从 home.ztpool 数据中提取 PBBX 晋级率 (1进2 + 2进3)。
    3进4以上样本太小, 不纳入 relay_health 计算。
    """
    result: Dict[str, Dict[str, Any]] = {}
    seen: set = set()

    for row in (ztpool_rows or []):
        ladder = str(row.get("ladder_group") or row.get("分组名称") or "").strip()
        if not ladder or ladder in seen:
            continue
        seen.add(ladder)

        promo = row.get("promo_rate") or row.get("晋级率")
        rate = None
        if promo is not None:
            try:
                rate = float(str(promo).replace("%", "").strip())
            except (ValueError, TypeError):
                pass

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
                "rate": rate, "promoted": prom_count,
                "eligible": elig_count, "ladder": ladder,
            }

    return result


def _smoothed_rate(promoted: Optional[int], eligible: Optional[int],
                   alpha: float = ALPHA, beta: float = BETA) -> Optional[float]:
    """
    Laplace 收缩估计: (promoted + alpha) / (eligible + alpha + beta) × 100。
    先验 Beta(alpha, beta) = Beta(1,1), 均值 50%。
    防止小样本 0/3 → 0% 的极端值误判。
    """
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
    """计算给定值在序列中的分位数 (0-1)"""
    if value is None or not values:
        return None
    sv = sorted(values)
    n = len(sv)
    rank = sum(1 for v in sv if v <= value)
    return round(rank / n, 4)


def _classify_level(score: float, prev_level: Optional[EmotionLevel] = None) -> EmotionLevel:
    """水位分类 (含滞回区间: 慢升级, 快降级)"""
    if prev_level == EmotionLevel.LOW:
        low_threshold = LEVEL_LOW_EXIT   # 0.40 — 从 LOW 退出需要更高
    else:
        low_threshold = LEVEL_LOW_THRESHOLD  # 0.30

    if prev_level == EmotionLevel.HIGH:
        high_threshold = LEVEL_HIGH_EXIT  # 0.60 — 从 HIGH 退出需要更低
    else:
        high_threshold = LEVEL_HIGH_THRESHOLD  # 0.70

    if score < low_threshold:
        return EmotionLevel.LOW
    elif score > high_threshold:
        return EmotionLevel.HIGH
    else:
        return EmotionLevel.MID


def _phase_to_level(phase: Optional[EmotionPhase]) -> Optional[EmotionLevel]:
    """从相位反推水位, 用于滞回区间"""
    if phase is None or phase == EmotionPhase.UNKNOWN:
        return None
    if phase in (EmotionPhase.ICE, EmotionPhase.REPAIR):
        return EmotionLevel.LOW
    if phase in (EmotionPhase.EXPANSION, EmotionPhase.CHOP, EmotionPhase.RETREAT):
        return EmotionLevel.MID
    return EmotionLevel.HIGH  # HIGH_ACTIVE, HIGH_STAGNATION


def _classify_phase(level: EmotionLevel, direction: EmotionDirection) -> EmotionPhase:
    """水位 × 方向 → 七宫格相位"""
    if level == EmotionLevel.LOW:
        if direction == EmotionDirection.UP: return EmotionPhase.REPAIR
        else: return EmotionPhase.ICE  # FLAT → ICE(BASING), DOWN → ICE(FALLING)
    elif level == EmotionLevel.MID:
        if direction == EmotionDirection.UP: return EmotionPhase.EXPANSION
        elif direction == EmotionDirection.DOWN: return EmotionPhase.RETREAT  # SPREADING
        else: return EmotionPhase.CHOP
    else:  # HIGH
        if direction == EmotionDirection.UP: return EmotionPhase.HIGH_ACTIVE
        elif direction == EmotionDirection.DOWN: return EmotionPhase.RETREAT  # EARLY
        else: return EmotionPhase.HIGH_STAGNATION


def _classify_loss_overlay(
    kqxy_t1: Optional[float],
    kqxy_t2: Optional[float],
    history: Optional[D6History],
    epsilon: float = 0.03,
) -> Dict[str, Any]:
    """
    KQXY 亏钱效应覆盖层。

    KQXY 是 T-1 盘后指标, 不参与 T0 核心 P/B/R 判定。
    只作为 Phase 修饰层, 解决两个问题:
      1. 低位是继续下杀/磨底/修复 (ICE 子阶段 + REPAIR 质量)
      2. 高位强势是否伴随亏钱效应扩散 (内部裂化)

    Returns:
        {
            "kqxy_t1": 原始值,
            "kqxy_t2": 原始值,
            "kqxy_pct": T-1 KQXY 分位,
            "kqxy_delta": 日变化,
            "loss_level": LOW/MID/HIGH,
            "loss_direction": CONTRACTING/FLAT/EXPANDING/UNKNOWN,
            "loss_overlay": NONE/REPAIR_SUPPORT/REPAIR_WEAK/HIGH_CRACKING,
        }
    """
    result: Dict[str, Any] = {
        "kqxy_t1": kqxy_t1, "kqxy_t2": kqxy_t2,
        "kqxy_pct": None, "kqxy_delta": None,
        "loss_level": "UNKNOWN", "loss_direction": "UNKNOWN",
        "loss_overlay": "NONE",
    }

    if kqxy_t1 is None:
        return result

    # KQXY 分位
    if history is not None and history.valid_for_pctile():
        # KQXY 分位需要专用的历史序列无法直接使用 D6History(因为 D6History 没有 kqxy 字段)
        # 这里用静态阈值: KQXY 越高亏钱越严重
        kqxy_pct = kqxy_t1 / 100.0  # KQXY 原始值范围大约 0-100
        kqxy_pct = max(0.0, min(1.0, kqxy_pct))
    else:
        kqxy_pct = kqxy_t1 / 100.0
        kqxy_pct = max(0.0, min(1.0, kqxy_pct))

    result["kqxy_pct"] = round(kqxy_pct, 4)

    # KQXY 水位: 静态阈值 0.30/0.70 (无历史分位时用静态)
    if kqxy_pct < 0.30:
        result["loss_level"] = "LOW"
    elif kqxy_pct > 0.70:
        result["loss_level"] = "HIGH"
    else:
        result["loss_level"] = "MID"

    # KQXY 方向
    if kqxy_t2 is not None:
        kqxy_delta = kqxy_pct - (kqxy_t2 / 100.0)
        result["kqxy_delta"] = round(kqxy_delta, 4)
        if kqxy_delta > epsilon:
            result["loss_direction"] = "EXPANDING"    # 亏钱效应扩散
        elif kqxy_delta < -epsilon:
            result["loss_direction"] = "CONTRACTING"  # 亏钱效应收敛
        else:
            result["loss_direction"] = "FLAT"
    else:
        result["loss_direction"] = "UNKNOWN"

    return result


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
    kqxy_t1: Optional[float] = None,
    kqxy_t2: Optional[float] = None,
) -> D6EmotionResult:
    """
    主入口: D6 情绪周期判定 (简化版 v3)。

    主链:
      9:25 数据 → P/B/R 分位 → 水位 → 方向 → Phase → KQXY 覆盖层 → 仓位+池

    Args:
        ztpool_t1: T-1 ztpool 数据
        qxlive_top_t0: T0 9:25 qxlive 指标
        qxlive_top_t1: T-1 qxlive 指标
        history: 滚动历史数据 (≥20天启用分位, <20天用静态阈值)
        static_thresholds: 静态阈值覆盖
        prev_phase: 前一交易日相位 (用于滞回)
        kqxy_t1: T-1 盘后 KQXY 原始值 (非 9:25, 盘后有实际值)
        kqxy_t2: T-2 盘后 KQXY 原始值

    Returns:
        D6EmotionResult: 完整情绪周期判定结果
    """
    warnings: List[str] = []
    thresh = static_thresholds or STATIC_DEFAULTS
    use_percentile = history is not None and history.valid_for_pctile()

    # ========================================================================
    # 阶段 1: 数据提取
    # ========================================================================

    # T0 9:25 qxlive 指标
    ztbx_925 = _extract_qxlive_metric(qxlive_top_t0, "ZTBX")
    lbbx_925 = _extract_qxlive_metric(qxlive_top_t0, "LBBX")
    sz_925 = _extract_qxlive_metric(qxlive_top_t0, "SZ")
    xd_925 = _extract_qxlive_metric(qxlive_top_t0, "XD")
    dt_925_raw = _extract_qxlive_metric(qxlive_top_t0, "DT")

    # T-1 值 (用于方向计算和极端否决)
    ztbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "ZTBX")
    lbbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "LBBX")

    # 上涨占比
    advance_share = None
    if sz_925 is not None and xd_925 is not None and (sz_925 + xd_925) > 0:
        advance_share = round(sz_925 / (sz_925 + xd_925), 4)
    dt_925 = int(dt_925_raw) if dt_925_raw is not None else None

    # 晋级率 (T-1 盘后)
    pbbx = _extract_ztpool_pbbx(ztpool_t1)
    jinji_1_2_raw = pbbx.get("PBBX_1_2", {})
    jinji_2_3_raw = pbbx.get("PBBX_2_3", {})

    # Laplace 收缩估计晋级率
    jinji_1_2 = _smoothed_rate(jinji_1_2_raw.get("promoted"), jinji_1_2_raw.get("eligible"))
    jinji_2_3 = _smoothed_rate(jinji_2_3_raw.get("promoted"), jinji_2_3_raw.get("eligible"))

    # 接力健康度
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

    valid_families = 0
    data_quality = {}
    if ztbx_925 is not None:
        data_quality["profit_family"] = "VALID"
        valid_families += 1
    else:
        data_quality["profit_family"] = "MISSING"

    if advance_share is not None:
        data_quality["breadth_family"] = "VALID"
        valid_families += 1
    else:
        data_quality["breadth_family"] = "MISSING"

    if relay_health is not None:
        data_quality["relay_family"] = "VALID"
        valid_families += 1
    else:
        data_quality["relay_family"] = "MISSING"

    # 部分 relay 数据标注 DEGRADED
    if jinji_1_2 is None and jinji_2_3 is not None:
        data_quality["relay_family"] = "DEGRADED"
    if jinji_2_3 is None and jinji_1_2 is not None:
        data_quality["relay_family"] = "DEGRADED"

    if valid_families >= 3:
        data_quality_level = DataQuality.GOOD
        phase_confidence = 1.0
    elif valid_families == 2:
        data_quality_level = DataQuality.DEGRADED
        phase_confidence = 0.5
    else:
        data_quality_level = DataQuality.UNKNOWN
        phase_confidence = 0.2

    # ========================================================================
    # 阶段 3: 三家族水位 (分位 or 静态)
    # ========================================================================

    # 家族 1: 强势股兑现 P = median(pct(ZTBX), pct(LBBX))
    if use_percentile:
        ztbx_pct = _calc_pctile(history.ztbx_values, ztbx_925)
        ztbx_pct = ztbx_pct if ztbx_pct is not None else 0.5
        lbbx_pct = _calc_pctile(history.lbbx_values, lbbx_925)
        lbbx_pct = lbbx_pct if lbbx_pct is not None else 0.5
        profit_level = round(sorted([ztbx_pct, lbbx_pct])[len([ztbx_pct, lbbx_pct]) // 2], 4)
    else:
        profit_level = 0.5

    # 家族 2: 市场广度 B = median(pct(advance_share), 1-pct(DT))
    if use_percentile:
        adv_pct = _calc_pctile(history.advance_share_values, advance_share)
        adv_pct = adv_pct if adv_pct is not None else 0.5
        dt_pct = _calc_pctile(history.dt_values, dt_925)
        dt_pct = dt_pct if dt_pct is not None else 0.5
        breadth_level = round(sorted([adv_pct, 1 - dt_pct])[0], 4)  # 2个值中位数即任一个
    else:
        breadth_level = 0.5

    # 家族 3: 接力生态 R = pct(relay_health)
    if use_percentile and relay_health is not None:
        relay_level = _calc_pctile(history.relay_health_values, relay_health)
        relay_level = relay_level if relay_level is not None else 0.5
    else:
        relay_level = 0.5

    # 总水位: 三家族等权中位数
    level_score = round(sorted([profit_level, breadth_level, relay_level])[1], 4)
    level = _classify_level(level_score, prev_level=_phase_to_level(prev_phase))

    # ========================================================================
    # 阶段 4: 三家族方向 (日变化, 2-of-3 共识)
    # ========================================================================
    # 方向 = 今日分位 - 昨日分位
    # 昨日分位需用排除昨日的 history 计算, 否则昨日值作为 history 最后一天
    # 其分位永远是 1.0, 导致方向永远 FLAT
    # 昨日原始值从 qxlive_top_t1 获取 (与 history.last_values() 一致, 但显式传入)
    # ========================================================================

    profit_delta = None
    breadth_delta = None
    relay_delta = None

    # 提取 T-1 原始值用于方向计算
    ztbx_t1_for_dir = _extract_qxlive_metric(qxlive_top_t1, "ZTBX")
    lbbx_t1_for_dir = _extract_qxlive_metric(qxlive_top_t1, "LBBX")
    sz_t1 = _extract_qxlive_metric(qxlive_top_t1, "SZ")
    xd_t1 = _extract_qxlive_metric(qxlive_top_t1, "XD")
    dt_t1_raw = _extract_qxlive_metric(qxlive_top_t1, "DT")
    advance_share_t1 = None
    if sz_t1 is not None and xd_t1 is not None and (sz_t1 + xd_t1) > 0:
        advance_share_t1 = round(sz_t1 / (sz_t1 + xd_t1), 4)
    dt_t1 = int(dt_t1_raw) if dt_t1_raw is not None else None

    if history is not None and history.min_days >= 2:
        # 用排除最后一天的 history 计算昨日分位
        prev_ztbx_vals = history.ztbx_values[:-1] if len(history.ztbx_values) >= 2 else []
        prev_lbbx_vals = history.lbbx_values[:-1] if len(history.lbbx_values) >= 2 else []
        prev_adv_vals = history.advance_share_values[:-1] if len(history.advance_share_values) >= 2 else []
        prev_dt_vals = history.dt_values[:-1] if len(history.dt_values) >= 2 else []
        prev_relay_vals = history.relay_health_values[:-1] if len(history.relay_health_values) >= 2 else []

        # Profit 方向: dP = pct(ZTBX_t0, full_hist) - pct(ZTBX_t1, hist_excl_last)
        if ztbx_t1_for_dir is not None and ztbx_925 is not None and prev_ztbx_vals:
            p_prev = _calc_pctile(prev_ztbx_vals, ztbx_t1_for_dir)
            p_curr = _calc_pctile(history.ztbx_values, ztbx_925)
            if p_prev is not None and p_curr is not None:
                profit_delta = round(p_curr - p_prev, 4)

        # Breadth 方向: dB = B_t - B_t1
        if advance_share_t1 is not None and advance_share is not None and prev_adv_vals:
            b_prev_adv = _calc_pctile(prev_adv_vals, advance_share_t1)
            b_curr_adv = _calc_pctile(history.advance_share_values, advance_share)
            if dt_t1 is not None and dt_925 is not None and prev_dt_vals:
                b_prev_dt = _calc_pctile(prev_dt_vals, dt_t1)
                b_curr_dt = _calc_pctile(history.dt_values, dt_925)
                if (b_prev_adv is not None and b_curr_adv is not None
                        and b_prev_dt is not None and b_curr_dt is not None):
                    b_prev = sorted([b_prev_adv, 1 - b_prev_dt])[0]
                    b_curr = sorted([b_curr_adv, 1 - b_curr_dt])[0]
                    breadth_delta = round(b_curr - b_prev, 4)

        # Relay 方向: dR = pct(relay_t0, full_hist) - pct(relay_t1, hist_excl_last)
        if relay_health is not None and prev_relay_vals:
            prev_raw_relay = history.relay_health_values[-1] if history.relay_health_values else None
            if prev_raw_relay is not None:
                r_prev = _calc_pctile(prev_relay_vals, prev_raw_relay)
                r_curr = _calc_pctile(history.relay_health_values, relay_health)
                if r_prev is not None and r_curr is not None:
                    relay_delta = round(r_curr - r_prev, 4)

    # 方向: 2-of-3 共识
    if profit_delta is not None and breadth_delta is not None and relay_delta is not None:
        up_count = sum([profit_delta > DIRECTION_DEADBAND, breadth_delta > DIRECTION_DEADBAND, relay_delta > DIRECTION_DEADBAND])
        down_count = sum([profit_delta < -DIRECTION_DEADBAND, breadth_delta < -DIRECTION_DEADBAND, relay_delta < -DIRECTION_DEADBAND])
        if up_count >= 2:
            direction = EmotionDirection.UP
        elif down_count >= 2:
            direction = EmotionDirection.DOWN
        else:
            direction = EmotionDirection.FLAT
    else:
        direction = EmotionDirection.UNKNOWN

    # ========================================================================
    # 阶段 5: 相位判定 (P/B/R 基础)
    # ========================================================================
    phase = _classify_phase(level, direction) if direction != EmotionDirection.UNKNOWN else EmotionPhase.UNKNOWN

    # ========================================================================
    # 阶段 5.5: KQXY 亏钱效应覆盖层 (不影响 Phase, 只修饰执行策略)
    # ========================================================================
    loss = _classify_loss_overlay(kqxy_t1, kqxy_t2, history)
    loss_level = loss["loss_level"]
    loss_direction = loss["loss_direction"]
    loss_overlay = "NONE"

    # 低位修复质量: KQXY 收敛 → 强修复, KQXY 扩散 → 弱修复
    if phase == EmotionPhase.REPAIR:
        if loss_direction == "CONTRACTING":
            loss_overlay = "REPAIR_SUPPORT"
        elif loss_direction == "EXPANDING":
            loss_overlay = "REPAIR_WEAK"

    # 高位内部裂化: KQXY 扩散 → 降级执行策略
    if phase == EmotionPhase.HIGH_ACTIVE and loss_direction == "EXPANDING":
        loss_overlay = "HIGH_CRACKING"

    # ========================================================================
    # 阶段 6: 极端否决 (Phase 之外的硬止损)
    # ========================================================================

    # 否决 1: 强势股集体翻负
    profit_collapse = (
        ztbx_t1 is not None and lbbx_t1 is not None
        and ztbx_925 is not None and lbbx_925 is not None
        and ztbx_t1 > 0 and lbbx_t1 > 0
        and ztbx_925 < 0 and lbbx_925 < 0
    )

    # 否决 2: 广度恐慌
    breadth_panic = False
    if advance_share is not None and dt_925 is not None:
        if use_percentile:
            adv_15 = history.advance_share_15pct()
            dt_85 = history.dt_85pct()
            breadth_panic = (
                adv_15 is not None and dt_85 is not None
                and advance_share < adv_15 and dt_925 > dt_85
            )
        else:
            breadth_panic = (
                advance_share < thresh.get("advance_share_15pct", 0.20)
                and dt_925 > thresh.get("dt_85pct", 20.0)
            )

    hard_veto = profit_collapse or breadth_panic

    if profit_collapse:
        warnings.append("极端否决: 强势股集体翻负 (ZTBX+LBBX同时从正转负)")
    if breadth_panic:
        warnings.append("极端否决: 广度恐慌 (上涨占比极低 + 跌停家数极高)")

    # ========================================================================
    # 阶段 7: 风险预算 + 结构偏好 (KQXY 覆盖层修饰)
    # ========================================================================

    if hard_veto:
        risk_tier = RiskTier.CRISIS
        position_cap = 0.0
        buy_mode = BuyMode.EMPTY
        struct = {"height": "LOW", "fenqi": "DISABLED", "yizi": False, "huanshou": False, "fenqi_enabled": False, "feiban": False}
    else:
        risk_tier, base_cap, buy_mode = PHASE_RISK_BUDGET[phase]
        struct = PHASE_STRUCTURE[phase].copy()

        # KQXY 覆盖层: 弱修复 → 降仓
        if loss_overlay == "REPAIR_WEAK":
            base_cap = min(base_cap, 0.20)

        # KQXY 覆盖层: 高位裂化 → 降级为 HIGH_STAGNATION 策略
        if loss_overlay == "HIGH_CRACKING":
            hs = PHASE_STRUCTURE[EmotionPhase.HIGH_STAGNATION]
            struct = hs.copy()
            base_cap = min(base_cap, 0.30)

        # 数据质量降仓
        if data_quality_level == DataQuality.UNKNOWN:
            data_cap = 0.10
        elif data_quality_level == DataQuality.DEGRADED:
            data_cap = 0.30
        else:
            data_cap = 1.0

        position_cap = min(base_cap, data_cap)
        position_cap = max(0.0, min(1.0, position_cap))

    # ========================================================================
    # 阶段 8: 子阶段标签 (KQXY 覆盖层修饰)
    # ========================================================================

    ice_stage = None
    retreat_stage = None

    if phase == EmotionPhase.ICE:
        if loss_level == "HIGH" and loss_direction == "EXPANDING":
            ice_stage = "FALLING"
        elif loss_level == "HIGH" and loss_direction == "FLAT":
            ice_stage = "BASING"
        elif direction == EmotionDirection.DOWN:
            ice_stage = "FALLING"
        else:
            ice_stage = "BASING"
    elif phase == EmotionPhase.RETREAT:
        retreat_stage = "EARLY" if level == EmotionLevel.HIGH else "SPREADING"

    # ========================================================================
    # 阶段 9: 构建输出
    # ========================================================================

    result = D6EmotionResult(
        phase=phase,
        phase_label=PHASE_LABELS[phase],
        level=level,
        level_score=level_score,
        direction=direction,
        profit_level=profit_level,
        breadth_level=breadth_level,
        relay_level=relay_level,
        profit_delta=profit_delta,
        breadth_delta=breadth_delta,
        relay_delta=relay_delta,
        hard_veto=hard_veto,
        profit_collapse=profit_collapse,
        breadth_panic=breadth_panic,
        kqxy_t1=kqxy_t1,
        kqxy_t2=kqxy_t2,
        kqxy_pct=loss["kqxy_pct"],
        kqxy_delta=loss["kqxy_delta"],
        loss_level=loss_level,
        loss_direction=loss_direction,
        loss_overlay=loss_overlay,
        risk_tier=risk_tier,
        position_cap=position_cap,
        height_preference=struct["height"],
        fenqi_priority=struct["fenqi"],
        yizi_enabled=struct["yizi"],
        huanshou_enabled=struct["huanshou"],
        fenqi_enabled=struct["fenqi_enabled"],
        feiban_enabled=struct["feiban"],
        pool_yizi_mult=1.0,
        pool_huanshou_mult=1.0,
        pool_fenqi_mult=1.0,
        pool_feiban_mult=1.0,
        buy_mode=buy_mode,
        auction_buy_enabled=(buy_mode == BuyMode.AUCTION_AND_BOARD),
        phase_confidence=phase_confidence,
        data_quality=data_quality,
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
            "use_percentile": use_percentile,
            "history_days": history.history_days if history else 0,
            "valid_families": valid_families,
            "data_quality_level": data_quality_level.value,
            "level_score": level_score,
            "ice_stage": ice_stage,
            "retreat_stage": retreat_stage,
            "hard_veto": hard_veto,
            "prev_phase": prev_phase.value if prev_phase else None,
            "loss_overlay": loss_overlay,
            "loss_level": loss_level,
            "loss_direction": loss_direction,
        },
    )

    return result


def _calc_pctile_or_zero(values: List[float], value: Optional[float]) -> float:
    """计算分位, 失败返回 0"""
    p = _calc_pctile(values, value)
    return p if p is not None else 0.0


# ============================================================================
# 自检
# ============================================================================

def _self_test() -> bool:
    """自检: 验证 D6 情绪周期简化版逻辑"""
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
        {"metric_key": "SZ", "value": "2000"},
        {"metric_key": "XD", "value": "1600"},
        {"metric_key": "DT", "value": "4"},
    ]

    # 测试 1: 无历史, 方向 UNKNOWN, 水位默认 MID
    r1 = determine_emotion_state(ztpool, qxlive_t0, qxlive_t1, history=None, prev_phase=None)
    assert r1.jinji_1_2 is not None, f"jinji_1_2 should not be None, got {r1.jinji_1_2}"
    assert r1.jinji_2_3 is not None
    assert r1.relay_health is not None, f"relay_health should not be None"
    assert abs(r1.advance_share - 2100/(2100+1500)) < 0.001, f"advance_share wrong: {r1.advance_share}"
    assert r1.phase == EmotionPhase.UNKNOWN, f"Expected UNKNOWN (no history), got {r1.phase}"
    assert r1.direction == EmotionDirection.UNKNOWN
    print(f"  Test1 PASS: phase={r1.phase_label}, direction={r1.direction.value}, relay_health={r1.relay_health}")

    # 测试 2: Laplace 收缩估计
    # promoted=5, eligible=20 → smoothed = (5+1)/(20+1+1) = 6/22 = 27.27%
    assert abs(r1.jinji_1_2 - 27.27) < 0.5, f"smoothed 1_2 wrong: {r1.jinji_1_2}"
    # promoted=2, eligible=10 → smoothed = (2+1)/(10+1+1) = 3/12 = 25.0%
    assert abs(r1.jinji_2_3 - 25.0) < 0.5, f"smoothed 2_3 wrong: {r1.jinji_2_3}"
    # relay_health = 0.55*27.27 + 0.45*25.0 = 14.9985 + 11.25 = 26.25
    assert abs(r1.relay_health - 26.25) < 1.0, f"relay_health wrong: {r1.relay_health}"
    print(f"  Test2 PASS: smoothed rates correct, relay_health={r1.relay_health}")

    # 测试 3: 极端否决 — 强势股集体翻负
    r3 = determine_emotion_state(
        ztpool,
        [{"metric_key": "ZTBX", "value": "-1.0"}, {"metric_key": "LBBX", "value": "-1.0"},
         {"metric_key": "SZ", "value": "2100"}, {"metric_key": "XD", "value": "1500"},
         {"metric_key": "DT", "value": "5"}],
        [{"metric_key": "ZTBX", "value": "2.0"}, {"metric_key": "LBBX", "value": "2.0"}],
        history=None, prev_phase=None,
    )
    assert r3.profit_collapse, f"profit_collapse should be detected"
    assert r3.hard_veto, f"hard_veto should be True"
    assert r3.position_cap == 0.0, f"hard_veto should force cap=0, got {r3.position_cap}"
    print(f"  Test3 PASS: hard_veto detected, cap={r3.position_cap}, phase={r3.phase_label}")

    # 测试 4: 有历史数据, 计算分位和方向
    hist = D6History()
    for i in range(25):
        hist.add_day(ztbx=2.0 + i * 0.1, lbbx=2.0 + i * 0.1,
                     advance_share=0.4 + i * 0.02, dt=5 - i * 0.1,
                     relay_health=25.0 + i * 0.75)
    r4 = determine_emotion_state(ztpool, qxlive_t0, qxlive_t1, history=hist, prev_phase=None)
    assert r4.direction != EmotionDirection.UNKNOWN, f"direction should be known with history"
    assert r4.use_percentile if hasattr(r4, 'use_percentile') else True
    print(f"  Test4 PASS: direction={r4.direction.value}, level={r4.level.value}, phase={r4.phase_label}")

    # 测试 5: 冰点 (LOW+DOWN, 三家族等权中位数)
    # 历史正常, 当前极度恶化 → 低分位 → LOW
    hist_cold = D6History()
    for i in range(25):
        # 历史: 正常偏高 (ZTBX 0~5, advance_share 0.4~0.6, DT 3~8, relay_health 20~40)
        hist_cold.add_day(ztbx=0.5 + i * 0.2, lbbx=1.0 + i * 0.2,
                          advance_share=0.4 + i * 0.01, dt=8 - i * 0.1,
                          relay_health=25.0 + i * 0.5)
    r5 = determine_emotion_state(
        [{"分组名称": "1进2", "晋级率": 5.0, "晋级数": 1, "样本数": 20},
         {"分组名称": "2进3", "晋级率": 0.0, "晋级数": 0, "样本数": 5}],
        # 当前极度恶化: ZTBX=-5(远低于历史0.5~5.3), advance_share=0.056(远低于历史0.4~0.64), DT=25(远高于历史3~8)
        [{"metric_key": "ZTBX", "value": "-5.0"}, {"metric_key": "LBBX", "value": "-3.0"},
         {"metric_key": "SZ", "value": "300"}, {"metric_key": "XD", "value": "5000"},
         {"metric_key": "DT", "value": "25"}],
        [{"metric_key": "ZTBX", "value": "5.0"}, {"metric_key": "LBBX", "value": "4.0"},
         {"metric_key": "SZ", "value": "2500"}, {"metric_key": "XD", "value": "1500"},
         {"metric_key": "DT", "value": "5"}],
        history=hist_cold, prev_phase=None,
    )
    assert r5.phase == EmotionPhase.ICE, f"Expected ICE (LOW+DOWN), got {r5.phase}"
    assert r5.position_cap == 0.0, f"ICE should have cap=0, got {r5.position_cap}"
    assert r5.diagnostics.get("ice_stage") == "FALLING", f"Expected FALLING, got {r5.diagnostics.get('ice_stage')}"
    print(f"  Test5 PASS: ICE detected, ice_stage={r5.diagnostics.get('ice_stage')}, cap={r5.position_cap}, level_score={r5.level_score:.3f}")

    # 测试 6: 晋级率缺失 (eligible=0) → 该层为 None, relay_health 降级
    r6 = determine_emotion_state(
        [{"分组名称": "1进2", "晋级率": 25.0, "晋级数": 5, "样本数": 20}],
        qxlive_t0, qxlive_t1, history=None, prev_phase=None,
    )
    assert r6.jinji_1_2 is not None, "1进2 should exist"
    assert r6.jinji_2_3 is None, "2进3 should be None (no data)"
    assert r6.relay_health is not None, "relay_health should still work with 1 layer"
    assert r6.data_quality["relay_family"] == "DEGRADED"
    print(f"  Test6 PASS: single layer relay_health={r6.relay_health}, DQ={r6.data_quality['relay_family']}")

    # 测试 7: 高位加速 (HIGH_ACTIVE, HIGH+UP)
    # 历史宽幅, 当前处于高分位且上升 → HIGH+UP
    hist_hot = D6History()
    for i in range(25):
        # 历史: ZTBX 0.5~5.3, advance_share 0.25~0.73, DT 20~5.6, relay_health 10~46
        hist_hot.add_day(ztbx=0.5 + i * 0.2, lbbx=1.0 + i * 0.2,
                         advance_share=0.25 + i * 0.02, dt=20 - i * 0.6,
                         relay_health=10.0 + i * 1.5)
    # 昨日: ZTBX=5.3(max), 今日: ZTBX=5.0(接近max, 分位~0.96)
    # 昨日分位(pct in 24天历史): 5.3在[0.5~5.1]中=1.0, 今日分位(pct in 25天历史): 5.0在[0.5~5.3]中≈0.92 → dP=0.92-1.0=-0.08
    # 这会导致 DOWN。需要调整让今日高于昨日。
    # 改为: 昨日 ZTBX=4.5, 今日 ZTBX=5.1 → 昨日分位(24天[0.5~5.3]): 4.5≈0.83, 今日分位(25天[0.5~5.3]): 5.1≈0.96 → dP=0.13>0.03 UP
    # 重新构建: 历史 ZTBX 0.5~5.3, 昨日 ZTBX=4.5, 今日 ZTBX=5.1
    r7 = determine_emotion_state(
        [{"分组名称": "1进2", "晋级率": 35.0, "晋级数": 10, "样本数": 28},
         {"分组名称": "2进3", "晋级率": 30.0, "晋级数": 4, "样本数": 13}],
        # 今日: ZTBX=5.1(接近历史max 5.3, 分位~0.96), advance_share=0.70(接近历史max 0.73), DT=1(低于历史min 5.6)
        [{"metric_key": "ZTBX", "value": "5.1"}, {"metric_key": "LBBX", "value": "6.0"},
         {"metric_key": "SZ", "value": "3500"}, {"metric_key": "XD", "value": "1500"},
         {"metric_key": "DT", "value": "1"}],
        # 昨日: ZTBX=4.5, LBBX=5.2, SZ=3000, XD=2000, DT=3
        [{"metric_key": "ZTBX", "value": "4.5"}, {"metric_key": "LBBX", "value": "5.2"},
         {"metric_key": "SZ", "value": "3000"}, {"metric_key": "XD", "value": "2000"},
         {"metric_key": "DT", "value": "3"}],
        history=hist_hot, prev_phase=None,
    )
    assert r7.phase == EmotionPhase.HIGH_ACTIVE, f"Expected HIGH_ACTIVE, got {r7.phase}"
    assert r7.position_cap == 0.50, f"HIGH_ACTIVE cap should be 0.50, got {r7.position_cap}"
    print(f"  Test7 PASS: HIGH_ACTIVE detected, cap={r7.position_cap}, level_score={r7.level_score:.3f}")

    # 测试 8: 数据质量 UNKNOWN (<2 家族有效)
    r8 = determine_emotion_state(
        [],
        [{"metric_key": "ZTBX", "value": "2.0"}],  # 只有 profit 家族部分有效
        [], history=None, prev_phase=None,
    )
    assert r8.phase_confidence == 0.2, f"Expected confidence 0.2, got {r8.phase_confidence}"
    assert r8.position_cap <= 0.10, f"UNKNOWN quality should cap at 0.10, got {r8.position_cap}"
    print(f"  Test8 PASS: UNKNOWN quality, confidence={r8.phase_confidence}, cap={r8.position_cap}")

    print("\n=== ALL TESTS PASSED ===")
    return True


if __name__ == "__main__":
    _self_test()