#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_d6_emotion.py  --  v4.2 D6 情绪周期状态机 (v4 双时间截面版)
============================================================================
版本演进
============================================================================
v3 → v4 核心变更: 引入"双时间截面"架构

问题背景:
  v3 中 P/B 家族的水位和方向都使用盘前 9:25 数据, 而 R 家族使用盘后数据。
  三大家族测量的不是同一个时间截面的市场状态, 导致:
  1. 盘前数据信息量低, 竞价瞬间易受大单干扰
  2. 当竞价与全天走势背离时, P/B 水位与实际市场底色严重脱节
  3. R 家族方向计算存在逻辑缺陷: 用同一个 T-1 值在"包含自己"和"不包含自己"
     两个分布中求分位差, 实际测量的是"偏离中位数的方向"而非"日间变化"

v4 双时间截面设计:
  ┌──────────────────────────────────────────────────────────────┐
  │  市场底色 (盘后)  →  水位 (Level)  →  决定仓位上限、风险等级   │
  │  竞价情绪 (盘前)  →  方向 (Direction) → 决定进攻/防守方向      │
  │  Phase = 水位 × 方向 → 七宫格 (不变)                         │
  └──────────────────────────────────────────────────────────────┘

具体改动:
  1. D6History: 新增盘后 P/B 字段 (ztbx_close/lbbx_close/advance_share_close/dt_close)
  2. 水位计算: 盘后值在盘后历史分布中求分位, 盘后数据不足时回退盘前
  3. 方向计算: P/B 保持盘前 vs 盘前; R 修复为 T-1 relay vs T-2 relay 日间变化
  4. KQXY: 自动从 history.kqxy_values 提取, 不再依赖调用方传入 (修复 kqxy_t1/t2 始终为 None 的问题)
  5. 极端否决: 仍用盘前数据 (竞价瞬间的极端信号需要即时响应)

设计哲学 (不变)
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
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import math

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
    ICE = "ICE"                         # 冰点: LOW+FLAT/DOWN
    REPAIR = "REPAIR"                   # 冰点修复: LOW+UP
    # 中位
    CHOP = "CHOP"                       # 震荡混沌: MID+FLAT
    RETREAT = "RETREAT"                 # 退潮: MID+DOWN / HIGH+DOWN
    EXPANSION = "EXPANSION"             # 发酵主升: MID+UP
    # 高位
    HIGH_ACTIVE = "HIGH_ACTIVE"         # 高位加速: HIGH+UP
    HIGH_STAGNATION = "HIGH_STAGNATION" # 高位钝化: HIGH+FLAT
    # 数据不足
    UNKNOWN = "UNKNOWN"

class RiskTier(Enum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRISIS = "CRISIS"

class BuyMode(Enum):
    AUCTION_AND_BOARD = "auction_and_board"   # 竞价+排板
    BOARD_ONLY = "board_only"                 # 排板为主
    OBSERVE_ONLY = "observe_only"             # 仅观察
    EMPTY = "empty"                           # 空仓

class DataQuality(Enum):
    GOOD = "GOOD"           # 3个家族全有效
    DEGRADED = "DEGRADED"   # 2个家族有效
    UNKNOWN = "UNKNOWN"     # <2个家族有效

# ============================================================================
# 数据类
# ============================================================================
@dataclass
class D6History:
    """
    D6 历史数据 — v4 双时间截面版。

    盘前 (pre) 系列: 9:25 竞价快照, 用于方向计算 (竞价情绪)
    盘后 (close) 系列: 收盘快照, 用于水位计算 (市场底色)
    接力生态 (relay_health): 仅盘后 (来自 ztpool 晋级率)
    KQXY: 仅盘后 (亏钱效应)
    QX: 盘前 (pre_qx) + 盘后 (close_qx), 仅展示
    """

    # === 盘前 (9:25) — 竞价情绪, 用于方向计算 ===
    ztbx_pre_values: List[float] = field(default_factory=list)
    lbbx_pre_values: List[float] = field(default_factory=list)
    advance_share_pre_values: List[float] = field(default_factory=list)
    dt_pre_values: List[float] = field(default_factory=list)

    # === 盘后 (收盘) — 市场底色, 用于水位计算 ===
    ztbx_close_values: List[float] = field(default_factory=list)
    lbbx_close_values: List[float] = field(default_factory=list)
    advance_share_close_values: List[float] = field(default_factory=list)
    dt_close_values: List[float] = field(default_factory=list)

    # === 接力生态 (仅盘后) ===
    relay_health_values: List[float] = field(default_factory=list)

    # === KQXY 亏钱效应 (盘后) ===
    kqxy_values: List[float] = field(default_factory=list)

    # === QX 综合情绪 ===
    pre_qx_values: List[float] = field(default_factory=list)      # 盘前 QX@9:25
    close_qx_values: List[float] = field(default_factory=list)    # 盘后 QX 收盘

    _WINDOW = 60
    _MIN_DAYS_FOR_PCTILE = 20

    # ========================================================================
    # 向后兼容属性 — 旧代码可能引用旧字段名
    # ========================================================================
    @property
    def ztbx_values(self) -> List[float]:
        """向后兼容: 返回盘前 ZTBX (旧代码中的 ztbx_values)"""
        return self.ztbx_pre_values

    @property
    def lbbx_values(self) -> List[float]:
        """向后兼容: 返回盘前 LBBX"""
        return self.lbbx_pre_values

    @property
    def advance_share_values(self) -> List[float]:
        """向后兼容: 返回盘前 上涨占比"""
        return self.advance_share_pre_values

    @property
    def dt_values(self) -> List[float]:
        """向后兼容: 返回盘前 DT"""
        return self.dt_pre_values

    # ========================================================================
    # add_day — v4 双时间截面版
    # ========================================================================
    def add_day(self,
                # 盘前 (9:25)
                ztbx_pre: Optional[float] = None,
                lbbx_pre: Optional[float] = None,
                advance_share_pre: Optional[float] = None,
                dt_pre: Optional[float] = None,
                # 盘后 (收盘)
                ztbx_close: Optional[float] = None,
                lbbx_close: Optional[float] = None,
                advance_share_close: Optional[float] = None,
                dt_close: Optional[float] = None,
                # 其他
                relay_health: Optional[float] = None,
                kqxy: Optional[float] = None,
                pre_qx: Optional[float] = None,
                close_qx: Optional[float] = None,
                # === 向后兼容旧参数名 ===
                ztbx: Optional[float] = None,
                lbbx: Optional[float] = None,
                advance_share: Optional[float] = None,
                dt: Optional[float] = None,
                ) -> None:
        """
        添加一个交易日的数据。

        新代码应使用 ztbx_pre/ztbx_close 等明确参数。
        旧参数名 (ztbx/lbbx/advance_share/dt) 作为向后兼容,
        自动映射到盘前字段 (与 v3 行为一致)。
        """
        # 向后兼容: 旧参数名 → 盘前
        if ztbx is not None and ztbx_pre is None:
            ztbx_pre = ztbx
        if lbbx is not None and lbbx_pre is None:
            lbbx_pre = lbbx
        if advance_share is not None and advance_share_pre is None:
            advance_share_pre = advance_share
        if dt is not None and dt_pre is None:
            dt_pre = dt

        # 盘前
        if ztbx_pre is not None:
            self.ztbx_pre_values.append(ztbx_pre)
        if lbbx_pre is not None:
            self.lbbx_pre_values.append(lbbx_pre)
        if advance_share_pre is not None:
            self.advance_share_pre_values.append(advance_share_pre)
        if dt_pre is not None:
            self.dt_pre_values.append(dt_pre)

        # 盘后
        if ztbx_close is not None:
            self.ztbx_close_values.append(ztbx_close)
        if lbbx_close is not None:
            self.lbbx_close_values.append(lbbx_close)
        if advance_share_close is not None:
            self.advance_share_close_values.append(advance_share_close)
        if dt_close is not None:
            self.dt_close_values.append(dt_close)

        # 其他
        if relay_health is not None:
            self.relay_health_values.append(relay_health)
        if kqxy is not None:
            self.kqxy_values.append(kqxy)
        if pre_qx is not None:
            self.pre_qx_values.append(pre_qx)
        if close_qx is not None:
            self.close_qx_values.append(close_qx)

    # ========================================================================
    # 分位数计算
    # ========================================================================
    def _pctile(self, values: List[float], q: float) -> Optional[float]:
        """计算分位数, 需要至少 MIN_DAYS_FOR_PCTILE 天"""
        if len(values) < self._MIN_DAYS_FOR_PCTILE:
            return None
        sv = sorted(values)
        idx = int(len(sv) * q)
        return sv[min(idx, len(sv) - 1)]

    def percentile(self, values: List[float], q: float) -> Optional[float]:
        return self._pctile(values, q)

    # ========================================================================
    # 数据充足性检查
    # ========================================================================
    def valid_for_close_pctile(self) -> bool:
        """盘后数据是否足够计算分位数 (用于水位)"""
        return (len(self.ztbx_close_values) >= self._MIN_DAYS_FOR_PCTILE
                and len(self.advance_share_close_values) >= self._MIN_DAYS_FOR_PCTILE
                and len(self.relay_health_values) >= self._MIN_DAYS_FOR_PCTILE)

    def valid_for_pre_pctile(self) -> bool:
        """盘前数据是否足够计算分位数 (用于方向)"""
        return (len(self.ztbx_pre_values) >= self._MIN_DAYS_FOR_PCTILE
                and len(self.advance_share_pre_values) >= self._MIN_DAYS_FOR_PCTILE
                and len(self.relay_health_values) >= self._MIN_DAYS_FOR_PCTILE)

    # 向后兼容
    def valid_for_pctile(self) -> bool:
        """向后兼容: 优先检查盘后, 回退盘前"""
        return self.valid_for_close_pctile() or self.valid_for_pre_pctile()

    def valid_for_kqxy_pctile(self) -> bool:
        """KQXY 历史是否足够计算分位数"""
        return len(self.kqxy_values) >= self._MIN_DAYS_FOR_PCTILE

    # ========================================================================
    # 极端否决阈值 (仍用盘前数据 — 竞价瞬间的极端信号)
    # ========================================================================
    def advance_share_15pct(self) -> Optional[float]:
        return self._pctile(self.advance_share_pre_values, 0.15)

    def dt_85pct(self) -> Optional[float]:
        return self._pctile(self.dt_pre_values, 0.85)

    # ========================================================================
    # KQXY 分位阈值
    # ========================================================================
    def kqxy_30pct(self) -> Optional[float]:
        return self._pctile(self.kqxy_values, 0.30)

    def kqxy_70pct(self) -> Optional[float]:
        return self._pctile(self.kqxy_values, 0.70)

    # ========================================================================
    # 属性
    # ========================================================================
    @property
    def min_days(self) -> int:
        """三家族最短历史天数 (用于向后兼容)"""
        return min(
            len(self.ztbx_pre_values),
            len(self.advance_share_pre_values),
            len(self.relay_health_values),
        )

    @property
    def history_days(self) -> int:
        return self.min_days

    @property
    def close_days(self) -> int:
        """盘后数据天数"""
        return min(
            len(self.ztbx_close_values),
            len(self.advance_share_close_values),
            len(self.relay_health_values),
        )

    def last_values(self) -> Dict[str, Optional[float]]:
        """获取最近一日的盘前值, 用于向后兼容"""
        return {
            "ztbx": self.ztbx_pre_values[-1] if self.ztbx_pre_values else None,
            "lbbx": self.lbbx_pre_values[-1] if self.lbbx_pre_values else None,
            "advance_share": self.advance_share_pre_values[-1] if self.advance_share_pre_values else None,
            "dt": self.dt_pre_values[-1] if self.dt_pre_values else None,
            "relay_health": self.relay_health_values[-1] if self.relay_health_values else None,
        }


@dataclass
class D6EmotionResult:
    """D6 情绪周期完整输出 (v4 双时间截面版)"""

    # === 周期主状态 ===
    phase: EmotionPhase = EmotionPhase.UNKNOWN
    phase_label: str = "数据不足"

    # === 2D定位 ===
    level: EmotionLevel = EmotionLevel.MID
    level_score: float = 0.5            # 总水位分 (盘后口径, 市场底色)
    direction: EmotionDirection = EmotionDirection.UNKNOWN

    # === 双时间截面水位 (v4 新增) ===
    close_level_score: float = 0.5       # 盘后水位分 (市场底色, 用于风险决策)
    pre_level_score: float = 0.5         # 盘前水位分 (竞价情绪, 仅供参考)
    level_source: str = "UNKNOWN"        # 水位数据来源: "CLOSE" / "PRE" / "STATIC"

    # === 三个家族水位 (盘后口径) ===
    profit_level: float = 0.5
    breadth_level: float = 0.5
    relay_level: float = 0.5

    # === 三个家族日变化 (盘前口径 for P/B, 盘后口径 for R) ===
    profit_delta: Optional[float] = None
    breadth_delta: Optional[float] = None
    relay_delta: Optional[float] = None

    # === 极端否决 (仍用盘前数据) ===
    hard_veto: bool = False
    profit_collapse: bool = False
    breadth_panic: bool = False

    # === 风险 ===
    risk_tier: RiskTier = RiskTier.NORMAL
    position_cap: float = 1.0

    # === 结构偏好 ===
    height_preference: str = "MID"
    fenqi_priority: str = "NORMAL"
    yizi_enabled: bool = True
    huanshou_enabled: bool = True
    fenqi_enabled: bool = True
    feiban_enabled: bool = True

    # === 池乘子 ===
    pool_yizi_mult: float = 1.0
    pool_huanshou_mult: float = 1.0
    pool_fenqi_mult: float = 1.0
    pool_feiban_mult: float = 1.0

    # === 执行 ===
    buy_mode: BuyMode = BuyMode.AUCTION_AND_BOARD
    auction_buy_enabled: bool = True

    # === KQXY 亏钱效应覆盖层 ===
    kqxy_t1: Optional[float] = None
    kqxy_t2: Optional[float] = None
    kqxy_pct: Optional[float] = None
    kqxy_delta: Optional[float] = None
    loss_level: str = "UNKNOWN"
    loss_direction: str = "UNKNOWN"
    loss_overlay: str = "NONE"

    # === QX 综合情绪 (仅展示) ===
    qx_925: Optional[float] = None
    qx_stats: Dict[str, Any] = field(default_factory=lambda: {
        "pre_qx": {"median": None, "std": None, "band1_low": None, "band1_high": None,
                   "band2_low": None, "band2_high": None, "n": 0},
        "close_qx": {"median": None, "std": None, "band1_low": None, "band1_high": None,
                     "band2_low": None, "band2_high": None, "n": 0},
        "today_position": "UNKNOWN",
    })

    # === 质量 ===
    phase_confidence: float = 0.0
    data_quality: Dict[str, str] = field(default_factory=lambda: {
        "profit_family": "MISSING",
        "breadth_family": "MISSING",
        "relay_family": "MISSING",
    })

    # === 原始指标 (盘前口径 for display) ===
    ztbx_925: Optional[float] = None
    lbbx_925: Optional[float] = None
    advance_share: Optional[float] = None
    dt_925: Optional[int] = None
    jinji_1_2: Optional[float] = None
    jinji_2_3: Optional[float] = None
    relay_health: Optional[float] = None

    # === 盘后原始指标 (v4 新增, 用于透明度) ===
    ztbx_close: Optional[float] = None
    lbbx_close: Optional[float] = None
    advance_share_close: Optional[float] = None
    dt_close: Optional[int] = None

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
# 核心逻辑 — 数据提取
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
        if "1进2" in ladder:
            key = "PBBX_1_2"
        elif "2进3" in ladder:
            key = "PBBX_2_3"
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
        low_threshold = LEVEL_LOW_EXIT      # 0.40 — 从 LOW 退出需要更高
    else:
        low_threshold = LEVEL_LOW_THRESHOLD # 0.30

    if prev_level == EmotionLevel.HIGH:
        high_threshold = LEVEL_HIGH_EXIT    # 0.60 — 从 HIGH 退出需要更低
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


def _compute_qx_stats(history: Optional[D6History], qx_925: Optional[float]) -> Dict[str, Any]:
    """
    计算 QX 盘前/盘后统计 (仅展示, 不参与决策)。
    """
    result: Dict[str, Any] = {
        "pre_qx": {"median": None, "std": None, "band1_low": None, "band1_high": None,
                   "band2_low": None, "band2_high": None, "n": 0},
        "close_qx": {"median": None, "std": None, "band1_low": None, "band1_high": None,
                     "band2_low": None, "band2_high": None, "n": 0},
        "today_position": "UNKNOWN",
    }

    def _stats(values: List[float]) -> Dict[str, Any]:
        if len(values) < 5:
            return {"median": None, "std": None, "band1_low": None, "band1_high": None,
                    "band2_low": None, "band2_high": None, "n": len(values)}
        sv = sorted(values)
        n = len(sv)
        median = sv[n // 2] if n % 2 == 1 else (sv[n // 2 - 1] + sv[n // 2]) / 2.0
        mean = sum(sv) / n
        variance = sum((v - mean) ** 2 for v in sv) / n
        std = math.sqrt(variance)
        return {
            "median": round(median, 4),
            "std": round(std, 4),
            "band1_low": round(median - std, 4),
            "band1_high": round(median + std, 4),
            "band2_low": round(median - 2 * std, 4),
            "band2_high": round(median + 2 * std, 4),
            "n": n,
        }

    if history is not None:
        result["pre_qx"] = _stats(history.pre_qx_values[-20:])
        result["close_qx"] = _stats(history.close_qx_values[-20:])

    if qx_925 is not None and result["close_qx"]["median"] is not None:
        m = result["close_qx"]["median"]
        s = result["close_qx"]["std"]
        if s is not None and s > 0:
            if qx_925 < m - 2 * s:
                result["today_position"] = "LOW_TAIL"
            elif qx_925 < m - s:
                result["today_position"] = "BELOW_1SIGMA"
            elif qx_925 <= m + s:
                result["today_position"] = "WITHIN_1SIGMA"
            elif qx_925 <= m + 2 * s:
                result["today_position"] = "ABOVE_1SIGMA"
            else:
                result["today_position"] = "HIGH_TAIL"

    return result


def _classify_phase(level: EmotionLevel, direction: EmotionDirection) -> EmotionPhase:
    """水位 × 方向 → 七宫格相位"""
    if level == EmotionLevel.LOW:
        if direction == EmotionDirection.UP:
            return EmotionPhase.REPAIR
        else:
            return EmotionPhase.ICE  # FLAT → ICE(BASING), DOWN → ICE(FALLING)
    elif level == EmotionLevel.MID:
        if direction == EmotionDirection.UP:
            return EmotionPhase.EXPANSION
        elif direction == EmotionDirection.DOWN:
            return EmotionPhase.RETREAT  # SPREADING
        else:
            return EmotionPhase.CHOP
    else:  # HIGH
        if direction == EmotionDirection.UP:
            return EmotionPhase.HIGH_ACTIVE
        elif direction == EmotionDirection.DOWN:
            return EmotionPhase.RETREAT  # EARLY
        else:
            return EmotionPhase.HIGH_STAGNATION


def _classify_loss_overlay(
    kqxy_t1: Optional[float],
    kqxy_t2: Optional[float],
    history: Optional[D6History],
    epsilon: float = 0.03,
) -> Dict[str, Any]:
    """
    KQXY 亏钱效应覆盖层。
    KQXY 是 T-1 盘后指标, 不参与 T0 核心 P/B/R 判定。
    只作为 Phase 修饰层。
    """
    result: Dict[str, Any] = {
        "kqxy_t1": kqxy_t1, "kqxy_t2": kqxy_t2,
        "kqxy_pct": None, "kqxy_delta": None,
        "loss_level": "UNKNOWN", "loss_direction": "UNKNOWN",
        "loss_overlay": "NONE",
    }

    if kqxy_t1 is None:
        return result

    # KQXY 分位: 优先用历史序列算真实分位, 不足时用静态阈值
    if history is not None and history.valid_for_kqxy_pctile():
        kqxy_pct = _calc_pctile(history.kqxy_values, kqxy_t1)
        kqxy_pct = kqxy_pct if kqxy_pct is not None else 0.5
    else:
        kqxy_pct = kqxy_t1 / 100.0
        kqxy_pct = max(0.0, min(1.0, kqxy_pct))
    result["kqxy_pct"] = round(kqxy_pct, 4)

    # KQXY 水位
    if history is not None and history.valid_for_kqxy_pctile():
        k30 = history.kqxy_30pct()
        k70 = history.kqxy_70pct()
        if k30 is not None and k70 is not None:
            if kqxy_t1 < k30:
                result["loss_level"] = "LOW"
            elif kqxy_t1 > k70:
                result["loss_level"] = "HIGH"
            else:
                result["loss_level"] = "MID"
        else:
            if kqxy_pct < 0.30:
                result["loss_level"] = "LOW"
            elif kqxy_pct > 0.70:
                result["loss_level"] = "HIGH"
            else:
                result["loss_level"] = "MID"
    else:
        if kqxy_pct < 0.30:
            result["loss_level"] = "LOW"
        elif kqxy_pct > 0.70:
            result["loss_level"] = "HIGH"
        else:
            result["loss_level"] = "MID"

    # KQXY 方向: 用真实分位差
    if kqxy_t2 is not None:
        if history is not None and history.valid_for_kqxy_pctile():
            prev_pct = _calc_pctile(history.kqxy_values, kqxy_t2)
            prev_pct = prev_pct if prev_pct is not None else kqxy_t2 / 100.0
        else:
            prev_pct = kqxy_t2 / 100.0
        kqxy_delta = kqxy_pct - prev_pct
        result["kqxy_delta"] = round(kqxy_delta, 4)
        if kqxy_delta > epsilon:
            result["loss_direction"] = "EXPANDING"
        elif kqxy_delta < -epsilon:
            result["loss_direction"] = "CONTRACTING"
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
    qxlive_close_t1: Optional[List[Dict[str, Any]]] = None,  # v4 新增: T-1 盘后 qxlive
    history: Optional[D6History] = None,
    static_thresholds: Optional[Dict[str, float]] = None,
    prev_phase: Optional[EmotionPhase] = None,
    kqxy_t1: Optional[float] = None,       # 保留但优先从 history 自动提取
    kqxy_t2: Optional[float] = None,       # 保留但优先从 history 自动提取
) -> D6EmotionResult:
    """
    主入口: D6 情绪周期判定 (v4 双时间截面版)。

    双时间截面架构:
    ┌──────────────┬──────────────────┬─────────────────────────────┐
    │ 数据源        │ 用途              │ 时间截面                     │
    ├──────────────┼──────────────────┼─────────────────────────────┤
    │ qxlive_close │ 水位 (Level)      │ T-1 盘后 (市场底色)          │
    │ qxlive_top   │ 方向 (Direction)  │ T0/T-1 盘前 (竞价情绪)       │
    │ ztpool       │ 接力生态 (R)      │ T-1 盘后                     │
    └──────────────┴──────────────────┴─────────────────────────────┘

    水位计算优先级:
      1. 盘后值在盘后历史分布中求分位 (最可靠)
      2. 盘前值在盘前历史分布中求分位 (向后兼容)
      3. 静态默认值 0.5 (数据不足)

    方向计算:
      P/B: T0 盘前 vs T-1 盘前 (同口径同时间点, 竞价情绪变化)
      R:   T-1 relay vs T-2 relay (修复 v3 bug, 真日间变化)

    KQXY: 优先从 history.kqxy_values 自动提取, 显式传入的参数作为覆盖。

    Args:
        ztpool_t1: T-1 ztpool 数据 (用于 relay_health 计算)
        qxlive_top_t0: T0 9:25 qxlive 指标 (盘前)
        qxlive_top_t1: T-1 9:25 qxlive 指标 (盘前)
        qxlive_close_t1: T-1 盘后 qxlive 指标 (收盘, v4 新增)
        history: 滚动历史数据 (≥20天启用分位)
        static_thresholds: 静态阈值覆盖
        prev_phase: 前一交易日相位 (用于滞回)
        kqxy_t1: T-1 盘后 KQXY 原始值 (可选, 优先从 history 提取)
        kqxy_t2: T-2 盘后 KQXY 原始值 (可选, 优先从 history 提取)

    Returns:
        D6EmotionResult: 完整情绪周期判定结果
    """
    warnings: List[str] = []
    thresh = static_thresholds or STATIC_DEFAULTS

    # ========================================================================
    # 阶段 0: KQXY 自动提取 (v4 新增)
    # 优先从 history 提取, 显式传入的参数作为覆盖
    # ========================================================================
    if kqxy_t1 is None and history is not None and len(history.kqxy_values) >= 1:
        kqxy_t1 = history.kqxy_values[-1]
    if kqxy_t2 is None and history is not None and len(history.kqxy_values) >= 2:
        kqxy_t2 = history.kqxy_values[-2]

    # ========================================================================
    # 阶段 1: 数据提取
    # ========================================================================

    # --- 盘前数据 (T0 9:25) — 用于方向计算和极端否决 ---
    ztbx_925 = _extract_qxlive_metric(qxlive_top_t0, "ZTBX")
    lbbx_925 = _extract_qxlive_metric(qxlive_top_t0, "LBBX")
    sz_925 = _extract_qxlive_metric(qxlive_top_t0, "SZ")
    xd_925 = _extract_qxlive_metric(qxlive_top_t0, "XD")
    dt_925_raw = _extract_qxlive_metric(qxlive_top_t0, "DT")
    qx_925 = _extract_qxlive_metric(qxlive_top_t0, "QX")

    # T-1 盘前值 (用于方向计算和极端否决)
    ztbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "ZTBX")
    lbbx_t1 = _extract_qxlive_metric(qxlive_top_t1, "LBBX")

    # 上涨占比 (盘前)
    advance_share = None
    if sz_925 is not None and xd_925 is not None and (sz_925 + xd_925) > 0:
        advance_share = round(sz_925 / (sz_925 + xd_925), 4)
    dt_925 = int(dt_925_raw) if dt_925_raw is not None else None

    # --- 盘后数据 (T-1 收盘) — 用于水位计算 (v4 新增) ---
    ztbx_close = None
    lbbx_close = None
    advance_share_close = None
    dt_close = None
    if qxlive_close_t1 is not None:
        ztbx_close = _extract_qxlive_metric(qxlive_close_t1, "ZTBX")
        lbbx_close = _extract_qxlive_metric(qxlive_close_t1, "LBBX")
        sz_close = _extract_qxlive_metric(qxlive_close_t1, "SZ")
        xd_close = _extract_qxlive_metric(qxlive_close_t1, "XD")
        dt_close_raw = _extract_qxlive_metric(qxlive_close_t1, "DT")
        if sz_close is not None and xd_close is not None and (sz_close + xd_close) > 0:
            advance_share_close = round(sz_close / (sz_close + xd_close), 4)
        dt_close = int(dt_close_raw) if dt_close_raw is not None else None

    # --- 晋级率 (T-1 盘后) ---
    pbbx = _extract_ztpool_pbbx(ztpool_t1)
    jinji_1_2_raw = pbbx.get("PBBX_1_2", {})
    jinji_2_3_raw = pbbx.get("PBBX_2_3", {})
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

    # P 家族: 需要盘后 ZTBX+LBBX (水位) 或盘前 ZTBX+LBBX (方向)
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
    # 阶段 3: 三家族水位 (v4 核心变更: 优先使用盘后数据)
    #
    # 水位 = 市场底色, 应使用 T-1 盘后数据 (全天交易结果, 最可靠)。
    # 盘后数据不足时回退盘前, 再不足回退静态默认值。
    # ========================================================================
    use_close_pctile = (history is not None
                        and history.valid_for_close_pctile()
                        and ztbx_close is not None
                        and advance_share_close is not None)

    use_pre_pctile = (history is not None
                      and history.valid_for_pre_pctile()
                      and not use_close_pctile)  # 只在盘后不可用时回退盘前

    if use_close_pctile:
        level_source = "CLOSE"
        # 家族 1: P = median(pct(ZTBX_close), pct(LBBX_close))
        ztbx_pct = _calc_pctile(history.ztbx_close_values, ztbx_close)
        ztbx_pct = ztbx_pct if ztbx_pct is not None else 0.5
        lbbx_pct = _calc_pctile(history.lbbx_close_values, lbbx_close)
        lbbx_pct = lbbx_pct if lbbx_pct is not None else 0.5
        profit_level = round(sorted([ztbx_pct, lbbx_pct])[len([ztbx_pct, lbbx_pct]) // 2], 4)

        # 家族 2: B = median(pct(advance_share_close), 1-pct(DT_close))
        adv_pct = _calc_pctile(history.advance_share_close_values, advance_share_close)
        adv_pct = adv_pct if adv_pct is not None else 0.5
        dt_pct = _calc_pctile(history.dt_close_values, dt_close)
        dt_pct = dt_pct if dt_pct is not None else 0.5
        breadth_level = round(sorted([adv_pct, 1 - dt_pct])[0], 4)

        # 家族 3: R = pct(relay_health) (本身就是盘后, 不变)
        if relay_health is not None:
            relay_level = _calc_pctile(history.relay_health_values, relay_health)
            relay_level = relay_level if relay_level is not None else 0.5
        else:
            relay_level = 0.5

        # 盘前水位分 (仅供参考, 不参与决策)
        if history.valid_for_pre_pctile():
            ztbx_pre_pct = _calc_pctile(history.ztbx_pre_values, ztbx_925)
            ztbx_pre_pct = ztbx_pre_pct if ztbx_pre_pct is not None else 0.5
            lbbx_pre_pct = _calc_pctile(history.lbbx_pre_values, lbbx_925)
            lbbx_pre_pct = lbbx_pre_pct if lbbx_pre_pct is not None else 0.5
            pre_profit = round(sorted([ztbx_pre_pct, lbbx_pre_pct])[len([ztbx_pre_pct, lbbx_pre_pct]) // 2], 4)
            adv_pre_pct = _calc_pctile(history.advance_share_pre_values, advance_share)
            adv_pre_pct = adv_pre_pct if adv_pre_pct is not None else 0.5
            dt_pre_pct = _calc_pctile(history.dt_pre_values, dt_925)
            dt_pre_pct = dt_pre_pct if dt_pre_pct is not None else 0.5
            pre_breadth = round(sorted([adv_pre_pct, 1 - dt_pre_pct])[0], 4)
            pre_level_score = round(sorted([pre_profit, pre_breadth, relay_level])[1], 4)
        else:
            pre_level_score = 0.5

        close_level_score = round(sorted([profit_level, breadth_level, relay_level])[1], 4)
        level_score = close_level_score  # 主水位 = 盘后水位

    elif use_pre_pctile:
        level_source = "PRE"
        # 回退: 盘前值在盘前历史分布中求分位 (与 v3 行为一致)
        ztbx_pct = _calc_pctile(history.ztbx_pre_values, ztbx_925)
        ztbx_pct = ztbx_pct if ztbx_pct is not None else 0.5
        lbbx_pct = _calc_pctile(history.lbbx_pre_values, lbbx_925)
        lbbx_pct = lbbx_pct if lbbx_pct is not None else 0.5
        profit_level = round(sorted([ztbx_pct, lbbx_pct])[len([ztbx_pct, lbbx_pct]) // 2], 4)

        adv_pct = _calc_pctile(history.advance_share_pre_values, advance_share)
        adv_pct = adv_pct if adv_pct is not None else 0.5
        dt_pct = _calc_pctile(history.dt_pre_values, dt_925)
        dt_pct = dt_pct if dt_pct is not None else 0.5
        breadth_level = round(sorted([adv_pct, 1 - dt_pct])[0], 4)

        if relay_health is not None:
            relay_level = _calc_pctile(history.relay_health_values, relay_health)
            relay_level = relay_level if relay_level is not None else 0.5
        else:
            relay_level = 0.5

        level_score = round(sorted([profit_level, breadth_level, relay_level])[1], 4)
        close_level_score = level_score
        pre_level_score = level_score

    else:
        level_source = "STATIC"
        profit_level = 0.5
        breadth_level = 0.5
        relay_level = 0.5
        level_score = 0.5
        close_level_score = 0.5
        pre_level_score = 0.5

    level = _classify_level(level_score, prev_level=_phase_to_level(prev_phase))

    # ========================================================================
    # 阶段 4: 三家族方向 (v4 修复: P/B 盘前 vs 盘前, R 修复为 T-1 vs T-2)
    #
    # P/B 方向: T0 盘前 vs T-1 盘前 (同口径同时间点, 竞价情绪变化)
    # R 方向:   T-1 relay vs T-2 relay (修复 v3 bug)
    # ========================================================================
    profit_delta = None
    breadth_delta = None
    relay_delta = None

    # --- P/B 方向: T0 盘前 vs T-1 盘前 (同口径) ---
    # 提取 T-1 盘前原始值
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
        # 用排除最后一天的 history 计算 T-1 分位 (PIT, 无前视偏差)
        prev_ztbx_vals = history.ztbx_pre_values[:-1] if len(history.ztbx_pre_values) >= 2 else []
        prev_lbbx_vals = history.lbbx_pre_values[:-1] if len(history.lbbx_pre_values) >= 2 else []
        prev_adv_vals = history.advance_share_pre_values[:-1] if len(history.advance_share_pre_values) >= 2 else []
        prev_dt_vals = history.dt_pre_values[:-1] if len(history.dt_pre_values) >= 2 else []

        # Profit 方向: dP = pct(ZTBX_t0, full_hist) - pct(ZTBX_t1, hist_excl_last)
        if ztbx_t1_for_dir is not None and ztbx_925 is not None and prev_ztbx_vals:
            p_prev = _calc_pctile(prev_ztbx_vals, ztbx_t1_for_dir)
            p_curr = _calc_pctile(history.ztbx_pre_values, ztbx_925)
            if p_prev is not None and p_curr is not None:
                profit_delta = round(p_curr - p_prev, 4)

        # Breadth 方向: dB = B_t0 - B_t1
        if advance_share_t1 is not None and advance_share is not None and prev_adv_vals:
            b_prev_adv = _calc_pctile(prev_adv_vals, advance_share_t1)
            b_curr_adv = _calc_pctile(history.advance_share_pre_values, advance_share)
            if dt_t1 is not None and dt_925 is not None and prev_dt_vals:
                b_prev_dt = _calc_pctile(prev_dt_vals, dt_t1)
                b_curr_dt = _calc_pctile(history.dt_pre_values, dt_925)
                if (b_prev_adv is not None and b_curr_adv is not None
                        and b_prev_dt is not None and b_curr_dt is not None):
                    b_prev = sorted([b_prev_adv, 1 - b_prev_dt])[0]
                    b_curr = sorted([b_curr_adv, 1 - b_curr_dt])[0]
                    breadth_delta = round(b_curr - b_prev, 4)

    # --- R 方向 (v4 修复): T-1 relay vs T-2 relay 日间变化 ---
    # v3 bug: 用同一个 T-1 值在"包含自己"和"不包含自己"两个分布中求分位差,
    # 实际测量的是"偏离中位数的方向"而非"日间变化"
    if history is not None and len(history.relay_health_values) >= 3:
        relay_t1 = history.relay_health_values[-1]  # T-1
        relay_t2 = history.relay_health_values[-2]  # T-2
        vals_excl_t1 = history.relay_health_values[:-1]  # 排除 T-1

        r_prev = _calc_pctile(vals_excl_t1, relay_t2)  # T-2 在 [T-3, T-4, ...] 中的分位
        r_curr = _calc_pctile(history.relay_health_values, relay_t1)  # T-1 在完整分布中的分位
        if r_prev is not None and r_curr is not None:
            relay_delta = round(r_curr - r_prev, 4)

    # 方向: 2-of-3 共识
    if profit_delta is not None and breadth_delta is not None and relay_delta is not None:
        up_count = sum([
            profit_delta > DIRECTION_DEADBAND,
            breadth_delta > DIRECTION_DEADBAND,
            relay_delta > DIRECTION_DEADBAND,
        ])
        down_count = sum([
            profit_delta < -DIRECTION_DEADBAND,
            breadth_delta < -DIRECTION_DEADBAND,
            relay_delta < -DIRECTION_DEADBAND,
        ])
        if up_count >= 2:
            direction = EmotionDirection.UP
        elif down_count >= 2:
            direction = EmotionDirection.DOWN
        else:
            direction = EmotionDirection.FLAT
    elif profit_delta is not None and breadth_delta is not None:
        # R 家族不可用时, 退化为 P/B 2-of-2 共识
        up_count = sum([
            profit_delta > DIRECTION_DEADBAND,
            breadth_delta > DIRECTION_DEADBAND,
        ])
        down_count = sum([
            profit_delta < -DIRECTION_DEADBAND,
            breadth_delta < -DIRECTION_DEADBAND,
        ])
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
    # 阶段 5.5: KQXY 亏钱效应覆盖层 (v4 修复: kqxy_t1/t2 现在从 history 自动提取)
    # ========================================================================
    loss = _classify_loss_overlay(kqxy_t1, kqxy_t2, history)
    loss_level = loss["loss_level"]
    loss_direction = loss["loss_direction"]
    loss_overlay = "NONE"

    if phase == EmotionPhase.REPAIR:
        if loss_direction == "CONTRACTING":
            loss_overlay = "REPAIR_SUPPORT"
        elif loss_direction == "EXPANDING":
            loss_overlay = "REPAIR_WEAK"

    if phase == EmotionPhase.HIGH_ACTIVE and loss_direction == "EXPANDING":
        loss_overlay = "HIGH_CRACKING"

    # ========================================================================
    # 阶段 5.6: QX 综合情绪统计 (仅展示, 不参与决策)
    # ========================================================================
    qx_stats = _compute_qx_stats(history, qx_925)

    # ========================================================================
    # 阶段 6: 极端否决 (Phase 之外的硬止损, 仍用盘前数据)
    #
    # 极端否决使用盘前数据是合理的: 竞价瞬间的极端信号需要即时响应,
    # 不应等到盘后确认。ZTBX+LBBX 同时翻负是强烈的盘中危险信号,
    # advance_share 极低 + DT 极高是广度恐慌, 都需要立即空仓。
    # ========================================================================
    profit_collapse = (
        ztbx_t1 is not None and lbbx_t1 is not None
        and ztbx_925 is not None and lbbx_925 is not None
        and ztbx_t1 > 0 and lbbx_t1 > 0
        and ztbx_925 < 0 and lbbx_925 < 0
    )

    breadth_panic = False
    if advance_share is not None and dt_925 is not None:
        if use_close_pctile or use_pre_pctile:
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
        struct = {"height": "LOW", "fenqi": "DISABLED", "yizi": False, "huanshou": False,
                  "fenqi_enabled": False, "feiban": False}
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
        # 双时间截面水位 (v4 新增)
        close_level_score=close_level_score,
        pre_level_score=pre_level_score,
        level_source=level_source,
        # 三个家族水位 (盘后口径)
        profit_level=profit_level,
        breadth_level=breadth_level,
        relay_level=relay_level,
        # 三个家族日变化
        profit_delta=profit_delta,
        breadth_delta=breadth_delta,
        relay_delta=relay_delta,
        # 极端否决
        hard_veto=hard_veto,
        profit_collapse=profit_collapse,
        breadth_panic=breadth_panic,
        # KQXY
        kqxy_t1=kqxy_t1,
        kqxy_t2=kqxy_t2,
        kqxy_pct=loss["kqxy_pct"],
        kqxy_delta=loss["kqxy_delta"],
        loss_level=loss_level,
        loss_direction=loss_direction,
        loss_overlay=loss_overlay,
        # QX
        qx_925=qx_925,
        qx_stats=qx_stats,
        # 风险
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
        # 原始指标 (盘前)
        ztbx_925=ztbx_925,
        lbbx_925=lbbx_925,
        advance_share=advance_share,
        dt_925=dt_925,
        jinji_1_2=jinji_1_2,
        jinji_2_3=jinji_2_3,
        relay_health=relay_health,
        # 盘后原始指标 (v4 新增)
        ztbx_close=ztbx_close,
        lbbx_close=lbbx_close,
        advance_share_close=advance_share_close,
        dt_close=dt_close,
        # 诊断
        warnings=warnings,
        diagnostics={
            "pbbx_raw": pbbx,
            "ztbx_t1": ztbx_t1,
            "lbbx_t1": lbbx_t1,
            "sz_925": sz_925,
            "xd_925": xd_925,
            "use_close_pctile": use_close_pctile,
            "use_pre_pctile": use_pre_pctile,
            "level_source": level_source,
            "history_days": history.history_days if history else 0,
            "history_close_days": history.close_days if history else 0,
            "valid_families": valid_families,
            "data_quality_level": data_quality_level.value,
            "level_score": level_score,
            "close_level_score": close_level_score,
            "pre_level_score": pre_level_score,
            "ice_stage": ice_stage,
            "retreat_stage": retreat_stage,
            "hard_veto": hard_veto,
            "prev_phase": prev_phase.value if prev_phase else None,
            "loss_overlay": loss_overlay,
            "loss_level": loss_level,
            "loss_direction": loss_direction,
            "kqxy_from_history": (kqxy_t1 is not None and history is not None
                                  and len(history.kqxy_values) >= 1),
            "ztbx_close": ztbx_close,
            "lbbx_close": lbbx_close,
            "advance_share_close": advance_share_close,
            "dt_close": dt_close,
            "version": "v4.0-dual-timeslice",
        },
    )
    return result