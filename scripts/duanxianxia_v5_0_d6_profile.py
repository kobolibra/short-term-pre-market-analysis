#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v5_0_d6_profile.py  --  v5.0 统计剖面引擎

============================================================================
v4.2 → v5.0 核心重构
============================================================================

问题诊断:
  v4.2 的 6指标→3家族→1水位→Phase→查表 链路存在严重的信息压缩:
  1. P家族 = median(ZTBX, LBBX) — 两个指标被压成一个数
  2. B家族 = min(advance, 1-DT) — 两个指标被压成一个数
  3. 总水位 = median(P, B, R) — 三家族被压成一个数
  4. Phase = 水位×方向 — 7宫格离散化
  5. Phase → 查表获取仓位/池乘子/买点 — 主观阈值

  每一步压缩都丢失了结构信息, 最终 Phase 标签无法区分:
  "ZTBX=0.9 + LBBX=0.1 + 涨占比=0.5" vs "全部=0.5" → 都是 MID

v5.0 统计剖面框架:
  ┌─────────────────────────────────────────────────────────────────┐
  │  7 指标 × 2 时间截面 = 12 个独立数据点                            │
  │  每个数据点独立计算在各自历史分布中的分位数                          │
  │  4 个统计量从分位数剖面中提取:                                     │
  │    Bottleneck = min(close分位数)  — 最弱维度, 木桶短板             │
  │    Heat       = mean(close分位数) — 市场平均温度                   │
  │    Divergence = std(close分位数)  — 指标间分歧度                   │
  │    Tilt       = 瓶颈维度名称      — 风险来源定位                   │
  │  Direction   = mean(pre分位 - close分位) — 竞价偏离度              │
  │  Position    = bottleneck × (1 - divergence)  — 连续仓位映射       │
  │  无主观阈值, 无黑盒评分, 全链路可追溯                              │
  └─────────────────────────────────────────────────────────────────┘

7 指标:
  ┌──────────┬────────────┬─────────────┬──────────────┐
  │ 指标      │ 盘后(close) │ 盘前(pre)    │ 时间截面      │
  ├──────────┼────────────┼─────────────┼──────────────┤
  │ 涨占比     │ review_daily│ qxlive       │ 双截面        │
  │ 跌停(DT)   │ review_daily│ qxlive       │ 双截面        │
  │ ZTBX      │ review_daily│ qxlive       │ 双截面        │
  │ LBBX      │ review_daily│ qxlive       │ 双截面        │
  │ QX        │ review_daily│ qxlive       │ 双截面        │
  │ 接力       │ ztpool      │ N/A          │ 单截面(close) │
  │ KQXY      │ review_daily│ N/A          │ 单截面(close) │
  └──────────┴────────────┴─────────────┴──────────────┘

策略衔接:
  - D7 路由: 不变 (只关心个股结构, 不受市场情绪影响)
  - 池排名: 不变 (池内竞争逻辑独立于市场状态)
  - 风控执行: 池乘子由瓶颈维度驱动, 仓位由 position 连续映射
  - 买点模式: 由 heat × divergence 驱动

============================================================================
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
# 常数
# ============================================================================

# 分位数计算最小历史天数
MIN_DAYS_FOR_PCTILE = 20

# 7 个指标定义
INDICATOR_DEFS = {
    "advance_share": {"label": "涨占比",   "dual_section": True,  "polarity": +1},
    "dt":           {"label": "跌停",      "dual_section": True,  "polarity": -1},
    "ztbx":         {"label": "ZTBX",      "dual_section": True,  "polarity": +1},
    "lbbx":         {"label": "LBBX",      "dual_section": True,  "polarity": +1},
    "qx":           {"label": "QX",        "dual_section": True,  "polarity": +1},
    "relay":        {"label": "接力",       "dual_section": False, "polarity": +1},
    "kqxy":         {"label": "KQXY",      "dual_section": False, "polarity": -1},
}

# 双截面指标 (有 pre 数据的)
DUAL_SECTION_KEYS = [k for k, v in INDICATOR_DEFS.items() if v["dual_section"]]

# 单截面指标 (只有 close 数据)
SINGLE_SECTION_KEYS = [k for k, v in INDICATOR_DEFS.items() if not v["dual_section"]]

# 所有指标 (用于 close 分位数)
ALL_INDICATOR_KEYS = list(INDICATOR_DEFS.keys())

# 极端否决静态阈值 (历史不足时回退)
STATIC_VETO_THRESHOLDS = {
    "advance_share_15pct": 0.20,
    "dt_85pct": 20.0,
}

# 池基础仓位 (与 v4.2 一致)
POOL_BASE_POSITION = {
    "yizi": 4.0,
    "huanshou": 3.0,
    "fenqi": 1.5,
    "feiban": 3.0,
}

# 池仓位上限
POOL_POSITION_CAP = {
    "yizi": 8.0,
    "huanshou": 6.0,
    "fenqi": 3.0,
    "feiban": 6.0,
}


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class ProfileHistory:
    """
    v5.0 历史数据存储。

    每个指标独立存储 close 和 pre 两个序列。
    单截面指标只有 close, pre 为空列表。
    """
    # --- Close 序列 (盘后, 来自 review_daily) ---
    advance_share_close: List[float] = field(default_factory=list)
    dt_close: List[float] = field(default_factory=list)
    ztbx_close: List[float] = field(default_factory=list)
    lbbx_close: List[float] = field(default_factory=list)
    qx_close: List[float] = field(default_factory=list)
    kqxy_close: List[float] = field(default_factory=list)

    # --- Pre 序列 (盘前 9:25, 来自 qxlive) ---
    advance_share_pre: List[float] = field(default_factory=list)
    dt_pre: List[float] = field(default_factory=list)
    ztbx_pre: List[float] = field(default_factory=list)
    lbbx_pre: List[float] = field(default_factory=list)
    qx_pre: List[float] = field(default_factory=list)

    # --- 接力 (单截面, 来自 ztpool) ---
    relay_health: List[float] = field(default_factory=list)

    _WINDOW = 60
    _MIN_DAYS = MIN_DAYS_FOR_PCTILE

    def add_day(self,
                # Close 值
                advance_share_close: Optional[float] = None,
                dt_close: Optional[float] = None,
                ztbx_close: Optional[float] = None,
                lbbx_close: Optional[float] = None,
                qx_close: Optional[float] = None,
                kqxy_close: Optional[float] = None,
                # Pre 值
                advance_share_pre: Optional[float] = None,
                dt_pre: Optional[float] = None,
                ztbx_pre: Optional[float] = None,
                lbbx_pre: Optional[float] = None,
                qx_pre: Optional[float] = None,
                # 接力
                relay_health: Optional[float] = None,
                ) -> None:
        """添加一个交易日的数据。None 值自动跳过。"""
        if advance_share_close is not None:
            self.advance_share_close.append(advance_share_close)
        if dt_close is not None:
            self.dt_close.append(dt_close)
        if ztbx_close is not None:
            self.ztbx_close.append(ztbx_close)
        if lbbx_close is not None:
            self.lbbx_close.append(lbbx_close)
        if qx_close is not None:
            self.qx_close.append(qx_close)
        if kqxy_close is not None:
            self.kqxy_close.append(kqxy_close)

        if advance_share_pre is not None:
            self.advance_share_pre.append(advance_share_pre)
        if dt_pre is not None:
            self.dt_pre.append(dt_pre)
        if ztbx_pre is not None:
            self.ztbx_pre.append(ztbx_pre)
        if lbbx_pre is not None:
            self.lbbx_pre.append(lbbx_pre)
        if qx_pre is not None:
            self.qx_pre.append(qx_pre)

        if relay_health is not None:
            self.relay_health.append(relay_health)

    def _get_close_list(self, key: str) -> List[float]:
        """根据指标 key 获取 close 序列。"""
        mapping = {
            "advance_share": self.advance_share_close,
            "dt": self.dt_close,
            "ztbx": self.ztbx_close,
            "lbbx": self.lbbx_close,
            "qx": self.qx_close,
            "kqxy": self.kqxy_close,
            "relay": self.relay_health,
        }
        return mapping.get(key, [])

    def _get_pre_list(self, key: str) -> List[float]:
        """根据指标 key 获取 pre 序列。"""
        mapping = {
            "advance_share": self.advance_share_pre,
            "dt": self.dt_pre,
            "ztbx": self.ztbx_pre,
            "lbbx": self.lbbx_pre,
            "qx": self.qx_pre,
        }
        return mapping.get(key, [])

    def valid_for_close_pctile(self) -> bool:
        """盘后数据是否足够计算分位数 (至少 MIN_DAYS 天)。"""
        return (len(self.ztbx_close) >= self._MIN_DAYS
                and len(self.advance_share_close) >= self._MIN_DAYS
                and len(self.relay_health) >= self._MIN_DAYS)

    def valid_for_pre_pctile(self) -> bool:
        """盘前数据是否足够计算分位数。"""
        return (len(self.ztbx_pre) >= self._MIN_DAYS
                and len(self.advance_share_pre) >= self._MIN_DAYS)

    def close_days(self) -> int:
        """盘后数据最小天数 (用于诊断)。"""
        return min(
            len(self.ztbx_close), len(self.advance_share_close),
            len(self.relay_health), len(self.kqxy_close),
            len(self.lbbx_close), len(self.dt_close), len(self.qx_close),
        )

    def pre_days(self) -> int:
        """盘前数据最小天数 (用于诊断)。"""
        return min(
            len(self.ztbx_pre), len(self.advance_share_pre),
            len(self.lbbx_pre), len(self.dt_pre), len(self.qx_pre),
        )

    def advance_share_15pct(self) -> Optional[float]:
        """上涨占比 15% 分位数 (用于极端否决)。"""
        return _pctile_static(self.advance_share_pre, 0.15)

    def dt_85pct(self) -> Optional[float]:
        """跌停 85% 分位数 (用于极端否决)。"""
        return _pctile_static(self.dt_pre, 0.85)


@dataclass
class IndicatorProfile:
    """单个指标在历史分布中的剖面。"""
    key: str = ""
    label: str = ""
    polarity: int = 1          # +1: 越高越好; -1: 越低越好 (DT, KQXY)

    # 原始值
    close_raw: Optional[float] = None
    pre_raw: Optional[float] = None

    # 分位数 (0-1, 统一为"越高越好"方向: 对 polarity=-1 的指标, pct = 1 - raw_pct)
    close_pct: Optional[float] = None
    pre_pct: Optional[float] = None

    # 方向 (仅双截面指标): pre_pct - close_pct, 竞价偏离度
    direction: Optional[float] = None

    # 诊断
    close_history_n: int = 0
    pre_history_n: int = 0
    is_dual_section: bool = True


@dataclass
class MarketProfile:
    """
    v5.0 市场统计剖面 — 完整输出。

    与 v4.2 D6EmotionResult 的关键区别:
    - 不再有 Phase 标签 (ICE/REPAIR/EXPANSION 等)
    - 不再有 RiskTier 枚举 (NORMAL/WARNING/CRISIS)
    - 不再有硬编码的 Phase→仓位 查表
    - 所有决策量由统计量连续映射得出
    """
    date: str = ""

    # === 7 个指标剖面 ===
    indicators: Dict[str, IndicatorProfile] = field(default_factory=dict)

    # === 4 个统计量 (从 close 分位数计算) ===
    bottleneck: float = 0.5          # min(close分位数), 木桶短板
    bottleneck_name: str = ""        # 瓶颈维度名称
    heat: float = 0.5                # mean(close分位数), 市场温度
    divergence: float = 0.0          # std(close分位数), 指标分歧度
    tilt: str = ""                   # 倾斜描述: 瓶颈维度 + 严重程度

    # === 方向 ===
    direction_summary: float = 0.0   # mean(方向), 竞价整体偏离度 (正=竞价强于盘后)

    # === 决策量 (连续映射, 无阈值) ===
    position: float = 0.5            # bottleneck × (1 - divergence), 总仓位系数
    pool_multipliers: Dict[str, float] = field(default_factory=lambda: {
        "yizi": 1.0, "huanshou": 1.0, "fenqi": 1.0, "feiban": 1.0,
    })
    buy_mode: str = "board_only"     # auction_and_board / board_only / observe_only / empty

    # === 极端否决 ===
    extreme_veto: bool = False
    veto_reason: str = ""
    profit_collapse: bool = False
    breadth_panic: bool = False

    # === 池启用状态 (由池乘子 > 0 推导) ===
    yizi_enabled: bool = True
    huanshou_enabled: bool = True
    fenqi_enabled: bool = True
    feiban_enabled: bool = True

    # === 诊断 ===
    warnings: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 分位数计算
# ============================================================================

def _pctile_static(values: List[float], q: float) -> Optional[float]:
    """计算静态分位数 (给定序列的 q 分位值)。"""
    if len(values) < MIN_DAYS_FOR_PCTILE:
        return None
    sv = sorted(values)
    idx = int(len(sv) * q)
    return sv[min(idx, len(sv) - 1)]


def _calc_pctile(values: List[float], value: Optional[float]) -> Optional[float]:
    """
    计算给定值在序列中的分位数 (0-1)。

    返回: 0-1 之间的分位数, 数据不足或值为 None 时返回 None。
    """
    if value is None or len(values) < MIN_DAYS_FOR_PCTILE:
        return None
    sv = sorted(values)
    n = len(sv)
    rank = sum(1 for v in sv if v <= value)
    return round(rank / n, 4)


def _calc_pctile_normalized(
    values: List[float],
    value: Optional[float],
    polarity: int,
) -> Optional[float]:
    """
    计算分位数并归一化到"越高越好"方向。

    对于 polarity=-1 的指标 (DT, KQXY), 原始值越高越差,
    所以 pct = 1 - raw_pct, 使得高分位=好。

    返回: 0-1, 越高越好。
    """
    raw = _calc_pctile(values, value)
    if raw is None:
        return None
    if polarity < 0:
        return round(1.0 - raw, 4)
    return raw


# ============================================================================
# 极端否决
# ============================================================================

def _check_extreme_veto(
    advance_share_pre: Optional[float],
    dt_pre: Optional[float],
    ztbx_pre: Optional[float],
    lbbx_pre: Optional[float],
    ztbx_pre_t1: Optional[float],
    lbbx_pre_t1: Optional[float],
    history: Optional[ProfileHistory],
    static_thresholds: Optional[Dict[str, float]] = None,
) -> Tuple[bool, bool, bool, str]:
    """
    检查两个极端否决条件 (使用盘前数据, 竞价瞬间的极端信号需要即时响应)。

    1. 强势股集体翻负: ZTBX+LBBX 同时从 T-1 正翻 T0 负
    2. 广度恐慌: 上涨占比极低 AND 跌停极高

    Returns:
        (hard_veto, profit_collapse, breadth_panic, reason)
    """
    thresh = static_thresholds or STATIC_VETO_THRESHOLDS
    profit_collapse = False
    breadth_panic = False
    reasons: List[str] = []

    # 1. 强势股集体翻负
    if (ztbx_pre_t1 is not None and lbbx_pre_t1 is not None
            and ztbx_pre is not None and lbbx_pre is not None):
        if ztbx_pre_t1 > 0 and lbbx_pre_t1 > 0 and ztbx_pre < 0 and lbbx_pre < 0:
            profit_collapse = True
            reasons.append("强势股集体翻负 (ZTBX+LBBX同时从正转负)")

    # 2. 广度恐慌
    if advance_share_pre is not None and dt_pre is not None:
        if history is not None and history.valid_for_pre_pctile():
            adv_15 = history.advance_share_15pct()
            dt_85 = history.dt_85pct()
            if adv_15 is not None and dt_85 is not None:
                if advance_share_pre < adv_15 and dt_pre > dt_85:
                    breadth_panic = True
                    reasons.append(f"广度恐慌 (上涨占比{advance_share_pre:.1%}<P15={adv_15:.1%}, "
                                   f"跌停{dt_pre:.0f}>P85={dt_85:.0f})")
        else:
            if (advance_share_pre < thresh.get("advance_share_15pct", 0.20)
                    and dt_pre > thresh.get("dt_85pct", 20.0)):
                breadth_panic = True
                reasons.append("广度恐慌 (静态阈值)")

    hard_veto = profit_collapse or breadth_panic
    reason = "; ".join(reasons) if reasons else ""
    return hard_veto, profit_collapse, breadth_panic, reason


# ============================================================================
# 池乘子计算
# ============================================================================

def _calc_pool_multipliers(
    bottleneck_name: str,
    bottleneck_pct: float,
    heat: float,
    divergence: float,
) -> Dict[str, float]:
    """
    从统计剖面推导池乘子。

    核心逻辑: 瓶颈维度决定结构偏好, heat 决定仓位强度, divergence 施加折扣。

    瓶颈维度 → 结构偏好:
      - 涨占比 → 广度恐慌, 偏好换手封/分歧封 (真金白银), 减仓一字封
      - 跌停   → DT 飙升, 全面减仓
      - ZTBX/LBBX → 强势股裂化, 偏好换手封 (接力), 减仓一字封
      - 接力   → 接力断裂, 偏好非板 (新周期萌芽), 减仓一字封
      - KQXY   → 亏钱效应扩散, 激进减仓
      - QX     → 综合情绪弱, 等比减仓
    """
    mults = {"yizi": 1.0, "huanshou": 1.0, "fenqi": 1.0, "feiban": 1.0}

    # --- 瓶颈维度结构偏好 ---
    if bottleneck_name == "advance_share":
        # 广度恐慌: 涨跌比极端, 市场普跌, 只有真金白银接力的换手封和分歧修复值得关注
        mults["yizi"] = 0.5
        mults["huanshou"] = 1.1
        mults["fenqi"] = 1.3
        mults["feiban"] = 1.0

    elif bottleneck_name == "dt":
        # DT 飙升: 恐慌蔓延, 全面收缩
        mults["yizi"] = 0.4
        mults["huanshou"] = 0.6
        mults["fenqi"] = 0.7
        mults["feiban"] = 0.6

    elif bottleneck_name in ("ztbx", "lbbx"):
        # 强势股裂化: 昨日涨停股表现差, 接力信心不足
        # 一字封的封单逻辑最脆弱, 换手封是真金白银更有韧性
        mults["yizi"] = 0.4
        mults["huanshou"] = 1.1
        mults["fenqi"] = 0.8
        mults["feiban"] = 0.9

    elif bottleneck_name == "relay":
        # 接力断裂: 晋级率低, 高位股风险大
        # 非板 (新周期萌芽) 优先级最高, 一字封/换手封依赖接力生态
        mults["yizi"] = 0.4
        mults["huanshou"] = 0.7
        mults["fenqi"] = 0.8
        mults["feiban"] = 1.3

    elif bottleneck_name == "kqxy":
        # 亏钱效应扩散: 最危险的信号, 全面激进减仓
        mults["yizi"] = 0.2
        mults["huanshou"] = 0.4
        mults["fenqi"] = 0.5
        mults["feiban"] = 0.4

    elif bottleneck_name == "qx":
        # 综合情绪弱: 等比减仓
        mults["yizi"] = 0.6
        mults["huanshou"] = 0.7
        mults["fenqi"] = 0.8
        mults["feiban"] = 0.7

    # --- Heat 调制: 温度越高, 仓位越积极 ---
    # heat 0→1 映射到 heat_factor 0.5→1.5
    heat_factor = 0.5 + heat
    for k in mults:
        mults[k] *= heat_factor

    # --- Divergence 惩罚: 分歧越大, 仓位越保守 ---
    # divergence 0→0.5 映射到 penalty 1.0→0.7
    div_penalty = 1.0 - divergence * 0.6
    for k in mults:
        mults[k] *= div_penalty

    # --- 瓶颈严重度惩罚 ---
    # bottleneck_pct 越低, 惩罚越重
    if bottleneck_pct < 0.15:
        severity = 0.5
    elif bottleneck_pct < 0.25:
        severity = 0.7
    elif bottleneck_pct < 0.35:
        severity = 0.85
    else:
        severity = 1.0
    for k in mults:
        mults[k] *= severity

    # --- 钳位 ---
    for k in mults:
        mults[k] = round(max(0.0, min(2.0, mults[k])), 4)

    return mults


# ============================================================================
# 买点模式
# ============================================================================

def _calc_buy_mode(heat: float, divergence: float, bottleneck_pct: float) -> str:
    """
    从 heat × divergence 推导买点模式。

    连续映射, 无阈值枚举:
      - 高共识看多 (heat>0.6, divergence<0.2): 竞价+排板
      - 温和看多 (heat>0.4): 排板为主
      - 谨慎 (heat>0.2): 排板为主
      - 危机 (bottleneck<0.10): 空仓
      - 近危机 (bottleneck<0.20): 仅观察
    """
    if bottleneck_pct < 0.10:
        return "empty"
    if bottleneck_pct < 0.20:
        return "observe_only"
    if heat > 0.60 and divergence < 0.20:
        return "auction_and_board"
    if heat > 0.40:
        return "board_only"
    return "board_only"


# ============================================================================
# 主入口: 计算市场统计剖面
# ============================================================================

def calculate_profile(
    close_data: Dict[str, Optional[float]],
    pre_data: Dict[str, Optional[float]],
    relay_health: Optional[float],
    history: Optional[ProfileHistory] = None,
    static_thresholds: Optional[Dict[str, float]] = None,
    # 极端否决所需的前一日盘前数据
    ztbx_pre_t1: Optional[float] = None,
    lbbx_pre_t1: Optional[float] = None,
) -> MarketProfile:
    """
    计算市场统计剖面 (v5.0 核心入口)。

    Args:
        close_data: 盘后指标值, key 为 indicator key
            - "advance_share": 上涨占比 (0-1)
            - "dt": 跌停家数 (int)
            - "ztbx": 昨涨停表现 (%)
            - "lbbx": 昨连板表现 (%)
            - "qx": 情绪指标
            - "kqxy": 亏钱效应
        pre_data: 盘前指标值 (仅双截面指标)
            - "advance_share", "dt", "ztbx", "lbbx", "qx"
        relay_health: 接力健康度 (来自 ztpool)
        history: 历史数据 (用于分位数计算)
        static_thresholds: 静态阈值覆盖
        ztbx_pre_t1: T-1 盘前 ZTBX (用于极端否决)
        lbbx_pre_t1: T-1 盘前 LBBX (用于极端否决)

    Returns:
        MarketProfile: 完整统计剖面
    """
    warnings: List[str] = []
    diagnostics: Dict[str, Any] = {}
    profiles: Dict[str, IndicatorProfile] = {}

    # ========================================================================
    # 阶段 1: 计算每个指标的分位数剖面
    # ========================================================================
    history_available = history is not None and history.valid_for_close_pctile()

    for key, defn in INDICATOR_DEFS.items():
        label = defn["label"]
        polarity = defn["polarity"]
        is_dual = defn["dual_section"]

        ip = IndicatorProfile(
            key=key, label=label, polarity=polarity,
            is_dual_section=is_dual,
        )

        # --- Close 分位数 ---
        if key == "relay":
            ip.close_raw = relay_health
            if history_available and history is not None:
                ip.close_history_n = len(history.relay_health)
                ip.close_pct = _calc_pctile_normalized(
                    history.relay_health, relay_health, polarity,
                )
        elif key == "kqxy":
            ip.close_raw = close_data.get("kqxy")
            if history_available and history is not None:
                ip.close_history_n = len(history.kqxy_close)
                ip.close_pct = _calc_pctile_normalized(
                    history.kqxy_close, ip.close_raw, polarity,
                )
        else:
            ip.close_raw = close_data.get(key)
            if history_available and history is not None:
                close_list = history._get_close_list(key)
                ip.close_history_n = len(close_list)
                ip.close_pct = _calc_pctile_normalized(
                    close_list, ip.close_raw, polarity,
                )

        # --- Pre 分位数 (仅双截面指标) ---
        if is_dual:
            ip.pre_raw = pre_data.get(key)
            if (history is not None and history.valid_for_pre_pctile()
                    and ip.pre_raw is not None):
                pre_list = history._get_pre_list(key)
                ip.pre_history_n = len(pre_list)
                ip.pre_pct = _calc_pctile_normalized(
                    pre_list, ip.pre_raw, polarity,
                )

            # --- 方向: pre_pct - close_pct ---
            if ip.close_pct is not None and ip.pre_pct is not None:
                ip.direction = round(ip.pre_pct - ip.close_pct, 4)

        profiles[key] = ip

    # 记录数据质量
    valid_close = sum(1 for ip in profiles.values() if ip.close_pct is not None)
    valid_pre = sum(1 for ip in profiles.values()
                    if ip.is_dual_section and ip.pre_pct is not None)
    diagnostics["valid_close_indicators"] = valid_close
    diagnostics["valid_pre_indicators"] = valid_pre
    diagnostics["history_available"] = history_available
    diagnostics["history_close_days"] = history.close_days() if history else 0
    diagnostics["history_pre_days"] = history.pre_days() if history else 0

    if valid_close < 3:
        warnings.append(f"有效 close 指标仅 {valid_close}/7, 剖面可靠性低")

    # ========================================================================
    # 阶段 2: 计算 4 个统计量
    # ========================================================================

    # 收集所有 close 分位数 (None 跳过)
    close_pcts: List[Tuple[str, float]] = []
    for key, ip in profiles.items():
        if ip.close_pct is not None:
            close_pcts.append((key, ip.close_pct))

    if close_pcts:
        pct_values = [v for _, v in close_pcts]

        # Bottleneck: 最弱维度
        min_idx = min(range(len(pct_values)), key=lambda i: pct_values[i])
        bottleneck_name = close_pcts[min_idx][0]
        bottleneck = pct_values[min_idx]

        # Heat: 平均温度
        heat = round(statistics.mean(pct_values), 4)

        # Divergence: 标准差 (分歧度)
        if len(pct_values) >= 2:
            divergence = round(statistics.stdev(pct_values), 4)
        else:
            divergence = 0.0

        # Tilt: 瓶颈维度描述
        bottleneck_label = INDICATOR_DEFS[bottleneck_name]["label"]
        if bottleneck < 0.15:
            tilt = f"{bottleneck_label}极度承压"
        elif bottleneck < 0.30:
            tilt = f"{bottleneck_label}显著偏弱"
        elif bottleneck < 0.45:
            tilt = f"{bottleneck_label}略弱"
        else:
            tilt = f"{bottleneck_label}正常"
    else:
        bottleneck = 0.5
        bottleneck_name = ""
        heat = 0.5
        divergence = 0.0
        tilt = "数据不足"
        warnings.append("无有效 close 分位数, 使用默认值 0.5")

    # ========================================================================
    # 阶段 3: 方向汇总
    # ========================================================================
    directions = [ip.direction for ip in profiles.values()
                  if ip.is_dual_section and ip.direction is not None]
    if directions:
        direction_summary = round(statistics.mean(directions), 4)
    else:
        direction_summary = 0.0

    # ========================================================================
    # 阶段 4: 极端否决
    # ========================================================================
    hard_veto, profit_collapse, breadth_panic, veto_reason = _check_extreme_veto(
        advance_share_pre=pre_data.get("advance_share"),
        dt_pre=pre_data.get("dt"),
        ztbx_pre=pre_data.get("ztbx"),
        lbbx_pre=pre_data.get("lbbx"),
        ztbx_pre_t1=ztbx_pre_t1,
        lbbx_pre_t1=lbbx_pre_t1,
        history=history,
        static_thresholds=static_thresholds,
    )

    # ========================================================================
    # 阶段 5: 决策量计算
    # ========================================================================

    if hard_veto:
        position = 0.0
        pool_mults = {"yizi": 0.0, "huanshou": 0.0, "fenqi": 0.0, "feiban": 0.0}
        buy_mode = "empty"
    else:
        # Position = bottleneck × (1 - divergence)
        # bottleneck 0-1, divergence max ~0.5
        position = round(bottleneck * (1.0 - divergence), 4)
        position = max(0.0, min(1.0, position))

        # Pool multipliers
        pool_mults = _calc_pool_multipliers(bottleneck_name, bottleneck, heat, divergence)

        # Buy mode
        buy_mode = _calc_buy_mode(heat, divergence, bottleneck)

    # 池启用状态
    yizi_enabled = pool_mults["yizi"] > 0.05
    huanshou_enabled = pool_mults["huanshou"] > 0.05
    fenqi_enabled = pool_mults["fenqi"] > 0.05
    feiban_enabled = pool_mults["feiban"] > 0.05

    # ========================================================================
    # 阶段 6: 构建输出
    # ========================================================================
    result = MarketProfile(
        indicators=profiles,
        bottleneck=bottleneck,
        bottleneck_name=bottleneck_name,
        heat=heat,
        divergence=divergence,
        tilt=tilt,
        direction_summary=direction_summary,
        position=position,
        pool_multipliers=pool_mults,
        buy_mode=buy_mode,
        extreme_veto=hard_veto,
        veto_reason=veto_reason,
        profit_collapse=profit_collapse,
        breadth_panic=breadth_panic,
        yizi_enabled=yizi_enabled,
        huanshou_enabled=huanshou_enabled,
        fenqi_enabled=fenqi_enabled,
        feiban_enabled=feiban_enabled,
        warnings=warnings,
        diagnostics=diagnostics,
    )

    return result


# ============================================================================
# 输出格式化
# ============================================================================

def format_profile(profile: MarketProfile) -> str:
    """
    将 MarketProfile 格式化为可读文本。

    输出结构:
      1. 统计剖面总览 (4 统计量 + 方向)
      2. 7 指标明细表
      3. 决策量 (仓位, 池乘子, 买点)
      4. 极端否决 (如有)
    """
    lines = []
    lines.append("=" * 70)
    lines.append("  市场统计剖面 v5.0")
    if profile.date:
        lines.append(f"  日期: {profile.date}")
    lines.append("=" * 70)

    # --- 统计剖面总览 ---
    lines.append(f"\n  📊 统计剖面")
    lines.append(f"  Bottleneck:  {profile.bottleneck:.3f} ({profile.bottleneck_name}) "
                 f"— {profile.tilt}")
    lines.append(f"  Heat:        {profile.heat:.3f}")
    lines.append(f"  Divergence:  {profile.divergence:.3f}")
    lines.append(f"  Direction:   {profile.direction_summary:+.3f} "
                 f"({'竞价偏多' if profile.direction_summary > 0.03 else '竞价偏空' if profile.direction_summary < -0.03 else '竞价中性'})")

    # --- 7 指标明细 ---
    lines.append(f"\n  📋 指标明细")
    lines.append(f"  {'指标':<10} {'Close分位':>10} {'Pre分位':>10} {'方向':>10} {'Close原始':>12}")
    lines.append(f"  {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")
    for key in ALL_INDICATOR_KEYS:
        ip = profile.indicators.get(key)
        if ip is None:
            continue
        close_str = f"{ip.close_pct:.3f}" if ip.close_pct is not None else "N/A"
        pre_str = f"{ip.pre_pct:.3f}" if ip.pre_pct is not None else "N/A"
        dir_str = f"{ip.direction:+.3f}" if ip.direction is not None else "N/A"
        raw_str = f"{ip.close_raw}" if ip.close_raw is not None else "N/A"
        # 标记瓶颈
        marker = " ◀ 瓶颈" if key == profile.bottleneck_name else ""
        lines.append(f"  {ip.label:<10} {close_str:>10} {pre_str:>10} {dir_str:>10} {raw_str:>12}{marker}")

    # --- 决策量 ---
    lines.append(f"\n  🎯 决策量")
    lines.append(f"  Position:     {profile.position:.3f} ({profile.position*100:.0f}%)")
    buy_mode_labels = {
        "auction_and_board": "竞价+排板",
        "board_only": "排板为主",
        "observe_only": "仅观察",
        "empty": "空仓",
    }
    lines.append(f"  Buy Mode:     {buy_mode_labels.get(profile.buy_mode, profile.buy_mode)}")
    lines.append(f"  池乘子:")
    for pool, mult in profile.pool_multipliers.items():
        pool_labels = {"yizi": "一字封", "huanshou": "换手封", "fenqi": "分歧封", "feiban": "非板"}
        enabled = "✓" if mult > 0.05 else "✗"
        lines.append(f"    {enabled} {pool_labels.get(pool, pool):<8} ×{mult:.3f}")

    # --- 极端否决 ---
    if profile.extreme_veto:
        lines.append(f"\n  🚨 极端否决触发!")
        lines.append(f"  {profile.veto_reason}")

    # --- 警告 ---
    if profile.warnings:
        lines.append(f"\n  ⚠️ 警告:")
        for w in profile.warnings:
            lines.append(f"    - {w}")

    return "\n".join(lines)


def profile_to_dict(profile: MarketProfile) -> Dict[str, Any]:
    """将 MarketProfile 转换为可序列化字典。"""
    return {
        "date": profile.date,
        "bottleneck": profile.bottleneck,
        "bottleneck_name": profile.bottleneck_name,
        "heat": profile.heat,
        "divergence": profile.divergence,
        "tilt": profile.tilt,
        "direction_summary": profile.direction_summary,
        "position": profile.position,
        "pool_multipliers": profile.pool_multipliers,
        "buy_mode": profile.buy_mode,
        "extreme_veto": profile.extreme_veto,
        "veto_reason": profile.veto_reason,
        "profit_collapse": profile.profit_collapse,
        "breadth_panic": profile.breadth_panic,
        "yizi_enabled": profile.yizi_enabled,
        "huanshou_enabled": profile.huanshou_enabled,
        "fenqi_enabled": profile.fenqi_enabled,
        "feiban_enabled": profile.feiban_enabled,
        "indicators": {
            key: {
                "label": ip.label,
                "close_raw": ip.close_raw,
                "close_pct": ip.close_pct,
                "pre_raw": ip.pre_raw,
                "pre_pct": ip.pre_pct,
                "direction": ip.direction,
                "is_dual_section": ip.is_dual_section,
            }
            for key, ip in profile.indicators.items()
        },
        "warnings": profile.warnings,
        "diagnostics": profile.diagnostics,
    }


# ============================================================================
# 自检
# ============================================================================

def _self_test() -> bool:
    """自检: 验证统计剖面计算逻辑。"""
    import random
    random.seed(42)

    # 构建模拟历史数据 (60 天)
    history = ProfileHistory()
    for i in range(60):
        history.add_day(
            advance_share_close=random.uniform(0.2, 0.8),
            dt_close=random.uniform(0, 30),
            ztbx_close=random.uniform(-2, 5),
            lbbx_close=random.uniform(-3, 6),
            qx_close=random.uniform(20, 80),
            kqxy_close=random.uniform(0, 50),
            advance_share_pre=random.uniform(0.2, 0.8),
            dt_pre=random.uniform(0, 30),
            ztbx_pre=random.uniform(-2, 5),
            lbbx_pre=random.uniform(-3, 6),
            qx_pre=random.uniform(20, 80),
            relay_health=random.uniform(20, 80),
        )

    # 测试 1: 正常市场
    close_data = {
        "advance_share": 0.55, "dt": 8.0, "ztbx": 2.5,
        "lbbx": 3.0, "qx": 55.0, "kqxy": 15.0,
    }
    pre_data = {
        "advance_share": 0.60, "dt": 6.0, "ztbx": 3.0,
        "lbbx": 3.5, "qx": 60.0,
    }
    profile = calculate_profile(
        close_data=close_data, pre_data=pre_data,
        relay_health=55.0, history=history,
    )

    assert profile.heat > 0.4, f"正常市场 heat 应 > 0.4, 实际 {profile.heat}"
    assert profile.bottleneck > 0.3, f"正常市场 bottleneck 应 > 0.3, 实际 {profile.bottleneck}"
    assert profile.position > 0.3, f"正常市场 position 应 > 0.3, 实际 {profile.position}"
    assert profile.buy_mode != "empty", f"正常市场不应空仓, 实际 {profile.buy_mode}"
    assert len(profile.indicators) == 7, f"应有 7 个指标, 实际 {len(profile.indicators)}"
    print("  [PASS] 测试 1: 正常市场剖面")

    # 测试 2: 恐慌市场 (所有指标偏低)
    close_data_panic = {
        "advance_share": 0.08, "dt": 45.0, "ztbx": -3.0,
        "lbbx": -4.0, "qx": 15.0, "kqxy": 60.0,
    }
    pre_data_panic = {
        "advance_share": 0.05, "dt": 50.0, "ztbx": -4.0,
        "lbbx": -5.0, "qx": 10.0,
    }
    profile_panic = calculate_profile(
        close_data=close_data_panic, pre_data=pre_data_panic,
        relay_health=15.0, history=history,
    )
    assert profile_panic.heat < 0.3, f"恐慌市场 heat 应 < 0.3, 实际 {profile_panic.heat}"
    assert profile_panic.bottleneck < 0.15, f"恐慌市场 bottleneck 应 < 0.15, 实际 {profile_panic.bottleneck}"
    assert profile_panic.position < 0.15, f"恐慌市场 position 应 < 0.15, 实际 {profile_panic.position}"
    print("  [PASS] 测试 2: 恐慌市场剖面")

    # 测试 3: 高分歧市场 (指标分散)
    # 涨占比高但 DT 也高 (矛盾信号)
    close_data_div = {
        "advance_share": 0.70, "dt": 35.0, "ztbx": 1.0,
        "lbbx": 0.5, "qx": 40.0, "kqxy": 20.0,
    }
    pre_data_div = {
        "advance_share": 0.72, "dt": 30.0, "ztbx": 1.5,
        "lbbx": 1.0, "qx": 45.0,
    }
    profile_div = calculate_profile(
        close_data=close_data_div, pre_data=pre_data_div,
        relay_health=40.0, history=history,
    )
    assert profile_div.divergence > 0.15, f"高分歧市场 divergence 应 > 0.15, 实际 {profile_div.divergence}"
    print("  [PASS] 测试 3: 高分歧市场剖面")

    # 测试 4: 极端否决 — 强势股集体翻负
    profile_veto = calculate_profile(
        close_data=close_data, pre_data={
            "advance_share": 0.50, "dt": 10.0, "ztbx": -2.0,
            "lbbx": -3.0, "qx": 40.0,
        },
        relay_health=50.0, history=history,
        ztbx_pre_t1=2.0, lbbx_pre_t1=1.5,
    )
    assert profile_veto.extreme_veto, "ZTBX+LBBX 从正翻负应触发极端否决"
    assert profile_veto.profit_collapse, "应标记为 profit_collapse"
    assert profile_veto.position == 0.0, "极端否决下仓位应为 0"
    assert profile_veto.buy_mode == "empty", "极端否决下应空仓"
    print("  [PASS] 测试 4: 极端否决 — 强势股集体翻负")

    # 测试 5: 历史数据不足时回退
    history_short = ProfileHistory()
    for i in range(10):
        history_short.add_day(
            advance_share_close=random.uniform(0.2, 0.8),
            dt_close=random.uniform(0, 30),
            ztbx_close=random.uniform(-2, 5),
            lbbx_close=random.uniform(-3, 6),
            qx_close=random.uniform(20, 80),
            kqxy_close=random.uniform(0, 50),
            relay_health=random.uniform(20, 80),
        )
    profile_short = calculate_profile(
        close_data=close_data, pre_data=pre_data,
        relay_health=55.0, history=history_short,
    )
    assert profile_short.heat == 0.5, "数据不足时 heat 应为默认值 0.5"
    assert not profile_short.indicators["advance_share"].close_pct, "数据不足时分位数应为 None"
    print("  [PASS] 测试 5: 历史数据不足回退")

    # 测试 6: 池乘子计算
    mults_normal = _calc_pool_multipliers("ztbx", 0.5, 0.6, 0.15)
    assert mults_normal["huanshou"] > mults_normal["yizi"], "ZTBX瓶颈时换手封应优于一字封"
    mults_panic = _calc_pool_multipliers("kqxy", 0.1, 0.2, 0.3)
    assert mults_panic["yizi"] < 0.3, "KQXY瓶颈时一字封应大幅降仓"
    print("  [PASS] 测试 6: 池乘子计算")

    # 测试 7: 格式化输出不崩溃
    text = format_profile(profile)
    assert len(text) > 100, "格式化输出应有一定长度"
    d = profile_to_dict(profile)
    assert "indicators" in d, "字典输出应包含 indicators"
    assert len(d["indicators"]) == 7, "字典输出应有 7 个指标"
    print("  [PASS] 测试 7: 格式化输出")

    print("\n  ✅ 所有自检通过 (7/7)")
    return True


if __name__ == "__main__":
    import sys
    ok = _self_test()
    sys.exit(0 if ok else 1)