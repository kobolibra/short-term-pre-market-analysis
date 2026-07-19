#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v5_0_risk_exec.py  --  v5.0 风控与执行模块

============================================================================
v4.2 → v5.0 核心变更
============================================================================

v4.2:
  - 消费 D6EmotionResult → Phase 查表 → position_cap / pool_mult / buy_mode
  - 池乘子硬编码在 PHASE_STRUCTURE 表中
  - 仓位公式: base × height × risk × emotion_cap × pool_mult

v5.0:
  - 消费 MarketProfile → 统计量直接连续映射
  - 池乘子由瓶颈维度类型 + heat + divergence 连续推导
  - 仓位公式: base × height × risk × position × pool_mult
  - position 是连续的 bottleneck×(1-divergence)
  - 无查表, 无硬编码 Phase

============================================================================
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v5_0_d6_profile import (
    MarketProfile, POOL_BASE_POSITION, POOL_POSITION_CAP,
)
from duanxianxia_v4_2_d7_router import PoolType, RiskTag, RoutedStock
from duanxianxia_v4_2_pool_ranker import PoolRankResult, RankedStock


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class ExecutionOrder:
    """单只股票的执行指令"""
    code: str
    name: str = ""
    pool: PoolType = PoolType.POOL_FEIBAN
    pool_label: str = "非板"

    # 仓位
    position_pct: float = 0.0
    base_position_pct: float = 0.0
    height_mult: float = 1.0
    risk_mult: float = 1.0
    profile_position: float = 0.5      # MarketProfile.position (总仓位系数)
    pool_mult: float = 1.0             # 该池的乘子

    # 买点
    buy_mode: str = ""
    buy_strategy: str = ""

    # 风险
    risk_tags: List[str] = field(default_factory=list)
    confirmation_threshold: str = "正常"

    # 排名
    rank: int = 0
    pool_rank: int = 0

    # 诊断
    diagnostics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionPlan:
    """完整执行计划"""
    date: str = ""
    profile: Optional[MarketProfile] = None

    # 总仓位
    profile_position: float = 0.5

    # 各池执行指令
    orders: List[ExecutionOrder] = field(default_factory=list)

    # 池级别汇总
    pool_summary: Dict[str, Any] = field(default_factory=dict)

    # 仓位分配
    allocated_position: float = 0.0
    reserve_position: float = 0.0

    # 诊断
    warnings: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 仓位计算
# ============================================================================

def _calc_risk_tag_multiplier(risk_tags: List[RiskTag]) -> float:
    """计算风险标签综合乘子 (与 v4.2 一致)。"""
    mult = 1.0
    for tag in risk_tags:
        if tag == RiskTag.HEAVY_DIVERGENCE:
            mult *= 0.7
        elif tag == RiskTag.WEAK_SEAL:
            mult *= 0.8
    return mult


def _calc_final_position(
    pool_type: PoolType,
    height_mult: float,
    risk_mult: float,
    profile_position: float,
    pool_mult: float = 1.0,
) -> float:
    """
    v5.0 最终仓位公式:

      position = base × height × risk × profile_position × pool_mult

    其中:
      - base: 池基础仓位 (与 v4.2 一致)
      - height_mult: 高度乘子 (D7 路由产出)
      - risk_mult: 风险标签乘子 (D7 路由产出)
      - profile_position: MarketProfile.position (bottleneck×(1-divergence))
      - pool_mult: 该池的瓶颈维度乘子 (MarketProfile.pool_multipliers)
    """
    base = POOL_BASE_POSITION.get(pool_type, 1.0)
    cap = POOL_POSITION_CAP.get(pool_type, 5.0)
    position = base * height_mult * risk_mult * profile_position * pool_mult
    return min(position, cap)


# ============================================================================
# 买点策略
# ============================================================================

def _determine_buy_strategy(
    pool_type: PoolType,
    buy_mode: str,
    profile: MarketProfile,
) -> str:
    """
    v5.0 买点策略: 由 buy_mode + 池类型 + 瓶颈维度决定。

    与 v4.2 的区别: 不再依赖 RiskTier 枚举, 改用 buy_mode 字符串。
    """
    if buy_mode == "empty":
        return "空仓, 禁止买入"

    if buy_mode == "observe_only":
        if pool_type == PoolType.POOL_FENQI:
            return "仅分歧封极轻仓试错, 排板确认"
        return "仅观察, 不买入"

    # board_only 或 auction_and_board
    if pool_type == PoolType.POOL_YIZI:
        if buy_mode == "auction_and_board":
            return "排板为主 (一字封竞价无法买入)"
        return "排板为主"
    elif pool_type == PoolType.POOL_HUANSHOU:
        if buy_mode == "auction_and_board":
            return "竞价挂涨停价买 + 排板并行"
        return "排板/扫板确认"
    elif pool_type == PoolType.POOL_FENQI:
        if buy_mode == "auction_and_board":
            return "竞价挂涨停价买 (分歧修复)"
        return "排板确认 (分歧修复)"
    elif pool_type == PoolType.POOL_FEIBAN:
        if buy_mode == "auction_and_board":
            return "竞价挂涨停价买 (新共识形成)"
        return "排板/扫板确认"

    return "排板为主"


# ============================================================================
# 执行计划构建
# ============================================================================

def build_execution_plan(
    pool_results: Dict[PoolType, PoolRankResult],
    profile: MarketProfile,
    date: str = "",
) -> ExecutionPlan:
    """
    v5.0: 根据池排名结果和统计剖面, 构建完整执行计划。

    Args:
        pool_results: 各池排名结果 (D7路由 + 池排名)
        profile: MarketProfile 统计剖面 (D6产出)
        date: 日期

    Returns:
        ExecutionPlan 完整执行计划
    """
    plan = ExecutionPlan(
        date=date,
        profile=profile,
        profile_position=profile.position,
    )

    orders: List[ExecutionOrder] = []
    pool_summary: Dict[str, Any] = {}

    # 建仓顺序: 一字封 → 换手封 → 非板 → 分歧封 (与 v4.2 一致)
    pool_order = [
        PoolType.POOL_YIZI,
        PoolType.POOL_HUANSHOU,
        PoolType.POOL_FEIBAN,
        PoolType.POOL_FENQI,
    ]

    # 池乘子映射 (pool_type → multiplier key)
    pool_mult_map = {
        PoolType.POOL_YIZI: "yizi",
        PoolType.POOL_HUANSHOU: "huanshou",
        PoolType.POOL_FENQI: "fenqi",
        PoolType.POOL_FEIBAN: "feiban",
    }

    # 池启用映射
    pool_enabled_map = {
        PoolType.POOL_YIZI: profile.yizi_enabled,
        PoolType.POOL_HUANSHOU: profile.huanshou_enabled,
        PoolType.POOL_FENQI: profile.fenqi_enabled,
        PoolType.POOL_FEIBAN: profile.feiban_enabled,
    }

    for pool_type in pool_order:
        result = pool_results.get(pool_type)
        pool_label = result.pool_label if result else pool_type.value
        pool_mult_key = pool_mult_map.get(pool_type, "feiban")
        pool_mult = profile.pool_multipliers.get(pool_mult_key, 1.0)
        pool_enabled = pool_enabled_map.get(pool_type, True)

        if not result or not result.top_n:
            pool_summary[pool_label] = {
                "enabled": False,
                "n_candidates": 0,
                "reason": "无候选" if not result else "池被禁用或排名为空",
                "pool_mult": pool_mult,
            }
            continue

        # 极端否决: 全池关闭
        if profile.extreme_veto:
            pool_enabled = False

        pool_summary[pool_label] = {
            "enabled": pool_enabled,
            "n_candidates": len(result.top_n),
            "n_total": len(result.candidates),
            "n_filtered": len(result.filtered_out),
            "pool_mult": pool_mult,
        }

        if not pool_enabled:
            reason = "极端否决" if profile.extreme_veto else "统计剖面禁用"
            plan.warnings.append(f"{pool_label}池被禁用: {reason}")
            continue

        # 池乘子过低 → 跳过
        if pool_mult < 0.05:
            plan.warnings.append(f"{pool_label}池乘子过低 ({pool_mult:.3f}), 跳过")
            continue

        # 为 Top N 构建执行指令
        for rk in result.top_n:
            if rk.filtered:
                continue

            rs = rk.routed
            height_mult = rs.height_multiplier if rs else 1.0
            risk_mult = _calc_risk_tag_multiplier(rs.risk_tags if rs else [])
            position = _calc_final_position(
                pool_type, height_mult, risk_mult,
                profile.position, pool_mult,
            )

            buy_strategy = _determine_buy_strategy(pool_type, profile.buy_mode, profile)

            order = ExecutionOrder(
                code=rk.code,
                name=rk.name,
                pool=pool_type,
                pool_label=pool_label,
                position_pct=round(position, 2),
                base_position_pct=POOL_BASE_POSITION.get(pool_type, 1.0),
                height_mult=height_mult,
                risk_mult=round(risk_mult, 2),
                profile_position=profile.position,
                pool_mult=pool_mult,
                buy_mode=profile.buy_mode,
                buy_strategy=buy_strategy,
                risk_tags=[t.value for t in (rs.risk_tags if rs else [])],
                confirmation_threshold=rs.confirmation_threshold if rs else "正常",
                rank=rk.rank,
                pool_rank=rk.rank,
                diagnostics={
                    "score_primary": rk.score_primary,
                    "score_secondary": rk.score_secondary,
                    "bonus_applied": rk.bonus_applied,
                    "fill_ratio": rk.diagnostics.get("fill_ratio"),
                },
            )
            orders.append(order)

    plan.orders = orders
    plan.allocated_position = sum(o.position_pct for o in orders)
    plan.reserve_position = max(0, 100.0 - plan.allocated_position)
    plan.pool_summary = pool_summary

    # 机动仓建议
    if plan.reserve_position < 15.0 and plan.allocated_position > 0:
        plan.warnings.append(f"机动仓仅 {plan.reserve_position:.1f}%, 建议保留 15%-25%")

    plan.diagnostics = {
        "n_orders": len(orders),
        "allocated_pct": plan.allocated_position,
        "reserve_pct": plan.reserve_position,
        "profile_position": profile.position,
        "bottleneck": profile.bottleneck_name,
        "heat": profile.heat,
        "divergence": profile.divergence,
    }

    return plan


# ============================================================================
# 输出格式化
# ============================================================================

def format_execution_plan(plan: ExecutionPlan) -> str:
    """将执行计划格式化为可读文本。"""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  盘前竞价选股系统 v5.0 — 执行计划")
    lines.append(f"  日期: {plan.date}")
    if plan.profile:
        lines.append(f"  瓶颈: {plan.profile.bottleneck_name} ({plan.profile.bottleneck:.3f}) "
                     f"| 温度: {plan.profile.heat:.3f} | 分歧: {plan.profile.divergence:.3f}")
        lines.append(f"  总仓位系数: {plan.profile.position:.3f} "
                     f"| 买点: {plan.profile.buy_mode}")
    lines.append("=" * 60)

    if plan.orders:
        lines.append(f"\n📊 可下单列表 ({len(plan.orders)} 只):")
        lines.append("-" * 60)
        for i, order in enumerate(plan.orders):
            lines.append(f"\n  [{i+1}] {order.code} {order.name}")
            lines.append(f"      池: {order.pool_label} | 排名: #{order.pool_rank}")
            lines.append(f"      仓位: {order.position_pct:.1f}% "
                         f"(基础{order.base_position_pct:.1f}% "
                         f"× 高度{order.height_mult:.2f} "
                         f"× 风险{order.risk_mult:.2f} "
                         f"× 剖面{order.profile_position:.3f} "
                         f"× 池乘子{order.pool_mult:.3f})")
            lines.append(f"      买点: {order.buy_strategy}")
            if order.risk_tags:
                lines.append(f"      风险: {', '.join(order.risk_tags)}")
            lines.append(f"      确认门槛: {order.confirmation_threshold}")
    else:
        lines.append("\n⚠️ 今日无可下单股票")

    lines.append(f"\n📈 仓位分配:")
    lines.append(f"  已分配: {plan.allocated_position:.1f}%")
    lines.append(f"  机动仓: {plan.reserve_position:.1f}%")

    if plan.warnings:
        lines.append(f"\n⚠️ 警告:")
        for w in plan.warnings:
            lines.append(f"  - {w}")

    # 池汇总
    lines.append(f"\n📋 池汇总:")
    for pool_name, info in plan.pool_summary.items():
        status = "✅" if info.get("enabled") else "❌"
        mult = info.get("pool_mult", 1.0)
        lines.append(f"  {status} {pool_name}: {info.get('n_candidates', 0)}只入选 "
                     f"/ {info.get('n_total', 0)}只候选 "
                     f"/ {info.get('n_filtered', 0)}只过滤 "
                     f"| 乘子×{mult:.3f}")

    return "\n".join(lines)


def execution_plan_to_dict(plan: ExecutionPlan) -> Dict[str, Any]:
    """将执行计划转换为可序列化字典。"""
    return {
        "date": plan.date,
        "profile_position": plan.profile_position,
        "allocated_position": plan.allocated_position,
        "reserve_position": plan.reserve_position,
        "orders": [
            {
                "code": o.code,
                "name": o.name,
                "pool": o.pool_label,
                "rank": o.pool_rank,
                "position_pct": o.position_pct,
                "base_position_pct": o.base_position_pct,
                "height_mult": o.height_mult,
                "risk_mult": o.risk_mult,
                "profile_position": o.profile_position,
                "pool_mult": o.pool_mult,
                "buy_mode": o.buy_mode,
                "buy_strategy": o.buy_strategy,
                "risk_tags": o.risk_tags,
                "confirmation_threshold": o.confirmation_threshold,
            }
            for o in plan.orders
        ],
        "pool_summary": plan.pool_summary,
        "warnings": plan.warnings,
        "diagnostics": plan.diagnostics,
    }


# ============================================================================
# 自检
# ============================================================================

def _self_test() -> bool:
    """自检: 验证 v5.0 执行计划构建。"""

    from duanxianxia_v5_0_d6_profile import (
        calculate_profile, ProfileHistory, MarketProfile,
    )
    from duanxianxia_v4_2_pool_ranker import RankedStock, PoolRankResult

    # 构建测试用 MarketProfile
    history = ProfileHistory()
    import random
    random.seed(42)
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

    # 构造测试用的池排名结果
    def _make_routed(code, name, pool, board_height=2, height_mult=1.0):
        return RoutedStock(
            code=code, name=name, pool=pool,
            board_height=board_height, height_multiplier=height_mult,
            confirmation_threshold="正常",
        )

    def _make_ranked(code, name, pool, pool_label, rank, routed):
        return RankedStock(
            code=code, name=name, pool=pool, pool_label=pool_label,
            rank=rank, original_rank=rank,
            routed=routed,
            score_primary=80.0, score_secondary=70.0,
        )

    pool_results = {
        PoolType.POOL_YIZI: PoolRankResult(
            pool_type=PoolType.POOL_YIZI, pool_label="一字封",
            candidates=[
                _make_ranked("000001", "测试A", PoolType.POOL_YIZI, "一字封", 1,
                             _make_routed("000001", "测试A", PoolType.POOL_YIZI, 2, 0.85)),
            ],
            top_n=[
                _make_ranked("000001", "测试A", PoolType.POOL_YIZI, "一字封", 1,
                             _make_routed("000001", "测试A", PoolType.POOL_YIZI, 2, 0.85)),
            ],
        ),
        PoolType.POOL_HUANSHOU: PoolRankResult(
            pool_type=PoolType.POOL_HUANSHOU, pool_label="换手封",
            candidates=[
                _make_ranked("000002", "测试B", PoolType.POOL_HUANSHOU, "换手封", 1,
                             _make_routed("000002", "测试B", PoolType.POOL_HUANSHOU, 3, 0.9)),
            ],
            top_n=[
                _make_ranked("000002", "测试B", PoolType.POOL_HUANSHOU, "换手封", 1,
                             _make_routed("000002", "测试B", PoolType.POOL_HUANSHOU, 3, 0.9)),
            ],
        ),
        PoolType.POOL_FENQI: PoolRankResult(
            pool_type=PoolType.POOL_FENQI, pool_label="分歧封",
            candidates=[],
            top_n=[],
        ),
        PoolType.POOL_FEIBAN: PoolRankResult(
            pool_type=PoolType.POOL_FEIBAN, pool_label="非板",
            candidates=[],
            top_n=[],
        ),
    }

    # 测试 1: 正常执行计划
    plan = build_execution_plan(pool_results, profile, date="2026-07-19")
    assert len(plan.orders) >= 1, f"应有至少 1 个订单, 实际 {len(plan.orders)}"
    assert plan.orders[0].pool_mult > 0, "池乘子应 > 0"
    assert plan.orders[0].position_pct > 0, "仓位应 > 0"
    print("  [PASS] 测试 1: 正常执行计划")

    # 测试 2: 极端否决下全池关闭
    profile_veto = calculate_profile(
        close_data=close_data,
        pre_data={
            "advance_share": 0.50, "dt": 10.0, "ztbx": -2.0,
            "lbbx": -3.0, "qx": 40.0,
        },
        relay_health=55.0, history=history,
        ztbx_pre_t1=2.0, lbbx_pre_t1=1.5,
    )
    plan_veto = build_execution_plan(pool_results, profile_veto, date="2026-07-19")
    assert len(plan_veto.orders) == 0, "极端否决下应无订单"
    assert plan_veto.profile_position == 0.0, "极端否决下仓位系数应为 0"
    print("  [PASS] 测试 2: 极端否决全池关闭")

    # 测试 3: 格式化输出
    text = format_execution_plan(plan)
    assert len(text) > 100, "格式化输出应有一定长度"
    d = execution_plan_to_dict(plan)
    assert "orders" in d, "字典输出应包含 orders"
    print("  [PASS] 测试 3: 格式化输出")

    # 测试 4: 空池结果
    empty_results = {
        PoolType.POOL_YIZI: PoolRankResult(
            pool_type=PoolType.POOL_YIZI, pool_label="一字封",
            candidates=[], top_n=[],
        ),
        PoolType.POOL_HUANSHOU: PoolRankResult(
            pool_type=PoolType.POOL_HUANSHOU, pool_label="换手封",
            candidates=[], top_n=[],
        ),
        PoolType.POOL_FENQI: PoolRankResult(
            pool_type=PoolType.POOL_FENQI, pool_label="分歧封",
            candidates=[], top_n=[],
        ),
        PoolType.POOL_FEIBAN: PoolRankResult(
            pool_type=PoolType.POOL_FEIBAN, pool_label="非板",
            candidates=[], top_n=[],
        ),
    }
    plan_empty = build_execution_plan(empty_results, profile, date="2026-07-19")
    assert len(plan_empty.orders) == 0, "空池结果应无订单"
    print("  [PASS] 测试 4: 空池结果")

    print("\n  ✅ 所有自检通过 (4/4)")
    return True


if __name__ == "__main__":
    import sys
    ok = _self_test()
    sys.exit(0 if ok else 1)