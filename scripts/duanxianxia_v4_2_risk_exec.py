#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_risk_exec.py  --  v4.2 风控与执行模块

第四层：风控独立于排序层。高分票不一定是可交易结构。
风控层不回头检查已在路由层定性的静态标签。

输出:
  - 最终可下单列表
  - 仓位指令
  - 买点模式

设计文档: dimension-design-v4/dimension-design-v4.html §6
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_d6_emotion import RiskTier, BuyMode, D6EmotionResult
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
    position_pct: float = 0.0         # 最终仓位百分比
    base_position_pct: float = 0.0    # 基础仓位
    height_mult: float = 1.0          # 高度乘子
    risk_mult: float = 1.0            # 风险标签乘子
    emotion_cap: float = 1.0          # 情绪总仓位上限

    # 买点
    buy_mode: str = ""                # 买点模式描述
    buy_strategy: str = ""            # 具体执行策略

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
    emotion_state: str = ""
    emotion_result: Optional[D6EmotionResult] = None

    # 总仓位
    total_position_cap: float = 1.0

    # 各池执行指令
    orders: List[ExecutionOrder] = field(default_factory=list)

    # 池级别汇总
    pool_summary: Dict[str, Any] = field(default_factory=dict)

    # 仓位分配
    allocated_position: float = 0.0    # 已分配仓位
    reserve_position: float = 0.0      # 机动仓

    # 诊断
    warnings: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 仓位计算
# ============================================================================

# 各池单票基础仓位
_POOL_BASE_POSITION: Dict[PoolType, float] = {
    PoolType.POOL_YIZI: 4.0,       # 最强共识延续
    PoolType.POOL_HUANSHOU: 3.0,   # 真金白银接力
    PoolType.POOL_FENQI: 1.5,      # 分歧修复试错
    PoolType.POOL_FEIBAN: 3.0,     # 新共识形成
}

# 各池单票仓位上限
_POOL_POSITION_CAP: Dict[PoolType, float] = {
    PoolType.POOL_YIZI: 8.0,
    PoolType.POOL_HUANSHOU: 6.0,
    PoolType.POOL_FENQI: 3.0,
    PoolType.POOL_FEIBAN: 6.0,
}


def _calc_risk_tag_multiplier(risk_tags: List[RiskTag]) -> float:
    """计算风险标签综合乘子"""
    mult = 1.0
    for tag in risk_tags:
        if tag == RiskTag.HEAVY_DIVERGENCE:
            mult *= 0.7
        elif tag == RiskTag.WEAK_SEAL:
            mult *= 0.8
        # HIGH_LEVEL 和 GAP_UP_WEAK 不在此处处理
        # HIGH_LEVEL 已通过 height_multiplier 处理
        # GAP_UP_WEAK 已在风控过滤层否决
    return mult


def _calc_final_position(
    pool_type: PoolType,
    height_mult: float,
    risk_mult: float,
    emotion_cap: float,
    pool_mult: float = 1.0,
) -> float:
    """
    最终仓位 = 基础仓位 × 高度乘子 × 风险标签乘子 × 情绪总仓位 × 池乘子

    上限: 不超过该池的单票仓位上限。
    """
    base = _POOL_BASE_POSITION.get(pool_type, 1.0)
    cap = _POOL_POSITION_CAP.get(pool_type, 5.0)
    position = base * height_mult * risk_mult * emotion_cap * pool_mult
    return min(position, cap)


# ============================================================================
# 执行计划构建
# ============================================================================

def build_execution_plan(
    pool_results: Dict[PoolType, PoolRankResult],
    emotion_result: D6EmotionResult,
    date: str = "",
) -> ExecutionPlan:
    """
    根据池排名结果和情绪状态，构建完整执行计划。

    Args:
        pool_results: 各池排名结果
        emotion_result: D6 情绪周期结果
        date: 日期

    Returns:
        ExecutionPlan 完整执行计划
    """
    plan = ExecutionPlan(
        date=date,
        emotion_state=emotion_result.phase_label,
        emotion_result=emotion_result,
        total_position_cap=emotion_result.position_cap,
    )

    orders: List[ExecutionOrder] = []
    pool_summary: Dict[str, Any] = {}

    # 建仓顺序: 一字封 → 换手封 → 非板 → 分歧封
    pool_order = [
        PoolType.POOL_YIZI,
        PoolType.POOL_HUANSHOU,
        PoolType.POOL_FEIBAN,
        PoolType.POOL_FENQI,
    ]

    for pool_type in pool_order:
        result = pool_results.get(pool_type)
        if not result or not result.top_n:
            pool_summary[result.pool_label if result else pool_type.value] = {
                "enabled": False,
                "n_candidates": 0,
                "reason": "无候选" if not result else "池被禁用或排名为空",
            }
            continue

        # 检查 D6 是否禁用该池
        pool_enabled = True
        pool_mult = 1.0
        if pool_type == PoolType.POOL_YIZI:
            pool_enabled = emotion_result.yizi_enabled
            pool_mult = emotion_result.pool_yizi_mult
        elif pool_type == PoolType.POOL_HUANSHOU:
            pool_enabled = emotion_result.huanshou_enabled
            pool_mult = emotion_result.pool_huanshou_mult
        elif pool_type == PoolType.POOL_FENQI:
            pool_enabled = emotion_result.fenqi_enabled
            pool_mult = emotion_result.pool_fenqi_mult
        elif pool_type == PoolType.POOL_FEIBAN:
            pool_enabled = emotion_result.feiban_enabled
            pool_mult = emotion_result.pool_feiban_mult

        pool_summary[result.pool_label] = {
            "enabled": pool_enabled,
            "n_candidates": len(result.top_n),
            "n_total": len(result.candidates),
            "n_filtered": len(result.filtered_out),
            "pool_mult": pool_mult,
        }

        if not pool_enabled:
            plan.warnings.append(f"{result.pool_label}池被 D6 禁用")
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
                emotion_result.position_cap, pool_mult
            )

            # 确定买点策略
            buy_strategy = _determine_buy_strategy(pool_type, emotion_result.risk_tier)

            order = ExecutionOrder(
                code=rk.code,
                name=rk.name,
                pool=pool_type,
                pool_label=result.pool_label,
                position_pct=round(position, 2),
                base_position_pct=_POOL_BASE_POSITION.get(pool_type, 1.0),
                height_mult=height_mult,
                risk_mult=round(risk_mult, 2),
                emotion_cap=emotion_result.position_cap,
                buy_mode=emotion_result.buy_mode.value,
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

    # 保留机动仓建议
    if plan.reserve_position < 15.0:
        plan.warnings.append(f"机动仓仅 {plan.reserve_position:.1f}%, 建议保留 15%-25%")

    plan.diagnostics = {
        "n_orders": len(orders),
        "allocated_pct": plan.allocated_position,
        "reserve_pct": plan.reserve_position,
        "emotion_cap": emotion_result.position_cap,
    }

    return plan


def _determine_buy_strategy(pool_type: PoolType, risk_tier: RiskTier) -> str:
    """根据池类型和风险等级确定具体买点策略"""
    if risk_tier == RiskTier.CRISIS:
        if pool_type == PoolType.POOL_FENQI:
            return "仅分歧封轻仓试错，排板确认"
        return "CRISIS 禁用"

    if risk_tier == RiskTier.WARNING:
        if pool_type == PoolType.POOL_YIZI:
            return "排板为主，不竞价买"
        elif pool_type == PoolType.POOL_HUANSHOU:
            return "排板/扫板确认，不竞价买"
        elif pool_type == PoolType.POOL_FENQI:
            return "排板确认（WARNING下分歧封优先级提升）"
        elif pool_type == PoolType.POOL_FEIBAN:
            return "排板/扫板确认，不竞价买"

    # NORMAL
    if pool_type == PoolType.POOL_YIZI:
        return "排板为主（一字封竞价无法买入）"
    elif pool_type == PoolType.POOL_HUANSHOU:
        return "竞价挂涨停价买 + 排板并行"
    elif pool_type == PoolType.POOL_FENQI:
        return "竞价挂涨停价买（分歧修复）"
    elif pool_type == PoolType.POOL_FEIBAN:
        return "竞价挂涨停价买（新共识形成）"

    return "竞价挂涨停价买"


# ============================================================================
# 输出格式化
# ============================================================================

def format_execution_plan(plan: ExecutionPlan) -> str:
    """将执行计划格式化为可读文本"""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  盘前竞价选股系统 v4.2 — 执行计划")
    lines.append(f"  日期: {plan.date}")
    lines.append(f"  情绪状态: {plan.emotion_state}")
    lines.append(f"  总仓位上限: {plan.total_position_cap * 100:.0f}%")
    lines.append("=" * 60)

    if plan.orders:
        lines.append(f"\n📊 可下单列表 ({len(plan.orders)} 只):")
        lines.append("-" * 60)
        for i, order in enumerate(plan.orders):
            lines.append(f"\n  [{i+1}] {order.code} {order.name}")
            lines.append(f"      池: {order.pool_label} | 排名: #{order.pool_rank}")
            lines.append(f"      仓位: {order.position_pct:.1f}% (基础{order.base_position_pct:.1f}% × 高度{order.height_mult:.2f} × 风险{order.risk_mult:.2f} × 情绪{order.emotion_cap:.2f})")
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
        lines.append(f"  {status} {pool_name}: {info.get('n_candidates', 0)}只入选 / {info.get('n_total', 0)}只候选 / {info.get('n_filtered', 0)}只过滤")

    return "\n".join(lines)


def execution_plan_to_dict(plan: ExecutionPlan) -> Dict[str, Any]:
    """将执行计划转换为可序列化字典"""
    return {
        "date": plan.date,
        "emotion_state": plan.emotion_state,
        "total_position_cap": plan.total_position_cap,
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
                "emotion_cap": o.emotion_cap,
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
    """自检: 验证执行计划构建"""

    # 构造测试数据
    emo = D6EmotionResult(
        risk_tier=RiskTier.NORMAL, phase_label="NORMAL",
        position_cap=1.0,
        yizi_enabled=True, huanshou_enabled=True,
        fenqi_enabled=True, feiban_enabled=True,
        pool_yizi_mult=1.0, pool_huanshou_mult=1.0,
        pool_fenqi_mult=1.0, pool_feiban_mult=1.0,
    )

    from duanxianxia_v4_2_pool_ranker import RankedStock, PoolRankResult

    # 一字封池
    yizi_result = PoolRankResult(
        pool_type=PoolType.POOL_YIZI, pool_label="一字封",
        candidates=[
            RankedStock(code="000001", name="A", pool=PoolType.POOL_YIZI, pool_label="一字封",
                        rank=1, original_rank=1,
                        routed=RoutedStock(code="000001", name="A", pool=PoolType.POOL_YIZI,
                                           board_height=2, height_multiplier=0.85,
                                           confirmation_threshold="正常")),
        ],
        top_n=[
            RankedStock(code="000001", name="A", pool=PoolType.POOL_YIZI, pool_label="一字封",
                        rank=1, original_rank=1,
                        routed=RoutedStock(code="000001", name="A", pool=PoolType.POOL_YIZI,
                                           board_height=2, height_multiplier=0.85,
                                           confirmation_threshold="正常")),
        ],
    )

    # 非板池
    feiban_result = PoolRankResult(
        pool_type=PoolType.POOL_FEIBAN, pool_label="非板",
        candidates=[
            RankedStock(code="000005", name="E", pool=PoolType.POOL_FEIBAN, pool_label="非板",
                        rank=1, original_rank=1,
                        routed=RoutedStock(code="000005", name="E", pool=PoolType.POOL_FEIBAN,
                                           board_height=0, height_multiplier=1.0,
                                           confirmation_threshold="正常")),
        ],
        top_n=[
            RankedStock(code="000005", name="E", pool=PoolType.POOL_FEIBAN, pool_label="非板",
                        rank=1, original_rank=1,
                        routed=RoutedStock(code="000005", name="E", pool=PoolType.POOL_FEIBAN,
                                           board_height=0, height_multiplier=1.0,
                                           confirmation_threshold="正常")),
        ],
    )

    pool_results = {
        PoolType.POOL_YIZI: yizi_result,
        PoolType.POOL_HUANSHOU: PoolRankResult(pool_type=PoolType.POOL_HUANSHOU, pool_label="换手封"),
        PoolType.POOL_FENQI: PoolRankResult(pool_type=PoolType.POOL_FENQI, pool_label="分歧封"),
        PoolType.POOL_FEIBAN: feiban_result,
    }

    # 测试 1: NORMAL 执行计划
    plan = build_execution_plan(pool_results, emo, date="2026-07-12")
    assert len(plan.orders) == 2, f"Expected 2 orders, got {len(plan.orders)}"
    assert plan.emotion_state == "NORMAL"
    assert plan.total_position_cap == 1.0

    # 一字封: 基础 4% × 高度 0.85 = 3.4%
    yizi_order = [o for o in plan.orders if o.pool == PoolType.POOL_YIZI][0]
    assert yizi_order.position_pct == 3.4, f"Expected 3.4%, got {yizi_order.position_pct}%"

    # 非板: 基础 3% × 高度 1.0 = 3.0%
    feiban_order = [o for o in plan.orders if o.pool == PoolType.POOL_FEIBAN][0]
    assert feiban_order.position_pct == 3.0, f"Expected 3.0%, got {feiban_order.position_pct}%"

    # 测试 2: CRISIS 执行计划
    emo_crisis = D6EmotionResult(
        risk_tier=RiskTier.CRISIS, phase_label="CRISIS",
        position_cap=0.2,
        yizi_enabled=False, huanshou_enabled=False,
        fenqi_enabled=True, feiban_enabled=False,
        pool_fenqi_mult=0.3,
    )
    plan_crisis = build_execution_plan(pool_results, emo_crisis, date="2026-07-12")
    # CRISIS 下只有分歧封可参与，但分歧封没有候选
    assert len(plan_crisis.orders) == 0, f"Expected 0 orders in CRISIS, got {len(plan_crisis.orders)}"

    # 测试 3: 格式化输出
    output = format_execution_plan(plan)
    assert "000001" in output
    assert "000005" in output
    assert "NORMAL" in output

    # 测试 4: 转字典
    d = execution_plan_to_dict(plan)
    assert len(d["orders"]) == 2
    assert d["emotion_state"] == "NORMAL"

    return True


_self_test()


if __name__ == "__main__":
    print("duanxianxia_v4_2_risk_exec self-test: PASS")