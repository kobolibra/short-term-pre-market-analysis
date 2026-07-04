"""
duanxianxia_v9_output.py — 全保真 v9/v10 输出层。

原则:交易视图(池/排序)用精简卡片,但任何已计算/下载的字段都不丢:
每行额外挂 row['full'],包含各 detail/raw/source_hits/signal_summary/risk_detail/
label_snapshot/anchors;顶层 meta 挂 market_env 全量 12 指标。
与 v7.2/v7.3 output 共存,不抢占原函数名。

动作层(BUY/WATCH/DROP):在 shape_v9_output 阶段对已排序候选打动作标签,
采用 regime 自适应的"分位数 + 绝对下限 + 数量上限"闸门(REGIME_ACTION_GATE),
取代旧的固定 edge 阈值(68~75),避免 edge 实际分布偏低时买入列表恒空。
动作字段(action_type/action_score/setup)随 shaped 持久化进 analysis_v9.json,
并产出 action_stats / setup_stats / meta['action_gate']。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

VERSION = "premarket_v9"

# alpha_type -> 交易池 + 优先级
ALPHA_POOL = {
    "AUCTION_ORDERFLOW": ("main_attack_pool", 10),
    "LOW_OPEN_REVERSAL": ("low_open_reversal_pool", 30),
    "ORDERBOOK_WEIMAI": ("orderbook_weimai_pool", 20),
    "THEME_BACKGROUND": ("theme_background_pool", 40),
}

# alpha_type -> setup 归类标签
SETUP_BY_ALPHA = {
    "AUCTION_ORDERFLOW": "竞价资金流抢筹",
    "LOW_OPEN_REVERSAL": "低开反包",
    "ORDERBOOK_WEIMAI": "盘口委买承接",
    "THEME_BACKGROUND": "题材背景接力",
}

# regime 自适应动作闸门:买入=按 edge 排名进入 top 分位 且 edge>=绝对下限 且 不超数量上限。
# 用分位数取代固定阈值,使闸门随当日 edge 分布自适应;绝对下限仅作"弱势日清零"保护。
# [基线] Task 0140 曾建议 cold 放宽 / cold_to_warming 收紧;但 Task 0144 历史 A/B(20天)实测
#   该改动【净变差】:overall mean_excess 3.46->2.11, win 0.73->0.61(cold 放宽 1->2 把边际票
#   稀释:cold 2.31->1.15)。故【已全部回退到基线】。cold_to_warming 的 -17.37 尾部风险(0140)
#   改由后续【单独测试过的】首板闸门处理,不再用未验证的门控猜测。
REGIME_ACTION_GATE: Dict[str, Dict[str, float]] = {
    "cold":            {"buy_top_frac": 0.015, "buy_floor": 50.0, "max_buys": 1},
    "cold_to_warming": {"buy_top_frac": 0.030, "buy_floor": 48.0, "max_buys": 3},
    "warming":         {"buy_top_frac": 0.030, "buy_floor": 48.0, "max_buys": 3},
    "normal":          {"buy_top_frac": 0.050, "buy_floor": 45.0, "max_buys": 4},
    "hot":             {"buy_top_frac": 0.080, "buy_floor": 42.0, "max_buys": 5},
}
DEFAULT_ACTION_GATE: Dict[str, float] = {"buy_top_frac": 0.030, "buy_floor": 48.0, "max_buys": 3}
RISK_EXTRA_MARGIN = 8.0   # 风险行买入需额外 edge 余量
WATCH_TOP_FRAC = 0.25     # 观察:edge 排名 top 25%
WATCH_FLOOR = 35.0        # 观察:edge 绝对下限


def _regime_label(market_env: Any, meta: Optional[Dict[str, Any]]) -> str:
    sources = [market_env]
    if isinstance(meta, dict):
        sources.append(meta.get("regime"))
    for src in sources:
        if isinstance(src, dict):
            reg = src.get("regime")
            if isinstance(reg, dict):
                lab = reg.get("regime") or reg.get("label")
                if lab:
                    return str(lab)
            if isinstance(reg, str) and reg:
                return reg
            lab = src.get("label")
            if isinstance(lab, str) and lab:
                return lab
        elif isinstance(src, str) and src:
            return src
    return ""


def _edge_of(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("edge_score") or 0)
    except Exception:
        return 0.0


def _assign_actions(
    ranked: List[Dict[str, Any]],
    market_env: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
):
    """对已按 edge 降序排好的候选原地打 action_type/action_score/setup,返回统计与闸门信息。"""
    regime = _regime_label(market_env, meta)
    gate = REGIME_ACTION_GATE.get(regime, DEFAULT_ACTION_GATE)
    n = len(ranked)
    buy_floor = float(gate["buy_floor"])
    max_buys = int(gate["max_buys"])
    buy_rank_cap = max(1, int(round(n * float(gate["buy_top_frac"])))) if n else 0
    watch_rank_cap = max(buy_rank_cap, int(round(n * WATCH_TOP_FRAC))) if n else 0

    action_stats: Dict[str, int] = {"BUY": 0, "WATCH": 0, "DROP": 0}
    setup_stats: Dict[str, int] = {}
    buys = 0
    for idx, row in enumerate(ranked):
        edge = _edge_of(row)
        risk = bool(row.get("risk_flag"))
        floor = buy_floor + (RISK_EXTRA_MARGIN if risk else 0.0)
        if (idx < buy_rank_cap) and (edge >= floor) and (buys < max_buys):
            action = "BUY"
            buys += 1
        elif (idx < watch_rank_cap) and (edge >= WATCH_FLOOR):
            action = "WATCH"
        else:
            action = "DROP"
        setup = SETUP_BY_ALPHA.get(str(row.get("alpha_type")), "其他")
        if risk and action != "BUY":
            setup = "风险规避"
        row["action_type"] = action
        row["action_score"] = round(edge, 2)
        row["setup"] = setup
        action_stats[action] += 1
        if action != "DROP":
            setup_stats[setup] = setup_stats.get(setup, 0) + 1
    gate_info = {
        "regime": regime or "(unknown)",
        "buy_top_frac": float(gate["buy_top_frac"]),
        "buy_rank_cap": buy_rank_cap,
        "buy_floor": buy_floor,
        "max_buys": max_buys,
        "watch_rank_cap": watch_rank_cap,
        "watch_floor": WATCH_FLOOR,
        "risk_extra_margin": RISK_EXTRA_MARGIN,
        "candidate_count": n,
        "buy_selected": action_stats["BUY"],
    }
    return action_stats, setup_stats, gate_info


def _full(d: Dict[str, Any]) -> Dict[str, Any]:
    """全保真:保留所有 detail 与原始/诊断字段。"""
    return {
        "code": d.get("code"),
        "name": d.get("name"),
        "alpha_type": d.get("alpha_type"),
        "edge_score": d.get("edge_score"),
        "edge_components": d.get("edge_components") or {},
        "action_type": d.get("action_type"),
        "action_score": d.get("action_score"),
        "setup": d.get("setup"),
        "final_score": d.get("final_score"),
        "risk_flag": d.get("risk_flag"),
        "risk_detail": d.get("risk_detail") or {},
        # 各层完整 detail(不丢)
        "auction_detail": d.get("auction_detail") or {},
        "weimai_detail": d.get("weimai_detail") or {},
        "theme_detail": d.get("theme_detail") or {},
        "context_detail": d.get("context_detail") or {},
        "signal_summary": d.get("signal_summary") or {},
        "label_snapshot": d.get("label_snapshot") or {},
        # 原始证据
        "source_hits": d.get("source_hits") or [],
        "source_family_count": d.get("source_family_count") or (d.get("auction_detail") or {}).get("source_family_count"),
        "raw_rows": d.get("raw_rows") or {},
        "matched_themes": d.get("matched_themes") or [],
        "intraday_anchors": d.get("intraday_anchors") or [],
    }


def _compact(d: Dict[str, Any]) -> Dict[str, Any]:
    """精简卡片(交易视图用),但仍携带 full 全量。"""
    a = d.get("auction_detail") or {}
    t = d.get("theme_detail") or {}
    w = d.get("weimai_detail") or {}
    c = d.get("context_detail") or {}
    return {
        "code": d.get("code"),
        "name": d.get("name"),
        "alpha_type": d.get("alpha_type"),
        "edge_score": d.get("edge_score"),
        "action_type": d.get("action_type"),
        "action_score": d.get("action_score"),
        "setup": d.get("setup"),
        "auction_pct": a.get("latest_change_pct"),
        "auction_strength": d.get("auction_strength"),
        "auction_amount_wan": a.get("auction_amount_wan"),
        "net_pressure": a.get("net_pressure"),
        "fengdan_status": a.get("fengdan_status"),
        "qiangchou_920_925_rank": a.get("qiangchou_920_925_rank"),
        "qiangchou_last_second_rank": a.get("qiangchou_last_second_rank"),
        "net_amount_rank": a.get("net_amount_rank"),
        # weimai 摘要
        "weimai_strength": w.get("weimai_strength"),
        "weimai_amount_wan": w.get("weimai_amount_wan"),
        "weimai_board_label": w.get("board_label"),
        # 题材摘要(含资金/涨停数/子标签)
        "theme_strength_t0": d.get("theme_strength_t0"),
        "matched_plate": t.get("matched_plate"),
        "matched_tags": t.get("matched_tags") or [],
        "matched_level": t.get("matched_level"),
        "t0_plate_inflow_wan": t.get("t0_plate_inflow_wan"),
        "t0_limitup_count": t.get("t0_limitup_count"),
        "plate_strength_rank": t.get("plate_strength_rank"),
        "plate_inflow_rank": t.get("plate_inflow_rank"),
        # 上下文摘要
        "cashflow_continuity": c.get("cashflow_continuity"),
        "t1_in_ztpool": c.get("t1_in_ztpool"),
        "market_longtou_height": c.get("market_longtou_height"),
        "risk_flag": d.get("risk_flag"),
        "risk_detail": d.get("risk_detail") or {},
        # 全量明细挂载(不丢)
        "full": _full(d),
    }


def _sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows or [], key=lambda r: float(r.get("edge_score") or 0), reverse=True)


def build_pools(decisions: List[Dict[str, Any]], pool_max: int = 15) -> Dict[str, List[Dict[str, Any]]]:
    pools: Dict[str, List[Dict[str, Any]]] = {name: [] for name, _pri in ALPHA_POOL.values()}
    pools["risk_pool"] = []
    ranked = _sort(decisions)
    for d in ranked:
        if d.get("risk_flag") and float(d.get("edge_score") or 0) < 40:
            if len(pools["risk_pool"]) < pool_max:
                pools["risk_pool"].append(_compact(d))
            continue
        pool_name = ALPHA_POOL.get(str(d.get("alpha_type")), ("theme_background_pool", 40))[0]
        if len(pools[pool_name]) < pool_max:
            pools[pool_name].append(_compact(d))
    return pools


def shape_v9_output(
    decisions: List[Dict[str, Any]],
    market_env: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    max_candidates: int = 30,
    pool_max: int = 15,
) -> Dict[str, Any]:
    ranked = _sort(decisions)
    # 动作层:原地标注 action_type/action_score/setup(随后 _compact/_full/build_pools 自动携带)
    action_stats, setup_stats, action_gate = _assign_actions(ranked, market_env, meta)
    shaped_meta = dict(meta or {})
    shaped_meta["market_env"] = market_env or {}   # qxlive 全量 12 指标进 meta
    shaped_meta["action_gate"] = action_gate
    shaped_meta.setdefault("interpretation_notes", [])
    shaped_meta["interpretation_notes"] = list(shaped_meta["interpretation_notes"]) + [
        "v9 全量重构:所有盘前下载数据均保留(原始+派生+解释),每行挂 full 明细。",
        "主因子=竞价订单流+资金+成本赔率;辅=weimai/封单;背景=题材/环境/历史;风险单列。",
        "auction.jjyd.weimai 已完整接入 weimai_detail 与 edge 辅助因子。",
        "板块主力流入/涨停数量/子标签匹配已进 theme_detail 与 edge 背景因子。",
        "qxlive 12 指标全量保留(含 HSLN/PB/PBBX),进 meta.market_env 与 edge 背景/风险。",
        "动作层:BUY/WATCH/DROP 由 regime 自适应分位数闸门标定(见 meta.action_gate),已随分析文件持久化。",
    ]
    return {
        "version": VERSION,
        "meta": shaped_meta,
        "market_env": market_env or {},
        "alpha_stats": _alpha_stats(ranked),
        "action_stats": action_stats,
        "setup_stats": setup_stats,
        "candidate_pools": build_pools(ranked, pool_max=pool_max),
        "top_candidates": [_compact(d) for d in ranked[:max_candidates]],
        "all_candidates": [_compact(d) for d in ranked],   # 全量,含 full
    }


def _alpha_stats(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        k = str(r.get("alpha_type") or "UNKNOWN")
        out[k] = out.get(k, 0) + 1
    return out


def write_v9_outputs(output_dir: str, shaped: Dict[str, Any], filename: str = "analysis_v9.json") -> str:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    path.write_text(json.dumps(shaped, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return str(path)
