"""
duanxianxia_v9_output.py — 全保真 v9/v10 输出层。

原则:交易视图(池/排序)用精简卡片,但任何已计算/下载的字段都不丢:
每行额外挂 row['full'],包含各 detail/raw/source_hits/signal_summary/risk_detail/
label_snapshot/anchors;顶层 meta 挂 market_env 全量 12 指标。
与 v7.2/v7.3 output 共存,不抢占原函数名。
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
    shaped_meta = dict(meta or {})
    shaped_meta["market_env"] = market_env or {}   # qxlive 全量 12 指标进 meta
    shaped_meta.setdefault("interpretation_notes", [])
    shaped_meta["interpretation_notes"] = list(shaped_meta["interpretation_notes"]) + [
        "v9 全量重构:所有盘前下载数据均保留(原始+派生+解释),每行挂 full 明细。",
        "主因子=竞价订单流+资金+成本赔率;辅=weimai/封单;背景=题材/环境/历史;风险单列。",
        "auction.jjyd.weimai 已完整接入 weimai_detail 与 edge 辅助因子。",
        "板块主力流入/涨停数量/子标签匹配已进 theme_detail 与 edge 背景因子。",
        "qxlive 12 指标全量保留(含 HSLN/PB/PBBX),进 meta.market_env 与 edge 背景/风险。",
    ]
    return {
        "version": VERSION,
        "meta": shaped_meta,
        "market_env": market_env or {},
        "alpha_stats": _alpha_stats(ranked),
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
