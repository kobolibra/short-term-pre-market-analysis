"""
duanxianxia_v9_assemble.py — 六层装配器。

bundle + 现有 v7 decisions → 附加 weimai/theme(v9)/context detail → market_env
→ compute_edge_v9 → shape_v9_output。
不侵入现有 v7 计算;只做全量装配/重打分/全保真输出。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import duanxianxia_v9_weimai as v9wm
import duanxianxia_v9_theme_strength as v9theme
import duanxianxia_v9_market_env as v9env
import duanxianxia_v9_context as v9ctx
import duanxianxia_v9_edge as v9edge
import duanxianxia_v9_output as v9out


def _codes(decisions: List[Dict[str, Any]]) -> List[str]:
    out, seen = [], set()
    for d in decisions or []:
        code = str(d.get("code") or "").strip()
        if code and code not in seen:
            seen.add(code)
            out.append(code)
    return out


def assemble_v9(
    bundle: Any,                         # PremarketDataBundle (含 auction_weimai 等全量字段)
    decisions: List[Dict[str, Any]],     # 现有 v7 引擎产出的候选
    *,
    theme_history: Optional[Dict[str, Dict[str, Any]]] = None,
    industry_t1: Optional[Dict[str, Dict[str, Any]]] = None,
    meta: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p = params or {}
    codes = _codes(decisions)

    # L2 weimai
    weimai = v9wm.compute_weimai_features(codes, getattr(bundle, "auction_weimai", []) or [], p)
    # L3 theme(全字段)
    theme = v9theme.compute_theme_strengths(
        decisions, getattr(bundle, "kaipan_t1_rows", []) or [],  # 注:若有 T0 plate 应传 T0 rows
        theme_history or {}, industry_t1 or {}, p,
    )
    # L4 market env(qxlive 全量 12 指标)
    market_env = v9env.compute_market_env(
        getattr(bundle, "qxlive_top_t0_rows", None) or getattr(bundle, "qxlive_top_t1_rows", []) or [],
        getattr(bundle, "qxlive_top_t1_rows", []) or [],
        getattr(bundle, "qxlive_top_t2_rows", []) or [],
        p,
    )
    # L5 context(T-1/历史)
    context = v9ctx.compute_stock_context(
        codes,
        cashflow_today=getattr(bundle, "cashflow_today_t1", []) or [],
        cashflow_3day=getattr(bundle, "cashflow_3day_t1", []) or [],
        cashflow_5day=getattr(bundle, "cashflow_5day_t1", []) or [],
        cashflow_10day=getattr(bundle, "cashflow_10day_t1", []) or [],
        fupan_t1=getattr(bundle, "fupan_t1", []) or [],
        ltgd_5day_t1=getattr(bundle, "ltgd_5day_t1", []) or [],
        ztpool_t1=getattr(bundle, "ztpool_t1", []) or [],
        params=p,
    )

    enriched: List[Dict[str, Any]] = []
    for d in decisions or []:
        code = str(d.get("code") or "").strip()
        row = dict(d)
        row["weimai_detail"] = weimai.get(code, {"weimai_present": False, "weimai_strength": 0.0})
        # theme_detail:以 v9 为准,保留原 theme_detail 作为 fallback
        v9_theme = theme.get(code)
        if v9_theme:
            row["theme_detail"] = v9_theme
            row["theme_strength_t0"] = v9_theme.get("theme_strength_t0")
        row["context_detail"] = context.get(code, {})
        edge = v9edge.compute_edge_v9(row, market_env, p)
        row.update(edge)
        enriched.append(row)

    return v9out.shape_v9_output(enriched, market_env=market_env, meta=meta, params=p) \
        if "params" in v9out.shape_v9_output.__code__.co_varnames \
        else v9out.shape_v9_output(enriched, market_env=market_env, meta=meta)
