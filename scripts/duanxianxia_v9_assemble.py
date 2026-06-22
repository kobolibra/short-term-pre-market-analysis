"""
duanxianxia_v9_assemble.py — 六层装配器。

bundle + 现有 v7 decisions → 附加 weimai/theme(v9)/context detail → market_env
→ compute_edge_v9 → shape_v9_output。
不侵入现有 v7 计算;只做全量装配/重打分/全保真输出。

T0 数据取用约定:
  - 主题强度优先使用今日盘前 kaipan_t0_rows,缺失时回退 kaipan_t1_rows(昨日)。
  - 市场环境优先使用 qxlive_top_t0_rows,缺失时回退 qxlive_top_t1_rows。
  这要求 loader 产出的 bundle 携带 T0 字段(见 duanxianxia_v7_1_data_loader)。
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


def _first_nonempty(*rowsets: Optional[List[Any]]) -> List[Any]:
    """返回第一个非空的 rows 集合;全部空则返回空列表。"""
    for rows in rowsets:
        if rows:
            return rows
    return []


def _auction_amount_pct_map(decisions: List[Dict[str, Any]]) -> Dict[str, float]:
    """v10: 对当天全体候选的竞价成交额(auction_amount_wan)标定横截面百分位(0-100)。

    必须在逐行 compute_edge_v9 之前按全体候选标定;缺失个股由调用方取中性 50。
    """
    pairs: List[tuple] = []
    for d in decisions or []:
        ad = d.get("auction_detail") or {}
        v = ad.get("auction_amount_wan")
        try:
            if v not in (None, "", "-", "None"):
                pairs.append((str(d.get("code") or "").strip(), float(str(v).replace(",", "").strip())))
        except Exception:
            pass
    pct_map: Dict[str, float] = {}
    if len(pairs) > 1:
        pairs.sort(key=lambda kv: kv[1])
        m = len(pairs)
        for rank, (code, _) in enumerate(pairs):
            pct_map[code] = rank / (m - 1) * 100.0
    elif len(pairs) == 1:
        pct_map[pairs[0][0]] = 50.0
    return pct_map


def assemble_v9(
    bundle: Any,                         # PremarketDataBundle (含 auction_weimai / T0 等全量字段)
    decisions: List[Dict[str, Any]],     # 现有 v7 引擎产出的候选
    *,
    theme_history: Optional[Dict[str, Dict[str, Any]]] = None,
    industry_t1: Optional[Dict[str, Dict[str, Any]]] = None,
    meta: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p = params or {}
    codes = _codes(decisions)

    # 主题用今日盘前 plate(T0),回退昨日(T1)
    kaipan_rows = _first_nonempty(
        getattr(bundle, "kaipan_t0_rows", None),
        getattr(bundle, "kaipan_t1_rows", None),
    )
    # 市场环境 qxlive T0,回退 T1
    qxlive_t0_rows = _first_nonempty(
        getattr(bundle, "qxlive_top_t0_rows", None),
        getattr(bundle, "qxlive_top_t1_rows", None),
    )

    # L2 weimai
    weimai = v9wm.compute_weimai_features(codes, getattr(bundle, "auction_weimai", []) or [], p)
    # L3 theme(全字段,基于今日盘前 plate)
    theme = v9theme.compute_theme_strengths(
        decisions, kaipan_rows,
        theme_history or {}, industry_t1 or {}, p,
    )
    # L4 market env(qxlive 全量 12 指标,T0 优先)
    market_env = v9env.compute_market_env(
        qxlive_t0_rows,
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

    # v10: 竞价成交额横截面百分位(在逐行 edge 之前按全体候选标定)
    amt_pct_map = _auction_amount_pct_map(decisions)

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
        # v10: 注入竞价成交额横截面百分位(不改原始 decision 的 auction_detail)
        _ad = dict(row.get("auction_detail") or {})
        _ad["auction_amount_pct"] = amt_pct_map.get(code, 50.0)
        row["auction_detail"] = _ad
        edge = v9edge.compute_edge_v9(row, market_env, p)
        row.update(edge)
        enriched.append(row)

    return v9out.shape_v9_output(enriched, market_env=market_env, meta=meta)
