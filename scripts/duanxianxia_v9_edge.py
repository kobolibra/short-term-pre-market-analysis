"""
duanxianxia_v9_edge.py — 四类因子合成的 v10 alpha edge。

edge = main(主) + aux(辅) + background(背景) - risk_penalty(风险)
产出 edge_score(0-100) / edge_components / alpha_type / risk_flag / risk_detail,
全部作为可解释字段输出,不丢。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, "", "-", "None"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _low_cost_score(pct: Optional[float], lo: float = -1.5, hi: float = 4.0, center: float = 1.0) -> float:
    """成本赔率:贴近平/微正最优,高开越多赔率越差,深贴也扣分。"""
    if pct is None:
        return 45.0
    if lo <= pct <= hi:
        span = max(abs(hi - center), abs(center - lo))
        return _clamp(100.0 - abs(pct - center) / span * 45.0)
    if pct > hi:
        return _clamp(55.0 - (pct - hi) * 9.0)
    return _clamp(55.0 - (lo - pct) * 7.0)


def compute_edge_v9(
    decision: Dict[str, Any],
    market_env: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p = params or {}
    env = market_env or {}
    a = decision.get("auction_detail") or {}
    t = decision.get("theme_detail") or {}
    w = decision.get("weimai_detail") or {}
    c = decision.get("context_detail") or {}

    auction_strength = _f(decision.get("auction_strength") or a.get("auction_strength"))
    pct = a.get("latest_change_pct")
    pct = _f(pct, None) if pct is not None else _f(decision.get("auction_pct"), None)
    money = _f(a.get("money_intent_score") or a.get("net_amount_rank") or 0.0)
    net_pressure = _f(a.get("net_pressure"), 0.0)
    orderbook = _f(a.get("orderbook_quality_score"), 45.0)
    liquidity = _f(a.get("liquidity_score"), 50.0)
    source_evidence = _f(a.get("source_evidence_score"), 0.0)

    # --- 主因子:订单流 + 资金 + 成本赔率 ---
    low_cost = _low_cost_score(pct, float(p.get("edge_lowcost_lo", -1.5)), float(p.get("edge_lowcost_hi", 4.0)))
    pressure_score = _clamp(max(0.0, net_pressure) / float(p.get("net_pressure_full", 0.002)) * 100.0) if net_pressure else 0.0
    main_factor = _clamp(
        0.45 * auction_strength
        + 0.25 * max(money, pressure_score)
        + 0.20 * low_cost
        + 0.10 * min(100.0, source_evidence * 3.0)
    )

    # --- 辅助:weimai + 封单/盘口真实性 ---
    weimai_strength = _f(w.get("weimai_strength"), 0.0)
    aux_factor = _clamp(0.55 * weimai_strength + 0.30 * orderbook + 0.15 * liquidity)

    # --- 背景:题材 + 市场环境 + 历史资金 + 龙头高度 ---
    theme = _f(decision.get("theme_strength_t0") or t.get("theme_strength_t0"), 0.0)
    env_score = _f(env.get("market_env_score"), 50.0)
    continuity = {"strong": 80.0, "medium": 55.0, "weak": 30.0, "unknown": 45.0}.get(str(c.get("cashflow_continuity") or "unknown"), 45.0)
    longtou = _f(c.get("market_longtou_height"), 0.0)
    longtou_score = _clamp(longtou / float(p.get("longtou_full_height", 8.0)) * 100.0)
    background_factor = _clamp(0.45 * theme + 0.25 * env_score + 0.20 * continuity + 0.10 * longtou_score)

    # --- 风险因子 ---
    risk_detail: Dict[str, Any] = {}
    risk_penalty = 0.0
    if pct is not None and pct >= float(p.get("edge_high_cost_pct", 7.0)):
        risk_detail["high_open_cost"] = pct
        risk_penalty += float(p.get("edge_high_cost_penalty", 14))
    if liquidity <= float(p.get("edge_low_liquidity", 35)):
        risk_detail["low_liquidity"] = liquidity
        risk_penalty += float(p.get("edge_low_liquidity_penalty", 12))
    if str(a.get("fengdan_status") or "").lower() in {"fake", "consume", "假封单", "消耗"}:
        risk_detail["fake_or_consuming_seal"] = a.get("fengdan_status")
        risk_penalty += float(p.get("edge_fake_seal_penalty", 16))
    if str(a.get("auction_setup_type") or "") == "FAKE_STRENGTH":
        risk_detail["fake_strength"] = True
        risk_penalty += float(p.get("edge_fake_strength_penalty", 18))
    env_flags = env.get("risk_flags") or []
    if "relay_deteriorating" in env_flags:
        risk_detail["relay_env_deteriorating"] = True
        risk_penalty += float(p.get("edge_relay_penalty", 8))
    if env_flags:
        risk_detail["market_env_flags"] = env_flags

    edge_score = _clamp(
        float(p.get("edge_w_main", 0.50)) * main_factor
        + float(p.get("edge_w_aux", 0.22)) * aux_factor
        + float(p.get("edge_w_background", 0.28)) * background_factor
        - risk_penalty
    )

    # alpha_type:主导因子
    contributions = {
        "AUCTION_ORDERFLOW": float(p.get("edge_w_main", 0.50)) * main_factor,
        "ORDERBOOK_WEIMAI": float(p.get("edge_w_aux", 0.22)) * aux_factor,
        "THEME_BACKGROUND": float(p.get("edge_w_background", 0.28)) * background_factor,
    }
    alpha_type = max(contributions, key=contributions.get)
    if pct is not None and pct < 0 and main_factor >= float(p.get("reversal_min_main", 40)):
        alpha_type = "LOW_OPEN_REVERSAL"

    return {
        "edge_score": round(edge_score, 2),
        "alpha_type": alpha_type,
        "edge_components": {
            "main_factor": round(main_factor, 2),
            "aux_factor": round(aux_factor, 2),
            "background_factor": round(background_factor, 2),
            "risk_penalty": round(risk_penalty, 2),
            "sub": {
                "auction_strength": round(auction_strength, 2),
                "low_cost": round(low_cost, 2),
                "money": round(money, 2),
                "pressure_score": round(pressure_score, 2),
                "weimai_strength": round(weimai_strength, 2),
                "orderbook": round(orderbook, 2),
                "liquidity": round(liquidity, 2),
                "theme_strength_t0": round(theme, 2),
                "market_env_score": round(env_score, 2),
                "cashflow_continuity_score": round(continuity, 2),
                "longtou_score": round(longtou_score, 2),
            },
        },
        "risk_flag": bool(risk_detail),
        "risk_detail": risk_detail,
    }


def _self_test() -> None:
    d = {
        "code": "600000", "auction_strength": 78, "theme_strength_t0": 70,
        "auction_detail": {"latest_change_pct": 3.0, "money_intent_score": 70,
                           "net_pressure": 0.0015, "orderbook_quality_score": 60,
                           "liquidity_score": 70, "source_evidence_score": 25},
        "theme_detail": {"theme_strength_t0": 70},
        "weimai_detail": {"weimai_strength": 65},
        "context_detail": {"cashflow_continuity": "strong", "market_longtou_height": 6},
    }
    env = {"market_env_score": 68, "risk_flags": []}
    out = compute_edge_v9(d, env, {})
    assert out["edge_score"] > 55
    assert out["alpha_type"] in {"AUCTION_ORDERFLOW", "THEME_BACKGROUND", "ORDERBOOK_WEIMAI"}
    assert out["risk_flag"] is False
    print("v9_edge _self_test passed")


if __name__ == "__main__":
    _self_test()
