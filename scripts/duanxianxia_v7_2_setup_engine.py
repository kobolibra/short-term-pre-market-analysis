"""v7.2 setup engine — T0-driven premarket scoring.

Design:
    today_signal_raw = auction/theme/hotness weighted score
    t1_multiplier    = clamp(1 + T-1 soft adjustments, 0.75, 1.35)
    final_score      = today_signal_raw * t1_multiplier * regime_multiplier * risk_penalty

Only ST / delisting is a hard kill.  Historical signals only amplify or degrade.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def _norm_code(v: Any) -> str:
    m = re.search(r"(\d{6})", str(v or ""))
    return m.group(1) if m else ""


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, "", "-"):
            return default
        return float(str(v).replace("%", "").strip())
    except Exception:
        return default


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _is_st_or_delist(candidate: Dict[str, Any], daily_rows: Optional[List[Dict[str, Any]]] = None) -> bool:
    name = str(candidate.get("name") or candidate.get("名称") or "")
    if "ST" in name.upper() or "退" in name:
        return True
    for row in daily_rows or []:
        if str(row.get("isST") or row.get("is_st") or "").strip() in {"1", "true", "True", "是"}:
            return True
        if "退" in str(row.get("name") or row.get("名称") or ""):
            return True
    return False


def _board_count(zt: Dict[str, Any]) -> int:
    for key in ("board_count", "连板数", "板数", "boards"):
        if key in zt:
            try:
                return int(float(str(zt.get(key)).strip()))
            except Exception:
                pass
    pattern = str(zt.get("zt_pattern") or zt.get("pattern") or "")
    if "三板" in pattern:
        return 3
    if "二板" in pattern:
        return 2
    if "首板" in pattern:
        return 1
    return 0


def _is_exploded(zt: Dict[str, Any]) -> bool:
    text = " ".join(str(zt.get(k) or "") for k in (
        "zt_seal_verified", "seal_verified", "status", "状态", "状态样式", "ztpool_status", "ztpool_status_class", "zt_quality", "quality_label"
    ))
    return "exploded" in text.lower() or "zha" in text.lower() or "炸" in text or "烂板" in text


def _main_flow_wan(stock_t1: Dict[str, Any], cash: Dict[str, Any]) -> float:
    """Return today's main flow in 万.

    v7.1 stock_t1 exposes `main_inflow_wan`; cashflow_continuity exposes
    `today_wan`.  Also support future/Chinese aliases.
    """
    for obj in (stock_t1, cash):
        for key in (
            "main_inflow_wan", "today_wan", "main_net_inflow_wan", "main_net_wan",
            "主力净流入万", "today_main_net_wan", "net_main_wan",
        ):
            if key in obj:
                return _f(obj.get(key), 0.0)
    return 0.0


def _churn_type(tech: Dict[str, Any], params: Dict[str, Any]) -> str:
    existing = str(tech.get("churn_type") or "").strip()
    if existing in {"panic_churn", "dull_churn", "none"}:
        return existing
    profile = str(tech.get("tech_profile") or tech.get("label") or "")
    if profile != "churn_high_volume":
        return "none"
    pct = _f(tech.get("pct_chg") or tech.get("pct_chg_t1") or tech.get("change_pct") or tech.get("latest_pct"), 0.0)
    vol_ratio = _f(tech.get("volume_ratio") or tech.get("vol_ratio") or tech.get("vol_ratio_t1"), 0.0)
    if pct <= float(params.get("churn_panic_pct_chg_max", -3.0)) and vol_ratio >= float(params.get("churn_panic_vol_ratio_min", 3.0)):
        return "panic_churn"
    return "dull_churn"


def _is_breakdown_profile(profile: str) -> bool:
    p = str(profile or "")
    return p == "breakdown" or p == "weak" or p.startswith("weak:")


def compute_today_signal_raw(auction: float, theme: float, hotness: Optional[float], params: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    wa = float(params.get("weight_auction", 0.60))
    wt = float(params.get("weight_theme", 0.20))
    wh = float(params.get("weight_hotness", 0.20))
    if hotness is None:
        s = wa + wt
        wa2, wt2, wh2 = wa / s, wt / s, 0.0
        score = wa2 * auction + wt2 * theme
    else:
        wa2, wt2, wh2 = wa, wt, wh
        score = wa * auction + wt * theme + wh * float(hotness)
    return round(score, 2), {"auction": round(wa2, 4), "theme": round(wt2, 4), "hotness": round(wh2, 4)}


def compute_t1_multiplier(snapshot: Dict[str, Any], auction_strength: float, params: Dict[str, Any]) -> Tuple[float, List[Dict[str, Any]]]:
    adj: List[Dict[str, Any]] = []
    longtou = str(snapshot.get("longtou_status") or "none")
    cash_label = str(snapshot.get("cashflow_continuity") or "none")
    zt_quality = str(snapshot.get("zt_quality") or "average")
    tech_profile = str(snapshot.get("tech_profile") or "unknown")
    stock_t1 = snapshot.get("stock_t1") or {}
    cash_obj = snapshot.get("cashflow_raw") or {}
    main_flow = _main_flow_wan(stock_t1 if isinstance(stock_t1, dict) else {}, cash_obj if isinstance(cash_obj, dict) else {})

    iceberg = auction_strength >= float(params.get("iceberg_auction_threshold", 80)) and longtou == "none"
    if iceberg:
        adj.append({"key": "iceberg", "value": float(params.get("iceberg_bonus", 0.10))})
    if longtou == "confirmed_longtou":
        adj.append({"key": "longtou_confirmed", "value": float(params.get("longtou_confirmed_bonus", 0.10))})
    elif longtou == "board_leader":
        adj.append({"key": "longtou_board_leader", "value": float(params.get("longtou_board_leader_bonus", 0.05))})
    if cash_label == "accumulating_strong":
        adj.append({"key": "cashflow_strong", "value": float(params.get("cashflow_strong_bonus", 0.08))})
    if cash_label == "distributing" and main_flow < float(params.get("cashflow_distributing_outflow_threshold_wan", -300)):
        adj.append({"key": "cashflow_distributing", "value": float(params.get("cashflow_distributing_penalty", -0.10))})
    if zt_quality == "clean":
        adj.append({"key": "zt_clean", "value": float(params.get("zt_clean_bonus", 0.05))})
    elif zt_quality == "dirty":
        adj.append({"key": "zt_dirty", "value": float(params.get("zt_dirty_penalty", -0.08))})
    if _is_breakdown_profile(tech_profile) and not iceberg:
        adj.append({"key": "tech_breakdown", "value": float(params.get("tech_breakdown_penalty", -0.08))})

    churn = _churn_type(snapshot.get("tech_raw") or {}, params)
    if churn == "dull_churn":
        value = float(params.get("churn_dull_iceberg_penalty", -0.07)) if iceberg else float(params.get("churn_dull_penalty", -0.15))
        adj.append({"key": "dull_churn", "value": value})
    elif churn == "panic_churn":
        adj.append({"key": "panic_churn", "value": float(params.get("churn_panic_penalty", -0.05))})

    raw = 1.0 + sum(float(x["value"]) for x in adj)
    lo = float(params.get("t1_multiplier_min", 0.75))
    hi = float(params.get("t1_multiplier_max", 1.35))
    return round(_clip(raw, lo, hi), 4), adj


def compute_risk_penalty(candidate: Dict[str, Any], snapshot: Dict[str, Any], params: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    if _is_st_or_delist(candidate, snapshot.get("daily_rows")):
        return 0.0, {"hard_kill": "ST_or_delist"}
    zt = snapshot.get("zt_raw") or {}
    stock_t1 = snapshot.get("stock_t1") or {}
    cash_obj = snapshot.get("cashflow_raw") or {}
    board = _board_count(zt if isinstance(zt, dict) else {})
    exploded = _is_exploded(zt if isinstance(zt, dict) else {})
    main_flow = _main_flow_wan(stock_t1 if isinstance(stock_t1, dict) else {}, cash_obj if isinstance(cash_obj, dict) else {})
    heavy_outflow = main_flow < float(params.get("risk_main_outflow_heavy_wan", -20000))

    if exploded and board >= int(params.get("risk_high_board_threshold", 6)):
        p = float(params.get("risk_high_board_penalty", 0.75))
    elif exploded and heavy_outflow and int(params.get("risk_mid_board_min", 4)) <= board <= int(params.get("risk_mid_board_max", 5)):
        p = float(params.get("risk_mid_board_penalty", 0.85))
    elif exploded and heavy_outflow and board < int(params.get("risk_mid_board_min", 4)):
        p = float(params.get("risk_low_board_penalty", 0.92))
    elif exploded or heavy_outflow:
        p = float(params.get("risk_only_one_factor_penalty", 0.95))
    else:
        p = 1.0
    return round(p, 4), {"board_count": board, "exploded": exploded, "main_flow_wan": main_flow, "heavy_outflow": heavy_outflow}


def classify_setup(longtou: str, auction: float, theme: float, hotness: Optional[float], params: Dict[str, Any], risk_penalty: float) -> Tuple[str, str]:
    if risk_penalty <= 0:
        return "none", "none"
    if longtou in {"confirmed_longtou", "board_leader", "confirmed"} and auction >= float(params.get("setup_lead_auction_min", 70)):
        return "T0-LEAD", "high"
    if longtou == "none" and auction >= float(params.get("setup_new_auction_min", 70)):
        high = (hotness is not None and hotness >= float(params.get("setup_new_high_confidence_hotness", 50))) or auction >= float(params.get("setup_new_high_confidence_auction", 85))
        return "T0-NEW", "high" if high else "low"
    if longtou != "confirmed_longtou" and theme >= float(params.get("setup_rotate_theme_min", 65)) and auction >= float(params.get("setup_rotate_auction_min", 50)):
        return "T0-ROTATE", "high" if theme >= 80 else "low"
    return "T0-GENERAL", "low"


def regime_multiplier(regime: str, setup: str, config: Dict[str, Any]) -> float:
    table = config.get("regime_multiplier") or {}
    return float((table.get(regime) or {}).get(setup, 1.0))


def compat_setup_v71(setup: str, auction: float, confidence: str) -> str:
    if setup == "T0-LEAD":
        return "A_ice" if auction >= 80 else "A"
    if setup == "T0-NEW":
        return "D" if confidence == "high" else "E"
    if setup == "T0-ROTATE":
        return "B" if confidence == "high" else "C1"
    if setup == "T0-GENERAL":
        return "C2"
    return "none"


def classify_candidates_v72(
    candidates: List[Dict[str, Any]],
    labels: Dict[str, Any],
    auction_strengths: Dict[str, Dict[str, Any]],
    theme_strengths: Dict[str, Dict[str, Any]],
    hotness_scores: Dict[str, Optional[float]],
    config: Dict[str, Any],
    max_candidates: Optional[int] = None,
) -> List[Dict[str, Any]]:
    params = config.get("params") or {}
    regime_obj = labels.get("regime") or {}
    regime = str(regime_obj.get("regime") or regime_obj.get("label") or "normal")
    decisions: List[Dict[str, Any]] = []

    for c in candidates or []:
        code = _norm_code(c.get("code"))
        if not code:
            continue
        zt_raw = (labels.get("zt") or {}).get(code) or {}
        longtou_raw = (labels.get("longtou") or {}).get(code) or {}
        cash_raw = (labels.get("cashflow_continuity") or {}).get(code) or {}
        stock_t1 = (labels.get("stock_t1") or {}).get(code) or {}
        tech_raw = (labels.get("tech_profile") or {}).get(code) or {}

        auction = float((auction_strengths.get(code) or {}).get("auction_strength") or 0.0)
        theme_info = theme_strengths.get(code) or {}
        theme = float(theme_info.get("theme_strength_t0") or 0.0)
        hot = hotness_scores.get(code)

        snapshot = {
            "longtou_status": str(longtou_raw.get("longtou_status") or longtou_raw.get("label") or "none"),
            "cashflow_continuity": str(cash_raw.get("cashflow_continuity") or cash_raw.get("label") or "none"),
            "zt_quality": str(zt_raw.get("zt_quality") or zt_raw.get("quality_label") or zt_raw.get("quality") or "average"),
            "tech_profile": str(tech_raw.get("tech_profile") or tech_raw.get("label") or "unknown"),
            "zt_raw": zt_raw,
            "stock_t1": stock_t1,
            "cashflow_raw": cash_raw,
            "tech_raw": tech_raw,
            "daily_rows": labels.get("dailyline", {}).get(code) if isinstance(labels.get("dailyline"), dict) else None,
        }
        risk, risk_detail = compute_risk_penalty(c, snapshot, params)
        today_signal, weights = compute_today_signal_raw(auction, theme, hot, params)
        t1_mult, adjustments = compute_t1_multiplier(snapshot, auction, params)
        setup, confidence = classify_setup(snapshot["longtou_status"], auction, theme, hot, params, risk)
        reg_mult = regime_multiplier(regime, setup, config) if setup != "none" else 0.0
        final = round(today_signal * t1_mult * reg_mult * risk, 2)
        compat = compat_setup_v71(setup, auction, confidence)

        decisions.append({
            "code": code,
            "name": c.get("name"),
            "setup_v72": setup,
            "setup_v71_compat": compat,
            "confidence": confidence,
            "final_score": final,
            "today_signal_raw": today_signal,
            "t1_multiplier": t1_mult,
            "regime_multiplier": reg_mult,
            "risk_penalty": risk,
            "auction_strength": auction,
            "theme_strength_t0": theme,
            "hotness_score": hot,
            "risk_flag": risk < 1.0,
            "score_weights": weights,
            "t1_adjustments": adjustments,
            "risk_detail": risk_detail,
            "regime": regime,
            "theme_detail": theme_info,
            "auction_detail": auction_strengths.get(code) or {},
            "label_snapshot": snapshot,
        })

    decisions.sort(key=lambda x: x.get("final_score") or 0, reverse=True)
    if max_candidates is not None:
        return decisions[:max_candidates]
    return decisions
