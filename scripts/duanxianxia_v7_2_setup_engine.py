"""v7.2 setup engine — T0-driven premarket scoring with V8 auction types."""
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
        return float(str(v).replace("%", "").replace(",", "").strip())
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


def _main_flow_wan(stock_t1: Dict[str, Any], cash: Dict[str, Any]) -> float:
    for obj in (stock_t1, cash):
        for key in ("main_inflow_wan", "today_wan", "main_net_inflow_wan", "main_net_wan", "主力净流入万"):
            if key in obj:
                return _f(obj.get(key), 0.0)
    return 0.0


def _float_market_value_wan(stock_t1: Dict[str, Any], cash: Dict[str, Any]) -> Optional[float]:
    for obj in (stock_t1, cash):
        for key in ("float_market_value_wan", "流通市值万", "流通值万"):
            if key in obj:
                v = _f(obj.get(key), 0.0)
                return v if v > 0 else None
        for key in ("float_market_value_yi", "流通值", "流通市值", "流通市值(亿)"):
            if key in obj:
                s = str(obj.get(key) or "").replace(",", "").strip()
                if not s or s == "-":
                    continue
                try:
                    if "亿" in s:
                        return float(s.replace("亿", "")) * 10000.0
                    if "万" in s:
                        return float(s.replace("万", ""))
                    return float(s) * 10000.0
                except Exception:
                    continue
    return None


def _heavy_outflow(main_flow_wan: float, stock_t1: Dict[str, Any], cash: Dict[str, Any], params: Dict[str, Any]) -> Tuple[bool, Optional[float], Optional[float], str]:
    float_mv_wan = _float_market_value_wan(stock_t1, cash)
    if float_mv_wan and float_mv_wan > 0:
        outflow_ratio = main_flow_wan / float_mv_wan
        threshold = float(params.get("risk_outflow_float_mv_ratio", -0.005))
        return outflow_ratio <= threshold, outflow_ratio, float_mv_wan / 10000.0, "float_mv_ratio"
    abs_threshold = float(params.get("risk_main_outflow_heavy_wan", -20000))
    return main_flow_wan < abs_threshold, None, None, "absolute_fallback"


def _churn_type(tech: Dict[str, Any], params: Dict[str, Any]) -> str:
    existing = str(tech.get("churn_type") or "").strip()
    if existing in {"panic_churn", "dull_churn", "none"}:
        return existing
    profile = str(tech.get("tech_profile") or tech.get("label") or "")
    if profile in {"unknown", ""} or profile != "churn_high_volume":
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
    wa = float(params.get("weight_auction", 0.65))
    wt = float(params.get("weight_theme", 0.25))
    wh = float(params.get("weight_hotness", 0.10))
    if hotness is None:
        s = wa + wt
        wa2, wt2, wh2 = wa / s, wt / s, 0.0
        score = wa2 * auction + wt2 * theme
    else:
        wa2, wt2, wh2 = wa, wt, wh
        score = wa * auction + wt * theme + wh * float(hotness)
    return round(score, 2), {"auction": round(wa2, 4), "theme": round(wt2, 4), "hotness": round(wh2, 4)}


def compute_t1_multiplier(snapshot: Dict[str, Any], auction_strength: float, params: Dict[str, Any]) -> Tuple[float, List[Dict[str, Any]]]:
    use_review = bool(params.get("use_t1_review_context", False))
    adj: List[Dict[str, Any]] = []
    longtou = str(snapshot.get("longtou_status") or "none") if use_review else "none"
    cash_label = str(snapshot.get("cashflow_continuity") or "none")
    tech_profile = str(snapshot.get("tech_profile") or "unknown")
    stock_t1 = snapshot.get("stock_t1") or {}
    cash_obj = snapshot.get("cashflow_raw") or {}
    main_flow = _main_flow_wan(stock_t1 if isinstance(stock_t1, dict) else {}, cash_obj if isinstance(cash_obj, dict) else {})
    iceberg = auction_strength >= float(params.get("iceberg_auction_threshold", 80)) and longtou == "none"
    if iceberg:
        adj.append({"key": "iceberg", "value": float(params.get("iceberg_bonus", 0.00))})
    if cash_label == "accumulating_strong":
        adj.append({"key": "cashflow_strong", "value": float(params.get("cashflow_strong_bonus", 0.03))})
    if cash_label == "distributing" and main_flow < float(params.get("cashflow_distributing_outflow_threshold_wan", -300)):
        adj.append({"key": "cashflow_distributing", "value": float(params.get("cashflow_distributing_penalty", -0.05))})
    if _is_breakdown_profile(tech_profile) and not iceberg:
        adj.append({"key": "tech_breakdown", "value": float(params.get("tech_breakdown_penalty", -0.04))})
    churn = _churn_type(snapshot.get("tech_raw") or {}, params)
    if churn == "dull_churn":
        adj.append({"key": "dull_churn", "value": float(params.get("churn_dull_iceberg_penalty", -0.03)) if iceberg else float(params.get("churn_dull_penalty", -0.06))})
    elif churn == "panic_churn":
        adj.append({"key": "panic_churn", "value": float(params.get("churn_panic_penalty", -0.05))})
    if not use_review:
        adj.append({"key": "t1_review_context_disabled", "value": 0.0})
    raw = 1.0 + sum(float(x["value"]) for x in adj)
    return round(_clip(raw, float(params.get("t1_multiplier_min", 0.90)), float(params.get("t1_multiplier_max", 1.10))), 4), adj


def compute_risk_penalty(candidate: Dict[str, Any], snapshot: Dict[str, Any], params: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    if _is_st_or_delist(candidate, snapshot.get("daily_rows")):
        return 0.0, {"hard_kill": "ST_or_delist"}
    stock_t1 = snapshot.get("stock_t1") or {}
    cash_obj = snapshot.get("cashflow_raw") or {}
    main_flow = _main_flow_wan(stock_t1 if isinstance(stock_t1, dict) else {}, cash_obj if isinstance(cash_obj, dict) else {})
    heavy_outflow, outflow_ratio, float_mv_yi, outflow_method = _heavy_outflow(main_flow, stock_t1 if isinstance(stock_t1, dict) else {}, cash_obj if isinstance(cash_obj, dict) else {}, params)
    p = float(params.get("risk_only_one_factor_penalty", 0.95)) if heavy_outflow else 1.0
    return round(p, 4), {"main_flow_wan": main_flow, "heavy_outflow": heavy_outflow, "outflow_ratio": outflow_ratio, "float_market_value_yi": float_mv_yi, "outflow_method": outflow_method, "t1_review_context_used": bool(params.get("use_t1_review_context", False))}


def _hot_ge(hotness: Optional[float], threshold: float) -> bool:
    return hotness is not None and hotness >= threshold


def classify_setup(longtou: str, auction: float, theme: float, hotness: Optional[float], params: Dict[str, Any], risk_penalty: float, entry_tag: str = "normal", auction_setup_type: str = "GENERAL_WATCH") -> Tuple[str, str, str]:
    if risk_penalty <= 0:
        return "none", "none", "hard_risk_kill"
    if entry_tag == "avoid" or auction_setup_type == "FAKE_STRENGTH":
        return "none", "none", "fake_or_entry_avoid"
    if auction_setup_type == "LOW_OPEN_REVERSAL":
        return "T0-REVERSAL", "high" if theme >= float(params.get("setup_reversal_high_theme_min", 65)) else "low", "low_open_reversal_separate_pool"
    if auction_setup_type == "HEALTHY_DIVERGENCE":
        return "T0-DIVERGENCE", "high" if theme >= float(params.get("setup_divergence_high_theme_min", 65)) else "low", "healthy_divergence_separate_pool"

    use_review = bool(params.get("use_t1_review_context", False))
    effective_longtou = longtou if use_review else "none"
    lead_entry_tags = set(params.get("setup_lead_entry_tags") or ["board_watch", "high_open_confirm"])
    if effective_longtou in {"confirmed_longtou", "board_leader", "confirmed"} and auction >= float(params.get("setup_legacy_lead_auction_min", 70)):
        return "T0-LEAD", "high", "review_leader_plus_t0_auction"
    if auction >= float(params.get("setup_lead_auction_min", 80)) and (entry_tag in lead_entry_tags or theme >= float(params.get("setup_lead_theme_min", 65)) or _hot_ge(hotness, float(params.get("setup_lead_hotness_min", 55))) or auction_setup_type in {"HIGH_OPEN_ATTACK", "BOARD_LOCK_WATCH", "SUSTAINED_PLUS_LAST_SECOND"}):
        return "T0-LEAD", "high", f"t0_attack_resonance:{auction_setup_type}"
    if theme >= float(params.get("setup_rotate_theme_min", 65)) and auction >= float(params.get("setup_rotate_auction_min", 50)):
        conf = "high" if (theme >= float(params.get("setup_rotate_high_theme_min", 80)) and auction >= float(params.get("setup_rotate_high_auction_min", 60))) else "low"
        return "T0-ROTATE", conf, "theme_strength_plus_auction_confirm"
    if auction >= float(params.get("setup_new_auction_min", 70)):
        high = _hot_ge(hotness, float(params.get("setup_new_high_confidence_hotness", 50))) or auction >= float(params.get("setup_new_high_confidence_auction", 85)) or entry_tag in lead_entry_tags
        return "T0-NEW", "high" if high else "low", "new_t0_auction_candidate"
    if auction >= float(params.get("setup_general_auction_min", 45)) or theme >= float(params.get("setup_general_theme_min", 55)):
        return "T0-GENERAL", "low", "weak_or_single_factor_watch"
    return "none", "none", "below_t0_thresholds"


def regime_multiplier(regime: str, setup: str, config: Dict[str, Any]) -> float:
    return float(((config.get("regime_multiplier") or {}).get(regime) or {}).get(setup, 1.0))


def compat_setup_v71(setup: str, auction: float, confidence: str) -> str:
    if setup == "T0-LEAD":
        return "A_ice" if auction >= 80 else "A"
    if setup == "T0-NEW":
        return "D" if confidence == "high" else "E"
    if setup == "T0-ROTATE":
        return "B" if confidence == "high" else "C1"
    if setup == "T0-GENERAL":
        return "C2"
    if setup == "T0-REVERSAL":
        return "REVERSAL"
    if setup == "T0-DIVERGENCE":
        return "DIVERGENCE"
    return "none"


def _decision_signal_summary(auction_detail: Dict[str, Any], theme_info: Dict[str, Any], hot: Optional[float], setup_reason: str) -> Dict[str, Any]:
    return {
        "setup_reason": setup_reason,
        "auction_setup_type": auction_detail.get("auction_setup_type"),
        "source_evidence_score": auction_detail.get("source_evidence_score"),
        "auction_alpha_score": auction_detail.get("auction_alpha_score"),
        "price_intent_score": auction_detail.get("price_intent_score"),
        "money_intent_score": auction_detail.get("money_intent_score"),
        "orderbook_quality_score": auction_detail.get("orderbook_quality_score"),
        "liquidity_score": auction_detail.get("liquidity_score"),
        "risk_multiplier": auction_detail.get("risk_multiplier"),
        "tradability_multiplier": auction_detail.get("tradability_multiplier"),
        "qiangchou_primary_signal": auction_detail.get("qiangchou_primary_signal"),
        "qiangchou_920_925_rank": auction_detail.get("qiangchou_920_925_rank"),
        "qiangchou_last_second_rank": auction_detail.get("qiangchou_last_second_rank"),
        "auction_amount_wan": auction_detail.get("auction_amount_wan"),
        "net_pressure": auction_detail.get("net_pressure"),
        "fengdan_status": auction_detail.get("fengdan_status"),
        "matched_tags": theme_info.get("matched_tags") or [],
        "matched_plate": theme_info.get("matched_plate"),
        "t0_plate_strength_raw": theme_info.get("t0_plate_strength_raw"),
        "hotness_score": hot,
    }


def classify_candidates_v72(candidates: List[Dict[str, Any]], labels: Dict[str, Any], auction_strengths: Dict[str, Dict[str, Any]], theme_strengths: Dict[str, Dict[str, Any]], hotness_scores: Dict[str, Optional[float]], config: Dict[str, Any], max_candidates: Optional[int] = None) -> List[Dict[str, Any]]:
    params = config.get("params") or {}
    regime_obj = labels.get("regime") or {}
    regime = str(regime_obj.get("regime") or regime_obj.get("label") or "normal")
    use_review = bool(params.get("use_t1_review_context", False))
    decisions: List[Dict[str, Any]] = []

    for c in candidates or []:
        code = _norm_code(c.get("code"))
        if not code:
            continue
        cash_raw = (labels.get("cashflow_continuity") or {}).get(code) or {}
        stock_t1 = (labels.get("stock_t1") or {}).get(code) or {}
        tech_raw = (labels.get("tech_profile") or {}).get(code) or {}
        auction_detail = auction_strengths.get(code) or {}
        auction = float(auction_detail.get("auction_strength") or 0.0)
        theme_info = theme_strengths.get(code) or {}
        theme = float(theme_info.get("theme_strength_t0") or 0.0)
        hot = hotness_scores.get(code)
        entry_tag = auction_detail.get("entry_tag") or "normal"
        entry_reason = auction_detail.get("entry_reason") or "normal"
        auction_setup_type = auction_detail.get("auction_setup_type") or "GENERAL_WATCH"
        snapshot = {"longtou_status": "none" if not use_review else str(((labels.get("longtou") or {}).get(code) or {}).get("longtou_status") or "none"), "cashflow_continuity": str(cash_raw.get("cashflow_continuity") or cash_raw.get("label") or "none"), "tech_profile": str(tech_raw.get("tech_profile") or tech_raw.get("label") or "unknown"), "stock_t1": stock_t1, "cashflow_raw": cash_raw, "tech_raw": tech_raw, "daily_rows": labels.get("dailyline", {}).get(code) if isinstance(labels.get("dailyline"), dict) else None}
        risk, risk_detail = compute_risk_penalty(c, snapshot, params)
        risk = round(risk * float(auction_detail.get("risk_multiplier") or 1.0), 4)
        today_signal, weights = compute_today_signal_raw(auction, theme, hot, params)
        t1_mult, adjustments = compute_t1_multiplier(snapshot, auction, params)
        setup, confidence, setup_reason = classify_setup(snapshot["longtou_status"], auction, theme, hot, params, risk, entry_tag=entry_tag, auction_setup_type=auction_setup_type)
        reg_mult = regime_multiplier(regime, setup, config) if setup != "none" else 0.0
        final = round(today_signal * t1_mult * reg_mult * risk, 2)
        signal_summary = _decision_signal_summary(auction_detail, theme_info, hot, setup_reason)
        decisions.append({"code": code, "name": c.get("name"), "setup_v72": setup, "setup_v71_compat": compat_setup_v71(setup, auction, confidence), "confidence": confidence, "setup_reason": setup_reason, "auction_setup_type": auction_setup_type, "final_score": final, "today_signal_raw": today_signal, "t1_multiplier": t1_mult, "regime_multiplier": reg_mult, "risk_penalty": risk, "auction_strength": auction, "theme_strength_t0": theme, "hotness_score": hot, "risk_flag": risk < 1.0, "entry_tag": entry_tag, "entry_reason": entry_reason, "score_weights": weights, "t1_adjustments": adjustments, "risk_detail": risk_detail, "regime": regime, "theme_detail": theme_info, "auction_detail": auction_detail, "signal_summary": signal_summary, "label_snapshot": snapshot, "t1_review_context_used": use_review})
    decisions.sort(key=lambda x: x.get("final_score") or 0, reverse=True)
    return decisions[:max_candidates] if max_candidates is not None else decisions


def _self_test() -> None:
    params = {"use_t1_review_context": False, "setup_lead_auction_min": 80, "setup_lead_theme_min": 65}
    assert classify_setup("none", 88, 70, None, params, 1.0, auction_setup_type="HIGH_OPEN_ATTACK")[0] == "T0-LEAD"
    assert classify_setup("none", 60, 70, None, params, 1.0, auction_setup_type="LOW_OPEN_REVERSAL")[0] == "T0-REVERSAL"
    assert classify_setup("none", 55, 70, None, params, 1.0, auction_setup_type="HEALTHY_DIVERGENCE")[0] == "T0-DIVERGENCE"
    print("setup_engine v8-style _self_test passed")


if __name__ == "__main__":
    _self_test()
