"""
duanxianxia_v7_2_auction_strength.py — V8-style T0 auction evidence model.

This keeps the v7.2 public function name, but implements the earlier V8 design
proposal:
- Do not use `max(source ranks)` as the whole base.
- Convert ranks with exponential decay, then fuse independent source evidence
  with noisy-or.
- Split the auction signal into alpha / liquidity / tradability / risk.
- Classify auction behavior first, then let setup/output rank within pools.

User constraints from 2026-05 discussion are preserved:
- `auction.jjyd.qiangchou` group `grab` = 9:20-9:25 sustained抢筹, primary.
- `auction.jjyd.qiangchou` group `qiangchou` = 9:24:59 last-second抢筹,
  useful as confirmation but discounted.
- T0 主力流入 / 今日封板率 / T0 plate 涨停数量 are not used here.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _to_int(v: Any) -> Optional[int]:
    try:
        if v in (None, "", "-", "none"):
            return None
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-", "none"):
            return None
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return None


def _parse_money_to_wan(v: Any) -> Optional[float]:
    if v in (None, "", "-", "none"):
        return None
    s = str(v).replace(",", "").strip()
    try:
        if "亿" in s:
            return float(s.replace("亿", "")) * 10000.0
        if "万" in s:
            return float(s.replace("万", ""))
        return float(s)
    except Exception:
        return None


def _parse_yi(v: Any) -> Optional[float]:
    wan = _parse_money_to_wan(v)
    return None if wan is None else wan / 10000.0


def _first_money_wan(row: Optional[Dict[str, Any]], keys: List[str]) -> Optional[float]:
    if not row:
        return None
    for k in keys:
        if k in row:
            v = _parse_money_to_wan(row.get(k))
            if v is not None:
                return v
    return None


def _first_pct(row: Optional[Dict[str, Any]], keys: List[str]) -> Optional[float]:
    if not row:
        return None
    for k in keys:
        if k in row:
            v = _to_float(row.get(k))
            if v is not None:
                return v
    return None


def _money_yi_from_keys(row: Optional[Dict[str, Any]], keys: List[str]) -> Optional[float]:
    if not row:
        return None
    for k in keys:
        if k in row:
            v = _parse_yi(row.get(k))
            if v is not None:
                return v
    return None


def _index_by_code_min_rank(rows: List[Dict[str, Any]], group: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if group is not None and str(row.get("group") or "").strip().lower() != group:
            continue
        code = _norm_code(row.get("code") or row.get("代码"))
        if not code:
            continue
        rank = _to_int(row.get("rank") or row.get("排名"))
        old = out.get(code)
        old_rank = _to_int((old or {}).get("rank") or (old or {}).get("排名")) if old else None
        if old is None or (rank is not None and (old_rank is None or rank < old_rank)):
            out[code] = row
    return out


def _linear_rank_score(rank: Optional[int], top_n: int) -> float:
    if rank is None or rank <= 0 or rank > top_n:
        return 0.0
    return (top_n - rank + 1) / top_n * 100.0


def _exp_rank_score(rank: Optional[int], top_n: int, tau: float) -> float:
    if rank is None or rank <= 0 or rank > top_n:
        return 0.0
    floor = math.exp(-(top_n - 1) / max(tau, 1e-6))
    raw = math.exp(-(rank - 1) / max(tau, 1e-6))
    return max(0.0, min(100.0, (raw - floor) / (1.0 - floor) * 100.0))


def _noisy_or(weighted_scores_0_100: Dict[str, Tuple[float, float]]) -> float:
    remain = 1.0
    for score, weight in weighted_scores_0_100.values():
        s = max(0.0, min(1.0, score / 100.0))
        w = max(0.0, min(1.0, weight))
        remain *= (1.0 - w * s)
    return max(0.0, min(100.0, (1.0 - remain) * 100.0))


def _classify_fengdan(row: Optional[Dict[str, Any]], latest_pct: Optional[float], params: Dict[str, Any]) -> Dict[str, Any]:
    if row is None:
        return {"status": "none", "consume_type": None, "amount_915_yi": None, "amount_920_yi": None, "amount_925_yi": None, "ratio_920_915": None, "ratio_925_920": None, "behavior_bonus": 0.0, "penalty_multiplier": 1.0, "reason": "missing_row"}
    a915 = _money_yi_from_keys(row, ["amount_915", "9:15", "915", "f15"]) or 0.0
    a920 = _money_yi_from_keys(row, ["amount_920", "9:20", "920", "f20"]) or 0.0
    a925 = _money_yi_from_keys(row, ["amount_925", "9:25", "925", "f25"]) or 0.0
    r20 = (a920 / a915) if a915 > 0 else None
    r25 = (a925 / a920) if a920 > 0 else None

    def resp(status: str, reason: str, consume_type: Optional[str] = None, bonus: float = 0.0, mult: float = 1.0) -> Dict[str, Any]:
        return {"status": status, "consume_type": consume_type, "amount_915_yi": a915 if a915 > 0 else None, "amount_920_yi": a920 if a920 > 0 else None, "amount_925_yi": a925 if a925 > 0 else 0.0, "ratio_920_915": r20, "ratio_925_920": r25, "behavior_bonus": bonus, "penalty_multiplier": mult, "reason": reason}

    fake_drop = float(params.get("fengdan_fake_drop_ratio", 0.30))
    fake_f15_min_wan = float(params.get("fengdan_fake_f15_min_wan", 1000))
    consume_ratio = float(params.get("fengdan_consume_ratio", 0.80))
    lock_ratio = float(params.get("fengdan_lock_ratio", 0.90))
    lock_pct = float(params.get("fengdan_lock_latest_min_pct", 9.5))

    if a915 * 10000 >= fake_f15_min_wan and (a920 <= 0 or (r20 is not None and r20 < fake_drop)):
        return resp("fake", "915_large_but_920_collapsed", mult=float(params.get("fengdan_fake_penalty_multiplier", 0.70)))
    if a920 > 0 and r25 is not None and r25 < consume_ratio:
        ctype = "zero" if a925 <= 0 else "partial"
        bonus = 0.0 if ctype == "zero" else float(params.get("fengdan_consume_weak_bonus", 2))
        return resp("consume", f"920_to_925_consumed ratio={round(r25, 4)}", consume_type=ctype, bonus=bonus)
    if a920 > 0 and a925 > 0 and r25 is not None and r25 >= lock_ratio and latest_pct is not None and latest_pct >= lock_pct:
        return resp("lock", f"920_to_925_locked ratio={round(r25, 4)}", bonus=float(params.get("fengdan_lock_bonus", 15)))
    if a925 > 0:
        return resp("stable", "925_valid")
    return resp("none", "no_positive_amount")


def _auction_amount_multiplier(amount_wan: Optional[float], params: Dict[str, Any]) -> Tuple[float, bool]:
    if amount_wan is None:
        return 1.0, True
    if amount_wan < float(params.get("min_auction_amount_wan", 500)):
        return float(params.get("auction_amount_low_multiplier", 0.5)), False
    if amount_wan < float(params.get("full_auction_amount_wan", 1000)):
        return float(params.get("auction_amount_mid_multiplier", 0.8)), False
    return 1.0, False


def _price_intent_score(latest_pct: Optional[float], params: Dict[str, Any]) -> float:
    if latest_pct is None:
        return float(params.get("price_intent_missing_score", 50))
    if latest_pct < -3:
        return float(params.get("price_intent_low_open_score", 20))
    if latest_pct < 0:
        return float(params.get("price_intent_negative_score", 35))
    if latest_pct < 2:
        return float(params.get("price_intent_turning_score", 55))
    if latest_pct <= 7:
        return float(params.get("price_intent_attack_score", 100))
    if latest_pct < 9.5:
        return float(params.get("price_intent_high_cost_score", 75))
    return float(params.get("price_intent_board_watch_score", 55))


def _turnover_health_score(turnover_pct: Optional[float], params: Dict[str, Any]) -> Tuple[float, Optional[str]]:
    if turnover_pct is None:
        return 50.0, None
    if turnover_pct < float(params.get("turnover_low_pct", 0.3)):
        return 35.0, "low_turnover"
    if turnover_pct <= float(params.get("turnover_healthy_high_pct", 1.5)):
        return 100.0, "healthy_turnover"
    if turnover_pct <= float(params.get("turnover_divergence_high_pct", 3.0)):
        return 75.0, "divergence_turnover"
    return 45.0, "overheated_turnover"


def _amount_scores(amount_wan: Optional[float], net_row: Optional[Dict[str, Any]], params: Dict[str, Any]) -> Tuple[float, Optional[float], Optional[float]]:
    amount_abs = 0.0 if amount_wan is None else min(100.0, amount_wan / float(params.get("amount_abs_full_wan", 3000)) * 100.0)
    net = _first_money_wan(net_row, ["main_net_inflow_wan", "主力净买", "主力净流入", "net_amount_wan"])
    mcap_yi = _to_float((net_row or {}).get("market_cap_yi") or (net_row or {}).get("流通值") or (net_row or {}).get("流通市值"))
    net_pressure = None
    amount_pressure = None
    pressure_score = 0.0
    if mcap_yi and mcap_yi > 0:
        if net is not None:
            net_pressure = net / (mcap_yi * 10000.0)
            pressure_score += min(100.0, max(0.0, net_pressure / float(params.get("net_pressure_full_ratio", 0.002)) * 100.0)) * 0.6
        if amount_wan is not None:
            amount_pressure = amount_wan / (mcap_yi * 10000.0)
            pressure_score += min(100.0, max(0.0, amount_pressure / float(params.get("amount_pressure_full_ratio", 0.001)) * 100.0)) * 0.4
    else:
        pressure_score = amount_abs
    money_score = 0.5 * amount_abs + 0.5 * pressure_score
    return max(0.0, min(100.0, money_score)), net_pressure, amount_pressure


def _orderbook_quality_score(status: str, consume_type: Optional[str], ratio_925_920: Optional[float], params: Dict[str, Any]) -> float:
    if status == "fake":
        return float(params.get("orderbook_fake_score", 0))
    if status == "consume" and consume_type == "zero":
        return float(params.get("orderbook_consume_zero_score", 15))
    if status == "consume":
        return float(params.get("orderbook_consume_partial_score", 55))
    if status == "lock":
        return float(params.get("orderbook_lock_score", 85))
    if status == "stable":
        base = float(params.get("orderbook_stable_score", 70))
        if ratio_925_920 is not None:
            base = max(base, min(90.0, 50.0 + 40.0 * ratio_925_920))
        return base
    return float(params.get("orderbook_none_score", 45))


def _resonance_score(scores: Dict[str, float], ranks: Dict[str, Optional[int]], top_n: int) -> Tuple[float, int, int, List[str]]:
    families = [k for k, v in scores.items() if v > 0]
    top_families = [k for k, r in ranks.items() if r is not None and 0 < r <= max(10, top_n // 3)]
    count_score = min(100.0, len(families) / 4.0 * 70.0)
    top_score = min(30.0, len(top_families) * 10.0)
    return min(100.0, count_score + top_score), len(families), len(top_families), families


def _risk_and_tradability(
    latest_pct: Optional[float],
    amount_wan: Optional[float],
    turnover_state: Optional[str],
    f_status: str,
    consume_type: Optional[str],
    f25_yi: Optional[float],
    params: Dict[str, Any],
) -> Tuple[float, float, List[str], str, str]:
    risk = 1.0
    trad = 1.0
    flags: List[str] = []
    entry_tag, entry_reason = "normal", "normal"

    if f_status == "fake":
        risk *= float(params.get("risk_fake_multiplier", 0.65))
        flags.append("fake_fengdan")
        entry_tag, entry_reason = "avoid", "fake_fengdan"
    elif f_status == "consume" and consume_type == "zero":
        risk *= float(params.get("risk_consume_zero_multiplier", 0.85))
        flags.append("consume_zero")
    elif f_status == "lock" and latest_pct is not None and latest_pct >= float(params.get("entry_board_watch_pct", 9.5)):
        trad *= float(params.get("tradability_board_lock_multiplier", 0.75))
        flags.append("board_lock_hard_to_buy")
        if (f25_yi or 0) >= float(params.get("entry_lock_large_f25_yi", 1.0)):
            entry_tag, entry_reason = "board_watch", "lock_near_limit_large_f25"

    if latest_pct is not None and latest_pct >= float(params.get("entry_high_open_pct", 8.5)) and entry_tag == "normal":
        trad *= float(params.get("tradability_high_open_multiplier", 0.90))
        entry_tag, entry_reason = "high_open_confirm", "near_limit_high_open"
    if latest_pct is not None and latest_pct < 0:
        flags.append("negative_open")
    if amount_wan is not None and amount_wan < float(params.get("min_auction_amount_wan", 500)):
        trad *= float(params.get("tradability_low_amount_multiplier", 0.70))
        flags.append("low_auction_amount")
        if entry_tag == "normal":
            entry_tag, entry_reason = "low_liquidity_confirm", "auction_amount_below_min"
    if turnover_state == "overheated_turnover":
        risk *= float(params.get("risk_overheated_turnover_multiplier", 0.90))
        flags.append("overheated_turnover")
    return max(0.0, min(1.0, risk)), max(0.0, min(1.0, trad)), flags, entry_tag, entry_reason


def _auction_setup_type(
    latest_pct: Optional[float],
    f_status: str,
    consume_type: Optional[str],
    qg_rank: Optional[int],
    ql_rank: Optional[int],
    n_rank: Optional[int],
    source_evidence: float,
    money_score: float,
    risk_mult: float,
    entry_tag: str,
    params: Dict[str, Any],
) -> str:
    if entry_tag == "avoid" or risk_mult <= float(params.get("fake_strength_risk_max", 0.70)):
        return "FAKE_STRENGTH"
    if latest_pct is not None and latest_pct >= float(params.get("board_lock_pct", 9.5)) and f_status == "lock":
        return "BOARD_LOCK_WATCH"
    if latest_pct is not None and latest_pct < 0:
        if (n_rank is not None and n_rank <= int(params.get("reversal_net_rank_max", 10))) or (qg_rank is not None and qg_rank <= int(params.get("reversal_qiangchou_rank_max", 15))):
            return "LOW_OPEN_REVERSAL"
        return "LOW_OPEN_WEAK"
    if f_status == "consume" and consume_type == "partial" and (qg_rank is not None or n_rank is not None) and money_score >= float(params.get("healthy_divergence_money_min", 45)):
        return "HEALTHY_DIVERGENCE"
    if latest_pct is not None and 2 <= latest_pct <= 7 and source_evidence >= float(params.get("high_open_attack_evidence_min", 45)):
        return "HIGH_OPEN_ATTACK"
    if qg_rank is not None and ql_rank is not None:
        return "SUSTAINED_PLUS_LAST_SECOND"
    return "GENERAL_WATCH"


def compute_auction_strengths(candidate_codes: List[str], vratio_rows: List[Dict[str, Any]], qiangchou_rows: List[Dict[str, Any]], netamount_rows: List[Dict[str, Any]], fengdan_rows: List[Dict[str, Any]], params: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    p = params or {}
    top_n = int(p.get("auction_top_rank_n", 30))
    tau = float(p.get("auction_rank_decay_tau", 8.0))

    v_idx = _index_by_code_min_rank(vratio_rows)
    q_grab_idx = _index_by_code_min_rank(qiangchou_rows, "grab")
    q_last_idx = _index_by_code_min_rank(qiangchou_rows, "qiangchou")
    n_idx = _index_by_code_min_rank(netamount_rows)
    f_idx = _index_by_code_min_rank([r for r in (fengdan_rows or []) if str(r.get("section_kind") or "").strip() in {"", "live"}])

    amount_keys = ["auction_turnover_wan", "auction_turnover_wan_text", "竞额", "竞价成交额", "竞价金额", "auction_amount_wan", "amount", "成交额"]
    turnover_keys = ["turnover_rate_pct", "竞价换手", "竞价换手率", "turnover_rate", "换手率"]
    pct_keys = ["latest_change_pct", "auction_change_pct", "auction_change_pct_text", "竞价涨幅", "涨幅"]

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = _norm_code(raw)
        if not code or code in out:
            continue
        v_row, qg_row, ql_row, n_row, f_row = v_idx.get(code), q_grab_idx.get(code), q_last_idx.get(code), n_idx.get(code), f_idx.get(code)
        v_rank = _to_int((v_row or {}).get("rank") or (v_row or {}).get("排名"))
        qg_rank = _to_int((qg_row or {}).get("rank") or (qg_row or {}).get("排名"))
        ql_rank = _to_int((ql_row or {}).get("rank") or (ql_row or {}).get("排名"))
        n_rank = _to_int((n_row or {}).get("rank") or (n_row or {}).get("排名"))
        f_rank = _to_int((f_row or {}).get("rank") or (f_row or {}).get("排名"))

        latest_pct = _first_pct(n_row, pct_keys) or _first_pct(v_row, pct_keys) or _first_pct(qg_row, pct_keys) or _first_pct(ql_row, pct_keys) or _first_pct(f_row, pct_keys)
        amount_wan = _first_money_wan(v_row, amount_keys) or _first_money_wan(qg_row, amount_keys) or _first_money_wan(ql_row, amount_keys) or _first_money_wan(n_row, amount_keys)
        turnover_pct = _first_pct(v_row, turnover_keys) or _first_pct(qg_row, turnover_keys) or _first_pct(ql_row, turnover_keys) or _first_pct(n_row, turnover_keys)

        f_behavior = _classify_fengdan(f_row, latest_pct, p)
        f_status = str(f_behavior.get("status") or "none")
        consume_type = f_behavior.get("consume_type")

        q_last_score = _exp_rank_score(ql_rank, top_n, tau) * float(p.get("qiangchou_last_second_multiplier", 0.85))
        source_scores = {
            "vratio": _exp_rank_score(v_rank, top_n, tau),
            "qiangchou_920_925": _exp_rank_score(qg_rank, top_n, tau),
            "qiangchou_last_second": q_last_score,
            "net_amount": _exp_rank_score(n_rank, top_n, tau),
            "fengdan": _exp_rank_score(f_rank, top_n, tau) if f_status not in {"fake", "none"} else 0.0,
        }
        weights = {
            "vratio": float(p.get("source_weight_vratio", 0.20)),
            "qiangchou_920_925": float(p.get("source_weight_qiangchou_920_925", 0.35)),
            "qiangchou_last_second": float(p.get("source_weight_qiangchou_last_second", 0.18)),
            "net_amount": float(p.get("source_weight_net_amount", 0.25)),
            "fengdan": float(p.get("source_weight_fengdan", 0.20)),
        }
        source_evidence = _noisy_or({k: (v, weights[k]) for k, v in source_scores.items()})
        legacy_base = max({
            "vratio": _linear_rank_score(v_rank, top_n),
            "qiangchou_920_925": _linear_rank_score(qg_rank, top_n),
            "qiangchou_last_second": _linear_rank_score(ql_rank, top_n) * float(p.get("qiangchou_last_second_multiplier", 0.85)),
            "net_amount": _linear_rank_score(n_rank, top_n),
            "fengdan": _linear_rank_score(f_rank, top_n) if f_status not in {"fake", "none"} else 0.0,
        }.values())

        price_score = _price_intent_score(latest_pct, p)
        turnover_score, turnover_state = _turnover_health_score(turnover_pct, p)
        money_score, net_pressure, amount_pressure = _amount_scores(amount_wan, n_row, p)
        orderbook_score = _orderbook_quality_score(f_status, consume_type, f_behavior.get("ratio_925_920"), p)
        resonance_score, family_count, top_family_count, source_families = _resonance_score(source_scores, {"vratio": v_rank, "qiangchou_920_925": qg_rank, "qiangchou_last_second": ql_rank, "net_amount": n_rank, "fengdan": f_rank}, top_n)
        amount_mult, amount_missing = _auction_amount_multiplier(amount_wan, p)
        risk_mult, trad_mult, risk_flags, entry_tag, entry_reason = _risk_and_tradability(latest_pct, amount_wan, turnover_state, f_status, consume_type, f_behavior.get("amount_925_yi"), p)

        alpha = (
            float(p.get("auction_alpha_source_weight", 0.35)) * source_evidence
            + float(p.get("auction_alpha_price_weight", 0.20)) * price_score
            + float(p.get("auction_alpha_money_weight", 0.20)) * money_score
            + float(p.get("auction_alpha_orderbook_weight", 0.15)) * orderbook_score
            + float(p.get("auction_alpha_resonance_weight", 0.10)) * resonance_score
        )
        liquidity_score = 0.6 * min(100.0, (amount_wan or 0.0) / float(p.get("amount_abs_full_wan", 3000)) * 100.0) + 0.4 * turnover_score
        liquidity_mult = max(float(p.get("liquidity_multiplier_min", 0.50)), min(1.15, liquidity_score / 100.0 + 0.15))
        total = max(0.0, min(100.0, alpha * liquidity_mult * risk_mult * trad_mult * amount_mult))

        auction_type = _auction_setup_type(latest_pct, f_status, consume_type, qg_rank, ql_rank, n_rank, source_evidence, money_score, risk_mult, entry_tag, p)
        if auction_type == "LOW_OPEN_REVERSAL":
            # Keep low-open reversal visible but prevent it from ranking like a
            # normal high-open attack unless the downstream setup explicitly
            # separates it.
            total = min(total, float(p.get("low_open_reversal_strength_cap", 65)))
        elif latest_pct is not None and latest_pct < 0:
            total = min(total, float(p.get("negative_non_reversal_cap", 45)))

        out[code] = {
            "auction_strength": round(total, 2),
            "auction_strength_v8": round(total, 2),
            "legacy_max_base": round(legacy_base, 2),
            "auction_alpha_score": round(alpha, 2),
            "source_evidence_score": round(source_evidence, 2),
            "price_intent_score": round(price_score, 2),
            "money_intent_score": round(money_score, 2),
            "orderbook_quality_score": round(orderbook_score, 2),
            "resonance_score": round(resonance_score, 2),
            "liquidity_score": round(liquidity_score, 2),
            "risk_multiplier": round(risk_mult, 4),
            "tradability_multiplier": round(trad_mult, 4),
            "liquidity_multiplier": round(liquidity_mult, 4),
            "auction_amount_multiplier": round(amount_mult, 4),
            "auction_amount_missing": amount_missing,
            "auction_setup_type": auction_type,
            "source_family_count": family_count,
            "top_rank_family_count": top_family_count,
            "source_families": source_families,
            "risk_flags": risk_flags,
            "auction_amount_wan": amount_wan,
            "auction_turnover_pct": turnover_pct,
            "turnover_state": turnover_state,
            "latest_change_pct": latest_pct,
            "net_pressure": net_pressure,
            "amount_pressure": amount_pressure,
            "vratio_rank": v_rank,
            "qiangchou_rank": qg_rank if qg_rank is not None else ql_rank,
            "qiangchou_grab_rank": qg_rank,
            "qiangchou_920_925_rank": qg_rank,
            "qiangchou_last_second_rank": ql_rank,
            "qiangchou_primary_signal": "9:20-9:25" if qg_rank is not None else ("last_second" if ql_rank is not None else None),
            "net_amount_rank": n_rank,
            "fengdan_rank": f_rank,
            "fengdan_status": f_status,
            "fengdan_consume_type": consume_type,
            "fengdan_behavior_reason": f_behavior.get("reason"),
            "entry_tag": entry_tag,
            "entry_reason": entry_reason,
            "fengdan_amount_915_yi": f_behavior.get("amount_915_yi"),
            "fengdan_amount_920_yi": f_behavior.get("amount_920_yi"),
            "fengdan_amount_925_yi": f_behavior.get("amount_925_yi"),
            "fengdan_ratio_920_915": f_behavior.get("ratio_920_915"),
            "fengdan_ratio_925_920": f_behavior.get("ratio_925_920"),
            "hits_count": family_count,
        }
    return out


def _self_test() -> None:
    q = [
        {"group": "grab", "rank": 2, "code": "002297", "auction_turnover_wan": "11203", "latest_change_pct": "3.35", "turnover_rate_pct": 1.12},
        {"group": "qiangchou", "rank": 1, "code": "002297", "auction_turnover_wan": "11203", "latest_change_pct": "3.35", "turnover_rate_pct": 1.12},
        {"group": "qiangchou", "rank": 1, "code": "000001", "auction_turnover_wan": "200", "latest_change_pct": "6.0", "turnover_rate_pct": 0.1},
        {"group": "grab", "rank": 3, "code": "000002", "auction_turnover_wan": "3000", "latest_change_pct": "-1.0", "turnover_rate_pct": 1.0},
    ]
    n = [{"rank": 5, "code": "002297", "main_net_inflow_wan": 5000, "market_cap_yi": 100, "auction_turnover_wan": 11203, "latest_change_pct": 3.35}, {"rank": 1, "code": "000002", "main_net_inflow_wan": 4000, "market_cap_yi": 80, "latest_change_pct": -1.0}]
    f = [{"rank": 1, "code": "002297", "amount_915": "1亿", "amount_920": "1.2亿", "amount_925": "1.3亿", "latest_change_pct": "9.8%", "section_kind": "live"}]
    out = compute_auction_strengths(["002297", "000001", "000002"], [], q, n, f, {})
    assert out["002297"]["qiangchou_920_925_rank"] == 2, out["002297"]
    assert out["002297"]["qiangchou_last_second_rank"] == 1, out["002297"]
    assert out["002297"]["source_evidence_score"] > 0, out["002297"]
    assert out["002297"]["auction_alpha_score"] > 0, out["002297"]
    assert out["000001"]["auction_amount_multiplier"] == 0.5, out["000001"]
    assert out["000002"]["auction_setup_type"] == "LOW_OPEN_REVERSAL", out["000002"]
    print("auction_strength v8-style _self_test passed")


if __name__ == "__main__":
    _self_test()
