"""
duanxianxia_v7_2_auction_strength.py — conservative T0 auction scoring.

Key fixes from 2026-05 review:
- `auction.jjyd.qiangchou` has two different groups and they must not be merged:
  * group == "grab"      : 9:20-9:25 抢筹幅度, sustained no-cancel-window signal.
  * group == "qiangchou" : 9:24:59 最后1秒抢筹, terminal impulse/confirmation signal.
- The sustained 9:20-9:25 signal is primary. Last-second signal is useful but
  gets a modest reliability discount unless it also has amount support or
  confirms the sustained signal.
- Keep strength and money separate but both visible: rank/behavior determine
  intent; auction amount / net-pressure / turnover determine quality.
"""

from __future__ import annotations
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
    if not s or s == "-":
        return None
    try:
        if "亿" in s:
            return float(s.replace("亿", "").strip()) * 10000.0
        if "万" in s:
            return float(s.replace("万", "").strip())
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


def _inv_rank(rank: Optional[int], top_n: int) -> float:
    if rank is None or rank <= 0 or rank > top_n:
        return 0.0
    return (top_n - rank + 1) / top_n * 100.0


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


def _money_yi_from_keys(row: Optional[Dict[str, Any]], keys: List[str]) -> Optional[float]:
    if not row:
        return None
    for k in keys:
        if k in row:
            v = _parse_yi(row.get(k))
            if v is not None:
                return v
    return None


def _classify_fengdan(row: Optional[Dict[str, Any]], latest_pct: Optional[float], params: Dict[str, Any]) -> Dict[str, Any]:
    if row is None:
        return {"status": "none", "consume_type": None, "amount_915_yi": None, "amount_920_yi": None, "amount_925_yi": None, "ratio_920_915": None, "ratio_925_920": None, "behavior_bonus": 0.0, "penalty_multiplier": 1.0, "reason": "missing_row"}
    a915 = _money_yi_from_keys(row, ["amount_915", "9:15", "915", "f15"]) or 0.0
    a920 = _money_yi_from_keys(row, ["amount_920", "9:20", "920", "f20"]) or 0.0
    a925 = _money_yi_from_keys(row, ["amount_925", "9:25", "925", "f25"]) or 0.0
    r20 = (a920 / a915) if a915 > 0 else None
    r25 = (a925 / a920) if a920 > 0 else None

    fake_drop = float(params.get("fengdan_fake_drop_ratio", 0.30))
    fake_f15_min_wan = float(params.get("fengdan_fake_f15_min_wan", 1000))
    consume_ratio = float(params.get("fengdan_consume_ratio", 0.80))
    lock_ratio = float(params.get("fengdan_lock_ratio", 0.90))
    lock_pct = float(params.get("fengdan_lock_latest_min_pct", 9.5))

    def resp(status: str, reason: str, consume_type: Optional[str] = None, bonus: float = 0.0, mult: float = 1.0) -> Dict[str, Any]:
        return {"status": status, "consume_type": consume_type, "amount_915_yi": a915 if a915 > 0 else None, "amount_920_yi": a920 if a920 > 0 else None, "amount_925_yi": a925 if a925 > 0 else 0.0, "ratio_920_915": r20, "ratio_925_920": r25, "behavior_bonus": bonus, "penalty_multiplier": mult, "reason": reason}

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


def _turnover_bonus(turnover_pct: Optional[float], params: Dict[str, Any]) -> float:
    if turnover_pct is None:
        return 0.0
    if turnover_pct >= float(params.get("auction_turnover_bonus_high_pct", 1.0)):
        return float(params.get("auction_turnover_bonus_high", 5))
    if turnover_pct >= float(params.get("auction_turnover_bonus_mid_pct", 0.5)):
        return float(params.get("auction_turnover_bonus_mid", 3))
    return 0.0


def _amount_quality_bonus(amount_wan: Optional[float], params: Dict[str, Any]) -> float:
    if amount_wan is None:
        return 0.0
    if amount_wan >= float(params.get("auction_amount_bonus_high_wan", 5000)):
        return float(params.get("auction_amount_bonus_high", 5))
    if amount_wan >= float(params.get("auction_amount_bonus_mid_wan", 2000)):
        return float(params.get("auction_amount_bonus_mid", 3))
    if amount_wan >= float(params.get("auction_amount_bonus_low_wan", 1000)):
        return float(params.get("auction_amount_bonus_low", 1))
    return 0.0


def _net_pressure_bonus(row: Optional[Dict[str, Any]], params: Dict[str, Any]) -> Tuple[float, Optional[float]]:
    if not row:
        return 0.0, None
    net = _first_money_wan(row, ["main_net_inflow_wan", "主力净买", "主力净流入", "net_amount_wan"])
    mcap_yi = _to_float(row.get("market_cap_yi") or row.get("流通值") or row.get("流通市值"))
    if net is None or not mcap_yi or mcap_yi <= 0:
        return 0.0, None
    pressure = net / (mcap_yi * 10000.0)
    if pressure >= float(params.get("net_pressure_bonus_high_ratio", 0.002)):
        return float(params.get("net_pressure_bonus_high", 8)), pressure
    if pressure >= float(params.get("net_pressure_bonus_mid_ratio", 0.001)):
        return float(params.get("net_pressure_bonus_mid", 5)), pressure
    if pressure >= float(params.get("net_pressure_bonus_low_ratio", 0.0005)):
        return float(params.get("net_pressure_bonus_low", 3)), pressure
    return 0.0, pressure


def _negative_auction_cap(raw_total: float, latest_pct: Optional[float], params: Dict[str, Any]) -> Tuple[float, Optional[str]]:
    if latest_pct is None:
        return raw_total, None
    if latest_pct <= float(params.get("negative_auction_deep_pct", -6.0)):
        return min(raw_total, float(params.get("negative_auction_deep_cap", 20))), "deep_negative"
    if latest_pct <= float(params.get("negative_auction_mid_pct", -3.0)):
        return min(raw_total, float(params.get("negative_auction_mid_cap", 35))), "mid_negative"
    if latest_pct < 0:
        return min(raw_total, float(params.get("negative_auction_soft_cap", 50))), "soft_negative"
    return raw_total, None


def _entry_tag(f_status: str, f25_yi: Optional[float], latest_pct: Optional[float], amount_wan: Optional[float], params: Dict[str, Any]) -> Tuple[str, str]:
    if f_status == "fake":
        return "avoid", "fake_fengdan"
    if f_status == "lock" and latest_pct is not None and latest_pct >= float(params.get("entry_board_watch_pct", 9.5)) and (f25_yi or 0) >= float(params.get("entry_lock_large_f25_yi", 1.0)):
        return "board_watch", "lock_near_limit_large_f25"
    if latest_pct is not None and latest_pct >= float(params.get("entry_high_open_pct", 8.5)):
        return "high_open_confirm", "near_limit_high_open"
    if amount_wan is not None and amount_wan < float(params.get("min_auction_amount_wan", 500)):
        return "low_liquidity_confirm", "auction_amount_below_min"
    return "normal", "normal"


def _rank_quality_synergy_bonus(ranks: List[Optional[int]], top_n: int, params: Dict[str, Any]) -> float:
    hits = sorted([r for r in ranks if r is not None and 0 < r <= top_n])
    if len(hits) <= 1:
        return 0.0
    bonus = 0.0
    for r in hits[1:]:
        if r <= 10:
            bonus += float(params.get("auction_synergy_rank10_bonus", 5))
        elif r <= 20:
            bonus += float(params.get("auction_synergy_rank20_bonus", 2))
        else:
            bonus += float(params.get("auction_synergy_rank30_bonus", 1))
    return bonus


def compute_auction_strengths(candidate_codes: List[str], vratio_rows: List[Dict[str, Any]], qiangchou_rows: List[Dict[str, Any]], netamount_rows: List[Dict[str, Any]], fengdan_rows: List[Dict[str, Any]], params: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    p = params or {}
    top_n = int(p.get("auction_top_rank_n", 30))
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
        f_behavior = _classify_fengdan(f_row, latest_pct, p)
        f_status = str(f_behavior.get("status") or "none")
        f_base = _inv_rank(f_rank, top_n)
        if f_status == "lock":
            f_score = min(100.0, f_base + float(f_behavior.get("behavior_bonus") or 0.0))
        elif f_status == "stable":
            f_score = f_base
        elif f_status == "consume" and f_behavior.get("consume_type") != "zero":
            f_score = min(100.0, f_base + float(f_behavior.get("behavior_bonus") or 0.0))
        else:
            f_score = 0.0

        last_mult = float(p.get("qiangchou_last_second_multiplier", 0.85))
        scores = {
            "vratio": _inv_rank(v_rank, top_n),
            "qiangchou_920_925": _inv_rank(qg_rank, top_n),
            "qiangchou_last_second": _inv_rank(ql_rank, top_n) * last_mult,
            "net_amount": _inv_rank(n_rank, top_n),
            "fengdan": f_score,
        }
        base_table = max(scores, key=lambda k: scores[k])
        base = scores[base_table]

        amount_wan = _first_money_wan(v_row, amount_keys) or _first_money_wan(qg_row, amount_keys) or _first_money_wan(ql_row, amount_keys) or _first_money_wan(n_row, amount_keys)
        turnover_pct = _first_pct(v_row, turnover_keys) or _first_pct(qg_row, turnover_keys) or _first_pct(ql_row, turnover_keys) or _first_pct(n_row, turnover_keys)
        amount_mult, amount_missing = _auction_amount_multiplier(amount_wan, p)
        turnover_bonus = _turnover_bonus(turnover_pct, p)
        amount_bonus = _amount_quality_bonus(amount_wan, p)
        net_bonus, net_pressure = _net_pressure_bonus(n_row, p)
        synergy_bonus = _rank_quality_synergy_bonus([v_rank, qg_rank, ql_rank, n_rank], top_n, p)

        bonus = synergy_bonus + turnover_bonus + amount_bonus + net_bonus
        if qg_rank is not None and qg_rank <= top_n:
            bonus += float(p.get("qiangchou_920_925_bonus", 5))
        if qg_rank is not None and ql_rank is not None:
            bonus += float(p.get("qiangchou_last_second_confirm_bonus", 6))
        if f_score > 0:
            bonus += float(p.get("auction_bonus_fengdan", 3))

        raw_total = max(0.0, min(100.0, base + bonus))
        capped_total, neg_cap_reason = _negative_auction_cap(raw_total, latest_pct, p)
        total = capped_total * amount_mult * float(f_behavior.get("penalty_multiplier") or 1.0)
        total = max(0.0, min(100.0, total))
        entry_tag, entry_reason = _entry_tag(f_status, f_behavior.get("amount_925_yi"), latest_pct, amount_wan, p)

        out[code] = {
            "auction_strength": round(total, 2),
            "raw_auction_strength": round(raw_total, 2),
            "capped_auction_strength": round(capped_total, 2),
            "auction_amount_multiplier": round(amount_mult, 4),
            "auction_amount_missing": amount_missing,
            "auction_amount_wan": amount_wan,
            "auction_turnover_pct": turnover_pct,
            "latest_change_pct": latest_pct,
            "negative_auction_cap_reason": neg_cap_reason,
            "turnover_bonus": round(turnover_bonus, 2),
            "amount_quality_bonus": round(amount_bonus, 2),
            "net_pressure_bonus": round(net_bonus, 2),
            "net_pressure": net_pressure,
            "synergy_bonus": round(synergy_bonus, 2),
            "rank_synergy_bonus": round(synergy_bonus, 2),
            "base": round(base, 2),
            "base_table": base_table,
            "bonus": round(bonus, 2),
            "vratio_rank": v_rank,
            "qiangchou_rank": qg_rank if qg_rank is not None else ql_rank,
            "qiangchou_grab_rank": qg_rank,
            "qiangchou_920_925_rank": qg_rank,
            "qiangchou_last_second_rank": ql_rank,
            "qiangchou_primary_signal": "9:20-9:25" if qg_rank is not None else ("last_second" if ql_rank is not None else None),
            "net_amount_rank": n_rank,
            "fengdan_rank": f_rank,
            "fengdan_status": f_status,
            "fengdan_consume_type": f_behavior.get("consume_type"),
            "fengdan_behavior_reason": f_behavior.get("reason"),
            "entry_tag": entry_tag,
            "entry_reason": entry_reason,
            "fengdan_amount_915_yi": f_behavior.get("amount_915_yi"),
            "fengdan_amount_920_yi": f_behavior.get("amount_920_yi"),
            "fengdan_amount_925_yi": f_behavior.get("amount_925_yi"),
            "fengdan_ratio_920_915": f_behavior.get("ratio_920_915"),
            "fengdan_ratio_925_920": f_behavior.get("ratio_925_920"),
            "fengdan_behavior_bonus": round(float(f_behavior.get("behavior_bonus") or 0.0), 2),
            "fengdan_penalty_multiplier": round(float(f_behavior.get("penalty_multiplier") or 1.0), 4),
            "hits_count": sum(1 for r in [v_rank, qg_rank, ql_rank, n_rank] if r is not None and r <= top_n) + (1 if f_score > 0 else 0),
        }
    return out


def _self_test() -> None:
    q = [
        {"group": "grab", "rank": 2, "code": "002297", "auction_turnover_wan": "11203", "latest_change_pct": "3.35", "turnover_rate_pct": 1.12},
        {"group": "qiangchou", "rank": 1, "code": "002297", "auction_turnover_wan": "11203", "latest_change_pct": "3.35", "turnover_rate_pct": 1.12},
        {"group": "qiangchou", "rank": 1, "code": "000001", "auction_turnover_wan": "200", "latest_change_pct": "6.0", "turnover_rate_pct": 0.1},
    ]
    n = [{"rank": 5, "code": "002297", "main_net_inflow_wan": 5000, "market_cap_yi": 100, "auction_turnover_wan": 11203, "latest_change_pct": 3.35}]
    f = [{"rank": 1, "code": "002297", "amount_915": "1亿", "amount_920": "1.2亿", "amount_925": "1.3亿", "latest_change_pct": "9.8%", "section_kind": "live"}]
    out = compute_auction_strengths(["002297", "000001"], [], q, n, f, {})
    assert out["002297"]["qiangchou_920_925_rank"] == 2, out["002297"]
    assert out["002297"]["qiangchou_last_second_rank"] == 1, out["002297"]
    assert out["002297"]["qiangchou_primary_signal"] == "9:20-9:25", out["002297"]
    assert out["002297"]["amount_quality_bonus"] >= 5, out["002297"]
    assert out["000001"]["auction_amount_multiplier"] == 0.5, out["000001"]
    print("auction_strength conservative _self_test passed")


if __name__ == "__main__":
    _self_test()
