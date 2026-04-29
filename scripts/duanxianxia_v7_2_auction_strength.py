"""
duanxianxia_v7_2_auction_strength.py — v7.2 auction_strength scoring (0-100).

Final hardened logic:
  base  = max(inv_rank(vratio), inv_rank(qiangchou), inv_rank(net_amount), fengdan_score)
  bonus = 5 * (non-fengdan hit count - 1) + 3 * (fengdan hit and counted)
        + 5 * (qiangchou.group == 'grab') + turnover bonus
  raw   = clip(base + bonus, 0, 100)
  raw   = apply negative_auction_cap based on latest_change_pct
  total = clip(raw * auction_amount_multiplier, 0, 100)

Hardening (post real-data review):
  - real auction turnover field is `auction_turnover_wan` (not `竞额`)
  - missing amount must NOT punish the stock; it just flips a debug flag
  - fengdan with rank + amount_920 but amount_925 missing/'-' is an
    "unverified" near-limit pattern; give discounted credit, not zero
  - clearly weak / negative auction change caps raw strength so net-amount
    low-open names cannot dominate the T0 strong pool
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    if len(s) >= 6:
        s = s[-6:]
    return s


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


def _parse_yi(v: Any) -> Optional[float]:
    """Parse money into 亿."""
    wan = _parse_money_to_wan(v)
    return None if wan is None else wan / 10000.0


def _parse_money_to_wan(v: Any) -> Optional[float]:
    """Parse '3.4亿' / '5000万' / plain numeric into 万.

    Plain numeric fields in these captures (e.g. `auction_turnover_wan`) are
    treated as 万, matching the cashflow / capture conventions.
    """
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


def _index_by_code_min_rank(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        code = _norm_code(row.get("code") or row.get("代码"))
        if not code:
            continue
        rank = _to_int(row.get("rank") or row.get("排名"))
        if code not in out:
            out[code] = row
            continue
        old_rank = _to_int(out[code].get("rank") or out[code].get("排名"))
        if rank is not None and (old_rank is None or rank < old_rank):
            out[code] = row
    return out


def _classify_fengdan(
    fengdan_row: Optional[Dict[str, Any]],
    shrink_threshold: float,
) -> Tuple[str, Optional[float], Optional[float]]:
    """Classify fengdan as stable / unverified / withdrawn / none.

    Returns (status, amount_920_yi, amount_925_yi).

    - stable:     amount_925 valid, not heavily shrunk vs amount_920
    - unverified: rank present, amount_920 valid, amount_925 missing/'-'
                  (typical for near-limit names that have not produced a clean
                   9:25 seal-amount field yet)
    - withdrawn:  amount_925 valid but heavily shrunk vs amount_920
    - none:       no fengdan signal at all
    """
    if fengdan_row is None:
        return "none", None, None
    a920 = _parse_yi(fengdan_row.get("amount_920"))
    a925 = _parse_yi(fengdan_row.get("amount_925"))
    if a925 is None or a925 <= 0:
        if a920 is not None and a920 > 0:
            return "unverified", a920, None
        return "none", a920, None
    if a920 is None or a920 <= 0:
        return "stable", a920, a925
    if (a925 - a920) / a920 > shrink_threshold:
        return "stable", a920, a925
    return "withdrawn", a920, a925


def _auction_amount_multiplier(
    amount_wan: Optional[float], params: Dict[str, Any]
) -> Tuple[float, bool]:
    """Return (multiplier, amount_missing_flag).

    Missing amount is a data-quality issue, NOT a stock-quality issue, so it
    no longer slashes the score in half. We only discount when amount is
    explicitly small.
    """
    if amount_wan is None:
        return 1.0, True
    min_wan = float(params.get("min_auction_amount_wan", 500))
    full_wan = float(params.get("full_auction_amount_wan", 1000))
    if amount_wan < min_wan:
        return float(params.get("auction_amount_low_multiplier", 0.5)), False
    if amount_wan < full_wan:
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


def _negative_auction_cap(
    raw_total: float, latest_pct: Optional[float], params: Dict[str, Any]
) -> Tuple[float, Optional[str]]:
    """Cap raw_total when premarket auction change is clearly weak.

    Premarket selection is for high-conviction strong-open setups. Low-open
    names that rank by net inflow alone (e.g. 601778 -7.72%, 002176 -5.88%,
    600410 -6.75%) belong to a separate intraday rebound model, not to T0
    premarket main pool.
    """
    if latest_pct is None:
        return raw_total, None
    deep = float(params.get("negative_auction_deep_pct", -6.0))
    mid = float(params.get("negative_auction_mid_pct", -3.0))
    deep_cap = float(params.get("negative_auction_deep_cap", 20))
    mid_cap = float(params.get("negative_auction_mid_cap", 35))
    soft_cap = float(params.get("negative_auction_soft_cap", 50))
    if latest_pct <= deep:
        return min(raw_total, deep_cap), "deep_negative"
    if latest_pct <= mid:
        return min(raw_total, mid_cap), "mid_negative"
    if latest_pct < 0:
        return min(raw_total, soft_cap), "soft_negative"
    return raw_total, None


def compute_auction_strengths(
    candidate_codes: List[str],
    vratio_rows: List[Dict[str, Any]],
    qiangchou_rows: List[Dict[str, Any]],
    netamount_rows: List[Dict[str, Any]],
    fengdan_rows: List[Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    p = params or {}
    top_n = int(p.get("auction_top_rank_n", 30))
    bonus_strong = float(p.get("auction_bonus_strong", 5))
    bonus_fengdan = float(p.get("auction_bonus_fengdan", 3))
    bonus_grab = float(p.get("auction_bonus_grab", 5))
    shrink_threshold = float(p.get("fengdan_shrink_threshold", -0.20))
    fengdan_unverified_mult = float(p.get("fengdan_unverified_multiplier", 0.6))

    vratio_idx = _index_by_code_min_rank(vratio_rows)
    qiangchou_idx = _index_by_code_min_rank(qiangchou_rows)
    netamount_idx = _index_by_code_min_rank(netamount_rows)
    fengdan_idx = _index_by_code_min_rank([
        r for r in (fengdan_rows or [])
        if str(r.get("section_kind") or "").strip() in {"", "live"}
    ])

    # Real capture fields (verified from auction.jjyd.* data):
    #   auction_turnover_wan / auction_turnover_wan_text are the actual amounts.
    amount_keys = [
        "auction_turnover_wan",
        "auction_turnover_wan_text",
        "竞额",
        "竞价成交额",
        "竞价金额",
        "auction_amount_wan",
        "auction_amount",
        "amount",
        "成交额",
    ]
    turnover_keys = [
        "turnover_rate_pct",
        "竞价换手",
        "竞价换手率",
        "turnover_rate",
        "换手率",
    ]
    latest_pct_keys = [
        "latest_change_pct",
        "auction_change_pct",
        "auction_change_pct_text",
        "竞价涨幅",
        "涨幅",
    ]

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = _norm_code(raw)
        if not code or code in out:
            continue

        v_row = vratio_idx.get(code)
        q_row = qiangchou_idx.get(code)
        n_row = netamount_idx.get(code)
        f_row = fengdan_idx.get(code)

        v_rank = _to_int((v_row or {}).get("rank") or (v_row or {}).get("排名"))
        q_rank = _to_int((q_row or {}).get("rank") or (q_row or {}).get("排名"))
        n_rank = _to_int((n_row or {}).get("rank") or (n_row or {}).get("排名"))
        f_rank = _to_int((f_row or {}).get("rank") or (f_row or {}).get("排名"))

        f_status, f_a920, f_a925 = _classify_fengdan(f_row, shrink_threshold)
        if f_status == "stable":
            f_score = _inv_rank(f_rank, top_n)
        elif f_status == "unverified":
            f_score = _inv_rank(f_rank, top_n) * fengdan_unverified_mult
        else:
            f_score = 0.0

        scores = {
            "vratio": _inv_rank(v_rank, top_n),
            "qiangchou": _inv_rank(q_rank, top_n),
            "net_amount": _inv_rank(n_rank, top_n),
            "fengdan": f_score,
        }
        base_table = max(scores, key=lambda k: scores[k])
        base = scores[base_table]

        non_fengdan_hits = sum(1 for k in ("vratio", "qiangchou", "net_amount") if scores[k] > 0)
        bonus = max(0, non_fengdan_hits - 1) * bonus_strong
        if scores["fengdan"] > 0:
            bonus += bonus_fengdan

        q_group = str((q_row or {}).get("group") or (q_row or {}).get("分组") or "").strip().lower()
        if q_group == "grab":
            bonus += bonus_grab

        # Amount: prefer vratio, then qiangchou, then net_amount, then fengdan seal_total.
        auction_amount_wan = (
            _first_money_wan(v_row, amount_keys)
            or _first_money_wan(q_row, amount_keys)
            or _first_money_wan(n_row, amount_keys)
        )
        # Turnover %: prefer the table that gave us the base.
        turnover_pct = (
            _first_pct(v_row, turnover_keys)
            or _first_pct(q_row, turnover_keys)
            or _first_pct(n_row, turnover_keys)
        )
        turnover_bonus = _turnover_bonus(turnover_pct, p)
        bonus += turnover_bonus

        # Latest %: used for negative-auction cap.
        latest_pct = (
            _first_pct(n_row, latest_pct_keys)
            or _first_pct(v_row, latest_pct_keys)
            or _first_pct(q_row, latest_pct_keys)
            or _first_pct(f_row, latest_pct_keys)
        )

        raw_total = max(0.0, min(100.0, base + bonus))
        capped_total, neg_cap_reason = _negative_auction_cap(raw_total, latest_pct, p)
        amount_multiplier, amount_missing = _auction_amount_multiplier(
            auction_amount_wan, p
        )
        total = max(0.0, min(100.0, capped_total * amount_multiplier))

        out[code] = {
            "auction_strength": round(total, 2),
            "raw_auction_strength": round(raw_total, 2),
            "capped_auction_strength": round(capped_total, 2),
            "auction_amount_multiplier": round(amount_multiplier, 4),
            "auction_amount_missing": amount_missing,
            "auction_amount_wan": auction_amount_wan,
            "auction_turnover_pct": turnover_pct,
            "latest_change_pct": latest_pct,
            "negative_auction_cap_reason": neg_cap_reason,
            "turnover_bonus": round(turnover_bonus, 2),
            "base": round(base, 2),
            "base_table": base_table,
            "bonus": round(bonus, 2),
            "vratio_rank": v_rank,
            "qiangchou_rank": q_rank,
            "qiangchou_group": q_group or None,
            "net_amount_rank": n_rank,
            "fengdan_rank": f_rank,
            "fengdan_status": f_status,
            "fengdan_amount_920_yi": f_a920,
            "fengdan_amount_925_yi": f_a925,
            "hits_count": non_fengdan_hits + (1 if scores["fengdan"] > 0 else 0),
        }
    return out


def _self_test() -> None:
    # 1) strong with real auction_turnover_wan field
    vratio = [
        {"rank": 1, "code": "603629", "auction_turnover_wan": "1200", "turnover_rate_pct": 1.2, "latest_change_pct": "5.0"},
        {"rank": 1, "code": "000001", "auction_turnover_wan": "200", "turnover_rate_pct": 0.1, "latest_change_pct": "3.0"},
        {"rank": 5, "code": "000709", "auction_turnover_wan": "5694", "turnover_rate_pct": 0.66, "latest_change_pct": "5.75"},
    ]
    qiangchou = [{"rank": 2, "code": "603629", "group": "grab"}, {"rank": 5, "code": "000001"}]
    netamount = [
        {"rank": 3, "code": "603629", "latest_change_pct": 5.0},
        {"rank": 1, "code": "601778", "auction_turnover_wan": 22598, "latest_change_pct": -7.72},
    ]
    fengdan = [
        {"rank": 1, "code": "603629", "amount_920": "5亿", "amount_925": "8亿", "section_kind": "live"},
        {"rank": 2, "code": "000001", "amount_920": "10亿", "amount_925": "3亿", "section_kind": "live"},
        {"rank": 5, "code": "603630", "amount_920": "1.6亿", "amount_925": "-", "latest_change_pct": "9.75%", "section_kind": "live"},
    ]
    out = compute_auction_strengths(
        ["603629", "000001", "000709", "601778", "603630"],
        vratio, qiangchou, netamount, fengdan, {},
    )
    # 603629: real amount 1200万 -> mid multiplier 0.8
    assert out["603629"]["auction_amount_multiplier"] == 0.8, out["603629"]
    assert out["603629"]["auction_amount_missing"] is False
    # 000001: amount 200万 < 500 -> 0.5; fengdan withdrawn -> not counted
    assert out["000001"]["auction_amount_multiplier"] == 0.5
    assert out["000001"]["fengdan_status"] == "withdrawn"
    # 000709: amount 5694万 >= 1000 -> 1.0 multiplier; auction_strength should be ~100
    assert out["000709"]["auction_amount_multiplier"] == 1.0, out["000709"]
    assert out["000709"]["auction_strength"] >= 80, out["000709"]
    # 601778: -7.72% deep negative -> cap 20
    assert out["601778"]["negative_auction_cap_reason"] == "deep_negative"
    assert out["601778"]["auction_strength"] <= 20, out["601778"]
    # 603630: fengdan unverified -> not zero, partial credit
    assert out["603630"]["fengdan_status"] == "unverified", out["603630"]
    assert out["603630"]["auction_strength"] > 0, out["603630"]
    print("auction_strength _self_test passed")


if __name__ == "__main__":
    _self_test()
