"""
duanxianxia_v7_2_auction_strength.py — v7.2 auction_strength scoring (0-100).

Updated logic:
  base  = max(inv_rank(vratio), inv_rank(qiangchou), inv_rank(net_amount), fengdan_score)
  bonus = rank-quality synergy bonus + 3 * (fengdan hit and counted)
        + 5 * (qiangchou.group == 'grab') + turnover bonus
  raw   = clip(base + bonus, 0, 100)
  raw   = apply negative_auction_cap based on latest_change_pct
  total = clip(raw * auction_amount_multiplier, 0, 100)

Fengdan behavior uses 9:15 / 9:20 / 9:25 when available:
  fake / consume / lock / stable / none.

Important trading semantics:
  - Missing/blank amount_925 is treated as 0 in this table.
  - F20 > 0 and F25 missing/0 means the 9:20 seal was fully consumed,
    i.e. consume with consume_type="zero", not unverified.
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


def _money_yi_from_keys(row: Optional[Dict[str, Any]], keys: List[str]) -> Optional[float]:
    if not row:
        return None
    for k in keys:
        if k in row:
            v = _parse_yi(row.get(k))
            if v is not None:
                return v
    return None


def _classify_fengdan(
    fengdan_row: Optional[Dict[str, Any]],
    shrink_threshold: float,
    latest_pct: Optional[float],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Classify fengdan using 9:15/9:20/9:25 behavior.

    Returned dict keys:
      status, consume_type, amount_915_yi, amount_920_yi, amount_925_yi,
      ratio_920_915, ratio_925_920, behavior_bonus, penalty_multiplier, reason.

    Trading behavior labels:
    - fake:
      9:15 showed meaningful seal, but 9:20 collapsed before no-cancel stage.
      This is withdrawable-stage fake/inducement risk.
    - consume:
      9:20 had valid seal, but 9:25 was materially lower.
      Missing/blank 9:25 is treated as 0, so F20>0 and F25="-" is consume_zero.
    - lock:
      9:20→9:25 remained stable and latest pct is near limit-up.
      Strong, but may be hard to buy.
    - stable:
      9:25 valid, but not lock/consume/fake.
    - none:
      No usable fengdan signal.
    """
    p = params or {}
    if fengdan_row is None:
        return {
            "status": "none",
            "consume_type": None,
            "amount_915_yi": None,
            "amount_920_yi": None,
            "amount_925_yi": None,
            "ratio_920_915": None,
            "ratio_925_920": None,
            "ratio_920_vs_915": None,
            "ratio_925_vs_920": None,
            "behavior_bonus": 0.0,
            "penalty_multiplier": 1.0,
            "reason": "missing_row",
        }

    raw_915 = _money_yi_from_keys(
        fengdan_row,
        ["amount_915", "9:15", "915", "f15", "seal_915", "t15_amount"],
    )
    raw_920 = _money_yi_from_keys(
        fengdan_row,
        ["amount_920", "9:20", "920", "f20", "seal_920", "t20_amount"],
    )
    raw_925 = _money_yi_from_keys(
        fengdan_row,
        ["amount_925", "9:25", "925", "f25", "seal_925", "t25_amount"],
    )

    # Trading semantics for this table:
    # amount_925 missing / "-" / blank means final 9:25 seal is effectively 0.
    # This is NOT "unverified"; if F20 > 0, it is consume_zero.
    a915 = raw_915 if raw_915 is not None else 0.0
    a920 = raw_920 if raw_920 is not None else 0.0
    a925 = raw_925 if raw_925 is not None else 0.0

    fake_drop_ratio = float(p.get("fengdan_fake_drop_ratio", 0.30))
    fake_f15_min_wan = float(p.get("fengdan_fake_f15_min_wan", 1000))
    consume_ratio = float(p.get("fengdan_consume_ratio", 0.80))
    lock_ratio = float(p.get("fengdan_lock_ratio", 0.90))
    lock_latest_min_pct = float(p.get("fengdan_lock_latest_min_pct", 9.5))
    consume_limitup_pct = float(p.get("fengdan_consume_limitup_pct", 9.9))
    fake_penalty = float(p.get("fengdan_fake_penalty_multiplier", 0.70))

    ratio_920_915 = (a920 / a915) if a915 > 0 else None
    ratio_925_920 = (a925 / a920) if a920 > 0 else None

    def _resp(
        status: str,
        *,
        reason: str,
        consume_type: Optional[str] = None,
        behavior_bonus: float = 0.0,
        penalty_multiplier: float = 1.0,
    ) -> Dict[str, Any]:
        return {
            "status": status,
            "consume_type": consume_type,
            "amount_915_yi": a915 if a915 > 0 else None,
            "amount_920_yi": a920 if a920 > 0 else None,
            "amount_925_yi": a925 if a925 > 0 else 0.0,
            "ratio_920_915": ratio_920_915,
            "ratio_925_920": ratio_925_920,
            "ratio_920_vs_915": ratio_920_915,
            "ratio_925_vs_920": ratio_925_920,
            "behavior_bonus": behavior_bonus,
            "penalty_multiplier": penalty_multiplier,
            "reason": reason,
        }

    # 1) fake: 9:15 可撤单阶段有大封单，9:20 不可撤单前大幅消失。
    if a915 * 10000.0 >= fake_f15_min_wan:
        if a920 <= 0 or (ratio_920_915 is not None and ratio_920_915 < fake_drop_ratio):
            return _resp(
                "fake",
                reason=(
                    f"915->920 collapse ratio={round(ratio_920_915, 4)}"
                    if ratio_920_915 is not None
                    else "915_large_but_920_zero"
                ),
                behavior_bonus=0.0,
                penalty_multiplier=fake_penalty,
            )

    # 2) consume: 9:20 后不能撤单，F20 -> F25 的下降视为被卖盘消耗。
    # F25 missing/blank has already been converted to 0.0 above.
    if a920 > 0 and ratio_925_920 is not None and ratio_925_920 < consume_ratio:
        consume_type = "zero" if a925 <= 0 else "partial"
        if consume_type == "zero":
            bonus = 0.0
            reason = "920->925 fully consumed ratio=0.0"
        else:
            bonus = (
                float(p.get("fengdan_consume_limitup_bonus", 6))
                if (latest_pct is not None and latest_pct >= consume_limitup_pct)
                else float(p.get("fengdan_consume_weak_bonus", 2))
            )
            reason = f"920->925 consume ratio={round(ratio_925_920, 4)}"

        return _resp(
            "consume",
            consume_type=consume_type,
            reason=reason,
            behavior_bonus=bonus,
        )

    # 3) lock: 9:20 到 9:25 基本锁住，且价格接近涨停。
    if (
        a920 > 0
        and a925 > 0
        and ratio_925_920 is not None
        and ratio_925_920 >= lock_ratio
        and latest_pct is not None
        and latest_pct >= lock_latest_min_pct
    ):
        return _resp(
            "lock",
            reason=f"920->925 locked ratio={round(ratio_925_920, 4)}, latest_pct={latest_pct}",
            behavior_bonus=float(p.get("fengdan_lock_bonus", 15)),
        )

    # 4) 兼容旧 shrink_threshold：只在 F20/F25 都有效时保留。
    if a920 > 0 and a925 > 0 and (a925 - a920) / a920 <= shrink_threshold:
        return _resp(
            "consume",
            consume_type="partial",
            reason=f"920->925 shrink={round((a925 - a920) / a920, 4)}",
            behavior_bonus=float(p.get("fengdan_consume_weak_bonus", 2)),
        )

    # 5) stable: 最终 9:25 有封单，但不属于 lock/consume/fake。
    if a925 > 0:
        return _resp(
            "stable",
            reason=(
                f"920->925 stable ratio={round(ratio_925_920, 4)}"
                if ratio_925_920 is not None
                else "925_valid"
            ),
        )

    # 6) none: 全程没有有效封单。
    return _resp("none", reason="no_positive_amount")


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
    names that rank by net inflow alone belong to a separate intraday rebound
    model, not to the T0 premarket main pool.
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


def _entry_tag(
    *,
    fengdan_status: str,
    fengdan_amount_925_yi: Optional[float],
    latest_pct: Optional[float],
    auction_amount_wan: Optional[float],
    params: Dict[str, Any],
) -> Tuple[str, str]:
    """Simple tradability tag.

    This is not a new scoring factor. It only explains whether a strong signal
    is practically tradable.
    """
    if fengdan_status == "fake":
        return "avoid", "fake_fengdan"

    board_watch_pct = float(params.get("entry_board_watch_pct", 9.5))
    lock_large_f25_yi = float(params.get("entry_lock_large_f25_yi", 1.0))
    if (
        fengdan_status == "lock"
        and latest_pct is not None
        and latest_pct >= board_watch_pct
        and fengdan_amount_925_yi is not None
        and fengdan_amount_925_yi >= lock_large_f25_yi
    ):
        return "board_watch", "lock_near_limit_large_f25"

    high_open_pct = float(params.get("entry_high_open_pct", 8.5))
    if latest_pct is not None and latest_pct >= high_open_pct:
        return "high_open_confirm", "near_limit_high_open"

    min_amount = float(params.get("min_auction_amount_wan", 500))
    if auction_amount_wan is not None and auction_amount_wan < min_amount:
        return "low_liquidity_confirm", "auction_amount_below_min"

    return "normal", "normal"

def _rank_quality_synergy_bonus(ranks: List[Optional[int]], top_n: int, params: Dict[str, Any]) -> float:
    """Reward resonance by extra rank quality, not just hit count.

    The best table already contributes to base. Here we only score the extra
    valid ranks from the remaining tables.
    """
    hits = sorted([r for r in ranks if r is not None and 0 < r <= top_n])
    if len(hits) <= 1:
        return 0.0
    b10 = float(params.get("auction_synergy_rank10_bonus", 5))
    b20 = float(params.get("auction_synergy_rank20_bonus", 2))
    b30 = float(params.get("auction_synergy_rank30_bonus", 1))
    bonus = 0.0
    for r in hits[1:]:
        if r <= 10:
            bonus += b10
        elif r <= 20:
            bonus += b20
        else:
            bonus += b30
    return bonus


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
    bonus_fengdan = float(p.get("auction_bonus_fengdan", 3))
    bonus_grab = float(p.get("auction_bonus_grab", 5))
    shrink_threshold = float(p.get("fengdan_shrink_threshold", -0.20))

    vratio_idx = _index_by_code_min_rank(vratio_rows)
    qiangchou_idx = _index_by_code_min_rank(qiangchou_rows)
    netamount_idx = _index_by_code_min_rank(netamount_rows)
    fengdan_idx = _index_by_code_min_rank([
        r for r in (fengdan_rows or [])
        if str(r.get("section_kind") or "").strip() in {"", "live"}
    ])

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

        latest_pct = (
            _first_pct(n_row, latest_pct_keys)
            or _first_pct(v_row, latest_pct_keys)
            or _first_pct(q_row, latest_pct_keys)
            or _first_pct(f_row, latest_pct_keys)
        )

        f_behavior = _classify_fengdan(f_row, shrink_threshold, latest_pct, p)
        f_status = str(f_behavior.get("status") or "none")

        f_base_rank_score = _inv_rank(f_rank, top_n)
        if f_status == "stable":
            f_score = f_base_rank_score
        elif f_status == "lock":
            f_score = f_base_rank_score + float(f_behavior.get("behavior_bonus") or 0.0)
            if f_rank is not None and f_rank <= 5:
                f_score += float(p.get("fengdan_lock_top5_bonus", 5))
        elif f_status == "consume":
            # consume_partial: 被消耗但最终仍有封单，可以保留少量分歧承接加分。
            # consume_zero: F25 缺失/为0，说明最终封单归零，不给封单正分。
            if str(f_behavior.get("consume_type") or "") == "zero":
                f_score = 0.0
            else:
                f_score = f_base_rank_score + float(f_behavior.get("behavior_bonus") or 0.0)
        else:
            f_score = 0.0
        f_score = max(0.0, min(100.0, f_score))

        scores = {
            "vratio": _inv_rank(v_rank, top_n),
            "qiangchou": _inv_rank(q_rank, top_n),
            "net_amount": _inv_rank(n_rank, top_n),
            "fengdan": f_score,
        }
        base_table = max(scores, key=lambda k: scores[k])
        base = scores[base_table]

        non_fengdan_hits = sum(1 for k in ("vratio", "qiangchou", "net_amount") if scores[k] > 0)
        synergy_bonus = _rank_quality_synergy_bonus([v_rank, q_rank, n_rank], top_n, p)
        bonus = synergy_bonus
        if scores["fengdan"] > 0:
            bonus += bonus_fengdan

        q_group = str((q_row or {}).get("group") or (q_row or {}).get("分组") or "").strip().lower()
        if q_group == "grab":
            bonus += bonus_grab

        auction_amount_wan = (
            _first_money_wan(v_row, amount_keys)
            or _first_money_wan(q_row, amount_keys)
            or _first_money_wan(n_row, amount_keys)
        )
        turnover_pct = (
            _first_pct(v_row, turnover_keys)
            or _first_pct(q_row, turnover_keys)
            or _first_pct(n_row, turnover_keys)
        )
        turnover_bonus = _turnover_bonus(turnover_pct, p)
        bonus += turnover_bonus

        raw_total = max(0.0, min(100.0, base + bonus))
        capped_total, neg_cap_reason = _negative_auction_cap(raw_total, latest_pct, p)
        amount_multiplier, amount_missing = _auction_amount_multiplier(auction_amount_wan, p)
        total = max(0.0, min(100.0, capped_total * amount_multiplier))
        total = max(0.0, min(100.0, total * float(f_behavior.get("penalty_multiplier") or 1.0)))

        entry_tag, entry_reason = _entry_tag(
            fengdan_status=f_status,
            fengdan_amount_925_yi=f_behavior.get("amount_925_yi"),
            latest_pct=latest_pct,
            auction_amount_wan=auction_amount_wan,
            params=p,
        )

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
            "synergy_bonus": round(synergy_bonus, 2),
            "rank_synergy_bonus": round(synergy_bonus, 2),
            "base": round(base, 2),
            "base_table": base_table,
            "bonus": round(bonus, 2),
            "vratio_rank": v_rank,
            "qiangchou_rank": q_rank,
            "qiangchou_group": q_group or None,
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
            "fengdan_ratio_920_vs_915": f_behavior.get("ratio_920_vs_915"),
            "fengdan_ratio_925_vs_920": f_behavior.get("ratio_925_vs_920"),
            "fengdan_behavior_bonus": round(float(f_behavior.get("behavior_bonus") or 0.0), 2),
            "fengdan_penalty_multiplier": round(float(f_behavior.get("penalty_multiplier") or 1.0), 4),
            "hits_count": non_fengdan_hits + (1 if scores["fengdan"] > 0 else 0),
        }
    return out


def _self_test() -> None:
    vratio = [
        {"rank": 1, "code": "603629", "auction_turnover_wan": "1200", "turnover_rate_pct": 1.2, "latest_change_pct": "9.8"},
        {"rank": 1, "code": "000001", "auction_turnover_wan": "200", "turnover_rate_pct": 0.1, "latest_change_pct": "3.0"},
        {"rank": 5, "code": "000709", "auction_turnover_wan": "5694", "turnover_rate_pct": 0.66, "latest_change_pct": "5.75"},
    ]
    qiangchou = [{"rank": 2, "code": "603629", "group": "grab"}, {"rank": 5, "code": "000001"}]
    netamount = [
        {"rank": 3, "code": "603629", "latest_change_pct": 9.8},
        {"rank": 1, "code": "601778", "auction_turnover_wan": 22598, "latest_change_pct": -7.72},
    ]
    fengdan = [
        {"rank": 1, "code": "603629", "amount_915": "5亿", "amount_920": "6亿", "amount_925": "8亿", "latest_change_pct": "9.8%", "section_kind": "live"},
        {"rank": 2, "code": "000001", "amount_920": "10亿", "amount_925": "3亿", "section_kind": "live"},
        {"rank": 5, "code": "603630", "amount_920": "1.6亿", "amount_925": "-", "latest_change_pct": "9.75%", "section_kind": "live"},
    ]
    out = compute_auction_strengths(
        ["603629", "000001", "000709", "601778", "603630", "603631"],
        vratio, qiangchou, netamount, fengdan, {},
    )
    # 603629: real amount 1200万 >= 1000 -> full multiplier 1.0; lock because 8/5 >= 1.2
    assert out["603629"]["auction_amount_multiplier"] == 1.0, out["603629"]
    assert out["603629"]["auction_amount_missing"] is False
    assert out["603629"]["fengdan_status"] == "lock", out["603629"]
    # 000001: amount 200万 < 500 -> 0.5; fengdan consume
    assert out["000001"]["auction_amount_multiplier"] == 0.5
    assert out["000001"]["fengdan_status"] == "consume", out["000001"]
    # 000709: amount 5694万 >= 1000 -> 1.0 multiplier; score should stay high
    assert out["000709"]["auction_amount_multiplier"] == 1.0, out["000709"]
    assert out["000709"]["auction_strength"] >= 80, out["000709"]
    # 601778: -7.72% deep negative -> cap 20
    assert out["601778"]["negative_auction_cap_reason"] == "deep_negative"
    assert out["601778"]["auction_strength"] <= 20, out["601778"]
    # 603630: missing 9:25 is treated as 0; F20>0 and F25=0 => consume_zero.
    assert out["603630"]["fengdan_status"] == "consume", out["603630"]
    assert out["603630"]["fengdan_consume_type"] == "zero", out["603630"]
    assert out["603630"]["fengdan_behavior_bonus"] == 0.0, out["603630"]
    assert out["603629"]["fengdan_status"] == "lock", out["603629"]
    assert out["603629"]["synergy_bonus"] >= 2, out["603629"]
    print("auction_strength _self_test passed")


if __name__ == "__main__":
    _self_test()
