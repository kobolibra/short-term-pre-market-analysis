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
  fake / consume / lock / stable / unverified / none.

Hardening (post real-data review):
  - real auction turnover field is `auction_turnover_wan` (not `竞额`)
  - missing amount must NOT punish the stock; it just flips a debug flag
  - negative auction names are capped before amount multiplier
  - multi-table resonance is based on rank quality, not just hit count
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
      status, amount_915_yi, amount_920_yi, amount_925_yi,
      ratio_920_915, ratio_925_920, behavior_bonus, penalty_multiplier, reason.

    Basic behavior labels:
    - fake:       9:15 showed meaningful seal, 9:20 collapsed before no-cancel stage
    - consume:    9:20→9:25 seal was materially consumed by sell pressure
    - lock:       9:20→9:25 remained stable and latest pct is near limit-up
    - stable:     9:25 valid but not lock
    - unverified: 9:20 valid but 9:25 missing/'-'
    - none:       no usable fengdan signal
    """
    p = params or {}
    if fengdan_row is None:
        return {
            "status": "none",
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

    a915 = _money_yi_from_keys(fengdan_row, ["amount_915", "9:15", "915", "f15", "seal_915", "t15_amount"])
    a920 = _money_yi_from_keys(fengdan_row, ["amount_920", "9:20", "920", "f20", "seal_920", "t20_amount"])
    a925 = _money_yi_from_keys(fengdan_row, ["amount_925", "9:25", "925", "f25", "seal_925", "t25_amount"])

    fake_drop_ratio = float(p.get("fengdan_fake_drop_ratio", 0.30))
    fake_f15_min_wan = float(p.get("fengdan_fake_f15_min_wan", 1000))
    consume_ratio = float(p.get("fengdan_consume_ratio", 0.80))
    lock_ratio = float(p.get("fengdan_lock_ratio", 0.90))
    lock_latest_min_pct = float(p.get("fengdan_lock_latest_min_pct", 9.5))
    consume_limitup_pct = float(p.get("fengdan_consume_limitup_pct", 9.9))
    fake_penalty = float(p.get("fengdan_fake_penalty_multiplier", 0.70))

    ratio_920_915 = (a920 / a915) if (a915 is not None and a915 > 0 and a920 is not None) else None
    ratio_925_920 = (a925 / a920) if (a920 is not None and a920 > 0 and a925 is not None) else None

    def _resp(status: str, *, reason: str, behavior_bonus: float = 0.0, penalty_multiplier: float = 1.0) -> Dict[str, Any]:
        return {
            "status": status,
            "amount_915_yi": a915,
            "amount_920_yi": a920,
            "amount_925_yi": a925,
            "ratio_920_915": ratio_920_915,
            "ratio_925_920": ratio_925_920,
            "ratio_920_vs_915": ratio_920_915,
            "ratio_925_vs_920": ratio_925_920,
            "behavior_bonus": behavior_bonus,
            "penalty_multiplier": penalty_multiplier,
            "reason": reason,
        }

    # 9:15 有有效大封单，但 9:20 前大幅消失，属于可撤单阶段的诱多嫌疑。
    if a915 is not None and a915 * 10000.0 >= fake_f15_min_wan:
        if a920 is None or a920 <= 0 or (ratio_920_915 is not None and ratio_920_915 < fake_drop_ratio):
            return _resp(
                "fake",
                reason=(f"915->920 collapse ratio={round(ratio_920_915, 4)}" if ratio_920_915 is not None else "915_large_but_920_missing"),
                behavior_bonus=0.0,
                penalty_multiplier=fake_penalty,
            )

    if a925 is None or a925 <= 0:
        if a920 is not None and a920 > 0:
            return _resp("unverified", reason="missing_925")
        return _resp("none", reason="no_positive_amount")

    if a920 is None or a920 <= 0:
        return _resp("stable", reason="missing_920_but_925_valid")

    if ratio_925_920 is not None and ratio_925_920 < consume_ratio:
        bonus = (
            float(p.get("fengdan_consume_limitup_bonus", 6))
            if (latest_pct is not None and latest_pct >= consume_limitup_pct)
            else float(p.get("fengdan_consume_weak_bonus", 2))
        )
        return _resp(
            "consume",
            reason=f"920->925 consume ratio={round(ratio_925_920, 4)}",
            behavior_bonus=bonus,
        )

    if ratio_925_920 is not None and ratio_925_920 >= lock_ratio and latest_pct is not None and latest_pct >= lock_latest_min_pct:
        return _resp(
            "lock",
            reason=f"920->925 locked ratio={round(ratio_925_920, 4)}, latest_pct={latest_pct}",
            behavior_bonus=float(p.get("fengdan_lock_bonus", 15)),
        )

    # 兼容旧 shrink_threshold：如果 9:25 相比 9:20 明显缩小但未触发 consume_ratio，也视为 consume-like。
    if (a925 - a920) / a920 <= shrink_threshold:
        return _resp(
            "consume",
            reason=f"920->925 shrink={round((a925 - a920) / a920, 4)}",
            behavior_bonus=float(p.get("fengdan_consume_weak_bonus", 2)),
        )

    return _resp("stable", reason=(f"920->925 stable ratio={round(ratio_925_920, 4)}" if ratio_925_920 is not None else "stable"))


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
    fengdan_unverified_mult = float(p.get("fengdan_unverified_multiplier", 0.6))

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
            f_score = f_base_rank_score + float(f_behavior.get("behavior_bonus") or 0.0)
        elif f_status == "unverified":
            f_score = f_base_rank_score * fengdan_unverified_mult
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
            "fengdan_behavior_reason": f_behavior.get("reason"),
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
    # 603630: missing 9:25 => unverified, not zero
    assert out["603630"]["fengdan_status"] == "unverified", out["603630"]
    assert out["603630"]["auction_strength"] > 0, out["603630"]
    assert out["603629"]["fengdan_status"] == "lock", out["603629"]
    assert out["603629"]["synergy_bonus"] >= 2, out["603629"]
    print("auction_strength _self_test passed")


if __name__ == "__main__":
    _self_test()
