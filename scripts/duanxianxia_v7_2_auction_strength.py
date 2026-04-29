"""
duanxianxia_v7_2_auction_strength.py — v7.2 auction_strength scoring (0-100).

Final hardened logic:
  base  = max(inv_rank(vratio), inv_rank(qiangchou), inv_rank(net_amount), inv_rank(fengdan_validated))
  bonus = 5 * (non-fengdan hit count - 1) + 3 * (fengdan hit and validated)
        + 5 * (qiangchou.group == 'grab') + turnover bonus
  total = clip(base + bonus, 0, 100) * auction_amount_multiplier

Hardening added after final review:
  - low auction amount is a hidden multiplier to suppress fake-liquidity microcaps
  - auction turnover adds a small hard bonus for real active exchange
  - fengdan must pass amount_920 -> amount_925 shrinkage validation before it can count
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    if len(s) >= 6:
        s = s[-6:]
    return s


def _to_int(v: Any) -> Optional[int]:
    try:
        if v in (None, "", "-"):
            return None
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
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

    Plain numeric fields in these captures are treated as 万, matching cashflow
    table conventions and DeepSeek's “竞额(万)” recommendation.
    """
    if v in (None, "", "-"):
        return None
    s = str(v).replace(",", "").strip()
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


def _fengdan_stable(fengdan_row: Optional[Dict[str, Any]], shrink_threshold: float) -> bool:
    if fengdan_row is None:
        return False
    a920 = _parse_yi(fengdan_row.get("amount_920"))
    a925 = _parse_yi(fengdan_row.get("amount_925"))
    if a925 is None or a925 <= 0:
        return False
    if a920 is None or a920 <= 0:
        return True
    return (a925 - a920) / a920 > shrink_threshold


def _auction_amount_multiplier(amount_wan: Optional[float], params: Dict[str, Any]) -> float:
    min_wan = float(params.get("min_auction_amount_wan", 500))
    full_wan = float(params.get("full_auction_amount_wan", 1000))
    if amount_wan is None or amount_wan < min_wan:
        return float(params.get("auction_amount_low_multiplier", 0.5))
    if amount_wan < full_wan:
        return float(params.get("auction_amount_mid_multiplier", 0.8))
    return 1.0


def _turnover_bonus(turnover_pct: Optional[float], params: Dict[str, Any]) -> float:
    if turnover_pct is None:
        return 0.0
    if turnover_pct >= float(params.get("auction_turnover_bonus_high_pct", 1.0)):
        return float(params.get("auction_turnover_bonus_high", 5))
    if turnover_pct >= float(params.get("auction_turnover_bonus_mid_pct", 0.5)):
        return float(params.get("auction_turnover_bonus_mid", 3))
    return 0.0


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

    vratio_idx = _index_by_code_min_rank(vratio_rows)
    qiangchou_idx = _index_by_code_min_rank(qiangchou_rows)
    netamount_idx = _index_by_code_min_rank(netamount_rows)
    fengdan_idx = _index_by_code_min_rank([
        r for r in (fengdan_rows or [])
        if str(r.get("section_kind") or "").strip() in {"", "live"}
    ])

    amount_keys = ["竞额", "竞价成交额", "竞价金额", "auction_amount_wan", "auction_amount", "amount", "成交额"]
    turnover_keys = ["竞价换手", "竞价换手率", "turnover_rate_pct", "turnover_rate", "换手率"]

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

        f_stable = _fengdan_stable(f_row, shrink_threshold)
        scores = {
            "vratio": _inv_rank(v_rank, top_n),
            "qiangchou": _inv_rank(q_rank, top_n),
            "net_amount": _inv_rank(n_rank, top_n),
            "fengdan": _inv_rank(f_rank, top_n) if f_stable else 0.0,
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

        auction_amount_wan = _first_money_wan(v_row, amount_keys)
        if auction_amount_wan is None:
            auction_amount_wan = _first_money_wan(q_row, amount_keys)
        turnover_pct = _first_pct(v_row, turnover_keys)
        q_turnover = _first_pct(q_row, turnover_keys)
        if q_turnover is not None and (turnover_pct is None or q_turnover > turnover_pct):
            turnover_pct = q_turnover
        turnover_bonus = _turnover_bonus(turnover_pct, p)
        bonus += turnover_bonus

        raw_total = max(0.0, min(100.0, base + bonus))
        amount_multiplier = _auction_amount_multiplier(auction_amount_wan, p)
        total = max(0.0, min(100.0, raw_total * amount_multiplier))

        out[code] = {
            "auction_strength": round(total, 2),
            "raw_auction_strength": round(raw_total, 2),
            "auction_amount_multiplier": round(amount_multiplier, 4),
            "auction_amount_wan": auction_amount_wan,
            "auction_turnover_pct": turnover_pct,
            "turnover_bonus": round(turnover_bonus, 2),
            "base": round(base, 2),
            "base_table": base_table,
            "bonus": round(bonus, 2),
            "vratio_rank": v_rank,
            "qiangchou_rank": q_rank,
            "qiangchou_group": q_group or None,
            "net_amount_rank": n_rank,
            "fengdan_rank": f_rank,
            "fengdan_stable": f_stable,
            "fengdan_amount_920_yi": _parse_yi((f_row or {}).get("amount_920")),
            "fengdan_amount_925_yi": _parse_yi((f_row or {}).get("amount_925")),
            "hits_count": non_fengdan_hits + (1 if scores["fengdan"] > 0 else 0),
        }
    return out


def _self_test() -> None:
    vratio = [
        {"rank": 1, "code": "603629", "竞额": "1200万", "竞价换手": "1.2%"},
        {"rank": 1, "code": "000001", "竞额": "200万", "竞价换手": "0.1%"},
    ]
    qiangchou = [{"rank": 2, "code": "603629", "group": "grab"}, {"rank": 5, "code": "000001"}]
    netamount = [{"rank": 3, "code": "603629"}]
    fengdan = [
        {"rank": 1, "code": "603629", "amount_920": "5亿", "amount_925": "8亿", "section_kind": "live"},
        {"rank": 2, "code": "000001", "amount_920": "10亿", "amount_925": "3亿", "section_kind": "live"},
    ]
    out = compute_auction_strengths(["603629", "000001"], vratio, qiangchou, netamount, fengdan, {})
    assert out["603629"]["auction_strength"] >= 95, out["603629"]
    assert out["603629"]["turnover_bonus"] == 5
    assert out["000001"]["auction_amount_multiplier"] == 0.5, out["000001"]
    assert out["000001"]["fengdan_stable"] is False
    print("auction_strength _self_test passed", out)


if __name__ == "__main__":
    _self_test()
