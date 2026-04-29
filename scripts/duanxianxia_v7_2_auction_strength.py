"""
duanxianxia_v7_2_auction_strength.py — v7.2 auction_strength scoring (0-100).

For each candidate, compute auction_strength based on its rank in 4 tables:
  - auction.jjyd.vratio       (real volume ratio, +5 bonus)
  - auction.jjyd.qiangchou    (subgroup grab = 末秒抢筹 +5 bonus)
  - auction.jjyd.net_amount   (real main inflow, +5 bonus)
  - auction.jjlive.fengdan    (queued seal amount, +3 bonus, validated)

Algorithm:
  base   = max inv_rank across the 4 tables (top-30, rank=1 → ~100)
  bonus  = +5 per additional non-base table hit (vratio/qiangchou/net_amount)
  bonus += +3 per additional fengdan hit, but only if amount_925 has not
          shrunk by more than `fengdan_shrink_threshold` vs amount_920
  bonus += +5 if qiangchou row's group == 'grab' (末秒抢筹)
  total  = clip(base + bonus, 0, 100)

fengdan section_kind filter: only rows with section_kind in {'', 'live'} are used.
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
        return int(float(str(v).strip()))
    except Exception:
        return None


def _parse_yi(v: Any) -> Optional[float]:
    """Parse '3.4亿' / '5000万' / plain number → 亿 (float)."""
    if v in (None, "", "-"):
        return None
    s = str(v).strip()
    try:
        if "亿" in s:
            return float(s.replace("亿", "").strip())
        if "万" in s:
            return float(s.replace("万", "").strip()) / 10000.0
        return float(s)
    except Exception:
        return None


def _inv_rank(rank: Optional[int], top_n: int) -> float:
    if rank is None or rank <= 0 or rank > top_n:
        return 0.0
    return (top_n - rank + 1) / top_n * 100.0


def _index_by_code_min_rank(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index by code; on duplicate, keep the row with the smallest rank."""
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        code = _norm_code(row.get("code") or row.get("代码"))
        if not code:
            continue
        rank = _to_int(row.get("rank") or row.get("排名"))
        if code not in out:
            out[code] = row
            continue
        existing = out[code]
        existing_rank = _to_int(existing.get("rank") or existing.get("排名"))
        if rank is not None and (existing_rank is None or rank < existing_rank):
            out[code] = row
    return out


def _fengdan_stable(fengdan_row: Optional[Dict[str, Any]], shrink_threshold: float) -> bool:
    """True iff fengdan capture indicates a stable seal queue."""
    if fengdan_row is None:
        return False
    a920 = _parse_yi(fengdan_row.get("amount_920"))
    a925 = _parse_yi(fengdan_row.get("amount_925"))
    if a925 is None or a925 <= 0:
        return False
    if a920 is None or a920 <= 0:
        return True
    shrink = (a925 - a920) / a920
    return shrink > shrink_threshold


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
    fengdan_filtered = [
        r for r in (fengdan_rows or [])
        if str(r.get("section_kind") or "").strip() in {"", "live"}
    ]
    fengdan_idx = _index_by_code_min_rank(fengdan_filtered)

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

        v_score = _inv_rank(v_rank, top_n)
        q_score = _inv_rank(q_rank, top_n)
        n_score = _inv_rank(n_rank, top_n)
        f_score = _inv_rank(f_rank, top_n)

        scores = {
            "vratio": v_score, "qiangchou": q_score,
            "net_amount": n_score, "fengdan": f_score,
        }
        base_table = max(scores, key=lambda k: scores[k])
        base = scores[base_table]

        f_stable = _fengdan_stable(f_row, shrink_threshold)
        if base_table == "fengdan" and base > 0 and not f_stable:
            base *= 0.5

        bonus = 0.0
        for table, score in scores.items():
            if table == base_table or score <= 0:
                continue
            if table == "fengdan":
                if f_stable:
                    bonus += bonus_fengdan
            else:
                bonus += bonus_strong

        q_group = str((q_row or {}).get("group") or (q_row or {}).get("分组") or "").strip().lower()
        if q_group == "grab":
            bonus += bonus_grab

        total = max(0.0, min(100.0, base + bonus))
        out[code] = {
            "auction_strength": round(total, 2),
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
            "hits_count": sum(1 for s in scores.values() if s > 0),
        }
    return out


def _self_test() -> None:
    vratio = [{"rank": 1, "code": "603629"}]
    qiangchou = [
        {"rank": 2, "code": "603629", "group": "grab"},
        {"rank": 5, "code": "000001"},
    ]
    netamount = [{"rank": 3, "code": "603629"}]
    fengdan = [
        {"rank": 1, "code": "603629", "amount_920": "5亿", "amount_925": "8亿", "section_kind": "live"},
        {"rank": 2, "code": "000001", "amount_920": "10亿", "amount_925": "3亿", "section_kind": "live"},
    ]
    out = compute_auction_strengths(["603629", "000001"], vratio, qiangchou, netamount, fengdan, {})
    assert out["603629"]["auction_strength"] >= 95, out["603629"]
    assert out["603629"]["base_table"] == "vratio", out["603629"]
    assert out["000001"]["auction_strength"] < 50, out["000001"]
    print("auction_strength _self_test passed", out)


if __name__ == "__main__":
    _self_test()
