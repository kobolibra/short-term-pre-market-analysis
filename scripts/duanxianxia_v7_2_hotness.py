"""
duanxianxia_v7_2_hotness.py — v7.2 hotness_score from rocket + hot_stock_day.

Reads premarket capture of:
  - rank.rocket (飙升榜)
  - rank.hot_stock_day (热度榜)

For each candidate code, returns hotness_score in [0, 100].

If both captures are empty for the code, returns None — callers should
re-distribute hotness weight to auction/theme rather than treating None as zero.

Hardening:
  - If latest_change_pct >= 9.7, hotness is capped at 20. These names are hot,
    but often already unavailable at 9:25; do not let them occupy top slots
    purely because of heat.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

DS_RANK_ROCKET = "rank.rocket"
DS_RANK_HOTSTOCK_DAY = "rank.hot_stock_day"


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


def _inv_rank_score(rank: Optional[int], top_n: int) -> float:
    if rank is None or rank <= 0 or rank > top_n:
        return 0.0
    return max(0.0, (top_n - rank + 1) / top_n * 100.0)


def _index_by_code(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        code = _norm_code(row.get("code") or row.get("代码"))
        if not code or code in out:
            continue
        out[code] = row
    return out


def _latest_change_pct(*rows: Optional[Dict[str, Any]]) -> Optional[float]:
    keys = ["latest_change_pct", "最新涨幅", "涨幅", "change_pct", "竞价涨幅", "auction_change_pct"]
    best: Optional[float] = None
    for row in rows:
        if not row:
            continue
        for k in keys:
            if k in row:
                v = _to_float(row.get(k))
                if v is not None:
                    best = v if best is None else max(best, v)
    return best


def compute_hotness_scores(
    rocket_rows: Optional[List[Dict[str, Any]]],
    hotstock_day_rows: Optional[List[Dict[str, Any]]],
    candidate_codes: List[str],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Optional[float]]:
    """Returns {code: hotness_score | None}."""
    p = params or {}
    rocket_top_n = int(p.get("hotness_rocket_top_n", 50))
    hotday_top_n = int(p.get("hotness_hotday_top_n", 100))
    w_rocket = float(p.get("hotness_rocket_weight", 0.6))
    w_hotday = float(p.get("hotness_hotday_weight", 0.4))
    cap_pct = float(p.get("hotness_limitup_cap_pct", 9.7))
    cap_score = float(p.get("hotness_limitup_cap_score", 20))

    rocket_idx = _index_by_code(rocket_rows or [])
    hotday_idx = _index_by_code(hotstock_day_rows or [])

    out: Dict[str, Optional[float]] = {}
    for raw in candidate_codes or []:
        code = _norm_code(raw)
        if not code or code in out:
            continue
        rocket_row = rocket_idx.get(code)
        hotday_row = hotday_idx.get(code)
        if rocket_row is None and hotday_row is None:
            out[code] = None
            continue
        rocket_rank = _to_int((rocket_row or {}).get("rank") or (rocket_row or {}).get("排名"))
        hotday_rank = _to_int((hotday_row or {}).get("rank") or (hotday_row or {}).get("排名"))
        rocket_score = _inv_rank_score(rocket_rank, rocket_top_n)
        hotday_score = _inv_rank_score(hotday_rank, hotday_top_n)
        if rocket_row is None:
            score = hotday_score
        elif hotday_row is None:
            score = rocket_score
        else:
            score = w_rocket * rocket_score + w_hotday * hotday_score

        pct = _latest_change_pct(rocket_row, hotday_row)
        if pct is not None and pct >= cap_pct:
            score = min(score, cap_score)
        out[code] = round(score, 2)
    return out


def hotness_data_available(
    rocket_rows: Optional[List[Dict[str, Any]]],
    hotstock_day_rows: Optional[List[Dict[str, Any]]],
    min_rows: int = 5,
) -> bool:
    return (len(rocket_rows or []) + len(hotstock_day_rows or [])) >= min_rows


def _self_test() -> None:
    rocket = [
        {"rank": 1, "code": "603629", "name": "利通电子", "latest_change_pct": "9.8%"},
        {"rank": 5, "code": "000001", "name": "平安银行", "latest_change_pct": "3.0%"},
    ]
    hotday = [
        {"排名": 1, "代码": "603629", "名称": "利通电子", "涨幅": "9.9%"},
        {"排名": 50, "代码": "000002", "名称": "万科A"},
    ]
    out = compute_hotness_scores(rocket, hotday, ["603629", "000001", "000002", "999999"], {})
    assert out["603629"] == 20, out
    assert out["000001"] is not None and 70 < out["000001"] < 100, out
    assert out["000002"] is not None, out
    assert out["999999"] is None, out
    print("hotness _self_test passed", out)


if __name__ == "__main__":
    _self_test()
