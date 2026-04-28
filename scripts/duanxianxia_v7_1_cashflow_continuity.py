"""
duanxianxia_v7_1_cashflow_continuity.py — v7.1 资金连续性标签

输入: 股票代码,cashflow_today/3day/5day/10day_t1
label:
  - accumulating_strong: 今日+3日+5日+10日 都 >= 0 且 今日 ≥ 1000 万
  - accumulating: 今日+3日+5日 都 >= 0
  - distributing: 今日+5日 都 <= 0
  - neutral: 其他
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from duanxianxia_v7_parsing import parse_money_to_wan
from duanxianxia_v7_1_stock_t1_label import _index_by_code


def _main_wan(idx: Dict[str, Dict[str, Any]], code: str) -> Optional[float]:
    row = idx.get(code)
    if row is None:
        return None
    return parse_money_to_wan(row.get("主力净流入"))


def compute_cashflow_continuity(
    candidate_codes: List[str],
    cashflow_today_t1: List[Dict[str, Any]],
    cashflow_3day_t1: List[Dict[str, Any]],
    cashflow_5day_t1: List[Dict[str, Any]],
    cashflow_10day_t1: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """返回 {code: {label, today, three, five, ten}}。"""
    today_idx = _index_by_code(cashflow_today_t1)
    three_idx = _index_by_code(cashflow_3day_t1)
    five_idx = _index_by_code(cashflow_5day_t1)
    ten_idx = _index_by_code(cashflow_10day_t1)

    strong_today_min_wan = float(params.get("super_ratio_main_inflow_floor_wan", 1000))

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = str(raw or "").strip()
        if not code:
            continue
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        if code in out:
            continue

        t = _main_wan(today_idx, code)
        d3 = _main_wan(three_idx, code)
        d5 = _main_wan(five_idx, code)
        d10 = _main_wan(ten_idx, code)

        def _pos(x: Optional[float]) -> bool:
            return x is not None and x > 0

        def _nonneg(x: Optional[float]) -> bool:
            return x is not None and x >= 0

        def _neg(x: Optional[float]) -> bool:
            return x is not None and x < 0

        if _pos(t) and _nonneg(d3) and _nonneg(d5) and _nonneg(d10) and t >= strong_today_min_wan:
            label = "accumulating_strong"
        elif _nonneg(t) and _nonneg(d3) and _nonneg(d5):
            label = "accumulating"
        elif _neg(t) and _neg(d5):
            label = "distributing"
        else:
            label = "neutral"

        out[code] = {
            "label": label,
            "today_wan": t,
            "three_wan": d3,
            "five_wan": d5,
            "ten_wan": d10,
        }
    return out


def _self_test() -> None:
    params = {"super_ratio_main_inflow_floor_wan": 1000}
    today = [{"代码": "a", "主力净流入": "2000"}, {"代码": "b", "主力净流入": "500"}, {"代码": "c", "主力净流入": "-3000"}]
    d3 = [{"代码": "a", "主力净流入": "5000"}, {"代码": "b", "主力净流入": "1000"}, {"代码": "c", "主力净流入": "-5000"}]
    d5 = [{"代码": "a", "主力净流入": "8000"}, {"代码": "b", "主力净流入": "3000"}, {"代码": "c", "主力净流入": "-12000"}]
    d10 = [{"代码": "a", "主力净流入": "15000"}, {"代码": "b", "主力净流入": "-1000"}]
    out = compute_cashflow_continuity(["a", "b", "c", "d"], today, d3, d5, d10, params)
    assert out["a"]["label"] == "accumulating_strong", out["a"]
    assert out["b"]["label"] == "accumulating", out["b"]    # 今 主力<1000 但三五十均 ≥ 0/<0;十日<0 但 5日 不 <0 → 不是 distrib
    # b: today=500, d3=1000, d5=3000, d10=-1000 → today≥ 0, d3≥0, d5≥0 → accumulating(不看 10日)
    assert out["c"]["label"] == "distributing", out["c"]
    assert out["d"]["label"] == "neutral", out["d"]
    print("cashflow_continuity _self_test passed")


if __name__ == "__main__":
    _self_test()
