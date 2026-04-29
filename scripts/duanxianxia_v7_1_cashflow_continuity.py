"""
duanxianxia_v7_1_cashflow_continuity.py — v7.1 资金连续性标签

金额不再只看正负,默认要求等效 ≥300万/日:
  today ≥ 300万, 3day ≥ 900万, 5day ≥ 1500万, 10day ≥ 3000万。
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
    today_idx = _index_by_code(cashflow_today_t1)
    three_idx = _index_by_code(cashflow_3day_t1)
    five_idx = _index_by_code(cashflow_5day_t1)
    ten_idx = _index_by_code(cashflow_10day_t1)
    unit = float(params.get("cashflow_effective_min_wan", 300))
    strong_today_min_wan = float(params.get("super_ratio_main_inflow_floor_wan", 1000))

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = str(raw or "").strip()
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        if not code or code in out:
            continue

        t = _main_wan(today_idx, code)
        d3 = _main_wan(three_idx, code)
        d5 = _main_wan(five_idx, code)
        d10 = _main_wan(ten_idx, code)

        eff_t = t is not None and t >= unit
        eff_3 = d3 is not None and d3 >= unit * 3
        eff_5 = d5 is not None and d5 >= unit * 5
        eff_10 = d10 is not None and d10 >= unit * 10
        neg_t = t is not None and t <= -unit
        neg_5 = d5 is not None and d5 <= -unit * 5

        if eff_t and eff_3 and eff_5 and eff_10 and (t or 0) >= strong_today_min_wan:
            label = "accumulating_strong"
        elif eff_t and eff_3 and eff_5:
            label = "accumulating"
        elif neg_t and neg_5:
            label = "distributing"
        else:
            label = "neutral"

        out[code] = {
            "label": label,
            "today_wan": t,
            "three_wan": d3,
            "five_wan": d5,
            "ten_wan": d10,
            "effective_min_wan_per_day": unit,
        }
    return out


if __name__ == "__main__":
    params = {"cashflow_effective_min_wan": 300, "super_ratio_main_inflow_floor_wan": 1000}
    today = [{"代码": "000001", "主力净流入": "2000万"}, {"代码": "000002", "主力净流入": "200万"}, {"代码": "000003", "主力净流入": "-500万"}]
    d3 = [{"代码": "000001", "主力净流入": "2000万"}, {"代码": "000002", "主力净流入": "1000万"}, {"代码": "000003", "主力净流入": "-1000万"}]
    d5 = [{"代码": "000001", "主力净流入": "3000万"}, {"代码": "000002", "主力净流入": "2000万"}, {"代码": "000003", "主力净流入": "-2000万"}]
    d10 = [{"代码": "000001", "主力净流入": "5000万"}]
    out = compute_cashflow_continuity(["000001", "000002", "000003"], today, d3, d5, d10, params)
    assert out["000001"]["label"] == "accumulating_strong", out
    assert out["000002"]["label"] == "neutral", out
    assert out["000003"]["label"] == "distributing", out
    print("cashflow_continuity _self_test passed")
