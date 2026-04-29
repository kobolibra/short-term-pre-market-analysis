"""
duanxianxia_v7_1_stock_t1_label.py — v7.1 个股资金趋势 T-1 标签

super_ratio = 特大单净流入 / abs(主力净流入),代表主力中特大单占比
阈值分档:
  - top: super_ratio >= super_ratio_top (0.4)
  - mid: super_ratio_mid (0.3) ≤ super_ratio < top
  - miss: 不足 mid 或 主力额 < super_ratio_main_inflow_floor_wan (1000万)
后缀:
  - strong: cashflow_3day_t1 中 主力净流入 也为正 且 连续多日跨过阈值(近似)
  - retail: 仅今日达标

v7.2 compatibility:
  - expose float_market_value_yi / float_market_value_wan from cashflow row if present
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from duanxianxia_v7_parsing import parse_money_to_wan


def _index_by_code(rows: List[Dict[str, Any]], code_keys: List[str] = ("代码", "code")) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        code = ""
        for k in code_keys:
            v = row.get(k)
            if v not in (None, ""):
                code = str(v).strip()
                if code:
                    break
        if not code:
            continue
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        out[code] = row
    return out


def _row_super_ratio(row: Dict[str, Any]) -> Optional[float]:
    super_wan = parse_money_to_wan(row.get("特大单净流入"))
    main_wan = parse_money_to_wan(row.get("主力净流入"))
    if super_wan is None or main_wan is None:
        return None
    if abs(main_wan) < 1e-6:
        return None
    return abs(super_wan) / abs(main_wan)


def _row_main_inflow_wan(row: Dict[str, Any]) -> Optional[float]:
    return parse_money_to_wan(row.get("主力净流入"))


def _row_float_market_value_wan(row: Dict[str, Any]) -> Optional[float]:
    """Parse float market value into 万.

    Supports common field names from cashflow/rank tables. Values with 亿/万
    are parsed by shared parser. Plain numeric values are treated as 万, which
    matches existing cashflow numeric conventions.
    """
    for key in (
        "流通值", "流通市值", "流通市值(亿)", "float_market_value", "float_market_value_yi", "float_mv", "流通盘",
    ):
        if key not in row:
            continue
        raw = row.get(key)
        if raw in (None, "", "-"):
            continue
        # If field name explicitly says 亿 and raw is plain numeric, convert manually.
        if key in {"流通市值(亿)", "float_market_value_yi"} and "亿" not in str(raw) and "万" not in str(raw):
            try:
                return float(str(raw).replace(",", "").strip()) * 10000.0
            except Exception:
                pass
        val = parse_money_to_wan(raw)
        if val is not None:
            return val
    return None


def compute_stock_t1_labels(
    candidate_codes: List[str],
    cashflow_today_t1: List[Dict[str, Any]],
    cashflow_3day_t1: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    today_idx = _index_by_code(cashflow_today_t1)
    three_idx = _index_by_code(cashflow_3day_t1)

    th_top = float(params.get("super_ratio_top", 0.4))
    th_mid = float(params.get("super_ratio_mid", 0.3))
    floor_wan = float(params.get("super_ratio_main_inflow_floor_wan", 1000))

    out: Dict[str, Dict[str, Any]] = {}
    for raw_code in candidate_codes or []:
        code = str(raw_code or "").strip()
        if not code:
            continue
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        if code in out:
            continue

        today_row = today_idx.get(code)
        if today_row is None:
            out[code] = {
                "label": "miss",
                "reason": "missing_in_cashflow_today",
                "super_ratio": None,
                "main_inflow_wan": None,
                "float_market_value_wan": None,
                "float_market_value_yi": None,
            }
            continue

        sr = _row_super_ratio(today_row)
        main_wan = _row_main_inflow_wan(today_row)
        float_mv_wan = _row_float_market_value_wan(today_row)
        float_mv_yi = None if float_mv_wan is None else float_mv_wan / 10000.0

        base = {
            "super_ratio": sr,
            "main_inflow_wan": main_wan,
            "float_market_value_wan": float_mv_wan,
            "float_market_value_yi": float_mv_yi,
        }

        if sr is None or main_wan is None:
            out[code] = {"label": "miss", "reason": "unparseable_cashflow_fields", **base}
            continue

        if main_wan < floor_wan:
            out[code] = {"label": "miss", "reason": f"main_inflow_wan {main_wan} below floor {floor_wan}", **base}
            continue

        if sr >= th_top:
            tier = "top"
        elif sr >= th_mid:
            tier = "mid"
        else:
            out[code] = {"label": "miss", "reason": f"super_ratio {sr:.3f} below mid {th_mid}", **base}
            continue

        suffix = "retail"
        three_row = three_idx.get(code)
        if three_row is not None:
            sr_3 = _row_super_ratio(three_row)
            main_wan_3 = _row_main_inflow_wan(three_row)
            if sr_3 is not None and main_wan_3 is not None and main_wan_3 > 0 and sr_3 >= th_mid:
                suffix = "strong"

        out[code] = {
            "label": f"hit_{tier}_{suffix}",
            "reason": "",
            **base,
            "super_ratio_3day": _row_super_ratio(three_row) if three_row else None,
            "main_inflow_wan_3day": _row_main_inflow_wan(three_row) if three_row else None,
        }
    return out


def _self_test() -> None:
    today_rows = [
        {"代码": "000001", "主力净流入": "5000", "特大单净流入": "3000", "流通值": "30亿"},
        {"代码": "000002", "主力净流入": "3000", "特大单净流入": "1100"},
        {"代码": "000003", "主力净流入": "500", "特大单净流入": "500"},
        {"代码": "000004", "主力净流入": "-2000", "特大单净流入": "-1500"},
    ]
    three_rows = [{"代码": "000001", "主力净流入": "15000", "特大单净流入": "5000"}]
    params = {"super_ratio_top": 0.4, "super_ratio_mid": 0.3, "super_ratio_main_inflow_floor_wan": 1000}
    out = compute_stock_t1_labels(["000001", "000002", "000003", "000004", "999999"], today_rows, three_rows, params)
    assert out["000001"]["label"] == "hit_top_strong", out["000001"]
    assert out["000001"]["float_market_value_yi"] == 30.0, out["000001"]
    assert out["000002"]["label"] == "hit_mid_retail", out["000002"]
    assert out["000003"]["label"] == "miss", out["000003"]
    assert out["000004"]["label"] == "miss", out["000004"]
    assert out["999999"]["label"] == "miss"
    print("stock_t1_label _self_test passed")


if __name__ == "__main__":
    _self_test()
