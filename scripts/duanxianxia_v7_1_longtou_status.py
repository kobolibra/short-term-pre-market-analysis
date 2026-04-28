"""
duanxianxia_v7_1_longtou_status.py — v7.1 龙头状态标签

confirmed_longtou 加严:
  - 板数 >=4 且 5日区间涨幅排名 <=5;或
  - 板数 ==3 且 5日区间涨幅排名 <=3。
"""

from __future__ import annotations
from typing import Any, Dict, List
from duanxianxia_v7_parsing import parse_int_safely
from duanxianxia_v7_1_stock_t1_label import _index_by_code


def _ltgd_rank_idx(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows or []:
        code = str(row.get("代码", "") or "").strip()
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        rank = parse_int_safely(row.get("排名"))
        if code and rank is not None and code not in out:
            out[code] = rank
    return out


def compute_longtou_status(candidate_codes: List[str], fupan_t1: List[Dict[str, Any]], ltgd_5day_t1: List[Dict[str, Any]], params: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    fupan_idx = _index_by_code(fupan_t1)
    ltgd_idx = _ltgd_rank_idx(ltgd_5day_t1)
    top_n = int(params.get("ltgd_top_n_for_longtou", 5))
    board_min = int(params.get("ltgd_confirmed_board_min", 4))
    board3_rank_max = int(params.get("ltgd_confirmed_board3_rank_max", 3))
    mid_max_rank = int(params.get("ltgd_mid_max_rank", 15))

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = str(raw or "").strip()
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        if not code or code in out:
            continue
        fupan_row = fupan_idx.get(code)
        boards = parse_int_safely(fupan_row.get("板数")) if fupan_row else None
        rank = ltgd_idx.get(code)
        b = boards or 0

        if rank is not None and ((b >= board_min and rank <= top_n) or (b == 3 and rank <= board3_rank_max)):
            label = "confirmed_longtou"
        elif b == 2 or (rank is not None and rank <= mid_max_rank and b >= 1):
            label = "mid_position"
        elif b >= 1:
            label = "follower"
        else:
            label = "none"
        out[code] = {"label": label, "boards": boards, "ltgd_rank": rank}
    return out


if __name__ == "__main__":
    params = {"ltgd_top_n_for_longtou": 5, "ltgd_confirmed_board_min": 4, "ltgd_confirmed_board3_rank_max": 3}
    fupan = [{"代码":"000001","板数":"4"},{"代码":"000002","板数":"3"},{"代码":"000003","板数":"3"}]
    ltgd = [{"代码":"000001","排名":"5"},{"代码":"000002","排名":"3"},{"代码":"000003","排名":"5"}]
    out = compute_longtou_status(["000001","000002","000003"], fupan, ltgd, params)
    assert out["000001"]["label"] == "confirmed_longtou"
    assert out["000002"]["label"] == "confirmed_longtou"
    assert out["000003"]["label"] == "mid_position"
    print("longtou_status _self_test passed")
