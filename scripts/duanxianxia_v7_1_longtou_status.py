"""
duanxianxia_v7_1_longtou_status.py — v7.1 龙头状态标签

输入: 候选股代码, fupan_t1 (review.fupan.plate), ltgd_5day_t1 (review.ltgd.range 中 5日行), params
labels:
  - confirmed_longtou: ltgd 5日 排名 ≤ ltgd_top_n_for_longtou (5) 且 fupan 板数 ≥ 3
  - mid_position: fupan 板数 == 2 或 (ltgd 排名 6..15 且 板数 ≥ 1)
  - follower: fupan 板数 == 1 且 不在 ltgd 前 15
  - none: 默认
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from duanxianxia_v7_parsing import parse_int_safely
from duanxianxia_v7_1_stock_t1_label import _index_by_code


def _ltgd_rank_idx(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """{code: 排名}。rows 应已过滤为 5日。"""
    out: Dict[str, int] = {}
    for row in rows or []:
        code = str(row.get("代码", "") or "").strip()
        if not code:
            continue
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        rank = parse_int_safely(row.get("排名"))
        if rank is not None and code not in out:
            out[code] = rank
    return out


def compute_longtou_status(
    candidate_codes: List[str],
    fupan_t1: List[Dict[str, Any]],
    ltgd_5day_t1: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    fupan_idx = _index_by_code(fupan_t1)
    ltgd_idx = _ltgd_rank_idx(ltgd_5day_t1)

    top_n = int(params.get("ltgd_top_n_for_longtou", 5))
    mid_max_rank = int(params.get("ltgd_mid_max_rank", 15))

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

        fupan_row = fupan_idx.get(code)
        boards = parse_int_safely(fupan_row.get("板数")) if fupan_row else None
        ltgd_rank = ltgd_idx.get(code)

        if ltgd_rank is not None and ltgd_rank <= top_n and (boards or 0) >= 3:
            label = "confirmed_longtou"
        elif (boards or 0) == 2:
            label = "mid_position"
        elif ltgd_rank is not None and ltgd_rank <= mid_max_rank and (boards or 0) >= 1:
            label = "mid_position"
        elif (boards or 0) == 1:
            label = "follower"
        else:
            label = "none"

        out[code] = {
            "label": label,
            "boards": boards,
            "ltgd_rank": ltgd_rank,
        }
    return out


def _self_test() -> None:
    params = {"ltgd_top_n_for_longtou": 5, "ltgd_mid_max_rank": 15}
    fupan = [
        {"代码": "000001", "板数": "4"},
        {"代码": "000002", "板数": "2"},
        {"代码": "000003", "板数": "1"},
        {"代码": "000004", "板数": "1"},
        {"代码": "000005", "板数": "3"},
    ]
    ltgd = [
        {"代码": "000001", "排名": "2", "周期": "5日"},
        {"代码": "000003", "排名": "10", "周期": "5日"},
        {"代码": "000005", "排名": "30", "周期": "5日"},
    ]
    out = compute_longtou_status(["000001", "000002", "000003", "000004", "000005", "999999"], fupan, ltgd, params)
    assert out["000001"]["label"] == "confirmed_longtou", out["000001"]
    assert out["000002"]["label"] == "mid_position", out["000002"]
    assert out["000003"]["label"] == "mid_position", out["000003"]  # ltgd 前15 + 板数≥1
    assert out["000004"]["label"] == "follower", out["000004"]      # 板数=1 不在 ltgd
    assert out["000005"]["label"] == "follower", out["000005"]      # 板数=3 但 ltgd 排名 30 > 15 且 < 5 → 后面逻辑 板数>=3但没 confirmed → 接 follower
    assert out["999999"]["label"] == "none"
    print("longtou_status _self_test passed")


if __name__ == "__main__":
    _self_test()
