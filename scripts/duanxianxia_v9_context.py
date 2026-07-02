"""
duanxianxia_v9_context.py — T-1/历史/复盘上下文层

把 loader 已经加载但被 v9 output 丢掉的历史数据系统化为每个候选的 context_detail:
  - 个股 T-1 资金流 (cashflow today/3day/5day/10day) + 连续性
  - T-1 复盘题材 (review.fupan.plate)
  - 龙头高度 (review.ltgd.range)
  - 涨停池反馈 (home.ztpool): 是否昨日涨停/连板
定位:辅助/背景/风险,不作主因子;但完整保留+输出。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _money_to_wan(v: Any) -> Optional[float]:
    if v in (None, "", "-"):
        return None
    s = str(v).replace(",", "").strip()
    try:
        if "亿" in s:
            return float(s.replace("亿", "")) * 10000.0
        if "万" in s:
            return float(s.replace("万", ""))
        return float(s)
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
            return None
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return None


def _index_by_code(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        code = _norm_code(r.get("code") or r.get("代码"))
        if code and code not in out:
            out[code] = r
    return out


NET_KEYS = ["main_net_inflow_wan", "主力净流入", "main_net_inflow", "net_amount", "净额"]


def _net_inflow(row: Optional[Dict[str, Any]]) -> Optional[float]:
    if not row:
        return None
    for k in NET_KEYS:
        if k in row:
            v = _money_to_wan(row.get(k))
            if v is not None:
                return v
    return None


def compute_stock_context(
    candidate_codes: List[str],
    *,
    cashflow_today: List[Dict[str, Any]],
    cashflow_3day: List[Dict[str, Any]],
    cashflow_5day: List[Dict[str, Any]],
    cashflow_10day: List[Dict[str, Any]],
    fupan_t1: List[Dict[str, Any]],
    ltgd_5day_t1: List[Dict[str, Any]],
    ztpool_t1: List[Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    p = params or {}
    cf_today = _index_by_code(cashflow_today)
    cf_3 = _index_by_code(cashflow_3day)
    cf_5 = _index_by_code(cashflow_5day)
    cf_10 = _index_by_code(cashflow_10day)
    fupan = _index_by_code(fupan_t1)
    ztpool = _index_by_code(ztpool_t1)
    ltgd = _index_by_code(ltgd_5day_t1)   # Task 0108: 个股级龙头梯队查表(review.ltgd.range)

    # 龙头高度:取最高连板高度作为市场背景(所有候选共享)
    # ⚠️ 0107 实证 review.ltgd.range 无 高度/height 字段,此项对该表恒为 None;
    #    保留不动(市场级口径),个股级信号见下方 t1_ltgd_* (Task 0108 新增,独立字段)。
    longtou_height = None
    for r in ltgd_5day_t1 or []:
        h = _to_float(r.get("高度") or r.get("连板高度") or r.get("height"))
        if h is not None:
            longtou_height = max(longtou_height or 0.0, h)

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = _norm_code(raw)
        if not code or code in out:
            continue
        n_today = _net_inflow(cf_today.get(code))
        n_3 = _net_inflow(cf_3.get(code))
        n_5 = _net_inflow(cf_5.get(code))
        n_10 = _net_inflow(cf_10.get(code))
        # 资金连续性:多周期净流入同号为正的个数
        signs = [x for x in (n_today, n_3, n_5, n_10) if x is not None]
        positive_streak = sum(1 for x in signs if x > 0)
        cashflow_continuity = (
            "strong" if positive_streak >= 3 else
            "medium" if positive_streak == 2 else
            "weak" if signs else "unknown"
        )
        fupan_row = fupan.get(code) or {}
        zt_row = ztpool.get(code) or {}
        zt_board = str(zt_row.get("连板标签") or zt_row.get("board_label") or "").strip()

        # --- Task 0108: 个股级龙头梯队(review.ltgd.range)派生 ---
        # 不依赖当日涨停池(跌停股会掉出涨停池),补 0105 闸门盲区。
        # schema(0107 实证): 代码/名称/周期/排名/区间涨幅/日期区间/板块/板块顺序/概念/概念键
        ltgd_row = ltgd.get(code) or {}
        t1_ltgd_leader = bool(ltgd_row)
        t1_ltgd_rank = None
        t1_ltgd_range_gain_pct = None
        if ltgd_row:
            _rk = _to_float(ltgd_row.get("排名") or ltgd_row.get("rank"))
            t1_ltgd_rank = int(_rk) if _rk is not None else None
            t1_ltgd_range_gain_pct = _to_float(
                ltgd_row.get("区间涨幅") or ltgd_row.get("range_gain_pct") or ltgd_row.get("range_gain")
            )

        out[code] = {
            "cashflow_net_today_wan": n_today,
            "cashflow_net_3day_wan": n_3,
            "cashflow_net_5day_wan": n_5,
            "cashflow_net_10day_wan": n_10,
            "cashflow_positive_streak": positive_streak,
            "cashflow_continuity": cashflow_continuity,
            "t1_fupan_theme": str(fupan_row.get("题材") or fupan_row.get("板块") or fupan_row.get("theme") or "").strip(),
            "t1_fupan_present": bool(fupan_row),
            "t1_in_ztpool": bool(zt_row),
            "t1_zt_board_label": zt_board,
            "market_longtou_height": longtou_height,   # 市场级背景,所有候选一致
            "t1_ltgd_leader": t1_ltgd_leader,            # Task 0108: 在5日龙头梯队
            "t1_ltgd_rank": t1_ltgd_rank,                # 梯队排名
            "t1_ltgd_range_gain_pct": t1_ltgd_range_gain_pct,  # 区间涨幅%(解析"45%"->45.0)
            "cashflow_raw_today": cf_today.get(code),  # 原样留底
            "ztpool_raw": zt_row or None,
            "ltgd_raw": ltgd_row or None,                # Task 0108: 龙头梯队原样留底
        }
    return out


def _self_test() -> None:
    ctx = compute_stock_context(
        ["600000", "000001", "002674"],
        cashflow_today=[{"code": "600000", "主力净流入": "5000万"}],
        cashflow_3day=[{"code": "600000", "主力净流入": "1.2亿"}],
        cashflow_5day=[{"code": "600000", "主力净流入": "2亿"}],
        cashflow_10day=[{"code": "600000", "主力净流入": "-1亿"}],
        fupan_t1=[{"code": "600000", "题材": "算力"}],
        ltgd_5day_t1=[
            {"高度": "6"},
            {"代码": "002674", "名称": "兴业科技", "排名": 17, "区间涨幅": "45%"},
        ],
        ztpool_t1=[{"code": "600000", "连板标签": "2板"}],
        params={},
    )
    assert ctx["600000"]["cashflow_positive_streak"] == 3
    assert ctx["600000"]["cashflow_continuity"] == "strong"
    assert ctx["600000"]["t1_in_ztpool"] is True
    assert ctx["600000"]["market_longtou_height"] == 6.0
    assert ctx["000001"]["cashflow_continuity"] == "unknown"
    # Task 0108: 002674 在龙头梯队(区间涨幅45%)但跌停掉出当日涨停池
    assert ctx["002674"]["t1_ltgd_leader"] is True
    assert ctx["002674"]["t1_ltgd_rank"] == 17
    assert ctx["002674"]["t1_ltgd_range_gain_pct"] == 45.0
    assert ctx["002674"]["t1_in_ztpool"] is False
    assert ctx["002674"]["market_longtou_height"] == 6.0  # 市场级不受个股级新增影响
    print("v9_context _self_test passed")


if __name__ == "__main__":
    _self_test()
