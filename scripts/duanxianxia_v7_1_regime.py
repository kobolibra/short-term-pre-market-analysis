"""
duanxianxia_v7_1_regime.py — market regime classifier.

For v7.2 premarket use, this module deliberately uses only stable, available
T0 qxlive signals:
- QX / 情绪指标
- DT / 跌停家数
- KQXY / 亏钱效应
- SZ + XD / 涨跌家数 breadth
- LBBX / 昨连板表现
- ZTBX / 昨涨停表现

It explicitly does NOT use:
- HSLN / 主力流入: premarket value is often 0/unstable and user confirmed it
  should not be used.
- PB / 今日封板率: early-session value often has no stable meaning.
- PBBX: in home.qxlive.top_metrics this is 沪深5分钟量能, not 晋级率.

The function still accepts legacy rows with 指标名称/指标值 for backward
compatibility, but metric_key/metric_label/value is the preferred real schema.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


_IGNORE_KEYS = {"HSLN", "PB", "PBBX"}


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
            return None
        return float(str(v).replace("%", "").replace("亿", "").replace(",", "").strip())
    except Exception:
        return None


def _extract_by_key_or_label(rows: List[Dict[str, Any]], metric_key: str, labels: set[str]) -> Optional[float]:
    if not rows:
        return None
    for row in rows:
        key = str(row.get("metric_key") or "").strip()
        label = str(row.get("metric_label") or row.get("指标名称") or row.get("name") or row.get("名称") or "").strip()
        if key in _IGNORE_KEYS:
            continue
        if key == metric_key or label in labels:
            for value_key in ("raw_chart_tail_value", "raw_value", "value", "指标值", "值"):
                if value_key in row:
                    val = _to_float(row.get(value_key))
                    if val is not None:
                        return val
    return None


def _breadth_ratio(sz: Optional[float], xd: Optional[float]) -> Optional[float]:
    if sz is None or xd is None:
        return None
    total = sz + xd
    if total <= 0:
        return None
    return sz / total


def compute_regime(
    qxlive_t0_top: Dict[str, Any],
    qxlive_t1_top: Dict[str, Any],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """Return regime dict using stable qxlive metrics only."""
    t0_rows = (qxlive_t0_top or {}).get("rows") or []
    t1_rows = (qxlive_t1_top or {}).get("rows") or []

    qx_t0 = _extract_by_key_or_label(t0_rows, "QX", {"情绪", "情绪指标"})
    qx_t1 = _extract_by_key_or_label(t1_rows, "QX", {"情绪", "情绪指标"})
    dt_t0 = _extract_by_key_or_label(t0_rows, "DT", {"跌停家数"})
    kqxy_t0 = _extract_by_key_or_label(t0_rows, "KQXY", {"亏钱效应"})
    sz_t0 = _extract_by_key_or_label(t0_rows, "SZ", {"上涨家数"})
    xd_t0 = _extract_by_key_or_label(t0_rows, "XD", {"下跌家数"})
    lbbx_t0 = _extract_by_key_or_label(t0_rows, "LBBX", {"昨连板表现", "连板表现"})
    ztbx_t0 = _extract_by_key_or_label(t0_rows, "ZTBX", {"昨涨停表现", "涨停表现"})
    lbbx_t1 = _extract_by_key_or_label(t1_rows, "LBBX", {"昨连板表现", "连板表现"})
    breadth_t0 = _breadth_ratio(sz_t0, xd_t0)

    qx_hot = float(params.get("regime_hot_qx_min", 65))
    lbbx_hot = float(params.get("regime_hot_lbbx_min", 5))
    qx_cold = float(params.get("regime_cold_qx_max", 30))
    dt_cold = float(params.get("regime_cold_dt_min", 20))
    kqxy_cold = float(params.get("regime_cold_kqxy_min", 10))
    breadth_cold = float(params.get("regime_cold_breadth_max", 0.28))
    qx_warm_today = float(params.get("regime_warming_qx_today_min", 35))
    qx_warm_yest = float(params.get("regime_warming_qx_yesterday_max", 30))

    label = "normal"
    reason = ""

    if qx_t1 is not None and qx_t1 <= qx_warm_yest and qx_t0 is not None and qx_t0 >= qx_warm_today:
        label = "cold_to_warming"
        reason = f"qx {qx_t1}→{qx_t0}"
    elif qx_t0 is not None and (qx_t0 >= qx_hot or (lbbx_t0 is not None and lbbx_t0 >= lbbx_hot)):
        label = "hot"
        reason = f"qx={qx_t0}, lbbx={lbbx_t0}"
    elif (
        (qx_t0 is not None and qx_t0 <= qx_cold)
        or (dt_t0 is not None and dt_t0 >= dt_cold)
        or (kqxy_t0 is not None and kqxy_t0 >= kqxy_cold)
        or (breadth_t0 is not None and breadth_t0 <= breadth_cold)
    ):
        label = "cold"
        reason = f"qx={qx_t0}, dt={dt_t0}, kqxy={kqxy_t0}, breadth={breadth_t0}"
    else:
        reason = f"qx={qx_t0}, lbbx={lbbx_t0}, ztbx={ztbx_t0}, breadth={breadth_t0}"

    return {
        "label": label,
        "reason": reason,
        "qx_t0": qx_t0,
        "qx_t1": qx_t1,
        "dt_t0": dt_t0,
        "kqxy_t0": kqxy_t0,
        "sz_t0": sz_t0,
        "xd_t0": xd_t0,
        "breadth_t0": breadth_t0,
        "lbbx_t0": lbbx_t0,
        "lbbx_t1": lbbx_t1,
        "ztbx_t0": ztbx_t0,
        "promo_t0": None,
        "ignored_metrics": sorted(_IGNORE_KEYS),
    }


def _self_test() -> None:
    params = {
        "regime_warming_qx_today_min": 35,
        "regime_warming_qx_yesterday_max": 30,
        "regime_hot_qx_min": 65,
        "regime_hot_lbbx_min": 5,
        "regime_cold_qx_max": 30,
        "regime_cold_breadth_max": 0.28,
    }
    t0 = {"rows": [{"metric_key": "QX", "value": "40"}, {"metric_key": "HSLN", "value": "0"}]}
    t1 = {"rows": [{"metric_key": "QX", "value": "25"}]}
    assert compute_regime(t0, t1, params)["label"] == "cold_to_warming"

    t0 = {"rows": [{"metric_key": "QX", "value": "70"}, {"metric_key": "PB", "value": "100%"}]}
    assert compute_regime(t0, {"rows": []}, params)["label"] == "hot"

    t0 = {"rows": [{"metric_key": "QX", "value": "40"}, {"metric_key": "SZ", "value": "1000"}, {"metric_key": "XD", "value": "5000"}]}
    assert compute_regime(t0, {"rows": []}, params)["label"] == "cold"

    print("regime conservative _self_test passed")


if __name__ == "__main__":
    _self_test()
