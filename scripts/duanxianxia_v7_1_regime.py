"""
duanxianxia_v7_1_regime.py — v7.1 市场 regime 判定

输入:
  - qxlive_t0_top: 今日 09:30 前 top_metrics(rows + meta)
  - qxlive_t1_top: T-1 末场 top_metrics
  - params
从 rows 中提取 “指标名称” 在 情绪/连板宽度/涨停宽度/平宽/连跌宽度/跌停宽度/晋升率/破板率 中的赋值。

labels:
  - cold_to_warming: 昨QX ≤ 30 且 今 QX ≥ 35 且 昨 LBBX ≤ 0 且 今 LBBX ≥ 2  (冷转暑)
  - hot:         今 QX ≥ 70 或 今 LBBX ≥ 5
  - hot_to_downgrading: 今晋升率 ≤ 0.20 且 昨 QX ≥ 60         (热后转凉)
  - cold:        今 QX ≤ 30
  - normal:      其他
依据:premarket_scoring.yaml v6.3 market_regime + v7.1 spec params
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


METRIC_NAME_KEYS = ("指标名称", "name", "名称")
METRIC_VALUE_KEYS = ("指标值", "value", "值")


def _extract_metric(rows: List[Dict[str, Any]], target_name: str) -> Optional[float]:
    """从 top_metrics rows 中按名称匹配提取指标值(float 转换失败返回 None)。"""
    if not rows:
        return None
    for row in rows:
        name = ""
        for k in METRIC_NAME_KEYS:
            v = row.get(k)
            if v not in (None, ""):
                name = str(v).strip()
                break
        if name != target_name:
            continue
        for k in METRIC_VALUE_KEYS:
            v = row.get(k)
            if v in (None, ""):
                continue
            try:
                # 可能带 “%” 后缀
                s = str(v).strip()
                if s.endswith("%"):
                    return float(s[:-1]) / 100.0
                return float(s)
            except Exception:
                return None
    return None


def compute_regime(
    qxlive_t0_top: Dict[str, Any],
    qxlive_t1_top: Dict[str, Any],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """返回 regime 字典。qxlive_*_top 为 {rows: [...], meta: {...}} 格式。"""
    t0_rows = (qxlive_t0_top or {}).get("rows") or []
    t1_rows = (qxlive_t1_top or {}).get("rows") or []

    qx_t0 = _extract_metric(t0_rows, "情绪")
    qx_t1 = _extract_metric(t1_rows, "情绪")
    lbbx_t0 = _extract_metric(t0_rows, "连板宽度")
    lbbx_t1 = _extract_metric(t1_rows, "连板宽度")
    promo_t0 = _extract_metric(t0_rows, "晋升率")

    qx_warm_today = float(params.get("regime_warming_qx_today_min", 35))
    qx_warm_yest = float(params.get("regime_warming_qx_yesterday_max", 30))
    lbbx_warm_today = float(params.get("regime_warming_lbbx_today_min", 2))
    lbbx_warm_yest = float(params.get("regime_warming_lbbx_yesterday_max", 0))
    promo_max = float(params.get("regime_downgrade_promo_rate_max", 0.20))

    label = "normal"
    reason = ""

    # cold_to_warming 连路(昨 cold, 今 明显 暑)
    if (
        qx_t1 is not None and qx_t1 <= qx_warm_yest
        and qx_t0 is not None and qx_t0 >= qx_warm_today
        and lbbx_t1 is not None and lbbx_t1 <= lbbx_warm_yest
        and lbbx_t0 is not None and lbbx_t0 >= lbbx_warm_today
    ):
        label = "cold_to_warming"
        reason = f"qx {qx_t1}→{qx_t0}, lbbx {lbbx_t1}→{lbbx_t0}"
    elif qx_t0 is not None and (qx_t0 >= 70 or (lbbx_t0 is not None and lbbx_t0 >= 5)):
        label = "hot"
        reason = f"qx={qx_t0}, lbbx={lbbx_t0}"
    elif (
        promo_t0 is not None and promo_t0 <= promo_max
        and qx_t1 is not None and qx_t1 >= 60
    ):
        label = "hot_to_downgrading"
        reason = f"promo {promo_t0}, qx_t1 {qx_t1}"
    elif qx_t0 is not None and qx_t0 <= 30:
        label = "cold"
        reason = f"qx={qx_t0}"
    else:
        reason = f"qx={qx_t0}, lbbx={lbbx_t0}, promo={promo_t0}"

    return {
        "label": label,
        "reason": reason,
        "qx_t0": qx_t0,
        "qx_t1": qx_t1,
        "lbbx_t0": lbbx_t0,
        "lbbx_t1": lbbx_t1,
        "promo_t0": promo_t0,
    }


def _self_test() -> None:
    params = {
        "regime_warming_qx_today_min": 35,
        "regime_warming_qx_yesterday_max": 30,
        "regime_warming_lbbx_today_min": 2,
        "regime_warming_lbbx_yesterday_max": 0,
        "regime_downgrade_promo_rate_max": 0.20,
    }
    # cold_to_warming
    t0 = {"rows": [{"指标名称": "情绪", "指标值": "40"}, {"指标名称": "连板宽度", "指标值": "3"}]}
    t1 = {"rows": [{"指标名称": "情绪", "指标值": "25"}, {"指标名称": "连板宽度", "指标值": "0"}]}
    r = compute_regime(t0, t1, params)
    assert r["label"] == "cold_to_warming", r

    # hot
    t0 = {"rows": [{"指标名称": "情绪", "指标值": "75"}]}
    t1 = {"rows": [{"指标名称": "情绪", "指标值": "60"}]}
    r = compute_regime(t0, t1, params)
    assert r["label"] == "hot", r

    # cold
    t0 = {"rows": [{"指标名称": "情绪", "指标值": "20"}]}
    t1 = {"rows": [{"指标名称": "情绪", "指标值": "40"}]}
    r = compute_regime(t0, t1, params)
    assert r["label"] == "cold", r

    # hot_to_downgrading: 昨 热 (qx_t1 ≥ 60), 今晋升率 ≤ 0.20
    t0 = {"rows": [{"指标名称": "情绪", "指标值": "45"}, {"指标名称": "晋升率", "指标值": "15%"}]}
    t1 = {"rows": [{"指标名称": "情绪", "指标值": "68"}]}
    r = compute_regime(t0, t1, params)
    assert r["label"] == "hot_to_downgrading", r
    assert abs(r["promo_t0"] - 0.15) < 1e-9

    # normal
    t0 = {"rows": [{"指标名称": "情绪", "指标值": "50"}]}
    t1 = {"rows": [{"指标名称": "情绪", "指标值": "50"}]}
    r = compute_regime(t0, t1, params)
    assert r["label"] == "normal", r

    print("regime _self_test passed")


if __name__ == "__main__":
    _self_test()
