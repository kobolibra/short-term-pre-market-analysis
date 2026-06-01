"""
duanxianxia_v9_market_env.py — 全量 qxlive 市场环境层 (home.qxlive.top_metrics)

保留并使用全部 12 个顶部指标:
  QX 情绪 / ZT 涨停家数 / DT 跌停家数 / KQXY 亏钱效应 / HSLN 主力流入 /
  LBGD 连板高度 / SZ 上涨家数 / XD 下跌家数 / PB 今日封板率 /
  ZTBX 昨涨停表现 / LBBX 昨连板表现 / PBBX 沪深5分钟量能

分组:
  risk_breadth   = QX / DT / KQXY / SZ / XD   (市场风险与广度)
  board_relay    = ZT / LBGD / PB             (打板与接力环境)
  yesterday_fb   = ZTBX / LBBX                (昨日涨停/连板反馈)
  money_bg       = HSLN                       (市场级资金流入背景)
  volume_env     = PBBX                       (早盘量能环境)

之前被 regime 显式忽略的 HSLN/PB/PBBX 在这里全部保留输出,只是默认低权重。
regime 标签仍复用 duanxianxia_v7_1_regime.compute_regime(稳定信号),
本模块额外给 market_env_score 与各分组诊断,供 edge 的背景/风险因子使用。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

try:
    from duanxianxia_v7_1_regime import compute_regime as _compute_regime
except Exception:  # pragma: no cover - regime 可缺省
    _compute_regime = None

# 全部 12 个指标的 key -> (中文标签集合, 分组)
METRIC_DEFS = {
    "QX":   ({"情绪", "情绪指标"}, "risk_breadth"),
    "ZT":   ({"涨停家数"}, "board_relay"),
    "DT":   ({"跌停家数"}, "risk_breadth"),
    "KQXY": ({"亏钱效应"}, "risk_breadth"),
    "HSLN": ({"主力流入"}, "money_bg"),
    "LBGD": ({"连板高度"}, "board_relay"),
    "SZ":   ({"上涨家数"}, "risk_breadth"),
    "XD":   ({"下跌家数"}, "risk_breadth"),
    "PB":   ({"今日封板率"}, "board_relay"),
    "ZTBX": ({"昨涨停表现", "涨停表现"}, "yesterday_fb"),
    "LBBX": ({"昨连板表现", "连板表现"}, "yesterday_fb"),
    "PBBX": ({"沪深5分钟量能", "5分钟量能", "量能"}, "volume_env"),
}


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
            return None
        return float(str(v).replace("%", "").replace("亿", "").replace(",", "").strip())
    except Exception:
        return None


def _extract(rows: List[Dict[str, Any]], metric_key: str, labels: set) -> Optional[float]:
    for row in rows or []:
        key = str(row.get("metric_key") or "").strip()
        label = str(row.get("metric_label") or row.get("指标名称") or row.get("name") or row.get("名称") or "").strip()
        if key == metric_key or label in labels:
            for vk in ("raw_chart_tail_value", "raw_value", "value", "指标值", "值"):
                if vk in row:
                    val = _to_float(row.get(vk))
                    if val is not None:
                        return val
    return None


def _extract_all(rows: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    return {k: _extract(rows, k, labels) for k, (labels, _g) in METRIC_DEFS.items()}


def _grouped(metrics: Dict[str, Optional[float]]) -> Dict[str, Dict[str, Optional[float]]]:
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for k, (_labels, group) in METRIC_DEFS.items():
        out.setdefault(group, {})[k] = metrics.get(k)
    return out


def _breadth_ratio(sz: Optional[float], xd: Optional[float]) -> Optional[float]:
    if sz is None or xd is None:
        return None
    total = sz + xd
    return (sz / total) if total > 0 else None


def _market_env_score(m: Dict[str, Optional[float]], params: Dict[str, Any]) -> float:
    """0-100 市场风险偏好分:越高=越适合进攻。全部 12 指标都参与,缺失项按中性处理。"""
    p = params or {}
    score, weight = 0.0, 0.0

    def add(val: Optional[float], lo: float, hi: float, w: float, invert: bool = False) -> None:
        nonlocal score, weight
        if val is None:
            return
        norm = max(0.0, min(1.0, (val - lo) / (hi - lo))) if hi > lo else 0.0
        if invert:
            norm = 1.0 - norm
        score += norm * 100.0 * w
        weight += w

    add(m.get("QX"), 20, 80, 0.22)                       # 情绪
    add(m.get("KQXY"), 0, 30, 0.14, invert=True)         # 亏钱效应(越高越差)
    add(m.get("DT"), 0, 40, 0.10, invert=True)           # 跌停家数(越多越差)
    add(_breadth_ratio(m.get("SZ"), m.get("XD")) and _breadth_ratio(m.get("SZ"), m.get("XD")) * 100.0, 25, 70, 0.14)  # 广度
    add(m.get("LBGD"), 2, 9, 0.12)                       # 连板高度
    add(m.get("ZT"), 20, 100, 0.08)                      # 涨停家数
    add(m.get("LBBX"), -5, 8, 0.10)                      # 昨连板表现
    add(m.get("ZTBX"), -5, 8, 0.06)                      # 昨涨停表现
    add(m.get("HSLN"), -200, 300, 0.04)                  # 主力流入背景(低权重)
    return round(score / weight, 2) if weight > 0 else 50.0


def compute_market_env(
    qxlive_t0_rows: List[Dict[str, Any]],
    qxlive_t1_rows: List[Dict[str, Any]],
    qxlive_t2_rows: Optional[List[Dict[str, Any]]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p = params or {}
    m_t0 = _extract_all(qxlive_t0_rows)
    m_t1 = _extract_all(qxlive_t1_rows)
    m_t2 = _extract_all(qxlive_t2_rows) if qxlive_t2_rows else {}

    regime = None
    if _compute_regime is not None:
        try:
            regime = _compute_regime({"rows": qxlive_t0_rows or []}, {"rows": qxlive_t1_rows or []}, p)
        except Exception as e:  # pragma: no cover
            regime = {"label": "normal", "reason": f"regime_error:{e}"}

    env_score = _market_env_score(m_t0, p)
    breadth = _breadth_ratio(m_t0.get("SZ"), m_t0.get("XD"))

    # 风险标志(进入 edge 的风险因子)
    risk_flags: List[str] = []
    if m_t0.get("KQXY") is not None and m_t0["KQXY"] >= float(p.get("env_kqxy_risk", 12)):
        risk_flags.append("high_loss_effect")
    if m_t0.get("DT") is not None and m_t0["DT"] >= float(p.get("env_dt_risk", 25)):
        risk_flags.append("many_limit_down")
    if breadth is not None and breadth <= float(p.get("env_breadth_risk", 0.30)):
        risk_flags.append("weak_breadth")
    if m_t0.get("LBBX") is not None and m_t0["LBBX"] <= float(p.get("env_lbbx_risk", -3)):
        risk_flags.append("relay_deteriorating")  # 接力环境恶化

    return {
        "regime": regime,
        "market_env_score": env_score,
        "breadth_ratio_t0": round(breadth, 4) if breadth is not None else None,
        "risk_flags": risk_flags,
        "metrics_t0": m_t0,
        "metrics_t1": m_t1,
        "metrics_t2": m_t2,
        "grouped_t0": _grouped(m_t0),
        "grouped_t1": _grouped(m_t1),
        # 显式记录:本版不再丢弃任何指标
        "retained_metrics": sorted(METRIC_DEFS.keys()),
        "previously_ignored_now_retained": ["HSLN", "PB", "PBBX", "ZT", "LBGD"],
    }


def _self_test() -> None:
    t0 = [
        {"metric_key": "QX", "value": "70"}, {"metric_key": "KQXY", "value": "5"},
        {"metric_key": "DT", "value": "8"}, {"metric_key": "SZ", "value": "3500"},
        {"metric_key": "XD", "value": "1500"}, {"metric_key": "LBGD", "value": "6"},
        {"metric_key": "HSLN", "value": "120"}, {"metric_key": "PB", "value": "80%"},
        {"metric_key": "PBBX", "value": "1.1"},
    ]
    env = compute_market_env(t0, [], None, {})
    assert env["metrics_t0"]["HSLN"] == 120.0   # 不再被丢弃
    assert env["metrics_t0"]["PBBX"] == 1.1
    assert env["market_env_score"] > 50
    assert "HSLN" in env["retained_metrics"]
    print("v9_market_env _self_test passed")


if __name__ == "__main__":
    _self_test()
