"""
duanxianxia_v7_1_tech_profile.py — v7.1 技术形态标签

新增 churn_high_volume: 放量滞涨,即 vol_ratio > 2 且 pctChg < 2%。
这种票除纯竞价异动 D 外应被 setup_engine 降级/过滤。
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except Exception:
        return None


def _normalize_code(s: str) -> str:
    s = str(s or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    if len(s) >= 6:
        s = s[-6:]
    return s


def compute_tech_profile(candidate_codes: List[str], dailyline_dict: Dict[str, List[Dict[str, Any]]], params: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    lookback = int(params.get("tech_profile_lookback_volume_days", 20))
    vol_min = float(params.get("tech_profile_volume_ratio_min", 0.5))
    churn_vol_min = float(params.get("tech_profile_churn_volume_ratio_min", 2.0))
    churn_pct_max = float(params.get("tech_profile_churn_pct_chg_max", 2.0))
    norm_dict = {_normalize_code(k): v for k, v in (dailyline_dict or {}).items()}
    out: Dict[str, Dict[str, Any]] = {}

    for raw in candidate_codes or []:
        code = _normalize_code(raw)
        if not code or code in out:
            continue
        rows = norm_dict.get(code, [])
        if len(rows) < lookback:
            out[code] = {"label": "unknown", "reason": f"insufficient_dailyline ({len(rows)} < {lookback})", "ma20": None, "vol_ma20": None, "vol_ratio_t1": None, "distance_to_ma20": None, "pct_to_recent_high": None, "pct_chg_t1": None}
            continue
        window = rows[-lookback:]
        closes = [_to_float(r.get("close")) for r in window]
        highs = [_to_float(r.get("high")) for r in window]
        vols = [_to_float(r.get("volume")) for r in window]
        if any(x is None for x in closes) or any(x is None for x in vols):
            out[code] = {"label": "unknown", "reason": "unparseable_dailyline", "ma20": None, "vol_ma20": None, "vol_ratio_t1": None, "distance_to_ma20": None, "pct_to_recent_high": None, "pct_chg_t1": None}
            continue
        close_vals = [float(x) for x in closes if x is not None]
        high_vals = [float(x) for x in highs if x is not None]
        vol_vals = [float(x) for x in vols if x is not None]
        ma20 = sum(close_vals) / len(close_vals)
        vol_ma20 = sum(vol_vals) / len(vol_vals)
        last_close = close_vals[-1]
        last_vol = vol_vals[-1]
        recent_high = max(high_vals) if high_vals else last_close
        dist = (last_close - ma20) / ma20 if ma20 > 0 else 0.0
        vol_ratio = last_vol / vol_ma20 if vol_ma20 > 0 else None
        pct_to_high = (recent_high - last_close) / recent_high if recent_high > 0 else 0.0
        pct_chg = _to_float(window[-1].get("pctChg"))

        if vol_ratio is not None and pct_chg is not None and vol_ratio > churn_vol_min and pct_chg < churn_pct_max:
            label = "churn_high_volume"
        elif dist > 0.05 and vol_ratio is not None and vol_ratio > 1.5:
            label = "breakout"
        elif 0.0 < dist <= 0.05:
            label = "healthy"
        elif -0.05 <= dist <= 0.0:
            label = "cooling"
        else:
            label = "weak"
        if label != "churn_high_volume" and vol_ratio is not None and vol_ratio < vol_min:
            label += ":low_vol"
        out[code] = {"label": label, "ma20": ma20, "vol_ma20": vol_ma20, "vol_ratio_t1": vol_ratio, "distance_to_ma20": dist, "pct_to_recent_high": pct_to_high, "pct_chg_t1": pct_chg}
    return out


if __name__ == "__main__":
    rows = [{"close": 10, "high": 10.2, "volume": 100, "pctChg": 1.0} for _ in range(19)] + [{"close": 10.1, "high": 10.3, "volume": 300, "pctChg": 1.5}]
    out = compute_tech_profile(["000001"], {"000001": rows}, {"tech_profile_lookback_volume_days": 20})
    assert out["000001"]["label"] == "churn_high_volume", out
    print("tech_profile _self_test passed")
