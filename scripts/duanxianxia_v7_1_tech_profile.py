"""
duanxianxia_v7_1_tech_profile.py — v7.1 技术形态标签

输入: candidate_codes, dailyline_dict {code: List[csv row dicts]}, params
使用 last 20 行计算 ma20 / vol_ma20 / distance_to_ma20 / vol_ratio_t1 / pct_to_recent_high
阈值:
  - tech_profile_volume_ratio_min (0.5):vol_ratio_t1 低于此值走 weak 后缀
  - tech_profile_lookback_volume_days (20):MA 窗口

labels:
  - breakout: distance_to_ma20 > 0.05 且 vol_ratio_t1 > 1.5
  - healthy:  distance_to_ma20 ∈ (0, 0.05] (价位在 MA20 上方快走靠)
  - cooling:  distance_to_ma20 ∈ [-0.05, 0]
  - weak:     distance_to_ma20 < -0.05
  - unknown:  数据不足 lookback (< lookback_volume_days)
如果 vol_ratio_t1 低于 min,附加 :low_vol 后缀,该股今日准入需额外谨慎。
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


def compute_tech_profile(
    candidate_codes: List[str],
    dailyline_dict: Dict[str, List[Dict[str, Any]]],
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """为每个候选股计算 tech_profile。dailyline_dict key 可以带或不带 sz./sh. 前缀。"""
    lookback = int(params.get("tech_profile_lookback_volume_days", 20))
    vol_min = float(params.get("tech_profile_volume_ratio_min", 0.5))

    # 标准化 dailyline_dict key
    norm_dict: Dict[str, List[Dict[str, Any]]] = {}
    for k, v in (dailyline_dict or {}).items():
        norm_dict[_normalize_code(k)] = v

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = _normalize_code(raw)
        if not code or code in out:
            continue

        rows = norm_dict.get(code, [])
        if not rows or len(rows) < lookback:
            out[code] = {
                "label": "unknown",
                "reason": f"insufficient_dailyline ({len(rows)} < {lookback})",
                "ma20": None,
                "vol_ma20": None,
                "vol_ratio_t1": None,
                "distance_to_ma20": None,
                "pct_to_recent_high": None,
            }
            continue

        window = rows[-lookback:]
        closes = [_to_float(r.get("close")) for r in window]
        highs = [_to_float(r.get("high")) for r in window]
        volumes = [_to_float(r.get("volume")) for r in window]

        valid_closes = [x for x in closes if x is not None]
        valid_highs = [x for x in highs if x is not None]
        valid_volumes = [x for x in volumes if x is not None]

        if len(valid_closes) < lookback or len(valid_volumes) < lookback:
            out[code] = {
                "label": "unknown",
                "reason": "unparseable_dailyline",
                "ma20": None,
                "vol_ma20": None,
                "vol_ratio_t1": None,
                "distance_to_ma20": None,
                "pct_to_recent_high": None,
            }
            continue

        ma20 = sum(valid_closes) / len(valid_closes)
        vol_ma20 = sum(valid_volumes) / len(valid_volumes)
        last_close = valid_closes[-1]
        last_volume = valid_volumes[-1]
        recent_high = max(valid_highs) if valid_highs else last_close

        distance_to_ma20 = (last_close - ma20) / ma20 if ma20 > 0 else 0.0
        vol_ratio_t1 = last_volume / vol_ma20 if vol_ma20 > 0 else None
        pct_to_recent_high = (recent_high - last_close) / recent_high if recent_high > 0 else 0.0

        if distance_to_ma20 > 0.05 and vol_ratio_t1 is not None and vol_ratio_t1 > 1.5:
            base = "breakout"
        elif 0.0 < distance_to_ma20 <= 0.05:
            base = "healthy"
        elif -0.05 <= distance_to_ma20 <= 0.0:
            base = "cooling"
        else:
            base = "weak"

        suffix = ""
        if vol_ratio_t1 is not None and vol_ratio_t1 < vol_min:
            suffix = ":low_vol"
        label = base + suffix

        out[code] = {
            "label": label,
            "ma20": ma20,
            "vol_ma20": vol_ma20,
            "vol_ratio_t1": vol_ratio_t1,
            "distance_to_ma20": distance_to_ma20,
            "pct_to_recent_high": pct_to_recent_high,
        }
    return out


def _self_test() -> None:
    params = {"tech_profile_lookback_volume_days": 20, "tech_profile_volume_ratio_min": 0.5}

    def _mk(close_seq, vol_seq, high_seq=None):
        if high_seq is None:
            high_seq = [c * 1.01 for c in close_seq]
        return [{"close": c, "volume": v, "high": h} for c, v, h in zip(close_seq, vol_seq, high_seq)]

    # breakout: 最后价 > ma20 * 1.05, 量能 > vol_ma20 * 1.5
    closes_breakout = [10.0] * 19 + [11.0]
    vols_breakout = [100.0] * 19 + [200.0]
    # healthy: 最后价 在 ma20+0..5%, 体量适中
    closes_healthy = [10.0] * 19 + [10.2]
    vols_healthy = [100.0] * 20
    # cooling: 最后价 在 ma20-5..0%
    closes_cooling = [10.0] * 19 + [9.7]
    vols_cooling = [100.0] * 20
    # weak: 最后价 < ma20*0.95
    closes_weak = [10.0] * 19 + [9.0]
    vols_weak = [100.0] * 20
    # weak + low_vol
    closes_weak_lv = [10.0] * 19 + [9.0]
    vols_weak_lv = [100.0] * 19 + [30.0]  # 30/(均近100) = 0.3 < 0.5
    # unknown: 不足 20 天
    closes_short = [10.0] * 5
    vols_short = [100.0] * 5

    dailyline = {
        "sz.000001": _mk(closes_breakout, vols_breakout),
        "000002": _mk(closes_healthy, vols_healthy),
        "sh.000003": _mk(closes_cooling, vols_cooling),
        "000004": _mk(closes_weak, vols_weak),
        "000005": _mk(closes_weak_lv, vols_weak_lv),
        "000006": _mk(closes_short, vols_short),
    }
    out = compute_tech_profile(["000001", "000002", "000003", "000004", "000005", "000006", "999999"], dailyline, params)

    assert out["000001"]["label"] == "breakout", out["000001"]
    assert out["000002"]["label"] == "healthy", out["000002"]
    assert out["000003"]["label"] == "cooling", out["000003"]
    assert out["000004"]["label"] == "weak", out["000004"]
    assert out["000005"]["label"] == "weak:low_vol", out["000005"]
    assert out["000006"]["label"] == "unknown", out["000006"]
    assert out["999999"]["label"] == "unknown"
    print("tech_profile _self_test passed")


if __name__ == "__main__":
    _self_test()
