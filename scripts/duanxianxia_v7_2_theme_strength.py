"""
duanxianxia_v7_2_theme_strength.py — v7.2 theme_strength_t0 (0-100).

Final hardened formula:

  if no matched theme:
      theme_strength_t0 = no_theme_base  # default 20
  else:
      theme_strength_t0 = 0.70 * t0_plate_strength_pct + 0.30 * yesterday_plate_rank

where:
  - t0_plate_strength_pct: candidate's strongest matched plate's percentile
    among today's home.kaipan.plate.summary plates (0-100)
  - yesterday_plate_rank: T-1 plate strength percentile from the v7.1
    industry_t1 label module (`pct_strength * 100`). Missing -> 0.

The no-theme base respects independent / iceberg movers without letting them
outrank genuine plate-resonance candidates by theme alone.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
            return None
        return float(str(v).replace("%", "").strip())
    except Exception:
        return None


def _plate_strength_index(kaipan_t0_rows: List[Dict[str, Any]]) -> Dict[str, float]:
    rows = kaipan_t0_rows or []
    parsed: List[Dict[str, Any]] = []
    for r in rows:
        name = str(r.get("主标签名称") or r.get("plate_name") or r.get("name") or "").strip()
        strength = _to_float(
            r.get("板块强度原值")
            or r.get("板块强度")
            or r.get("strength_value")
            or r.get("强度")
        )
        if name and strength is not None:
            parsed.append({"name": name, "strength": strength})
    if not parsed:
        return {}
    parsed.sort(key=lambda x: x["strength"], reverse=True)
    n = len(parsed)
    out: Dict[str, float] = {}
    for i, item in enumerate(parsed):
        pct = (n - i) / n * 100.0
        if item["name"] not in out:
            out[item["name"]] = round(pct, 2)
    return out


def _industry_pct_strength_0_100(obj: Dict[str, Any]) -> float:
    v = _to_float((obj or {}).get("pct_strength"))
    if v is None:
        v = _to_float((obj or {}).get("yesterday_plate_rank"))
    if v is None:
        return 0.0
    return round(v * 100.0, 2) if 0.0 <= v <= 1.0 else round(max(0.0, min(v, 100.0)), 2)


def compute_theme_strength_t0(
    matched_themes: List[str],
    industry_t1: Dict[str, Dict[str, Any]],
    theme_history: Dict[str, Dict[str, Any]],
    plate_pct_index: Dict[str, float],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p = params or {}
    w_t0 = float(p.get("theme_t0_weight", 0.70))
    w_yday = float(p.get("theme_yesterday_weight", p.get("theme_inertia_weight", 0.30)))
    no_theme_base = float(p.get("no_theme_base", 20))

    if not matched_themes:
        return {
            "best_theme": None,
            "theme_strength_t0": round(no_theme_base, 2),
            "t0_plate_pct": 0.0,
            "yesterday_plate_rank": 0.0,
            "theme_history_label": "none",
            "streak_days": 0,
            "no_theme_base_applied": True,
        }

    best_theme: Optional[str] = None
    best_t0_pct = 0.0
    best_yday = 0.0
    best_label = "none"
    best_streak = 0
    best_score = -1.0

    for t in matched_themes or []:
        hist = theme_history.get(t) or {}
        canonical = hist.get("theme_canonical") or t

        t0_pct = plate_pct_index.get(t)
        if t0_pct is None and canonical:
            t0_pct = plate_pct_index.get(str(canonical))
        t0_pct = float(t0_pct) if t0_pct is not None else 0.0

        ind_obj = industry_t1.get(t) or (industry_t1.get(str(canonical)) if canonical else {}) or {}
        yday_pct = _industry_pct_strength_0_100(ind_obj)

        history_label = str(hist.get("label") or "none")
        streak = int(hist.get("streak_days") or 0)
        score = w_t0 * t0_pct + w_yday * yday_pct
        if score > best_score:
            best_score = score
            best_theme = t
            best_t0_pct = t0_pct
            best_yday = yday_pct
            best_label = history_label
            best_streak = streak

    if best_theme is None:
        best_score = no_theme_base

    return {
        "best_theme": best_theme,
        "theme_strength_t0": round(best_score, 2),
        "t0_plate_pct": round(best_t0_pct, 2),
        "yesterday_plate_rank": round(best_yday, 2),
        "theme_history_label": best_label,
        "streak_days": best_streak,
        "no_theme_base_applied": False,
    }


def compute_theme_strengths(
    candidates: List[Dict[str, Any]],
    kaipan_t0_rows: List[Dict[str, Any]],
    theme_history: Dict[str, Dict[str, Any]],
    industry_t1: Dict[str, Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    plate_pct_index = _plate_strength_index(kaipan_t0_rows)
    out: Dict[str, Dict[str, Any]] = {}
    for c in candidates or []:
        code = str(c.get("code") or "").strip()
        if not code or code in out:
            continue
        themes = c.get("matched_themes") or []
        out[code] = compute_theme_strength_t0(
            themes, industry_t1, theme_history, plate_pct_index, params,
        )
    return out


def _self_test() -> None:
    candidates = [
        {"code": "000001", "matched_themes": ["算力", "AI算力"]},
        {"code": "000002", "matched_themes": ["猪肉"]},
        {"code": "000003", "matched_themes": []},
    ]
    kaipan = [
        {"主标签名称": "算力", "板块强度原值": "85"},
        {"主标签名称": "半导体", "板块强度原值": "70"},
        {"主标签名称": "猪肉", "板块强度原值": "20"},
    ]
    theme_history = {
        "算力": {"streak_days": 2, "label": "day2_main", "theme_canonical": "算力"},
        "AI算力": {"streak_days": 2, "label": "day2_main", "theme_canonical": "算力"},
        "猪肉": {"streak_days": 0, "label": "fresh", "theme_canonical": "猪肉"},
    }
    industry = {"算力": {"pct_strength": 0.80}, "猪肉": {"pct_strength": 0.10}}
    out = compute_theme_strengths(candidates, kaipan, theme_history, industry, {"theme_t0_weight": 0.70, "theme_yesterday_weight": 0.30, "no_theme_base": 20})
    assert out["000001"]["theme_strength_t0"] > out["000002"]["theme_strength_t0"], out
    assert out["000001"]["yesterday_plate_rank"] == 80.0, out
    assert out["000003"]["theme_strength_t0"] == 20.0
    assert out["000003"]["no_theme_base_applied"] is True
    print("theme_strength _self_test passed", out)


if __name__ == "__main__":
    _self_test()
