"""
duanxianxia_v7_2_theme_strength.py — v7.2 theme_strength_t0 (0-100).

Replaces the bogus 'T0 net inflow intent' concept with a clean two-layer mix:

  theme_strength_t0 = w_t0 * t0_plate_strength_pct + w_inertia * yesterday_inertia

where:
  - t0_plate_strength_pct: candidate's strongest matched plate's percentile
    among today's home.kaipan.plate.summary plates (0-100)
  - yesterday_inertia: from theme_history streak (0-100)
      streak=0 → 0; streak=1 → 33; streak=2 → 66;
      streak == full_streak → 100;
      streak > full_streak (fading) → 50
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
    """Build {plate_name: percentile_0_100} from T0 kaipan summary."""
    rows = kaipan_t0_rows or []
    parsed: List[Dict[str, Any]] = []
    for r in rows:
        name = str(r.get("主标签名称") or r.get("plate_name") or r.get("name") or "").strip()
        strength = _to_float(
            r.get("板块强度")
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
        # rank 1 → 100; rank n → ~ (1/n) * 100
        pct = (n - i) / n * 100.0
        # don't downgrade duplicates with same strength
        if item["name"] in out:
            continue
        out[item["name"]] = round(pct, 2)
    return out


def _streak_to_inertia(streak: int, full_streak: int = 3) -> float:
    if streak <= 0:
        return 0.0
    if streak > full_streak:
        return 50.0  # fading, partial inertia
    if streak == full_streak:
        return 100.0
    return round(streak / full_streak * 100.0, 2)


def compute_theme_strength_t0(
    matched_themes: List[str],
    industry_t1: Dict[str, Dict[str, Any]],
    theme_history: Dict[str, Dict[str, Any]],
    plate_pct_index: Dict[str, float],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """For a candidate's matched themes, return the strongest one with breakdown."""
    p = params or {}
    w_t0 = float(p.get("theme_t0_weight", 0.70))
    w_inertia = float(p.get("theme_inertia_weight", 0.30))
    full_streak = int(p.get("theme_inertia_full_streak", 3))

    best_theme: Optional[str] = None
    best_t0_pct = 0.0
    best_inertia = 0.0
    best_label = "none"
    best_streak = 0
    best_score = -1.0

    for t in matched_themes or []:
        t_pct = plate_pct_index.get(t)
        if t_pct is None:
            canonical = (theme_history.get(t) or {}).get("theme_canonical")
            if canonical:
                t_pct = plate_pct_index.get(canonical)
        t_pct = float(t_pct) if t_pct is not None else 0.0

        history_obj = theme_history.get(t) or {}
        streak = int(history_obj.get("streak_days") or 0)
        history_label = str(history_obj.get("label") or "none")
        inertia = _streak_to_inertia(streak, full_streak)

        score = w_t0 * t_pct + w_inertia * inertia
        if score > best_score:
            best_score = score
            best_theme = t
            best_t0_pct = t_pct
            best_inertia = inertia
            best_label = history_label
            best_streak = streak

    if best_theme is None:
        best_score = 0.0

    return {
        "best_theme": best_theme,
        "theme_strength_t0": round(best_score, 2),
        "t0_plate_pct": round(best_t0_pct, 2),
        "yesterday_inertia": round(best_inertia, 2),
        "theme_history_label": best_label,
        "streak_days": best_streak,
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
        {"主标签名称": "算力", "板块强度": "85"},
        {"主标签名称": "半导体", "板块强度": "70"},
        {"主标签名称": "猪肉", "板块强度": "20"},
    ]
    theme_history = {
        "算力": {"streak_days": 2, "label": "day2_main", "theme_canonical": "算力"},
        "AI算力": {"streak_days": 2, "label": "day2_main", "theme_canonical": "算力"},
        "猪肉": {"streak_days": 0, "label": "fresh", "theme_canonical": "猪肉"},
    }
    out = compute_theme_strengths(candidates, kaipan, theme_history, {}, {})
    assert out["000001"]["theme_strength_t0"] > out["000002"]["theme_strength_t0"], out
    assert out["000003"]["theme_strength_t0"] == 0.0
    print("theme_strength _self_test passed", out)


if __name__ == "__main__":
    _self_test()
