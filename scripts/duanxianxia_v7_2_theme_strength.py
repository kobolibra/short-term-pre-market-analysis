"""
duanxianxia_v7_2_theme_strength.py — v7.2 theme_strength_t0 (0-100).

Updated formula:

Use the already-downloaded T0 home.kaipan.plate.summary more fully:
- T0 plate strength percentile
- T0 main inflow percentile
- T0 limit-up count percentile
- T0 subplate fallback
- T-1 plate inertia

Default:
  theme_strength_t0 =
    0.50*T0 strength + 0.25*T0 inflow + 0.15*T0 limit-up count + 0.10*T-1 strength

Broad themes (一季报增长 / 业绩增长 / 预增 ...) are capped:
- default cap: broad_theme_cap (60)
- if theme is fading or streak_days >= broad_theme_fading_streak (5):
  cap drops to broad_theme_fading_cap (55)
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


def _parse_money_to_wan(v: Any) -> Optional[float]:
    if v in (None, "", "-"):
        return None
    s = str(v).replace(",", "").strip()
    if not s or s == "-":
        return None
    try:
        if "亿" in s:
            return float(s.replace("亿", "").strip()) * 10000.0
        if "万" in s:
            return float(s.replace("万", "").strip())
        return float(s)
    except Exception:
        return None


def _split_subplates(v: Any) -> List[str]:
    if v in (None, "", "-"):
        return []
    raw_parts: List[Any]
    if isinstance(v, list):
        raw_parts = []
        for item in v:
            if isinstance(item, dict):
                name = item.get("子题材名称") or item.get("子标签名称") or item.get("name") or item.get("名称")
                raw_parts.append(name)
            else:
                raw_parts.append(item)
    else:
        text = str(v)
        for sep in ("|", "、", "/", "，", ",", ";", "；", "\n"):
            text = text.replace(sep, ",")
        raw_parts = text.split(",")
    out: List[str] = []
    seen = set()
    for p in raw_parts:
        token = str(p or "").strip()
        if not token:
            continue
        for prefix in ("子题材名称:", "子标签名称:", "name:", "名称:"):
            if token.startswith(prefix):
                token = token[len(prefix):].strip()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _pct_by_field(items: List[Dict[str, Any]], value_key: str) -> Dict[str, float]:
    valid = [x for x in items if x.get(value_key) is not None]
    if not valid:
        return {}
    valid.sort(key=lambda x: float(x.get(value_key) or 0.0), reverse=True)
    n = len(valid)
    out: Dict[str, float] = {}
    for i, item in enumerate(valid):
        name = item["name"]
        if name not in out:
            out[name] = round((n - i) / n * 100.0, 2)
    return out


def _plate_metrics_index(kaipan_t0_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build main-plate and subplate lookup from home.kaipan.plate.summary.

    Returned values are 0-100 percentiles. Subplates inherit parent metrics and
    are marked with matched_via=subplate so caller can apply a mild downgrade.
    """
    rows = kaipan_t0_rows or []
    items: List[Dict[str, Any]] = []
    for r in rows:
        name = str(r.get("主标签名称") or r.get("plate_name") or r.get("name") or "").strip()
        if not name:
            continue
        strength = _to_float(
            r.get("板块强度原值")
            or r.get("板块强度")
            or r.get("strength_value")
            or r.get("强度")
        )
        inflow = None
        if r.get("主力流入原值") is not None:
            inflow = _parse_money_to_wan(r.get("主力流入原值"))
        elif r.get("主力流入真实金额") is not None:
            raw_yuan = _to_float(r.get("主力流入真实金额"))
            inflow = (raw_yuan / 10000.0) if raw_yuan is not None else None
        if inflow is None:
            inflow = _parse_money_to_wan(r.get("主力流入"))
        limitup_count = _to_float(r.get("涨停数量") or r.get("limitup_count") or r.get("涨停数"))
        subplates = _split_subplates(r.get("子标签列表") or r.get("subplates") or r.get("子标签"))
        items.append({
            "name": name,
            "strength": strength,
            "inflow": inflow,
            "limitup_count": limitup_count,
            "subplates": subplates,
        })

    if not items:
        return {}

    strength_pct = _pct_by_field(items, "strength")
    inflow_pct = _pct_by_field(items, "inflow")
    limitup_pct = _pct_by_field(items, "limitup_count")

    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        name = item["name"]
        obj = {
            "plate": name,
            "matched_via": "main",
            "strength_pct": strength_pct.get(name, 0.0),
            "inflow_pct": inflow_pct.get(name, 0.0),
            "limitup_count_pct": limitup_pct.get(name, 0.0),
            "subplates": item.get("subplates") or [],
        }
        out[name] = obj
        for sp in item.get("subplates") or []:
            if sp and sp not in out:
                child = dict(obj)
                child["matched_via"] = "subplate"
                child["parent_plate"] = name
                out[sp] = child
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
    plate_index: Dict[str, Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p = params or {}
    # Prefer enriched v7.2 weights when present. If they are absent, use the
    # documented enriched defaults directly instead of silently falling back to
    # the old v7.1-style 0.70/0.30 formula.
    w_yday = float(p.get("theme_yesterday_weight", p.get("theme_inertia_weight", 0.10)))
    w_strength = float(p.get("theme_t0_strength_weight", 0.50))
    w_inflow = float(p.get("theme_t0_inflow_weight", 0.25))
    w_limitup = float(p.get("theme_t0_limitup_weight", 0.15))

    no_theme_base = float(p.get("no_theme_base", 20))
    broad_theme_names = set(p.get("broad_theme_names") or [])
    subplate_multiplier = float(p.get("theme_subplate_multiplier", 0.85))
    broad_theme_cap = float(p.get("broad_theme_cap", 60))
    broad_theme_fading_cap = float(p.get("broad_theme_fading_cap", 55))
    broad_theme_fading_streak = int(p.get("broad_theme_fading_streak", 5))

    if not matched_themes:
        return {
            "best_theme": None,
            "theme_strength_t0": round(no_theme_base, 2),
            "t0_plate_pct": 0.0,
            "t0_inflow_pct": 0.0,
            "t0_limitup_count_pct": 0.0,
            "yesterday_plate_rank": 0.0,
            "theme_history_label": "none",
            "streak_days": 0,
            "matched_via": "miss",
            "matched_plate": "",
            "no_theme_base_applied": True,
            "broad_theme_cap_applied": None,
        }

    best_theme: Optional[str] = None
    best_t0_pct = 0.0
    best_inflow_pct = 0.0
    best_limitup_pct = 0.0
    best_yday = 0.0
    best_label = "none"
    best_matched_via = "miss"
    best_plate = ""
    best_streak = 0
    best_score = -1.0

    for t in matched_themes or []:
        hist = theme_history.get(t) or {}
        canonical = hist.get("theme_canonical") or t

        plate_obj = plate_index.get(t) or (plate_index.get(str(canonical)) if canonical else None) or {}
        t0_pct = float(plate_obj.get("strength_pct") or 0.0)
        inflow_pct = float(plate_obj.get("inflow_pct") or 0.0)
        limitup_pct = float(plate_obj.get("limitup_count_pct") or 0.0)
        matched_via = str(plate_obj.get("matched_via") or "miss")
        plate_name = str(plate_obj.get("parent_plate") or plate_obj.get("plate") or "")

        ind_obj = industry_t1.get(t) or (industry_t1.get(str(canonical)) if canonical else {}) or {}
        yday_pct = _industry_pct_strength_0_100(ind_obj)

        history_label = str(hist.get("label") or "none")
        streak = int(hist.get("streak_days") or 0)
        t0_component = w_strength * t0_pct + w_inflow * inflow_pct + w_limitup * limitup_pct
        if matched_via == "subplate":
            t0_component *= subplate_multiplier
        score = t0_component + w_yday * yday_pct
        if score > best_score:
            best_score = score
            best_theme = t
            best_t0_pct = t0_pct
            best_inflow_pct = inflow_pct
            best_limitup_pct = limitup_pct
            best_yday = yday_pct
            best_label = history_label
            best_matched_via = matched_via
            best_plate = plate_name
            best_streak = streak

    matched_but_zero = False
    if best_theme is None or best_score <= 0 or (best_t0_pct == 0.0 and best_yday == 0.0 and best_inflow_pct == 0.0 and best_limitup_pct == 0.0):
        best_score = no_theme_base
        matched_but_zero = True

    broad_cap_applied: Optional[str] = None
    broad_key = best_plate or best_theme
    if broad_key and broad_key in broad_theme_names and not matched_but_zero:
        is_fading = (
            best_label == "fading"
            or best_streak >= broad_theme_fading_streak
        )
        if is_fading and best_score > broad_theme_fading_cap:
            best_score = broad_theme_fading_cap
            broad_cap_applied = "fading"
        elif not is_fading and best_score > broad_theme_cap:
            best_score = broad_theme_cap
            broad_cap_applied = "broad"

    return {
        "best_theme": best_theme,
        "theme_strength_t0": round(best_score, 2),
        "t0_plate_pct": round(best_t0_pct, 2),
        "t0_inflow_pct": round(best_inflow_pct, 2),
        "t0_limitup_count_pct": round(best_limitup_pct, 2),
        "yesterday_plate_rank": round(best_yday, 2),
        "matched_via": best_matched_via,
        "matched_plate": best_plate,
        "theme_history_label": best_label,
        "streak_days": best_streak,
        "no_theme_base_applied": matched_but_zero,
        "broad_theme_cap_applied": broad_cap_applied,
    }


def compute_theme_strengths(
    candidates: List[Dict[str, Any]],
    kaipan_t0_rows: List[Dict[str, Any]],
    theme_history: Dict[str, Dict[str, Any]],
    industry_t1: Dict[str, Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    plate_index = _plate_metrics_index(kaipan_t0_rows)
    out: Dict[str, Dict[str, Any]] = {}
    for c in candidates or []:
        code = str(c.get("code") or "").strip()
        if not code or code in out:
            continue
        themes = c.get("matched_themes") or []
        out[code] = compute_theme_strength_t0(
            themes, industry_t1, theme_history, plate_index, params,
        )
    return out


def _self_test() -> None:
    candidates = [
        {"code": "000001", "matched_themes": ["算力", "AI算力"]},
        {"code": "000002", "matched_themes": ["猪肉"]},
        {"code": "000003", "matched_themes": []},
        {"code": "000004", "matched_themes": ["一季报增长"]},
        {"code": "000005", "matched_themes": ["某个陈旧主题"]},
    ]
    kaipan = [
        {"主标签名称": "算力", "板块强度原值": "85", "主力流入原值": "50000", "涨停数量": "5", "子标签列表": "AI算力,液冷服务器"},
        {"主标签名称": "半导体", "板块强度原值": "70", "主力流入原值": "30000", "涨停数量": "3"},
        {"主标签名称": "猪肉", "板块强度原值": "20", "主力流入原值": "1000", "涨停数量": "1"},
        {"主标签名称": "一季报增长", "板块强度原值": "3360", "主力流入原值": "80000", "涨停数量": "8"},
    ]
    theme_history = {
        "算力": {"streak_days": 2, "label": "day2_main", "theme_canonical": "算力"},
        "AI算力": {"streak_days": 2, "label": "day2_main", "theme_canonical": "算力"},
        "猪肉": {"streak_days": 0, "label": "fresh", "theme_canonical": "猪肉"},
        "一季报增长": {"streak_days": 10, "label": "fading", "theme_canonical": "一季报增长"},
    }
    industry = {"算力": {"pct_strength": 0.80}, "猪肉": {"pct_strength": 0.10}}
    params = {
        "theme_t0_weight": 0.70,
        "theme_yesterday_weight": 0.30,
        "theme_t0_strength_weight": 0.50,
        "theme_t0_inflow_weight": 0.25,
        "theme_t0_limitup_weight": 0.15,
        "theme_subplate_multiplier": 0.85,
        "no_theme_base": 20,
        "broad_theme_names": ["一季报增长", "业绩增长", "预增"],
        "broad_theme_cap": 60,
        "broad_theme_fading_cap": 55,
        "broad_theme_fading_streak": 5,
    }
    out = compute_theme_strengths(candidates, kaipan, theme_history, industry, params)
    assert out["000001"]["theme_strength_t0"] > out["000002"]["theme_strength_t0"], out
    assert out["000001"]["yesterday_plate_rank"] == 80.0, out
    assert out["000001"]["matched_via"] in {"main", "subplate"}, out["000001"]
    assert out["000003"]["theme_strength_t0"] == 20.0
    assert out["000003"]["no_theme_base_applied"] is True
    assert out["000004"]["theme_strength_t0"] == 55.0, out["000004"]
    assert out["000004"]["broad_theme_cap_applied"] == "fading", out["000004"]
    assert out["000005"]["theme_strength_t0"] == 20.0, out["000005"]
    assert out["000005"]["no_theme_base_applied"] is True, out["000005"]
    print("theme_strength _self_test passed", out)


if __name__ == "__main__":
    _self_test()
