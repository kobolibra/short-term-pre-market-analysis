"""
duanxianxia_v7_2_theme_strength.py — conservative T0 theme strength.

Current production rule (2026-05 discussion):
- Use only T0 `home.kaipan.plate.summary` 板块强度.
- Do NOT use T0 主力流入: in live premarket it is often 0/unstable.
- Do NOT use T0 涨停数量: avoid over-complexity and early-session noise.
- Do NOT distinguish main-plate vs sub-plate matches in scoring.
- Do NOT infer aliases here. Candidate tags must exactly match a plate tag after
  light normalization and delimiter splitting.

A sub-tag inherits its parent plate's strength, but `matched_via` is reported as
`plate_tag` for both main and sub tags so downstream logic does not treat them
differently.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


_SPLIT_RE = re.compile(r"[|、,/，；;\n]+")


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
            return None
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return None


def _norm_tag(v: Any) -> str:
    text = str(v or "").strip()
    text = re.sub(r"\s+", "", text)
    return text


def _split_tags(v: Any) -> List[str]:
    if v in (None, "", "-"):
        return []
    raw: List[Any]
    if isinstance(v, list):
        raw = v
    else:
        raw = _SPLIT_RE.split(str(v))
    out: List[str] = []
    seen = set()
    for item in raw:
        if isinstance(item, dict):
            token = _norm_tag(item.get("子题材名称") or item.get("子标签名称") or item.get("name") or item.get("名称"))
        else:
            token = _norm_tag(item)
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _strength_percentiles(items: List[Tuple[str, float]]) -> Dict[str, float]:
    valid = [(name, val) for name, val in items if name and val is not None]
    if not valid:
        return {}
    valid.sort(key=lambda x: x[1], reverse=True)
    n = len(valid)
    out: Dict[str, float] = {}
    for i, (name, _val) in enumerate(valid):
        out.setdefault(name, round((n - i) / n * 100.0, 2))
    return out


def _plate_metrics_index(kaipan_t0_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    rows = kaipan_t0_rows or []
    main_items: List[Tuple[str, float]] = []
    parsed: List[Dict[str, Any]] = []
    for r in rows:
        name = _norm_tag(r.get("主标签名称") or r.get("plate_name") or r.get("name") or r.get("名称"))
        if not name:
            continue
        strength = _to_float(r.get("板块强度原值") or r.get("板块强度") or r.get("strength_value") or r.get("强度"))
        if strength is None:
            continue
        subplates = _split_tags(r.get("子标签列表") or r.get("subplates") or r.get("子标签"))
        parsed.append({"name": name, "strength": strength, "subplates": subplates})
        main_items.append((name, strength))

    strength_pct = _strength_percentiles(main_items)
    out: Dict[str, Dict[str, Any]] = {}
    for item in parsed:
        parent = item["name"]
        obj = {
            "plate": parent,
            "parent_plate": parent,
            "matched_via": "plate_tag",
            "strength_pct": strength_pct.get(parent, 0.0),
            "strength_raw": item["strength"],
            "subplates": item.get("subplates") or [],
            # Kept for output compatibility; intentionally unused.
            "inflow_pct": 0.0,
            "limitup_count_pct": 0.0,
        }
        out[parent] = obj
        for sp in item.get("subplates") or []:
            if sp and sp not in out:
                child = dict(obj)
                child["tag"] = sp
                out[sp] = child
    return out


def compute_theme_strength_t0(
    matched_themes: List[str],
    industry_t1: Dict[str, Dict[str, Any]],
    theme_history: Dict[str, Dict[str, Any]],
    plate_index: Dict[str, Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    del industry_t1, theme_history  # v7.2 conservative mode intentionally ignores T-1 theme context.
    p = params or {}
    no_theme_base = float(p.get("no_theme_base", 20))

    normalized_input: List[str] = []
    seen = set()
    for t in matched_themes or []:
        for token in _split_tags(t):
            if token and token not in seen:
                seen.add(token)
                normalized_input.append(token)

    best_theme: Optional[str] = None
    best_score = -1.0
    best_obj: Dict[str, Any] = {}
    matched_tags: List[str] = []
    for t in normalized_input:
        obj = plate_index.get(t)
        if not obj:
            continue
        matched_tags.append(t)
        score = float(obj.get("strength_pct") or 0.0)
        if score > best_score:
            best_score = score
            best_theme = t
            best_obj = obj

    if best_theme is None:
        return {
            "best_theme": None,
            "theme_strength_t0": round(no_theme_base, 2),
            "theme_matched": False,
            "matched_tags": [],
            "t0_plate_pct": 0.0,
            "t0_plate_strength_raw": 0.0,
            "t0_inflow_pct": 0.0,
            "t0_limitup_count_pct": 0.0,
            "yesterday_plate_rank": 0.0,
            "theme_history_label": "ignored",
            "streak_days": 0,
            "matched_via": "miss",
            "matched_plate": "",
            "no_theme_base_applied": True,
            "broad_theme_cap_applied": None,
            "ignored_fields": ["主力流入", "涨停数量", "T-1题材惯性"],
        }

    return {
        "best_theme": best_theme,
        "theme_strength_t0": round(max(0.0, min(best_score, 100.0)), 2),
        "theme_matched": True,
        "matched_tags": matched_tags,
        "t0_plate_pct": round(float(best_obj.get("strength_pct") or 0.0), 2),
        "t0_plate_strength_raw": round(float(best_obj.get("strength_raw") or 0.0), 2),
        "t0_inflow_pct": 0.0,
        "t0_limitup_count_pct": 0.0,
        "yesterday_plate_rank": 0.0,
        "matched_via": "plate_tag",
        "matched_plate": str(best_obj.get("parent_plate") or best_obj.get("plate") or ""),
        "theme_history_label": "ignored",
        "streak_days": 0,
        "no_theme_base_applied": False,
        "broad_theme_cap_applied": None,
        "ignored_fields": ["主力流入", "涨停数量", "T-1题材惯性"],
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
        out[code] = compute_theme_strength_t0(themes, industry_t1, theme_history, plate_index, params)
    return out


def _self_test() -> None:
    candidates = [
        {"code": "000001", "matched_themes": ["液冷", "AI算力"]},
        {"code": "000002", "matched_themes": ["燃气轮机"]},
        {"code": "000003", "matched_themes": []},
        {"code": "000004", "matched_themes": ["IDC"]},  # no alias guessing
    ]
    kaipan = [
        {"主标签名称": "一季报增长", "板块强度原值": "4980", "主力流入原值": "0", "涨停数量": "2"},
        {"主标签名称": "燃气轮机", "板块强度原值": "2595", "主力流入原值": "0", "涨停数量": "3"},
        {"主标签名称": "算力", "板块强度原值": "2059", "主力流入原值": "0", "涨停数量": "8", "子标签列表": "算力租赁、数据中心、液冷"},
    ]
    out = compute_theme_strengths(candidates, kaipan, {}, {}, {"no_theme_base": 20})
    assert out["000001"]["theme_matched"] is True, out["000001"]
    assert out["000001"]["matched_plate"] == "算力", out["000001"]
    assert out["000002"]["theme_strength_t0"] > out["000001"]["theme_strength_t0"], out
    assert out["000003"]["theme_strength_t0"] == 20.0
    assert out["000004"]["theme_matched"] is False, out["000004"]
    assert "主力流入" in out["000001"]["ignored_fields"]
    print("theme_strength conservative _self_test passed")


if __name__ == "__main__":
    _self_test()
