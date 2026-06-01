"""
duanxianxia_v9_theme_strength.py — 全字段题材/板块强度 (home.kaipan.plate.summary)

不再只用板块强度。本版把已下载的板块字段全部用上:
  - 板块强度(原值)  -> 题材强度主因子(percentile)
  - 板块主力流入    -> 题材资金确认(percentile)
  - 涨停数量        -> 题材扩散/赚钱效应确认(percentile)
  - 子标签列表      -> 个股题材匹配精度(主标签命中 vs 子标签命中)
签名与 v7.2 版兼容,可直接替换 import。
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


def _norm_tag(v: Any) -> str:
    return re.sub(r"\s+", "", str(v or "").strip())


def _split_tags(v: Any) -> List[str]:
    if v in (None, "", "-"):
        return []
    raw = v if isinstance(v, list) else _SPLIT_RE.split(str(v))
    out, seen = [], set()
    for item in raw:
        if isinstance(item, dict):
            token = _norm_tag(item.get("子题材名称") or item.get("子标签名称") or item.get("name") or item.get("名称"))
        else:
            token = _norm_tag(item)
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _percentiles(items: List[Tuple[str, float]]) -> Dict[str, float]:
    valid = [(n, v) for n, v in items if n and v is not None]
    if not valid:
        return {}
    valid.sort(key=lambda x: x[1], reverse=True)
    n = len(valid)
    out: Dict[str, float] = {}
    for i, (name, _v) in enumerate(valid):
        out.setdefault(name, round((n - i) / n * 100.0, 2))
    return out


def _ranks(items: List[Tuple[str, float]]) -> Dict[str, int]:
    valid = [(n, v) for n, v in items if n and v is not None]
    valid.sort(key=lambda x: x[1], reverse=True)
    return {name: i + 1 for i, (name, _v) in enumerate(valid)}


def _plate_metrics_index(kaipan_t0_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    rows = kaipan_t0_rows or []
    parsed: List[Dict[str, Any]] = []
    strength_items, inflow_items, limitup_items = [], [], []
    for r in rows:
        name = _norm_tag(r.get("主标签名称") or r.get("plate_name") or r.get("name") or r.get("名称"))
        if not name:
            continue
        strength = _to_float(r.get("板块强度原值") or r.get("板块强度") or r.get("strength_value") or r.get("强度"))
        inflow = _money_to_wan(r.get("主力流入真实金额") or r.get("主力流入原值") or r.get("主力流入") or r.get("inflow_real_wan") or r.get("inflow"))
        limitup = _to_float(r.get("涨停数量") or r.get("limitup_count") or r.get("涨停数"))
        subplates = _split_tags(r.get("子标签列表") or r.get("subplates") or r.get("子标签"))
        code = str(r.get("主标签代码") or r.get("plate_code") or "").strip()
        parsed.append({"name": name, "plate_code": code, "strength": strength,
                       "inflow_wan": inflow, "limitup": limitup, "subplates": subplates})
        strength_items.append((name, strength))
        inflow_items.append((name, inflow))
        limitup_items.append((name, limitup))

    strength_pct = _percentiles(strength_items)
    inflow_pct = _percentiles(inflow_items)
    limitup_pct = _percentiles(limitup_items)
    strength_rank = _ranks(strength_items)
    inflow_rank = _ranks(inflow_items)

    out: Dict[str, Dict[str, Any]] = {}
    for item in parsed:
        parent = item["name"]
        obj = {
            "plate": parent, "parent_plate": parent, "plate_code": item["plate_code"],
            "matched_via": "main_tag", "matched_level": "main",
            "strength_pct": strength_pct.get(parent, 0.0),
            "strength_raw": item["strength"],
            "inflow_wan": item["inflow_wan"],
            "inflow_pct": inflow_pct.get(parent, 0.0),
            "limitup_count": item["limitup"],
            "limitup_count_pct": limitup_pct.get(parent, 0.0),
            "strength_rank": strength_rank.get(parent, 0),
            "inflow_rank": inflow_rank.get(parent, 0),
            "subplates": item["subplates"],
        }
        out[parent] = obj
        for sp in item["subplates"]:
            if sp and sp not in out:
                child = dict(obj)
                child.update(tag=sp, matched_via="sub_tag", matched_level="sub")
                out[sp] = child
    return out


def compute_theme_strength_t0(
    matched_themes: List[str],
    industry_t1: Dict[str, Dict[str, Any]],
    theme_history: Dict[str, Dict[str, Any]],
    plate_index: Dict[str, Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    p = params or {}
    no_theme_base = float(p.get("no_theme_base", 20))
    w_strength = float(p.get("theme_w_strength", 0.55))
    w_inflow = float(p.get("theme_w_inflow", 0.25))
    w_limitup = float(p.get("theme_w_limitup", 0.20))
    sub_precision_bonus = float(p.get("theme_sub_match_bonus", 6))  # 子标签命中=更精准

    normalized_input: List[str] = []
    seen = set()
    for t in matched_themes or []:
        for token in _split_tags(t):
            if token and token not in seen:
                seen.add(token)
                normalized_input.append(token)

    best = None
    best_score = -1.0
    matched_tags: List[str] = []
    for t in normalized_input:
        obj = plate_index.get(t)
        if not obj:
            continue
        matched_tags.append(t)
        composite = (
            w_strength * float(obj.get("strength_pct") or 0.0)
            + w_inflow * float(obj.get("inflow_pct") or 0.0)
            + w_limitup * float(obj.get("limitup_count_pct") or 0.0)
        )
        if obj.get("matched_level") == "sub":
            composite += sub_precision_bonus
        if composite > best_score:
            best_score = composite
            best = obj

    # 题材资金惯性(T-1)作为低权重背景,不喧宾夺主
    history_label = "none"
    if best is not None and theme_history:
        h = theme_history.get(best.get("parent_plate") or "") or {}
        history_label = str(h.get("label") or "none")
        streak = int(h.get("streak_days") or 0)
        if streak >= int(p.get("theme_streak_strong_min", 2)):
            best_score += float(p.get("theme_streak_bonus", 4))

    if best is None:
        return {
            "best_theme": None, "theme_strength_t0": round(no_theme_base, 2),
            "theme_matched": False, "matched_tags": [], "matched_plate": "",
            "matched_via": "miss", "matched_level": "none",
            "t0_plate_pct": 0.0, "t0_plate_strength_raw": 0.0,
            "t0_inflow_pct": 0.0, "t0_plate_inflow_wan": None,
            "t0_limitup_count": None, "t0_limitup_count_pct": 0.0,
            "plate_strength_rank": 0, "plate_inflow_rank": 0,
            "subplates": [], "theme_history_label": history_label,
            "no_theme_base_applied": True,
        }

    return {
        "best_theme": best.get("tag") or best.get("parent_plate"),
        "theme_strength_t0": round(max(0.0, min(best_score, 100.0)), 2),
        "theme_matched": True,
        "matched_tags": matched_tags,
        "matched_plate": str(best.get("parent_plate") or best.get("plate") or ""),
        "matched_via": str(best.get("matched_via") or "main_tag"),
        "matched_level": str(best.get("matched_level") or "main"),
        "t0_plate_pct": round(float(best.get("strength_pct") or 0.0), 2),
        "t0_plate_strength_raw": round(float(best.get("strength_raw") or 0.0), 2),
        "t0_inflow_pct": round(float(best.get("inflow_pct") or 0.0), 2),
        "t0_plate_inflow_wan": best.get("inflow_wan"),
        "t0_limitup_count": best.get("limitup_count"),
        "t0_limitup_count_pct": round(float(best.get("limitup_count_pct") or 0.0), 2),
        "plate_strength_rank": int(best.get("strength_rank") or 0),
        "plate_inflow_rank": int(best.get("inflow_rank") or 0),
        "subplates": best.get("subplates") or [],
        "theme_history_label": history_label,
        "no_theme_base_applied": False,
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
        out[code] = compute_theme_strength_t0(
            c.get("matched_themes") or [], industry_t1, theme_history, plate_index, params
        )
    return out


def _self_test() -> None:
    candidates = [
        {"code": "000001", "matched_themes": ["液冷"]},      # 子标签命中 算力
        {"code": "000002", "matched_themes": ["算力"]},      # 主标签命中
        {"code": "000003", "matched_themes": []},
    ]
    kaipan = [
        {"主标签名称": "算力", "板块强度原值": "2059", "主力流入真实金额": "3.2亿",
         "涨停数量": "8", "子标签列表": "算力租赁、数据中心、液冷"},
        {"主标签名称": "消费", "板块强度原值": "900", "主力流入真实金额": "-1亿", "涨停数量": "1"},
    ]
    out = compute_theme_strengths(candidates, kaipan, {}, {}, {})
    assert out["000001"]["theme_matched"] and out["000001"]["matched_level"] == "sub"
    assert out["000001"]["matched_plate"] == "算力"
    assert out["000001"]["t0_limitup_count"] == 8
    assert out["000002"]["t0_plate_inflow_wan"] and out["000002"]["t0_plate_inflow_wan"] > 0
    assert out["000003"]["theme_strength_t0"] == 20.0
    print("v9_theme_strength _self_test passed")


if __name__ == "__main__":
    _self_test()
