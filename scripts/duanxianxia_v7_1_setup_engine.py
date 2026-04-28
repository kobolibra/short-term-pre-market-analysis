"""
duanxianxia_v7_1_setup_engine.py — v7.1 setup 汇总引擎

输入是已计算好的标签字典,不在本文件重复读 capture:
  candidate: 基础候选字段(code/name/source_hit_count/auction_change_pct/latest_change_pct/matched_themes 等)
  labels: industry_t1/theme_history/stock_t1/cashflow_continuity/zt/longtou/tech_profile/regime
输出每只股票最优 setup + 全部可解释 reasons。

v7.1 setups:
  A      主龙跳         priority 3.0
  A_ice  主龙跳(冰龙)   priority 3.5
  B      同步补涨       priority 2.5
  C1     新龙首次       priority 2.0
  C2     轮动接力       priority 1.8
  D      竞价微警       priority 1.5
  E      颃子身位       priority 1.0
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple


SETUP_META = {
    "A_ice": {"name": "主龙跳(冰龙)", "priority": 3.5},
    "A": {"name": "主龙跳", "priority": 3.0},
    "B": {"name": "同步补涨", "priority": 2.5},
    "C1": {"name": "新龙首次", "priority": 2.0},
    "C2": {"name": "轮动接力", "priority": 1.8},
    "D": {"name": "竞价微警", "priority": 1.5},
    "E": {"name": "颃子身位", "priority": 1.0},
}


@dataclass
class SetupDecision:
    code: str
    name: str
    setup_id: str
    setup_name: str
    priority: float
    passed: bool
    reasons: List[str]
    blocked_reasons: List[str]
    label_snapshot: Dict[str, Any]
    raw_candidate: Dict[str, Any]


def _norm_code(raw: Any) -> str:
    s = str(raw or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    if len(s) >= 6:
        s = s[-6:]
    return s


def _get_label(d: Dict[str, Any], code: str, key: str = "label", default: str = "none") -> str:
    obj = (d or {}).get(code) or {}
    return str(obj.get(key, default) or default)


def _themes(candidate: Dict[str, Any]) -> List[str]:
    vals = candidate.get("matched_themes") or candidate.get("themes") or candidate.get("concepts") or []
    if isinstance(vals, str):
        return [x.strip() for x in vals.replace("，", ",").split(",") if x.strip()]
    return [str(x).strip() for x in vals if str(x).strip()]


def _best_theme_label(themes: List[str], industry_labels: Dict[str, Any], theme_history: Dict[str, Any]) -> Tuple[str, str, str]:
    """返回(best_industry_label,best_theme_history_label,best_theme)。"""
    industry_rank = {
        "hit_strong:leader": 5,
        "hit_strong:rising": 4,
        "hit_strong:absorb_dip": 3,
        "hit_weak:fade": 2,
        "miss:new_entry": 1,
        "none": 0,
    }
    hist_rank = {"fresh": 5, "day1_fermenting": 4, "day2_main": 3, "day3_high": 2, "fading": 1, "none": 0}
    best_theme = ""
    best_ind = "none"
    best_hist = "none"
    best_score = -1
    for t in themes:
        i_obj = (industry_labels or {}).get(t) or {}
        h_obj = (theme_history or {}).get(t) or {}
        i = str(i_obj.get("label", "none") or "none")
        h = str(h_obj.get("label", "none") or "none")
        score = industry_rank.get(i, 0) * 10 + hist_rank.get(h, 0)
        if score > best_score:
            best_score = score
            best_theme = t
            best_ind = i
            best_hist = h
    return best_ind, best_hist, best_theme


def _source_hit_count(candidate: Dict[str, Any]) -> int:
    v = candidate.get("source_hit_count")
    if v is not None:
        try:
            return int(v)
        except Exception:
            pass
    sources = candidate.get("sources") or candidate.get("hit_sources") or []
    if isinstance(sources, list):
        return len(set(str(x) for x in sources))
    return 0


def _auction_pct(candidate: Dict[str, Any]) -> Optional[float]:
    for k in ("auction_change_pct", "latest_change_pct", "change_pct"):
        v = candidate.get(k)
        if v in (None, ""):
            continue
        try:
            return float(str(v).replace("%", ""))
        except Exception:
            continue
    return None


def _snapshot(
    code: str,
    candidate: Dict[str, Any],
    labels: Dict[str, Any],
    best_industry: str,
    best_history: str,
    best_theme: str,
) -> Dict[str, Any]:
    return {
        "best_theme": best_theme,
        "industry_t1_label": best_industry,
        "theme_history": best_history,
        "stock_t1_label": _get_label(labels.get("stock_t1", {}), code, default="miss"),
        "cashflow_continuity": _get_label(labels.get("cashflow_continuity", {}), code, default="neutral"),
        "zt_pattern": _get_label(labels.get("zt", {}), code, key="pattern", default="无"),
        "zt_quality": _get_label(labels.get("zt", {}), code, key="quality_label", default="dirty"),
        "zt_seal_verified": _get_label(labels.get("zt", {}), code, key="seal_verified", default="none"),
        "longtou_status": _get_label(labels.get("longtou", {}), code, default="none"),
        "tech_profile": _get_label(labels.get("tech_profile", {}), code, default="unknown"),
        "source_hit_count": _source_hit_count(candidate),
        "auction_change_pct": _auction_pct(candidate),
    }


def _pass_A(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    if s["industry_t1_label"] == "hit_strong:leader": r.append("industry leader")
    else: b.append("industry not leader")
    if s["zt_quality"] == "clean": r.append("zt clean")
    else: b.append("zt not clean")
    if s["zt_seal_verified"] in ("sealed", "none"): r.append("not exploded")
    else: b.append("zt exploded")
    if s["longtou_status"] in ("confirmed_longtou", "mid_position"): r.append("position ok")
    else: b.append("not longtou/mid")
    return len(b) == 0, r, b


def _pass_A_ice(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    if s["longtou_status"] == "confirmed_longtou": r.append("confirmed longtou")
    else: b.append("not confirmed longtou")
    if s["industry_t1_label"] in ("hit_strong:rising", "hit_strong:absorb_dip", "hit_weak:fade"):
        r.append("leader cooled then watch reactivation")
    else:
        b.append("no cooled leader state")
    if s["tech_profile"] in ("cooling", "healthy", "breakout"):
        r.append("tech not broken")
    else:
        b.append("tech weak/unknown")
    return len(b) == 0, r, b


def _pass_B(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    if s["industry_t1_label"] in ("hit_strong:leader", "hit_strong:rising"):
        r.append("industry strong")
    else: b.append("industry not strong")
    if s["theme_history"] in ("day3_high", "fading"):
        r.append("theme high/fading; watch补涨")
    else: b.append("theme not high/fading")
    if s["stock_t1_label"] in ("hit_top_strong", "hit_top_retail", "hit_mid_strong"):
        r.append("stock cashflow hit")
    else: b.append("stock cashflow weak")
    return len(b) == 0, r, b


def _pass_C1(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    if s["industry_t1_label"] in ("miss:new_entry", "hit_strong:rising"):
        r.append("new/rising theme")
    else: b.append("not new/rising theme")
    if s["theme_history"] in ("fresh", "day1_fermenting"):
        r.append("fresh/day1")
    else: b.append("theme too old")
    if s["stock_t1_label"].startswith("hit_top") or s["cashflow_continuity"] == "accumulating_strong":
        r.append("strong stock cashflow")
    else: b.append("stock cashflow not strong")
    return len(b) == 0, r, b


def _pass_C2(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    if s["industry_t1_label"] in ("hit_strong:rising", "hit_strong:absorb_dip"):
        r.append("rotation industry hit")
    else: b.append("not rotation hit")
    if s["theme_history"] in ("fresh", "day1_fermenting", "day2_main"):
        r.append("not overheated")
    else: b.append("theme overheated")
    if s["tech_profile"] in ("healthy", "breakout", "cooling"):
        r.append("tech acceptable")
    else: b.append("tech weak/unknown")
    return len(b) == 0, r, b


def _pass_D(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    if s["source_hit_count"] >= 3:
        r.append("auction >=3 sources")
    else: b.append("auction sources <3")
    pct = s.get("auction_change_pct")
    if pct is None or pct >= -1.0:
        r.append("auction pct not ugly")
    else: b.append("auction pct too weak")
    if s["zt_seal_verified"] != "exploded":
        r.append("not exploded")
    else: b.append("exploded")
    return len(b) == 0, r, b


def _pass_E(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    if s["zt_pattern"] in ("一字", "首板", "二板", "三板加"):
        r.append("zt pool candidate")
    else: b.append("not zt pattern")
    if s["source_hit_count"] >= 2:
        r.append("source_hit_count >=2")
    else: b.append("source_hit_count <2; avoid single-fengdan反指")
    if s["industry_t1_label"] != "hit_weak:fade":
        r.append("industry not fading")
    else: b.append("industry fading")
    return len(b) == 0, r, b


PASSERS = {
    "A_ice": _pass_A_ice,
    "A": _pass_A,
    "B": _pass_B,
    "C1": _pass_C1,
    "C2": _pass_C2,
    "D": _pass_D,
    "E": _pass_E,
}


def classify_candidate(candidate: Dict[str, Any], labels: Dict[str, Any]) -> SetupDecision:
    code = _norm_code(candidate.get("code") or candidate.get("代码"))
    name = str(candidate.get("name") or candidate.get("名称") or "")
    themes = _themes(candidate)
    best_ind, best_hist, best_theme = _best_theme_label(themes, labels.get("industry_t1", {}), labels.get("theme_history", {}))
    snap = _snapshot(code, candidate, labels, best_ind, best_hist, best_theme)

    all_results = []
    for setup_id, fn in PASSERS.items():
        passed, reasons, blocked = fn(snap)
        meta = SETUP_META[setup_id]
        all_results.append((passed, meta["priority"], setup_id, reasons, blocked))

    passed_results = [x for x in all_results if x[0]]
    if passed_results:
        _passed, priority, setup_id, reasons, blocked = sorted(passed_results, key=lambda x: (-x[1], x[2]))[0]
        meta = SETUP_META[setup_id]
        return SetupDecision(code, name, setup_id, meta["name"], priority, True, reasons, blocked, snap, candidate)

    # none:保留最高优先级失败原因,方便 debug
    _passed, priority, setup_id, reasons, blocked = sorted(all_results, key=lambda x: (-x[1], x[2]))[0]
    return SetupDecision(code, name, "none", "未入选", 0.0, False, [], blocked, snap, candidate)


def classify_candidates(candidates: List[Dict[str, Any]], labels: Dict[str, Any], max_candidates: Optional[int] = None) -> List[Dict[str, Any]]:
    decisions = [asdict(classify_candidate(c, labels)) for c in (candidates or [])]
    decisions.sort(key=lambda d: (-float(d.get("priority") or 0), -int((d.get("label_snapshot") or {}).get("source_hit_count") or 0), d.get("code") or ""))
    if max_candidates:
        return decisions[:max_candidates]
    return decisions


def setup_stats(decisions: List[Dict[str, Any]]) -> Dict[str, int]:
    stats: Dict[str, int] = {k: 0 for k in ["A_ice", "A", "B", "C1", "C2", "D", "E", "none"]}
    for d in decisions or []:
        sid = d.get("setup_id") or "none"
        stats[sid] = stats.get(sid, 0) + 1
    return stats


def _self_test() -> None:
    candidates = [
        {"code": "000001", "name": "龙头", "matched_themes": ["算力"], "source_hit_count": 4, "auction_change_pct": 2.0},
        {"code": "000002", "name": "冰龙", "matched_themes": ["消费"], "source_hit_count": 3, "auction_change_pct": 0.5},
        {"code": "000003", "name": "单封单", "matched_themes": ["一字"], "source_hit_count": 1, "auction_change_pct": None},
    ]
    labels = {
        "industry_t1": {"算力": {"label": "hit_strong:leader"}, "消费": {"label": "hit_strong:absorb_dip"}, "一字": {"label": "hit_strong:rising"}},
        "theme_history": {"算力": {"label": "day2_main"}, "消费": {"label": "fading"}, "一字": {"label": "fresh"}},
        "stock_t1": {"000001": {"label": "hit_top_strong"}, "000002": {"label": "hit_mid_strong"}, "000003": {"label": "miss"}},
        "cashflow_continuity": {"000001": {"label": "accumulating_strong"}, "000002": {"label": "accumulating"}, "000003": {"label": "neutral"}},
        "zt": {
            "000001": {"pattern": "三板加", "quality_label": "clean", "seal_verified": "sealed"},
            "000002": {"pattern": "三板加", "quality_label": "average", "seal_verified": "sealed"},
            "000003": {"pattern": "一字", "quality_label": "clean", "seal_verified": "sealed"},
        },
        "longtou": {"000001": {"label": "confirmed_longtou"}, "000002": {"label": "confirmed_longtou"}, "000003": {"label": "none"}},
        "tech_profile": {"000001": {"label": "breakout"}, "000002": {"label": "cooling"}, "000003": {"label": "healthy"}},
    }
    out = classify_candidates(candidates, labels)
    assert out[0]["setup_id"] == "A_ice", out[0]  # 冰龙优先级最高,但 000001 不满足 cooled,000002 满足
    assert any(d["code"] == "000001" and d["setup_id"] == "A" for d in out), out
    e = [d for d in out if d["code"] == "000003"][0]
    assert e["setup_id"] == "none", e
    assert "source_hit_count <2" in " | ".join(e["blocked_reasons"])
    print("setup_engine _self_test passed")


if __name__ == "__main__":
    _self_test()
