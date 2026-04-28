"""
duanxianxia_v7_1_setup_engine.py — v7.1 setup 汇总引擎

Hardening points:
- regime gates per setup
- churn_high_volume blocks all non-D setups
- E rejects exploded seal status
- setup_fit_score used for secondary ranking
- blocked_reasons include setup/rule/value detail
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

SETUP_META = {
    "A_ice": {"name": "主龙跳(冰龙)", "priority": 3.5, "allowed_regimes": {"cold_to_warming", "normal", "hot"}},
    "A": {"name": "主龙跳", "priority": 3.0, "allowed_regimes": {"normal", "hot"}},
    "B": {"name": "同步补涨", "priority": 2.5, "allowed_regimes": {"normal", "hot"}},
    "C1": {"name": "新龙首次", "priority": 2.0, "allowed_regimes": {"cold", "cold_to_warming", "normal", "hot", "hot_to_downgrading"}},
    "C2": {"name": "轮动接力", "priority": 1.8, "allowed_regimes": {"cold", "cold_to_warming", "normal", "hot", "hot_to_downgrading"}},
    "D": {"name": "竞价微警", "priority": 1.5, "allowed_regimes": {"cold_to_warming", "normal", "hot"}},
    "E": {"name": "颃子身位", "priority": 1.0, "allowed_regimes": {"normal", "hot"}},
}

@dataclass
class SetupDecision:
    code: str
    name: str
    setup_id: str
    setup_name: str
    priority: float
    setup_fit_score: float
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


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, "", "-"):
            return default
        return float(str(v).replace("%", ""))
    except Exception:
        return default


def _get_obj(d: Dict[str, Any], key: str) -> Dict[str, Any]:
    obj = (d or {}).get(key) or {}
    return obj if isinstance(obj, dict) else {}


def _get_label(d: Dict[str, Any], code: str, key: str = "label", default: str = "none") -> str:
    return str(_get_obj(d, code).get(key, default) or default)


def _themes(candidate: Dict[str, Any]) -> List[str]:
    vals = candidate.get("matched_themes") or candidate.get("themes") or candidate.get("concepts") or []
    if isinstance(vals, str):
        return [x.strip() for x in vals.replace("，", ",").split(",") if x.strip()]
    return [str(x).strip() for x in vals if str(x).strip()]


def _source_hit_count(candidate: Dict[str, Any]) -> int:
    v = candidate.get("source_hit_count")
    if v is not None:
        try:
            return int(v)
        except Exception:
            pass
    sources = candidate.get("source_hits") or candidate.get("sources") or candidate.get("hit_sources") or []
    return len(set(str(x) for x in sources)) if isinstance(sources, list) else 0


def _auction_pct(candidate: Dict[str, Any]) -> Optional[float]:
    for k in ("auction_change_pct", "latest_change_pct", "change_pct"):
        v = candidate.get(k)
        if v not in (None, ""):
            try:
                return float(str(v).replace("%", ""))
            except Exception:
                pass
    return None


def _best_theme(themes: List[str], industry_labels: Dict[str, Any], theme_history: Dict[str, Any]) -> Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]:
    """多板块匹配时取 pct_strength 最高者；这是当前资金最认可方向。"""
    best = ("none", "none", "", {}, {})
    best_score = -1.0
    hist_rank = {"fresh": 5, "day1_fermenting": 4, "day2_main": 3, "day3_high": 2, "fading": 1, "none": 0}
    for t in themes:
        i_obj = _get_obj(industry_labels, t)
        h_obj = _get_obj(theme_history, t)
        i = str(i_obj.get("label", "none") or "none")
        h = str(h_obj.get("label", "none") or "none")
        s = _to_float(i_obj.get("pct_strength"), 0.0)
        tie = hist_rank.get(h, 0) / 100.0
        score = s + tie
        if score > best_score:
            best_score = score
            best = (i, h, t, i_obj, h_obj)
    return best


def _snapshot(code: str, candidate: Dict[str, Any], labels: Dict[str, Any], best_ind: str, best_hist: str, best_theme: str, industry_obj: Dict[str, Any]) -> Dict[str, Any]:
    zt_obj = _get_obj(labels.get("zt", {}), code)
    stock_obj = _get_obj(labels.get("stock_t1", {}), code)
    tech_obj = _get_obj(labels.get("tech_profile", {}), code)
    regime_obj = labels.get("regime") or {}
    return {
        "best_theme": best_theme,
        "industry_t1_label": best_ind,
        "industry_pct_strength": _to_float(industry_obj.get("pct_strength"), 0.0),
        "industry_pct_inflow": _to_float(industry_obj.get("pct_inflow"), 0.0),
        "theme_history": best_hist,
        "stock_t1_label": str(stock_obj.get("label", "miss") or "miss"),
        "stock_super_ratio": _to_float(stock_obj.get("super_ratio"), 0.0),
        "stock_main_inflow_wan": _to_float(stock_obj.get("main_inflow_wan"), 0.0),
        "cashflow_continuity": _get_label(labels.get("cashflow_continuity", {}), code, default="neutral"),
        "zt_pattern": str(zt_obj.get("pattern", "无") or "无"),
        "zt_quality": str(zt_obj.get("quality_label", "dirty") or "dirty"),
        "zt_quality_score": _to_float(zt_obj.get("quality_score"), 0.0),
        "zt_seal_verified": str(zt_obj.get("seal_verified", "none") or "none"),
        "longtou_status": _get_label(labels.get("longtou", {}), code, default="none"),
        "tech_profile": str(tech_obj.get("label", "unknown") or "unknown"),
        "source_hit_count": _source_hit_count(candidate),
        "auction_change_pct": _auction_pct(candidate),
        "market_regime": str(regime_obj.get("label", "normal") or "normal"),
    }


def _block(setup_id: str, rule: str, actual: Any, expected: Any) -> str:
    return f"{setup_id} blocked: {rule}={actual} (need {expected})"


def _common_gate(setup_id: str, s: Dict[str, Any]) -> Tuple[bool, List[str]]:
    b: List[str] = []
    regime = s.get("market_regime", "normal")
    if regime not in SETUP_META[setup_id]["allowed_regimes"]:
        b.append(_block(setup_id, "market_regime", regime, sorted(SETUP_META[setup_id]["allowed_regimes"])))
    if setup_id != "D" and s.get("tech_profile") == "churn_high_volume":
        b.append(_block(setup_id, "tech_profile", "churn_high_volume", "not churn_high_volume"))
    return len(b) == 0, b


def _pass_A(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    ok, cb = _common_gate("A", s); b.extend(cb)
    if s["industry_t1_label"] == "hit_strong:leader": r.append("industry leader")
    else: b.append(_block("A", "industry_t1_label", s["industry_t1_label"], "hit_strong:leader"))
    if s["zt_quality"] == "clean": r.append("zt clean")
    else: b.append(_block("A", "zt_quality", s["zt_quality"], "clean"))
    if s["zt_seal_verified"] in ("sealed", "none"): r.append("not exploded")
    else: b.append(_block("A", "zt_seal_verified", s["zt_seal_verified"], "sealed/none"))
    if s["longtou_status"] in ("confirmed_longtou", "mid_position"): r.append("position ok")
    else: b.append(_block("A", "longtou_status", s["longtou_status"], "confirmed_longtou/mid_position"))
    return len(b) == 0, r, b


def _pass_A_ice(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    ok, cb = _common_gate("A_ice", s); b.extend(cb)
    if s["longtou_status"] == "confirmed_longtou": r.append("confirmed longtou")
    else: b.append(_block("A_ice", "longtou_status", s["longtou_status"], "confirmed_longtou"))
    if s["industry_t1_label"] in ("hit_strong:rising", "hit_strong:absorb_dip", "hit_weak:fade"): r.append("cooled leader state")
    else: b.append(_block("A_ice", "industry_t1_label", s["industry_t1_label"], "rising/absorb_dip/fade"))
    if s["tech_profile"] in ("cooling", "healthy", "breakout"): r.append("tech not broken")
    else: b.append(_block("A_ice", "tech_profile", s["tech_profile"], "cooling/healthy/breakout"))
    return len(b) == 0, r, b


def _pass_B(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    ok, cb = _common_gate("B", s); b.extend(cb)
    if s["industry_t1_label"] in ("hit_strong:leader", "hit_strong:rising"): r.append("industry strong")
    else: b.append(_block("B", "industry_t1_label", s["industry_t1_label"], "leader/rising"))
    if s["theme_history"] in ("day3_high", "fading"): r.append("theme high/fading")
    else: b.append(_block("B", "theme_history", s["theme_history"], "day3_high/fading"))
    if s["stock_t1_label"] in ("hit_top_strong", "hit_top_retail", "hit_mid_strong"): r.append("stock cashflow hit")
    else: b.append(_block("B", "stock_t1_label", s["stock_t1_label"], "top/mid strong"))
    return len(b) == 0, r, b


def _pass_C1(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    ok, cb = _common_gate("C1", s); b.extend(cb)
    if s["industry_t1_label"] in ("miss:new_entry", "hit_strong:rising"): r.append("new/rising theme")
    else: b.append(_block("C1", "industry_t1_label", s["industry_t1_label"], "new_entry/rising"))
    if s["theme_history"] in ("fresh", "day1_fermenting"): r.append("fresh/day1")
    else: b.append(_block("C1", "theme_history", s["theme_history"], "fresh/day1"))
    if s["stock_t1_label"].startswith("hit_top") or s["cashflow_continuity"] == "accumulating_strong": r.append("strong stock cashflow")
    else: b.append(_block("C1", "cashflow", f"{s['stock_t1_label']}/{s['cashflow_continuity']}", "hit_top or accumulating_strong"))
    return len(b) == 0, r, b


def _pass_C2(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    ok, cb = _common_gate("C2", s); b.extend(cb)
    if s["industry_t1_label"] in ("hit_strong:rising", "hit_strong:absorb_dip"): r.append("rotation industry hit")
    else: b.append(_block("C2", "industry_t1_label", s["industry_t1_label"], "rising/absorb_dip"))
    if s["theme_history"] in ("fresh", "day1_fermenting", "day2_main"): r.append("not overheated")
    else: b.append(_block("C2", "theme_history", s["theme_history"], "fresh/day1/day2"))
    if s["tech_profile"] in ("healthy", "breakout", "cooling"): r.append("tech acceptable")
    else: b.append(_block("C2", "tech_profile", s["tech_profile"], "healthy/breakout/cooling"))
    return len(b) == 0, r, b


def _pass_D(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    ok, cb = _common_gate("D", s); b.extend(cb)
    if s["source_hit_count"] >= 3: r.append("auction >=3 sources")
    else: b.append(_block("D", "source_hit_count", s["source_hit_count"], ">=3"))
    pct = s.get("auction_change_pct")
    if pct is None or pct >= -1.0: r.append("auction pct not ugly")
    else: b.append(_block("D", "auction_change_pct", pct, ">=-1 or null"))
    if s["zt_seal_verified"] != "exploded": r.append("not exploded")
    else: b.append(_block("D", "zt_seal_verified", "exploded", "not exploded"))
    return len(b) == 0, r, b


def _pass_E(s: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    r, b = [], []
    ok, cb = _common_gate("E", s); b.extend(cb)
    if s["zt_pattern"] in ("一字", "首板", "二板", "三板加"): r.append("zt pool candidate")
    else: b.append(_block("E", "zt_pattern", s["zt_pattern"], "一字/首板/二板/三板加"))
    if s["source_hit_count"] >= 2: r.append("source_hit_count >=2")
    else: b.append(_block("E", "source_hit_count", s["source_hit_count"], ">=2"))
    if s["industry_t1_label"] != "hit_weak:fade": r.append("industry not fading")
    else: b.append(_block("E", "industry_t1_label", "hit_weak:fade", "not fade"))
    if s["zt_seal_verified"] != "exploded": r.append("not exploded")
    else: b.append(_block("E", "zt_seal_verified", "exploded", "not exploded"))
    return len(b) == 0, r, b

PASSERS = {"A_ice": _pass_A_ice, "A": _pass_A, "B": _pass_B, "C1": _pass_C1, "C2": _pass_C2, "D": _pass_D, "E": _pass_E}


def _norm01(v: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min((v - lo) / (hi - lo), 1.0))


def _fit_score(setup_id: str, s: Dict[str, Any]) -> float:
    auction = _norm01(_to_float(s.get("auction_change_pct"), 0.0), -2.0, 8.0)
    source = min(_to_float(s.get("source_hit_count"), 0.0) / 4.0, 1.0)
    auction_strength = 0.7 * auction + 0.3 * source
    money = min(_to_float(s.get("stock_main_inflow_wan"), 0.0) / 10000.0, 1.0)
    super_ratio = min(_to_float(s.get("stock_super_ratio"), 0.0) / 0.6, 1.0)
    cashflow = 0.5 * money + 0.5 * super_ratio
    ztq = _to_float(s.get("zt_quality_score"), 0.0)
    theme = 0.7 * _to_float(s.get("industry_pct_strength"), 0.0) + 0.3 * _to_float(s.get("industry_pct_inflow"), 0.0)
    if setup_id in ("A", "A_ice"):
        score = 0.3 * ztq + 0.3 * cashflow + 0.2 * auction_strength + 0.2 * theme
    elif setup_id in ("B", "C1", "C2"):
        score = 0.4 * cashflow + 0.3 * theme + 0.3 * auction_strength
    else:
        score = 0.5 * auction_strength + 0.2 * cashflow + 0.2 * theme + 0.1 * source
    if s.get("tech_profile") == "churn_high_volume":
        score *= 0.6
    return round(score, 4)


def classify_candidate(candidate: Dict[str, Any], labels: Dict[str, Any]) -> SetupDecision:
    code = _norm_code(candidate.get("code") or candidate.get("代码"))
    name = str(candidate.get("name") or candidate.get("名称") or "")
    themes = _themes(candidate)
    best_ind, best_hist, best_theme, industry_obj, _hist_obj = _best_theme(themes, labels.get("industry_t1", {}), labels.get("theme_history", {}))
    snap = _snapshot(code, candidate, labels, best_ind, best_hist, best_theme, industry_obj)

    all_results = []
    for setup_id, fn in PASSERS.items():
        passed, reasons, blocked = fn(snap)
        meta = SETUP_META[setup_id]
        fit = _fit_score(setup_id, snap) if passed else 0.0
        all_results.append((passed, meta["priority"], fit, setup_id, reasons, blocked))

    passed_results = [x for x in all_results if x[0]]
    if passed_results:
        _p, priority, fit, setup_id, reasons, blocked = sorted(passed_results, key=lambda x: (-x[1], -x[2], x[3]))[0]
        meta = SETUP_META[setup_id]
        return SetupDecision(code, name, setup_id, meta["name"], priority, fit, True, reasons, blocked, snap, candidate)

    _p, _priority, _fit, setup_id, _reasons, blocked = sorted(all_results, key=lambda x: (-x[1], x[3]))[0]
    return SetupDecision(code, name, "none", "未入选", 0.0, 0.0, False, [], blocked, snap, candidate)


def classify_candidates(candidates: List[Dict[str, Any]], labels: Dict[str, Any], max_candidates: Optional[int] = None) -> List[Dict[str, Any]]:
    decisions = [asdict(classify_candidate(c, labels)) for c in (candidates or [])]
    decisions.sort(key=lambda d: (-float(d.get("priority") or 0), -float(d.get("setup_fit_score") or 0), d.get("code") or ""))
    return decisions[:max_candidates] if max_candidates else decisions


def setup_stats(decisions: List[Dict[str, Any]]) -> Dict[str, int]:
    stats: Dict[str, int] = {k: 0 for k in ["A_ice", "A", "B", "C1", "C2", "D", "E", "none"]}
    for d in decisions or []:
        sid = d.get("setup_id") or "none"
        stats[sid] = stats.get(sid, 0) + 1
    return stats


if __name__ == "__main__":
    print("setup_engine loaded")
