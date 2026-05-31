"""v7.2 output shaping and intraday anchors.

Action-pool report for premarket selection.  Production price/cost fields use
`auction_change_pct` only.  `latest_change_pct` / 最新涨幅 / 涨幅 are not used as
premarket selection inputs.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

DEFAULT_V72_ANCHORS = {
    "T0-LEAD": ["9:31 前是否出现瞬时封板/回封动作", "封单是否 ≥ 1 亿且撤单不明显", "9:35 前分时不破开盘价", "10:00 前不破开盘 -1.0%"],
    "T0-NEW-high": ["9:35 前涨幅 ≥ 2%", "9:30-9:35 分时不破开盘价", "10:00 前成交额 ≥ 3 亿"],
    "T0-NEW-low": ["9:35 前涨幅 ≥ 1.5%", "9:30-9:35 不破开盘 -0.5%", "10:00 前成交额 ≥ 2 亿"],
    "T0-ROTATE": ["10:00 前成交额 ≥ 5 亿", "10:00 不破开盘 -1.0%", "命中板块是否继续维持强度前排", "板块内其它高辨识度标的是否同步红盘"],
    "T0-REVERSAL": ["9:35 前是否快速收复竞价跌幅的 1/3", "9:45 前是否重新站上开盘价", "净额/成交额承接是否持续而不是一次性脉冲", "若 10:00 前仍弱于开盘价,降级为观察"],
    "T0-DIVERGENCE": ["9:35 前是否有承接而非单边回落", "分歧放量后是否重新站上均价线", "板块内是否仍有同步修复标的"],
    "T0-GENERAL": ["不破开盘 -2%", "9:45 前不放巨量阴线", "盘中观察,不作为主候选"],
    "none": ["仅观察,不作为盘中主候选"],
}

ACTION_PRIORITY = {"AUCTION_FOLLOW": 10, "THEME_CATCHUP": 20, "LOW_OPEN_REVERSAL": 30, "BOARD_WATCH": 40, "CONFIRMATION_WATCH": 80, "AVOID": 99}
DEFAULT_BROAD_THEMES = {"一季报增长", "业绩增长", "年报增长", "半年报增长", "业绩预增", "并购重组", "股权转让"}


def _anchor_key(decision: Dict[str, Any]) -> str:
    setup = decision.get("setup_v72") or "none"
    conf = decision.get("confidence") or "low"
    if setup == "T0-NEW":
        return "T0-NEW-high" if conf == "high" else "T0-NEW-low"
    return setup


def _f(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "-", "None"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _detail(d: Dict[str, Any]) -> Dict[str, Any]:
    return d.get("auction_detail") or {}


def _theme(d: Dict[str, Any]) -> Dict[str, Any]:
    return d.get("theme_detail") or {}


def _metric(d: Dict[str, Any], key: str, default: Optional[float] = 0.0) -> Optional[float]:
    if key in d:
        return _f(d.get(key), default)
    a = _detail(d)
    if key in a:
        return _f(a.get(key), default)
    s = d.get("signal_summary") or {}
    if key in s:
        return _f(s.get(key), default)
    return default


def _auction_pct(d: Dict[str, Any]) -> Optional[float]:
    return _metric(d, "auction_change_pct", None)


def _theme_is_broad(theme_detail: Dict[str, Any], action_cfg: Dict[str, Any]) -> bool:
    broad = set(action_cfg.get("broad_theme_names") or DEFAULT_BROAD_THEMES)
    matched_plate = str(theme_detail.get("matched_plate") or "")
    if matched_plate in broad:
        return True
    return any(str(tag) in broad for tag in theme_detail.get("matched_tags") or [])


def _pressure_score(net_pressure: Optional[float], full_ratio: float = 0.002) -> float:
    if net_pressure is None:
        return 0.0
    return _clamp(max(0.0, net_pressure) / max(full_ratio, 1e-9) * 100.0)


def _low_cost_score(pct: Optional[float], lo: float = -1.5, hi: float = 3.5) -> float:
    if pct is None:
        return 40.0
    if lo <= pct <= hi:
        center = 1.0
        span = max(abs(hi - center), abs(center - lo))
        return _clamp(100.0 - abs(pct - center) / span * 45.0)
    if pct > hi:
        return _clamp(45.0 - (pct - hi) * 10.0)
    return _clamp(45.0 - (lo - pct) * 8.0)


def _confidence_from_score(score: float, high: float = 65.0, mid: float = 45.0) -> str:
    if score >= high:
        return "high"
    if score >= mid:
        return "medium"
    return "low"


def _classify_action(d: Dict[str, Any], action_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = action_cfg or {}
    a = _detail(d)
    t = _theme(d)
    auction_type = str(d.get("auction_setup_type") or a.get("auction_setup_type") or "GENERAL_WATCH")
    entry_tag = str(d.get("entry_tag") or a.get("entry_tag") or "normal")
    setup = str(d.get("setup_v72") or "none")
    confidence = str(d.get("confidence") or "none")
    auction = float(_metric(d, "auction_strength", 0.0) or 0.0)
    theme = float(_metric(d, "theme_strength_t0", 0.0) or 0.0)
    hot = _metric(d, "hotness_score", None)
    hot_score = 0.0 if hot is None else float(hot)
    pct = _auction_pct(d)
    source_evidence = float(_metric(d, "source_evidence_score", 0.0) or 0.0)
    money_score = float(_metric(d, "money_intent_score", 0.0) or 0.0)
    resonance = float(_metric(d, "resonance_score", 0.0) or 0.0)
    orderbook = float(_metric(d, "orderbook_quality_score", 45.0) or 45.0)
    liquidity = float(_metric(d, "liquidity_score", 50.0) or 50.0)
    net_pressure = _metric(d, "net_pressure", None)
    family_count = int(_metric(d, "source_family_count", 0) or 0)
    q920_rank = a.get("qiangchou_920_925_rank")
    broad_theme = _theme_is_broad(t, cfg)

    action_tags: List[str] = []
    if broad_theme:
        action_tags.append("broad_theme")
    if family_count >= 3:
        action_tags.append("multi_source")
    if q920_rank not in (None, ""):
        action_tags.append("sustained_qiangchou")
    if pct is not None and pct >= float(cfg.get("high_cost_pct", 8.5)):
        action_tags.append("high_cost")
    if pct is not None and pct < 0:
        action_tags.append("negative_open")

    if entry_tag == "avoid" or auction_type == "FAKE_STRENGTH" or d.get("risk_penalty") == 0:
        score = 100.0 - min(100.0, auction + theme * 0.2)
        return {"action_type": "AVOID", "action_confidence": "high", "action_score": round(score, 2), "action_reason": "fake_strength_or_entry_avoid", "action_tags": action_tags, "action_priority": ACTION_PRIORITY["AVOID"], "theme_is_broad": broad_theme}

    board_pct = float(cfg.get("board_watch_pct", 9.5))
    if entry_tag == "board_watch" or auction_type == "BOARD_LOCK_WATCH" or (pct is not None and pct >= board_pct):
        score = _clamp(0.35 * auction + 0.25 * orderbook + 0.20 * liquidity + 0.20 * max(theme, hot_score))
        return {"action_type": "BOARD_WATCH", "action_confidence": _confidence_from_score(score, high=62, mid=42), "action_score": round(score, 2), "action_reason": "near_limit_or_locked_board_watch", "action_tags": action_tags, "action_priority": ACTION_PRIORITY["BOARD_WATCH"], "theme_is_broad": broad_theme}

    if auction_type == "LOW_OPEN_REVERSAL" and pct is not None and pct < 0 and auction >= float(cfg.get("reversal_min_auction_strength", 25)):
        score = _clamp(0.34 * auction + 0.22 * _pressure_score(net_pressure, float(cfg.get("net_pressure_full_ratio", 0.002))) + 0.18 * hot_score + 0.16 * min(100.0, source_evidence * 3.0) + 0.10 * liquidity)
        return {"action_type": "LOW_OPEN_REVERSAL", "action_confidence": _confidence_from_score(score, high=58, mid=38), "action_score": round(score, 2), "action_reason": "low_open_repair_with_premarket_support", "action_tags": action_tags, "action_priority": ACTION_PRIORITY["LOW_OPEN_REVERSAL"], "theme_is_broad": broad_theme}

    has_follow_pct = pct is not None and float(cfg.get("follow_pct_min", 2.0)) <= pct <= float(cfg.get("follow_pct_max", 7.5))
    has_follow_evidence = source_evidence >= float(cfg.get("follow_min_source_evidence", 18)) or family_count >= int(cfg.get("follow_min_source_family_count", 2))
    if has_follow_pct and auction >= float(cfg.get("follow_min_auction_strength", 50)) and has_follow_evidence:
        score = _clamp(0.30 * auction + 0.23 * source_evidence + 0.17 * theme + 0.13 * money_score + 0.10 * resonance + 0.07 * liquidity)
        return {"action_type": "AUCTION_FOLLOW", "action_confidence": _confidence_from_score(score, high=62, mid=45), "action_score": round(score, 2), "action_reason": "healthy_cost_auction_follow_through", "action_tags": action_tags, "action_priority": ACTION_PRIORITY["AUCTION_FOLLOW"], "theme_is_broad": broad_theme}

    if theme >= float(cfg.get("theme_catchup_min_theme", 80)) and (pct is None or float(cfg.get("theme_catchup_pct_min", -1.5)) <= pct <= float(cfg.get("theme_catchup_pct_max", 3.5))):
        low_cost = _low_cost_score(pct, float(cfg.get("theme_catchup_pct_min", -1.5)), float(cfg.get("theme_catchup_pct_max", 3.5)))
        score = _clamp(0.38 * theme + 0.24 * low_cost + 0.16 * auction + 0.12 * hot_score + 0.10 * liquidity)
        return {"action_type": "THEME_CATCHUP", "action_confidence": _confidence_from_score(score, high=62, mid=45), "action_score": round(score, 2), "action_reason": "low_cost_t0_theme_catchup", "action_tags": action_tags, "action_priority": ACTION_PRIORITY["THEME_CATCHUP"], "theme_is_broad": broad_theme}

    if setup != "none" or confidence != "none":
        score = _clamp(0.32 * auction + 0.25 * theme + 0.15 * hot_score + 0.15 * source_evidence + 0.13 * liquidity)
        return {"action_type": "CONFIRMATION_WATCH", "action_confidence": _confidence_from_score(score, high=65, mid=40), "action_score": round(score, 2), "action_reason": "incomplete_or_single_factor_signal", "action_tags": action_tags, "action_priority": ACTION_PRIORITY["CONFIRMATION_WATCH"], "theme_is_broad": broad_theme}

    return {"action_type": "CONFIRMATION_WATCH", "action_confidence": "low", "action_score": 0.0, "action_reason": "not_selected_but_kept_for_debug", "action_tags": action_tags, "action_priority": ACTION_PRIORITY["CONFIRMATION_WATCH"], "theme_is_broad": broad_theme}


def _with_action(d: Dict[str, Any], action_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = dict(d)
    out.update(_classify_action(d, action_cfg))
    return out


def _compact_decision(d: Dict[str, Any]) -> Dict[str, Any]:
    auction_detail = d.get("auction_detail") or {}
    theme_detail = d.get("theme_detail") or {}
    return {"code": d.get("code"), "name": d.get("name"), "setup_v72": d.get("setup_v72"), "confidence": d.get("confidence"), "setup_reason": d.get("setup_reason"), "action_type": d.get("action_type"), "action_confidence": d.get("action_confidence"), "action_score": d.get("action_score"), "action_reason": d.get("action_reason"), "action_tags": d.get("action_tags") or [], "theme_is_broad": d.get("theme_is_broad"), "final_score": d.get("final_score"), "today_signal_raw": d.get("today_signal_raw"), "auction_pct": auction_detail.get("auction_change_pct"), "auction_strength": d.get("auction_strength"), "theme_strength_t0": d.get("theme_strength_t0"), "hotness_score": d.get("hotness_score"), "auction_setup_type": d.get("auction_setup_type") or auction_detail.get("auction_setup_type"), "source_evidence_score": auction_detail.get("source_evidence_score"), "source_family_count": auction_detail.get("source_family_count"), "entry_tag": d.get("entry_tag") or "normal", "entry_reason": d.get("entry_reason") or "normal", "qiangchou_primary_signal": auction_detail.get("qiangchou_primary_signal"), "qiangchou_920_925_rank": auction_detail.get("qiangchou_920_925_rank"), "qiangchou_last_second_rank": auction_detail.get("qiangchou_last_second_rank"), "auction_amount_wan": auction_detail.get("auction_amount_wan"), "net_pressure": auction_detail.get("net_pressure"), "fengdan_status": auction_detail.get("fengdan_status"), "matched_plate": theme_detail.get("matched_plate"), "matched_tags": theme_detail.get("matched_tags") or [], "t0_plate_strength_raw": theme_detail.get("t0_plate_strength_raw"), "risk_flag": d.get("risk_flag")}


def _is_avoid(d: Dict[str, Any]) -> bool:
    return d.get("action_type") == "AVOID" or (d.get("entry_tag") == "avoid")


def _sort_action(decisions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(decisions or [], key=lambda x: (int(x.get("action_priority") or 999), -(float(x.get("action_score") or 0.0)), -(float(x.get("final_score") or 0.0))))


def _sort_score(decisions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(decisions or [], key=lambda x: x.get("final_score") or 0, reverse=True)


def build_candidate_pools(decisions: List[Dict[str, Any]], pool_max: int = 15) -> Dict[str, List[Dict[str, Any]]]:
    ranked_action = _sort_action(decisions)
    ranked_score = _sort_score(decisions)
    pools: Dict[str, List[Dict[str, Any]]] = {"main_attack_pool": [], "theme_rotation_pool": [], "theme_catchup_pool": [], "low_open_reversal_pool": [], "board_watch_pool": [], "confirmation_watch_pool": [], "avoid_or_risk_pool": []}

    def fill(pool: str, rows: List[Dict[str, Any]]) -> None:
        seen: set[str] = set()
        for d in rows:
            code = str(d.get("code") or "")
            if not code or code in seen or len(pools[pool]) >= pool_max:
                continue
            seen.add(code)
            pools[pool].append(_compact_decision(d))

    fill("main_attack_pool", [d for d in ranked_action if d.get("action_type") == "AUCTION_FOLLOW"])
    fill("theme_rotation_pool", [d for d in ranked_score if d.get("setup_v72") == "T0-ROTATE"])
    fill("theme_catchup_pool", [d for d in ranked_action if d.get("action_type") == "THEME_CATCHUP"])
    fill("low_open_reversal_pool", [d for d in ranked_action if d.get("action_type") == "LOW_OPEN_REVERSAL"])
    fill("board_watch_pool", [d for d in ranked_action if d.get("action_type") == "BOARD_WATCH"])
    fill("confirmation_watch_pool", [d for d in ranked_action if d.get("action_type") == "CONFIRMATION_WATCH" and not _is_avoid(d)])
    fill("avoid_or_risk_pool", [d for d in ranked_action if _is_avoid(d)])
    return pools


def build_intraday_anchors_v72(top_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in top_candidates or []:
        key = _anchor_key(d)
        compact = _compact_decision(d)
        action_type = d.get("action_type")
        action_extra = {"AUCTION_FOLLOW": ["9:35 前不能跌破开盘价", "10:00 前竞价强势来源至少保留一个: 抢筹/净额/量比"], "THEME_CATCHUP": ["板块内是否继续扩散,不能只靠单票脉冲", "若 9:45 前仍弱于开盘价且板块退潮,降级"], "LOW_OPEN_REVERSAL": ["优先看收复开盘价和均价线,不追弱反抽", "若净额承接消失或继续破低,放弃"], "BOARD_WATCH": ["只按排板/回封逻辑观察,不和普通追涨票比较", "封单撤单明显时立即降级"]}.get(str(action_type), [])
        compact.update({"setup_v71_compat": d.get("setup_v71_compat"), "anchors": (DEFAULT_V72_ANCHORS.get(key, DEFAULT_V72_ANCHORS["none"]) + action_extra), "risk_detail": d.get("risk_detail") or {}, "signal_summary": d.get("signal_summary") or {}})
        out.append(compact)
    return out


def setup_stats_v72(decisions: List[Dict[str, Any]]) -> Dict[str, int]:
    stats: Dict[str, int] = {}
    for d in decisions or []:
        setup = str(d.get("setup_v72") or "none")
        stats[setup] = stats.get(setup, 0) + 1
    return stats


def action_stats_v72(decisions: List[Dict[str, Any]]) -> Dict[str, int]:
    stats: Dict[str, int] = {}
    for d in decisions or []:
        action = str(d.get("action_type") or "CONFIRMATION_WATCH")
        stats[action] = stats.get(action, 0) + 1
    return stats


def _analysis_notes(decisions: List[Dict[str, Any]]) -> List[str]:
    pools = action_stats_v72(decisions)
    notes: List[str] = ["Premarket price/cost fields use auction_change_pct only."]
    if pools.get("AUCTION_FOLLOW", 0) == 0:
        notes.append("No strong AUCTION_FOLLOW candidate: do not force a main-attack trade.")
    if pools.get("THEME_CATCHUP", 0) > 0:
        notes.append("Theme catch-up candidates are separated from auction-follow candidates; broad themes are allowed only as low-cost catch-up or with strong evidence.")
    if pools.get("LOW_OPEN_REVERSAL", 0) > 0:
        notes.append("Low-open reversal candidates are ranked in a separate pool because their target is repair/excess return, not raw close_pct.")
    if pools.get("BOARD_WATCH", 0) > 0:
        notes.append("Board/high-cost candidates are watch-only by default because post-auction excess return can be structurally limited.")
    return notes


def shape_v7_2_output(decisions: List[Dict[str, Any]], meta: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 50, action_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    enriched = [_with_action(d, action_config) for d in (decisions or [])]
    legacy_ranked = _sort_score(enriched)
    action_ranked = _sort_action(enriched)
    actionable = [d for d in action_ranked if d.get("action_type") in {"AUCTION_FOLLOW", "THEME_CATCHUP", "LOW_OPEN_REVERSAL", "BOARD_WATCH"}]
    top_actionable = actionable[:max_candidates]
    legacy_top = [d for d in legacy_ranked if d.get("setup_v72") != "none"][:max_candidates]
    watch = action_ranked[:watch_tier_max]
    pools = build_candidate_pools(enriched, pool_max=max(10, max_candidates // 2))
    shaped_meta = dict(meta or {})
    shaped_meta.setdefault("interpretation_notes", [])
    shaped_meta["interpretation_notes"] = list(shaped_meta.get("interpretation_notes") or []) + _analysis_notes(enriched)
    return {"version": "premarket_v7_2", "meta": shaped_meta, "setup_stats": setup_stats_v72(enriched), "action_stats": action_stats_v72(enriched), "candidate_pools": pools, "top_candidates": top_actionable, "actionable_candidates": top_actionable, "legacy_top_candidates": legacy_top, "watch_tier": watch, "all_candidates_action_ranked": action_ranked, "all_candidates_debug": legacy_ranked, "intraday_anchors": build_intraday_anchors_v72(top_actionable[:20])}


def write_v7_2_outputs(output_dir: str, decisions: List[Dict[str, Any]], meta: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 50, analysis_filename: str = "analysis_v7_2.json", anchors_filename: str = "intraday_anchors.json", action_config: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    shaped = shape_v7_2_output(decisions, meta=meta, max_candidates=max_candidates, watch_tier_max=watch_tier_max, action_config=action_config)
    analysis_path = out_dir / analysis_filename
    anchors_path = out_dir / anchors_filename
    analysis_path.write_text(json.dumps(shaped, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    anchors_path.write_text(json.dumps(shaped["intraday_anchors"], ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"analysis_path": str(analysis_path), "anchors_path": str(anchors_path)}


def _self_test() -> None:
    decisions = [
        {"code": "000001", "name": "A", "setup_v72": "T0-NEW", "setup_v71_compat": "D", "confidence": "high", "final_score": 88, "auction_strength": 80, "theme_strength_t0": 70, "auction_detail": {"auction_change_pct": 4.0, "latest_change_pct": 9.9, "source_evidence_score": 30, "source_family_count": 3, "money_intent_score": 80, "resonance_score": 80, "liquidity_score": 90, "qiangchou_primary_signal": "9:20-9:25"}, "theme_detail": {"matched_tags": ["算力"], "matched_plate": "算力"}},
        {"code": "000002", "name": "B", "setup_v72": "T0-GENERAL", "setup_v71_compat": "C2", "confidence": "low", "final_score": 50, "auction_strength": 10, "theme_strength_t0": 95, "auction_detail": {"auction_change_pct": 1.0, "source_evidence_score": 0, "liquidity_score": 70}, "theme_detail": {"matched_tags": ["一季报增长"], "matched_plate": "一季报增长"}},
        {"code": "000003", "name": "C", "setup_v72": "T0-REVERSAL", "setup_v71_compat": "REVERSAL", "confidence": "low", "final_score": 40, "auction_strength": 42, "hotness_score": 60, "auction_detail": {"auction_setup_type": "LOW_OPEN_REVERSAL", "auction_change_pct": -4.0, "source_evidence_score": 10, "net_pressure": 0.0015, "liquidity_score": 90}},
        {"code": "000004", "name": "D", "setup_v72": "none", "setup_v71_compat": "none", "confidence": "none", "final_score": 0, "entry_tag": "avoid", "auction_detail": {"auction_setup_type": "FAKE_STRENGTH"}},
        {"code": "000005", "name": "E", "setup_v72": "T0-GENERAL", "setup_v71_compat": "C2", "confidence": "low", "final_score": 30, "auction_strength": 48, "auction_detail": {"auction_change_pct": 10.0, "orderbook_quality_score": 80, "liquidity_score": 90}, "entry_tag": "board_watch"},
    ]
    out = shape_v7_2_output(decisions)
    assert out["version"] == "premarket_v7_2"
    assert out["action_stats"]["AUCTION_FOLLOW"] == 1, out["action_stats"]
    assert out["action_stats"]["THEME_CATCHUP"] == 1, out["action_stats"]
    assert out["action_stats"]["LOW_OPEN_REVERSAL"] == 1, out["action_stats"]
    assert out["action_stats"]["BOARD_WATCH"] == 1, out["action_stats"]
    assert out["candidate_pools"]["main_attack_pool"][0]["auction_pct"] == 4.0, out
    print("output v7.2 auction_change_pct-only action-pool _self_test passed")


if __name__ == "__main__":
    _self_test()
