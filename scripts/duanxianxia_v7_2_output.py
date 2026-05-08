"""v7.2 output shaping and intraday anchors.

Outputs:
- analysis_v7_2.json
- intraday_anchors.json

The output now follows the earlier repo-review recommendation: do not give only
one flat rank. In addition to `top_candidates`, create actionable pools:
- main_attack_pool: high-conviction T0 attack candidates.
- theme_rotation_pool: strong T0 plate match + acceptable auction confirmation.
- board_watch_pool: near-limit/locked candidates that are strong but may be hard
  to buy and need board/回封 observation.
- confirmation_watch_pool: candidates with useful but incomplete signals.
- avoid_or_risk_pool: fake seal, hard risk, or poor tradability.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_V72_ANCHORS = {
    "T0-LEAD": [
        "9:31 前是否出现瞬时封板/回封动作",
        "封单是否 ≥ 1 亿且撤单不明显",
        "9:35 前分时不破开盘价",
        "10:00 前不破开盘 -1.0%",
    ],
    "T0-NEW-high": [
        "9:35 前涨幅 ≥ 2%",
        "9:30-9:35 分时不破开盘价",
        "10:00 前成交额 ≥ 3 亿",
    ],
    "T0-NEW-low": [
        "9:35 前涨幅 ≥ 1.5%",
        "9:30-9:35 不破开盘 -0.5%",
        "10:00 前成交额 ≥ 2 亿",
    ],
    "T0-ROTATE": [
        "10:00 前成交额 ≥ 5 亿",
        "10:00 不破开盘 -1.0%",
        "命中板块是否继续维持强度前排",
        "板块内其它高辨识度标的是否同步红盘",
    ],
    "T0-GENERAL": [
        "不破开盘 -2%",
        "9:45 前不放巨量阴线",
        "盘中观察,不作为主候选",
    ],
    "none": ["仅观察,不作为盘中主候选"],
}


def _anchor_key(decision: Dict[str, Any]) -> str:
    setup = decision.get("setup_v72") or "none"
    conf = decision.get("confidence") or "low"
    if setup == "T0-NEW":
        return "T0-NEW-high" if conf == "high" else "T0-NEW-low"
    return setup


def _compact_decision(d: Dict[str, Any]) -> Dict[str, Any]:
    auction_detail = d.get("auction_detail") or {}
    theme_detail = d.get("theme_detail") or {}
    return {
        "code": d.get("code"),
        "name": d.get("name"),
        "setup_v72": d.get("setup_v72"),
        "confidence": d.get("confidence"),
        "setup_reason": d.get("setup_reason"),
        "final_score": d.get("final_score"),
        "today_signal_raw": d.get("today_signal_raw"),
        "auction_strength": d.get("auction_strength"),
        "theme_strength_t0": d.get("theme_strength_t0"),
        "hotness_score": d.get("hotness_score"),
        "entry_tag": d.get("entry_tag") or "normal",
        "entry_reason": d.get("entry_reason") or "normal",
        "qiangchou_primary_signal": auction_detail.get("qiangchou_primary_signal"),
        "qiangchou_920_925_rank": auction_detail.get("qiangchou_920_925_rank"),
        "qiangchou_last_second_rank": auction_detail.get("qiangchou_last_second_rank"),
        "auction_amount_wan": auction_detail.get("auction_amount_wan"),
        "net_pressure": auction_detail.get("net_pressure"),
        "fengdan_status": auction_detail.get("fengdan_status"),
        "matched_plate": theme_detail.get("matched_plate"),
        "matched_tags": theme_detail.get("matched_tags") or [],
        "t0_plate_strength_raw": theme_detail.get("t0_plate_strength_raw"),
        "risk_flag": d.get("risk_flag"),
    }


def _is_avoid(d: Dict[str, Any]) -> bool:
    return (d.get("setup_v72") == "none" and (d.get("entry_tag") == "avoid" or d.get("risk_penalty") == 0)) or d.get("entry_tag") == "avoid"


def build_candidate_pools(decisions: List[Dict[str, Any]], pool_max: int = 15) -> Dict[str, List[Dict[str, Any]]]:
    ranked = sorted(decisions or [], key=lambda x: x.get("final_score") or 0, reverse=True)
    pools: Dict[str, List[Dict[str, Any]]] = {
        "main_attack_pool": [],
        "theme_rotation_pool": [],
        "board_watch_pool": [],
        "confirmation_watch_pool": [],
        "avoid_or_risk_pool": [],
    }
    seen: set[str] = set()

    def add(pool: str, d: Dict[str, Any]) -> None:
        code = str(d.get("code") or "")
        if not code or code in seen or len(pools[pool]) >= pool_max:
            return
        seen.add(code)
        pools[pool].append(_compact_decision(d))

    for d in ranked:
        if _is_avoid(d):
            add("avoid_or_risk_pool", d)
    for d in ranked:
        if d.get("setup_v72") == "T0-LEAD" or (d.get("setup_v72") == "T0-NEW" and d.get("confidence") == "high"):
            add("main_attack_pool", d)
    for d in ranked:
        if d.get("setup_v72") == "T0-ROTATE":
            add("theme_rotation_pool", d)
    for d in ranked:
        if d.get("entry_tag") == "board_watch":
            add("board_watch_pool", d)
    for d in ranked:
        if d.get("setup_v72") in {"T0-NEW", "T0-GENERAL"} and d.get("confidence") != "high" and not _is_avoid(d):
            add("confirmation_watch_pool", d)

    return pools


def build_intraday_anchors_v72(top_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in top_candidates or []:
        key = _anchor_key(d)
        compact = _compact_decision(d)
        compact.update({
            "setup_v71_compat": d.get("setup_v71_compat"),
            "anchors": DEFAULT_V72_ANCHORS.get(key, DEFAULT_V72_ANCHORS["none"]),
            "risk_detail": d.get("risk_detail") or {},
            "signal_summary": d.get("signal_summary") or {},
        })
        out.append(compact)
    return out


def setup_stats_v72(decisions: List[Dict[str, Any]]) -> Dict[str, int]:
    stats: Dict[str, int] = {}
    for d in decisions or []:
        setup = str(d.get("setup_v72") or "none")
        stats[setup] = stats.get(setup, 0) + 1
    return stats


def shape_v7_2_output(
    decisions: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
    max_candidates: int = 30,
    watch_tier_max: int = 50,
) -> Dict[str, Any]:
    ranked = sorted(decisions or [], key=lambda x: x.get("final_score") or 0, reverse=True)
    top = [d for d in ranked if d.get("setup_v72") != "none"][:max_candidates]
    watch = ranked[:watch_tier_max]
    pools = build_candidate_pools(ranked, pool_max=max(10, max_candidates // 2))
    return {
        "version": "premarket_v7_2",
        "meta": meta or {},
        "setup_stats": setup_stats_v72(decisions),
        "candidate_pools": pools,
        "top_candidates": top,
        "watch_tier": watch,
        "all_candidates_debug": ranked,
        "intraday_anchors": build_intraday_anchors_v72(top[:20]),
    }


def write_v7_2_outputs(
    output_dir: str,
    decisions: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
    max_candidates: int = 30,
    watch_tier_max: int = 50,
    analysis_filename: str = "analysis_v7_2.json",
    anchors_filename: str = "intraday_anchors.json",
) -> Dict[str, str]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    shaped = shape_v7_2_output(
        decisions,
        meta=meta,
        max_candidates=max_candidates,
        watch_tier_max=watch_tier_max,
    )
    analysis_path = out_dir / analysis_filename
    anchors_path = out_dir / anchors_filename
    analysis_path.write_text(json.dumps(shaped, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    anchors_path.write_text(json.dumps(shaped["intraday_anchors"], ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"analysis_path": str(analysis_path), "anchors_path": str(anchors_path)}


def _self_test() -> None:
    decisions = [
        {"code": "000001", "name": "A", "setup_v72": "T0-NEW", "setup_v71_compat": "D", "confidence": "high", "final_score": 88, "auction_detail": {"qiangchou_primary_signal": "9:20-9:25"}, "theme_detail": {"matched_tags": ["算力"], "matched_plate": "算力"}},
        {"code": "000002", "name": "B", "setup_v72": "T0-LEAD", "setup_v71_compat": "A", "confidence": "high", "final_score": 80, "entry_tag": "board_watch"},
        {"code": "000003", "name": "C", "setup_v72": "none", "setup_v71_compat": "none", "confidence": "none", "final_score": 0, "entry_tag": "avoid"},
        {"code": "000004", "name": "D", "setup_v72": "T0-ROTATE", "setup_v71_compat": "B", "confidence": "high", "final_score": 70},
    ]
    out = shape_v7_2_output(decisions)
    assert out["version"] == "premarket_v7_2"
    assert out["setup_stats"]["T0-NEW"] == 1
    assert out["candidate_pools"]["main_attack_pool"], out
    assert out["candidate_pools"]["theme_rotation_pool"], out
    assert out["candidate_pools"]["avoid_or_risk_pool"], out
    assert out["intraday_anchors"][0]["setup_v72"] == "T0-NEW"
    print("output v7.2 _self_test passed")


if __name__ == "__main__":
    _self_test()
