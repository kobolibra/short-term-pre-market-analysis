"""v7.2 output shaping and intraday anchors.

Single output path:
- analysis_v7_2.json
- intraday_anchors.json

intraday_anchors.json keeps top candidates by final_score and includes both
setup_v72 and setup_v71_compat for downstream compatibility.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_V72_ANCHORS = {
    "T0-LEAD": [
        "开盘 1 分钟内是否打板",
        "封单是否 ≥ 1 亿",
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
        "板块龙头/龙二是否同步红盘",
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


def build_intraday_anchors_v72(top_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in top_candidates or []:
        key = _anchor_key(d)
        out.append({
            "code": d.get("code"),
            "name": d.get("name"),
            "setup_v72": d.get("setup_v72"),
            "setup_v71_compat": d.get("setup_v71_compat"),
            "confidence": d.get("confidence"),
            "final_score": d.get("final_score"),
            "today_signal_raw": d.get("today_signal_raw"),
            "auction_strength": d.get("auction_strength"),
            "theme_strength_t0": d.get("theme_strength_t0"),
            "hotness_score": d.get("hotness_score"),
            "risk_flag": d.get("risk_flag"),
            "anchors": DEFAULT_V72_ANCHORS.get(key, DEFAULT_V72_ANCHORS["none"]),
            "risk_detail": d.get("risk_detail") or {},
        })
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
    return {
        "version": "premarket_v7_2",
        "meta": meta or {},
        "setup_stats": setup_stats_v72(decisions),
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
        {"code": "000001", "name": "A", "setup_v72": "T0-NEW", "setup_v71_compat": "D", "confidence": "high", "final_score": 88},
        {"code": "000002", "name": "B", "setup_v72": "none", "setup_v71_compat": "none", "confidence": "none", "final_score": 0},
    ]
    out = shape_v7_2_output(decisions)
    assert out["version"] == "premarket_v7_2"
    assert out["setup_stats"]["T0-NEW"] == 1
    assert out["intraday_anchors"][0]["setup_v72"] == "T0-NEW"
    print("output v7.2 _self_test passed")


if __name__ == "__main__":
    _self_test()
