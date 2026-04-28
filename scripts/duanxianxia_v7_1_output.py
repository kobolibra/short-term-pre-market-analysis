"""
duanxianxia_v7_1_output.py — v7.1 输出整形与 intraday anchors 写盘
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from duanxianxia_v7_1_setup_engine import setup_stats


DEFAULT_INTRADAY_ANCHORS = {
    "A_ice": ["09:35 是否重新站上 VWAP", "09:45 是否补量", "10:30 不破开盘低点", "午前是否回封/维持强势"],
    "A": ["09:35 是否高开后不回落", "09:45 封单/成交比是否维持", "10:00 板块前排是否继续加强"],
    "B": ["09:35 同题材补涨是否扩散", "10:00 龙头是否不炸", "盘中是否出现同圈子新首板"],
    "C1": ["09:35 新题材是否扩散", "09:45 是否出现第二只助攻", "10:30 资金是否继续回流"],
    "C2": ["09:35 轮动题材是否承接", "10:00 竞价强股是否维持红盘", "午前是否被主线抽血"],
    "D": ["09:35 是否快速兑现", "09:45 若破 VWAP 直接降权", "10:00 只保留成交额继续放大的票"],
    "E": ["09:30 一字封单是否稳定", "09:35 若开板是否快速回封", "09:45 单封单票严禁追高"],
    "none": ["仅观察,不作为盘中主候选"],
}


def shape_v7_1_output(
    decisions: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
    max_candidates: int = 30,
) -> Dict[str, Any]:
    """生成最终 analysis_v7_1.json 结构。"""
    top = [d for d in decisions if d.get("setup_id") != "none"][:max_candidates]
    none_count = len([d for d in decisions if d.get("setup_id") == "none"])
    total = len(decisions)
    stats = setup_stats(decisions)
    stats["none_ratio"] = round(none_count / total, 4) if total else 0

    return {
        "version": "premarket_v7_1",
        "meta": meta or {},
        "setup_stats": stats,
        "top_candidates": top,
        "all_candidates_debug": decisions,
        "intraday_anchors": build_intraday_anchors(top),
    }


def build_intraday_anchors(top_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """按候选生成盘中锚点。"""
    anchors: List[Dict[str, Any]] = []
    for d in top_candidates or []:
        sid = d.get("setup_id") or "none"
        anchors.append({
            "code": d.get("code"),
            "name": d.get("name"),
            "setup_id": sid,
            "setup_name": d.get("setup_name"),
            "priority": d.get("priority"),
            "anchors": DEFAULT_INTRADAY_ANCHORS.get(sid, DEFAULT_INTRADAY_ANCHORS["none"]),
            "label_snapshot": d.get("label_snapshot") or {},
        })
    return anchors


def write_v7_1_outputs(
    output_dir: str,
    decisions: List[Dict[str, Any]],
    meta: Optional[Dict[str, Any]] = None,
    max_candidates: int = 30,
    analysis_filename: str = "analysis_v7_1.json",
    anchors_filename: str = "intraday_anchors.json",
) -> Dict[str, str]:
    """写 analysis_v7_1.json 与 intraday_anchors.json。返回写出的路径。"""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    shaped = shape_v7_1_output(decisions, meta=meta, max_candidates=max_candidates)

    analysis_path = out_dir / analysis_filename
    anchors_path = out_dir / anchors_filename
    analysis_path.write_text(json.dumps(shaped, ensure_ascii=False, indent=2), encoding="utf-8")
    anchors_path.write_text(json.dumps(shaped["intraday_anchors"], ensure_ascii=False, indent=2), encoding="utf-8")
    return {"analysis_path": str(analysis_path), "anchors_path": str(anchors_path)}


def _self_test() -> None:
    decisions = [
        {"code": "000001", "name": "A股", "setup_id": "A", "setup_name": "主龙跳", "priority": 3.0, "label_snapshot": {}},
        {"code": "000002", "name": "无", "setup_id": "none", "setup_name": "未入选", "priority": 0, "label_snapshot": {}},
    ]
    out = shape_v7_1_output(decisions, meta={"date": "2026-04-28"}, max_candidates=30)
    assert out["version"] == "premarket_v7_1"
    assert out["setup_stats"]["A"] == 1
    assert out["setup_stats"]["none"] == 1
    assert out["setup_stats"]["none_ratio"] == 0.5
    assert len(out["top_candidates"]) == 1
    assert out["intraday_anchors"][0]["setup_id"] == "A"
    print("output _self_test passed")


if __name__ == "__main__":
    _self_test()
