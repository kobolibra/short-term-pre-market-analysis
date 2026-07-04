#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_show_selection.py — Task 0145.

读磁盘上指定交易日的盘前 analysis_v9.json,把当天真实的选股结果打出来:
市场档位(regime)+门控、BUY/WATCH/DROP 统计、BUY/WATCH 名单(带 edge 分与关键竞价字段)、
edge 前10。紧凑 stdout(防 tail 截断)。
用法: python3 scripts/duanxianxia_show_selection.py [YYYY-MM-DD]
"""
from __future__ import annotations
import json, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import DEFAULT_PROJECT_ROOT

DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-07-03"


def brief(c):
    return {
        "code": c.get("code"), "name": c.get("name"),
        "action": c.get("action_type"), "edge": c.get("edge_score"),
        "setup": c.get("setup"), "alpha": c.get("alpha_type"),
        "auction_pct": c.get("auction_pct"),
        "auction_amount_wan": c.get("auction_amount_wan"),
        "risk": c.get("risk_flag"),
    }


def main():
    root = Path(DEFAULT_PROJECT_ROOT)
    pm = root / "reports" / DATE / "premarket"
    out = {"date": DATE, "dir_exists": pm.is_dir()}
    files = sorted(pm.glob("*analysis_v9*.json")) if pm.is_dir() else []
    if not files:
        out["found"] = False
        out["listing"] = [p.name for p in pm.glob("*")] if pm.is_dir() else []
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return
    analysis = json.loads(files[-1].read_text(encoding="utf-8"))
    meta = analysis.get("meta") or {}
    cands = analysis.get("all_candidates") or analysis.get("top_candidates") or []
    buys = [brief(c) for c in cands if c.get("action_type") == "BUY"]
    watch = [brief(c) for c in cands if c.get("action_type") == "WATCH"]
    out.update({
        "found": True,
        "file": files[-1].name,
        "candidate_count": len(cands),
        "action_gate": meta.get("action_gate"),
        "action_stats": analysis.get("action_stats"),
        "alpha_stats": analysis.get("alpha_stats"),
        "BUY": buys,
        "WATCH_top15": watch[:15],
        "top10_by_edge": [brief(c) for c in cands[:10]],
    })
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
