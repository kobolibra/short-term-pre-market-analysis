#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0149: 逐日审计每天 premarket analysis_v9.json 的 all_candidates 中
 auction_amount_wan / auction_amount_pct 覆盖率，定位缺字泄漏
 是旧文件历史遗留还是生产至今仍在漏。紧凑 stdout(置尾防截)。
"""
from __future__ import annotations
import json, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import DEFAULT_PROJECT_ROOT


def _has_wan(cand):
    full = cand.get("full") or {}
    ad = full.get("auction_detail") or {}
    v = cand.get("auction_amount_wan")
    if v is None:
        v = ad.get("auction_amount_wan")
    return v not in (None, "", "-")


def _has_pct(cand):
    full = cand.get("full") or {}
    ad = full.get("auction_detail") or {}
    v = ad.get("auction_amount_pct")
    if v is None:
        v = (cand.get("auction_detail") or {}).get("auction_amount_pct")
    return v not in (None, "", "-")


def main():
    root = Path(DEFAULT_PROJECT_ROOT)
    rep = root / "reports"
    per_day = []
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if not files:
            continue
        try:
            analysis = json.loads(files[-1].read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = [c for c in (analysis.get("all_candidates") or []) if isinstance(c, dict) and c.get("code")]
        n = len(cands)
        if not n:
            continue
        wan = sum(1 for c in cands if _has_wan(c))
        pct = sum(1 for c in cands if _has_pct(c))
        per_day.append({"date": dd.name, "n": n,
                        "wan_cov": round(wan / n, 3), "pct_cov": round(pct / n, 3)})
    out = {"job": "0149_field_coverage_audit", "n_days": len(per_day), "per_day": per_day}
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
