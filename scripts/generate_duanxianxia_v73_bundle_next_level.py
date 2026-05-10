#!/usr/bin/env python3
"""Professional next-level wrapper for v7.3 bundle generation.

This wrapper applies the v7.3 next-level overlay, then patches reporting so the
new pools are visible and review profiles are computed from full rows instead of
compact diagnostics.
"""
from __future__ import annotations

import importlib.util
import sys
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import duanxianxia_v7_3_next_level_patch  # noqa: F401


def _load_base():
    path = SCRIPTS_DIR / "generate_duanxianxia_v73_bundle.py"
    spec = importlib.util.spec_from_file_location("generate_duanxianxia_v73_bundle_base", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _as_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL"):
            return None
        return float(v)
    except Exception:
        return None


def _install_professional_reporting(base) -> None:
    for pool in ["broad_repair_momentum_pool", "high_cost_repair_watch_pool"]:
        if pool not in base.POOL_ORDER:
            insert_at = base.POOL_ORDER.index("board_watch_pool") if pool == "broad_repair_momentum_pool" else base.POOL_ORDER.index("soft_avoid_repair_pool")
            base.POOL_ORDER.insert(insert_at, pool)

    def full_row_profiles(shaped: Dict[str, Any]) -> Dict[str, Any]:
        rows = list(shaped.get("all_candidates_action_ranked") or [])
        by_code = {base.code_key(r.get("code")): r for r in rows}
        diag = shaped.get("review_diagnostics") or {}

        def full_items(key: str) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            for item in diag.get(key) or []:
                code = base.code_key(item.get("code"))
                out.append(by_code.get(code, item))
            return out

        def metric(row: Dict[str, Any], key: str) -> Any:
            if key in row:
                return row.get(key)
            perf = base.performance_of(row)
            if key in perf:
                return perf.get(key)
            auction = row.get("auction_detail") or {}
            signal = row.get("signal_summary") or {}
            if key == "auction_pct":
                return perf.get("auction_pct") or row.get("auction_pct") or auction.get("latest_change_pct")
            if key in auction:
                return auction.get(key)
            if key in signal:
                return signal.get(key)
            return None

        def stats(items: List[Dict[str, Any]], key: str) -> Dict[str, Any]:
            nums = sorted(x for x in (_as_float(metric(r, key)) for r in items) if x is not None)
            if not nums:
                return {"count": 0}
            return {"count": len(nums), "min": round(nums[0], 2), "p25": round(nums[len(nums)//4], 2), "median": round(median(nums), 2), "p75": round(nums[(len(nums)*3)//4], 2), "max": round(nums[-1], 2), "avg": round(sum(nums)/len(nums), 2)}

        def bucket_pct(v: Any) -> str:
            x = _as_float(v)
            if x is None: return "missing"
            if x < -5: return "<-5"
            if x < -2: return "[-5,-2)"
            if x < 0: return "[-2,0)"
            if x < 2: return "[0,2)"
            if x < 5: return "[2,5)"
            if x < 7: return "[5,7)"
            if x < 9: return "[7,9)"
            return ">=9"

        def bucket_amt(v: Any) -> str:
            x = _as_float(v)
            if x is None: return "missing"
            if x < 500: return "<500w"
            if x < 1000: return "500-1000w"
            if x < 3000: return "1000-3000w"
            if x < 8000: return "3000-8000w"
            return ">=8000w"

        profile_keys = ["auction_setup_type", "action_type", "action_quality", "setup_v72", "confidence", "entry_tag"]
        numeric_keys = ["auction_pct", "auction_strength", "auction_amount_wan", "liquidity_score", "theme_strength_t0", "source_evidence_score", "source_family_count", "final_score", "expected_return_score", "action_score"]

        def profile(items: List[Dict[str, Any]]) -> Dict[str, Any]:
            if not items:
                return {}
            p: Dict[str, Any] = {"count": len(items)}
            for key in profile_keys:
                p[f"{key}_top"] = Counter(str(metric(r, key) or r.get(key) or "missing") for r in items).most_common(10)
            p["auction_pct_bucket"] = Counter(bucket_pct(metric(r, "auction_pct")) for r in items).most_common()
            p["auction_amount_bucket"] = Counter(bucket_amt(metric(r, "auction_amount_wan")) for r in items).most_common()
            p["numeric_stats"] = {key: stats(items, key) for key in numeric_keys}
            p["top_names"] = [f"{r.get('code')} {r.get('name')}" for r in items[:20]]
            return p

        shaped["review_profiles"] = {
            "missed_winners": profile(full_items("missed_winners")),
            "debug_missed_winners": profile(full_items("debug_missed_winners")),
            "avoid_missed_winners": profile(full_items("avoid_missed_winners")),
            "soft_avoid_missed_winners": profile(full_items("soft_avoid_missed_winners")),
            "fake_strength_watch_winners": profile(full_items("fake_strength_watch_winners")),
            "broad_repair_winners": profile(full_items("broad_repair_winners")),
            "broad_repair_false_positives": profile(full_items("broad_repair_false_positives")),
            "high_cost_repair_watch_winners": profile(full_items("high_cost_repair_watch_winners")),
            "false_positives": profile(full_items("false_positives")),
            "high_cost_confirmation_failures": profile(full_items("high_cost_confirmation_failures")),
        }
        return shaped

    base.add_review_profiles = full_row_profiles


if __name__ == "__main__":
    base = _load_base()
    _install_professional_reporting(base)
    raise SystemExit(base.main())
