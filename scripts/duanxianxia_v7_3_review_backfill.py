#!/usr/bin/env python3
"""Recompute v7.3 review metrics after close_pct/excess_return backfill.

Usage:
  python scripts/duanxianxia_v7_3_review_backfill.py \
    --analysis projects/duanxianxia/reports/2026-04-29/premarket/120615_analysis_v7_3.json \
    --flat projects/duanxianxia/reports/2026-04-29/premarket/120615_all_candidates_flat.csv

This patches the source v7.3 JSON so pool_performance/review_diagnostics are
consistent with the already-backfilled flat CSV/JSONL bundle.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from duanxianxia_v7_3_output import load_performance_map_from_flat, recompute_v73_review_metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Recompute v7.3 pool performance and diagnostics after review backfill")
    parser.add_argument("--analysis", required=True, help="Path to *_analysis_v7_3.json")
    parser.add_argument("--flat", required=True, help="Path to backfilled *_all_candidates_flat.csv or .jsonl")
    parser.add_argument("--output", default="", help="Output path. Default overwrites --analysis")
    parser.add_argument("--max-candidates", type=int, default=30)
    parser.add_argument("--watch-tier-max", type=int, default=60)
    parser.add_argument("--pool-max", type=int, default=15)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    analysis_path = Path(args.analysis)
    flat_path = Path(args.flat)
    output_path = Path(args.output) if args.output else analysis_path

    shaped = json.loads(analysis_path.read_text(encoding="utf-8"))
    perf_map = load_performance_map_from_flat(flat_path)
    updated = recompute_v73_review_metrics(
        shaped,
        perf_map,
        max_candidates=args.max_candidates,
        watch_tier_max=args.watch_tier_max,
        pool_max=args.pool_max,
    )
    output_path.write_text(json.dumps(updated, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    summary = {
        "analysis": str(analysis_path),
        "flat": str(flat_path),
        "output": str(output_path),
        "performance_rows_loaded": len(perf_map),
        "pool_performance": updated.get("pool_performance"),
        "diagnostic_counts": {k: len(v or []) for k, v in (updated.get("review_diagnostics") or {}).items()},
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    else:
        print("v7.3 review metrics recomputed")
        print(f"- performance rows loaded: {summary['performance_rows_loaded']}")
        print(f"- output: {summary['output']}")
        print(f"- diagnostics: {summary['diagnostic_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
