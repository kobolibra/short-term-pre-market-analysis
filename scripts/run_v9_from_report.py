#!/usr/bin/env python3
"""
run_v9_from_report.py — standalone runner that produces analysis_v9.json from an
existing premarket report, WITHOUT modifying duanxianxia_batch.py.

The premarket batch (duanxianxia_batch.py) writes a report JSON whose dict holds
both the captured items (report["items"], each with a capture_path) and the v5
premarket analysis (report["analysis"]). This runner loads such a report, runs
the v9 full-data assembly via duanxianxia_v9_from_report, and writes
analysis_v9.json next to the report (or to --out).

This is the zero-touch way to activate v9 in the existing pipeline: it imports
only the lightweight adapter (not duanxianxia_batch.py), so there is no heavy
import side effect and no risk to the v5 flow.

Usage:
    python run_v9_from_report.py path/to/report.json
    python run_v9_from_report.py path/to/report.json --out path/to/analysis_v9.json
    python run_v9_from_report.py --report-root /path/to/reports   # newest *.json

Typical cron wiring (after the premarket batch finishes):
    python duanxianxia_batch.py --premarket ...
    python run_v9_from_report.py --report-root "$REPORT_ROOT"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

# Make sibling modules importable when run from anywhere.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import duanxianxia_v9_from_report as v9fr
except Exception as exc:  # pragma: no cover
    sys.stderr.write(f"[run_v9_from_report] cannot import adapter: {exc}\n")
    raise

DEFAULT_FILENAME = "analysis_v9.json"


def _load_report(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"report JSON is not an object: {path}")
    return data


def _find_latest_report(report_root: str) -> Optional[str]:
    """Return the newest *.json under report_root (recursively), or None."""
    candidates: List[str] = []
    for base, _dirs, files in os.walk(report_root):
        for name in files:
            if not name.lower().endswith(".json"):
                continue
            # Skip our own outputs so we don't pick an analysis_v9.json as input.
            if name == DEFAULT_FILENAME:
                continue
            candidates.append(os.path.join(base, name))
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def run(
    report_path: str,
    out_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """Build the v9 block for one report and write it. Returns the output path."""
    report = _load_report(report_path)
    analysis = report.get("analysis") if isinstance(report.get("analysis"), dict) else None
    block = v9fr.build_v9_block(report, analysis, params=params)

    if out_path is None:
        out_path = os.path.join(os.path.dirname(os.path.abspath(report_path)), DEFAULT_FILENAME)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(block, fh, ensure_ascii=False, indent=2)

    enabled = bool(block.get("enabled")) if isinstance(block, dict) else False
    cand = len(block.get("candidates") or []) if isinstance(block, dict) else 0
    reason = block.get("reason") if isinstance(block, dict) else None
    status = "enabled" if enabled else f"disabled ({reason})"
    sys.stderr.write(
        f"[run_v9_from_report] wrote {out_path} | v9 {status} | candidates={cand}\n"
    )
    return out_path


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Produce analysis_v9.json from an existing premarket report."
    )
    parser.add_argument("report", nargs="?", help="Path to a premarket report JSON.")
    parser.add_argument(
        "--report-root",
        dest="report_root",
        default=None,
        help="Directory to scan for the newest *.json report (used if 'report' is omitted).",
    )
    parser.add_argument(
        "--out",
        dest="out",
        default=None,
        help="Output path for analysis_v9.json (defaults to next to the report).",
    )
    args = parser.parse_args(argv)

    report_path = args.report
    if not report_path and args.report_root:
        report_path = _find_latest_report(args.report_root)
        if not report_path:
            sys.stderr.write(
                f"[run_v9_from_report] no report JSON found under {args.report_root}\n"
            )
            return 2
    if not report_path:
        parser.error("provide a report path or --report-root")

    if not os.path.isfile(report_path):
        sys.stderr.write(f"[run_v9_from_report] report not found: {report_path}\n")
        return 2

    try:
        run(report_path, out_path=args.out)
    except Exception as exc:
        sys.stderr.write(f"[run_v9_from_report] failed: {type(exc).__name__}: {exc}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
