#!/usr/bin/env python3
"""Audit duanxianxia selector field lineage.

Purpose
-------
This script is deliberately conservative.  It does **not** claim that a field is
leaking into production just because a post-close value exists in a review file.
Instead it separates:

1. production decision rows and their premarket-visible fields;
2. review-only performance fields;
3. ambiguous names that require human/fixture verification.

It is meant to prevent exactly the failure mode where we over-interpret a few
review days or confuse review fields with production inputs.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

REVIEW_ONLY_KEYS = {
    "close_pct",
    "excess_return",
    "open_pct",
    "prev_close",
    "day_open",
    "day_high",
    "day_low",
    "day_close",
    "dailyline_found",
    "derived_performance",
    "performance",
}

PREMARKET_PRICE_KEYS = {
    "auction_change_pct",
    "auction_change_pct_text",
    "竞价涨幅",
    "auction_pct",
}

AMBIGUOUS_PRICE_KEYS = {
    "latest_change_pct",
    "最新涨幅",
    "涨幅",
}

PRODUCTION_CONTAINER_KEYS = {
    "auction_detail",
    "theme_detail",
    "signal_summary",
    "label_snapshot",
    "score_weights",
    "risk_detail",
    "t1_adjustments",
}

ROW_SOURCES = (
    "top_candidates",
    "actionable_candidates",
    "expected_return_candidates",
    "watch_tier",
    "all_candidates_action_ranked",
    "all_candidates_expected_return_ranked",
    "all_candidates_debug",
)


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_rows(shaped: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    seen: set[Tuple[str, str]] = set()
    for source in ROW_SOURCES:
        rows = shaped.get(source) or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code") or row.get("股票代码") or row.get("代码") or "")
            key = (source, code + ":" + str(id(row)))
            if key in seen:
                continue
            seen.add(key)
            yield source, row


def _flatten(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                _flatten(key, v, out)
            else:
                out[key] = v


def _code(row: Dict[str, Any]) -> str:
    return str(row.get("code") or row.get("股票代码") or row.get("代码") or "").zfill(6)


def _nonnull(v: Any) -> bool:
    return v not in (None, "", "None", "null", "NULL", "-")


def _read_flat_codes(path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    if not path:
        return {}
    if not path.exists():
        raise FileNotFoundError(path)
    rows: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".jsonl":
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
    else:
        with path.open("r", encoding="utf-8-sig", newline="") as fp:
            rows = list(csv.DictReader(fp))
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("code") or row.get("股票代码") or row.get("代码") or "").zfill(6)
        if code.strip("0"):
            out[code] = row
    return out


def audit_analysis(analysis_path: Path, flat_path: Optional[Path] = None) -> Dict[str, Any]:
    shaped = _load_json(analysis_path)
    flat = _read_flat_codes(flat_path)
    rows = list(_iter_rows(shaped))

    key_presence: Counter[str] = Counter()
    review_key_presence: Counter[str] = Counter()
    ambiguous_presence: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    examples: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    row_count = 0
    for source, row in rows:
        row_count += 1
        source_counts[source] += 1
        action = str(row.get("action_type") or row.get("action_quality") or "missing")
        action_counts[action] += 1
        flat_row: Dict[str, Any] = {}
        _flatten("", row, flat_row)
        for k, v in flat_row.items():
            base = k.split(".")[-1]
            if not _nonnull(v):
                continue
            if base in REVIEW_ONLY_KEYS:
                review_key_presence[k] += 1
            elif base in AMBIGUOUS_PRICE_KEYS:
                ambiguous_presence[k] += 1
            else:
                key_presence[k] += 1

        code = _code(row)
        if code in flat:
            for k in REVIEW_ONLY_KEYS:
                if _nonnull(flat[code].get(k)) and len(examples["flat_review_join_examples"]) < 10:
                    examples["flat_review_join_examples"].append({"code": code, "review_key": k, "value": flat[code].get(k)})
                    break
        for k in AMBIGUOUS_PRICE_KEYS:
            if _nonnull(row.get(k)) and len(examples["top_level_ambiguous_price_examples"]) < 10:
                examples["top_level_ambiguous_price_examples"].append({"code": code, "key": k, "value": row.get(k), "action_type": action})
        for container in PRODUCTION_CONTAINER_KEYS:
            obj = row.get(container)
            if isinstance(obj, dict):
                for k in AMBIGUOUS_PRICE_KEYS:
                    if _nonnull(obj.get(k)) and len(examples[f"{container}_ambiguous_price_examples"]) < 10:
                        examples[f"{container}_ambiguous_price_examples"].append({"code": code, "key": f"{container}.{k}", "value": obj.get(k), "action_type": action})
                for k in PREMARKET_PRICE_KEYS:
                    if _nonnull(obj.get(k)) and len(examples[f"{container}_premarket_price_examples"]) < 10:
                        examples[f"{container}_premarket_price_examples"].append({"code": code, "key": f"{container}.{k}", "value": obj.get(k), "action_type": action})

    warnings: List[str] = []
    if ambiguous_presence:
        warnings.append("ambiguous price field names are present; verify capture timestamp/schema before interpreting them as production cost or review close change")
    if review_key_presence:
        warnings.append("review-only fields are present in rows; this is acceptable for backfilled analysis, but selector code must not branch on them")
    if flat:
        warnings.append("flat review file was loaded; joined values are explicitly review-only")

    return {
        "analysis": str(analysis_path),
        "flat": str(flat_path) if flat_path else None,
        "row_count": row_count,
        "source_counts": dict(source_counts),
        "action_counts": dict(action_counts),
        "production_like_key_top30": key_presence.most_common(30),
        "review_only_key_presence": review_key_presence.most_common(50),
        "ambiguous_price_key_presence": ambiguous_presence.most_common(50),
        "examples": dict(examples),
        "warnings": warnings,
        "conclusion": "This audit is a boundary report, not a leak verdict.  Treat ambiguous names as schema work items and review fields as post-close-only unless code inspection proves otherwise.",
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Audit field lineage / production-review boundary for duanxianxia analysis JSON")
    p.add_argument("--analysis", required=True)
    p.add_argument("--flat", default="")
    p.add_argument("--output", default="")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    result = audit_analysis(Path(args.analysis), Path(args.flat) if args.flat else None)
    text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    if args.json or not args.output:
        print(text)
    else:
        print(f"field lineage audit written: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
