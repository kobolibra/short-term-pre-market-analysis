#!/usr/bin/env python3
"""Validation-first evaluator for duanxianxia selector outputs.

This is intentionally a measurement tool, not a curve-fitting tool.
It can evaluate any generated analysis JSON that already contains review
performance fields, or join an external flat CSV/JSONL produced after close.

Design principles
-----------------
- Never infer a market/board/sector rule from a tiny sample.
- Report sample size and mark small buckets as exploratory only.
- Separate decision quality (BUY/WATCH/REJECT/AVOID/action_type) from ranking
  quality (expected_return_rank/action_score/final_score).
- Optimize for expectancy: hit rate, average win, average loss, payoff ratio,
  tail-loss rate, and missed-winner diagnostics.
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple

PERFORMANCE_KEYS = ("auction_pct", "open_pct", "close_pct", "excess_return", "dailyline_found", "prev_close", "day_open", "day_high", "day_low", "day_close")
ROW_SOURCES = ("all_candidates_action_ranked", "all_candidates_debug", "top_candidates", "actionable_candidates", "watch_tier")


def _f(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _code(v: Any) -> str:
    text = str(v or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_unique_rows(shaped: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    seen: set[str] = set()
    for source in ROW_SOURCES:
        rows = shaped.get(source) or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            code = _code(row.get("code") or row.get("股票代码") or row.get("代码"))
            action = str(row.get("action_type") or "")
            key = code or f"anon:{id(row)}"
            # Prefer the first full ranked row.  Later compact duplicates are skipped.
            if key in seen:
                continue
            seen.add(key)
            copied = dict(row)
            copied["_row_source"] = source
            copied["_code"] = code
            copied["_action"] = action
            yield copied


def _perf(row: Dict[str, Any]) -> Dict[str, Any]:
    src = row.get("derived_performance") or row.get("performance") or row
    out: Dict[str, Any] = {}
    if isinstance(src, dict):
        for k in PERFORMANCE_KEYS:
            if src.get(k) is not None:
                out[k] = src.get(k)
    for k in PERFORMANCE_KEYS:
        if k not in out and row.get(k) is not None:
            out[k] = row.get(k)
    return out


def _read_flat(path: Path) -> Dict[str, Dict[str, Any]]:
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
        code = _code(row.get("code") or row.get("股票代码") or row.get("代码"))
        if not code:
            continue
        perf: Dict[str, Any] = {}
        for k in PERFORMANCE_KEYS:
            if row.get(k) not in (None, ""):
                perf[k] = row.get(k)
        out[code] = perf
    return out


def _attach_flat(row: Dict[str, Any], flat: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    if not flat:
        return row
    code = row.get("_code") or _code(row.get("code"))
    if code not in flat:
        return row
    copied = dict(row)
    perf = dict(_perf(row))
    perf.update(flat[code])
    copied["derived_performance"] = perf
    for k, v in perf.items():
        copied[k] = v
    return copied


def _avg(vals: List[float]) -> Optional[float]:
    return sum(vals) / len(vals) if vals else None


def _percentile(vals: List[float], q: float) -> Optional[float]:
    if not vals:
        return None
    s = sorted(vals)
    idx = min(len(s) - 1, max(0, int(round((len(s) - 1) * q))))
    return s[idx]


def _bucket_metrics(rows: List[Dict[str, Any]], min_sample: int) -> Dict[str, Any]:
    ex: List[float] = []
    close: List[float] = []
    no_perf = 0
    for row in rows:
        p = _perf(row)
        e = _f(p.get("excess_return"), None)
        c = _f(p.get("close_pct"), None)
        if e is None:
            no_perf += 1
        else:
            ex.append(e)
        if c is not None:
            close.append(c)
    wins = [x for x in ex if x > 0]
    losses = [x for x in ex if x < 0]
    avg_win = _avg(wins)
    avg_loss = _avg(losses)
    payoff = None
    if avg_win is not None and avg_loss is not None and avg_loss != 0:
        payoff = avg_win / abs(avg_loss)
    expectancy = _avg(ex)
    return {
        "count": len(rows),
        "with_performance": len(ex),
        "missing_performance": no_perf,
        "is_statistically_actionable": len(ex) >= min_sample,
        "sample_status": "actionable" if len(ex) >= min_sample else "exploratory_only",
        "hit_rate_excess_gt_0": round(len(wins) / len(ex), 4) if ex else None,
        "avg_excess_return": round(expectancy, 4) if expectancy is not None else None,
        "median_excess_return": round(median(ex), 4) if ex else None,
        "p25_excess_return": round(_percentile(ex, 0.25), 4) if ex else None,
        "p75_excess_return": round(_percentile(ex, 0.75), 4) if ex else None,
        "avg_win": round(avg_win, 4) if avg_win is not None else None,
        "avg_loss": round(avg_loss, 4) if avg_loss is not None else None,
        "payoff_ratio_avg_win_abs_avg_loss": round(payoff, 4) if payoff is not None else None,
        "tail_loss_rate_le_-3": round(sum(1 for x in ex if x <= -3) / len(ex), 4) if ex else None,
        "big_win_rate_ge_5": round(sum(1 for x in ex if x >= 5) / len(ex), 4) if ex else None,
        "avg_close_pct": round(_avg(close), 4) if close else None,
    }


def _group(rows: List[Dict[str, Any]], key: str) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        value = row.get(key)
        if value is None and key == "durable_pattern":
            value = row.get("gate_reason")
        if value is None and key == "action_quality":
            value = row.get("signal_quality")
        out[str(value or "missing")].append(row)
    return out


def _best_worst(rows: List[Dict[str, Any]], action_filter: Optional[set[str]], n: int, reverse: bool) -> List[Dict[str, Any]]:
    items: List[Tuple[float, Dict[str, Any]]] = []
    for row in rows:
        if action_filter is not None and str(row.get("action_type")) not in action_filter:
            continue
        e = _f(_perf(row).get("excess_return"), None)
        if e is None:
            continue
        items.append((e, row))
    items.sort(key=lambda x: x[0], reverse=reverse)
    out: List[Dict[str, Any]] = []
    for e, row in items[:n]:
        out.append({
            "code": row.get("code") or row.get("_code"),
            "name": row.get("name"),
            "action_type": row.get("action_type"),
            "durable_pattern": row.get("durable_pattern"),
            "gate_reason": row.get("gate_reason"),
            "excess_return": round(e, 4),
            "auction_pct": _perf(row).get("auction_pct"),
            "close_pct": _perf(row).get("close_pct"),
            "final_score": row.get("final_score"),
            "action_score": row.get("action_score"),
            "edge_score": row.get("edge_score"),
        })
    return out


def evaluate(analysis_paths: List[Path], flat_paths: List[Path], min_sample: int = 30, top_n: int = 20) -> Dict[str, Any]:
    flat: Dict[str, Dict[str, Any]] = {}
    for p in flat_paths:
        flat.update(_read_flat(p))
    all_rows: List[Dict[str, Any]] = []
    file_summaries: List[Dict[str, Any]] = []
    for path in analysis_paths:
        shaped = _load_json(path)
        rows = [_attach_flat(r, flat) for r in _iter_unique_rows(shaped)]
        for r in rows:
            r["_analysis_path"] = str(path)
            r["_selector"] = ((shaped.get("meta") or {}).get("selector") or shaped.get("version") or "unknown")
        all_rows.extend(rows)
        file_summaries.append({"analysis": str(path), "rows": len(rows), "selector": ((shaped.get("meta") or {}).get("selector") or shaped.get("version") or "unknown")})

    result: Dict[str, Any] = {
        "analysis_files": [str(p) for p in analysis_paths],
        "flat_files": [str(p) for p in flat_paths],
        "min_sample_for_actionable_bucket": min_sample,
        "files": file_summaries,
        "overall": _bucket_metrics(all_rows, min_sample),
        "by_action_type": {k: _bucket_metrics(v, min_sample) for k, v in sorted(_group(all_rows, "action_type").items())},
        "by_action_quality": {k: _bucket_metrics(v, min_sample) for k, v in sorted(_group(all_rows, "action_quality").items())},
        "by_setup_v72": {k: _bucket_metrics(v, min_sample) for k, v in sorted(_group(all_rows, "setup_v72").items())},
        "by_durable_pattern": {k: _bucket_metrics(v, min_sample) for k, v in sorted(_group(all_rows, "durable_pattern").items())},
        "diagnostics": {
            "buy_false_positives": _best_worst(all_rows, {"BUY", "AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "THEME_CATCHUP", "LOW_OPEN_REVERSAL"}, top_n, reverse=False),
            "watch_or_reject_missed_winners": _best_worst(all_rows, {"WATCH", "REJECT", "CONFIRMATION_WATCH", "DEBUG_ONLY", "AVOID", "FAKE_STRENGTH_WATCH", "SOFT_AVOID_REPAIR_CANDIDATE"}, top_n, reverse=True),
            "all_biggest_winners": _best_worst(all_rows, None, top_n, reverse=True),
            "all_biggest_losers": _best_worst(all_rows, None, top_n, reverse=False),
        },
        "anti_overfit_notes": [
            "Do not promote or kill a signal family unless its bucket is_statistically_actionable=true or the rule is a hard data-contract/safety rule.",
            "Use the listed false positives/missed winners as cases for feature design, not as direct threshold-fitting targets.",
            "If only a few days are available, treat conclusions as hypotheses and add more dates before changing production thresholds.",
        ],
    }
    return result


def _render_markdown(result: Dict[str, Any]) -> str:
    lines = ["# duanxianxia selector validation report", ""]
    lines.append(f"- analysis files: {len(result.get('analysis_files') or [])}")
    lines.append(f"- min sample for actionable bucket: {result.get('min_sample_for_actionable_bucket')}")
    overall = result.get("overall") or {}
    lines.append(f"- overall rows/perf: {overall.get('count')} / {overall.get('with_performance')}")
    lines.append(f"- overall avg excess: {overall.get('avg_excess_return')} | hit rate: {overall.get('hit_rate_excess_gt_0')} | payoff: {overall.get('payoff_ratio_avg_win_abs_avg_loss')}")
    lines.append("")
    for section in ["by_action_type", "by_durable_pattern", "by_setup_v72"]:
        lines.append(f"## {section}")
        lines.append("| bucket | count | with_perf | status | hit_rate | avg_excess | med_excess | payoff | tail_loss<=-3 | big_win>=5 |")
        lines.append("|---|---:|---:|---|---:|---:|---:|---:|---:|---:|")
        for bucket, m in (result.get(section) or {}).items():
            lines.append(f"| {bucket} | {m.get('count')} | {m.get('with_performance')} | {m.get('sample_status')} | {m.get('hit_rate_excess_gt_0')} | {m.get('avg_excess_return')} | {m.get('median_excess_return')} | {m.get('payoff_ratio_avg_win_abs_avg_loss')} | {m.get('tail_loss_rate_le_-3')} | {m.get('big_win_rate_ge_5')} |")
        lines.append("")
    lines.append("## Anti-overfit notes")
    for item in result.get("anti_overfit_notes") or []:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main() -> int:
    p = argparse.ArgumentParser(description="Evaluate duanxianxia selector outputs without overfitting")
    p.add_argument("--analysis", action="append", default=[], help="Analysis JSON path. Can repeat.")
    p.add_argument("--analysis-glob", action="append", default=[], help="Glob for analysis JSON files. Can repeat.")
    p.add_argument("--flat", action="append", default=[], help="Review flat CSV/JSONL. Can repeat.")
    p.add_argument("--min-sample", type=int, default=30)
    p.add_argument("--top-n", type=int, default=20)
    p.add_argument("--output-json", default="")
    p.add_argument("--output-md", default="")
    args = p.parse_args()

    analysis_paths = [Path(x) for x in args.analysis]
    for pattern in args.analysis_glob:
        analysis_paths.extend(Path(x) for x in glob.glob(pattern))
    analysis_paths = sorted(dict.fromkeys(analysis_paths))
    if not analysis_paths:
        raise SystemExit("no analysis files provided")
    result = evaluate(analysis_paths, [Path(x) for x in args.flat], min_sample=args.min_sample, top_n=args.top_n)
    if args.output_json:
        Path(args.output_json).write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    if args.output_md:
        Path(args.output_md).write_text(_render_markdown(result), encoding="utf-8")
    if not args.output_json and not args.output_md:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(json.dumps({"output_json": args.output_json or None, "output_md": args.output_md or None, "overall": result.get("overall")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
