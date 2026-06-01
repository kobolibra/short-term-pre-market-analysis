"""
duanxianxia_v9_from_report.py — adapter that runs the v9 full-data assembly
directly from a premarket report, without requiring a PremarketDataBundle.

WHY THIS EXISTS
---------------
The production premarket flow in duanxianxia_batch.py builds its analysis with
`build_premarket_analysis(report)`, which reads capture rows straight out of
`report["items"]` (via `load_capture_rows(item["capture_path"])`). It never
constructs a `PremarketDataBundle`, so the v9 entry point
`assemble_v9(bundle, decisions, ...)` cannot be called against that flow as-is.

This adapter bridges the two worlds:
  1. It reads the SAME T0 capture rows from a premarket report.
  2. It builds a lightweight bundle object that exposes exactly the attributes
     `assemble_v9` reads. Attributes backed by premarket-available datasets
     (open-auction 委卖 / T0 plate summary / T0 qxlive top metrics) are populated;
     attributes that only exist in the heavier intraday bundle (T1/T2 snapshots,
     cashflow history, fupan, ltgd, ztpool) are left as empty lists so the v9
     sub-models degrade gracefully instead of crashing.
  3. It uses the existing premarket `top_candidates` as `decisions`.
  4. It calls `assemble_v9` and returns the shaped v9 block.

Everything is defensive: `build_v9_block` never raises — on any error it returns a
disabled stub — so the caller can attach the result to the existing premarket
report (or write a side file) without any risk to the v5 output.

INTEGRATION (minimal, safe)
---------------------------
In duanxianxia_batch.py, after the premarket analysis dict is built (and before /
while the report is finalized), add:

    import duanxianxia_v9_from_report as v9fr
    premarket_analysis["v9"] = v9fr.build_v9_block(report, premarket_analysis)

Or, to also drop a side file next to the report:

    v9fr.write_v9_json(output_dir, report, premarket_analysis)

No existing v7/v5 logic needs to change.
"""

from __future__ import annotations

import json
import os
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

try:  # the heavy lifting lives in the v9 assemble module
    import duanxianxia_v9_assemble as v9asm
except Exception:  # pragma: no cover - import-time guard
    v9asm = None  # type: ignore

try:
    import duanxianxia_v9_output as v9out
except Exception:  # pragma: no cover
    v9out = None  # type: ignore


# Dataset ids that appear in a premarket report's items. These mirror the ids
# used by build_premarket_analysis / TABLE_SPECS in duanxianxia_batch.py.
DS_AUCTION_WEIMAI = "auction.jjyd.weimai"
DS_HOME_KAIPAN = "home.kaipan.plate.summary"
DS_HOME_QXLIVE_TOP = "home.qxlive.top_metrics"

# Default side-file name, matching write_v9_outputs' default.
DEFAULT_V9_FILENAME = "analysis_v9.json"


def _disabled_block(reason: str) -> Dict[str, Any]:
    return {
        "enabled": False,
        "version": "premarket_v9",
        "reason": reason,
        "candidates": [],
        "market_env": None,
    }


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _read_capture_rows(capture_path: Optional[str]) -> List[Dict[str, Any]]:
    """Minimal, dependency-free reader for a capture payload's rows.

    Mirrors duanxianxia_batch.load_capture_rows: a capture file is JSON with the
    actual records under a "rows" key (falling back to a top-level list).
    """
    if not capture_path:
        return []
    try:
        if not os.path.isfile(capture_path):
            return []
        with open(capture_path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def _rows_by_dataset_from_report(report: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Index a premarket report's captured rows by dataset id."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    items = (report or {}).get("items") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        dataset_id = item.get("dataset_id") or item.get("dataset") or item.get("id")
        if not dataset_id:
            continue
        rows = _read_capture_rows(item.get("capture_path"))
        if not rows:
            continue
        out.setdefault(str(dataset_id), []).extend(rows)
    return out


def build_bundle_from_rows(rows_by_dataset: Dict[str, List[Dict[str, Any]]]) -> SimpleNamespace:
    """Build a lightweight bundle exposing exactly the attributes assemble_v9 reads.

    Premarket-available datasets populate their attributes; everything else is an
    empty list so the downstream v9 sub-models degrade gracefully.
    """
    rows_by_dataset = rows_by_dataset or {}
    kaipan_t0 = list(rows_by_dataset.get(DS_HOME_KAIPAN, []) or [])
    qxlive_top_t0 = list(rows_by_dataset.get(DS_HOME_QXLIVE_TOP, []) or [])
    weimai = list(rows_by_dataset.get(DS_AUCTION_WEIMAI, []) or [])
    return SimpleNamespace(
        # populated from premarket T0 captures
        auction_weimai=weimai,
        kaipan_t0_rows=kaipan_t0,
        qxlive_top_t0_rows=qxlive_top_t0,
        # not available pre-market -> empty so assemble_v9 falls back cleanly
        kaipan_t1_rows=[],
        qxlive_top_t1_rows=[],
        qxlive_top_t2_rows=[],
        cashflow_today_t1=[],
        cashflow_3day_t1=[],
        cashflow_5day_t1=[],
        cashflow_10day_t1=[],
        fupan_t1=[],
        ltgd_5day_t1=[],
        ztpool_t1=[],
    )


def _decisions_from_analysis(premarket_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Use the existing premarket candidates as v9 decisions.

    assemble_v9 only requires a stable `code` per decision; we pass the candidate
    dicts through (normalizing code) so any extra fields remain available.
    """
    analysis = premarket_analysis or {}
    candidates = (
        analysis.get("top_candidates")
        or analysis.get("candidates")
        or []
    )
    decisions: List[Dict[str, Any]] = []
    for cand in candidates:
        if not isinstance(cand, dict):
            continue
        code = _norm_code(cand.get("code") or cand.get("代码"))
        if not code:
            continue
        d = dict(cand)
        d["code"] = code
        decisions.append(d)
    return decisions


def build_v9_block_from_rows(
    rows_by_dataset: Dict[str, List[Dict[str, Any]]],
    decisions: List[Dict[str, Any]],
    *,
    meta: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Pure-ish core: build a bundle from indexed rows and run assemble_v9."""
    if v9asm is None or not hasattr(v9asm, "assemble_v9"):
        return _disabled_block("duanxianxia_v9_assemble unavailable")
    if not decisions:
        return _disabled_block("no_candidates")
    bundle = build_bundle_from_rows(rows_by_dataset)
    shaped = v9asm.assemble_v9(
        bundle,
        decisions,
        theme_history=None,
        industry_t1=None,
        meta=meta,
        params=params,
    )
    if isinstance(shaped, dict):
        shaped.setdefault("enabled", True)
        shaped.setdefault("source", "premarket_report_adapter")
    return shaped


def build_v9_block(
    report: Dict[str, Any],
    premarket_analysis: Optional[Dict[str, Any]] = None,
    *,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Top-level, fully defensive entry point. Never raises.

    Reads T0 capture rows from `report`, derives decisions from
    `premarket_analysis` (its top_candidates), runs the v9 assembly, and returns
    the shaped v9 block. On any failure returns a disabled stub so the caller can
    safely attach the result to the existing report.
    """
    try:
        analysis = premarket_analysis or (report or {}).get("analysis") or {}
        if isinstance(analysis, dict) and analysis.get("enabled") is False:
            return _disabled_block("premarket_analysis_disabled")
        rows_by_dataset = _rows_by_dataset_from_report(report or {})
        decisions = _decisions_from_analysis(analysis)
        meta = {
            "source": "premarket_report_adapter",
            "base_version": (analysis or {}).get("version"),
            "generated_at": (report or {}).get("generated_at"),
        }
        return build_v9_block_from_rows(
            rows_by_dataset, decisions, meta=meta, params=params
        )
    except Exception as exc:  # pragma: no cover - last-resort guard
        return _disabled_block(f"exception:{type(exc).__name__}:{exc}")


def write_v9_json(
    output_dir: str,
    report: Dict[str, Any],
    premarket_analysis: Optional[Dict[str, Any]] = None,
    *,
    params: Optional[Dict[str, Any]] = None,
    filename: str = DEFAULT_V9_FILENAME,
) -> Optional[str]:
    """Build the v9 block and persist it next to the report. Returns the path.

    Uses write_v9_outputs when available; otherwise falls back to a plain JSON
    dump. Never raises.
    """
    try:
        block = build_v9_block(report, premarket_analysis, params=params)
        if v9out is not None and hasattr(v9out, "write_v9_outputs"):
            try:
                return v9out.write_v9_outputs(output_dir, block, filename=filename)
            except TypeError:
                return v9out.write_v9_outputs(output_dir, block)
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, filename)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(block, fh, ensure_ascii=False, indent=2)
        return path
    except Exception:
        return None


def _self_test() -> None:
    """Filesystem-free smoke test of the adapter core."""
    rows_by_dataset = {
        DS_HOME_KAIPAN: [
            {"code": "002297", "name": "博光股份", "plate": "机器人", "change_pct": "5.2"},
            {"code": "000001", "name": "平安银行", "plate": "银行", "change_pct": "1.1"},
        ],
        DS_HOME_QXLIVE_TOP: [
            {"metric": "上涨家数", "value": "3200"},
            {"metric": "下跌家数", "value": "1500"},
        ],
        DS_AUCTION_WEIMAI: [
            {"code": "002297", "rank": 1, "weimai_amount_wan": "8000"},
        ],
    }
    decisions = [
        {"code": "002297", "name": "博光股份", "score": 88.0},
        {"code": "000001", "name": "平安银行", "score": 55.0},
    ]

    bundle = build_bundle_from_rows(rows_by_dataset)
    assert bundle.kaipan_t0_rows and bundle.qxlive_top_t0_rows, bundle
    assert bundle.auction_weimai, bundle
    # T1/T2 attributes must exist and be empty lists for graceful fallback.
    for attr in (
        "kaipan_t1_rows",
        "qxlive_top_t1_rows",
        "qxlive_top_t2_rows",
        "cashflow_today_t1",
        "fupan_t1",
        "ltgd_5day_t1",
        "ztpool_t1",
    ):
        assert getattr(bundle, attr) == [], attr

    # decisions derivation from a premarket-analysis-like dict
    derived = _decisions_from_analysis({"top_candidates": decisions})
    assert [d["code"] for d in derived] == ["002297", "000001"], derived

    # empty candidates -> disabled stub (never raises)
    stub = build_v9_block_from_rows(rows_by_dataset, [], meta=None, params=None)
    assert stub.get("enabled") is False and stub.get("reason") == "no_candidates", stub

    # full path: only assert it runs and returns a dict when assemble is present.
    if v9asm is not None and hasattr(v9asm, "assemble_v9"):
        block = build_v9_block_from_rows(
            rows_by_dataset, decisions, meta={"source": "selftest"}, params=None
        )
        assert isinstance(block, dict), type(block)
        print("v9_from_report _self_test passed (assemble_v9 present)")
    else:
        print("v9_from_report _self_test passed (assemble_v9 absent; core verified)")


if __name__ == "__main__":
    _self_test()
