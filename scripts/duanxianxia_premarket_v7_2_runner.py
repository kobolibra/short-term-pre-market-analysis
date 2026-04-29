#!/usr/bin/env python3
"""v7.2 premarket runner: T0-driven scoring pipeline.

Pipeline:
  v7.1 loader/labels -> v7.2 auction_strength/theme_strength/hotness
  -> v7.2 setup engine -> v7.2 output.

OpenClaw dependency:
  Add rank.rocket and rank.hot_stock_day to premarket captures.  If absent,
  v7.2 still runs and reweights today_signal_raw to auction+theme.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from duanxianxia_premarket_v7_1_runner import build_candidates_from_auction, compute_all_labels
from duanxianxia_v7_2_auction_strength import compute_auction_strengths
from duanxianxia_v7_2_data_loader import load_premarket_v72_bundle
from duanxianxia_v7_2_hotness import compute_hotness_scores
from duanxianxia_v7_2_output import shape_v7_2_output, write_v7_2_outputs
from duanxianxia_v7_2_setup_engine import classify_candidates_v72
from duanxianxia_v7_2_theme_strength import compute_theme_strengths

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_PROJECT_ROOT = Path("/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia")
CONFIG_REL = Path("config/premarket_v7_2_setups.yaml")


def load_v7_2_config(project_root: Path) -> Dict[str, Any]:
    path = project_root / CONFIG_REL
    if not path.exists():
        raise FileNotFoundError(f"missing v7.2 config: {path}")
    if yaml is None:
        raise RuntimeError("PyYAML is required for v7.2 config loading")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data.setdefault("version", "premarket_v7_2")
    data.setdefault("params", {})
    data.setdefault("theme_aliases", [])
    data.setdefault("output", {"max_candidates": 30, "watch_tier_max": 50})
    return data


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-"):
            return None
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return None


def _build_candidate_latest_pct(candidates: list) -> Dict[str, float]:
    """Best-effort latest 9:25 change pct per candidate, used for hotness cap."""
    out: Dict[str, float] = {}
    for c in candidates or []:
        code = str(c.get("code") or "").strip()
        if not code:
            continue
        pct = (
            _to_float(c.get("latest_change_pct"))
            or _to_float(c.get("auction_change_pct"))
            or _to_float(c.get("change_pct"))
        )
        if pct is not None:
            out[code] = pct
    return out


def run_v7_2(
    date_str: str,
    project_root: Path,
    output_dir: Optional[Path] = None,
    no_write: bool = False,
) -> Dict[str, Any]:
    config = load_v7_2_config(project_root)
    params = config.get("params") or {}
    cutoff = str(params.get("premarket_auction_cutoff", "092900"))

    bundle = load_premarket_v72_bundle(date_str, project_root, premarket_auction_cutoff=cutoff)
    v71 = bundle.v71

    candidates = build_candidates_from_auction(v71, config.get("theme_aliases") or [])
    labels = compute_all_labels(v71, candidates, config, project_root)
    codes = [c["code"] for c in candidates if c.get("code")]
    candidate_latest_pct = _build_candidate_latest_pct(candidates)

    auction_strengths = compute_auction_strengths(
        codes,
        v71.auction_vratio,
        v71.auction_qiangchou,
        v71.auction_netamount,
        v71.auction_fengdan,
        params,
    )
    hotness_scores = compute_hotness_scores(
        bundle.rocket_rows,
        bundle.hot_stock_day_rows,
        codes,
        params,
        candidate_latest_pct=candidate_latest_pct,
    )
    theme_strengths = compute_theme_strengths(
        candidates,
        bundle.kaipan_plate_t0_rows,
        labels.get("theme_history") or {},
        labels.get("industry_t1") or {},
        params,
    )

    decisions = classify_candidates_v72(
        candidates,
        labels,
        auction_strengths,
        theme_strengths,
        hotness_scores,
        config,
        max_candidates=None,
    )

    out_cfg = config.get("output") or {}
    max_candidates = int(out_cfg.get("max_candidates", 30))
    watch_tier_max = int(out_cfg.get("watch_tier_max", 50))
    meta = {
        "date_t0": bundle.date_t0,
        "date_t1": bundle.date_t1,
        "date_t2": bundle.date_t2,
        "generated_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
        "candidate_count": len(candidates),
        "bundle_summary": bundle.to_summary_dict(),
        "regime": labels.get("regime"),
        "warnings": bundle.warnings,
        "notes": [
            "v7.2 is additive; v7.1 files are preserved.",
            "If rank.rocket / rank.hot_stock_day are missing, hotness_score is None and weights are reallocated.",
        ],
    }

    shaped = shape_v7_2_output(
        decisions,
        meta=meta,
        max_candidates=max_candidates,
        watch_tier_max=watch_tier_max,
    )

    if not no_write:
        if output_dir is None:
            output_dir = project_root / "reports" / date_str / "premarket"
            analysis_name = f"{datetime.now(TZ_SHANGHAI).strftime('%H%M%S')}_analysis_v7_2.json"
        else:
            analysis_name = "analysis_v7_2.json"
        shaped["paths"] = write_v7_2_outputs(
            str(output_dir),
            decisions,
            meta=meta,
            max_candidates=max_candidates,
            watch_tier_max=watch_tier_max,
            analysis_filename=analysis_name,
            anchors_filename="intraday_anchors.json",
        )
    return shaped


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    p.add_argument("--output-dir", default="")
    p.add_argument("--no-write", action="store_true")
    p.add_argument("--json", action="store_true")
    a = p.parse_args()

    result = run_v7_2(
        a.date,
        Path(a.project_root),
        Path(a.output_dir) if a.output_dir else None,
        a.no_write,
    )
    if a.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(
            f"v7.2 done date={a.date} candidates={result['meta']['candidate_count']} "
            f"setup_stats={result['setup_stats']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
