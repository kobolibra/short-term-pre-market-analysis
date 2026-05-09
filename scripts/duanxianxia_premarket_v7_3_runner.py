#!/usr/bin/env python3
"""v7.3 premarket runner: formal action-pool production entry."""
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

from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT, run_v7_2
from duanxianxia_v7_3_output import upgrade_shaped_v72_to_v73

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
CONFIG_REL = Path("config/premarket_v7_3_setups.yaml")


def _merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a or {})
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def load_v7_3_overlay(project_root: Path) -> Dict[str, Any]:
    path = project_root / CONFIG_REL
    if not path.exists():
        return {"version": "premarket_v7_3", "action_pools": {}, "output": {"max_candidates": 30, "watch_tier_max": 60, "pool_max": 15}}
    if yaml is None:
        raise RuntimeError("PyYAML is required for v7.3 config loading")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data.setdefault("version", "premarket_v7_3")
    data.setdefault("action_pools", {})
    data.setdefault("output", {"max_candidates": 30, "watch_tier_max": 60, "pool_max": 15})
    return data


def run_v7_3(date_str: str, project_root: Path, output_dir: Optional[Path] = None, no_write: bool = False) -> Dict[str, Any]:
    overlay = load_v7_3_overlay(project_root)
    out_cfg = overlay.get("output") or {}
    max_candidates = int(out_cfg.get("max_candidates", 30))
    watch_tier_max = int(out_cfg.get("watch_tier_max", 60))
    pool_max = int(out_cfg.get("pool_max", 15))

    shaped_v72 = run_v7_2(date_str, project_root, output_dir=None, no_write=True)
    base_action_cfg = ((shaped_v72.get("meta") or {}).get("action_pools") or {})
    action_cfg = _merge(base_action_cfg, overlay.get("action_pools") or {})
    shaped = upgrade_shaped_v72_to_v73(shaped_v72, action_config=action_cfg, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)
    shaped.setdefault("meta", {})
    shaped["meta"]["version_overlay"] = overlay.get("version", "premarket_v7_3")
    shaped["meta"]["generated_by"] = "duanxianxia_premarket_v7_3_runner.py"

    if not no_write:
        if output_dir is None:
            output_dir = project_root / "reports" / date_str / "premarket"
            analysis_name = f"{datetime.now(TZ_SHANGHAI).strftime('%H%M%S')}_analysis_v7_3.json"
        else:
            analysis_name = "analysis_v7_3.json"
        output_dir.mkdir(parents=True, exist_ok=True)
        analysis_path = output_dir / analysis_name
        anchors_path = output_dir / "intraday_anchors_v7_3.json"
        analysis_path.write_text(json.dumps(shaped, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        anchors_path.write_text(json.dumps(shaped.get("intraday_anchors") or [], ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        shaped["paths"] = {"analysis_path": str(analysis_path), "anchors_path": str(anchors_path)}
    return shaped


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    p.add_argument("--output-dir", default="")
    p.add_argument("--no-write", action="store_true")
    p.add_argument("--json", action="store_true")
    a = p.parse_args()
    result = run_v7_3(a.date, Path(a.project_root), Path(a.output_dir) if a.output_dir else None, a.no_write)
    if a.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"v7.3 done date={a.date} candidates={result['meta']['candidate_count']} action_stats={result.get('action_stats')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
