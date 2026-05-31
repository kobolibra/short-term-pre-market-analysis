#!/usr/bin/env python3
"""v7.3/v7.4 premarket runner: production edge decision entry."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT, run_v7_2
import duanxianxia_v7_3_next_level_patch  # noqa: F401 - applies v7.4 overlay
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
        return {"version": "premarket_v7_3", "action_pools": {}, "output": {"max_candidates": 5, "watch_tier_max": 25, "pool_max": 8}}
    if yaml is None:
        raise RuntimeError("PyYAML is required for v7.3 config loading")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data.setdefault("version", "premarket_v7_3")
    data.setdefault("action_pools", {})
    data.setdefault("output", {"max_candidates": 5, "watch_tier_max": 25, "pool_max": 8})
    return data


def _fmt_num(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        num = float(value)
    except Exception:
        return str(value)
    if abs(num - round(num)) < 1e-9:
        return str(int(round(num)))
    return f"{num:.2f}".rstrip("0").rstrip(".")


def _as_float(value: Any) -> Optional[float]:
    try:
        if value in (None, "", "-"):
            return None
        return float(value)
    except Exception:
        return None


def _auction_pct(row: Mapping[str, Any]) -> Optional[float]:
    """Premarket price/cost. Only auction_change_pct / auction_pct are valid."""
    auction_detail = row.get("auction_detail") if isinstance(row.get("auction_detail"), Mapping) else {}
    for value in (
        row.get("auction_pct"),
        row.get("auction_change_pct"),
        auction_detail.get("auction_change_pct"),
    ):
        pct = _as_float(value)
        if pct is not None:
            return pct
    perf = row.get("performance") if isinstance(row.get("performance"), Mapping) else {}
    pct = _as_float(perf.get("auction_pct"))
    return pct


def _push_unique(parts: list[str], value: str) -> None:
    text = str(value or "").strip()
    if text and text not in parts:
        parts.append(text)


def _source_hit_count(row: Mapping[str, Any]) -> int:
    auction_detail = row.get("auction_detail") if isinstance(row.get("auction_detail"), Mapping) else {}
    value = auction_detail.get("source_family_count")
    if value not in (None, ""):
        try:
            return int(value)
        except Exception:
            pass
    families = auction_detail.get("source_families") or []
    if isinstance(families, list):
        return len([x for x in families if str(x).strip()])
    return 0


def _candidate_reasons(row: Mapping[str, Any]) -> list[str]:
    parts: list[str] = []
    _push_unique(parts, str(row.get("action_reason") or row.get("setup_reason") or ""))
    for tag in row.get("action_tags") or []:
        text = str(tag).strip()
        if text in {"buy", "watch", "reject", "avoid", "high_conviction_buy", "quality_gate_failed", "structural_avoid"}:
            _push_unique(parts, text)
    auction_detail = row.get("auction_detail") if isinstance(row.get("auction_detail"), Mapping) else {}
    theme_detail = row.get("theme_detail") if isinstance(row.get("theme_detail"), Mapping) else {}
    matched_plate = str(theme_detail.get("matched_plate") or row.get("matched_plate") or "").strip()
    if matched_plate:
        _push_unique(parts, f"题材 {matched_plate}")
    primary_signal = str(auction_detail.get("qiangchou_primary_signal") or row.get("qiangchou_primary_signal") or "").strip()
    if primary_signal:
        _push_unique(parts, f"抢筹 {primary_signal}")
    pct = _auction_pct(row)
    if pct is not None:
        _push_unique(parts, f"竞价 {_fmt_num(pct)}%")
    if row.get("conviction_score") not in (None, ""):
        _push_unique(parts, f"conviction {_fmt_num(row.get('conviction_score'))}")
    return parts[:5]


def _candidate_risks(row: Mapping[str, Any]) -> list[str]:
    parts: list[str] = []
    auction_detail = row.get("auction_detail") if isinstance(row.get("auction_detail"), Mapping) else {}
    for flag in auction_detail.get("risk_flags") or []:
        _push_unique(parts, str(flag))
    reason = str(row.get("action_reason") or "")
    for prefix in ("quality_gate_failed:", "structural_avoid:", "AVOID:", "REJECT:"):
        if prefix in reason:
            _push_unique(parts, reason.split(prefix, 1)[1])
    entry_reason = str(row.get("entry_reason") or auction_detail.get("entry_reason") or "").strip()
    if entry_reason and entry_reason != "normal":
        _push_unique(parts, entry_reason)
    return parts[:4]


def _infer_trade_date_from_report(report: Mapping[str, Any]) -> str:
    for item in report.get("items", []) or []:
        capture_path = str((item or {}).get("capture_path") or "").strip()
        if not capture_path:
            continue
        parts = Path(capture_path).parts
        for idx, part in enumerate(parts):
            if part == "captures" and idx + 1 < len(parts):
                try:
                    return datetime.fromisoformat(parts[idx + 1]).strftime("%Y-%m-%d")
                except Exception:
                    pass
    generated_at = str(report.get("generated_at") or "").strip()
    if len(generated_at) >= 10:
        try:
            return datetime.fromisoformat(generated_at[:10]).strftime("%Y-%m-%d")
        except Exception:
            pass
    return datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d")


def _adapt_for_batch(result: Dict[str, Any]) -> Dict[str, Any]:
    adapted = dict(result)
    top_rows = list(result.get("actionable_candidates") or result.get("top_candidates") or [])
    patched_top: list[Dict[str, Any]] = []
    for idx, raw in enumerate(top_rows, start=1):
        row = dict(raw)
        row.setdefault("rank", idx)
        row.setdefault("score", row.get("conviction_score", row.get("action_score", row.get("final_score"))))
        row.setdefault("source_hit_count", _source_hit_count(row))
        pct = _auction_pct(row)
        if pct is not None:
            row.setdefault("auction_pct", pct)
        row.setdefault("reasons", _candidate_reasons(row))
        row.setdefault("risks", _candidate_risks(row))
        patched_top.append(row)
    adapted["enabled"] = True
    adapted["candidate_count"] = (adapted.get("meta") or {}).get("candidate_count", len(patched_top))
    adapted["top_candidates"] = patched_top
    adapted.setdefault("actionable_candidates", patched_top)
    return adapted


def _pool_rows(pools: Mapping[str, Any], *names: str) -> list[Mapping[str, Any]]:
    for name in names:
        rows = pools.get(name)
        if rows:
            return list(rows)
    return []


def render_text(result: Dict[str, Any]) -> str:
    meta = result.get("meta") or {}
    paths = result.get("paths") or {}
    buy_rows = list(result.get("actionable_candidates") or result.get("top_candidates") or [])
    pools = result.get("candidate_pools") or {}
    watch_rows = _pool_rows(pools, "watch", "quality_watch_pool", "confirmation_watch_pool")
    reject_rows = _pool_rows(pools, "reject")
    avoid_rows = _pool_rows(pools, "avoid", "structural_avoid_pool", "avoid_or_risk_pool")

    lines = [
        "**盘前 v7.4 生产 edge 决策**",
        f"- 版本：{(meta.get('version_overlay') or result.get('version') or 'premarket_v7_3')}",
        f"- 交易日：{meta.get('date_t0') or '-'}",
        f"- 候选池：{meta.get('candidate_count') or 0}；BUY 候选：{len(buy_rows)}",
        "- 价格/成本口径：auction_change_pct only",
    ]
    if paths.get("analysis_path"):
        lines.append(f"- 分析文件：`{paths['analysis_path']}`")

    lines.append("")
    lines.append("**BUY / 高置信候选**")
    if not buy_rows:
        lines.append("- 无。证据不够时不强行推荐。")
    else:
        for idx, row in enumerate(buy_rows[:5], start=1):
            lines.append(
                f"- {idx}. {row.get('name')}（{row.get('code')}）"
                f"｜原动作 {row.get('pre_gate_action_type') or row.get('action_type') or '-'}"
                f"｜conviction {_fmt_num(row.get('conviction_score') or row.get('expected_return_score'))}"
                f"｜竞价 {_fmt_num(_auction_pct(row))}%"
                f"｜金额 {_fmt_num(row.get('auction_amount_wan') or (row.get('auction_detail') or {}).get('auction_amount_wan'))}万"
                f"｜原因：{row.get('action_reason') or '-'}"
            )

    if watch_rows:
        lines.append("")
        lines.append("**WATCH / 临界观察**")
        for idx, row in enumerate(watch_rows[:5], start=1):
            lines.append(
                f"- {idx}. {row.get('name')}（{row.get('code')}）"
                f"｜conviction {_fmt_num(row.get('conviction_score') or row.get('expected_return_score') or row.get('action_score'))}"
                f"｜竞价 {_fmt_num(_auction_pct(row))}%"
                f"｜原因：{row.get('action_reason') or '-'}"
            )

    if reject_rows:
        lines.append("")
        lines.append("**REJECT / 证据不足**")
        for idx, row in enumerate(reject_rows[:5], start=1):
            lines.append(f"- {idx}. {row.get('name')}（{row.get('code')}）｜{row.get('action_reason') or '-'}")

    if avoid_rows:
        lines.append("")
        lines.append("**AVOID / 结构性排除**")
        for idx, row in enumerate(avoid_rows[:3], start=1):
            lines.append(f"- {idx}. {row.get('name')}（{row.get('code')}）｜{row.get('action_reason') or '-'}")
    return "\n".join(lines)


def run_v7_3(date_str: str, project_root: Path, output_dir: Optional[Path] = None, no_write: bool = False) -> Dict[str, Any]:
    overlay = load_v7_3_overlay(project_root)
    out_cfg = overlay.get("output") or {}
    max_candidates = int(out_cfg.get("max_candidates", 5))
    watch_tier_max = int(out_cfg.get("watch_tier_max", 25))
    pool_max = int(out_cfg.get("pool_max", 8))

    shaped_v72 = run_v7_2(date_str, project_root, output_dir=None, no_write=True)
    base_action_cfg = ((shaped_v72.get("meta") or {}).get("action_pools") or {})
    action_cfg = _merge(base_action_cfg, overlay.get("action_pools") or {})
    shaped = upgrade_shaped_v72_to_v73(shaped_v72, action_config=action_cfg, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)
    shaped.setdefault("meta", {})
    shaped["meta"]["version_overlay"] = overlay.get("version", "premarket_v7_3")
    shaped["meta"]["generated_by"] = "duanxianxia_premarket_v7_3_runner.py"
    shaped["meta"]["price_cost_field"] = "auction_change_pct"

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


def build_premarket_analysis_v7_3(report: Mapping[str, Any], project_root: Optional[Path | str] = None) -> Dict[str, Any]:
    trade_date = _infer_trade_date_from_report(report)
    root = Path(project_root) if project_root is not None else DEFAULT_PROJECT_ROOT
    result = run_v7_3(trade_date, root, output_dir=None, no_write=False)
    return _adapt_for_batch(result)


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
        print(render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
