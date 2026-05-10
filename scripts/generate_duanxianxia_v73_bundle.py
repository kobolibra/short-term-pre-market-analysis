#!/usr/bin/env python3
"""Generate a production-grade v7.3 detailed review bundle.

Key guarantees:
- Loads the same v7.3 YAML action-pool config used by the runner before
  recomputing review metrics, so bundle backfill cannot silently drift from
  production classification.
- Separates action-order Top30 from expected-return-proxy Top30 in markdown.
- Renders review diagnostics and profile blocks for fast missed-winner analysis.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from duanxianxia_v7_3_output import load_performance_map_from_flat, recompute_v73_review_metrics

DEFAULT_PROJECT_ROOT = Path("projects/duanxianxia")
DEFAULT_CONFIG_REL = Path("config/premarket_v7_3_setups.yaml")
PERF_KEYS = ["auction_pct", "open_pct", "close_pct", "excess_return", "dailyline_found", "prev_close", "day_open", "day_high", "day_low", "day_close", "trade_date"]
POOL_ORDER = [
    "main_attack_pool",
    "momentum_catchup_pool",
    "theme_rotation_pool",
    "theme_catchup_pool",
    "low_open_reversal_pool",
    "board_watch_pool",
    "confirmation_watch_pool",
    "fake_strength_watch_pool",
    "soft_avoid_repair_pool",
    "avoid_or_risk_pool",
    "debug_only_pool",
]
PROFILE_KEYS = ["auction_setup_type", "action_type", "action_quality", "setup_v72", "confidence", "entry_tag"]
NUMERIC_PROFILE_KEYS = ["auction_pct", "auction_strength", "auction_amount_wan", "liquidity_score", "theme_strength_t0", "source_evidence_score", "source_family_count", "final_score"]


def load_action_config(project_root: Path, config_path: Optional[Path]) -> Dict[str, Any]:
    path = config_path or (project_root / DEFAULT_CONFIG_REL)
    if not path.exists():
        return {}
    if yaml is None:
        raise RuntimeError("PyYAML is required to load v7.3 action config")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return dict(data.get("action_pools") or {})


def fmt_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "True" if v else "False"
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def md_value(v: Any) -> str:
    return "" if v is None else str(v)


def code_key(v: Any) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits.zfill(6) if digits else s


def as_float(v: Any) -> Optional[float]:
    if v in (None, "", "None", "null", "NULL"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def nested_metric(row: Dict[str, Any], key: str) -> Any:
    if key in row:
        return row.get(key)
    perf = performance_of(row)
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


def performance_of(row: Dict[str, Any]) -> Dict[str, Any]:
    return dict(row.get("derived_performance") or row.get("performance") or {})


def add_review_profiles(shaped: Dict[str, Any]) -> Dict[str, Any]:
    """Attach compact diagnostic profiles for missed winners / false positives.

    These profiles are review-only and do not affect production classification.
    They make the next rule iteration faster by showing whether leaked winners
    share common auction-cost, liquidity, amount, setup, or source-evidence shapes.
    """
    diag = shaped.get("review_diagnostics") or {}

    def metric_stats(rows: List[Dict[str, Any]], key: str) -> Dict[str, Any]:
        vals = [as_float(nested_metric(r, key)) for r in rows]
        nums = sorted(v for v in vals if v is not None)
        if not nums:
            return {"count": 0}
        return {
            "count": len(nums),
            "min": round(nums[0], 2),
            "p25": round(nums[len(nums) // 4], 2),
            "median": round(median(nums), 2),
            "p75": round(nums[(len(nums) * 3) // 4], 2),
            "max": round(nums[-1], 2),
            "avg": round(sum(nums) / len(nums), 2),
        }

    def bucket_auction_pct(v: Any) -> str:
        x = as_float(v)
        if x is None:
            return "missing"
        if x < -5:
            return "<-5"
        if x < -2:
            return "[-5,-2)"
        if x < 0:
            return "[-2,0)"
        if x < 2:
            return "[0,2)"
        if x < 5:
            return "[2,5)"
        if x < 7:
            return "[5,7)"
        if x < 9:
            return "[7,9)"
        return ">=9"

    def bucket_amount(v: Any) -> str:
        x = as_float(v)
        if x is None:
            return "missing"
        if x < 500:
            return "<500w"
        if x < 1000:
            return "500-1000w"
        if x < 3000:
            return "1000-3000w"
        if x < 8000:
            return "3000-8000w"
        return ">=8000w"

    def profile(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        rows = [dict(x) for x in items or []]
        if not rows:
            return {}
        out: Dict[str, Any] = {"count": len(rows)}
        for key in PROFILE_KEYS:
            out[f"{key}_top"] = Counter(str(nested_metric(r, key) or r.get(key) or "missing") for r in rows).most_common(10)
        out["auction_pct_bucket"] = Counter(bucket_auction_pct(nested_metric(r, "auction_pct")) for r in rows).most_common()
        out["auction_amount_bucket"] = Counter(bucket_amount(nested_metric(r, "auction_amount_wan")) for r in rows).most_common()
        out["numeric_stats"] = {key: metric_stats(rows, key) for key in NUMERIC_PROFILE_KEYS}
        out["top_names"] = [f"{r.get('code')} {r.get('name')}" for r in rows[:20]]
        return out

    shaped["review_profiles"] = {
        "missed_winners": profile(diag.get("missed_winners") or []),
        "debug_missed_winners": profile(diag.get("debug_missed_winners") or []),
        "avoid_missed_winners": profile(diag.get("avoid_missed_winners") or []),
        "soft_avoid_missed_winners": profile(diag.get("soft_avoid_missed_winners") or []),
        "fake_strength_watch_winners": profile(diag.get("fake_strength_watch_winners") or []),
        "false_positives": profile(diag.get("false_positives") or []),
        "high_cost_confirmation_failures": profile(diag.get("high_cost_confirmation_failures") or []),
    }
    return shaped


def resolve_csv_value(row: Dict[str, Any], col: str, trade_date: str) -> Any:
    perf = performance_of(row)
    auction = row.get("auction_detail") or {}
    signal = row.get("signal_summary") or {}
    label = row.get("label_snapshot") or {}
    stock_t1 = label.get("stock_t1") or {}
    cash = label.get("cashflow_raw") or {}
    tech = label.get("tech_raw") or {}
    theme = row.get("theme_detail") or {}
    risk = row.get("risk_detail") or {}

    if col in row:
        return row.get(col)
    if col == "trade_date":
        return perf.get("trade_date") or trade_date
    if col in PERF_KEYS:
        return perf.get(col)
    if col.endswith("_json"):
        base = col[:-5]
        return row.get(base)
    if col.startswith("auction_"):
        return auction.get(col[len("auction_"):])
    if col.startswith("signal_"):
        return signal.get(col[len("signal_"):])
    if col.startswith("label_"):
        return label.get(col[len("label_"):])
    if col.startswith("stock_t1_"):
        return stock_t1.get(col[len("stock_t1_"):])
    if col.startswith("cashflow_raw_"):
        return cash.get(col[len("cashflow_raw_"):])
    if col.startswith("tech_raw_"):
        return tech.get(col[len("tech_raw_"):])
    if col.startswith("theme_"):
        return theme.get(col[len("theme_"):])
    if col.startswith("risk_"):
        return risk.get(col[len("risk_"):])
    return None


def rows_with_pool_hints(shaped: Dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    ranked = list(shaped.get("all_candidates_action_ranked") or [])
    pool_map: dict[str, list[str]] = {}
    for pool_name, items in (shaped.get("candidate_pools") or {}).items():
        for item in items or []:
            code = code_key(item.get("code"))
            if code:
                pool_map.setdefault(code, []).append(pool_name)
    return ranked, pool_map


def numeric_list(rows: Iterable[Dict[str, Any]], key: str) -> List[float]:
    out: List[float] = []
    for row in rows:
        v = performance_of(row).get(key)
        if isinstance(v, (int, float)):
            out.append(float(v))
    return out


def stat_line(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = numeric_list(rows, "close_pct")
    excess = numeric_list(rows, "excess_return")
    matched = sum(1 for r in rows if performance_of(r).get("dailyline_found") is True)
    out: Dict[str, Any] = {"dailyline_matched": matched, "total": len(rows)}
    if closes:
        out.update(avg_close_pct=round(sum(closes) / len(closes), 2), med_close_pct=round(median(closes), 2), pos_close_count=sum(1 for x in closes if x > 0))
    if excess:
        out.update(avg_excess_return=round(sum(excess) / len(excess), 2), med_excess_return=round(median(excess), 2), pos_excess_count=sum(1 for x in excess if x > 0))
    return out


def render_counter_md(title: str, counter: Counter) -> List[str]:
    lines = [f"## {title}", ""]
    for k, v in counter.items():
        lines.append(f"- `{k}`: `{v}`")
    lines.append("")
    return lines


def render_diagnostics_md(shaped: Dict[str, Any]) -> List[str]:
    lines = ["## review_diagnostics", ""]
    for k, v in (shaped.get("review_diagnostics") or {}).items():
        lines.append(f"- `{k}`: `{len(v or [])}`")
    lines.append("")
    profiles = shaped.get("review_profiles") or {}
    if profiles:
        lines.extend(["## review_profiles", ""])
        for name, profile in profiles.items():
            lines.append(f"### {name}")
            lines.append("")
            if not profile:
                lines.append("- empty")
                lines.append("")
                continue
            for key, value in profile.items():
                lines.append(f"- `{key}`: `{json.dumps(value, ensure_ascii=False)}`")
            lines.append("")
    return lines


def rows_by_key(shaped: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
    return list(shaped.get(key) or [])


def table_for_rows(title: str, rows: List[Dict[str, Any]], pool_map: Dict[str, List[str]], max_rows: int = 30) -> List[str]:
    lines = [
        f"## {title}",
        "",
        "| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |",
        "|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for i, row in enumerate(rows[:max_rows], start=1):
        perf = performance_of(row)
        code = code_key(row.get("code"))
        pool_hint = "|".join(pool_map.get(code, []))
        lines.append(
            f"| {i} | {md_value(row.get('code'))} | {md_value(row.get('name'))} | {md_value(row.get('action_type'))} | {md_value(row.get('action_quality') or row.get('signal_quality'))} | {md_value(row.get('action_reason'))} | {md_value(row.get('setup_v72'))} | {md_value(row.get('confidence'))} | {md_value(row.get('final_score'))} | {md_value(perf.get('auction_pct', row.get('auction_pct')))} | {md_value(perf.get('close_pct'))} | {md_value(perf.get('excess_return'))} | {pool_hint} |"
        )
    lines.append("")
    return lines


def render_stats_block(title: str, stats: Dict[str, Any], rows: List[Dict[str, Any]]) -> List[str]:
    ex_base = len(numeric_list(rows, "excess_return"))
    return [
        f"## {title}",
        "",
        f"- `dailyline_matched`: `{stats['dailyline_matched']} / {stats['total']}`",
        f"- `avg_close_pct`: `{stats.get('avg_close_pct')}`",
        f"- `med_close_pct`: `{stats.get('med_close_pct')}`",
        f"- `avg_excess_return`: `{stats.get('avg_excess_return')}`",
        f"- `med_excess_return`: `{stats.get('med_excess_return')}`",
        f"- `pos_close_count`: `{stats.get('pos_close_count')}/{stats['dailyline_matched']}`",
        f"- `pos_excess_count`: `{stats.get('pos_excess_count')}/{ex_base}`",
        "",
    ]


def render_summary(shaped: Dict[str, Any], analysis_name: str, rows: List[Dict[str, Any]], pool_map: Dict[str, List[str]]) -> str:
    meta = shaped.get("meta") or {}
    action_top = rows_by_key(shaped, "actionable_candidates") or rows[:30]
    expected_top = rows_by_key(shaped, "expected_return_candidates") or action_top
    lines: List[str] = []
    lines.append(f"# {analysis_name} 全量候选摘要")
    lines.append("")
    lines.append(f"- source_report: `{analysis_name}`")
    lines.append(f"- version: `{shaped.get('version')}`")
    lines.append(f"- date_t0: `{meta.get('date_t0')}`")
    lines.append(f"- generated_at: `{meta.get('generated_at')}`")
    lines.append(f"- candidate_count: `{meta.get('candidate_count')}`")
    lines.append(f"- regime: `{meta.get('regime')}`")
    lines.append(f"- regime reason: `{(meta.get('regime') or {}).get('reason') if isinstance(meta.get('regime'), dict) else None}`")
    lines.append("")

    lines.extend(render_counter_md("action_stats", Counter(shaped.get("action_stats") or {})))
    lines.extend(render_counter_md("action_quality_stats", Counter(shaped.get("action_quality_stats") or {})))

    lines.append("## pool_performance")
    lines.append("")
    for k, v in (shaped.get("pool_performance") or {}).items():
        lines.append(f"- `{k}`: `{json.dumps(v, ensure_ascii=False)}`")
    lines.append("")
    lines.extend(render_diagnostics_md(shaped))

    lines.append("## candidate_pools counts")
    lines.append("")
    for k in POOL_ORDER:
        if k in (shaped.get("candidate_pools") or {}):
            lines.append(f"- `{k}`: {len((shaped.get('candidate_pools') or {}).get(k) or [])}")
    lines.append("")

    lines.append("## 绩效补充口径")
    lines.append("")
    lines.append("- `auction_pct`: 竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`")
    lines.append("- `open_pct`: 当日开盘相对昨收涨幅")
    lines.append("- `close_pct`: 当日收盘相对昨收涨幅")
    lines.append("- `excess_return = close_pct - auction_pct`")
    lines.append("")

    lines.extend(render_stats_block("收盘涨幅 / 超额收益（全量）", stat_line(rows), rows))
    lines.extend(render_stats_block("收盘涨幅 / 超额收益（Action Order Top30）", stat_line(action_top[:30]), action_top[:30]))
    lines.extend(render_stats_block("收盘涨幅 / 超额收益（Expected Return Proxy Top30）", stat_line(expected_top[:30]), expected_top[:30]))

    lines.extend(render_counter_md("setup_v72 分布", Counter(str(r.get("setup_v72")) for r in rows)))
    lines.extend(render_counter_md("action_type 分布", Counter(str(r.get("action_type")) for r in rows)))
    lines.extend(render_counter_md("action_quality 分布", Counter(str(r.get("action_quality") or r.get("signal_quality")) for r in rows)))
    lines.extend(render_counter_md("confidence 分布", Counter(str(r.get("confidence")) for r in rows)))
    lines.extend(render_counter_md("auction_setup_type 分布", Counter(str(r.get("auction_setup_type")) for r in rows)))
    lines.extend(table_for_rows("Action Order Top30（交易动作顺序，不等于纯收益预测）", action_top, pool_map))
    lines.extend(table_for_rows("Expected Return Proxy Top30（盘前可见字段的收益预期展示）", expected_top, pool_map))
    return "\n".join(lines) + "\n"


def render_field_catalog(analysis_name: str) -> str:
    lines = [
        f"# {analysis_name} field catalog",
        "",
        "- `version`: 报告版本，应为 `premarket_v7_3`",
        "- `action_type`: 动作分层类型",
        "- `action_quality`: v7.3 动作质量分层；描述盘前信号质量，不是事后收益质量",
        "- `signal_quality`: `action_quality` 的语义化别名，强调其为盘前信号质量",
        "- `action_reason`: 动作归类原因",
        "- `action_score`: 动作评分",
        "- `action_priority`: 动作优先级；用于 action-order，不等于收益预测排序",
        "- `action_confidence`: 动作置信级别",
        "- `action_tags`: 动作附加标签，例如 `high_cost_confirmation` / `needs_intraday_repair`",
        "- `expected_return_candidates`: 用盘前可见字段生成的展示型收益预期排序，不使用收盘收益",
        "- `expected_return_watch_tier`: expected-return proxy 的观察层",
        "- `all_candidates_expected_return_ranked`: 全量 expected-return proxy 排序",
        "- `pool_performance`: 池级表现摘要，仅复盘使用",
        "- `review_diagnostics`: 复盘诊断列表，包括 missed/false-positive/high-cost failure",
        "- `review_profiles`: missed winners / false positives 的字段画像，用于发现下一轮规则共性",
        "- `soft_avoid_repair_pool`: 非盘前交易池；用于避免 moderate avoid 被误称 hard avoid",
        "- `auction_pct`: 竞价涨幅",
        "- `open_pct`: 当日开盘相对昨收涨幅",
        "- `close_pct`: 当日收盘相对昨收涨幅",
        "- `excess_return`: `close_pct - auction_pct`",
        "- `anchors`: 盘中观察锚点文本拼接",
        "",
    ]
    return "\n".join(lines)


def render_ranked_list(stem: str, shaped: Dict[str, Any], rows: List[Dict[str, Any]], pool_map: Dict[str, List[str]], key: str = "all_candidates_action_ranked") -> str:
    title = "action ranked" if key == "all_candidates_action_ranked" else "expected return proxy ranked"
    lines = [
        f"# {stem} all candidates {title} list",
        "",
        f"- source_report: `{stem}_analysis_v7_3.json`",
        f"- version: `{shaped.get('version')}`",
        f"- candidate_count: `{len(rows)}`",
        "",
        "| rank | code | name | action_type | action_quality | setup_v72 | confidence | final_score | auction_pct | close_pct | excess_return | pool_hint |",
        "|---:|---|---|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for i, row in enumerate(rows, start=1):
        perf = performance_of(row)
        code = code_key(row.get("code"))
        pool_hint = "|".join(pool_map.get(code, []))
        lines.append(
            f"| {i} | {md_value(row.get('code'))} | {md_value(row.get('name'))} | {md_value(row.get('action_type'))} | {md_value(row.get('action_quality') or row.get('signal_quality'))} | {md_value(row.get('setup_v72'))} | {md_value(row.get('confidence'))} | {md_value(row.get('final_score'))} | {md_value(perf.get('auction_pct', row.get('auction_pct')))} | {md_value(perf.get('close_pct'))} | {md_value(perf.get('excess_return'))} | {pool_hint} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_pools_detail(stem: str, shaped: Dict[str, Any]) -> str:
    pools = shaped.get("candidate_pools") or {}
    lines = [f"# {stem} candidate pools detail", "", f"- source_report: `{stem}_analysis_v7_3.json`", f"- version: `{shaped.get('version')}`", ""]
    for pool_name in POOL_ORDER:
        items = pools.get(pool_name)
        if items is None:
            continue
        lines.append(f"## {pool_name} ({len(items)})")
        lines.append("")
        for i, item in enumerate(items, start=1):
            perf = item.get("performance") or {}
            lines.append(
                f"{i}. {md_value(item.get('code'))} {md_value(item.get('name'))} | action_type={md_value(item.get('action_type'))} | action_quality={md_value(item.get('action_quality') or item.get('signal_quality'))} | action_reason={md_value(item.get('action_reason'))} | final={md_value(item.get('final_score'))} | auction_pct={md_value(perf.get('auction_pct'))} | close_pct={md_value(perf.get('close_pct'))} | excess_return={md_value(perf.get('excess_return'))}"
            )
        lines.append("")
    return "\n".join(lines)


def render_readme(stem: str, analysis_name: str, ref_flat_name: str, config_name: str) -> str:
    lines = [
        f"# 2026-04-29 premarket detailed review bundle (v7.3, {stem})",
        "",
        f"本目录基于最新代码生成的 v7.3 报告：`{analysis_name}`。",
        "",
        "## 关键点",
        "",
        "- 使用本地已有 `2026-04-29` 盘前 captures 重跑，未重复下载盘前数据。",
        "- 本次源报告已是 `premarket_v7_3`。",
        f"- bundle 生成时加载 `{config_name}`，避免 runner 与 backfill/review 分类逻辑漂移。",
        f"- 复用了同日已 backfill 的 flat CSV (`{ref_flat_name}`) 作为绩效来源，给新 `{stem}` 报告重算了 `pool_performance` / `review_diagnostics`。",
        "- 报告区分 `Action Order Top30` 与 `Expected Return Proxy Top30`。前者是交易动作顺序，后者是盘前可见字段的收益预期展示。",
        "- 附带 `review_profiles`，用于快速定位 debug missed winners / avoid missed winners 的共性。",
        "",
        "## v7.3 关键结构",
        "",
        "- `action_stats`",
        "- `action_quality_stats`",
        "- `pool_performance`",
        "- `review_diagnostics`",
        "- `review_profiles`",
        "- `expected_return_candidates`",
        "- `candidate_pools.momentum_catchup_pool`",
        "- `candidate_pools.debug_only_pool`",
        "- `candidate_pools.fake_strength_watch_pool`",
        "- `candidate_pools.soft_avoid_repair_pool`",
        "",
        "## 文件说明",
        "",
        f"- `{analysis_name}`：原始 v7.3 报告（已重算 review metrics）",
        f"- `{stem}_all_candidates_flat.csv`：全量扁平化 CSV，已补充绩效字段与动作字段",
        f"- `{stem}_all_candidates_flat.jsonl`：全量扁平化 JSONL，已补充绩效字段与动作字段",
        f"- `{stem}_analysis_summary.md`：摘要，包括 action stats、quality stats、pool performance、diagnostics、profiles、Action Top30、Expected Top30",
        f"- `{stem}_analysis_field_catalog.md`：字段说明",
        f"- `{stem}_all_candidates_ranked_list.md`：全量 action-order 排序清单",
        f"- `{stem}_all_candidates_expected_return_ranked_list.md`：全量 expected-return proxy 排序清单",
        f"- `{stem}_candidate_pools_detail.md`：分池明细",
        "",
        "## 绩效字段口径",
        "",
        "- `auction_pct`: 竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`",
        "- `open_pct`: 当日开盘相对昨收涨幅",
        "- `close_pct`: 当日收盘相对昨收涨幅",
        "- `excess_return`: `close_pct - auction_pct`",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--analysis", required=True)
    ap.add_argument("--performance-flat", required=True)
    ap.add_argument("--reference-flat", required=True)
    ap.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    ap.add_argument("--config", default="", help="Optional explicit v7.3 setup YAML path")
    args = ap.parse_args()

    analysis_path = Path(args.analysis)
    out_dir = analysis_path.parent
    stem = analysis_path.name.replace("_analysis_v7_3.json", "")
    analysis_name = analysis_path.name
    project_root = Path(args.project_root)
    config_path = Path(args.config) if args.config else None
    action_config = load_action_config(project_root, config_path)

    shaped = json.loads(analysis_path.read_text(encoding="utf-8"))
    perf_map = load_performance_map_from_flat(args.performance_flat)
    shaped = recompute_v73_review_metrics(shaped, perf_map, action_config=action_config)
    shaped = add_review_profiles(shaped)
    analysis_path.write_text(json.dumps(shaped, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    trade_date = ((shaped.get("meta") or {}).get("date_t0")) or ""
    rows, pool_map = rows_with_pool_hints(shaped)

    with Path(args.reference_flat).open("r", encoding="utf-8-sig", newline="") as fp:
        header = list(csv.DictReader(fp).fieldnames or [])
    if not header:
        raise SystemExit("reference flat header missing")

    flat_rows: List[Dict[str, Any]] = []
    for row in rows:
        out: Dict[str, Any] = {}
        for col in header:
            out[col] = resolve_csv_value(row, col, trade_date)
        flat_rows.append(out)

    flat_csv = out_dir / f"{stem}_all_candidates_flat.csv"
    flat_jsonl = out_dir / f"{stem}_all_candidates_flat.jsonl"
    summary_md = out_dir / f"{stem}_analysis_summary.md"
    catalog_md = out_dir / f"{stem}_analysis_field_catalog.md"
    ranked_md = out_dir / f"{stem}_all_candidates_ranked_list.md"
    expected_ranked_md = out_dir / f"{stem}_all_candidates_expected_return_ranked_list.md"
    pools_md = out_dir / f"{stem}_candidate_pools_detail.md"
    readme_md = out_dir / f"README_detailed_review_bundle_v7_3_{stem}.md"

    with flat_csv.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=header)
        writer.writeheader()
        for row in flat_rows:
            writer.writerow({k: fmt_value(v) for k, v in row.items()})

    with flat_jsonl.open("w", encoding="utf-8") as fp:
        for row in flat_rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    expected_rows = list(shaped.get("all_candidates_expected_return_ranked") or [])
    summary_md.write_text(render_summary(shaped, analysis_name, rows, pool_map), encoding="utf-8")
    catalog_md.write_text(render_field_catalog(analysis_name) + "\n", encoding="utf-8")
    ranked_md.write_text(render_ranked_list(stem, shaped, rows, pool_map) + "\n", encoding="utf-8")
    expected_ranked_md.write_text(render_ranked_list(stem, shaped, expected_rows, pool_map, key="all_candidates_expected_return_ranked") + "\n", encoding="utf-8")
    pools_md.write_text(render_pools_detail(stem, shaped) + "\n", encoding="utf-8")
    readme_md.write_text(render_readme(stem, analysis_name, Path(args.performance_flat).name, str(config_path or (project_root / DEFAULT_CONFIG_REL))), encoding="utf-8")

    print(json.dumps({
        "analysis": str(analysis_path),
        "flat_csv": str(flat_csv),
        "flat_jsonl": str(flat_jsonl),
        "summary_md": str(summary_md),
        "catalog_md": str(catalog_md),
        "ranked_md": str(ranked_md),
        "expected_ranked_md": str(expected_ranked_md),
        "pools_md": str(pools_md),
        "readme_md": str(readme_md),
        "rows": len(rows),
        "performance_rows": len(perf_map),
        "action_config_keys": sorted(action_config.keys()),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
