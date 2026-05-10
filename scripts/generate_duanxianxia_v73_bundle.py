#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from duanxianxia_v7_3_output import load_performance_map_from_flat, recompute_v73_review_metrics

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
    if s.endswith('.0'):
        s = s[:-2]
    digits = ''.join(ch for ch in s if ch.isdigit())
    return digits.zfill(6) if digits else s


def performance_of(row: Dict[str, Any]) -> Dict[str, Any]:
    return dict(row.get("derived_performance") or row.get("performance") or {})


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
    out: Dict[str, Any] = {
        "dailyline_matched": matched,
        "total": len(rows),
    }
    if closes:
        out.update(
            avg_close_pct=round(sum(closes) / len(closes), 2),
            med_close_pct=round(median(closes), 2),
            pos_close_count=sum(1 for x in closes if x > 0),
        )
    if excess:
        out.update(
            avg_excess_return=round(sum(excess) / len(excess), 2),
            med_excess_return=round(median(excess), 2),
            pos_excess_count=sum(1 for x in excess if x > 0),
        )
    return out


def render_counter_md(title: str, counter: Counter) -> List[str]:
    lines = [f"## {title}", ""]
    for k, v in counter.items():
        lines.append(f"- `{k}`: `{v}`")
    lines.append("")
    return lines


def top30_table(rows: List[Dict[str, Any]], pool_map: Dict[str, List[str]]) -> List[str]:
    lines = [
        "## Top 30 候选（含动作 / 质量 / 收盘涨幅 / 超额收益）",
        "",
        "| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |",
        "|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for i, row in enumerate(rows[:30], start=1):
        perf = performance_of(row)
        code = code_key(row.get("code"))
        pool_hint = "|".join(pool_map.get(code, []))
        lines.append(
            f"| {i} | {md_value(row.get('code'))} | {md_value(row.get('name'))} | {md_value(row.get('action_type'))} | {md_value(row.get('action_quality') or row.get('signal_quality'))} | {md_value(row.get('action_reason'))} | {md_value(row.get('setup_v72'))} | {md_value(row.get('confidence'))} | {md_value(row.get('final_score'))} | {md_value(perf.get('auction_pct', row.get('auction_pct')))} | {md_value(perf.get('close_pct'))} | {md_value(perf.get('excess_return'))} | {pool_hint} |"
        )
    lines.append("")
    return lines


def render_summary(shaped: Dict[str, Any], analysis_name: str, rows: List[Dict[str, Any]], pool_map: Dict[str, List[str]]) -> str:
    meta = shaped.get("meta") or {}
    full_stats = stat_line(rows)
    top30_stats = stat_line(rows[:30])
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

    lines.append("## review_diagnostics")
    lines.append("")
    for k, v in (shaped.get("review_diagnostics") or {}).items():
        lines.append(f"- `{k}`: `{len(v or [])}`")
    lines.append("")

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

    lines.append("## 收盘涨幅 / 超额收益（全量）")
    lines.append("")
    lines.append(f"- `dailyline_matched`: `{full_stats['dailyline_matched']} / {full_stats['total']}`")
    lines.append(f"- `avg_close_pct`: `{full_stats.get('avg_close_pct')}`")
    lines.append(f"- `med_close_pct`: `{full_stats.get('med_close_pct')}`")
    lines.append(f"- `avg_excess_return`: `{full_stats.get('avg_excess_return')}`")
    lines.append(f"- `med_excess_return`: `{full_stats.get('med_excess_return')}`")
    lines.append(f"- `pos_close_count`: `{full_stats.get('pos_close_count')}/{full_stats['dailyline_matched']}`")
    pos_ex_base = len(numeric_list(rows, 'excess_return'))
    lines.append(f"- `pos_excess_count`: `{full_stats.get('pos_excess_count')}/{pos_ex_base}`")
    lines.append("")

    lines.append("## 收盘涨幅 / 超额收益（Top30）")
    lines.append("")
    lines.append(f"- `avg_close_pct_top30`: `{top30_stats.get('avg_close_pct')}`")
    lines.append(f"- `med_close_pct_top30`: `{top30_stats.get('med_close_pct')}`")
    lines.append(f"- `avg_excess_return_top30`: `{top30_stats.get('avg_excess_return')}`")
    lines.append(f"- `med_excess_return_top30`: `{top30_stats.get('med_excess_return')}`")
    lines.append(f"- `pos_close_count_top30`: `{top30_stats.get('pos_close_count')}/{top30_stats['dailyline_matched']}`")
    top30_ex_base = len(numeric_list(rows[:30], 'excess_return'))
    lines.append(f"- `pos_excess_count_top30`: `{top30_stats.get('pos_excess_count')}/{top30_ex_base}`")
    lines.append("")

    lines.extend(render_counter_md("setup_v72 分布", Counter(str(r.get("setup_v72")) for r in rows)))
    lines.extend(render_counter_md("action_type 分布", Counter(str(r.get("action_type")) for r in rows)))
    lines.extend(render_counter_md("action_quality 分布", Counter(str(r.get("action_quality") or r.get("signal_quality")) for r in rows)))
    lines.extend(render_counter_md("confidence 分布", Counter(str(r.get("confidence")) for r in rows)))
    lines.extend(render_counter_md("auction_setup_type 分布", Counter(str(r.get("auction_setup_type")) for r in rows)))
    lines.extend(top30_table(rows, pool_map))
    return "\n".join(lines) + "\n"


def render_field_catalog(analysis_name: str) -> str:
    lines = [
        f"# {analysis_name} field catalog",
        "",
        "- `version`: 报告版本，应为 `premarket_v7_3`",
        "- `action_type`: 动作分层类型",
        "- `action_quality`: v7.3 动作质量分层",
        "- `action_reason`: 动作归类原因",
        "- `action_score`: 动作评分",
        "- `action_priority`: 动作优先级",
        "- `action_confidence`: 动作置信级别",
        "- `action_tags`: 动作附加标签",
        "- `auction_pct`: 竞价涨幅",
        "- `open_pct`: 当日开盘相对昨收涨幅",
        "- `close_pct`: 当日收盘相对昨收涨幅",
        "- `excess_return`: `close_pct - auction_pct`",
        "- `anchors`: 盘中观察锚点文本拼接",
        "- `pool_performance`: v7.3 新增池级表现摘要（源 JSON 顶层）",
        "- `review_diagnostics`: v7.3 新增复盘诊断摘要（源 JSON 顶层）",
        "",
    ]
    return "\n".join(lines)


def render_ranked_list(stem: str, shaped: Dict[str, Any], rows: List[Dict[str, Any]], pool_map: Dict[str, List[str]]) -> str:
    lines = [
        f"# {stem} all candidates ranked list",
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
    lines = [
        f"# {stem} candidate pools detail",
        "",
        f"- source_report: `{stem}_analysis_v7_3.json`",
        f"- version: `{shaped.get('version')}`",
        "",
    ]
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


def render_readme(stem: str, analysis_name: str, ref_flat_name: str) -> str:
    lines = [
        f"# 2026-04-29 premarket detailed review bundle (v7.3, {stem})",
        "",
        f"本目录基于最新代码生成的 v7.3 报告：`{analysis_name}`。",
        "",
        "## 关键点",
        "",
        "- 使用本地已有 `2026-04-29` 盘前 captures 重跑，未重复下载盘前数据。",
        "- 本次源报告已是 `premarket_v7_3`。",
        f"- 复用了同日已 backfill 的 flat CSV (`{ref_flat_name}`) 作为绩效来源，给新 `{stem}` 报告重算了 `pool_performance` / `review_diagnostics`。",
        "- 附带补充了当日绩效字段：`auction_pct` / `open_pct` / `close_pct` / `excess_return`。",
        "- 附带补充了动作分层与质量字段：`action_type` / `action_quality` / `action_reason` / `action_score` / `action_priority` / `action_confidence` / `action_tags`。",
        "- 单独输出了 `candidate_pools` 分池明细文件。",
        "",
        "## v7.3 关键结构",
        "",
        "- `action_stats`",
        "- `action_quality_stats`",
        "- `pool_performance`",
        "- `review_diagnostics`",
        "- `candidate_pools.momentum_catchup_pool`",
        "- `candidate_pools.debug_only_pool`",
        "- `candidate_pools.fake_strength_watch_pool`",
        "",
        "## 文件说明",
        "",
        f"- `{analysis_name}`：原始 v7.3 报告（已重算 review metrics）",
        f"- `{stem}_all_candidates_flat.csv`：全量扁平化 CSV，已补充绩效字段与动作字段",
        f"- `{stem}_all_candidates_flat.jsonl`：全量扁平化 JSONL，已补充绩效字段与动作字段",
        f"- `{stem}_analysis_summary.md`：摘要，包括 action stats、quality stats、pool performance、diagnostics 与 Top30 表",
        f"- `{stem}_analysis_field_catalog.md`：字段说明",
        f"- `{stem}_all_candidates_ranked_list.md`：全量排序清单",
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
    args = ap.parse_args()

    analysis_path = Path(args.analysis)
    out_dir = analysis_path.parent
    stem = analysis_path.name.replace("_analysis_v7_3.json", "")
    analysis_name = analysis_path.name

    shaped = json.loads(analysis_path.read_text(encoding="utf-8"))
    perf_map = load_performance_map_from_flat(args.performance_flat)
    shaped = recompute_v73_review_metrics(shaped, perf_map)
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

    summary_md.write_text(render_summary(shaped, analysis_name, rows, pool_map), encoding="utf-8")
    catalog_md.write_text(render_field_catalog(analysis_name) + "\n", encoding="utf-8")
    ranked_md.write_text(render_ranked_list(stem, shaped, rows, pool_map) + "\n", encoding="utf-8")
    pools_md.write_text(render_pools_detail(stem, shaped) + "\n", encoding="utf-8")
    readme_md.write_text(render_readme(stem, analysis_name, Path(args.performance_flat).name), encoding="utf-8")

    print(json.dumps({
        "analysis": str(analysis_path),
        "flat_csv": str(flat_csv),
        "flat_jsonl": str(flat_jsonl),
        "summary_md": str(summary_md),
        "catalog_md": str(catalog_md),
        "ranked_md": str(ranked_md),
        "pools_md": str(pools_md),
        "readme_md": str(readme_md),
        "rows": len(rows),
        "performance_rows": len(perf_map),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
