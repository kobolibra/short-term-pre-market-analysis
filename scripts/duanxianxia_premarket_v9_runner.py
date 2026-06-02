#!/usr/bin/env python3
"""
duanxianxia_premarket_v9_runner.py — v9 full-data premarket DECISION engine.

Pipeline:
    build_v72_decisions(date)  -> 完整 v7.2 decisions + 全量数据 bundle
    assemble_v9(bundle.v71, decisions, ...) -> v9 六层全保真重打分 (含 edge_score)
                                               + 动作层 (BUY/WATCH/DROP, 见 v9_output)
    _select_buys(...)          -> 读取 v9_output 分位数动作闸门产出的 BUY 行
    _adapt_for_batch(...)      -> batch.py 所需结构 (enabled/top_candidates/...)

本引擎不侵入 v7.x 计算:它复用 v7.2 的完整 decisions 与数据 bundle,
仅在其上做 v9 全字段装配 + 重打分 + 动作决策。

动作决策层 (BUY/WATCH/DROP) 与买入闸门已下沉到 duanxianxia_v9_output.py,
采用 regime 自适应的"分位数 + 绝对下限 + 数量上限"闸门(REGIME_ACTION_GATE),
取代旧的固定 edge 阈值(68~78),避免 edge 实际分布偏低时买入列表恒空。

⚠️ 风险提示:动作闸门参数 (REGIME_ACTION_GATE / RISK_EXTRA_MARGIN) 为本次新增,
   尚未经过历史回测验证。实盘前应先纸面验证。如需回退到 v7.3,改
   duanxianxia_premarket_v7_runner.py 中的 ACTIVE_ENGINE 一行即可。

Usage:
    python3 scripts/duanxianxia_premarket_v9_runner.py --date 2026-06-01 --json
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT, build_v72_decisions
from duanxianxia_premarket_v7_3_runner import _infer_trade_date_from_report
import duanxianxia_v9_assemble as v9asm
import duanxianxia_v9_output as v9out

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")

ALPHA_LABELS = {
    "AUCTION_ORDERFLOW": "竞价资金流",
    "ORDERBOOK_WEIMAI": "盘口委买",
    "THEME_BACKGROUND": "题材背景",
    "LOW_OPEN_REVERSAL": "低开反包",
}


def _fmt_num(v: Any, nd: int = 1) -> Optional[str]:
    try:
        if v in (None, ""):
            return None
        return f"{float(v):.{nd}f}"
    except Exception:
        return None


def _regime_label(market_env: Any, meta: Optional[Mapping[str, Any]]) -> str:
    for src in (market_env, (meta or {}).get("regime") if isinstance(meta, Mapping) else None):
        if isinstance(src, Mapping):
            reg = src.get("regime")
            if isinstance(reg, Mapping):
                lab = reg.get("regime") or reg.get("label")
                if lab:
                    return str(lab)
            if isinstance(reg, str) and reg:
                return reg
            lab = src.get("label")
            if isinstance(lab, str) and lab:
                return lab
        elif isinstance(src, str) and src:
            return src
    return ""


def _candidate_reasons(row: Mapping[str, Any]) -> List[str]:
    reasons: List[str] = []
    full = row.get("full") if isinstance(row.get("full"), Mapping) else {}
    alpha = row.get("alpha_type") or full.get("alpha_type")
    if alpha:
        reasons.append(ALPHA_LABELS.get(str(alpha), str(alpha)))
    plate = row.get("matched_plate") or full.get("matched_plate")
    if plate:
        reasons.append(f"题材:{plate}")
    apct = _fmt_num(row.get("auction_pct"))
    if apct is not None:
        reasons.append(f"竞价{apct}%")
    cont = row.get("cashflow_continuity") or full.get("cashflow_continuity")
    if cont:
        reasons.append(f"资金连续:{cont}")
    edge = _fmt_num(row.get("edge_score"))
    if edge is not None:
        reasons.append(f"edge={edge}")
    return reasons


def _candidate_risks(row: Mapping[str, Any]) -> List[str]:
    detail = row.get("risk_detail")
    out: List[str] = []
    if isinstance(detail, Mapping):
        for k, v in detail.items():
            if v:
                out.append(str(k) if isinstance(v, bool) else f"{k}:{v}")
    elif isinstance(detail, (list, tuple)):
        out = [str(x) for x in detail if x]
    elif isinstance(detail, str) and detail:
        out = [detail]
    if row.get("risk_flag") and "risk_flag" not in out:
        out.append("risk_flag")
    return out


def _source_hit_count(row: Mapping[str, Any]) -> int:
    full = row.get("full") if isinstance(row.get("full"), Mapping) else {}
    for src in (full.get("source_family_count"), row.get("source_family_count")):
        try:
            if src is not None:
                return int(src)
        except Exception:
            pass
    hits = full.get("source_hits")
    if isinstance(hits, (list, tuple)):
        return len(hits)
    return 0


def _to_batch_row(row: Mapping[str, Any], rank: int) -> Dict[str, Any]:
    out = dict(row)
    edge = row.get("edge_score")
    alpha = row.get("alpha_type")
    reasons = _candidate_reasons(row)
    out["rank"] = rank
    out["score"] = edge
    out["conviction_score"] = edge
    out["expected_return_score"] = edge
    out["action_type"] = row.get("action_type") or alpha
    out["pre_gate_action_type"] = alpha
    out["action_reason"] = "；".join(reasons)
    out["reasons"] = reasons
    out["risks"] = _candidate_risks(row)
    out["source_hit_count"] = _source_hit_count(row)
    return out


def _select_buys(ranked: List[Mapping[str, Any]], market_env: Any, meta: Optional[Mapping[str, Any]]):
    """读取 v9_output 动作层结果:action_type == 'BUY' 即买入候选。

    闸门参数已在 shape_v9_output 阶段按 regime 分位数标定并写入 meta['action_gate'],
    此处不再重复计算阈值,仅消费动作标签,保证决策"单一来源"。
    """
    buys = [row for row in ranked if str(row.get("action_type")) == "BUY"]
    gate_info: Dict[str, Any] = {}
    if isinstance(meta, Mapping) and isinstance(meta.get("action_gate"), Mapping):
        gate_info = dict(meta["action_gate"])
    gate_info.setdefault("regime", _regime_label(market_env, meta) or "(unknown)")
    gate_info["selected"] = len(buys)
    return buys, gate_info


def _adapt_for_batch(shaped: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(shaped)
    market_env = shaped.get("market_env")
    meta = shaped.get("meta") if isinstance(shaped.get("meta"), Mapping) else {}
    ranked = shaped.get("all_candidates") or shaped.get("top_candidates") or []
    buys, gate_info = _select_buys(ranked, market_env, meta)
    buy_codes = {str(b.get("code")) for b in buys}
    buy_rows = [_to_batch_row(b, i + 1) for i, b in enumerate(buys)]
    watch_src = [
        r for r in ranked
        if str(r.get("action_type")) == "WATCH" and str(r.get("code")) not in buy_codes
    ][:15]
    watch_rows = [_to_batch_row(r, i + 1) for i, r in enumerate(watch_src)]

    out["enabled"] = True
    out["version"] = shaped.get("version", "premarket_v9")
    out["candidate_count"] = len(ranked)
    out["top_candidates"] = buy_rows
    out["actionable_candidates"] = buy_rows
    out["watch_candidates"] = watch_rows
    out["buy_gate"] = gate_info
    new_meta = dict(meta)
    new_meta["buy_gate"] = gate_info
    new_meta["engine"] = "premarket_v9"
    out["meta"] = new_meta
    return out


def run_v9(
    date_str: str,
    project_root: Path,
    output_dir: Optional[Path] = None,
    no_write: bool = False,
) -> Dict[str, Any]:
    ctx = build_v72_decisions(date_str, project_root)
    bundle = ctx["bundle"]
    decisions = ctx["decisions"]
    labels = ctx["labels"]
    params = ctx["params"]
    meta = dict(ctx["meta"])
    meta["engine"] = "premarket_v9"
    meta["base_pipeline"] = "premarket_v7_2"

    shaped = v9asm.assemble_v9(
        bundle.v71,
        decisions,
        theme_history=labels.get("theme_history") or {},
        industry_t1=labels.get("industry_t1") or {},
        meta=meta,
        params=params,
    )

    # 确保 regime 在 shaped.meta 上可用,供买入闸门读取。
    if isinstance(shaped, dict):
        sm = shaped.get("meta")
        if isinstance(sm, dict) and sm.get("regime") is None and meta.get("regime") is not None:
            sm["regime"] = meta.get("regime")

    if not no_write:
        if output_dir is None:
            output_dir = project_root / "reports" / date_str / "premarket"
            analysis_name = f"{datetime.now(TZ_SHANGHAI).strftime('%H%M%S')}_analysis_v9.json"
        else:
            analysis_name = "analysis_v9.json"
        paths = v9out.write_v9_outputs(str(output_dir), shaped, filename=analysis_name)
        if isinstance(shaped, dict):
            shaped["paths"] = paths
    return shaped


def build_premarket_analysis_v9(report: Mapping[str, Any], project_root: Optional[Path | str] = None) -> Dict[str, Any]:
    """batch.py adapter: report -> v9 决策 (与 build_premarket_analysis_v7_3 同签名)。"""
    trade_date = _infer_trade_date_from_report(report)
    root = Path(project_root) if project_root is not None else DEFAULT_PROJECT_ROOT
    shaped = run_v9(trade_date, root, output_dir=None, no_write=False)
    return _adapt_for_batch(shaped)


def render_text(result: Mapping[str, Any]) -> str:
    meta = result.get("meta") or {}
    gate = result.get("buy_gate") or {}
    paths = result.get("paths") or {}
    lines = [
        "**盘前 v9 全量数据决策**",
        f"- 引擎：{meta.get('engine') or 'premarket_v9'}（基于 {meta.get('base_pipeline') or 'premarket_v7_2'}）",
        f"- 交易日：{meta.get('date_t0') or '-'}",
        f"- regime：{gate.get('regime')}｜买入下限 edge {gate.get('buy_floor')}｜Top {gate.get('buy_rank_cap')}｜上限 {gate.get('max_buys')}",
        f"- 候选池：{result.get('candidate_count') or 0}；买入候选：{len(result.get('top_candidates') or [])}；观察：{len(result.get('watch_candidates') or [])}",
    ]
    if paths.get("analysis_path"):
        lines.append(f"- 分析文件：`{paths['analysis_path']}`")
    lines.append("")
    lines.append("**BUY 候选**")
    buy_rows = result.get("top_candidates") or []
    if not buy_rows:
        lines.append("- 无。证据不足时不强行推荐。")
    else:
        for row in buy_rows:
            lines.append(
                f"- {row.get('rank')}. {row.get('name')}（{row.get('code')}）"
                f"｜edge {_fmt_num(row.get('score'))}"
                f"｜原因：{row.get('action_reason') or '-'}"
            )
    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    p.add_argument("--output-dir", default="")
    p.add_argument("--no-write", action="store_true")
    p.add_argument("--json", action="store_true")
    a = p.parse_args()
    shaped = run_v9(a.date, Path(a.project_root), Path(a.output_dir) if a.output_dir else None, a.no_write)
    result = _adapt_for_batch(shaped)
    if a.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
