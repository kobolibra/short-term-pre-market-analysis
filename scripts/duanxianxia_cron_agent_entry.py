#!/usr/bin/env python3
# DEPRECATED ENTRYPOINT
#
# Cron jobs must call Git-tracked shell runners instead:
#   - bash scripts/duanxianxia_cron_runner.sh premarket
#   - bash scripts/duanxianxia_cron_runner.sh intraday_cashflow
#   - bash scripts/duanxianxia_postmarket_chain_runner.sh
#
# This file is intentionally kept only for manual inspection/backward compatibility.
# Do not wire production cron/jobs.json back to this local-only script.
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from duanxianxia_batch import load_report, render_text  # type: ignore
from duanxianxia_intraday_validator import validate as validate_intraday  # type: ignore
from duanxianxia_premarket_v7_2_runner import run_v7_2  # type: ignore
from duanxianxia_v7_1_data_loader import CaptureNotFoundError  # type: ignore

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _now_date() -> str:
    return datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d")


def _run(cmd: List[str], *, timeout: int) -> Tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(WORKSPACE),
        text=True,
        capture_output=True,
        timeout=timeout,
        env=os.environ.copy(),
    )
    return proc.returncode, proc.stdout or "", proc.stderr or ""


def _extract_json_payload(stdout: str) -> Dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}

    # Some upstream scripts occasionally print login/logout noise before the
    # JSON payload. Prefer the last complete JSON object in stdout.
    candidates: List[str] = []
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace != -1 and last_brace >= first_brace:
        candidates.append(text[first_brace : last_brace + 1])

    for line_idx, line in enumerate(text.splitlines()):
        if line.lstrip().startswith("{"):
            tail = "\n".join(text.splitlines()[line_idx:]).strip()
            if tail:
                candidates.append(tail)
            break

    candidates.append(text)

    seen = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        try:
            payload = json.loads(candidate)
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload

    raise ValueError("no JSON object found in stdout")



def _run_json(cmd: List[str], *, timeout: int) -> Tuple[int, Dict[str, Any], str, str]:
    rc, stdout, stderr = _run(cmd, timeout=timeout)
    payload: Dict[str, Any] = {}
    try:
        payload = _extract_json_payload(stdout)
    except Exception as exc:
        raise RuntimeError(
            f"failed to parse JSON output for command {' '.join(cmd)}: {exc}\nSTDOUT:\n{stdout[:4000]}\nSTDERR:\n{stderr[:4000]}"
        ) from exc
    return rc, payload, stdout, stderr


def _paths_section(report: Dict[str, Any]) -> str:
    lines = ["", "**文件路径**"]
    report_path = str(report.get("report_path") or "").strip()
    if report_path:
        lines.append(f"- 报告文件：`{report_path}`")
    source_report_path = str(report.get("source_report_path") or "").strip()
    if source_report_path:
        lines.append(f"- 源报告文件：`{source_report_path}`")
    for item in report.get("items", []) or []:
        label = item.get("dataset_label") or item.get("dataset_id") or item.get("dataset") or "unknown"
        capture_path = str(item.get("capture_path") or "").strip()
        if capture_path:
            lines.append(f"- {label}：`{capture_path}`")
    return "\n".join(lines)


def _stderr_section(stderr: str) -> str:
    text = stderr.strip()
    if not text:
        return ""
    return "\n".join(["", "**stderr**", text[:12000]])


def _render_v72_summary(result: Dict[str, Any]) -> str:
    meta = result.get("meta") or {}
    paths = result.get("paths") or {}
    warnings = meta.get("warnings") or []
    cutoffs_used = meta.get("cutoffs_used") or {}
    top_candidates = result.get("top_candidates") or []
    actionable = result.get("actionable_candidates") or []

    lines = [
        "",
        "**盘前 v7.2 分析结果**",
        f"- 版本：{result.get('version') or 'premarket_v7_2'}",
        f"- 交易日：{meta.get('date_t0') or '-'}",
        f"- 生成时间：{meta.get('generated_at') or '-'}",
        f"- 候选数：{meta.get('candidate_count') or 0}",
        f"- setup_stats：{json.dumps(result.get('setup_stats') or {}, ensure_ascii=False)}",
        f"- action_stats：{json.dumps(result.get('action_stats') or {}, ensure_ascii=False)}",
    ]
    if cutoffs_used:
        lines.append(
            f"- 分析窗口：auction<={cutoffs_used.get('premarket_auction_cutoff') or '-'} / qxlive<={cutoffs_used.get('qxlive_t0_cutoff') or '-'}"
        )
        if cutoffs_used.get("late_start_fallback"):
            lines.append("- 模式：迟启动降级分析（已放宽截止时间以避免只抓取不出结论）")
    if paths:
        if paths.get("analysis_path"):
            lines.append(f"- 分析文件：`{paths['analysis_path']}`")
        if paths.get("anchors_path"):
            lines.append(f"- 盘中锚点文件：`{paths['anchors_path']}`")
    if warnings:
        lines.append("- warnings：")
        for warning in warnings:
            lines.append(f"  - {warning}")
    show_rows = actionable or top_candidates
    if show_rows:
        lines.append("- Top candidates：")
        for idx, row in enumerate(show_rows[:10], start=1):
            lines.append(
                f"  - {idx}. {row.get('name')}（{row.get('code')}）"
                f"｜action={row.get('action')}｜setup={row.get('setup_v72') or row.get('setup')}"
                f"｜score={row.get('final_score') or row.get('score')}"
            )
    return "\n".join(lines)


def _latest_capture_hhmmss(report: Dict[str, Any], dataset_ids: List[str]) -> str:
    latest = ""
    wanted = set(dataset_ids)
    for item in report.get("items", []) or []:
        dataset_id = str(item.get("dataset_id") or "").strip()
        if dataset_id not in wanted:
            continue
        capture_path = str(item.get("capture_path") or "").strip()
        if not capture_path:
            continue
        stem = Path(capture_path).stem
        if len(stem) == 6 and stem.isdigit() and stem > latest:
            latest = stem
    return latest


def _render_premarket_failure_summary(capture_report: Dict[str, Any], exc: Exception, *, late_cutoff: str = "") -> str:
    lines = [
        render_text(capture_report),
        _paths_section(capture_report),
        "",
        "**盘前分析状态**",
        "- 状态：失败",
        f"- 原因：{exc.__class__.__name__}: {str(exc).strip()}",
    ]
    if late_cutoff:
        lines.append(f"- 说明：已尝试按迟启动兜底窗口放宽到 <= {late_cutoff}，仍未能产出分析结果")
    else:
        lines.append("- 说明：仅完成抓取，未能产出盘前分析；本次结果不应视为有效盘前结论")
    return "\n".join(lines)


def _render_validator_summary(result: Dict[str, Any]) -> str:
    lines = ["", "**盘中锚点校验**"]
    lines.append(f"- enabled：{result.get('enabled')}")
    if result.get("reason"):
        lines.append(f"- reason：{result.get('reason')}")
    if result.get("validation_path"):
        lines.append(f"- validation_path：`{result['validation_path']}`")
    setup_summary = result.get("setup_summary") or {}
    if setup_summary:
        lines.append(f"- setup_summary：{json.dumps(setup_summary, ensure_ascii=False)}")
    return "\n".join(lines)


def run_premarket(target_date: str) -> Tuple[int, str]:
    rc_capture, capture_report, _, capture_stderr = _run_json(
        [
            "python3",
            "scripts/duanxianxia_batch.py",
            "premarket",
            "--capture-only",
            "--json",
            "--webhook-url",
            "",
        ],
        timeout=900,
    )
    capture_report_path = str(capture_report.get("report_path") or "").strip()
    if not capture_report_path:
        raise RuntimeError("premarket capture_only did not return report_path")

    capture_loaded = load_report(capture_report_path)
    late_cutoff = _latest_capture_hhmmss(
        capture_loaded,
        [
            "auction.jjyd.vratio",
            "auction.jjyd.qiangchou",
            "auction.jjyd.net_amount",
            "auction.jjlive.fengdan",
            "home.qxlive.top_metrics",
            "home.kaipan.plate.summary",
            "rank.rocket",
            "rank.hot_stock_day",
        ],
    )

    try:
        v72_result = run_v7_2(target_date, PROJECT_ROOT)
    except CaptureNotFoundError as exc:
        if late_cutoff:
            try:
                v72_result = run_v7_2(
                    target_date,
                    PROJECT_ROOT,
                    premarket_auction_cutoff_override=late_cutoff,
                    qxlive_t0_cutoff_override=late_cutoff,
                )
            except Exception as fallback_exc:
                summary = _render_premarket_failure_summary(capture_loaded, fallback_exc, late_cutoff=late_cutoff)
                summary += _stderr_section(capture_stderr)
                return 2, summary
        else:
            summary = _render_premarket_failure_summary(capture_loaded, exc)
            summary += _stderr_section(capture_stderr)
            return 2, summary
    except Exception as exc:
        summary = _render_premarket_failure_summary(capture_loaded, exc)
        summary += _stderr_section(capture_stderr)
        return 2, summary

    summary = render_text(capture_loaded)
    summary += _paths_section(capture_loaded)
    summary += _render_v72_summary(v72_result)
    summary += _stderr_section(capture_stderr)

    return (0 if rc_capture == 0 else rc_capture), summary


def run_intraday() -> Tuple[int, str]:
    rc, report, _, stderr = _run_json(
        [
            "python3",
            "scripts/duanxianxia_batch.py",
            "intraday_cashflow",
            "--json",
            "--webhook-url",
            "",
        ],
        timeout=900,
    )
    summary = render_text(report)
    summary += _paths_section(report)
    validator = validate_intraday(project_root=PROJECT_ROOT)
    summary += _render_validator_summary(validator)
    summary += _stderr_section(stderr)
    return rc, summary


def run_postmarket(target_date: str) -> Tuple[int, str]:
    rc_capture, capture_report, _, capture_stderr = _run_json(
        [
            "python3",
            "scripts/duanxianxia_batch.py",
            "postmarket_cashflow",
            "--capture-only",
            "--json",
            "--webhook-url",
            "",
        ],
        timeout=1200,
    )
    capture_report_path = str(capture_report.get("report_path") or "").strip()
    if not capture_report_path:
        raise RuntimeError("postmarket capture_only did not return report_path")

    rc_dailyline, dailyline_report, _, dailyline_stderr = _run_json(
        [
            "python3",
            "scripts/duanxianxia_batch.py",
            "dailyline",
            "--target-date",
            target_date,
            "--json",
        ],
        timeout=1800,
    )

    rc_analysis, final_report, _, analysis_stderr = _run_json(
        [
            "python3",
            "scripts/duanxianxia_batch.py",
            "postmarket_cashflow",
            "--report-path",
            capture_report_path,
            "--save-analysis-copy",
            "--json",
            "--webhook-url",
            "",
        ],
        timeout=1200,
    )

    summary = render_text(final_report)
    summary += _paths_section(final_report)
    summary += "\n\n**复盘日线下载补充**\n"
    summary += render_text(dailyline_report)
    summary += _paths_section(dailyline_report)
    summary += _stderr_section(capture_stderr + ("\n" if capture_stderr and dailyline_stderr else "") + dailyline_stderr + ("\n" if (capture_stderr or dailyline_stderr) and analysis_stderr else "") + analysis_stderr)

    if rc_capture != 0:
        return rc_capture, summary
    if rc_dailyline != 0:
        return rc_dailyline, summary
    return rc_analysis, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Cron-safe entrypoint for duanxianxia jobs")
    parser.add_argument("group", choices=["premarket", "intraday_cashflow", "postmarket"])
    parser.add_argument("--target-date", default="", help="Override trade date (YYYY-MM-DD), default today in Asia/Shanghai")
    args = parser.parse_args()

    target_date = args.target_date.strip() or _now_date()

    if args.group == "premarket":
        rc, summary = run_premarket(target_date)
    elif args.group == "intraday_cashflow":
        rc, summary = run_intraday()
    else:
        rc, summary = run_postmarket(target_date)

    sys.stdout.write(summary.rstrip() + "\n")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
