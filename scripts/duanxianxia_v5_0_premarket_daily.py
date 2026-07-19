#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v5_0_premarket_daily.py  --  v5.0 盘前分析重跑

从 9:25 cron 已生成的 premarket report 中读取数据, 重跑 v5.0 管线。
由 agent_daily_refresh.py 入队, agent_job_worker.py 执行。
结果写入 reports/_audit/v5_0_premarket/YYYY-MM-DD.json。

用法: python3 duanxianxia_v5_0_premarket_daily.py --date 2026-07-19
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v5_0_runner import (
    run_v5_0_pipeline, VERSION,
    _build_bundle_from_report,
)
from duanxianxia_v5_0_d6_profile import (
    ProfileHistory,
)
from duanxianxia_v4_2_d6_emotion import (
    _extract_qxlive_metric,
    _extract_ztpool_pbbx, _smoothed_rate,
    RELAY_WEIGHT_1_2, RELAY_WEIGHT_2_3,
)
from duanxianxia_v7_1_data_loader import (
    load_capture_at_time, _extract_rows,
    DS_HOME_QXLIVE_TOP, DS_HOME_ZTPOOL, DS_REVIEW_DAILY,
    QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
)

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v5_0_premarket"
BACKTEST_DIR = PROJECT_ROOT / "reports" / "_audit" / "v5_0_backtest"
REPORTS_DIR = PROJECT_ROOT / "reports"
CAPTURES_DIR = PROJECT_ROOT / "captures"


def _shanghai_today() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    except Exception:
        return date.today().isoformat()


def _find_latest_premarket_report(today: str) -> Optional[Path]:
    """找今天 9:25 cron 生成的最新 premarket 报告。"""
    day_dir = REPORTS_DIR / today
    if not day_dir.is_dir():
        return None
    premarket_dir = day_dir / "premarket"
    if premarket_dir.is_dir():
        candidates = sorted(
            premarket_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return candidates[0]
    candidates = sorted(
        day_dir.glob("premarket_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _extract_review_metric(rows: List[Dict[str, Any]], metric_key: str) -> Optional[float]:
    """从 review_daily rows 中提取指定 metric_key 的值。"""
    for row in rows:
        if str(row.get("metric_key", "")) == metric_key:
            try:
                v = row.get("value")
                return float(v) if v not in (None, "") else None
            except (ValueError, TypeError):
                return None
    return None


# ============================================================================
# ProfileHistory 构建
# ============================================================================

@functools.lru_cache(maxsize=1)
def _build_history_from_raw_captures(project_root: Path, max_days: int = 60) -> ProfileHistory:
    """
    直接从 captures/ 原始数据构建 ProfileHistory, 不依赖过去分析结果。

    扫描 captures/ 下所有日期目录, 读取:
      - qxlive (盘前 9:25): ZTBX, LBBX, SZ, XD, DT, QX
      - review_daily (盘后收盘): ZTBX, LBBX, SZ, XD, DT, KQXY, QX
      - ztpool (盘后): PBBX 晋级率 → relay_health

    用于首次运行或过去分析结果不足时的回退方案。
    """
    history = ProfileHistory()
    if not CAPTURES_DIR.is_dir():
        return history

    date_dirs = sorted(
        [d for d in CAPTURES_DIR.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )[-max_days:]

    for day_dir in date_dirs:
        date_str = day_dir.name
        if len(date_str) != 10 or date_str[4] != "-":
            continue

        # --- 盘前 qxlive (9:25) ---
        qxlive = load_capture_at_time(
            project_root, date_str, DS_HOME_QXLIVE_TOP,
            max_hhmmss=QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
            pick="earliest_before", raise_if_missing=False,
        )
        qxlive_rows = _extract_rows(qxlive)

        ztbx_pre = _extract_qxlive_metric(qxlive_rows, "ZTBX")
        lbbx_pre = _extract_qxlive_metric(qxlive_rows, "LBBX")
        sz_pre = _extract_qxlive_metric(qxlive_rows, "SZ")
        xd_pre = _extract_qxlive_metric(qxlive_rows, "XD")
        dt_pre_raw = _extract_qxlive_metric(qxlive_rows, "DT")
        qx_pre = _extract_qxlive_metric(qxlive_rows, "QX")

        advance_share_pre = None
        if sz_pre is not None and xd_pre is not None and (sz_pre + xd_pre) > 0:
            advance_share_pre = round(sz_pre / (sz_pre + xd_pre), 4)
        dt_pre = int(dt_pre_raw) if dt_pre_raw is not None else None

        # --- 盘后 review_daily ---
        review = load_capture_at_time(
            project_root, date_str, DS_REVIEW_DAILY,
            pick="latest", raise_if_missing=False,
        )
        review_rows = _extract_rows(review) if review else []

        ztbx_close = _extract_review_metric(review_rows, "ZTBX")
        lbbx_close = _extract_review_metric(review_rows, "LBBX")
        sz_close = _extract_review_metric(review_rows, "SZ")
        xd_close = _extract_review_metric(review_rows, "XD")
        dt_close_raw = _extract_review_metric(review_rows, "DT")
        qx_close = _extract_review_metric(review_rows, "QX")
        kqxy_close = _extract_review_metric(review_rows, "KQXY")

        advance_share_close = None
        if sz_close is not None and xd_close is not None and (sz_close + xd_close) > 0:
            advance_share_close = round(sz_close / (sz_close + xd_close), 4)
        dt_close = int(dt_close_raw) if dt_close_raw is not None else None

        # --- 盘后 ztpool (接力) ---
        ztpool = load_capture_at_time(
            project_root, date_str, DS_HOME_ZTPOOL,
            pick="latest", raise_if_missing=False,
        )
        ztpool_rows = _extract_rows(ztpool)
        pbbx = _extract_ztpool_pbbx(ztpool_rows)

        j12 = _smoothed_rate(
            pbbx.get("PBBX_1_2", {}).get("promoted"),
            pbbx.get("PBBX_1_2", {}).get("eligible"),
        )
        j23 = _smoothed_rate(
            pbbx.get("PBBX_2_3", {}).get("promoted"),
            pbbx.get("PBBX_2_3", {}).get("eligible"),
        )

        relay_health = None
        if j12 is not None and j23 is not None:
            relay_health = round(RELAY_WEIGHT_1_2 * j12 + RELAY_WEIGHT_2_3 * j23, 2)
        elif j12 is not None:
            relay_health = round(j12, 2)
        elif j23 is not None:
            relay_health = round(j23, 2)

        # --- 添加到历史 (v5.0: ProfileHistory) ---
        # 只要有任何数据就加入, 不因单个指标缺失而丢弃整个交易日
        history.add_day(
            advance_share_close=advance_share_close,
            dt_close=float(dt_close) if dt_close is not None else None,
            ztbx_close=ztbx_close,
            lbbx_close=lbbx_close,
            qx_close=qx_close,
            kqxy_close=kqxy_close,
            advance_share_pre=advance_share_pre,
            dt_pre=float(dt_pre) if dt_pre is not None else None,
            ztbx_pre=ztbx_pre,
            lbbx_pre=lbbx_pre,
            qx_pre=qx_pre,
            relay_health=relay_health,
        )

    return history


def _build_history_from_past_results(source_dir: Path, max_days: int = 60) -> ProfileHistory:
    """
    从过去 v5.0 分析结果构建 ProfileHistory, 用于滚动分位计算。

    读取 OUTPUT_DIR 或 BACKTEST_DIR 下的 JSON 文件,
    提取 indicators 中的 close/pre 值重建历史。
    """
    history = ProfileHistory()
    if not source_dir.is_dir():
        return history

    records: List[Dict[str, Any]] = []
    for f in sorted(source_dir.glob("*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            if "results" in d:
                for r in d["results"]:
                    if r.get("status") == "ok":
                        records.append(r)
            elif d.get("status") == "ok":
                records.append(d)
        except Exception:
            continue

    records.sort(key=lambda r: r.get("date", ""))

    for r in records[-max_days:]:
        indicators = r.get("indicators", {})
        if not indicators:
            # 尝试从旧格式 (v4.2) 读取
            history.add_day(
                ztbx_close=r.get("ztbx_close"),
                lbbx_close=r.get("lbbx_close"),
                advance_share_close=r.get("advance_share_close"),
                dt_close=r.get("dt_close"),
                qx_close=r.get("close_qx"),
                kqxy_close=r.get("kqxy_t1"),
                ztbx_pre=r.get("ztbx_925"),
                lbbx_pre=r.get("lbbx_925"),
                advance_share_pre=r.get("advance_share"),
                dt_pre=r.get("dt_925"),
                qx_pre=r.get("qx_925"),
                relay_health=r.get("relay_health"),
            )
            continue

        # v5.0 格式: 从 indicators 中提取
        history.add_day(
            advance_share_close=_safe_float(indicators, "advance_share", "close_raw"),
            dt_close=_safe_float(indicators, "dt", "close_raw"),
            ztbx_close=_safe_float(indicators, "ztbx", "close_raw"),
            lbbx_close=_safe_float(indicators, "lbbx", "close_raw"),
            qx_close=_safe_float(indicators, "qx", "close_raw"),
            kqxy_close=_safe_float(indicators, "kqxy", "close_raw"),
            advance_share_pre=_safe_float(indicators, "advance_share", "pre_raw"),
            dt_pre=_safe_float(indicators, "dt", "pre_raw"),
            ztbx_pre=_safe_float(indicators, "ztbx", "pre_raw"),
            lbbx_pre=_safe_float(indicators, "lbbx", "pre_raw"),
            qx_pre=_safe_float(indicators, "qx", "pre_raw"),
            relay_health=_safe_float(indicators, "relay", "close_raw"),
        )

    return history


def _safe_float(indicators: Dict[str, Any], key: str, field: str) -> Optional[float]:
    """安全提取指标值。"""
    ind = indicators.get(key, {})
    if isinstance(ind, dict):
        v = ind.get(field)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                return None
    return None


def _merge_histories(*histories: ProfileHistory) -> ProfileHistory:
    """
    合并多个 ProfileHistory, 按各序列独立排序后去重重建。

    注意: 合并后丢失时间对应关系, 但分位计算只依赖值的分布,
    不依赖时间顺序, 因此不影响统计剖面计算。
    """
    all_vals: Dict[str, List[float]] = {
        "advance_share_close": [], "dt_close": [], "ztbx_close": [],
        "lbbx_close": [], "qx_close": [], "kqxy_close": [],
        "advance_share_pre": [], "dt_pre": [], "ztbx_pre": [],
        "lbbx_pre": [], "qx_pre": [], "relay_health": [],
    }

    for h in histories:
        for arr, key in [
            (h.advance_share_close, "advance_share_close"),
            (h.dt_close, "dt_close"),
            (h.ztbx_close, "ztbx_close"),
            (h.lbbx_close, "lbbx_close"),
            (h.qx_close, "qx_close"),
            (h.kqxy_close, "kqxy_close"),
            (h.advance_share_pre, "advance_share_pre"),
            (h.dt_pre, "dt_pre"),
            (h.ztbx_pre, "ztbx_pre"),
            (h.lbbx_pre, "lbbx_pre"),
            (h.qx_pre, "qx_pre"),
            (h.relay_health, "relay_health"),
        ]:
            for v in arr:
                if v is not None:
                    all_vals[key].append(v)

    result = ProfileHistory()
    max_len = max(len(v) for v in all_vals.values())
    for i in range(max_len):
        result.add_day(
            advance_share_close=_get_or_none(all_vals["advance_share_close"], i),
            dt_close=_get_or_none(all_vals["dt_close"], i),
            ztbx_close=_get_or_none(all_vals["ztbx_close"], i),
            lbbx_close=_get_or_none(all_vals["lbbx_close"], i),
            qx_close=_get_or_none(all_vals["qx_close"], i),
            kqxy_close=_get_or_none(all_vals["kqxy_close"], i),
            advance_share_pre=_get_or_none(all_vals["advance_share_pre"], i),
            dt_pre=_get_or_none(all_vals["dt_pre"], i),
            ztbx_pre=_get_or_none(all_vals["ztbx_pre"], i),
            lbbx_pre=_get_or_none(all_vals["lbbx_pre"], i),
            qx_pre=_get_or_none(all_vals["qx_pre"], i),
            relay_health=_get_or_none(all_vals["relay_health"], i),
        )
    return result


def _get_or_none(lst: List[float], idx: int) -> Optional[float]:
    return lst[idx] if idx < len(lst) else None


# ============================================================================
# 主入口
# ============================================================================

def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="分析日期, 默认上海时间今天")
    args = p.parse_args()
    today = args.date or _shanghai_today()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{today}.json"

    print(f"v5.0 premarket analysis: running for {today}...")

    report_path = _find_latest_premarket_report(today)
    if report_path is None:
        summary: Dict[str, Any] = {
            "version": VERSION,
            "date": today,
            "status": "error",
            "error": f"找不到 {today} 的 premarket 报告, 9:25 cron 可能未运行",
        }
        out_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"v5.0 premarket ERROR: no report found for {today}")
        return 1

    print(f"v5.0 premarket: using report {report_path}")

    try:
        with open(report_path, "r", encoding="utf-8") as f:
            report_data = json.load(f)
    except Exception as e:
        summary = {
            "version": VERSION, "date": today, "status": "error",
            "error": f"读取报告失败: {e}",
        }
        out_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return 1

    bundle = _build_bundle_from_report(report_data, PROJECT_ROOT, today)

    # 构建 ProfileHistory: 优先级 1) 过去分析结果  2) 原始 captures 数据
    history = _merge_histories(
        _build_history_from_past_results(OUTPUT_DIR),
        _build_history_from_past_results(BACKTEST_DIR),
        _build_history_from_raw_captures(PROJECT_ROOT),
    )
    print(f"v5.0 premarket: history close_days={history.close_days()}, "
          f"pre_days={history.pre_days()}")

    result = run_v5_0_pipeline(
        date_t0=today,
        project_root=str(PROJECT_ROOT),
        bundle=bundle,
        history=history,
    )

    if "error" in result:
        summary = {
            "version": VERSION,
            "date": today,
            "status": "error",
            "error": result["error"],
        }
    else:
        prof = result.get("profile", {})
        ep = result.get("execution_plan", {})
        orders = ep.get("orders", [])
        summary = {
            "version": VERSION,
            "date": today,
            "status": "ok",
            # 统计剖面
            "bottleneck": prof.get("bottleneck"),
            "bottleneck_name": prof.get("bottleneck_name"),
            "heat": prof.get("heat"),
            "divergence": prof.get("divergence"),
            "tilt": prof.get("tilt"),
            "direction_summary": prof.get("direction_summary"),
            "position": prof.get("position"),
            "buy_mode": prof.get("buy_mode"),
            "extreme_veto": prof.get("extreme_veto"),
            "veto_reason": prof.get("veto_reason"),
            "profit_collapse": prof.get("profit_collapse", False),
            "breadth_panic": prof.get("breadth_panic", False),
            # 指标明细
            "indicators": prof.get("indicators", {}),
            # 池乘子
            "pool_multipliers": prof.get("pool_multipliers", {}),
            "yizi_enabled": prof.get("yizi_enabled", True),
            "huanshou_enabled": prof.get("huanshou_enabled", True),
            "fenqi_enabled": prof.get("fenqi_enabled", True),
            "feiban_enabled": prof.get("feiban_enabled", True),
            # 订单
            "n_orders": len(orders),
            "orders": [
                {
                    "code": o["code"],
                    "name": o.get("name", ""),
                    "pool": o["pool"],
                    "position_pct": o["position_pct"],
                    "buy_strategy": o.get("buy_strategy", ""),
                    "height_mult": o.get("height_mult", 1.0),
                    "risk_mult": o.get("risk_mult", 1.0),
                    "profile_position": o.get("profile_position", 0.5),
                    "pool_mult": o.get("pool_mult", 1.0),
                }
                for o in orders
            ],
            "allocated_position": ep.get("allocated_position", 0),
            "reserve_position": ep.get("reserve_position", 0),
            "pools": {
                pn: {
                    "n_total": pd.get("n_total", 0),
                    "n_filtered": pd.get("n_filtered", 0),
                    "pool_mult": pd.get("pool_mult", 1.0),
                }
                for pn, pd in result.get("pools", {}).items()
            },
            "warnings": result.get("warnings", []),
            "diagnostics": result.get("diagnostics", {}),
            "_history_diag": {
                "close_days": history.close_days(),
                "pre_days": history.pre_days(),
                "advance_share_close_days": len(history.advance_share_close),
                "dt_close_days": len(history.dt_close),
                "ztbx_close_days": len(history.ztbx_close),
                "lbbx_close_days": len(history.lbbx_close),
                "qx_close_days": len(history.qx_close),
                "kqxy_close_days": len(history.kqxy_close),
                "advance_share_pre_days": len(history.advance_share_pre),
                "dt_pre_days": len(history.dt_pre),
                "ztbx_pre_days": len(history.ztbx_pre),
                "lbbx_pre_days": len(history.lbbx_pre),
                "qx_pre_days": len(history.qx_pre),
                "relay_health_days": len(history.relay_health),
            },
        }

    out_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"v5.0 premarket done: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())