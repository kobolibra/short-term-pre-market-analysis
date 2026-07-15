#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_v4_2_premarket_daily.py  --  v4.2 盘前分析重跑

从 9:25 cron 已生成的 premarket report 中读取数据，重跑 v4.2 管线。
由 agent_daily_refresh.py 入队，agent_job_worker.py 执行。
结果写入 reports/_audit/v4_2_premarket/YYYY-MM-DD.json。

用法: python3 duanxianxia_v4_2_premarket_daily.py --date 2026-07-14
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))

from duanxianxia_v4_2_runner import (
    run_v4_2_pipeline, VERSION,
    _build_bundle_from_report,
)
from duanxianxia_v4_2_d6_emotion import (
    D6History, _extract_qxlive_metric,
    _extract_ztpool_pbbx, _smoothed_rate,
    RELAY_WEIGHT_1_2, RELAY_WEIGHT_2_3,
)
from duanxianxia_v7_1_data_loader import (
    load_capture_at_time, _extract_rows,
    DS_HOME_QXLIVE_TOP, DS_HOME_ZTPOOL,
    QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
)

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE / "projects" / "duanxianxia"
OUTPUT_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_premarket"
BACKTEST_DIR = PROJECT_ROOT / "reports" / "_audit" / "v4_2_backtest"
REPORTS_DIR = PROJECT_ROOT / "reports"
CAPTURES_DIR = PROJECT_ROOT / "captures"


def _shanghai_today() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
    except Exception:
        return date.today().isoformat()


def _find_latest_premarket_report(today: str) -> Optional[Path]:
    """找今天 9:25 cron 生成的最新 premarket 报告 (与 feishu_rerun 同逻辑)"""
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


@functools.lru_cache(maxsize=1)
def _build_history_from_raw_captures(project_root: Path, max_days: int = 60) -> D6History:
    """直接从 captures/ 原始数据构建 D6History, 不依赖过去分析结果。

    扫描 captures/ 下所有日期目录, 读取 qxlive 和 ztpool 数据,
    提取 ztbx_925, lbbx_925, advance_share, dt_925, relay_health, kqxy。

    用于首次运行或过去分析结果不足时的回退方案。
    """
    history = D6History()
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

        # qxlive (盘前, 最早 <= 09:30:00) — 用于 ZTBX/LBBX/SZ/XD/DT
        qxlive = load_capture_at_time(
            project_root, date_str, DS_HOME_QXLIVE_TOP,
            max_hhmmss=QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
            pick="earliest_before", raise_if_missing=False,
        )
        qxlive_rows = _extract_rows(qxlive)

        ztbx = _extract_qxlive_metric(qxlive_rows, "ZTBX")
        lbbx = _extract_qxlive_metric(qxlive_rows, "LBBX")
        sz = _extract_qxlive_metric(qxlive_rows, "SZ")
        xd = _extract_qxlive_metric(qxlive_rows, "XD")
        dt = _extract_qxlive_metric(qxlive_rows, "DT")

        # 盘前 QX (仅展示, 不参与决策)
        pre_qx = _extract_qxlive_metric(qxlive_rows, "QX")

        advance_share = None
        if sz is not None and xd is not None and (sz + xd) > 0:
            advance_share = round(sz / (sz + xd), 4)

        # qxlive 盘后 (取最新, 不限时间) — 用于 KQXY + QX + P/B 盘后指标 (v4)
        kqxy = None
        close_qx = None
        ztbx_close = None
        lbbx_close = None
        advance_share_close = None
        dt_close = None
        kq_live = load_capture_at_time(
            project_root, date_str, DS_HOME_QXLIVE_TOP,
            pick="latest", raise_if_missing=False,
        )
        if kq_live:
            kq_rows = _extract_rows(kq_live)
            kq_val = _extract_qxlive_metric(kq_rows, "KQXY")
            if kq_val is not None and kq_val > 0:
                kqxy = kq_val
            qx_close = _extract_qxlive_metric(kq_rows, "QX")
            if qx_close is not None:
                close_qx = qx_close
            # v4 新增: P/B 盘后指标 (用于水位计算, 市场底色)
            ztbx_close = _extract_qxlive_metric(kq_rows, "ZTBX")
            lbbx_close = _extract_qxlive_metric(kq_rows, "LBBX")
            sz_close = _extract_qxlive_metric(kq_rows, "SZ")
            xd_close = _extract_qxlive_metric(kq_rows, "XD")
            dt_close_raw = _extract_qxlive_metric(kq_rows, "DT")
            if sz_close is not None and xd_close is not None and (sz_close + xd_close) > 0:
                advance_share_close = round(sz_close / (sz_close + xd_close), 4)
            dt_close = int(dt_close_raw) if dt_close_raw is not None else None

        # ztpool (盘后, 取最新)
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

        # v4: 只要有任何数据就加入, 不因盘前缺失而丢弃盘后/relay数据
        # 盘前 ZTBX 缺失不代表盘后数据不可用, relay_health 来自 ztpool 也独立于盘前
        if ztbx is not None or ztbx_close is not None or relay_health is not None:
            history.add_day(
                # 盘前 (向后兼容旧参数名)
                ztbx=ztbx, lbbx=lbbx,
                advance_share=advance_share,
                dt=int(dt) if dt is not None else None,
                # 盘后 (v4 新增, 用于水位计算)
                ztbx_close=ztbx_close,
                lbbx_close=lbbx_close,
                advance_share_close=advance_share_close,
                dt_close=dt_close,
                # 其他
                relay_health=relay_health,
                kqxy=kqxy,
                pre_qx=pre_qx,
                close_qx=close_qx,
            )

    return history


def _build_history_from_past_results(source_dir: Path, max_days: int = 60) -> D6History:
    """从过去分析结果构建 D6History, 用于滚动分位计算。"""
    history = D6History()
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
        # v4: 放宽门控, 有盘后或relay数据的记录也要加入
        if r.get("ztbx_925") is not None or r.get("ztbx_close") is not None or r.get("relay_health") is not None:
            history.add_day(
                ztbx=r["ztbx_925"],
                lbbx=r.get("lbbx_925"),
                advance_share=r.get("advance_share"),
                dt=r.get("dt_925"),
                # v4 新增: 盘后指标 (旧数据可能不存在, None 时自动跳过)
                ztbx_close=r.get("ztbx_close"),
                lbbx_close=r.get("lbbx_close"),
                advance_share_close=r.get("advance_share_close"),
                dt_close=r.get("dt_close"),
                relay_health=r.get("relay_health"),
                kqxy=r.get("kqxy_t1"),
                pre_qx=r.get("qx_925"),
                close_qx=r.get("close_qx"),
            )
    return history


def _merge_histories(*histories: D6History) -> D6History:
    """合并多个 D6History, 按各序列独立排序后去重重建。

    注意: 合并后丢失时间对应关系, 但分位计算只依赖值的分布,
    不依赖时间顺序, 因此不影响水位/方向计算。
    """
    all_vals: Dict[str, List[float]] = {
        "ztbx": [], "lbbx": [], "adv": [], "dt": [],
        "ztbx_close": [], "lbbx_close": [], "adv_close": [], "dt_close": [],
        "relay": [], "kqxy": [], "pre_qx": [], "close_qx": [],
    }
    for h in histories:
        for arr, key in [
            (h.ztbx_pre_values, "ztbx"), (h.lbbx_pre_values, "lbbx"),
            (h.advance_share_pre_values, "adv"), (h.dt_pre_values, "dt"),
            (h.ztbx_close_values, "ztbx_close"), (h.lbbx_close_values, "lbbx_close"),
            (h.advance_share_close_values, "adv_close"), (h.dt_close_values, "dt_close"),
            (h.relay_health_values, "relay"), (h.kqxy_values, "kqxy"),
            (h.pre_qx_values, "pre_qx"), (h.close_qx_values, "close_qx"),
        ]:
            for v in arr:
                if v is not None:
                    all_vals[key].append(v)
    result = D6History()
    ztbx = sorted(all_vals["ztbx"])
    lbbx = sorted(all_vals["lbbx"])
    adv = sorted(all_vals["adv"])
    dt = sorted(all_vals["dt"])
    ztbx_close = sorted(all_vals["ztbx_close"])
    lbbx_close = sorted(all_vals["lbbx_close"])
    adv_close = sorted(all_vals["adv_close"])
    dt_close = sorted(all_vals["dt_close"])
    relay = sorted(all_vals["relay"])
    kqxy = sorted(all_vals["kqxy"])
    pre_qx = sorted(all_vals["pre_qx"])
    close_qx = sorted(all_vals["close_qx"])
    max_len = max(
        len(ztbx), len(lbbx), len(adv), len(dt),
        len(ztbx_close), len(lbbx_close), len(adv_close), len(dt_close),
        len(relay), len(kqxy), len(pre_qx), len(close_qx),
    )
    for i in range(max_len):
        result.add_day(
            ztbx=ztbx[i] if i < len(ztbx) else None,
            lbbx=lbbx[i] if i < len(lbbx) else None,
            advance_share=adv[i] if i < len(adv) else None,
            dt=dt[i] if i < len(dt) else None,
            ztbx_close=ztbx_close[i] if i < len(ztbx_close) else None,
            lbbx_close=lbbx_close[i] if i < len(lbbx_close) else None,
            advance_share_close=adv_close[i] if i < len(adv_close) else None,
            dt_close=dt_close[i] if i < len(dt_close) else None,
            relay_health=relay[i] if i < len(relay) else None,
            kqxy=kqxy[i] if i < len(kqxy) else None,
            pre_qx=pre_qx[i] if i < len(pre_qx) else None,
            close_qx=close_qx[i] if i < len(close_qx) else None,
        )
    return result


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="分析日期, 默认上海时间今天")
    args = p.parse_args()
    today = args.date or _shanghai_today()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{today}.json"

    print(f"v4.2 premarket analysis: running for {today}...")

    report_path = _find_latest_premarket_report(today)
    if report_path is None:
        summary: Dict[str, Any] = {
            "version": VERSION,
            "date": today,
            "status": "error",
            "error": f"找不到 {today} 的 premarket 报告, 9:25 cron 可能未运行",
        }
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"v4.2 premarket ERROR: no report found for {today}")
        return 1

    print(f"v4.2 premarket: using report {report_path}")

    try:
        with open(report_path, "r", encoding="utf-8") as f:
            report_data = json.load(f)
    except Exception as e:
        summary = {
            "version": VERSION, "date": today, "status": "error",
            "error": f"读取报告失败: {e}",
        }
        out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return 1

    bundle = _build_bundle_from_report(report_data, PROJECT_ROOT, today)

    # 构建 D6History: 优先级 1) 过去分析结果  2) 原始 captures 数据
    # v4: 始终合并 raw captures, 因为盘后 P/B 数据只存在于原始 captures 中,
    # 旧分析结果 JSON 不包含 ztbx_close 等 v4 新字段
    history = _merge_histories(
        _build_history_from_past_results(OUTPUT_DIR),
        _build_history_from_past_results(BACKTEST_DIR),
        _build_history_from_raw_captures(PROJECT_ROOT),
    )
    print(f"v4.2 premarket: history days(pre)={history.history_days}, close_days={history.close_days}")

    result = run_v4_2_pipeline(
        date_t0=today,
        project_root=str(PROJECT_ROOT),
        bundle=bundle,
        history=history,
    )

    if "error" in result:
        summary: Dict[str, Any] = {
            "version": VERSION,
            "date": today,
            "status": "error",
            "error": result["error"],
        }
    else:
        emo = result.get("emotion", {})
        ep = result.get("execution_plan", {})
        orders = ep.get("orders", [])
        summary = {
            "version": VERSION,
            "date": today,
            "status": "ok",
            "phase": emo.get("phase"),
            "phase_label": emo.get("phase_label"),
            "level": emo.get("level"),
            "level_score": emo.get("level_score"),
            "direction": emo.get("direction"),
            "risk_tier": emo.get("risk_tier"),
            "position_cap": emo.get("position_cap", 1.0),
            "buy_mode": emo.get("buy_mode"),
            "relay_health": emo.get("relay_health"),
            "jinji_1_2": emo.get("jinji_1_2"),
            "jinji_2_3": emo.get("jinji_2_3"),
            "ztbx_925": emo.get("ztbx_925"),
            "lbbx_925": emo.get("lbbx_925"),
            "advance_share": emo.get("advance_share"),
            "dt_925": emo.get("dt_925"),
            "hard_veto": emo.get("hard_veto"),
            "profit_collapse": emo.get("profit_collapse", False),
            "breadth_panic": emo.get("breadth_panic", False),
            "kqxy_t1": emo.get("kqxy_t1"),
            "kqxy_t2": emo.get("kqxy_t2"),
            "loss_level": emo.get("loss_level"),
            "loss_direction": emo.get("loss_direction"),
            "loss_overlay": emo.get("loss_overlay"),
            "qx_925": emo.get("qx_925"),
            "qx_stats": emo.get("qx_stats", {}),
            "profit_level": emo.get("profit_level"),
            "breadth_level": emo.get("breadth_level"),
            "relay_level": emo.get("relay_level"),
            "profit_delta": emo.get("profit_delta"),
            "breadth_delta": emo.get("breadth_delta"),
            "relay_delta": emo.get("relay_delta"),
            # v4 新增: 双时间截面水位 + 盘后原始指标
            "close_level_score": emo.get("close_level_score"),
            "pre_level_score": emo.get("pre_level_score"),
            "level_source": emo.get("level_source"),
            "ztbx_close": emo.get("ztbx_close"),
            "lbbx_close": emo.get("lbbx_close"),
            "advance_share_close": emo.get("advance_share_close"),
            "dt_close": emo.get("dt_close"),
            "height_preference": emo.get("height_preference"),
            "fenqi_priority": emo.get("fenqi_priority"),
            "pool_enabled": emo.get("pool_enabled", {}),
            "pool_mult": emo.get("pool_mult", {}),
            "phase_confidence": emo.get("phase_confidence"),
            "data_quality": emo.get("data_quality"),
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
                }
                for o in orders
            ],
            "allocated_position": ep.get("allocated_position", 0),
            "reserve_position": ep.get("reserve_position", 0),
            "pools": {
                pn: {
                    "n_total": pd.get("n_total", 0),
                    "n_filtered": pd.get("n_filtered", 0),
                }
                for pn, pd in result.get("pools", {}).items()
            },
            "warnings": result.get("warnings", []),
            "diagnostics": result.get("diagnostics", {}),
            "_history_diag": {
                "ztbx_pre_days": len(history.ztbx_pre_values),
                "ztbx_close_days": len(history.ztbx_close_values),
                "advance_share_pre_days": len(history.advance_share_pre_values),
                "advance_share_close_days": len(history.advance_share_close_values),
                "relay_health_days": len(history.relay_health_values),
                "dt_pre_days": len(history.dt_pre_values),
                "dt_close_days": len(history.dt_close_values),
                "kqxy_days": len(history.kqxy_values),
                "close_qx_days": len(history.close_qx_values),
                "pre_qx_days": len(history.pre_qx_values),
            },
        }

    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"v4.2 premarket done: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())