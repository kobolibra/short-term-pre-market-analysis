#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0105: validate Task 0105 高位/连板/前一日炸败 risk gate.

Re-runs the v9 engine on 2026-07-01 real captured data with NO push
(duanxianxia_premarket_v9_runner.py --date 2026-07-01 --json --no-write),
then inspects 002674 (and the full candidate pool) to confirm the new
risk gate fires (risk_flag / risk_detail / dropped out of BUY).

READ-ONLY wrt production: does NOT touch webhook / feishu bitable /
production reports (those live only in duanxianxia_batch.py, which is
not invoked here). Also runs v9_edge unit self-test as a guard.
"""
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

WS = Path("/home/investmentofficehku/.openclaw/workspace")
V9_RUNNER = WS / "scripts" / "duanxianxia_premarket_v9_runner.py"
EDGE_MOD = WS / "scripts" / "duanxianxia_v9_edge.py"
TZ = ZoneInfo("Asia/Shanghai")
DATE = "2026-07-01"
TARGET = "002674"


def _find(rows, code):
    for r in rows or []:
        if str(r.get("code") or "").strip() == code:
            return r
    return None


def main():
    print("[0105] start %s" % datetime.now(TZ).isoformat())
    summary = {"job": "0105_risk_gate_validate", "date": DATE, "target": TARGET}

    # 1) unit self-test of patched edge module (proves gate logic locally)
    st = subprocess.run([sys.executable, str(EDGE_MOD)], cwd=str(WS),
                        capture_output=True, text=True, timeout=120)
    summary["edge_self_test_rc"] = st.returncode
    summary["edge_self_test_tail"] = (st.stdout + st.stderr)[-500:]

    # 2) re-run v9 engine on real 2026-07-01 data, NO push, NO write
    proc = subprocess.run(
        [sys.executable, str(V9_RUNNER), "--date", DATE, "--json", "--no-write"],
        cwd=str(WS), capture_output=True, text=True, timeout=720,
    )
    summary["runner_rc"] = proc.returncode
    if proc.stderr.strip():
        summary["runner_stderr_tail"] = proc.stderr[-800:]

    result = {}
    try:
        result = json.loads(proc.stdout)
    except Exception as exc:
        summary["parse_error"] = str(exc)
        summary["stdout_tail"] = proc.stdout[-800:]

    if result:
        allc = result.get("all_candidates") or []
        buys = result.get("top_candidates") or []
        watch = result.get("watch_candidates") or []
        summary["candidate_count"] = result.get("candidate_count")
        summary["buy_count"] = len(buys)
        summary["buy_codes"] = [str(b.get("code")) for b in buys]
        summary["target_in_buy"] = TARGET in summary["buy_codes"]

        row = _find(allc, TARGET) or _find(buys, TARGET) or _find(watch, TARGET)
        if row:
            ctx = row.get("context_detail") or {}
            comps = row.get("edge_components") or {}
            summary["target_row"] = {
                "edge_score": row.get("edge_score"),
                "score": row.get("score"),
                "action_type": row.get("action_type"),
                "alpha_type": row.get("alpha_type"),
                "risk_flag": row.get("risk_flag"),
                "risk_detail": row.get("risk_detail"),
                "risk_penalty": (comps.get("risk_penalty") if isinstance(comps, dict) else None),
                "reasons": (row.get("reasons") or [])[:5],
                "risks": (row.get("risks") or [])[:6],
                "context_detail": {
                    "t1_zt_board_label": ctx.get("t1_zt_board_label"),
                    "t1_in_ztpool": ctx.get("t1_in_ztpool"),
                    "ztpool_raw": ctx.get("ztpool_raw"),
                    "market_longtou_height": ctx.get("market_longtou_height"),
                    "cashflow_continuity": ctx.get("cashflow_continuity"),
                },
            }
            gate = row.get("risk_detail") or {}
            summary["gate_fired"] = bool(gate)
            if summary["target_in_buy"]:
                summary["verdict"] = "STILL_BUY"
            elif bool(gate):
                summary["verdict"] = "VETOED_OR_DOWNGRADED"
            else:
                summary["verdict"] = "NOT_IN_BUY_BUT_NO_GATE_DATA"
        else:
            summary["target_row"] = None
            summary["verdict"] = "TARGET_NOT_IN_POOL"

    print("[0105] SUMMARY_JSON_BEGIN")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print("[0105] SUMMARY_JSON_END")
    ok = summary.get("runner_rc") == 0 and summary.get("edge_self_test_rc") == 0
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
