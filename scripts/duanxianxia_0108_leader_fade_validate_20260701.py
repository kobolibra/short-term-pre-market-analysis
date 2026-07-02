#!/usr/bin/env python3
"""
duanxianxia_0108_leader_fade_validate_20260701.py — Task 0108 验证脚本

目标: 验证"高位龙头褪色(leader_fade)"闸门已接入 0105 风险层。
  review.ltgd.range 个股级(在梯队 + 区间涨幅>=30% + 未在当日涨停池) -> 对 002674
  这类 6/30 跌停掉出涨停池、但仍在 5 日龙头梯队的妖股下硬否决/降级。

只读验证: v9 跑 --date 2026-07-01 --json --no-write (NO webhook/bitable),
  同时跑 v9_edge 自测(self-test), dump 002674 全行 + context_detail(t1_ltgd_*),
  证明 002674 由 STILL_BUY 转为 VETOED_OR_DOWNGRADED。
  不 import duanxianxia_batch, 不触发任何 webhook / 多维表推送。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import traceback

WS = "/home/investmentofficehku/.openclaw/workspace"
V9_RUNNER = os.path.join(WS, "scripts", "duanxianxia_premarket_v9_runner.py")
EDGE_MOD = os.path.join(WS, "scripts", "duanxianxia_v9_edge.py")
TARGET = "002674"
DATE = "2026-07-01"

SUMMARY_JSON_BEGIN = "===SUMMARY_JSON_BEGIN==="
SUMMARY_JSON_END = "===SUMMARY_JSON_END==="


def _norm(code):
    s = str(code or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _run_edge_self_test():
    print("[0108] running v9_edge self-test ...")
    try:
        r = subprocess.run([sys.executable, EDGE_MOD], capture_output=True, text=True, timeout=120)
        print("[0108] edge self-test stdout:\n" + (r.stdout or ""))
        if r.stderr:
            print("[0108] edge self-test stderr:\n" + r.stderr)
        return r.returncode
    except Exception as e:
        print("[0108] edge self-test EXCEPTION: %r" % e)
        traceback.print_exc()
        return 99


def _run_v9_runner():
    print("[0108] running v9 runner --date %s --json --no-write ..." % DATE)
    try:
        r = subprocess.run(
            [sys.executable, V9_RUNNER, "--date", DATE, "--json", "--no-write"],
            capture_output=True, text=True, timeout=720,
        )
        if r.stderr:
            print("[0108] v9 runner stderr (tail):\n" + "\n".join(r.stderr.splitlines()[-40:]))
        return r.returncode, r.stdout or ""
    except Exception as e:
        print("[0108] v9 runner EXCEPTION: %r" % e)
        traceback.print_exc()
        return 98, ""


def _parse_runner_json(stdout):
    # runner 以 --json 输出一个 JSON 对象(可能夹杂日志); 取最后一个大括号块。
    s = stdout.strip()
    if not s:
        return None
    start = s.find("{")
    end = s.rfind("}")
    if start < 0 or end < 0 or end <= start:
        return None
    for probe_start in (start, s.find("{", start)):
        try:
            return json.loads(s[probe_start:end + 1])
        except Exception:
            continue
    # 逐行回退: 找能解析的最大 JSON 尾块
    try:
        return json.loads(s[start:end + 1])
    except Exception:
        return None


def _find_row(rows, target):
    for r in rows or []:
        if _norm(r.get("code")) == target:
            return r
    return None


def main():
    summary = {"job": "0108_leader_fade_validate", "date": DATE, "target": TARGET}

    edge_self_test_rc = _run_edge_self_test()
    summary["edge_self_test_rc"] = edge_self_test_rc

    runner_rc, stdout = _run_v9_runner()
    summary["runner_rc"] = runner_rc

    data = _parse_runner_json(stdout)
    summary["runner_json_parsed"] = bool(data)

    verdict = "UNKNOWN"
    target_row = None
    gate = {}
    if data:
        all_c = data.get("all_candidates") or []
        top_c = data.get("top_candidates") or []
        watch_c = data.get("watch_candidates") or []
        summary["candidate_count"] = data.get("candidate_count") or len(all_c)
        summary["top_count"] = len(top_c)

        in_buy = _find_row(top_c, TARGET)
        target_row = in_buy or _find_row(all_c, TARGET) or _find_row(watch_c, TARGET)

        if target_row is not None:
            gate = target_row.get("risk_detail") or {}
            ctx = target_row.get("context_detail") or {}
            summary["target_action_type"] = target_row.get("action_type")
            summary["target_edge_score"] = target_row.get("edge_score")
            summary["target_score"] = target_row.get("score")
            summary["target_risk_flag"] = target_row.get("risk_flag")
            summary["target_risk_detail"] = gate
            summary["context_detail"] = {
                "t1_zt_board_label": ctx.get("t1_zt_board_label"),
                "t1_in_ztpool": ctx.get("t1_in_ztpool"),
                "ztpool_raw": ctx.get("ztpool_raw"),
                "market_longtou_height": ctx.get("market_longtou_height"),
                "cashflow_continuity": ctx.get("cashflow_continuity"),
                "t1_ltgd_leader": ctx.get("t1_ltgd_leader"),
                "t1_ltgd_rank": ctx.get("t1_ltgd_rank"),
                "t1_ltgd_range_gain_pct": ctx.get("t1_ltgd_range_gain_pct"),
            }

            if in_buy is not None:
                verdict = "STILL_BUY"
            elif gate.get("leader_fade") or gate.get("hard_veto") or gate.get("high_board_position") or gate.get("prev_broken_limit_up"):
                verdict = "VETOED_OR_DOWNGRADED"
            else:
                verdict = "NOT_IN_BUY_BUT_NO_GATE_DATA"
        else:
            verdict = "TARGET_NOT_IN_POOL"

    summary["leader_fade_fired"] = bool(gate.get("leader_fade"))
    summary["verdict"] = verdict

    ok = (runner_rc == 0 and edge_self_test_rc == 0)
    summary["ok"] = ok
    summary["pass"] = bool(ok and verdict == "VETOED_OR_DOWNGRADED")

    print(SUMMARY_JSON_BEGIN)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(SUMMARY_JSON_END)

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
