#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0104: run premarket pipeline NOW with REAL push (user-approved).
No hard 9:25 gate in batch.py. Uses duanxianxia_premarket_v7_runner.py
(v9 engine) end-to-end: capture -> selection -> bitable + webhook push.
Reads freshest premarket report and prints compact summary.
"""
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

WS = Path("/home/investmentofficehku/.openclaw/workspace")
RUNNER = WS / "scripts" / "duanxianxia_premarket_v7_runner.py"
REPORT_ROOT = WS / "projects" / "duanxianxia" / "reports"
TZ = ZoneInfo("Asia/Shanghai")


def latest_report(since_ts):
    date_str = datetime.now(TZ).strftime("%Y-%m-%d")
    d = REPORT_ROOT / date_str
    best, best_mt = None, -1.0
    if d.exists():
        for f in d.rglob("*.json"):
            try:
                mt = f.stat().st_mtime
            except OSError:
                continue
            if mt >= since_ts - 1 and mt > best_mt:
                best, best_mt = f, mt
    return best


def main():
    start_ts = datetime.now(TZ).timestamp()
    print("[0104] start %s" % datetime.now(TZ).isoformat())
    proc = subprocess.run(
        [sys.executable, str(RUNNER), "premarket"],
        cwd=str(WS), capture_output=True, text=True, timeout=540,
    )
    print("[0104] runner rc=%s" % proc.returncode)
    if proc.stderr.strip():
        print("[0104] stderr:\n" + proc.stderr[-1500:])
    rep_file = latest_report(start_ts)
    summary = {"job": "0104_premarket_backfill", "runner_rc": proc.returncode,
               "report_file": str(rep_file) if rep_file else ""}
    if rep_file:
        try:
            rep = json.loads(rep_file.read_text(encoding="utf-8"))
        except Exception as exc:
            rep = {}
            summary["report_read_error"] = str(exc)
        if rep:
            wh = rep.get("webhook", {}) or {}
            bt = rep.get("bitable_sync", {}) or {}
            an = rep.get("analysis", {}) or {}
            cands = an.get("top_candidates", []) or []
            summary.update({
                "success": rep.get("success"), "complete": rep.get("complete"),
                "failed_items": rep.get("failed_items", []),
                "missing_items": rep.get("missing_items", []),
                "webhook": {"enabled": wh.get("enabled"), "success": wh.get("success"),
                            "http_status": wh.get("http_status"),
                            "response_excerpt": str(wh.get("response_excerpt", ""))[:300]},
                "bitable_sync": {"enabled": bt.get("enabled"),
                                 "created_count": bt.get("created_count"),
                                 "reason": bt.get("reason", ""),
                                 "records": [{"code": r.get("code"), "name": r.get("name"),
                                              "status": r.get("status")}
                                             for r in (bt.get("records", []) or [])[:10]]},
                "analysis_version": an.get("version"),
                "candidate_count": len(cands),
                "top_candidates": [{"rank": c.get("rank"), "code": c.get("code"),
                                    "name": c.get("name"), "score": c.get("score"),
                                    "reasons": (c.get("reasons") or [])[:3],
                                    "risks": (c.get("risks") or [])[:2]}
                                   for c in cands[:10]],
            })
    print("[0104] SUMMARY_JSON_BEGIN")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("[0104] SUMMARY_JSON_END")
    return 0 if proc.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
