#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_premarket_capture_probe_0159.py -- Task 0159 (diagnostic).

在服务器上真实跑一次盘前 capture-only, 逐表报告 auction.jjyd.* 四张表
的成功/失败与错误信息, 用于定位“这几天竞价数据不下载”的真因。
中午跑的这份时间戳 > 09:29, 会被盘前截止过滤, 不污染盘前分析。
"""
from __future__ import annotations
import json
import os
import subprocess
from pathlib import Path
from datetime import datetime

WS = Path("/home/investmentofficehku/.openclaw/workspace")
AUDIT = WS / "projects" / "duanxianxia" / "reports" / "_audit"
AUDIT.mkdir(parents=True, exist_ok=True)

cmd = ["python3", "scripts/duanxianxia_batch.py", "premarket", "--capture-only", "--json", "--webhook-url", ""]
try:
    proc = subprocess.run(cmd, cwd=str(WS), text=True, capture_output=True, timeout=800, env=os.environ.copy())
    rc, out, err = proc.returncode, proc.stdout or "", proc.stderr or ""
except Exception as e:
    rc, out, err = -1, "", repr(e)

report = None
t = out.strip()
if t:
    fb = t.find("{")
    lb = t.rfind("}")
    if fb != -1 and lb != -1 and lb >= fb:
        try:
            report = json.loads(t[fb:lb + 1])
        except Exception as e:
            report = {"parse_error": repr(e)}

items_summary = []
if isinstance(report, dict):
    for it in report.get("items", []) or []:
        items_summary.append({
            "dataset": it.get("dataset_id") or it.get("dataset_label") or it.get("dataset"),
            "ok": it.get("ok") if "ok" in it else it.get("status"),
            "row_count": it.get("row_count") if "row_count" in it else it.get("rows"),
            "capture_path": it.get("capture_path"),
            "error": it.get("error") or it.get("message"),
        })

rec = {
    "job": "0159_premarket_capture_probe",
    "generated_at": datetime.now().isoformat(timespec="seconds"),
    "rc": rc,
    "report_path": report.get("report_path") if isinstance(report, dict) else None,
    "items_summary": items_summary,
    "stderr_tail": err[-6000:],
    "stdout_head": out[:1000],
}
outp = AUDIT / "premarket_capture_probe_0159.json"
outp.write_text(json.dumps({"rec": rec, "full_report": report}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

auction = [i for i in items_summary if str(i.get("dataset") or "").startswith("auction.")]
print(json.dumps({
    "rc": rc,
    "n_items": len(items_summary),
    "auction_items": auction,
    "all_datasets": [i.get("dataset") for i in items_summary],
    "stderr_tail": err[-1500:],
    "out": outp.name,
}, ensure_ascii=False))
