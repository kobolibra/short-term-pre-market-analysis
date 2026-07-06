#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_diag_data_0158.py -- Task 0158 (read-only diagnostic).

查服务器本地磁盘上指定交易日的数据可用性:
  1. captures/<date>/ 每个 dataset 的文件数 (确认竞价四表到底有没有)
  2. reports/<date>/ 递归列出 (找 premarket 分析产物)
  3. 若有 *analysis_v9.json, 解析其 schema + 首个候选 edge_components.sub
只读, 不写业务数据。
用法: python3 scripts/duanxianxia_diag_data_0158.py [YYYY-MM-DD]
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import DEFAULT_PROJECT_ROOT

DATE = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
root = Path(DEFAULT_PROJECT_ROOT)

out = {"date": DATE, "root": str(root)}
out["root_listing"] = sorted(p.name for p in root.iterdir()) if root.is_dir() else []

cap = root / "captures" / DATE
capinfo = {}
if cap.is_dir():
    for d in sorted(cap.iterdir()):
        if d.is_dir():
            capinfo[d.name] = len(list(d.glob("*.json")))
out["captures_date_datasets"] = capinfo

rep = root / "reports" / DATE
rep_files = []
if rep.is_dir():
    for p in sorted(rep.rglob("*")):
        if p.is_file():
            rep_files.append({"path": str(p.relative_to(rep)), "size": p.stat().st_size})
out["reports_date_exists"] = rep.is_dir()
out["reports_date_files"] = rep_files

av9 = sorted(rep.rglob("*analysis_v9.json")) if rep.is_dir() else []
out["analysis_v9_files"] = [str(p.relative_to(root)) for p in av9]
sample = None
if av9:
    try:
        data = json.loads(av9[-1].read_text(encoding="utf-8"))
        sample = {"top_keys": list(data.keys()) if isinstance(data, dict) else "list"}
        cand = None
        if isinstance(data, dict):
            for k in ("candidates", "all_candidates", "results", "picks", "ranked", "rows"):
                if isinstance(data.get(k), list) and data.get(k):
                    cand = data[k]
                    sample["cand_key"] = k
                    break
        if cand:
            sample["n_cand"] = len(cand)
            c0 = cand[0]
            if isinstance(c0, dict):
                sample["cand0_keys"] = list(c0.keys())
                ec = c0.get("edge_components")
                if isinstance(ec, dict):
                    sample["cand0_edge_components_keys"] = list(ec.keys())
                    if isinstance(ec.get("sub"), dict):
                        sample["cand0_edge_sub"] = ec["sub"]
                sample["cand0_preview"] = {k: c0[k] for k in list(c0.keys())[:25]}
    except Exception as e:
        sample = {"error": repr(e)}
out["analysis_sample"] = sample

audit = root / "reports" / "_audit"
audit.mkdir(parents=True, exist_ok=True)
outp = audit / ("diag_data_" + DATE + "_0158.json")
outp.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

print(json.dumps({
    "date": DATE,
    "root_listing": out["root_listing"],
    "captures_date_datasets": out["captures_date_datasets"],
    "reports_date_exists": out["reports_date_exists"],
    "reports_date_files": out["reports_date_files"][:40],
    "analysis_v9_files": out["analysis_v9_files"],
    "analysis_sample": sample,
}, ensure_ascii=False))
