#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v20_env_probe.py — 重构第0步: 探测执行环境与数据存量(只读)。

回答两件事, 为“顶级选股模型”重构定地基:
  1) 执行环境能用什么 ML 库(numpy/pandas/scipy/sklearn/lightgbm/xgboost/torch...) + Python/CPU。
  2) 到底有多少可训练数据: 有 v9 快照的交易日数/日期范围/样本行数/日线覆盖。

输出: reports/_audit/premarket_env_probe.{json,md}
用法: python3 scripts/v20_env_probe.py
"""
from __future__ import annotations
import argparse
import importlib
import json
import os
import platform
import sys
import traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12

LIBS = ["numpy", "pandas", "scipy", "sklearn", "lightgbm", "xgboost",
        "statsmodels", "torch", "joblib", "sympy"]


def probe_libs():
    out = {}
    for name in LIBS:
        try:
            m = importlib.import_module(name)
            out[name] = getattr(m, "__version__", "installed(no __version__)")
        except Exception as e:
            out[name] = f"MISSING ({type(e).__name__})"
    return out


def data_inventory(root):
    rep = root / "reports"
    cap_days = []
    total_candidates = 0
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if not files:
            continue
        try:
            analysis = json.loads(files[-1].read_text(encoding="utf-8"))
            cands = analysis.get("all_candidates") or []
            total_candidates += len(cands) if isinstance(cands, list) else 0
        except Exception:
            pass
        cap_days.append(dd.name)
    dl = root / "dailyline" / "stocks"
    dl_n = len(list(dl.glob("*.csv"))) if dl.is_dir() else 0
    return cap_days, total_candidates, dl_n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)

    libs = probe_libs()
    cap_days, total_candidates, dl_n = data_inventory(root)

    # trainable sample (excess computable, day >=30 rows)
    try:
        days = v12.load_days_plus(root, v10.Daily(root))
        n_train_days = len(days)
        n_train_rows = sum(len(d["rows"]) for d in days)
        train_dates = [d["date"] for d in days]
    except Exception as e:
        n_train_days, n_train_rows, train_dates = -1, -1, [f"ERR {e}"]

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "python": sys.version.replace("\n", " "),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "libraries": libs,
        "data": {
            "capture_days": len(cap_days),
            "capture_date_range": [cap_days[0], cap_days[-1]] if cap_days else [],
            "capture_dates": cap_days,
            "total_candidate_rows_raw": total_candidates,
            "dailyline_csv_files": dl_n,
            "trainable_days": n_train_days,
            "trainable_rows": n_train_rows,
            "trainable_dates": train_dates,
        },
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_env_probe.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    avail = [k for k, v in libs.items() if not str(v).startswith("MISSING")]
    miss = [k for k, v in libs.items() if str(v).startswith("MISSING")]
    L = ["# v20 环境 + 数据存量探针 (重构地基)", "",
         f"- 生成: {report['generated_at']}",
         f"- Python: {report['python']}",
         f"- 平台: {report['platform']} ｜CPU: {report['cpu_count']}", "",
         "## 可用 ML 库", "",
         f"- 已装: {', '.join(avail) if avail else '无'}",
         f"- 缺失: {', '.join(miss) if miss else '无'}", "",
         "| 库 | 版本/状态 |", "|---|---|"]
    for k in LIBS:
        L.append(f"| {k} | {libs[k]} |")
    d = report["data"]
    L += ["", "## 数据存量", "",
          f"- 有 v9 快照的交易日: **{d['capture_days']}** ｜范围: {d['capture_date_range']}",
          f"- 原始候选行总数: {d['total_candidate_rows_raw']}",
          f"- 可训练交易日(含 excess, 日>=30行): **{d['trainable_days']}** ｜可训练样本行: **{d['trainable_rows']}**",
          f"- dailyline CSV 文件数: {d['dailyline_csv_files']}",
          f"- 可训练日期: {', '.join(d['trainable_dates'])}", "",
          "> 用途: 定模型形态(有无 lightgbm/sklearn 决定能否上学习排序) 与 量化数据瓶颈(样本太少则首要任务是累积/回填数据)。"]
    (audit / "premarket_env_probe.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"libraries": libs, "data": report["data"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
