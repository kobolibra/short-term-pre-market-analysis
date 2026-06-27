#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v43_lagged_context_ic.py

背景: 0052 证实 pool.*/cashflow.*/review.*/ztpool 都是盘中(~10:01)或傍晚(~17:20)抓取,
不能当日直接用(会泄漏). 但它们作为昨日已知信息, 可以滞后一日(T-1)无泄漏使用.
本作业: 对每个预测日 D, 取该数据集 < D 的最近一个抓取日快照(files[-1]), 用其每个数值字段
预测 D 的 excess=(close-open)/preclose*100, 计算日截面 Spearman IC/ICIR. 这是这批“只能滞后用”
数据的真实盘前(次日)预测力.
输出 reports/_audit/lagged_context_ic_v43.{json,md}
用法: python3 scripts/v43_lagged_context_ic.py
"""
from __future__ import annotations
import json
import sys
import traceback
from collections import defaultdict
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10

CODE_KEYS = ["code", "\u4ee3\u7801"]
LAGGED_DATASETS = [
    "pool.hot", "pool.surge",
    "cashflow.stock.today", "cashflow.stock.3day", "cashflow.stock.5day", "cashflow.stock.10day",
    "home.ztpool", "review.daily.top_metrics", "review.fupan.plate", "review.ltgd.range",
]


def pnum(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("%", "").replace("+", "")
    if s in ("", "-", "--", "None"):
        return None
    mult = 1.0
    if s.endswith("\u4ebf"):
        mult, s = 1e4, s[:-1]
    elif s.endswith("\u4e07"):
        mult, s = 1.0, s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return None


def _norm(v):
    s = str(v or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:].zfill(6) if len(s) >= 6 else s


def code_of(r):
    for k in CODE_KEYS:
        if r.get(k) not in (None, ""):
            return _norm(r.get(k))
    return ""


def latest_rows(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return []
    files = sorted(d.glob("*.json"))
    if not files:
        return []
    try:
        payload = json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []
    names = [d.name for d in date_dirs]
    by_name = {d.name: d for d in date_dirs}

    ds_dates = {}
    for ds in LAGGED_DATASETS:
        ds_dates[ds] = [n for n in names if (by_name[n] / ds).is_dir() and any((by_name[n] / ds).glob("*.json"))]

    ic_daily = defaultdict(list)
    present = defaultdict(int)
    numeric = defaultdict(int)

    for D in names:
        for ds in LAGGED_DATASETS:
            priors = [n for n in ds_dates[ds] if n < D]
            if not priors:
                continue
            prev = priors[-1]
            rows = latest_rows(by_name[prev], ds)
            if not rows:
                continue
            idx = {}
            for r in rows:
                c = code_of(r)
                if c:
                    idx.setdefault(c, r)
            if not idx:
                continue
            exmap = {}
            for c in idx:
                e = daily.excess(c, D)
                if e is not None:
                    exmap[c] = e
            if len(exmap) < 8:
                continue
            fields = set()
            for r in idx.values():
                fields |= set(r.keys())
            for k in fields:
                if k in CODE_KEYS:
                    continue
                xs, ys = [], []
                for c, r in idx.items():
                    if c not in exmap:
                        continue
                    raw = r.get(k)
                    if raw not in (None, "", "-"):
                        present[(ds, k)] += 1
                    v = pnum(raw)
                    if v is None:
                        continue
                    numeric[(ds, k)] += 1
                    xs.append(v)
                    ys.append(exmap[c])
                if len(xs) >= 8:
                    ic = v10.spearman(xs, ys)
                    if ic is not None:
                        ic_daily[(ds, k)].append(ic)

    out = []
    for (ds, k), lst in ic_daily.items():
        pres = present[(ds, k)]
        num = numeric[(ds, k)]
        if pres == 0 or num / max(pres, 1) < 0.6:
            continue
        m, icir, nd = v10.mean_icir(lst)
        if m is None or nd < 3:
            continue
        out.append({"dataset": ds, "field": k, "mean_ic": m, "icir": icir, "n_days": nd, "numeric_rows": num})
    out.sort(key=lambda x: abs(x["mean_ic"]), reverse=True)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "lagged_context_ic_v43",
        "datasets": LAGGED_DATASETS,
        "n_dates": len(names),
        "fields": out,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "lagged_context_ic_v43.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# \u6ede\u540e(T-1)\u4e0a\u4e0b\u6587\u6570\u636e IC v43", "",
         "- \u751f\u6210: " + report["generated_at"] + " \uff5c\u65e5\u671f\u6570: " + str(len(names)), "",
         "| \u6570\u636e\u96c6 | \u5b57\u6bb5 | mean_ic | icir | n_days | rows |", "|---|---|---|---|---|---|"]
    for r in out[:40]:
        L.append("| " + r["dataset"] + " | " + r["field"] + " | " + str(r["mean_ic"]) + " | " + str(r["icir"]) + " | " + str(r["n_days"]) + " | " + str(r["numeric_rows"]) + " |")
    (audit / "lagged_context_ic_v43.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"n_dates": len(names), "top20": out[:20]}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
