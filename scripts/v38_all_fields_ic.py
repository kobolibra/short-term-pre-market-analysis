#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v38_all_fields_ic.py — job 0049: 穷举所有盘前数据集的所有数值字段的横截面 IC.

回答“是不是所有字段都测了”: v36 只测了 16 个工程因子(来自 ~9 个字段/9 个数据集),
未覆盖 cashflow.{today,3day,5day,10day}、pool.hot、pool.surge 共 6 个数据集, 以及
大量原始字段(weimai 全量、auction_volume_ratio、seal_amount_wan、raw_rate 等).

本作业: 对每个个股级数据集, 自动识别每个数值字段, 逐字段算每日横截面 Spearman IC/
ICIR/覆盖率(对当日 excess). 不预设方向(报带符号 IC, 按 |IC| 排序). 纯只读.
板块级/大盘级数据集(kaipan、qxlive)只导出 schema, 不做个股横截面.

excess=(close-open)/preclose*100
输出 reports/_audit/premarket_all_fields_ic_v38.{json,md}
用法: python3 scripts/v38_all_fields_ic.py
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

STOCK_DATASETS = [
    "auction.jjyd.vratio", "auction.jjyd.qiangchou", "auction.jjyd.net_amount",
    "auction.jjlive.fengdan", "auction.jjyd.weimai",
    "rank.rocket", "rank.hot_stock_day",
    "cashflow.stock.today", "cashflow.stock.3day", "cashflow.stock.5day", "cashflow.stock.10day",
    "pool.hot", "pool.surge",
]
NON_STOCK = ["home.kaipan.plate.summary", "home.qxlive.top_metrics"]
CODE_KEYS = ["code", "\u4ee3\u7801"]


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


def code_of(r):
    for k in CODE_KEYS:
        if r.get(k) not in (None, ""):
            return _norm(r.get(k))
    return ""


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []

    field_daily = defaultdict(list)
    present = defaultdict(int)
    numeric = defaultdict(int)
    all_fields = defaultdict(set)
    n_days = 0

    for dd in date_dirs:
        ds_idx = {}
        for ds in STOCK_DATASETS:
            rows = latest_rows(dd, ds)
            idx = {}
            for r in rows:
                c = code_of(r)
                if not c:
                    continue
                idx.setdefault(c, r)
                for k, v in r.items():
                    all_fields[ds].add(k)
                    if v not in (None, "", "-"):
                        present[(ds, k)] += 1
                        if pnum(v) is not None:
                            numeric[(ds, k)] += 1
            ds_idx[ds] = idx
        codes = set()
        for ds in STOCK_DATASETS:
            codes |= set(ds_idx[ds].keys())
        exmap = {}
        for c in codes:
            e = daily.excess(c, dd.name)
            if e is not None:
                exmap[c] = e
        if len(exmap) < 8:
            continue
        n_days += 1
        for ds in STOCK_DATASETS:
            idx = ds_idx[ds]
            for k in all_fields[ds]:
                if k in CODE_KEYS:
                    continue
                xs, ys = [], []
                for c, r in idx.items():
                    if c not in exmap:
                        continue
                    v = pnum(r.get(k))
                    if v is None:
                        continue
                    xs.append(v)
                    ys.append(exmap[c])
                if len(xs) >= 8:
                    ic = v10.spearman(xs, ys)
                    if ic is not None:
                        field_daily[(ds, k)].append(ic)

    out = []
    for (ds, k), ics in field_daily.items():
        pres = present[(ds, k)]
        num = numeric[(ds, k)]
        if pres == 0 or num / pres < 0.6:
            continue
        m, icir, nd = v10.mean_icir(ics)
        if m is None or nd < 3:
            continue
        out.append({"dataset": ds, "field": k, "mean_ic": m, "icir": icir,
                    "n_days": nd, "numeric_rows": num})
    out.sort(key=lambda x: abs(x["mean_ic"]), reverse=True)

    schema = {ds: sorted(all_fields[ds]) for ds in STOCK_DATASETS}
    non_stock_schema = {}
    for dd in date_dirs[-1:]:
        for ds in NON_STOCK:
            rows = latest_rows(dd, ds)
            keys = set()
            for r in rows:
                keys |= set(r.keys())
            non_stock_schema[ds] = sorted(keys)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0049_all_fields_ic_v38",
        "n_days": n_days,
        "tested_datasets": STOCK_DATASETS,
        "schema_only_datasets": NON_STOCK,
        "n_fields_tested": len(out),
        "field_ic": out,
        "stock_dataset_fields": schema,
        "non_stock_dataset_fields": non_stock_schema,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_all_fields_ic_v38.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# 全字段穷举 IC v38 (job 0049)", "",
         f"- 生成: {report['generated_at']} ｜有效天: {n_days} ｜测试字段数: {len(out)}",
         f"- 个股级数据集(已逐字段测): {', '.join(STOCK_DATASETS)}",
         f"- 仅导出schema(板块/大盘级): {', '.join(NON_STOCK)}", "",
         "## 所有数值字段横截面 IC (按 |IC| 排序)", "",
         "| 数据集 | 字段 | mean_ic | icir | n_days |", "|---|---|---|---|---|"]
    for r in out:
        L.append(f"| {r['dataset']} | {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} |")
    (audit / "premarket_all_fields_ic_v38.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"n_days": n_days, "n_fields_tested": len(out), "top20": out[:20]},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
