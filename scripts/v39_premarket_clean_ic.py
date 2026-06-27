#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v39_premarket_clean_ic.py — job 0050: 只用盘前(<=09:30)快照的全字段 IC, 剔除前视泄漏.

v38(0049) 暴露严重前视泄漏: cashflow.* 与 pool.* 的快照是开盘后(~10:01)甚至收盘后
(~20:49)抓取的, 其 涨跌幅/change_pct/资金流 字段≈当日已实现收益, 故 IC 高达 0.77-0.82
(ICIR>7)是假的. 同时 v38 用 files[-1](最后一个快照=盘后)读 auction 数据, latest_change_pct
也可能被收盘价污染.

本作业修正: 每个数据集只取 HHMMSS<=093000 的盘前快照(取盘前批次里最后一个);
没有盘前快照的数据集(cashflow/pool 多为开盘后抓取)记为 post-open 并排除. 然后对所有
数值字段重算每日横截面 Spearman IC/ICIR. 这才是可用于盘前选股的真实预测力.

excess=(close-open)/preclose*100
输出 reports/_audit/premarket_clean_ic_v39.{json,md}
用法: python3 scripts/v39_premarket_clean_ic.py
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

PREOPEN_CUTOFF = 93000  # HHMMSS

STOCK_DATASETS = [
    "auction.jjyd.vratio", "auction.jjyd.qiangchou", "auction.jjyd.net_amount",
    "auction.jjlive.fengdan", "auction.jjyd.weimai",
    "rank.rocket", "rank.hot_stock_day",
    "cashflow.stock.today", "cashflow.stock.3day", "cashflow.stock.5day", "cashflow.stock.10day",
    "pool.hot", "pool.surge",
]
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


def premarket_file(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return None, False
    has_any = False
    cand = []
    for f in sorted(d.glob("*.json")):
        has_any = True
        try:
            t = int(f.stem)
        except Exception:
            continue
        if t <= PREOPEN_CUTOFF:
            cand.append((t, f))
    if not cand:
        return None, has_any
    cand.sort()
    return cand[-1][1], has_any


def rows_of(f):
    try:
        payload = json.loads(f.read_text(encoding="utf-8"))
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
    ds_preopen_days = defaultdict(int)
    ds_anyfile_days = defaultdict(int)
    n_days = 0

    for dd in date_dirs:
        ds_idx = {}
        for ds in STOCK_DATASETS:
            f, has_any = premarket_file(dd, ds)
            if has_any:
                ds_anyfile_days[ds] += 1
            rows = rows_of(f) if f is not None else []
            if f is not None and rows:
                ds_preopen_days[ds] += 1
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

    post_open_excluded = [ds for ds in STOCK_DATASETS if ds_preopen_days[ds] == 0]
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

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0050_premarket_clean_ic_v39",
        "preopen_cutoff_hhmmss": PREOPEN_CUTOFF,
        "n_days": n_days,
        "dataset_preopen_days": {ds: ds_preopen_days[ds] for ds in STOCK_DATASETS},
        "dataset_anyfile_days": {ds: ds_anyfile_days[ds] for ds in STOCK_DATASETS},
        "post_open_excluded_datasets": post_open_excluded,
        "n_fields_tested": len(out),
        "field_ic": out,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_clean_ic_v39.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# 无泄漏盘前全字段 IC v39 (job 0050)", "",
         f"- 生成: {report['generated_at']} ｜盘前截止: {PREOPEN_CUTOFF} ｜有效天: {n_days} ｜测试字段数: {len(out)}",
         f"- 因无盘前快照被排除(开盘后抓取=前视泄漏): {', '.join(post_open_excluded) if post_open_excluded else '无'}", "",
         "## 盘前快照下所有数值字段横截面 IC (按 |IC| 排序)", "",
         "| 数据集 | 字段 | mean_ic | icir | n_days |", "|---|---|---|---|---|"]
    for r in out:
        L.append(f"| {r['dataset']} | {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} |")
    (audit / "premarket_clean_ic_v39.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"n_days": n_days, "n_fields_tested": len(out),
                      "post_open_excluded": post_open_excluded,
                      "dataset_preopen_days": report["dataset_preopen_days"],
                      "top20": out[:20]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
