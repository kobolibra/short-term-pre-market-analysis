#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_pit_panel.py -- Task 0114 (additive, read-only).

修正 0113 的硬伤: 19 张表不是同一批次/同一时点, 不能直接按 code 并成一行。
本模块引入 point-in-time (as-of) 决策上下文, 保证无未来函数泄漏:
  - 每个面板绑定一个 as_of 时点 (默认盘前 09:29)。
  - 今日 live 表(竞价族/盘前qxlive等): 只取 as_of 前的今日快照。
  - 盘中/盘后/EOD 表(pool.*/ztpool/fupan/cashflow/ltgd/daily): 今日盘前尚不存在,
    只能取 T-1 最终态, 打 __lag=t1_eod 标签; 绝不当作今日同时点字段。
  - 同一 dataset 一天多快照: 按文件名 HHMMSS 选对那一张 (盘前取≤cutoff的最新;
    T-1 取当日最后一张)。绝不取 files[-1] 冒充。
  - 每个值都带 __src(来源表) / __batch(来源日期+HHMMSS) / __lag(today_live|t1_eod)。

字段<->表索引、别名归一、危险重名隔离继承自 duanxianxia_master_indicators。
真源 = fixed-table-contract.md 的时段分组 (盘前/盘中/盘后)。只读; 不写 git。
用法: python3 scripts/duanxianxia_pit_panel.py [captures_dir] [--asof premarket] [--cutoff 09:29]
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WS = HERE.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from duanxianxia_master_indicators import (  # noqa: E402
    DATASETS, INDICATORS, tables_for, STOCK,
    _norm_code, _rows_of, _row_code, _first_key,
)

# ---------------------------------------------------------------------------
# AVAILABILITY -- 每张表“数据何时可得” (slot) 与 horizon
#   slot: 数据最早可用的时段; horizon: live / eod / eod_window
#   依据 fixed-table-contract.md 的盘前/盘中/盘后分组
# ---------------------------------------------------------------------------
SLOT_ORDER = {"premarket": 1, "intraday": 2, "postmarket": 3}
AVAILABILITY = {
    "auction.jjyd.vratio":       ("premarket", "live"),
    "auction.jjyd.qiangchou":    ("premarket", "live"),
    "auction.jjyd.net_amount":   ("premarket", "live"),
    "auction.jjyd.weimai":       ("premarket", "live"),
    "auction.jjlive.fengdan":    ("premarket", "live"),
    "home.kaipan.plate.summary": ("premarket", "live"),
    "home.qxlive.top_metrics":   ("premarket", "live"),
    "pool.hot":                  ("intraday", "live"),
    "pool.surge":                ("intraday", "live"),
    "home.ztpool":               ("intraday", "eod"),
    "rank.rocket":               ("premarket", "live"),
    "rank.hot_stock_day":        ("premarket", "live"),
    "cashflow.stock.today":      ("postmarket", "eod"),
    "cashflow.stock.3day":       ("postmarket", "eod_window"),
    "cashflow.stock.5day":       ("postmarket", "eod_window"),
    "cashflow.stock.10day":      ("postmarket", "eod_window"),
    "review.fupan.plate":        ("postmarket", "eod"),
    "review.ltgd.range":         ("postmarket", "eod_window"),
    "review.daily.top_metrics":  ("postmarket", "eod"),
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _self_test():
    # 每张 dataset 都要有 availability, 且 slot 合法
    for ds in DATASETS:
        assert ds in AVAILABILITY, f"missing availability: {ds}"
        slot, hz = AVAILABILITY[ds]
        assert slot in SLOT_ORDER and hz in ("live", "eod", "eod_window")
    # 盘前决策时, 竞价族必须是 today_live, 盘中池必须不是
    assert _use_today("auction.jjyd.vratio", "premarket")
    assert not _use_today("pool.hot", "premarket")
    assert not _use_today("review.fupan.plate", "premarket")
    assert _use_today("pool.hot", "intraday")
    return True


def _use_today(ds, as_of_slot):
    slot, _hz = AVAILABILITY[ds]
    return SLOT_ORDER[slot] <= SLOT_ORDER[as_of_slot]


def _cutoff_secs(cutoff):
    try:
        h, m = cutoff.split(":")
        return int(h) * 3600 + int(m) * 60
    except Exception:  # noqa: BLE001
        return 9 * 3600 + 29 * 60


def _snap_secs(p):
    m = re.match(r"(\d{2})(\d{2})(\d{2})", p.stem)
    if not m:
        return None
    h, mi, s = (int(x) for x in m.groups())
    return h * 3600 + mi * 60 + s


def _pick_snapshot(dd, max_secs):
    """盘前: max_secs=cutoff 取 ≤cutoff 的最新; T-1: max_secs=None 取当日最后一张。"""
    if not dd.is_dir():
        return None, None
    cands = []
    for p in dd.glob("*.json"):
        secs = _snap_secs(p)
        cands.append((secs if secs is not None else -1, p))
    if not cands:
        return None, None
    if max_secs is not None:
        elig = [(s, p) for s, p in cands if s is not None and 0 <= s <= max_secs]
        if not elig:
            return None, None
        elig.sort()
        return elig[-1][1], elig[-1][0]
    cands.sort()
    return cands[-1][1], (cands[-1][0] if cands[-1][0] >= 0 else None)


def _prior_trading_day(root, date):
    dates = sorted(p.name for p in root.iterdir()
                   if p.is_dir() and DATE_RE.match(p.name) and p.name < date)
    return dates[-1] if dates else None


def _load_layer():
    imp = {}
    try:
        from duanxianxia_canonical import REGISTRY
        reg = set(REGISTRY.keys())
    except Exception as e:  # noqa: BLE001
        reg, imp["REGISTRY_err"] = set(), str(e)
    try:
        from duanxianxia_canonical_routing import canonicalize_row
    except Exception as e:  # noqa: BLE001
        canonicalize_row, imp["routing_err"] = None, str(e)
    return reg, canonicalize_row, imp


def _load_map(dd, ds, chosen_file, canonicalize_row):
    payload = json.loads(chosen_file.read_text(encoding="utf-8"))
    by_code, errs = {}, 0
    for row in _rows_of(payload):
        c = canonicalize_row(ds, row)
        if not isinstance(c, dict) or c.get("_canonical_error"):
            errs += 1
            continue
        code = _row_code(row) or _norm_code(c.get("code"))
        if code:
            by_code[code] = c
    return by_code, errs


def build_pit_panel(captures_root, date, as_of_slot="premarket", cutoff="09:29"):
    root = Path(captures_root)
    reg, canonicalize_row, imp = _load_layer()
    cutoff_secs = _cutoff_secs(cutoff)
    prior = _prior_trading_day(root, date)

    loaded = {}       # ds -> {by_code, batch, lag, secs}
    plan = {}         # ds -> 诊断(为什么用/不用, 取自哪天)
    for ds, meta in DATASETS.items():
        if meta["scope"] != STOCK:
            plan[ds] = {"used": False, "reason": "context_table(non-stock)"}
            continue
        use_today = _use_today(ds, as_of_slot)
        if use_today:
            src_date, max_secs, lag = date, (cutoff_secs if as_of_slot == "premarket" else None), "today_live"
        else:
            if not prior:
                plan[ds] = {"used": False, "reason": "needs_t1_but_no_prior_day"}
                continue
            src_date, max_secs, lag = prior, None, "t1_eod"
        if not (meta["canonical"] and ds in reg and canonicalize_row):
            plan[ds] = {"used": False, "lag": lag, "src_date": src_date,
                        "reason": "unmapped_canonical" if not meta["canonical"] else "not_in_registry"}
            continue
        dd = root / src_date / ds
        chosen, secs = _pick_snapshot(dd, max_secs)
        if chosen is None:
            plan[ds] = {"used": False, "lag": lag, "src_date": src_date, "reason": "no_snapshot_at_asof"}
            continue
        by_code, errs = _load_map(dd, ds, chosen, canonicalize_row)
        loaded[ds] = {"by_code": by_code, "batch": f"{src_date} {chosen.stem}", "lag": lag, "secs": secs}
        plan[ds] = {"used": True, "lag": lag, "src_date": src_date,
                    "batch": f"{src_date} {chosen.stem}", "codes": len(by_code), "canonical_err": errs}

    # 候选宇宙 = 仅今日 live 股级表的代码并集 (盘前决策不能用 T-1 的股集)
    universe = set()
    for ds, info in loaded.items():
        if info["lag"] == "today_live":
            universe.update(info["by_code"].keys())
    universe = sorted(universe)

    runnable = [ind for ind in INDICATORS if any(ds in loaded for ds in tables_for(ind))]
    panel = {}
    cov = {ind: 0 for ind in runnable}
    prov = {ind: {} for ind in runnable}
    lagdist = {ind: {} for ind in runnable}
    for code in universe:
        row = {"code": code}
        name = None
        for ind in runnable:
            spec = INDICATORS[ind]
            val, src, batch, lag = None, None, None, None
            for ds, _prov in spec["sources"]:
                info = loaded.get(ds)
                if not info:
                    continue
                cd = info["by_code"].get(code)
                if cd is None:
                    continue
                if name is None and cd.get("name"):
                    name = cd.get("name")
                v = _first_key(cd, spec["keys"])
                if v is not None:
                    val, src, batch, lag = v, ds, info["batch"], info["lag"]
                    break
            row[ind] = val
            row[ind + "__src"] = src
            row[ind + "__batch"] = batch
            row[ind + "__lag"] = lag
            if val is not None:
                cov[ind] += 1
                prov[ind][src] = prov[ind].get(src, 0) + 1
                lagdist[ind][lag] = lagdist[ind].get(lag, 0) + 1
        row["name"] = name
        panel[code] = row

    n = len(universe)
    summary = {
        "date": date, "as_of_slot": as_of_slot, "cutoff": cutoff,
        "prior_trading_day": prior, "imports": imp,
        "universe_size": n,
        "today_live_tables": [d for d, i in loaded.items() if i["lag"] == "today_live"],
        "t1_eod_tables": [d for d, i in loaded.items() if i["lag"] == "t1_eod"],
        "plan": plan,
        "coverage_pct": {i: (round(100 * cov[i] / n, 1) if n else 0.0) for i in runnable},
        "provenance": prov,
        "lag_distribution": lagdist,
    }
    return {"summary": summary, "panel": panel}


_self_test()


if __name__ == "__main__":
    args = sys.argv[1:]
    as_of, cutoff, cap = "premarket", "09:29", None
    i = 0
    while i < len(args):
        if args[i] == "--asof" and i + 1 < len(args):
            as_of = args[i + 1]; i += 2; continue
        if args[i] == "--cutoff" and i + 1 < len(args):
            cutoff = args[i + 1]; i += 2; continue
        cap = args[i]; i += 1
    root = Path(cap) if cap else (WS / "projects" / "duanxianxia" / "captures")
    if not root.is_absolute():
        root = WS / root
    dates = sorted(p.name for p in root.iterdir()
                   if p.is_dir() and DATE_RE.match(p.name)) if root.is_dir() else []

    out = {"task": "0114_pit_panel", "captures_root": str(root),
           "as_of_slot": as_of, "cutoff": cutoff, "per_date": {}}
    for d in dates:
        try:
            res = build_pit_panel(root, d, as_of_slot=as_of, cutoff=cutoff)
            s = res["summary"]
            s["sample_rows"] = list(res["panel"].values())[:2]
            out["per_date"][d] = s
        except Exception as e:  # noqa: BLE001
            out["per_date"][d] = {"error": f"{type(e).__name__}: {e}"}
    print("=== DUANXIANXIA POINT-IN-TIME PANEL (Task 0114) ===")
    print(f"as_of={as_of} cutoff={cutoff} dates={len(dates)}")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
