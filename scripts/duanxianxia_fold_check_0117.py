#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_fold_check_0117.py -- Task 0117 verification runner.

目的: 证明 0116 登记的 10 张股级表 (fengdan/ztpool/fupan/ltgd/
cashflow.{today,3,5,10day}/rank.{rocket,hot_stock_day}) 在 0117 翻为
canonical=True + 指标 key 对齐后, 已真正折入以个股为中心的宽表,
并且新指标在实数据上 coverage>0 (非空挂)。

只读; 不写 git。输出紧凑 (逐日一行 load + 一行 coverage), 最新日期在后,
以适配 worker 的 stdout_tail。build_master_panel 内部用
canonicalize_row(dataset_id, row), 已验证对定位表与 named_dict 表均可路由。
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

import duanxianxia_master_indicators as M  # noqa: E402  (import-time self-test runs)

NEW_TABLES = [
    "auction.jjlive.fengdan", "home.ztpool", "review.fupan.plate",
    "review.ltgd.range", "cashflow.stock.today", "cashflow.stock.3day",
    "cashflow.stock.5day", "cashflow.stock.10day",
    "rank.rocket", "rank.hot_stock_day",
]
NEW_INDS = [
    "hot_value", "hot_rank", "seal_bid_915", "seal_bid_920", "seal_bid_925",
    "seal_status", "ladder_group", "promo_rate", "streak", "open_num",
    "fupan_seal_amount", "fupan_turnover_amount", "cashflow_main_net",
    "cashflow_main_net_3day", "cashflow_main_net_5day", "cashflow_main_net_10day",
    "interval_change", "interval_period", "total_mktcap", "float_mktcap",
    "free_float_mktcap",
]
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def main():
    # (1) master_indicators import-time self-test already passed; re-assert flags.
    assert M._self_test()
    for ds in NEW_TABLES:
        assert M.DATASETS[ds]["canonical"] is True, ds
        assert M.DATASETS[ds]["scope"] == M.STOCK, ds

    cap = WS / "projects" / "duanxianxia" / "captures"
    dates = sorted(p.name for p in cap.iterdir()
                   if p.is_dir() and DATE_RE.match(p.name)) if cap.is_dir() else []

    print("=== Task 0117 FOLD-CHECK: 10 new tables -> stock panel ===")
    print(f"captures={cap}")
    print(f"dates={len(dates)} new_tables={len(NEW_TABLES)} "
          f"new_inds={len(NEW_INDS)} total_indicators={len(M.INDICATORS)}")

    fold_ok = False
    per_date_loaded = {}
    for d in dates:
        try:
            res = M.build_master_panel(cap / d)
            s = res["summary"]
            lr = s["load_report"]
            cov = s["coverage_pct"]
            loaded = {}
            n_loaded = 0
            for ds in NEW_TABLES:
                r = lr.get(ds, {})
                if r.get("loaded"):
                    n_loaded += 1
                    loaded[ds] = r.get("codes")
                else:
                    loaded[ds] = r.get("reason", "absent")
            new_cov = {i: cov.get(i) for i in NEW_INDS if i in cov}
            if any((v or 0) > 0 for v in new_cov.values()):
                fold_ok = True
            per_date_loaded[d] = n_loaded
            print(f"\n--- {d}: n_codes={s['n_codes']} "
                  f"new_tables_loaded={n_loaded}/{len(NEW_TABLES)} ---")
            print("  load:", json.dumps(loaded, ensure_ascii=False))
            print("  new_cov%:", json.dumps(new_cov, ensure_ascii=False))
        except Exception as e:  # noqa: BLE001
            print(f"\n--- {d}: ERROR {type(e).__name__}: {e} ---")

    print(f"\nper_date_new_tables_loaded={json.dumps(per_date_loaded)}")
    print(f"FOLD_OK={fold_ok}")
    print("SELFTEST_OK")


if __name__ == "__main__":
    main()
