#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0116 validation runner.

Imports the canonical + routing layers (their import-time _self_test() blocks on
any regression) and prints the registered-dataset summary so the job result
captures proof that all 10 stock-scope tables are registered and canonicalising
correctly.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duanxianxia_canonical as C          # noqa: E402  (import-time self-test)
import duanxianxia_canonical_routing as R   # noqa: E402  (import-time self-test)

print("=== Task 0116 canonical registration ===")
print("canonical datasets:", len(C.REGISTRY))
for k in sorted(C.REGISTRY):
    spec = C.REGISTRY[k]
    print(f"  - {k:26s} kind={spec['raw_kind']:14s} fields={len(spec['fields'])}")

print("routing KIND_TO_DATASET:", len(R.KIND_TO_DATASET))
for k in sorted(R.KIND_TO_DATASET):
    print(f"  - {k:20s} -> {R.KIND_TO_DATASET[k]}")

# coverage assertion: the 10 stock-scope tables must all be registered
_expected = [
    "rank.rocket", "rank.hot_stock_day",
    "cashflow.stock.today", "cashflow.stock.3day",
    "cashflow.stock.5day", "cashflow.stock.10day",
    "auction.jjlive.fengdan", "home.ztpool",
    "review.ltgd.range", "review.fupan.plate",
]
_missing = [d for d in _expected if d not in C.REGISTRY]
assert not _missing, f"missing registrations: {_missing}"
print("stock-scope tables registered:", len(_expected), "-> OK")
print("SELFTEST_OK")
