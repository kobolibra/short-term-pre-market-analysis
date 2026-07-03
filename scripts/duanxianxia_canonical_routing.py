#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_canonical_routing.py  --  Task 0091 entry point (Task 0116 extended).

Route ALL fetcher parsing through the canonical layer. Consumers pass a fetcher
FetchResult `kind` plus its `rows` and get back canonical dicts (correct names,
caliber-tagged market caps, money in 元) via duanxianxia_canonical.

  * positional datasets  -> canonicalised from each row's stored raw[] array
  * pool.hot             -> canonicalised from its legacy named-string keys
  * named_dict datasets  -> canonicalised from the row dict's own source keys
                            (Task 0116: rank.*, cashflow.*, fengdan, ztpool,
                             ltgd, fupan). These carry dataset_id directly in
                             their capture files, so consumers can also call
                             canonicalize_rows_by_id(dataset_id, rows).

Because callers read canonical names only, the fetcher's historical mislabels
(e.g. vratio/qiangchou raw[2] "auction_volume_ratio", which is really FF market
cap) no longer leak downstream.

Importing this module runs _self_test() on real sample rows; any routing/unit
regression raises AssertionError and blocks import.
"""
from __future__ import annotations

from duanxianxia_canonical import REGISTRY, raw_to_canonical, NAMED_KINDS

# fetcher FetchResult.kind  ->  canonical dataset_id
# named_dict tables are keyed by dataset_id in their captures; only the fetcher
# kinds whose dataset_kind is verified are mapped here. For everything else use
# canonicalize_rows_by_id(dataset_id, rows).
KIND_TO_DATASET = {
    "auction_vratio": "auction.jjyd.vratio",
    "auction_qiangchou": "auction.jjyd.qiangchou",
    "auction_net_amount": "auction.jjyd.net_amount",
    "auction_weimai": "auction.jjyd.weimai",
    "surge": "pool.surge",
    "hot": "pool.hot",
    # Task 0116 verified dataset_kind -> dataset_id
    "cashflow_today": "cashflow.stock.today",
    "auction_fengdan": "auction.jjlive.fengdan",
}


def dataset_id_for_kind(kind):
    try:
        return KIND_TO_DATASET[kind]
    except KeyError:
        raise KeyError(f"no canonical dataset mapped for fetcher kind {kind!r}")


def _row_source(spec, row):
    """Pick what canonical needs from a fetcher row.
    named_* (pool.hot / named_dict): the row dict itself (source keys).
    positional: the row's stored raw[] (or the row if it already is a list)."""
    if spec["raw_kind"] in NAMED_KINDS:
        return row if isinstance(row, dict) else None
    if isinstance(row, (list, tuple)):
        return row
    if isinstance(row, dict):
        return row.get("raw")
    return None


def canonicalize_row(dataset_id, row):
    """One fetcher row -> canonical dict, or an explicit _canonical_error marker
    when a positional row is missing its raw[] (never a silent drop)."""
    spec = REGISTRY[dataset_id]
    src = _row_source(spec, row)
    if src is None:
        return {"_canonical_error": "missing raw[]", "dataset_id": dataset_id}
    return raw_to_canonical(dataset_id, src)


def canonicalize_rows(kind, rows):
    """Fetcher (kind, rows) -> list of canonical dicts. Rows lacking raw[] yield a
    _canonical_error marker so callers can audit coverage."""
    dataset_id = dataset_id_for_kind(kind)
    return [canonicalize_row(dataset_id, row) for row in (rows or [])]


def canonicalize_rows_by_id(dataset_id, rows):
    """Capture (dataset_id, rows) -> list of canonical dicts. Preferred path for
    named_dict tables whose capture files carry dataset_id directly."""
    if dataset_id not in REGISTRY:
        raise KeyError(f"unknown dataset_id: {dataset_id!r}")
    return [canonicalize_row(dataset_id, row) for row in (rows or [])]


def _self_test():
    # weimai: real 002407 sealed row; raw[17]=208089 (万), raw[12]=FF 元
    w = ["002407", "\u591a\u6c1f\u591a", 45.66, 10, 2339609266, "none", 144416464,
         0.56, 258717139, 1016893860, 258717139, "\u6c22\u6c1f\u9178", 46177984662,
         144416464, 203217386, -58800922, "\u9996\u677f", 208089]
    cw = canonicalize_rows("auction_weimai", [{"code": "002407", "raw": w}])[0]
    assert cw["seal_amount"] == 208089 * 10000, cw["seal_amount"]
    assert cw["free_float_mktcap"] == 46177984662, cw["free_float_mktcap"]

    # vratio: legacy mislabel auction_volume_ratio IGNORED; FF from raw[2]=462亿
    v = ["002407", "\u591a\u6c1f\u591a", 462, 3120, 10.0, 8.0, 5000,
         "\u6c22\u6c1f