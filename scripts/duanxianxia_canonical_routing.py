#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_canonical_routing.py  --  Task 0091 entry point.

Route ALL fetcher parsing through the canonical layer. Consumers pass a fetcher
FetchResult `kind` plus its `rows` and get back canonical dicts (correct names,
caliber-tagged market caps, money in 元) via duanxianxia_canonical.

  * positional datasets  -> canonicalised from each row's stored raw[] array
  * pool.hot             -> canonicalised from its legacy named-string keys

Because callers read canonical names only, the fetcher's historical mislabels
(e.g. vratio/qiangchou raw[2] "auction_volume_ratio", which is really FF market
cap) no longer leak downstream -- the named field can stay until every consumer
is migrated, then be retired safely.

Importing this module runs _self_test() on the real 0089 sample rows; any
routing/unit regression raises AssertionError and blocks import.
"""
from __future__ import annotations

from duanxianxia_canonical import REGISTRY, raw_to_canonical

# fetcher FetchResult.kind  ->  canonical dataset_id
KIND_TO_DATASET = {
    "auction_vratio": "auction.jjyd.vratio",
    "auction_qiangchou": "auction.jjyd.qiangchou",
    "auction_net_amount": "auction.jjyd.net_amount",
    "auction_weimai": "auction.jjyd.weimai",
    "surge": "pool.surge",
    "hot": "pool.hot",
}


def dataset_id_for_kind(kind):
    try:
        return KIND_TO_DATASET[kind]
    except KeyError:
        raise KeyError(f"no canonical dataset mapped for fetcher kind {kind!r}")


def _row_source(spec, row):
    """Pick what canonical needs from a fetcher row.
    named_strings (pool.hot): the row dict itself (cn keys).
    positional: the row's stored raw[] (or the row if it already is a list)."""
    if spec["raw_kind"] == "named_strings":
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


def _self_test():
    # weimai: real 002407 sealed row; raw[17]=208089 (万), raw[12]=FF 元
    w = ["002407", "多氟多", 45.66, 10, 2339609266, "none", 144416464,
         0.56, 258717139, 1016893860, 258717139, "氢氟酸", 46177984662,
         144416464, 203217386, -58800922, "首板", 208089]
    cw = canonicalize_rows("auction_weimai", [{"code": "002407", "raw": w}])[0]
    assert cw["seal_amount"] == 208089 * 10000, cw["seal_amount"]
    assert cw["free_float_mktcap"] == 46177984662, cw["free_float_mktcap"]

    # vratio: legacy mislabel auction_volume_ratio IGNORED; FF from raw[2]=462亿
    v = ["002407", "多氟多", 462, 3120, 10.0, 8.0, 5000,
         "氢氟酸", 0, 0, 1234, 1.8, 0.56]
    cv = canonicalize_rows("auction_vratio", [{"auction_volume_ratio": 462, "raw": v}])[0]
    assert cv["free_float_mktcap"] == 46_200_000_000, cv["free_float_mktcap"]
    assert cv["volume_ratio"] == 1.8, cv["volume_ratio"]
    assert "auction_volume_ratio" not in cv

    # pool.hot: named-string row, no raw[] needed; '182亿' -> FF 1.82e10
    hrow = {"涨幅": "10.51%", "主力": "+9046万", "实际换手": "11.4%",
            "成交": "20.5亿", "流通": "182亿", "概念": "X", "板态": "首板"}
    ch = canonicalize_rows("hot", [hrow])[0]
    assert ch["free_float_mktcap"] == 18_200_000_000, ch["free_float_mktcap"]

    # missing raw[] on a positional dataset -> explicit error marker, no crash
    err = canonicalize_rows("surge", [{"code": "x"}])[0]
    assert err.get("_canonical_error"), err

    # unknown kind -> hard error (don't silently mis-route)
    try:
        dataset_id_for_kind("bogus_kind")
    except KeyError:
        pass
    else:
        raise AssertionError("dataset_id_for_kind accepted an unknown kind")
    return True


_self_test()


if __name__ == "__main__":
    print("duanxianxia_canonical_routing self-test: PASS")
    print("kinds:", ", ".join(KIND_TO_DATASET))
