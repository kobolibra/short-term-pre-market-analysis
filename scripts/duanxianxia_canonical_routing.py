#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_canonical_routing.py  --  Task 0091 entry point (Task 0116 extended).

Route ALL fetcher parsing through the canonical layer. Consumers pass a fetcher
FetchResult `kind` plus its `rows` and get back canonical dicts (correct names,
caliber-tagged market caps, money in \u5143) via duanxianxia_canonical.

  * positional datasets  -> canonicalised from each row's stored raw[] array
  * pool.hot             -> canonicalised from its legacy named-string keys
  * named_dict datasets  -> canonicalised from the row dict's own source keys
                            (Task 0116: rank.*, cashflow.*, fengdan, ztpool,
                             ltgd, fupan). These carry dataset_id directly in
                             their capture files, so consumers can also call
                             canonicalize_rows_by_id(dataset_id, rows).

Because callers read canonical names only, the fetcher's historical mislabels
(e.g. vratio/qiangchou raw[2] \"auction_volume_ratio\", which is really FF market
cap) no longer leak downstream.

Task 0126 C-fix: canonicalize_row now preserves a row-level 'group' tag
(grab/qiangchou for the qiangchou table) onto the canonical dict, so
master.build_master_panel can split the qiangchou table into the grp_grab /
grp_qiangchou virtual tables instead of overwriting one caliber with the other.

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
    when a positional row is missing its raw[] (never a silent drop).

    Task 0126 C-fix: if the source row dict carries a 'group' tag (the qiangchou
    table returns list.grab + list.qiangchou in one payload), preserve it on the
    canonical dict so downstream can separate the two calibers by group."""
    spec = REGISTRY[dataset_id]
    src = _row_source(spec, row)
    if src is None:
        return {"_canonical_error": "missing raw[]", "dataset_id": dataset_id}
    out = raw_to_canonical(dataset_id, src)
    if isinstance(out, dict) and isinstance(row, dict) and row.get("group") is not None:
        out.setdefault("group", row.get("group"))
    return out


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
    # --- positional: weimai real 002407 sealed row; raw[17]=208089(\u4e07), raw[12]=FF \u5143
    w = ["002407", "\u591a\u6c1f\u591a", 45.66, 10, 2339609266, "none", 144416464,
         0.56, 258717139, 1016893860, 258717139, "\u6c22\u6c1f\u9178", 46177984662,
         144416464, 203217386, -58800922, "\u9996\u677f", 208089]
    cw = canonicalize_rows("auction_weimai", [{"code": "002407", "raw": w}])[0]
    assert cw["seal_amount"] == 208089 * 10000, cw["seal_amount"]
    assert cw["free_float_mktcap"] == 46177984662, cw["free_float_mktcap"]

    # --- positional: vratio -- legacy mislabel ignored; FF from raw[2]=462\u4ebf, raw[11]=volume_ratio
    v = ["002407", "\u591a\u6c1f\u591a", 462, 3120, 10.0, 8.0, 5000,
         "\u6c22\u6c1f\u9178", None, None, 4800, 6.1, 0.52]
    cv = canonicalize_rows("auction_vratio", [{"raw": v}])[0]
    assert cv["free_float_mktcap"] == 46_200_000_000, cv["free_float_mktcap"]
    assert cv["volume_ratio"] == 6.1, cv["volume_ratio"]

    # --- positional row missing raw[] -> explicit error marker (never silent drop)
    err = canonicalize_rows("auction_vratio", [{"code": "x"}])[0]
    assert err.get("_canonical_error") == "missing raw[]", err

    # --- Task 0126 C-fix: qiangchou row 'group' tag preserved onto canonical dict
    #     (grab/qiangchou never dropped; rows without group get no spurious key)
    qg = canonicalize_row("auction.jjyd.qiangchou", {"group": "grab", "raw": v})
    qq = canonicalize_row("auction.jjyd.qiangchou", {"group": "qiangchou", "raw": v})
    assert qg.get("group") == "grab", qg
    assert qq.get("group") == "qiangchou", qq
    ng = canonicalize_row("auction.jjyd.qiangchou", {"raw": v})
    assert ng.get("group") is None, ng

    # --- named_dict via KIND_TO_DATASET (verified dataset_kind cashflow_today)
    cf = canonicalize_rows("cashflow_today", [
        {"\u4ee3\u7801": "603986", "\u540d\u79f0": "\u5146\u6613\u521b\u65b0",
         "\u6700\u65b0\u4ef7": "487.90", "\u6da8\u8dcc\u5e45": "4.47%",
         "\u4e3b\u529b\u51c0\u6d41\u5165": "7.76\u4ebf",
         "\u7279\u5927\u5355\u51c0\u6d41\u5165": "8.33\u4ebf",
         "\u5927\u5355\u51c0\u6d41\u5165": "-5720\u4e07",
         "\u4e2d\u5355\u51c0\u6d41\u5165": "-7.76\u4ebf",
         "\u5c0f\u5355\u51c0\u6d41\u5165": "-43.3\u4e07"}])[0]
    assert cf["code"] == "603986", cf
    assert cf["main_net"] == 776_000_000, cf["main_net"]

    # --- named_dict via canonicalize_rows_by_id (fupan GOLDEN mktcap anchor)
    fp = canonicalize_rows_by_id("review.fupan.plate", [
        {"\u4ee3\u7801": "605488", "\u540d\u79f0": "\u798f\u83b1\u65b0\u6750",
         "\u80a1\u4ef7": "34.32", "\u6da8\u5e45": "10.00%",
         "\u6da8\u505c\u7c7b\u578b": "\u56de\u5c01\u677f", "\u677f\u6570": "2\u59292\u677f",
         "\u8fde\u677f": "2", "\u5f00\u677f": "3", "\u5c01\u5355\u989d": "5697\u4e07",
         "\u6210\u4ea4\u989d": "7\u4ebf", "\u6362\u624b\u7387": "7.6%",
         "\u5b9e\u9645\u6d41\u901a": "42.4\u4ebf", "\u6d41\u901a\u5e02\u503c": "95\u4ebf",
         "\u603b\u5e02\u503c": "104\u4ebf", "\u9898\u6750\u540d\u79f0": "\u673a\u5668\u4eba",
         "\u9996\u6b21\u5c01\u677f": "10:09:26", "\u6700\u540e\u5c01\u677f": "13:26:39",
         "\u5f02\u52a8\u539f\u56e0": "x"}])[0]
    assert fp["code"] == "605488", fp
    assert fp["free_float_mktcap"] == 4_240_000_000, fp["free_float_mktcap"]
    assert fp["float_mktcap"] == 9_500_000_000, fp["float_mktcap"]
    assert fp["total_mktcap"] == 10_400_000_000, fp["total_mktcap"]

    # --- unknown fetcher kind must raise (never silently mis-route)
    try:
        dataset_id_for_kind("__no_such_kind__")
    except KeyError:
        pass
    else:
        raise AssertionError("dataset_id_for_kind should reject unknown kinds")

    # --- unknown dataset_id in by-id path must raise
    try:
        canonicalize_rows_by_id("__no_such_dataset__", [{}])
    except KeyError:
        pass
    else:
        raise AssertionError("canonicalize_rows_by_id should reject unknown dataset_id")

    return True


# Block import on any routing / unit regression.
_self_test()


if __name__ == "__main__":
    print("duanxianxia_canonical_routing self-test: PASS")
    print("kinds:", ", ".join(KIND_TO_DATASET))
