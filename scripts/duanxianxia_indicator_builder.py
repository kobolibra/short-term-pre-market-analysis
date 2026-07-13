#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_indicator_builder.py  --  v13 (D1-D6 rebuild).

Consumes the canonical T0 feature table from duanxianxia_feature_builder (v12,
which now overlays auction.jjlive.fengdan) and derives the six premarket
auction dimensions with the calibers finalised 2026-07-06 (see the dated
handoff doc under projects/duanxianxia/docs/).

Dimensions (per-stock scalars, except D6 which is market/section-level and is
resolved by an external layer -- this builder only passes concept through):

  D1  定价   pricing
      d1_auction_change_pct   竞价涨幅 %                    = changeRate
  D2  量能   volume / energy
      d2_bid_amount           竞价成交额 (元)             = bidAmount (raw6)
      d2_volume_ratio         量比                        = volumeRatio
      d2_turnover_rate        换手率                      = turnoverRate
      d2_grab_strength        抢筹强度 (与量比不同, 保留)   = grabStrength
  D3  资金质量 money quality
      d3_main_net_inflow      主力净额 (元)               = mainNetInflow
      d3_fund_ratio           资金占比                    = mainNetInflow / bidAmount
  D4  封板承接 sealing / absorption
      d4_true_seal            真封单 (元)                = sealAmountRaw(raw4) - bidAmount(raw8)
      d4_seal_ratio           承接                        = true_seal / FF
      d4_fengdan_925          fengdan 9:25 委买 (元)        = sealBid925  (cross-check)
  D5  分歧   divergence
      d5_fill_ratio           成交/委托                    = bidAmount(raw8) / sealAmountRaw(raw4)
      d5_time_divergence      时间分歧                    = (sealAmountRaw - sealBid920)/sealBid920
                                                            only when sealBid925 not in (None,0)
  D6  情绪环境 market / section environment (external; no per-stock scalar here)
      d6 concepts pass through the row's `concept` for the external layer.

DELETED vs v12: d1_auction_amount_pct, d3_super_large_order, d3_large_order,
d3_money, d3_money_pct, d4_seal_amount(raw17 动态封单弃用), d5_weimai_strength,
d5_orderbook.

Importing runs _self_test() on the real job-0089 sample rows (+ a synthetic
fengdan row); any caliber / coverage regression raises AssertionError and
blocks import.

Public API:
    VERSION, DIMENSIONS, INDICATOR_KEYS
    build_indicators(feature_table) -> dict
    build_indicators_from_datasets(datasets, **kw) -> dict
    build_indicators_from_capture(capture_dir, *, cutoff=...) -> dict
    _self_test()
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import duanxianxia_feature_builder as fb

VERSION = "indicator_builder_v14.0"

# Dimension -> (display name, ordered numeric indicator keys)
DIMENSIONS: Dict[str, Dict[str, Any]] = {
    "D1": {"name": "定价", "keys": ["d1_auction_change_pct"]},
    "D2": {"name": "量能", "keys": [
        "d2_bid_amount", "d2_volume_ratio",
        "d2_turnover_rate", "d2_grab_strength"]},
    "D3": {"name": "资金质量", "keys": ["d3_main_net_inflow", "d3_fund_ratio"]},
    "D4": {"name": "封板承接", "keys": [
        "d4_true_seal", "d4_seal_ratio", "d4_fengdan_925"]},
    "D5": {"name": "分歧", "keys": ["d5_fill_ratio", "d5_time_divergence"]},
    "D6": {"name": "情绪环境", "keys": []},  # market/section level, external
}

# Flat list of numeric indicator keys tracked for coverage (D6 has none here).
INDICATOR_KEYS: List[str] = [k for d in DIMENSIONS.values() for k in d["keys"]]

# Passthrough text/context columns (not counted in numeric coverage).
TEXT_KEYS = ("code", "name", "concept", "boardLabel", "free_float_mktcap")

_COVERAGE_WARN_RATE = 0.20


def _ratio(num: Any, den: Any) -> Optional[float]:
    if num is None or den in (None, 0):
        return None
    try:
        return num / den
    except (TypeError, ZeroDivisionError):
        return None


def _indicators_for(feat: Mapping[str, Any]) -> Dict[str, Any]:
    ff = feat.get("free_float_mktcap")
    bid = feat.get("bidAmount")            # 竞价成交额 (raw6/raw8, 元)
    main_net = feat.get("mainNetInflow")
    seal_raw = feat.get("sealAmountRaw")   # 未剔除成交的委买额 raw4 (元)
    f920 = feat.get("sealBid920")          # fengdan 9:20 委买 (元)
    f925 = feat.get("sealBid925")          # fengdan 9:25 委买 (元)
    change = feat.get("changeRate")        # 竞价涨幅 %

    true_seal = None
    if seal_raw is not None and bid is not None:
        true_seal = seal_raw - bid          # 真封单 = raw4 - raw8 = f925

    time_div = None
    if f925 not in (None, 0) and f920 not in (None, 0) and seal_raw is not None:
        time_div = (seal_raw - f920) / f920  # 9:20->最新 时间分歧

    return {
        # context / passthrough
        "code": feat.get("code"),
        "name": feat.get("name"),
        "concept": feat.get("concept"),        # D6 external layer consumes this
        "boardLabel": feat.get("boardLabel"),
        "free_float_mktcap": ff,
        # D1 定价
        "d1_auction_change_pct": change,
        # D2 量能
        "d2_bid_amount": bid,
        "d2_volume_ratio": feat.get("volumeRatio"),
        "d2_turnover_rate": feat.get("turnoverRate"),
        "d2_grab_strength": feat.get("grabStrength"),
        # D3 资金质量
        "d3_main_net_inflow": main_net,
        "d3_fund_ratio": _ratio(main_net, bid),
        # D4 封板承接
        "d4_true_seal": true_seal,
        "d4_seal_ratio": _ratio(true_seal, ff),
        "d4_fengdan_925": f925,
        # D5 分歧
        "d5_fill_ratio": _ratio(bid, seal_raw),
        "d5_time_divergence": time_div,
        # provenance
        "fengdan_hit": feat.get("fengdan_hit"),
        "source_hits": feat.get("source_hits"),
        "source_hit_count": feat.get("source_hit_count"),
        "_field_sources": feat.get("_field_sources"),
    }


def _coverage(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    cov: Dict[str, Dict[str, Any]] = {}
    n = len(rows) or 1
    for k in INDICATOR_KEYS:
        missing = sum(1 for r in rows if r.get(k) is None)
        rate = missing / n
        cov[k] = {
            "missing": missing,
            "present": len(rows) - missing,
            "missing_rate": round(rate, 4),
            "warn": rate > _COVERAGE_WARN_RATE,
        }
    return cov


def build_indicators(feature_table: Mapping[str, Any]) -> Dict[str, Any]:
    feats = feature_table.get("features") or []
    rows = [_indicators_for(f) for f in feats]
    rows.sort(key=lambda r: (-(r.get("source_hit_count") or 0), r.get("code") or ""))
    return {
        "version": VERSION,
        "feature_version": feature_table.get("version"),
        "date": feature_table.get("date"),
        "t0_cutoff": feature_table.get("t0_cutoff"),
        "dimensions": {d: {"name": v["name"], "keys": v["keys"]}
                       for d, v in DIMENSIONS.items()},
        "indicator_keys": INDICATOR_KEYS,
        "n_rows": len(rows),
        "n_fengdan": feature_table.get("n_fengdan"),
        "n_fengdan_merged": feature_table.get("n_fengdan_merged"),
        "coverage": _coverage(rows),
        "rows": rows,
    }


def build_indicators_from_datasets(datasets: Mapping[str, Sequence[Any]], **kw) -> Dict[str, Any]:
    return build_indicators(fb.build_from_datasets(datasets, **kw))


def build_indicators_from_capture(capture_dir: Any, *,
                                  cutoff: str = fb.T0_DEFAULT_CUTOFF) -> Dict[str, Any]:
    return build_indicators(fb.build_feature_table(capture_dir, cutoff=cutoff))


# --------------------------------------------------------------------------- #
# Self-test -- real job-0089 sample rows (shared with feature_builder) + fengdan
# --------------------------------------------------------------------------- #
def _self_test() -> bool:
    v = ["002407", "多氟多", 462, 32740, "none", "10.0",
         "1779", "氢氟酸", "10.0", "1779", "15", 6.1, 0.52]
    w = ["002407", "多氟多", 45.66, 10, 2339609266, "none",
         144416464, 0.56, 258717139, 1016893860, 258717139,
         "氢氟酸、电解液",
         46177984662, 144416464, 203217386, -58800922, "首板", 208089]
    n = ["002407", "多氟多", 10, 10, 14442, 25872, 461.8,
         "氢氟酸|电解液", 0.56]
    q = ["300279", "和晶科技", 22, None, "none", "1.01",
         "189", "机器人", "1.01", "189", None, "11.93", 0.09]
    fd = {"code": "002407", "name": "多氟多", "board_label": "首板",
          "amount_915": "1.5亿", "amount_920": "0.3亿", "amount_925": "0.2亿",
          "latest_change_pct": "10.00%", "tag_1": "氢氟酸"}
    datasets = {
        "auction.jjyd.vratio": [{"code": "002407", "raw": v}],
        "auction.jjyd.weimai": [{"code": "002407", "raw": w}],
        "auction.jjyd.net_amount": [{"code": "002407", "raw": n}],
        "auction.jjyd.qiangchou": [{"code": "300279", "raw": q}],
        "auction.jjlive.fengdan": [fd],
    }
    res = build_indicators_from_datasets(datasets, date="2026-06-29")
    assert set(res["coverage"]) == set(INDICATOR_KEYS), set(res["coverage"])
    rows = {r["code"]: r for r in res["rows"]}
    assert "002407" in rows and "300279" in rows, list(rows)
    a = rows["002407"]
    b = rows["300279"]

    # D1
    assert a["d1_auction_change_pct"] == 10.0, a["d1_auction_change_pct"]
    # D2
    assert a["d2_bid_amount"] == 17_790_000, a["d2_bid_amount"]
    assert a["d2_volume_ratio"] == 6.1, a["d2_volume_ratio"]
    assert a["d2_turnover_rate"] == 0.52, a["d2_turnover_rate"]
    assert a["d2_grab_strength"] is None, a["d2_grab_strength"]
    # D3
    assert a["d3_main_net_inflow"] == 144_420_000, a["d3_main_net_inflow"]
    assert abs(a["d3_fund_ratio"] - 144_420_000 / 17_790_000) < 1e-9
    # D4
    assert a["d4_true_seal"] == 2339609266 - 17_790_000, a["d4_true_seal"]
    assert abs(a["d4_seal_ratio"] - (2339609266 - 17_790_000) / 46177984662) < 1e-12
    assert a["d4_fengdan_925"] == 20_000_000, a["d4_fengdan_925"]
    # D5
    assert abs(a["d5_fill_ratio"] - 17_790_000 / 2339609266) < 1e-12
    assert abs(a["d5_time_divergence"] - (2339609266 - 30_000_000) / 30_000_000) < 1e-6

    # deleted keys must be gone
    for dead in ("d1_auction_amount_pct", "d3_super_large_order", "d3_large_order",
                 "d3_money", "d3_money_pct", "d4_seal_amount",
                 "d5_weimai_strength", "d5_orderbook"):
        assert dead not in a, dead

    # 300279: only qiangchou -> most sealing/money indicators are None
    assert b["d2_grab_strength"] == 11.93, b["d2_grab_strength"]
    assert b["d2_volume_ratio"] is None, b["d2_volume_ratio"]
    assert b["d1_auction_change_pct"] == 1.01, b["d1_auction_change_pct"]
    assert b["d3_main_net_inflow"] is None, b["d3_main_net_inflow"]
    assert b["d3_fund_ratio"] is None, b["d3_fund_ratio"]
    assert b["d4_true_seal"] is None, b["d4_true_seal"]
    assert b["d4_fengdan_925"] is None, b["d4_fengdan_925"]
    assert b["d5_time_divergence"] is None, b["d5_time_divergence"]
    assert b["fengdan_hit"] is False, b["fengdan_hit"]
    return True


_self_test()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Build D1-D6 premarket auction indicators from a captures/<date> dir")
    ap.add_argument("capture_dir", help="path to captures/<YYYY-MM-DD>")
    ap.add_argument("--cutoff", default=fb.T0_DEFAULT_CUTOFF)
    ap.add_argument("--out", help="write indicator JSON here (default: stdout)")
    args = ap.parse_args(argv)
    res = build_indicators_from_capture(args.capture_dir, cutoff=args.cutoff)
    payload = json.dumps(res, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
