#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_indicator_builder.py -- dimension->indicator layer (D1-D6).

Consumes the merged canonical feature table from duanxianxia_feature_builder
and derives the D1-D6 dimension indicators defined in
docs/fixed-table-contract.md §5.

Money 元, market cap 元 (feature_builder already unifies units), so every
*_pct / strength below is a pure dimensionless ratio amount_元 / FF_元.

D1 竞价量能 : auction_amount_pct = bidAmount / FF ; volume_ratio
D2 流动性   : turnover_rate
D3 资金     : main_net_inflow, super_large_order, large_order, money(sum), money_pct
D4 承接封单 : true_seal = sealAmountRaw - bidAmount ; pressure_score = true_seal / FF ; seal_amount
D5 竞价强度 : auction_change_pct ; weimai_strength = sealAmountRaw / FF ; orderbook = (super+large)/FF
D6 环境情绪 : market-level (review_daily / plate_summary), joined externally -- not per-stock here.

§5 REDUNDANCIES to fix in the edge refactor (do NOT double-count):
  - d3_money overlaps the 资金 dimension inputs (main_net counted inside money and standalone).
  - d4_pressure_score and d5_weimai_strength both embed sealAmountRaw (委买) -> double count.
  - legacy edge references net_amount 3x inside auction_strength, inflating its weight.

Public API: VERSION, DIMENSIONS, INDICATOR_KEYS, build_indicators,
build_indicators_from_datasets, build_indicators_from_capture, _self_test.

CLI: python3 duanxianxia_indicator_builder.py captures/<YYYY-MM-DD> [--cutoff HH:MM] [--out path]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

import duanxianxia_feature_builder as fb

VERSION = "indicator_builder_v12.0"

DIMENSIONS: Dict[str, str] = {
    "D1": "竞价量能",
    "D2": "流动性",
    "D3": "资金",
    "D4": "承接/封单",
    "D5": "竞价强度",
    "D6": "环境/情绪",
}

# indicator columns whose coverage (missing-rate) we track + warn on.
INDICATOR_KEYS: Sequence[str] = (
    "d1_auction_amount_pct", "d1_volume_ratio",
    "d2_turnover_rate",
    "d3_main_net_inflow", "d3_super_large_order", "d3_large_order",
    "d3_money", "d3_money_pct",
    "d4_true_seal", "d4_pressure_score", "d4_seal_amount",
    "d5_auction_change_pct", "d5_weimai_strength", "d5_orderbook",
)

_MISSING_WARN_THRESHOLD = 0.20


def _ratio(num: Any, den: Any) -> Optional[float]:
    if num is None or den in (None, 0):
        return None
    try:
        return num / den
    except (TypeError, ZeroDivisionError):
        return None


def _sum_nonnull(*vals: Any) -> Optional[float]:
    present = [v for v in vals if v is not None]
    if not present:
        return None
    return sum(present)


def _indicators_for(feat: Mapping[str, Any]) -> Dict[str, Any]:
    ff = feat.get("free_float_mktcap")
    bid = feat.get("bidAmount")
    main_net = feat.get("mainNetInflow")
    super_large = feat.get("superLargeOrder")
    large = feat.get("largeOrder")
    seal_raw = feat.get("sealAmountRaw")

    money = _sum_nonnull(main_net, super_large, large)
    true_seal = None
    if seal_raw is not None and bid is not None:
        true_seal = seal_raw - bid
    orderbook_amt = _sum_nonnull(super_large, large)

    return {
        "code": feat.get("code"),
        "name": feat.get("name"),
        "concept": feat.get("concept"),
        "boardLabel": feat.get("boardLabel"),
        "free_float_mktcap": ff,
        # D1 竞价量能
        "d1_auction_amount_pct": _ratio(bid, ff),
        "d1_volume_ratio": feat.get("volumeRatio"),
        # D2 流动性
        "d2_turnover_rate": feat.get("turnoverRate"),
        # D3 资金
        "d3_main_net_inflow": main_net,
        "d3_super_large_order": super_large,
        "d3_large_order": large,
        "d3_money": money,
        "d3_money_pct": _ratio(money, ff),
        # D4 承接/封单
        "d4_true_seal": true_seal,
        "d4_pressure_score": _ratio(true_seal, ff),
        "d4_seal_amount": feat.get("sealAmount"),
        # D5 竞价强度
        "d5_auction_change_pct": feat.get("changeRate"),
        "d5_weimai_strength": _ratio(seal_raw, ff),
        "d5_orderbook": _ratio(orderbook_amt, ff),
        # provenance passthrough
        "source_hits": feat.get("source_hits"),
        "source_hit_count": feat.get("source_hit_count"),
        "_field_sources": feat.get("_field_sources"),
    }


def _coverage(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    total = len(rows) or 1
    cov: Dict[str, Dict[str, Any]] = {}
    for key in INDICATOR_KEYS:
        present = sum(1 for r in rows if r.get(key) is not None)
        missing = len(rows) - present
        cov[key] = {
            "present": present,
            "missing": missing,
            "missing_rate": round(missing / total, 4),
        }
    return cov


def build_indicators(feature_table: Mapping[str, Any]) -> Dict[str, Any]:
    feats = feature_table.get("features") or []
    rows = [_indicators_for(f) for f in feats]
    rows.sort(key=lambda r: (-(r.get("source_hit_count") or 0), r.get("code") or ""))
    coverage = _coverage(rows)
    warnings = [
        f"{k}: missing_rate={c['missing_rate']:.2%}"
        for k, c in coverage.items()
        if c["missing_rate"] > _MISSING_WARN_THRESHOLD
    ]
    return {
        "version": VERSION,
        "feature_version": feature_table.get("version"),
        "date": feature_table.get("date"),
        "t0_cutoff": feature_table.get("t0_cutoff"),
        "dimensions": DIMENSIONS,
        "n_rows": len(rows),
        "coverage": coverage,
        "warnings": warnings,
        "indicators": rows,
    }


def build_indicators_from_datasets(datasets, **kw) -> Dict[str, Any]:
    return build_indicators(fb.build_from_datasets(datasets, **kw))


def build_indicators_from_capture(capture_dir, *, cutoff: str = fb.T0_DEFAULT_CUTOFF) -> Dict[str, Any]:
    return build_indicators(fb.build_feature_table(capture_dir, cutoff=cutoff))


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
    datasets = {
        "auction.jjyd.vratio": [{"code": "002407", "raw": v}],
        "auction.jjyd.weimai": [{"code": "002407", "raw": w}],
        "auction.jjyd.net_amount": [{"code": "002407", "raw": n}],
        "auction.jjyd.qiangchou": [{"code": "300279", "raw": q}],
    }
    res = build_indicators_from_datasets(datasets, date="2026-06-29")
    rows = {r["code"]: r for r in res["indicators"]}
    assert "002407" in rows and "300279" in rows, list(rows)

    a = rows["002407"]
    ff = 46177984662
    assert a["free_float_mktcap"] == ff, a["free_float_mktcap"]
    assert abs(a["d1_auction_amount_pct"] - 17_790_000 / ff) < 1e-12, a["d1_auction_amount_pct"]
    assert a["d1_volume_ratio"] == 6.1, a["d1_volume_ratio"]
    assert a["d2_turnover_rate"] == 0.52, a["d2_turnover_rate"]
    assert a["d3_main_net_inflow"] == 144_420_000, a["d3_main_net_inflow"]
    assert a["d3_super_large_order"] == 203217386, a["d3_super_large_order"]
    assert a["d3_large_order"] == -58800922, a["d3_large_order"]
    assert a["d3_money"] == 144_420_000 + 203217386 - 58800922, a["d3_money"]
    assert abs(a["d3_money_pct"] - a["d3_money"] / ff) < 1e-12, a["d3_money_pct"]
    assert a["d4_true_seal"] == 2339609266 - 17_790_000, a["d4_true_seal"]
    assert abs(a["d4_pressure_score"] - (2339609266 - 17_790_000) / ff) < 1e-12, a["d4_pressure_score"]
    assert a["d4_seal_amount"] == 208089 * 10000, a["d4_seal_amount"]
    assert a["d5_auction_change_pct"] == 10.0, a["d5_auction_change_pct"]
    assert abs(a["d5_weimai_strength"] - 2339609266 / ff) < 1e-12, a["d5_weimai_strength"]
    assert abs(a["d5_orderbook"] - (203217386 - 58800922) / ff) < 1e-12, a["d5_orderbook"]

    b = rows["300279"]
    assert b["d2_turnover_rate"] == 0.09, b["d2_turnover_rate"]
    assert b["d1_volume_ratio"] is None, b["d1_volume_ratio"]

    assert set(res["coverage"]) == set(INDICATOR_KEYS), set(res["coverage"])
    return True


_self_test()


def _main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Build the D1-D6 indicator table from a captures/<date> dir")
    ap.add_argument("capture_dir", help="path to captures/<YYYY-MM-DD>")
    ap.add_argument("--cutoff", default=fb.T0_DEFAULT_CUTOFF,
                    help="T0 time-isolation cutoff HH:MM (default 09:29)")
    ap.add_argument("--out", help="write indicator table JSON here (default: stdout)")
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
