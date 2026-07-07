#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_indicator_listing_0166.py -- verification listing for the D1-D6
rebuild (indicator_builder v13 + fengdan-merged feature_builder v12).

Replaces 0157 (which referenced deleted keys d3_super_large_order etc.).
Prints the new D1-D6 indicators for the real capture, sorted by D4 真封单
(d4_true_seal) descending, nulls last. A compact RECAP is printed LAST so it
survives stdout tail-truncation in the job runner.

Usage:
    python3 duanxianxia_indicator_listing_0166.py [YYYY-MM-DD] [--top N] [--cutoff HH:MM]
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duanxianxia_indicator_builder as ib


def _project_root() -> Path:
    env = os.environ.get("PROJECT_ROOT")
    if env and Path(env).is_dir():
        return Path(env)
    here = Path(__file__).resolve()
    for p in [here.parent, *here.parents]:
        if (p / "captures").is_dir():
            return p
    return here.parent


def _fmt(v, money: bool = False) -> str:
    if v is None:
        return "-"
    if money and isinstance(v, (int, float)):
        return f"{v / 1e8:.3f}亿"
    if isinstance(v, float):
        return f"{v:.4g}"
    return str(v)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("date", nargs="?", default=None)
    ap.add_argument("--top", type=int, default=40)
    ap.add_argument("--cutoff", default=ib.fb.T0_DEFAULT_CUTOFF)
    args = ap.parse_args(argv)

    import v10_optimize as _v10; root = Path(_v10.DEFAULT_PROJECT_ROOT)
    cap_root = root / "captures"
    date = args.date
    if date is None:
        dirs = sorted(p.name for p in cap_root.iterdir() if p.is_dir()) if cap_root.is_dir() else []
        if not dirs:
            print(f"[0166] no captures under {cap_root}", flush=True)
            return 2
        date = dirs[-1]
    cap_dir = cap_root / date
    print(f"[0166] project_root={root}", flush=True)
    print(f"[0166] capture_dir={cap_dir} exists={cap_dir.is_dir()}", flush=True)

    res = ib.build_indicators_from_capture(cap_dir, cutoff=args.cutoff)
    print(f"[0166] feature_version={res.get('feature_version')} "
          f"indicator_version={res.get('version')}", flush=True)
    print(f"[0166] date={res.get('date')} cutoff={res.get('t0_cutoff')} "
          f"n_rows={res.get('n_rows')} n_fengdan_merged={res.get('n_fengdan_merged')}",
          flush=True)
    print(f"[0166] indicator_keys={res.get('indicator_keys')}", flush=True)

    cov = res.get("coverage") or {}
    warn = [k for k, c in cov.items() if c.get("warn")]

    rows = res.get("rows") or []
    rows_sorted = sorted(
        rows,
        key=lambda r: (r.get("d4_true_seal") is None, -(r.get("d4_true_seal") or 0)),
    )
    top = rows_sorted[: args.top]

    header = ["code", "name", "board", "D1涨%", "D2竞价额", "D2量比", "D2抢筹",
              "D3主力", "D3资占", "D4真封单", "D4承接", "D4f925", "D5成/委", "D5时间分歧"]
    print("\t".join(header), flush=True)
    for r in top:
        line = [
            r.get("code") or "",
            (r.get("name") or "")[:6],
            r.get("boardLabel") or "-",
            _fmt(r.get("d1_auction_change_pct")),
            _fmt(r.get("d2_bid_amount"), money=True),
            _fmt(r.get("d2_volume_ratio")),
            _fmt(r.get("d2_grab_strength")),
            _fmt(r.get("d3_main_net_inflow"), money=True),
            _fmt(r.get("d3_fund_ratio")),
            _fmt(r.get("d4_true_seal"), money=True),
            _fmt(r.get("d4_seal_ratio")),
            _fmt(r.get("d4_fengdan_925"), money=True),
            _fmt(r.get("d5_fill_ratio")),
            _fmt(r.get("d5_time_divergence")),
        ]
        print("\t".join(line), flush=True)

    n_fengdan = sum(1 for r in rows if r.get("fengdan_hit"))
    # RECAP last (survives tail truncation)
    print(f"[0166] RECAP n_rows={res.get('n_rows')} "
          f"fengdan_hit={n_fengdan}/{len(rows)} "
          f"coverage_warn(>20%_missing)={warn}", flush=True)
    print("[0166] DONE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
