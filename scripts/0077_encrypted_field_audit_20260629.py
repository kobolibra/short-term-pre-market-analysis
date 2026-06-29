#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0077_encrypted_field_audit_20260629.py

Read-only audit: dump the newest capture for each encrypted auction dataset so
the market-cap caliber (流通 / 自由流通 / 总市值) and other field labels can be
verified against external truth. Globs the latest capture per dataset_id and
prints row0 (incl. positional `raw`) plus a compact (code/name/market_cap) scan.
"""
from __future__ import annotations
import json
from pathlib import Path

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
CAPTURE_ROOT = WORKSPACE / "projects" / "duanxianxia" / "captures"

DATASETS = [
    "auction.jjyd.net_amount",
    "auction.jjyd.weimai",
    "auction.jjlive.fengdan",
]

SCAN_FIELDS = (
    "market_cap_yi",
    "market_cap",
    "price",
    "turnover_rate_pct",
    "latest_change_pct",
    "auction_change_pct",
    "main_net_inflow_wan",
)


def latest_capture(dataset_id: str):
    files = sorted(CAPTURE_ROOT.glob(f"*/{dataset_id}/*.json"))
    return files[-1] if files else None


def main() -> int:
    out = {}
    for ds in DATASETS:
        f = latest_capture(ds)
        if not f:
            out[ds] = {"error": "no capture found"}
            continue
        data = json.loads(f.read_text(encoding="utf-8"))
        rows = data.get("rows", []) or []
        scan = []
        for r in rows[:40]:
            entry = {"code": r.get("code"), "name": r.get("name")}
            for k in SCAN_FIELDS:
                if k in r:
                    entry[k] = r.get(k)
            scan.append(entry)
        out[ds] = {
            "capture_path": str(f),
            "fetched_at": data.get("fetched_at"),
            "source_url": data.get("source_url"),
            "row_count": len(rows),
            "headers": data.get("headers", []),
            "row0": rows[0] if rows else {},
            "scan": scan,
        }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
