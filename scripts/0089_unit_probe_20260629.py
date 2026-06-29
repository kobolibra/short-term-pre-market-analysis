#!/usr/bin/env python3
"""
0089 unit probe (READ-ONLY): empirically pin the TRUE UNIT of ambiguous raw
fields before the canonical rename, by printing raw[] alongside current derived
fields for sample rows of the latest real captures. Touches NOTHING; prints to
stdout only (no file writes, no git, no push).

Why: rename-map says weimai item[17] is 万 but fetcher formats it as 元 (self-
contradictory); vratio item[2] is claimed FF-mktcap in 亿; net_amount item[6]
stored raw as market_cap_yi. Wrong unit assumptions -> silent double-conversion
after rename. Magnitude of real values resolves the unit unambiguously.
"""
import json, glob
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
CAP = WS / "projects" / "duanxianxia" / "captures"

DATASETS = {
    "weimai": "auction.jjyd.weimai",
    "vratio": "auction.jjyd.vratio",
    "qiangchou": "auction.jjyd.qiangchou",
    "net_amount": "auction.jjyd.net_amount",
    "surge": "pool.surge",
    "hot": "pool.hot",
}

SPOTLIGHT = {
    "weimai": {
        "raw_idx": [2, 4, 6, 8, 9, 12, 16, 17],
        "derived": ["auction_turnover", "main_net_inflow", "seal_volume", "auction_amount", "market_cap", "market_cap_yi", "seal_amount_wan", "seal_amount_text", "board_label"],
    },
    "vratio": {
        "raw_idx": [2, 3, 4, 5, 6, 11, 12],
        "derived": ["auction_volume_ratio", "seal_amount_wan", "auction_change_pct", "latest_change_pct", "auction_turnover_wan", "volume_ratio_multiple", "turnover_rate_pct"],
    },
    "qiangchou": {
        "raw_idx": [2, 3, 6, 11, 12],
        "derived": ["auction_volume_ratio", "seal_amount_wan", "auction_turnover_wan", "grab_strength", "turnover_rate_pct"],
    },
    "net_amount": {
        "raw_idx": [2, 3, 4, 5, 6, 8],
        "derived": ["auction_change_pct", "latest_change_pct", "main_net_inflow_wan", "auction_turnover_wan", "market_cap_yi", "turnover_rate_pct"],
    },
    "surge": {
        "raw_idx": [2, 7, 8, 9, 10],
        "derived": ["change_pct", "board_state", "amount", "float_market_cap", "turnover_ratio"],
    },
    "hot": {
        "raw_idx": [2, 6, 7, 8, 9, 10, 11],
        "derived": ["\u6da8\u5e45", "\u4e3b\u529b", "\u5b9e\u9645\u6362\u624b", "\u6210\u4ea4", "\u6d41\u901a", "\u6982\u5ff5", "\u677f\u6001"],
    },
}


def latest_capture(dsid):
    files = sorted(glob.glob(str(CAP / "*" / dsid / "*.json")))
    return files[-1] if files else None


def num(v):
    try:
        return float(v)
    except Exception:
        return None


def mag_hint(v):
    n = num(v)
    if n is None:
        return "non-numeric"
    a = abs(n)
    if a == 0:
        return "zero"
    if a >= 1e8:
        return ">=1e8 (looks like base-yuan, 亿-scale value)"
    if a >= 1e4:
        return ">=1e4 (base-yuan at 万-scale, OR a 万-count)"
    if a >= 100:
        return "100..1e4 (could be 亿-mktcap, or 万-unit)"
    if a >= 1:
        return "1..100 (亿-mktcap / % / ratio)"
    return "<1 (ratio or %)"


print("=== 0089 UNIT PROBE ===")
for kind, dsid in DATASETS.items():
    f = latest_capture(dsid)
    print(f"\n##### {kind} ({dsid})")
    if not f:
        print("  NO CAPTURE FOUND")
        continue
    print(f"  file: {f}")
    try:
        payload = json.loads(Path(f).read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  READ ERROR: {e}")
        continue
    rows = payload.get("rows", [])
    print(f"  row_count: {len(rows)}")
    spot = SPOTLIGHT.get(kind, {})
    for r in rows[:3]:
        code = r.get("code") or r.get("\u4ee3\u7801") or ""
        name = r.get("name") or r.get("\u540d\u79f0") or ""
        print(f"  -- {code} {name}")
        raw = r.get("raw")
        if isinstance(raw, list):
            print(f"     raw_len={len(raw)} raw={raw}")
            for i in spot.get("raw_idx", []):
                if i < len(raw):
                    print(f"       raw[{i}]={raw[i]!r}  | {mag_hint(raw[i])}")
        else:
            print("     (no raw stored)")
        for k in spot.get("derived", []):
            if k in r:
                print(f"       derived[{k}]={r[k]!r}  | {mag_hint(r[k])}")
print("\n=== END PROBE ===")
