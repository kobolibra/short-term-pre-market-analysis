#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0106: diagnose why 002674 T-1 context (ztpool/cashflow/fupan/longtou) is null
on the 2026-07-01 v9 run, i.e. why the 0105 risk gate had no data to fire on.

READ-ONLY. Does NOT touch production output / webhook / bitable / captures.
Enumerates the capture store + loads the premarket bundle for 2026-07-01 exactly
as the v9 pipeline does, and dumps:
  - which capture dates/datasets exist (esp. whether T-1 6/30 was ever captured)
  - the qxlive 2026-07-01 capture file times (to confirm they are after 093300)
  - how previous_trading_day resolves date_t1 / date_t2
  - per-dataset T-1 row counts + loader warnings
  - whether 002674 appears in ANY home.ztpool capture (and its 连板标签/状态)
"""
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

WS = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WS / "projects" / "duanxianxia"
SCRIPTS = WS / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
TZ = ZoneInfo("Asia/Shanghai")
DATE = "2026-07-01"
TARGET = "002674"

KEY_DATASETS = [
    "home.kaipan.plate.summary",
    "home.ztpool",
    "review.fupan.plate",
    "review.ltgd.range",
    "cashflow.stock.today",
    "cashflow.stock.3day",
    "home.qxlive.top_metrics",
    "auction.jjyd.vratio",
]


def _norm(code):
    s = str(code or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _inventory():
    cap = PROJECT_ROOT / "captures"
    out = {}
    if not cap.exists():
        return {"_error": "captures dir missing: %s" % cap}
    dates = sorted(d.name for d in cap.iterdir() if d.is_dir())
    out["all_capture_dates"] = dates
    per = {}
    for ds in KEY_DATASETS:
        rows = {}
        for d in dates:
            dsdir = cap / d / ds
            if dsdir.exists():
                files = sorted(p.name for p in dsdir.glob("*.json"))
                if files:
                    rows[d] = files
        per[ds] = rows
    out["by_dataset"] = per
    return out


def _scan_ztpool_for_target(inv):
    cap = PROJECT_ROOT / "captures"
    hits = {}
    zt = (inv.get("by_dataset") or {}).get("home.ztpool") or {}
    for d, files in zt.items():
        for f in files:
            p = cap / d / "home.ztpool" / f
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception as e:
                hits["%s/%s" % (d, f)] = "read_error: %s" % e
                continue
            if isinstance(data, dict):
                rows = data.get("rows") or []
            elif isinstance(data, list):
                rows = data
            else:
                rows = []
            match = None
            for r in rows or []:
                if isinstance(r, dict) and _norm(r.get("code") or r.get("\u4ee3\u7801")) == TARGET:
                    match = {k: r.get(k) for k in ("code", "\u4ee3\u7801", "\u540d\u79f0", "name", "\u8fde\u677f\u6807\u7b7e", "board_label", "\u72b6\u6001", "\u6da8\u5e45") if k in r}
                    break
            hits["%s/%s" % (d, f)] = {"row_count": len(rows or []), "target_002674": match}
    return hits


def main():
    summary = {"job": "0106_context_gap_diag", "date": DATE, "target": TARGET,
               "run_at": datetime.now(TZ).isoformat()}
    inv = _inventory()
    summary["capture_inventory"] = inv
    summary["ztpool_target_scan"] = _scan_ztpool_for_target(inv) if "by_dataset" in inv else {}

    try:
        from duanxianxia_v7_1_data_loader import load_premarket_bundle
        b = load_premarket_bundle(DATE, PROJECT_ROOT)
        summary["bundle_summary"] = b.to_summary_dict()
    except Exception as e:
        import traceback
        summary["bundle_error"] = "%s | %s" % (e, traceback.format_exc()[-800:])

    print("[0106] SUMMARY_JSON_BEGIN")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print("[0106] SUMMARY_JSON_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
