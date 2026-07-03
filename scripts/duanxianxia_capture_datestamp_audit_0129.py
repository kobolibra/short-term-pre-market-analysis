#!/usr/bin/env python3
"""0129 capture date-stamp audit (read-only).

Walk the capture tree and, for every dataset that carries its own content
trade-date, compare the on-disk folder date (== fetched_at date) against the
data's real trade-date. Report every mismatch WITH its fetched_at timestamp so
we can see WHEN the misplaced run actually executed (hour histogram + weekday).

No writes, no network. Purely diagnostic.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import date
from pathlib import Path

WS = Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace"))
CAPTURE_ROOT = WS / "projects" / "duanxianxia" / "captures"

# Datasets whose payload is expected to carry a real content trade-date.
DATE_BEARING = {
    "review.daily.top_metrics",
    "review.daily.top_metrics.core11",
    "review.fupan.plate",
    "review.ltgd.range",
    "home.ztpool",
    "home.qxlive.top_metrics",
}


def _clean_date(value):
    if not isinstance(value, str):
        return None
    v = value.strip()
    if len(v) >= 10 and v[:4].isdigit() and v[4] == "-":
        return v[:10]
    return None


def content_date_from_meta(payload):
    meta = payload.get("meta", {}) or {}
    for key in ("date", "latest_date", "trade_date"):
        d = _clean_date(meta.get(key))
        if d:
            return d
    return None


def content_date_from_rows(payload):
    rows = payload.get("rows", []) or []
    if rows and isinstance(rows[0], dict):
        for key in ("\u65e5\u671f", "date", "section_date"):
            d = _clean_date(rows[0].get(key))
            if d:
                return d
    return None


def weekday_of(folder_date):
    try:
        y, m, d = (int(x) for x in folder_date.split("-"))
        return date(y, m, d).strftime("%a")
    except Exception:
        return "?"


def main():
    result = {
        "task": "0129_capture_datestamp_audit",
        "ok": True,
        "errors": [],
        "capture_root": str(CAPTURE_ROOT),
        "root_exists": CAPTURE_ROOT.exists(),
        "total_files": 0,
        "date_bearing_files": 0,
        "content_date_resolved": 0,
        "mismatch_total": 0,
        "mismatch_by_dataset": {},
        "mismatch_fetched_hour_hist": {},
        "mismatch_folder_weekday_hist": {},
        "mismatches": [],
        "sample_ok": [],
    }
    if not CAPTURE_ROOT.exists():
        result["ok"] = False
        result["errors"].append("capture root missing")
        print("=== CAPTURE DATESTAMP AUDIT 0129 ===")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    by_ds = defaultdict(int)
    hour_hist = defaultdict(int)
    wday_hist = defaultdict(int)
    ok_samples = 0

    for date_dir in sorted(CAPTURE_ROOT.iterdir()):
        if not date_dir.is_dir():
            continue
        folder_date = date_dir.name
        for ds_dir in sorted(date_dir.iterdir()):
            if not ds_dir.is_dir():
                continue
            dataset_id = ds_dir.name
            for f in sorted(ds_dir.glob("*.json")):
                result["total_files"] += 1
                try:
                    payload = json.loads(f.read_text(encoding="utf-8"))
                except Exception as exc:
                    result["errors"].append("read fail %s: %s" % (f, exc))
                    continue
                if dataset_id in DATE_BEARING:
                    result["date_bearing_files"] += 1
                fetched_at = str(payload.get("fetched_at", ""))
                cdate = content_date_from_meta(payload) or content_date_from_rows(payload)
                if cdate is None:
                    continue
                result["content_date_resolved"] += 1
                if cdate != folder_date:
                    result["mismatch_total"] += 1
                    by_ds[dataset_id] += 1
                    hh = fetched_at[11:13] if len(fetched_at) >= 13 else "??"
                    hour_hist[hh] += 1
                    wday_hist[weekday_of(folder_date)] += 1
                    if len(result["mismatches"]) < 300:
                        result["mismatches"].append({
                            "dataset_id": dataset_id,
                            "folder_date": folder_date,
                            "content_date": cdate,
                            "fetched_at": fetched_at,
                            "file": str(f.relative_to(CAPTURE_ROOT)),
                        })
                elif ok_samples < 10:
                    result["sample_ok"].append({
                        "dataset_id": dataset_id,
                        "folder_date": folder_date,
                        "content_date": cdate,
                        "fetched_at": fetched_at,
                    })
                    ok_samples += 1

    result["mismatch_by_dataset"] = dict(sorted(by_ds.items()))
    result["mismatch_fetched_hour_hist"] = dict(sorted(hour_hist.items()))
    result["mismatch_folder_weekday_hist"] = dict(sorted(wday_hist.items()))
    print("=== CAPTURE DATESTAMP AUDIT 0129 ===")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
