#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_schema_probe.py  --  Task 0115

Authoritative raw-structure probe for EVERY dataset not yet in the canonical
REGISTRY, so the coming registration (0116) uses ground truth -- no guessing.

For each target dataset_id: scan all capture dates (newest first), load the most
recent capture, and emit compact ground-truth structure:
  * top-level keys, dataset_label, source_url, headers, row_count, meta
  * the located rows path + total
  * up to 2 sample rows: dict keys + (truncated) values, nested raw[] layout
    when present, or positional list layout

Streaming compact prints, hardest/unknown datasets LAST, so the worker's tail
capture keeps the ones that matter (ztpool / fupan / ltgd). Read-only.
"""
from __future__ import annotations
import os, json, glob

WS = os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace")
CAP = os.path.join(WS, "projects", "duanxianxia", "captures")

# Order: already-understood first, hardest/uncommitted LAST (survive tail).
TARGETS = [
    "rank.rocket",
    "rank.hot_stock_day",
    "cashflow.stock.today",
    "cashflow.stock.3day",
    "cashflow.stock.5day",
    "cashflow.stock.10day",
    "auction.jjlive.fengdan",
    "home.kaipan.plate.summary",
    "home.qxlive.top_metrics",
    "review.daily.top_metrics",
    "home.ztpool",
    "review.ltgd.range",
    "review.fupan.plate",
]


def _dates():
    if not os.path.isdir(CAP):
        return []
    return sorted(
        [d for d in os.listdir(CAP) if os.path.isdir(os.path.join(CAP, d))],
        reverse=True,
    )


def _latest_capture(dsid):
    for d in _dates():
        ddir = os.path.join(CAP, d, dsid)
        if os.path.isdir(ddir):
            files = sorted(glob.glob(os.path.join(ddir, "*.json")))
            if files:
                return d, files[-1]
    return None, None


def _trunc(v, n=90):
    s = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
    return s if len(s) <= n else s[:n] + "\u2026"


def _row_compact(row):
    if isinstance(row, dict):
        info = {"kind": "dict", "keys": list(row.keys())}
        raw = row.get("raw")
        if isinstance(raw, list):
            info["raw_len"] = len(raw)
            info["raw"] = [_trunc(x, 30) for x in raw]
        info["vals"] = {k: _trunc(row[k], 50) for k in row.keys() if k != "raw"}
        return info
    if isinstance(row, list):
        return {"kind": "list", "len": len(row), "vals": [_trunc(x, 30) for x in row]}
    return {"kind": type(row).__name__, "val": _trunc(row)}


def _find_rows(obj):
    if isinstance(obj, dict):
        for key in ("rows", "list", "data", "items"):
            v = obj.get(key)
            if isinstance(v, list):
                return key, v
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, list):
                        return key + "." + k2, v2
    if isinstance(obj, list):
        return "<root>", obj
    return None, None


def main():
    print("=== duanxianxia schema probe 0115 ===")
    print("captures_root=" + CAP)
    print("dates=" + json.dumps(_dates()[:8], ensure_ascii=False))
    for dsid in TARGETS:
        d, fp = _latest_capture(dsid)
        if not fp:
            print("@@ " + dsid + " " + json.dumps({"found": False}, ensure_ascii=False))
            continue
        entry = {"found": True, "date": d, "file": os.path.basename(fp)}
        try:
            with open(fp, "r", encoding="utf-8") as f:
                cap = json.load(f)
        except Exception as e:
            entry["error"] = repr(e)
            print("@@ " + dsid + " " + json.dumps(entry, ensure_ascii=False))
            continue
        entry["top_type"] = type(cap).__name__
        if isinstance(cap, dict):
            entry["top_keys"] = list(cap.keys())
            entry["dataset_label"] = cap.get("dataset_label")
            entry["source_url"] = cap.get("source_url")
            entry["headers"] = cap.get("headers")
            entry["row_count"] = cap.get("row_count")
            entry["meta"] = cap.get("meta")
        rows_path, rows = _find_rows(cap)
        entry["rows_path"] = rows_path
        if rows is not None:
            entry["rows_total"] = len(rows)
            entry["rows_sample"] = [_row_compact(r) for r in rows[:2]]
        print("@@ " + dsid + " " + json.dumps(entry, ensure_ascii=False))
    print("=== end probe 0115 ===")


if __name__ == "__main__":
    main()
