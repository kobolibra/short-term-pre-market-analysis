#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0125 -- focused follow-up to 0124 (READ-ONLY).

A) print the LIVE crontab (compact, so it is NOT truncated in stdout_tail) to
   identify the trigger behind the ~01:20 overnight fupan/daily captures.
A) list the overnight fupan/daily snapshots (snap before 06:00) -- exactly which
   dates landed overnight vs the normal 17:2x evening run.
B) locate where '竞价涨幅' actually lives: for today's vratio+qiangchou first
   rows AND a known-populated 06-01 qiangchou/vratio snapshot, dump
   auction_change_pct / auction_change_pct_text / latest_change_pct / raw so we
   can tell whether the value is genuinely absent at 09:25:3x or just filled by
   the site a little later (06-01 sample was 09:28).
"""
from __future__ import annotations
import os, json, subprocess, pathlib

WS = pathlib.Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace")).resolve()
CAP = WS / "projects" / "duanxianxia" / "captures"
TARGET = "2026-07-03"


def load(p):
    try:
        return json.loads(pathlib.Path(p).read_text(encoding="utf-8"))
    except Exception as e:
        return {"_err": f"{type(e).__name__}: {e}"}


def rows_of(o):
    r = o.get("rows") if isinstance(o, dict) else None
    return r if isinstance(r, list) else []


out = {}

cr = subprocess.run(["crontab", "-l"], text=True, capture_output=True)
out["crontab"] = [l for l in (cr.stdout or "").splitlines() if l.strip() and not l.strip().startswith("#")]
out["crontab_rc"] = cr.returncode
out["crontab_stderr"] = (cr.stderr or "")[-200:]

ov = {}
for ds in ("review.fupan.plate", "review.daily.top_metrics", "review.ltgd.range"):
    recs = []
    for d in sorted(CAP.glob("*")):
        dsdir = d / ds
        if not dsdir.is_dir():
            continue
        for s in sorted(dsdir.glob("*.json")):
            if s.stem.isdigit() and int(s.stem) < 60000:
                o = load(s)
                recs.append({"folder": d.name, "snap": s.stem,
                              "fetched_at": str(o.get("fetched_at", ""))[:19]})
    ov[ds] = recs
out["overnight_records"] = ov


def probe(path, n=3):
    o = load(path)
    res = {"path": str(path).replace(str(WS), "$WS"),
           "row_count": o.get("row_count", len(rows_of(o))),
           "meta": o.get("meta"), "rows": []}
    for r in rows_of(o)[:n]:
        if isinstance(r, dict):
            res["rows"].append({
                "group": r.get("group"),
                "code": r.get("code"), "name": r.get("name"),
                "auction_change_pct": r.get("auction_change_pct"),
                "auction_change_pct_text": r.get("auction_change_pct_text"),
                "latest_change_pct": r.get("latest_change_pct"),
                "raw": r.get("raw"),
            })
    return res


def first_snap(dstr, ds):
    dsdir = CAP / dstr / ds
    if not dsdir.is_dir():
        return None
    snaps = sorted(dsdir.glob("*.json"))
    return snaps[0] if snaps else None


probes = {}
for label, dstr, ds in (
    ("today_vratio", TARGET, "auction.jjyd.vratio"),
    ("today_qiangchou", TARGET, "auction.jjyd.qiangchou"),
    ("ref_0601_qiangchou", "2026-06-01", "auction.jjyd.qiangchou"),
    ("ref_0601_vratio", "2026-06-01", "auction.jjyd.vratio"),
):
    s = first_snap(dstr, ds)
    probes[label] = probe(s) if s else {"error": "no snapshot"}
out["field_probe"] = probes

print(json.dumps(out, ensure_ascii=False, indent=2))
