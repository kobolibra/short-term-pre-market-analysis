#!/usr/bin/env python3
"""
Job 0073 - rank.hot_stock_day full fields + capture-timing across ALL dates v63

0072 was truncated before rank.hot_stock_day, and revealed pool.hot/pool.surge
are captured ~10:02 (NOT premarket). Need to settle:
  1. rank.hot_stock_day: full field union + sample rows + its capture timestamps
  2. capture-timing histogram across ALL dates for the 4 candidate tables, so we
     know which are truly premarket(<=0925) vs intraday(>0930).
"""
import json, os
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
CAPTURES = PROJECT_ROOT / "captures"
TABLES = ["rank.hot_stock_day", "rank.rocket", "pool.hot", "pool.surge"]

def rows_of(d):
    if d is None: return []
    if isinstance(d, list): return d
    if isinstance(d, dict):
        for k in ["rows","data","list","items","result","stocks","datas"]:
            v = d.get(k)
            if isinstance(v, list): return v
    return []

date_dirs = sorted((p for p in CAPTURES.iterdir() if p.is_dir()))

# ---- timing histogram ----
print("="*60)
print("CAPTURE TIMING across all dates")
print("="*60)
for dsid in TABLES:
    print("\n# {}".format(dsid))
    for dd in date_dirs:
        p = dd / dsid
        if not p.exists(): continue
        stems = sorted(f.stem for f in p.iterdir() if f.suffix == ".json")
        print("  {}: {}".format(dd.name, stems))

# ---- rank.hot_stock_day full fields ----
print("\n" + "="*60)
print("rank.hot_stock_day FULL FIELD DUMP (recent 3 dates)")
print("="*60)
dsid = "rank.hot_stock_day"
dates_with = [dd for dd in sorted(date_dirs, reverse=True) if (dd / dsid).exists()]
for dd in dates_with[:3]:
    p = dd / dsid
    files = sorted(f for f in p.iterdir() if f.suffix == ".json")
    if not files: continue
    chosen = files[0]  # earliest capture of the day
    with open(chosen) as f:
        d = json.load(f)
    rows = rows_of(d)
    if isinstance(d, dict):
        print("\n[{}] file={} top_keys={} headers={}".format(
            dd.name, chosen.name, list(d.keys())[:14], d.get("headers")))
    keys = []
    for r in rows:
        if isinstance(r, dict):
            for k in r.keys():
                if k not in keys: keys.append(k)
    print("  n_rows={} key_union={}".format(len(rows), keys))
    for i, r in enumerate(rows[:8]):
        print("  row[{}]: {}".format(i, json.dumps(r, ensure_ascii=False)))

print("\n[DONE]")
