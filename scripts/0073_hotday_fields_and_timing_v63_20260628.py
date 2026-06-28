#!/usr/bin/env python3
"""
Job 0073 - rank.hot_stock_day full fields + capture-timing + dataset labels v63b

0072 was truncated before rank.hot_stock_day, and revealed pool.hot/pool.surge
are captured ~10:02 (NOT premarket). Settle definitively:
  1. dataset_label (official CN name) + headers + meta for ALL 4 tables
  2. capture-timing histogram across ALL dates (premarket<=0925 vs intraday)
  3. rank.hot_stock_day full field union + sample rows
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

# ---- dataset label / headers / meta per table ----
print("="*60)
print("DATASET LABELS / HEADERS / META")
print("="*60)
for dsid in TABLES:
    dd = next((d for d in sorted(date_dirs, reverse=True) if (d/dsid).exists()), None)
    if dd is None:
        print("\n# {}: (none)".format(dsid)); continue
    p = dd / dsid
    files = sorted(f for f in p.iterdir() if f.suffix == ".json")
    with open(files[0]) as f:
        d = json.load(f)
    if isinstance(d, dict):
        print("\n# {}".format(dsid))
        print("  dataset_label:", d.get("dataset_label"))
        print("  dataset_kind :", d.get("dataset_kind"))
        print("  source_url   :", d.get("source_url"))
        print("  headers      :", d.get("headers"))
        print("  meta         :", json.dumps(d.get("meta"), ensure_ascii=False)[:300])

# ---- timing histogram ----
print("\n" + "="*60)
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
    chosen = files[0]
    with open(chosen) as f:
        d = json.load(f)
    rows = rows_of(d)
    print("\n[{}] file={} n_rows={}".format(dd.name, chosen.name, len(rows)))
    keys = []
    for r in rows:
        if isinstance(r, dict):
            for k in r.keys():
                if k not in keys: keys.append(k)
    print("  key_union={}".format(keys))
    for i, r in enumerate(rows[:8]):
        print("  row[{}]: {}".format(i, json.dumps(r, ensure_ascii=False)))

print("\n[DONE]")
