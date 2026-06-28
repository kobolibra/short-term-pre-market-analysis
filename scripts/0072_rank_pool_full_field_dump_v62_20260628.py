#!/usr/bin/env python3
"""
Job 0072 - full field-level raw dump of 热度榜/飙升榜 v62

User wants field-by-field analysis of the DAILY 热度榜 & 飙升榜 snapshots
(not multi-day IC). Stop guessing schema/timing -> just dump everything.

For each candidate dataset, for the 2 most recent dates that have it:
  - list ALL capture timestamps (so we see WHEN it is captured, premarket?)
  - load the last premarket file (<=093000) if any else the LAST file of day
  - print n_rows, UNION of all keys seen across rows, and first 6 rows FULL.
"""
import json, os
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
CAPTURES = PROJECT_ROOT / "captures"
PREOPEN = "093000"

CANDIDATES = ["rank.hot_stock_day", "rank.rocket", "pool.hot", "pool.surge",
              "home.ztpool", "review.daily.top_metrics"]

def rows_of(d):
    if d is None: return []
    if isinstance(d, list): return d
    if isinstance(d, dict):
        for k in ["rows","data","list","items","result","stocks","datas"]:
            v = d.get(k)
            if isinstance(v, list): return v
        for v in d.values():
            if isinstance(v, dict):
                for k in ["rows","data","list","items"]:
                    if isinstance(v.get(k), list): return v[k]
    return []

date_dirs = sorted((p for p in CAPTURES.iterdir() if p.is_dir()), reverse=True)

for dsid in CANDIDATES:
    print("\n" + "#"*64)
    print("# DATASET:", dsid)
    print("#"*64)
    dates_with = [dd for dd in date_dirs if (dd / dsid).exists()]
    if not dates_with:
        print("  (no date dir has this dataset)"); continue
    print("  present in {} dates; most recent: {}".format(
        len(dates_with), [d.name for d in dates_with[:3]]))
    for dd in dates_with[:2]:
        p = dd / dsid
        files = sorted(f for f in p.iterdir() if f.suffix == ".json")
        stems = [f.stem for f in files]
        print("\n  === date {} ===".format(dd.name))
        print("  capture timestamps ({}): {}".format(len(stems), stems[:30]))
        pre = [f for f in files if f.stem <= PREOPEN]
        chosen = pre[-1] if pre else (files[-1] if files else None)
        if chosen is None:
            print("   no json files"); continue
        print("  chosen file: {}  (premarket={})".format(chosen.name, bool(pre)))
        try:
            with open(chosen) as f:
                d = json.load(f)
        except Exception as e:
            print("   load error:", e); continue
        if isinstance(d, dict):
            print("  top-level dict keys:", list(d.keys())[:25])
        rows = rows_of(d)
        print("  n_rows:", len(rows))
        keys = []
        for r in rows:
            if isinstance(r, dict):
                for k in r.keys():
                    if k not in keys: keys.append(k)
        print("  UNION of row keys ({}): {}".format(len(keys), keys))
        for i, r in enumerate(rows[:6]):
            print("  row[{}]: {}".format(i, json.dumps(r, ensure_ascii=False)))

print("\n[DONE]")
