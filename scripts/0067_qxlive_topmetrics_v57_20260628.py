#!/usr/bin/env python3
"""
Job 0067 - home.qxlive.top_metrics 深挖 v57
市场级指标表(每日一行), 分两部分:
1. 字段探测: 打印字段名+样本值, 确认语义
2. 择时测试: 市场级指标高/低 vs 当日全市场平均 excess + 单象限IC
"""
import json, os, sys
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, spearman

PREOPEN = "093000"
CAPTURES = PROJECT_ROOT / "captures"
DS = "home.qxlive.top_metrics"

def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None

def std(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2: return 0.0
    m = mean(xs)
    return (sum((x - m)**2 for x in xs) / len(xs)) ** 0.5

def pnum(s):
    if s is None: return None
    if isinstance(s, (int, float)): return float(s)
    s2 = str(s).replace(",","").replace("%","").replace("+","").strip()
    if s2 in ("","--","-","null","None"): return None
    try:
        if s2.endswith("\u4ebf"): return float(s2[:-1]) * 10000
        if s2.endswith("\u4e07"): return float(s2[:-1])
        return float(s2)
    except: return None

def load_snapshot(date_dir, dsid):
    """Load latest premarket snapshot; qxlive may be a single dict not list of rows"""
    p = date_dir / dsid
    if not p.exists(): return None
    files = sorted(f for f in p.iterdir() if f.suffix == ".json" and f.stem <= PREOPEN)
    if not files: return None
    with open(files[-1]) as f:
        d = json.load(f)
    return d

daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())
print("Total date dirs:", len(date_dirs))

# ---------- Step 1: field discovery ----------
print("\n" + "="*60)
print("STEP 1: Field discovery")
print("="*60)

day_records = []  # list of {date, raw_snapshot, flat_fields}
for dd in date_dirs:
    snap = load_snapshot(dd, DS)
    if snap is None: continue
    date_str = dd.name
    # flatten: snap may be dict or list
    if isinstance(snap, list):
        rows = snap
    elif isinstance(snap, dict):
        # try common keys
        rows = snap.get("rows") or snap.get("data") or snap.get("list") or [snap]
    else:
        rows = []
    if not rows: rows = [snap] if isinstance(snap, dict) else []
    for row in rows[:3]:  # print first few rows for discovery
        flat = {}
        for k, v in row.items() if isinstance(row, dict) else []:
            num = pnum(v)
            flat[k] = num if num is not None else str(v)[:40]
        day_records.append({"date": date_str, "flat": flat, "row": row})
    break  # only first available date for discovery

if day_records:
    print("Sample keys:", list(day_records[0]["flat"].keys()))
    for i, rec in enumerate(day_records[:2]):
        print("Row", i, ":", {k: v for k, v in list(rec["flat"].items())[:15]})
else:
    print("NO DATA FOUND for", DS)

# ---------- Step 2: load all days ----------
print("\n" + "="*60)
print("STEP 2: Load all days -> market-level metrics")
print("="*60)

market_days = []
for dd in date_dirs:
    snap = load_snapshot(dd, DS)
    if snap is None: continue
    date_str = dd.name
    if isinstance(snap, list): rows = snap
    elif isinstance(snap, dict):
        rows = snap.get("rows") or snap.get("data") or snap.get("list") or [snap]
    else: rows = []
    if not rows and isinstance(snap, dict): rows = [snap]
    # take first row as market summary
    row = rows[0] if rows else {}
    if not isinstance(row, dict): continue
    rec = {"date": date_str}
    for k, v in row.items():
        num = pnum(v)
        if num is not None:
            rec["mkt_" + k] = num
    if len(rec) > 1:
        market_days.append(rec)

print("Market days loaded:", len(market_days))
if market_days:
    print("Market fields:", [k for k in market_days[0] if k != "date"])

# ---------- Step 3: load individual stock excess per day ----------
print("\n" + "="*60)
print("STEP 3: Per-day mean excess from qiangchou universe")
print("="*60)

QIANG = "auction.jjyd.qiangchou"
def load_qiang_rows(date_dir):
    p = date_dir / QIANG
    if not p.exists(): return []
    files = sorted(f for f in p.iterdir() if f.suffix == ".json" and f.stem <= PREOPEN)
    if not files: return []
    with open(files[-1]) as f:
        d = json.load(f)
    return d.get("rows", [])

def code_of(row):
    for k in ["code", "\u4ee3\u7801"]:
        if k in row:
            s = str(row[k]).split(".")[0]
            return s[-6:].zfill(6)
    return None

day_mean_excess = {}
for dd in date_dirs:
    rows = load_qiang_rows(dd)
    date_str = dd.name
    excesses = []
    for row in rows:
        code = code_of(row)
        if not code: continue
        exc = daily.excess(code, date_str)
        if exc is not None:
            excesses.append(exc)
    if excesses:
        day_mean_excess[date_str] = mean(excesses)

print("Days with mean excess:", len(day_mean_excess))
for d, v in sorted(day_mean_excess.items()):
    print("  {}: mean_excess={:.3f}".format(d, v))

# ---------- Step 4: correlation between market indicators and day mean excess ----------
print("\n" + "="*60)
print("STEP 4: Market indicators vs day-mean excess")
print("="*60)

mkt_fields = [k for k in (market_days[0] if market_days else {}) if k != "date"]

for mf in mkt_fields:
    pairs = []
    for rec in market_days:
        date = rec["date"]
        mval = rec.get(mf)
        exc = day_mean_excess.get(date)
        if mval is not None and exc is not None:
            pairs.append((mval, exc))
    if len(pairs) < 5:
        print("  {}: n={} skip".format(mf, len(pairs)))
        continue
    xs, ys = zip(*pairs)
    ic = spearman(list(xs), list(ys))
    print("  {:40s} n={:3d} IC={:.3f}".format(mf, len(pairs), ic))

# ---------- Step 5: binary split: high vs low market day ----------
print("\n" + "="*60)
print("STEP 5: High vs Low market day mean excess")
print("="*60)

for mf in mkt_fields[:8]:  # top 8 fields
    vals = [(rec["date"], rec.get(mf)) for rec in market_days if rec.get(mf) is not None]
    if len(vals) < 6: continue
    med = sorted(v for _, v in vals)[len(vals)//2]
    high_days = [d for d, v in vals if v >= med]
    low_days = [d for d, v in vals if v < med]
    high_exc = [day_mean_excess[d] for d in high_days if d in day_mean_excess]
    low_exc = [day_mean_excess[d] for d in low_days if d in day_mean_excess]
    mh = mean(high_exc); ml = mean(low_exc)
    if mh is None or ml is None: continue
    print("  {}: high_mean={:.3f}(n={}) low_mean={:.3f}(n={}) diff={:.3f}".format(
        mf, mh, len(high_exc), ml, len(low_exc), mh - ml))

# ---------- Step 6: raw market day stats ----------
print("\n" + "="*60)
print("STEP 6: Raw market day values")
print("="*60)
for rec in market_days:
    print("  {}: {}".format(rec["date"], {k: round(v,2) for k,v in rec.items() if k != "date"}))

print("\n[DONE]")
