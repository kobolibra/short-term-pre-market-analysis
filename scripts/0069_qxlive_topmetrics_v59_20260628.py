#!/usr/bin/env python3
"""
Job 0069 - home.qxlive.top_metrics 深挖 v59 (修复 0067)
修复:
1. 长格式表: 每行是一个市场级指标(QX情绪/ZT涨停家数...), 需按 metric_key 透视
2. spearman 返回 None 时不能直接 {:.3f}
目标: 每个市场指标 vs 当日平均excess 择时测试
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
QIANG = "auction.jjyd.qiangchou"

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

def _norm(code):
    s = str(code).split(".")[0]
    return s[-6:].zfill(6)

def code_of(row):
    for k in ["code", "\u4ee3\u7801"]:
        if k in row: return _norm(row[k])
    return None

def load_rows(date_dir, dsid):
    p = date_dir / dsid
    if not p.exists(): return []
    files = sorted(f for f in p.iterdir() if f.suffix == ".json" and f.stem <= PREOPEN)
    if not files: return []
    with open(files[-1]) as f:
        d = json.load(f)
    if isinstance(d, list): return d
    return d.get("rows", []) or d.get("data", []) or d.get("list", [])

daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())
print("Total date dirs:", len(date_dirs))

# ---------- Step 1: pivot long-format metrics per date ----------
print("\n" + "="*60)
print("STEP 1: Pivot qxlive metrics by metric_key")
print("="*60)

metrics_by_date = {}  # date -> {metric_key: raw_value}
label_map = {}        # metric_key -> metric_label
for dd in date_dirs:
    rows = load_rows(dd, DS)
    if not rows: continue
    date_str = dd.name
    mp = {}
    for row in rows:
        if not isinstance(row, dict): continue
        mk = row.get("metric_key")
        if not mk: continue
        val = pnum(row.get("raw_value"))
        if val is None: val = pnum(row.get("value"))
        if val is not None:
            mp[mk] = val
            if mk not in label_map:
                label_map[mk] = str(row.get("metric_label", mk))
    if mp:
        metrics_by_date[date_str] = mp

all_metrics = sorted(set(k for mp in metrics_by_date.values() for k in mp))
print("Dates with metrics:", len(metrics_by_date))
print("Metric keys found:", [(k, label_map.get(k)) for k in all_metrics])

print("\n--- Raw metric values by date ---")
for date in sorted(metrics_by_date):
    mp = metrics_by_date[date]
    print("  {}: {}".format(date, {k: mp.get(k) for k in all_metrics}))

# ---------- Step 2: per-day mean excess (qiangchou universe) ----------
print("\n" + "="*60)
print("STEP 2: Per-day mean excess (qiangchou universe)")
print("="*60)

day_mean_excess = {}
for dd in date_dirs:
    rows = load_rows(dd, QIANG)
    date_str = dd.name
    excesses = []
    for row in rows:
        code = code_of(row)
        if not code: continue
        exc = daily.excess(code, date_str)
        if exc is not None: excesses.append(exc)
    if excesses:
        day_mean_excess[date_str] = mean(excesses)

for d in sorted(day_mean_excess):
    print("  {}: mean_excess={:.3f}".format(d, day_mean_excess[d]))

# ---------- Step 3: market indicator vs day-mean excess (timing) ----------
print("\n" + "="*60)
print("STEP 3: Each market metric vs day-mean excess (timing IC)")
print("="*60)

for mk in all_metrics:
    pairs = []
    for date, mp in metrics_by_date.items():
        mval = mp.get(mk)
        exc = day_mean_excess.get(date)
        if mval is not None and exc is not None:
            pairs.append((mval, exc))
    label = label_map.get(mk, mk)
    if len(pairs) < 5:
        print("  {} ({}): n={} skip".format(mk, label, len(pairs)))
        continue
    xs, ys = zip(*pairs)
    ic = spearman(list(xs), list(ys))
    ic_str = "{:.3f}".format(ic) if ic is not None else "None"
    print("  {:6s} ({:8s}) n={:3d} timing_IC={}".format(mk, label, len(pairs), ic_str))

# ---------- Step 4: high vs low metric day ----------
print("\n" + "="*60)
print("STEP 4: High vs Low metric day -> next mean excess")
print("="*60)

for mk in all_metrics:
    vals = [(date, mp[mk]) for date, mp in metrics_by_date.items() if mp.get(mk) is not None and date in day_mean_excess]
    if len(vals) < 6:
        continue
    sorted_v = sorted(v for _, v in vals)
    med = sorted_v[len(sorted_v)//2]
    high_exc = [day_mean_excess[d] for d, v in vals if v >= med]
    low_exc = [day_mean_excess[d] for d, v in vals if v < med]
    mh = mean(high_exc); ml = mean(low_exc)
    if mh is None or ml is None: continue
    label = label_map.get(mk, mk)
    print("  {:6s} ({:8s}) high_mean={:.3f}(n={}) low_mean={:.3f}(n={}) diff={:.3f}".format(
        mk, label, mh, len(high_exc), ml, len(low_exc), mh - ml))

print("\n[DONE]")
