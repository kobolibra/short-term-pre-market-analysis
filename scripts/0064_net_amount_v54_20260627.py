#!/usr/bin/env python3
"""
Job 0064 - net_amount 深挖 v54
修复: Daily(root) 单参数; excess(code, date_str) 双参数方法
"""
import json, os, sys
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, spearman

PREOPEN = "093000"
DS = "auction.jjyd.net_amount"
CAPTURES = PROJECT_ROOT / "captures"

def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else 0.0

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
        if s2.endswith("亿"): return float(s2[:-1]) * 10000
        if s2.endswith("万"): return float(s2[:-1])
        return float(s2)
    except: return None

def _norm(code):
    s = str(code).split(".")[0]
    return s[-6:].zfill(6)

def safe_div(a, b):
    if a is None or b is None: return None
    try:
        bv = float(b)
        return float(a) / bv if bv != 0 else None
    except: return None

CODE_KEYS = ["code", "\u4ee3\u7801"]
def code_of(row):
    for k in CODE_KEYS:
        if k in row: return _norm(row[k])
    return None

def load_rows(date_dir, dsid):
    p = date_dir / dsid
    if not p.exists(): return []
    files = sorted(f for f in p.iterdir() if f.suffix == ".json" and f.stem <= PREOPEN)
    if not files: return []
    with open(files[-1]) as f:
        d = json.load(f)
    return d.get("rows", [])

# ---------- init Daily once ----------
daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())
print(f"Total date dirs: {len(date_dirs)}")

records = []
for dd in date_dirs:
    rows = load_rows(dd, DS)
    if not rows: continue
    date_str = dd.name
    for row in rows:
        code = code_of(row)
        if not code: continue
        exc = daily.excess(code, date_str)
        if exc is None: continue
        r = {"date": date_str, "code": code, "excess": exc}
        r["main_wan"] = pnum(row.get("main_net_inflow_wan"))
        r["turnover_wan"] = pnum(row.get("auction_turnover_wan"))
        r["turnover_rate"] = pnum(row.get("turnover_rate_pct"))
        r["mktcap"] = pnum(row.get("market_cap_yi"))
        r["gap"] = pnum(row.get("auction_change_pct"))
        r["main_over_turnover"] = safe_div(r["main_wan"], r["turnover_wan"])
        r["main_over_mktcap"] = safe_div(r["main_wan"], r["mktcap"])
        records.append(r)

print(f"Total records: {len(records)}")
dates = sorted(set(r["date"] for r in records))
print(f"Dates: {len(dates)} -> {dates}")

# ---------- coverage ----------
FIELDS = ["main_wan","turnover_wan","turnover_rate","mktcap","gap","main_over_turnover","main_over_mktcap"]
print("\n--- Coverage ---")
for f in FIELDS:
    n_ok = sum(1 for r in records if r.get(f) is not None)
    print(f"  {f}: {n_ok}/{len(records)} = {n_ok/len(records)*100:.1f}%")

# ---------- per-date IC ----------
print("\n--- Per-date Spearman IC ---")
ic_by = {f: [] for f in FIELDS}
for date in dates:
    dr = [r for r in records if r["date"] == date]
    for f in FIELDS:
        pairs = [(r[f], r["excess"]) for r in dr if r.get(f) is not None]
        if len(pairs) < 5: continue
        xs, ys = zip(*pairs)
        ic_by[f].append(spearman(list(xs), list(ys)))

print(f"{'Field':<26} {'n':>4} {'mean_IC':>9} {'ICIR':>8}")
for f in FIELDS:
    ics = ic_by[f]
    if not ics:
        print(f"  {f:<24} {'0':>4}")
        continue
    m = mean(ics); s = std(ics)
    icir = m / s if s > 0 else 0.0
    print(f"  {f:<24} {len(ics):>4} {m:>9.3f} {icir:>8.3f}")

# ---------- main_wan tercile buckets ----------
print("\n--- main_wan tercile buckets ---")
bkt_main = {"top": [], "mid": [], "bot": []}
for date in dates:
    dr = sorted([r for r in records if r["date"] == date and r.get("main_wan") is not None], key=lambda x: x["main_wan"])
    n = len(dr); t = n // 3
    if n < 6: continue
    for i, r in enumerate(dr):
        bkt_main["top" if i >= n-t else "bot" if i < t else "mid"].append(r["excess"])
for k, v in bkt_main.items():
    print(f"  {k}: n={len(v)} mean={mean(v):.3f}")

# ---------- main_over_turnover tercile ----------
print("\n--- main_over_turnover tercile buckets ---")
bkt_mot = {"top": [], "mid": [], "bot": []}
for date in dates:
    dr = sorted([r for r in records if r["date"] == date and r.get("main_over_turnover") is not None], key=lambda x: x["main_over_turnover"])
    n = len(dr); t = n // 3
    if n < 6: continue
    for i, r in enumerate(dr):
        bkt_mot["top" if i >= n-t else "bot" if i < t else "mid"].append(r["excess"])
for k, v in bkt_mot.items():
    print(f"  {k}: n={len(v)} mean={mean(v):.3f}")

# ---------- gap sign bucket ----------
print("\n--- gap sign bucket ---")
gap_pos = [r["excess"] for r in records if r.get("gap") is not None and r["gap"] > 0]
gap_neg = [r["excess"] for r in records if r.get("gap") is not None and r["gap"] < 0]
gap_zer = [r["excess"] for r in records if r.get("gap") is not None and r["gap"] == 0]
print(f"  pos: n={len(gap_pos)} mean={mean(gap_pos):.3f}")
print(f"  neg: n={len(gap_neg)} mean={mean(gap_neg):.3f}")
print(f"  zero: n={len(gap_zer)} mean={mean(gap_zer):.3f}")

# ---------- mktcap buckets ----------
print("\n--- market_cap buckets ---")
bkt_mc = {"large(>=500yi)": [], "mid(100-500yi)": [], "small(<100yi)": []}
for r in records:
    mc = r.get("mktcap")
    if mc is None: continue
    k = "large(>=500yi)" if mc >= 500 else "mid(100-500yi)" if mc >= 100 else "small(<100yi)"
    bkt_mc[k].append(r["excess"])
for k, v in bkt_mc.items():
    print(f"  {k}: n={len(v)} mean={mean(v):.3f}")

# ---------- interaction: turnover_wan x main_over_turnover ----------
print("\n--- Interaction: turnover_wan (H/L) x main_over_turnover (H/L) ---")
quad = {"HH": [], "HL": [], "LH": [], "LL": []}
for date in dates:
    dr = [r for r in records if r["date"] == date and r.get("turnover_wan") is not None and r.get("main_over_turnover") is not None]
    if len(dr) < 8: continue
    t_med = sorted(r["turnover_wan"] for r in dr)[len(dr)//2]
    m_med = sorted(r["main_over_turnover"] for r in dr)[len(dr)//2]
    for r in dr:
        th = r["turnover_wan"] >= t_med; mh = r["main_over_turnover"] >= m_med
        quad["HH" if th and mh else "HL" if th and not mh else "LH" if not th and mh else "LL"].append(r["excess"])
for q, v in quad.items():
    print(f"  {q}: n={len(v)} mean={mean(v):.3f}")

# ---------- gap x main_over_turnover ----------
print("\n--- gap sign x main_over_turnover (H/L) ---")
gxm = {"gap+_mot_H": [], "gap+_mot_L": [], "gap-_mot_H": [], "gap-_mot_L": []}
for date in dates:
    dr = [r for r in records if r["date"] == date and r.get("gap") is not None and r.get("main_over_turnover") is not None]
    if len(dr) < 8: continue
    m_med = sorted(r["main_over_turnover"] for r in dr)[len(dr)//2]
    for r in dr:
        mh = r["main_over_turnover"] >= m_med
        if r["gap"] > 0: gxm["gap+_mot_H" if mh else "gap+_mot_L"].append(r["excess"])
        elif r["gap"] < 0: gxm["gap-_mot_H" if mh else "gap-_mot_L"].append(r["excess"])
for k, v in gxm.items():
    print(f"  {k}: n={len(v)} mean={mean(v):.3f}" if v else f"  {k}: 0")

# ---------- per-date universe size ----------
print("\n--- Per-date universe size ---")
for date in dates:
    n = len([r for r in records if r["date"] == date])
    print(f"  {date}: n={n}")

print("\n[DONE]")
