#!/usr/bin/env python3
"""
Job 0061 - 竞价净额 (net_amount) 深挖分析 v51
表⑤ auction.jjyd.net_amount: 竞价主力净额正流的全市场前75名
问题: 合并后按 main_net_inflow_wan 量级/归一化形态等 IC; 交互检验
"""
import json, os, sys
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, mean, pearson, spearman, pctl, extract, load_days, DEFAULT_PROJECT_ROOT, CORE_FIELDS

PREOPEN = "093000"
DS = "auction.jjyd.net_amount"
CAPTURES = PROJECT_ROOT / "captures"

# ---------- helpers ----------
def pnum(s):
    if s is None: return None
    if isinstance(s, (int, float)): return float(s)
    s2 = str(s).replace(",", "").replace("%", "").replace("+", "").strip()
    if s2 in ("", "--", "-", "null", "None"): return None
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
    rows = d.get("rows", [])
    if not rows and "data" in d:
        rows = d["data"]
    return rows

# ---------- load all dates ----------
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())
print(f"Total date dirs: {len(date_dirs)}")

records = []
for dd in date_dirs:
    rows = load_rows(dd, DS)
    if not rows: continue
    d_obj = Daily(str(PROJECT_ROOT), dd.name)
    for row in rows:
        code = code_of(row)
        if not code: continue
        exc = d_obj.excess.get(code)
        if exc is None: continue
        r = {"date": dd.name, "code": code, "excess": exc}
        r["main_wan"] = pnum(row.get("main_net_inflow_wan"))
        r["auction_turnover_wan"] = pnum(row.get("auction_turnover_wan"))
        r["turnover_rate_pct"] = pnum(row.get("turnover_rate_pct"))
        r["market_cap_yi"] = pnum(row.get("market_cap_yi"))
        r["auction_change_pct"] = pnum(row.get("auction_change_pct"))
        r["latest_change_pct"] = pnum(row.get("latest_change_pct"))
        # normalized
        r["main_over_turnover"] = safe_div(r["main_wan"], r["auction_turnover_wan"])
        r["main_over_mktcap"] = safe_div(r["main_wan"], r["market_cap_yi"])
        records.append(r)

print(f"Total (code,date) pairs: {len(records)}")
dates = sorted(set(r["date"] for r in records))
print(f"Dates with data: {len(dates)} -> {dates}")

# ---------- coverage ----------
FIELDS = ["main_wan", "auction_turnover_wan", "turnover_rate_pct", "market_cap_yi",
          "auction_change_pct", "latest_change_pct", "main_over_turnover", "main_over_mktcap"]
n_total = len(records)
print("\n--- Coverage ---")
for f in FIELDS:
    n_ok = sum(1 for r in records if r.get(f) is not None)
    print(f"  {f}: {n_ok}/{n_total} = {n_ok/n_total*100:.1f}%")

# ---------- per-date IC ----------
print("\n--- Per-date IC (Spearman) ---")
ic_by_field = {f: [] for f in FIELDS}
for date in dates:
    dr = [r for r in records if r["date"] == date]
    excs = [r["excess"] for r in dr]
    for f in FIELDS:
        vals = [r.get(f) for r in dr]
        pairs = [(v, e) for v, e in zip(vals, excs) if v is not None]
        if len(pairs) < 5: continue
        xs, ys = zip(*pairs)
        ic_by_field[f].append(spearman(list(xs), list(ys)))

print(f"{'Field':<30} {'n_dates':>7} {'mean_IC':>9} {'ICIR':>8}")
results = []
for f in FIELDS:
    ics = ic_by_field[f]
    if not ics:
        print(f"  {f:<28} {'0':>7} {'N/A':>9} {'N/A':>8}")
        continue
    m = mean(ics)
    std = (mean([x**2 for x in ics]) - m**2) ** 0.5
    icir = m / std if std > 0 else 0
    results.append((f, len(ics), m, icir))
    print(f"  {f:<28} {len(ics):>7} {m:>9.3f} {icir:>8.3f}")

# ---------- main_wan distribution & buckets ----------
print("\n--- main_wan magnitude buckets (per-date, excess vs market) ---")
bucket_excess = {"top33": [], "mid33": [], "bot33": []}
for date in dates:
    dr = [r for r in records if r["date"] == date and r.get("main_wan") is not None]
    if len(dr) < 6: continue
    dr_s = sorted(dr, key=lambda x: x["main_wan"])
    n = len(dr_s)
    t = n // 3
    for i, r in enumerate(dr_s):
        if i < t: bucket_excess["bot33"].append(r["excess"])
        elif i < 2*t: bucket_excess["mid33"].append(r["excess"])
        else: bucket_excess["top33"].append(r["excess"])

for bk, excs in bucket_excess.items():
    print(f"  {bk}: n={len(excs)} mean_excess={mean(excs):.3f}")

# ---------- main_over_turnover buckets ----------
print("\n--- main_over_turnover (normalized intensity) buckets ---")
bucket_mot = {"top33": [], "mid33": [], "bot33": []}
for date in dates:
    dr = [r for r in records if r["date"] == date and r.get("main_over_turnover") is not None]
    if len(dr) < 6: continue
    dr_s = sorted(dr, key=lambda x: x["main_over_turnover"])
    n = len(dr_s)
    t = n // 3
    for i, r in enumerate(dr_s):
        if i < t: bucket_mot["bot33"].append(r["excess"])
        elif i < 2*t: bucket_mot["mid33"].append(r["excess"])
        else: bucket_mot["top33"].append(r["excess"])

for bk, excs in bucket_mot.items():
    print(f"  {bk}: n={len(excs)} mean_excess={mean(excs):.3f}")

# ---------- auction_change_pct sign bucket ----------
print("\n--- auction_change_pct sign bucket ---")
gap_pos = [r["excess"] for r in records if r.get("auction_change_pct") is not None and r["auction_change_pct"] > 0]
gap_neg = [r["excess"] for r in records if r.get("auction_change_pct") is not None and r["auction_change_pct"] < 0]
gap_zero = [r["excess"] for r in records if r.get("auction_change_pct") is not None and r["auction_change_pct"] == 0]
print(f"  gap_pos: n={len(gap_pos)} mean={mean(gap_pos):.3f}" if gap_pos else "  gap_pos: 0")
print(f"  gap_neg: n={len(gap_neg)} mean={mean(gap_neg):.3f}" if gap_neg else "  gap_neg: 0")
print(f"  gap_zero: n={len(gap_zero)} mean={mean(gap_zero):.3f}" if gap_zero else "  gap_zero: 0")

# ---------- market cap bucket ----------
print("\n--- market_cap_yi buckets ---")
bucket_mktcap = {"large(>500yi)": [], "mid(100-500)": [], "small(<100yi)": []}
for r in records:
    mc = r.get("market_cap_yi")
    if mc is None: continue
    exc = r["excess"]
    if mc >= 500: bucket_mktcap["large(>500yi)"].append(exc)
    elif mc >= 100: bucket_mktcap["mid(100-500)"].append(exc)
    else: bucket_mktcap["small(<100yi)"].append(exc)

for bk, excs in bucket_mktcap.items():
    print(f"  {bk}: n={len(excs)} mean={mean(excs):.3f}" if excs else f"  {bk}: 0")

# ---------- interaction: auction_turnover x main_over_turnover ----------
print("\n--- Interaction: auction_turnover_wan (H/L) x main_over_turnover (H/L) ---")
for date in dates:
    dr = [r for r in records if r["date"] == date
          and r.get("auction_turnover_wan") is not None
          and r.get("main_over_turnover") is not None]
    if len(dr) < 8: continue
    t_med = sorted(r["auction_turnover_wan"] for r in dr)[len(dr)//2]
    m_med = sorted(r["main_over_turnover"] for r in dr)[len(dr)//2]
    for r in dr:
        th = r["auction_turnover_wan"] >= t_med
        mh = r["main_over_turnover"] >= m_med
        if "HH" not in bucket_excess: bucket_excess = {}
        key = ("HH" if th and mh else "HL" if th and not mh
               else "LH" if not th and mh else "LL")
        r["_quad"] = key

quad = {"HH": [], "HL": [], "LH": [], "LL": []}
for r in records:
    if "_quad" in r:
        quad[r["_quad"]].append(r["excess"])

for q, excs in quad.items():
    print(f"  {q}: n={len(excs)} mean={mean(excs):.3f}" if excs else f"  {q}: 0")

# ---------- overlap with weimai and qiangchou universes ----------
print("\n--- Universe stats per date ---")
for date in dates:
    n_na = len([r for r in records if r["date"] == date])
    print(f"  {date}: n={n_na}")

print("\n[DONE]")
