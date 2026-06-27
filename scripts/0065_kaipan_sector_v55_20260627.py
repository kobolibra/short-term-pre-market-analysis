#!/usr/bin/env python3
"""
Job 0065 - 开盘板块汇总表 home.kaipan.plate.summary v55
修复: Daily(root) + excess(code, date_str)
表⑥: 题材join到股票级 IC+分桶
"""
import json, os, sys
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, spearman

PREOPEN = "093000"
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
    return str(code).split(".")[0][-6:].zfill(6)

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
dates_with_kaipan = 0

for dd in date_dirs:
    date_str = dd.name
    # --- load kaipan sector table ---
    kaipan_rows = load_rows(dd, "home.kaipan.plate.summary")
    if not kaipan_rows: continue
    dates_with_kaipan += 1

    # Build sector lookup by name
    sector_map = {}
    strengths = [pnum(r.get("\u677f\u5757\u5f3a\u5ea6\u539f\u503c")) or pnum(r.get("\u677f\u5757\u5f3a\u5ea6")) for r in kaipan_rows]
    max_s = max((s for s in strengths if s is not None), default=1)
    for r in kaipan_rows:
        name = r.get("\u4e3b\u6807\u7b7e\u540d\u79f0", "")
        if not name: continue
        raw_s = pnum(r.get("\u677f\u5757\u5f3a\u5ea6\u539f\u503c")) or pnum(r.get("\u677f\u5757\u5f3a\u5ea6"))
        inflow = pnum(r.get("\u4e3b\u529b\u6d41\u5165\u539f\u503c"))
        zt = pnum(r.get("\u6da8\u505c\u6570\u91cf"))
        rank = r.get("\u4e3b\u6807\u7b7e\u5e8f\u53f7", 99)
        sector_map[name] = {
            "rank": int(rank) if rank else 99,
            "strength_norm": raw_s / max_s if raw_s and max_s > 0 else 0.0,
            "inflow_wan": inflow,
            "zt_count": int(zt or 0),
            "has_zt": 1 if (zt or 0) > 0 else 0,
            "inflow_pos": 1 if (inflow or 0) > 0 else 0,
        }

    # --- load stock-level sources ---
    for dsid_stock in ["auction.jjyd.qiangchou", "auction.jjyd.net_amount", "auction.jjyd.weimai"]:
        stock_rows = load_rows(dd, dsid_stock)
        for row in stock_rows:
            code = code_of(row)
            if not code: continue
            exc = daily.excess(code, date_str)
            if exc is None: continue

            c1 = row.get("concept_1", "") or ""
            c2 = row.get("concept_2", "") or ""
            concepts = [c for c in [c1, c2] if c]

            best_rank = 99; best_sec = None
            for c in concepts:
                if c in sector_map and sector_map[c]["rank"] < best_rank:
                    best_rank = sector_map[c]["rank"]
                    best_sec = sector_map[c]

            records.append({
                "date": date_str, "code": code, "excess": exc,
                "source": dsid_stock.split(".")[-1],
                "sector_rank": best_sec["rank"] if best_sec else None,
                "sector_strength_norm": best_sec["strength_norm"] if best_sec else None,
                "sector_inflow_wan": best_sec["inflow_wan"] if best_sec else None,
                "sector_zt_count": best_sec["zt_count"] if best_sec else None,
                "sector_has_zt": best_sec["has_zt"] if best_sec else None,
                "sector_inflow_pos": best_sec["inflow_pos"] if best_sec else None,
                "in_top1": 1 if best_sec and best_sec["rank"] == 1 else 0,
                "in_top3": 1 if best_sec and best_sec["rank"] <= 3 else 0,
                "in_top5": 1 if best_sec and best_sec["rank"] <= 5 else 0,
                "in_top10": 1 if best_sec and best_sec["rank"] <= 10 else 0,
            })

print(f"Dates with kaipan: {dates_with_kaipan}")
print(f"Total records: {len(records)}")
dates = sorted(set(r["date"] for r in records))
print(f"Dates: {dates}")

# --- join hit rates ---
print("\n--- Join hit rates ---")
for src in ["qiangchou", "net_amount", "weimai"]:
    sr = [r for r in records if r["source"] == src]
    hit = sum(1 for r in sr if r["sector_rank"] is not None and r["sector_rank"] < 99)
    if sr: print(f"  {src}: {len(sr)} rows, join_hit={hit} ({hit/len(sr)*100:.1f}%)")

# --- top-N membership (deduped by code+date, best sector) ---
def dedup(recs):
    seen = {}
    for r in recs:
        key = (r["date"], r["code"])
        if key not in seen or (r["sector_rank"] or 99) < (seen[key]["sector_rank"] or 99):
            seen[key] = r
    return list(seen.values())

all_dd = dedup(records)
print(f"\n--- Deduped total: {len(all_dd)} ---")
for label, flt in [
    ("in_top1", lambda r: r["in_top1"]==1),
    ("in_top3", lambda r: r["in_top3"]==1),
    ("in_top5", lambda r: r["in_top5"]==1),
    ("in_top10", lambda r: r["in_top10"]==1),
    ("no_match", lambda r: r["sector_rank"] is None or r["sector_rank"] >= 99),
]:
    sub = [r["excess"] for r in all_dd if flt(r)]
    print(f"  {label}: n={len(sub)} mean={mean(sub):.3f}")

# --- per-date IC ---
FEATURES = ["sector_rank", "sector_strength_norm", "sector_inflow_wan", "sector_zt_count", "in_top1", "in_top3", "in_top5"]
print("\n--- Per-date IC (deduped) ---")
ic_by = {f: [] for f in FEATURES}
for date in dates:
    dr = dedup([r for r in records if r["date"] == date])
    if len(dr) < 8: continue
    for f in FEATURES:
        pairs = [(r[f], r["excess"]) for r in dr if r.get(f) is not None]
        if len(pairs) < 5: continue
        xs, ys = zip(*pairs)
        ic_by[f].append(spearman(list(xs), list(ys)))

print(f"{'Feature':<28} {'n':>4} {'mean_IC':>9} {'ICIR':>8}")
for f in FEATURES:
    ics = ic_by[f]
    if not ics: print(f"  {f:<26} 0"); continue
    m = mean(ics); s = std(ics)
    icir = m / s if s > 0 else 0.0
    print(f"  {f:<26} {len(ics):>4} {m:>9.3f} {icir:>8.3f}")

# --- sector_has_zt x inflow_pos quadrant ---
print("\n--- sector_has_zt x inflow_pos (qiangchou, deduped) ---")
qiangchou_dd = dedup([r for r in records if r["source"] == "qiangchou"])
quad = {"zt+_pos+": [], "zt+_pos-": [], "zt0_pos+": [], "zt0_pos-": [], "no_match": []}
for r in qiangchou_dd:
    if r["sector_rank"] is None or r["sector_rank"] >= 99:
        quad["no_match"].append(r["excess"])
    elif r["sector_has_zt"] == 1 and r["sector_inflow_pos"] == 1:
        quad["zt+_pos+"].append(r["excess"])
    elif r["sector_has_zt"] == 1 and r["sector_inflow_pos"] == 0:
        quad["zt+_pos-"].append(r["excess"])
    elif r["sector_has_zt"] == 0 and r["sector_inflow_pos"] == 1:
        quad["zt0_pos+"].append(r["excess"])
    else:
        quad["zt0_pos-"].append(r["excess"])
for k, v in quad.items():
    print(f"  {k}: n={len(v)} mean={mean(v):.3f}" if v else f"  {k}: 0")

# --- sector rank bucket (qiangchou) ---
print("\n--- sector rank bucket (qiangchou) ---")
bkt = {"rank1": [], "rank2-5": [], "rank6-10": [], "no_match": []}
for r in qiangchou_dd:
    rk = r["sector_rank"]
    if rk is None or rk >= 99: bkt["no_match"].append(r["excess"])
    elif rk == 1: bkt["rank1"].append(r["excess"])
    elif rk <= 5: bkt["rank2-5"].append(r["excess"])
    else: bkt["rank6-10"].append(r["excess"])
for k, v in bkt.items():
    print(f"  {k}: n={len(v)} mean={mean(v):.3f}" if v else f"  {k}: 0")

print("\n[DONE]")
