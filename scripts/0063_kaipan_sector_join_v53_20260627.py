#!/usr/bin/env python3
"""
Job 0063 - 开盘板块汇总表 home.kaipan.plate.summary v53
表⑥: 题材级join到股票级, 计算板块热度/流入/涨停数 对股票超额的IC
通过 qiangchou 表的 concept_1/concept_2 作为 join key
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
    s = str(code).split(".")[0]
    return s[-6:].zfill(6)

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

# ---------- load all dates ----------
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())
print(f"Total date dirs: {len(date_dirs)}")

records = []
dates_with_kaipan = 0
for dd in date_dirs:
    # --- load kaipan sector table ---
    kaipan_rows = load_rows(dd, "home.kaipan.plate.summary")
    if not kaipan_rows: continue
    dates_with_kaipan += 1

    # Build sector lookup: name -> {rank, strength, inflow, zt_count}
    sector_map = {}
    strengths = [pnum(r.get("\u677f\u5757\u5f3a\u5ea6\u539f\u503c")) or pnum(r.get("\u677f\u5757\u5f3a\u5ea6")) for r in kaipan_rows]
    max_s = max((s for s in strengths if s), default=1)
    for r in kaipan_rows:
        name = r.get("\u4e3b\u6807\u7b7e\u540d\u79f0", "")
        if not name: continue
        rank = r.get("\u4e3b\u6807\u7b7e\u5e8f\u53f7", 99)
        raw_s = pnum(r.get("\u677f\u5757\u5f3a\u5ea6\u539f\u503c")) or pnum(r.get("\u677f\u5757\u5f3a\u5ea6"))
        inflow = pnum(r.get("\u4e3b\u529b\u6d41\u5165\u539f\u503c"))
        zt = pnum(r.get("\u6da8\u505c\u6570\u91cf"))
        sector_map[name] = {
            "rank": rank,
            "strength_raw": raw_s,
            "strength_norm": raw_s / max_s if raw_s and max_s > 0 else 0,  # 0-1
            "inflow_wan": inflow,
            "zt_count": zt or 0,
            "has_zt": 1 if (zt or 0) > 0 else 0,
            "inflow_pos": 1 if (inflow or 0) > 0 else 0,
        }

    # --- load qiangchou (stock-level, has concept_1/2) ---
    d_obj = Daily(str(PROJECT_ROOT), dd.name)

    for dsid_stock in ["auction.jjyd.qiangchou", "auction.jjyd.net_amount", "auction.jjyd.weimai"]:
        stock_rows = load_rows(dd, dsid_stock)
        for row in stock_rows:
            code = code_of(row)
            if not code: continue
            exc = d_obj.excess.get(code)
            if exc is None: continue

            c1 = row.get("concept_1", "") or ""
            c2 = row.get("concept_2", "") or ""
            concepts = [c for c in [c1, c2] if c]

            # Find best (lowest rank = highest) sector match
            best_rank = 99
            best_sec = None
            for c in concepts:
                if c in sector_map and sector_map[c]["rank"] < best_rank:
                    best_rank = sector_map[c]["rank"]
                    best_sec = sector_map[c]

            r_out = {
                "date": dd.name,
                "code": code,
                "excess": exc,
                "source": dsid_stock.split(".")[-1],
                "sector_rank": best_rank if best_sec else None,
                "sector_strength_norm": best_sec["strength_norm"] if best_sec else None,
                "sector_inflow_wan": best_sec["inflow_wan"] if best_sec else None,
                "sector_zt_count": best_sec["zt_count"] if best_sec else None,
                "sector_has_zt": best_sec["has_zt"] if best_sec else None,
                "sector_inflow_pos": best_sec["inflow_pos"] if best_sec else None,
                "in_top1": 1 if best_rank == 1 else 0,
                "in_top3": 1 if best_rank <= 3 else 0,
                "in_top5": 1 if best_rank <= 5 else 0,
                "in_top10": 1 if best_rank <= 10 else 0,
            }
            records.append(r_out)

print(f"Dates with kaipan: {dates_with_kaipan}")
print(f"Total records (joined): {len(records)}")
dates = sorted(set(r["date"] for r in records))
print(f"Dates: {dates}")

# --- coverage ---
print("\n--- Join hit rate ---")
for src in ["qiangchou", "net_amount", "weimai"]:
    sr = [r for r in records if r["source"] == src]
    hit = sum(1 for r in sr if r["sector_rank"] is not None and r["sector_rank"] < 99)
    print(f"  {src}: total={len(sr)} join_hit={hit} ({hit/len(sr)*100:.1f}%)" if sr else f"  {src}: 0")

print("\n--- Top-N membership ---")
top1 = [r for r in records if r["in_top1"] == 1]
top3 = [r for r in records if r["in_top3"] == 1]
top10 = [r for r in records if r["in_top10"] == 1]
none_ = [r for r in records if r["sector_rank"] == 99 or r["sector_rank"] is None]
print(f"  in_top1: n={len(top1)} mean_excess={mean(r['excess'] for r in top1):.3f}")
print(f"  in_top3: n={len(top3)} mean_excess={mean(r['excess'] for r in top3):.3f}")
print(f"  in_top10: n={len(top10)} mean_excess={mean(r['excess'] for r in top10):.3f}")
print(f"  not_matched: n={len(none_)} mean_excess={mean(r['excess'] for r in none_):.3f}")

# --- per-date IC of sector features ---
FEATURES = ["sector_rank", "sector_strength_norm", "sector_inflow_wan",
             "sector_zt_count", "in_top1", "in_top3", "in_top5"]
print("\n--- Per-date Spearman IC (all sources combined, deduped by code) ---")
ic_by = {f: [] for f in FEATURES}
for date in dates:
    # Dedup by code: keep best (lowest sector_rank) per stock per date
    seen = {}
    for r in records:
        if r["date"] != date: continue
        code = r["code"]
        if code not in seen or (r["sector_rank"] or 99) < (seen[code]["sector_rank"] or 99):
            seen[code] = r
    dr = list(seen.values())
    if len(dr) < 8: continue
    for f in FEATURES:
        pairs = [(r[f], r["excess"]) for r in dr if r.get(f) is not None]
        if len(pairs) < 5: continue
        xs, ys = zip(*pairs)
        ic_by[f].append(spearman(list(xs), list(ys)))

print(f"{'Feature':<28} {'n':>4} {'mean_IC':>9} {'ICIR':>8}")
for f in FEATURES:
    ics = ic_by[f]
    if not ics:
        print(f"  {f:<26} {'0':>4}")
        continue
    m = mean(ics)
    s = std(ics)
    icir = m / s if s > 0 else 0.0
    print(f"  {f:<26} {len(ics):>4} {m:>9.3f} {icir:>8.3f}")

# --- sector_has_zt x sector_inflow_pos interaction ---
print("\n--- sector_has_zt x sector_inflow_pos bucket (qiangchou, deduped) ---")
for date in dates:
    seen = {}
    for r in records:
        if r["date"] != date or r["source"] != "qiangchou": continue
        code = r["code"]
        if code not in seen or (r["sector_rank"] or 99) < (seen[code]["sector_rank"] or 99):
            seen[code] = r
quad = {"zt+_pos+": [], "zt+_pos-": [], "zt0_pos+": [], "zt0_pos-": [], "no_match": []}
for r in records:
    if r["source"] != "qiangchou": continue
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

# --- sector rank bucket (1 vs 2-5 vs 6-10 vs none) ---
print("\n--- sector rank bucket (qiangchou source) ---")
bkt = {"rank1": [], "rank2-5": [], "rank6-10": [], "no_match": []}
for r in records:
    if r["source"] != "qiangchou": continue
    rk = r["sector_rank"]
    if rk is None or rk >= 99: bkt["no_match"].append(r["excess"])
    elif rk == 1: bkt["rank1"].append(r["excess"])
    elif rk <= 5: bkt["rank2-5"].append(r["excess"])
    else: bkt["rank6-10"].append(r["excess"])
for k, v in bkt.items():
    print(f"  {k}: n={len(v)} mean={mean(v):.3f}" if v else f"  {k}: 0")

print("\n[DONE]")
