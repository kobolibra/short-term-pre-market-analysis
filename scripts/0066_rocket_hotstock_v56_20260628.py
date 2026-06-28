#!/usr/bin/env python3
"""
Job 0066 - rank.rocket + rank.hot_stock_day 深挖 v56
修复泄漏: 只用 HHMMSS <= 093000 的文件; files[-1] 旧写法会取盘后数据
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
    rows = d.get("rows", [])
    # also try list directly
    if not rows and isinstance(d, list): rows = d
    return rows

def analyze_dataset(dsid, daily, date_dirs):
    print("\n" + "="*60)
    print("Dataset:", dsid)
    print("="*60)

    records = []
    sample_keys = None
    for dd in date_dirs:
        rows = load_rows(dd, dsid)
        if not rows: continue
        date_str = dd.name
        if sample_keys is None:
            sample_keys = list(rows[0].keys()) if rows else []
            print("Sample keys:", sample_keys)
            print("First row:", {k: rows[0][k] for k in list(rows[0].keys())[:12]})
        for row in rows:
            code = code_of(row)
            if not code: continue
            exc = daily.excess(code, date_str)
            if exc is None: continue
            r = {"date": date_str, "code": code, "excess": exc}
            # try common field names
            r["rank"] = pnum(row.get("rank") or row.get("\u6392\u540d"))
            r["value"] = pnum(row.get("value") or row.get("score") or row.get("hot_score") or row.get("\u7b49\u7ea7") or row.get("\u8bc4\u5206"))
            r["change_pct"] = pnum(row.get("change_pct") or row.get("latest_change_pct") or row.get("\u6da8\u8dcc\u5e45") or row.get("\u6da8\u8dcc"))
            r["turnover"] = pnum(row.get("turnover_wan") or row.get("auction_turnover_wan") or row.get("\u6210\u4ea4\u989d"))
            r["turnover_rate"] = pnum(row.get("turnover_rate_pct") or row.get("turnover_rate") or row.get("\u6362\u624b\u7387"))
            # capture all numeric fields dynamically
            for k, v in row.items():
                if k not in ("code", "name", "\u4ee3\u7801", "\u540d\u79f0", "raw", "concept", "\u6982\u5ff5") and k not in r:
                    num = pnum(v)
                    if num is not None:
                        r["dyn_" + k] = num
            records.append(r)

    if not records:
        print("NO RECORDS - dataset not found or no premarket files")
        return

    print("Total records:", len(records))
    dates = sorted(set(r["date"] for r in records))
    print("Dates:", len(dates), "->", dates)

    # coverage
    CORE = ["rank", "value", "change_pct", "turnover", "turnover_rate"]
    dyn_keys = sorted(set(k for r in records for k in r if k.startswith("dyn_")))
    ALL_FIELDS = CORE + dyn_keys

    print("\n--- Coverage ---")
    for f in ALL_FIELDS:
        n_ok = sum(1 for r in records if r.get(f) is not None)
        if n_ok > 0:
            pct = n_ok / len(records) * 100
            print("  {:30s} {}/{} = {:.1f}%".format(f, n_ok, len(records), pct))

    # per-date IC
    print("\n--- Per-date Spearman IC ---")
    ic_by = {f: [] for f in ALL_FIELDS}
    for date in dates:
        dr = [r for r in records if r["date"] == date]
        for f in ALL_FIELDS:
            pairs = [(r[f], r["excess"]) for r in dr if r.get(f) is not None]
            if len(pairs) < 5: continue
            xs, ys = zip(*pairs)
            ic_by[f].append(spearman(list(xs), list(ys)))

    print("{:<30s} {:>4s} {:>9s} {:>8s}".format("Field", "n", "mean_IC", "ICIR"))
    for f in ALL_FIELDS:
        ics = ic_by[f]
        if not ics: continue
        m = mean(ics); s = std(ics)
        icir = m / s if s > 0 else 0.0
        print("  {:<28s} {:>4d} {:>9.3f} {:>8.3f}".format(f, len(ics), m, icir))

    # value/score tercile buckets
    fv = "value"
    has_v = sum(1 for r in records if r.get(fv) is not None)
    if has_v > 30:
        print("\n--- value/score tercile buckets (raw excess) ---")
        bkt = {"top": [], "mid": [], "bot": []}
        for date in dates:
            dr = sorted([r for r in records if r["date"] == date and r.get(fv) is not None], key=lambda x: x[fv])
            n = len(dr); t = n // 3
            if n < 6: continue
            for i, r in enumerate(dr):
                bkt["top" if i >= n-t else "bot" if i < t else "mid"].append(r["excess"])
        for k, v in bkt.items():
            print("  {}: n={} mean={:.3f}".format(k, len(v), mean(v)))

    # rank buckets (top10, 10-30, 30+)
    has_rank = sum(1 for r in records if r.get("rank") is not None)
    if has_rank > 30:
        print("\n--- rank buckets (raw excess) ---")
        bkt_r = {"top10": [], "r11_30": [], "r31plus": []}
        for r in records:
            rk = r.get("rank")
            if rk is None: continue
            if rk <= 10: bkt_r["top10"].append(r["excess"])
            elif rk <= 30: bkt_r["r11_30"].append(r["excess"])
            else: bkt_r["r31plus"].append(r["excess"])
        for k, v in bkt_r.items():
            print("  {}: n={} mean={:.3f}".format(k, len(v), mean(v)))

    # per-date universe size
    print("\n--- Per-date universe size ---")
    for date in dates:
        n = len([r for r in records if r["date"] == date])
        print("  {}: n={}".format(date, n))

# ---------- main ----------
daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())
print("Total date dirs:", len(date_dirs))

analyze_dataset("rank.rocket", daily, date_dirs)
analyze_dataset("rank.hot_stock_day", daily, date_dirs)

print("\n[DONE]")
