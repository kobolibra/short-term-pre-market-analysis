#!/usr/bin/env python3
"""
Job 0070 - hot/surge/rocket 信号价值深挖 v60
两个目的:
1. 枚举服务器上所有数据集id(确认 'surge' 到底是哪张表)
2. 对 hot/surge/rocket 相关表做 **情绪 regime 条件化** 分析:
   假设: 这些表无条件 IC≈0, 但可能依赖市场情绪 regime
   (表⑧发现: 情绪逆向。冷清日低位反包有效? 高潮日退潮失效?)
3. 修正 value 字段 'w'(万) 解析; raw_rate 100%覆盖为主
"""
import json, os, sys
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, spearman

PREOPEN = "093000"
CAPTURES = PROJECT_ROOT / "captures"
QIANG = "auction.jjyd.qiangchou"
QXLIVE = "home.qxlive.top_metrics"

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
    mult = 1.0
    if s2.endswith("\u4ebf"): mult = 1e8; s2 = s2[:-1]
    elif s2.endswith("\u4e07"): mult = 1e4; s2 = s2[:-1]
    elif s2.endswith("w") or s2.endswith("W"): mult = 1e4; s2 = s2[:-1]
    try:
        return float(s2) * mult
    except: return None

def _norm(code):
    s = str(code).split(".")[0]
    return s[-6:].zfill(6)

def code_of(row):
    for k in ["code", "\u4ee3\u7801", "symbol"]:
        if k in row and row[k] not in (None, ""):
            return _norm(row[k])
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

# ============================================================
# STEP 0: enumerate ALL dataset ids across captures
# ============================================================
print("\n" + "="*60)
print("STEP 0: All dataset ids (union across dates)")
print("="*60)
all_ds = {}
for dd in date_dirs:
    for sub in dd.iterdir():
        if sub.is_dir():
            all_ds[sub.name] = all_ds.get(sub.name, 0) + 1
for ds in sorted(all_ds):
    print("  {:45s} present_in {:2d} dates".format(ds, all_ds[ds]))

targets = sorted(ds for ds in all_ds
                 if any(k in ds.lower() for k in ["hot", "surge", "rocket", "\u5f02\u52a8"]))
print("\nTarget tables (hot/surge/rocket):", targets)

# ============================================================
# STEP 1: per-day QX sentiment regime + qiangchou universe
# ============================================================
print("\n" + "="*60)
print("STEP 1: QX sentiment per date + qiangchou universe")
print("="*60)

qx_by_date = {}
for dd in date_dirs:
    rows = load_rows(dd, QXLIVE)
    for row in rows:
        if isinstance(row, dict) and row.get("metric_key") == "QX":
            v = pnum(row.get("raw_value")) or pnum(row.get("value"))
            if v is not None:
                qx_by_date[dd.name] = v
            break

qiang_codes_by_date = {}
for dd in date_dirs:
    rows = load_rows(dd, QIANG)
    codes = set()
    for row in rows:
        c = code_of(row)
        if c: codes.add(c)
    if codes:
        qiang_codes_by_date[dd.name] = codes

qx_vals = sorted(qx_by_date.values())
qx_median = qx_vals[len(qx_vals)//2] if qx_vals else None
print("QX dates:", len(qx_by_date), "median QX:", qx_median)
for d in sorted(qx_by_date):
    print("  {}: QX={:.0f} {}".format(d, qx_by_date[d],
        "HOT" if qx_median is not None and qx_by_date[d] >= qx_median else "cold"))

# ============================================================
# helper: load a target table -> per-date list of recs
# ============================================================
def load_target(dsid):
    by_date = {}
    val_cov = 0; rr_cov = 0; tot = 0
    for dd in date_dirs:
        rows = load_rows(dd, dsid)
        if not rows: continue
        recs = []
        for row in rows:
            if not isinstance(row, dict): continue
            code = code_of(row)
            if not code: continue
            exc = daily.excess(code, dd.name)
            if exc is None: continue
            rk = pnum(row.get("rank"))
            val = pnum(row.get("value"))
            rr = pnum(row.get("raw_rate"))
            tot += 1
            if val is not None: val_cov += 1
            if rr is not None: rr_cov += 1
            recs.append({"code": code, "rank": rk, "value": val, "raw_rate": rr, "excess": exc})
        if recs:
            by_date[dd.name] = recs
    return by_date, val_cov, rr_cov, tot

def topN_demeaned(recs, n, key):
    """demeaned excess of top-n rows by key (desc); fallback to rank asc if key=rank."""
    valid = [r for r in recs if r.get(key) is not None]
    if len(valid) < n: return None, 0
    day_mean = mean([r["excess"] for r in recs])
    if day_mean is None: return None, 0
    if key == "rank":
        ordered = sorted(valid, key=lambda r: r["rank"])  # rank 1 best
    else:
        ordered = sorted(valid, key=lambda r: -r[key])    # higher better
    top = ordered[:n]
    dm = mean([r["excess"] - day_mean for r in top])
    return dm, len(top)

# ============================================================
# STEP 2: per target table -> regime-conditional signal
# ============================================================
for dsid in targets:
    print("\n" + "="*60)
    print("TABLE:", dsid)
    print("="*60)
    by_date, val_cov, rr_cov, tot = load_target(dsid)
    print("Dates:", len(by_date), "| value_cov={}/{} raw_rate_cov={}/{}".format(val_cov, tot, rr_cov, tot))
    if not by_date:
        print("  no data"); continue

    # pick ranking key: prefer raw_rate(100%) else value else rank
    key = "raw_rate" if rr_cov > tot*0.5 else ("value" if val_cov > tot*0.5 else "rank")
    print("Ranking key:", key)

    # ---- unconditional top-N demeaned ----
    print("\n-- Unconditional top-N demeaned (per-date) --")
    for n in [3, 5, 10]:
        dms = []
        for date, recs in by_date.items():
            dm, k = topN_demeaned(recs, n, key)
            if dm is not None: dms.append(dm)
        if dms:
            m = mean(dms); s = std(dms)
            wr = 100*sum(1 for x in dms if x>0)/len(dms)
            print("  top{:<3d} mean_dm={:.3f} ICIR={:.3f} win={:.0f}% n_days={}".format(
                n, m, m/s if s>0 else 0, wr, len(dms)))

    # ---- regime-conditional top-N demeaned (split by QX) ----
    print("\n-- Regime-conditional (QX HOT vs cold) top-N demeaned --")
    for n in [3, 5, 10]:
        hot_dms = []; cold_dms = []
        for date, recs in by_date.items():
            if date not in qx_by_date or qx_median is None: continue
            dm, k = topN_demeaned(recs, n, key)
            if dm is None: continue
            if qx_by_date[date] >= qx_median: hot_dms.append(dm)
            else: cold_dms.append(dm)
        hm = mean(hot_dms); cm = mean(cold_dms)
        print("  top{:<3d} HOT_regime mean_dm={} (n={})  cold_regime mean_dm={} (n={})".format(
            n,
            "{:.3f}".format(hm) if hm is not None else "NA", len(hot_dms),
            "{:.3f}".format(cm) if cm is not None else "NA", len(cold_dms)))

    # ---- continuous: corr(top10 daily demeaned, QX) ----
    pairs = []
    for date, recs in by_date.items():
        if date not in qx_by_date: continue
        dm, k = topN_demeaned(recs, 10, key)
        if dm is not None: pairs.append((qx_by_date[date], dm))
    if len(pairs) >= 6:
        xs, ys = zip(*pairs)
        ic = spearman(list(xs), list(ys))
        print("\n  corr(QX, top10_daily_demeaned) = {} (n={})  [neg => picks work on COLD days]".format(
            "{:.3f}".format(ic) if ic is not None else "NA", len(pairs)))

    # ---- overlap with qiangchou: do top picks that ALSO grab-chips do better? ----
    print("\n-- top10 overlap with qiangchou universe --")
    in_q = []; out_q = []
    for date, recs in by_date.items():
        qc = qiang_codes_by_date.get(date, set())
        valid = [r for r in recs if r.get(key) is not None]
        if len(valid) < 10: continue
        day_mean = mean([r["excess"] for r in recs])
        if day_mean is None: continue
        ordered = sorted(valid, key=lambda r: r["rank"]) if key=="rank" else sorted(valid, key=lambda r: -r[key])
        top = ordered[:10]
        for r in top:
            dm = r["excess"] - day_mean
            (in_q if r["code"] in qc else out_q).append(dm)
    print("  top10 IN qiangchou : mean_dm={} n={}".format(
        "{:.3f}".format(mean(in_q)) if in_q else "NA", len(in_q)))
    print("  top10 NOT qiangchou: mean_dm={} n={}".format(
        "{:.3f}".format(mean(out_q)) if out_q else "NA", len(out_q)))

print("\n[DONE]")
