#!/usr/bin/env python3
"""
Job 0071 - pool.surge / pool.hot schema probe + flexible analysis v61

Why: 0070 found DISTINCT datasets pool.hot & pool.surge (present 17 dates each)
but load_target() returned 0 rows because code_of() found no code/symbol field
-> these tables use a different schema. This job:
  STEP A: dump raw structure (top-level type/keys + first 3 rows) of pool.hot,
          pool.surge for up to 3 dates each, so we learn the real field names.
  STEP B: flexible code detection (regex 6-digit) + flexible ranking-value
          detection, then run unconditional top-N demeaned + QX regime split
          + qiangchou overlap (same methodology as 0070).
"""
import json, os, sys, re
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, spearman

PREOPEN = "093000"
CAPTURES = PROJECT_ROOT / "captures"
QIANG = "auction.jjyd.qiangchou"
QXLIVE = "home.qxlive.top_metrics"
TARGETS = ["pool.surge", "pool.hot"]
CODE_RE = re.compile(r"\b(\d{6})\b")

def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs)/len(xs) if xs else None

def std(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2: return 0.0
    m = mean(xs)
    return (sum((x-m)**2 for x in xs)/len(xs))**0.5

def pnum(s):
    if s is None: return None
    if isinstance(s, (int, float)): return float(s)
    s2 = str(s).replace(",","").replace("%","").replace("+","").strip()
    if s2 in ("","--","-","null","None"): return None
    mult = 1.0
    if s2.endswith("\u4ebf"): mult = 1e8; s2 = s2[:-1]
    elif s2.endswith("\u4e07"): mult = 1e4; s2 = s2[:-1]
    elif s2.endswith("w") or s2.endswith("W"): mult = 1e4; s2 = s2[:-1]
    try: return float(s2)*mult
    except: return None

def _norm(code):
    s = str(code).split(".")[0]
    return s[-6:].zfill(6)

def raw_file(date_dir, dsid):
    p = date_dir / dsid
    if not p.exists(): return None
    files = sorted(f for f in p.iterdir() if f.suffix == ".json" and f.stem <= PREOPEN)
    if not files: return None
    with open(files[-1]) as f:
        return json.load(f)

def rows_of(d):
    if d is None: return []
    if isinstance(d, list): return d
    if isinstance(d, dict):
        for k in ["rows","data","list","items","result","stocks"]:
            v = d.get(k)
            if isinstance(v, list): return v
        # nested one level
        for v in d.values():
            if isinstance(v, dict):
                for k in ["rows","data","list","items"]:
                    if isinstance(v.get(k), list): return v[k]
    return []

def find_code(row):
    if not isinstance(row, dict): return None
    for k in ["code","\u4ee3\u7801","symbol","stock_code","ts_code","secid"]:
        if k in row and row[k] not in (None, ""):
            m = CODE_RE.search(str(row[k]))
            if m: return _norm(m.group(1))
    for v in row.values():
        if isinstance(v, str):
            m = CODE_RE.search(v)
            if m: return _norm(m.group(1))
    return None

def numeric_keys(rows):
    """keys whose values are mostly numeric (after pnum)."""
    from collections import defaultdict
    cov = defaultdict(int); tot = 0
    for r in rows:
        if not isinstance(r, dict): continue
        tot += 1
        for k,v in r.items():
            if pnum(v) is not None: cov[k] += 1
    return {k: cov[k]/tot for k in cov} if tot else {}

daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())

# ============================================================
# STEP A: raw schema dump
# ============================================================
for dsid in TARGETS:
    print("\n" + "="*60)
    print("SCHEMA DUMP:", dsid)
    print("="*60)
    shown = 0
    for dd in date_dirs:
        d = raw_file(dd, dsid)
        if d is None: continue
        print("\n[date {}] top-level type: {}".format(dd.name, type(d).__name__))
        if isinstance(d, dict):
            print("  top keys:", list(d.keys())[:20])
        rows = rows_of(d)
        print("  n_rows:", len(rows))
        for i, r in enumerate(rows[:3]):
            print("  row[{}]: {}".format(i, json.dumps(r, ensure_ascii=False)[:400]))
        nk = numeric_keys(rows)
        if nk:
            print("  numeric-ish keys (coverage):",
                  {k: round(v,2) for k,v in sorted(nk.items(), key=lambda x:-x[1])[:8]})
        shown += 1
        if shown >= 3: break
    if shown == 0:
        print("  NO FILES FOUND")

# ============================================================
# STEP B: QX regime + qiangchou universe
# ============================================================
qx_by_date = {}
for dd in date_dirs:
    d = raw_file(dd, QXLIVE)
    for row in rows_of(d):
        if isinstance(row, dict) and row.get("metric_key") == "QX":
            v = pnum(row.get("raw_value")) or pnum(row.get("value"))
            if v is not None: qx_by_date[dd.name] = v
            break
qx_vals = sorted(qx_by_date.values())
qx_median = qx_vals[len(qx_vals)//2] if qx_vals else None

qiang_by_date = {}
for dd in date_dirs:
    d = raw_file(dd, QIANG)
    codes = set()
    for row in rows_of(d):
        c = find_code(row)
        if c: codes.add(c)
    if codes: qiang_by_date[dd.name] = codes

print("\n" + "="*60)
print("STEP B: flexible-parse regime analysis")
print("QX dates:", len(qx_by_date), "median:", qx_median)
print("="*60)

def load_flex(dsid):
    by_date = {}; tot=0; code_cov=0
    # determine best ranking key once from a sample day
    rank_key = None
    for dd in date_dirs:
        rows = rows_of(raw_file(dd, dsid))
        if not rows: continue
        nk = numeric_keys(rows)
        cand = {k:v for k,v in nk.items() if v>0.8 and k.lower() not in ("rank","\u6392\u540d","index","id")}
        if cand:
            rank_key = max(cand, key=cand.get); break
    for dd in date_dirs:
        rows = rows_of(raw_file(dd, dsid))
        recs = []
        for ri, row in enumerate(rows):
            if not isinstance(row, dict): continue
            code = find_code(row); tot += 1
            if not code: continue
            code_cov += 1
            exc = daily.excess(code, dd.name)
            if exc is None: continue
            rv = pnum(row.get(rank_key)) if rank_key else None
            recs.append({"code":code, "rank":ri+1, "rv":rv, "excess":exc})
        if recs: by_date[dd.name] = recs
    return by_date, rank_key, tot, code_cov

def topN_dm(recs, n, use_rv):
    if use_rv:
        valid = [r for r in recs if r.get("rv") is not None]
        if len(valid) < n: return None
        ordered = sorted(valid, key=lambda r: -r["rv"])
    else:
        ordered = sorted(recs, key=lambda r: r["rank"])  # capture order = server rank
        if len(ordered) < n: return None
    dmn = mean([r["excess"] for r in recs])
    if dmn is None: return None
    return mean([r["excess"]-dmn for r in ordered[:n]])

for dsid in TARGETS:
    print("\n----- analysis:", dsid, "-----")
    by_date, rank_key, tot, code_cov = load_flex(dsid)
    print("dates:", len(by_date), "| code_cov={}/{}".format(code_cov, tot), "| rank_key:", rank_key)
    if not by_date:
        print("  still no usable rows"); continue
    use_rv = rank_key is not None
    print("  ranking by:", "value("+str(rank_key)+")" if use_rv else "capture order")
    for n in [3,5,10]:
        dms=[]
        for date,recs in by_date.items():
            x=topN_dm(recs,n,use_rv)
            if x is not None: dms.append(x)
        if dms:
            m=mean(dms); s=std(dms)
            wr=100*sum(1 for x in dms if x>0)/len(dms)
            print("  top{:<3d} mean_dm={:.3f} ICIR={:.3f} win={:.0f}% n={}".format(n,m,m/s if s>0 else 0,wr,len(dms)))
    # regime
    for n in [3,10]:
        hot=[];cold=[]
        for date,recs in by_date.items():
            if date not in qx_by_date or qx_median is None: continue
            x=topN_dm(recs,n,use_rv)
            if x is None: continue
            (hot if qx_by_date[date]>=qx_median else cold).append(x)
        hm=mean(hot);cm=mean(cold)
        print("  top{:<3d} HOT={} (n={}) cold={} (n={})".format(n,
            "{:.3f}".format(hm) if hm is not None else "NA",len(hot),
            "{:.3f}".format(cm) if cm is not None else "NA",len(cold)))
    # corr
    pairs=[]
    for date,recs in by_date.items():
        if date not in qx_by_date: continue
        x=topN_dm(recs,10,use_rv)
        if x is not None: pairs.append((qx_by_date[date],x))
    if len(pairs)>=6:
        xs,ys=zip(*pairs); ic=spearman(list(xs),list(ys))
        print("  corr(QX, top10_dm)={} (n={})".format("{:.3f}".format(ic) if ic is not None else "NA",len(pairs)))
    # qiangchou overlap
    inq=[];outq=[]
    for date,recs in by_date.items():
        qc=qiang_by_date.get(date,set())
        ordered=sorted([r for r in recs if r.get('rv') is not None],key=lambda r:-r['rv']) if use_rv else sorted(recs,key=lambda r:r['rank'])
        if len(ordered)<10: continue
        dmn=mean([r['excess'] for r in recs])
        if dmn is None: continue
        for r in ordered[:10]:
            (inq if r['code'] in qc else outq).append(r['excess']-dmn)
    print("  top10 IN qiangchou: mean_dm={} n={}".format("{:.3f}".format(mean(inq)) if inq else "NA",len(inq)))
    print("  top10 NOT qiangchou: mean_dm={} n={}".format("{:.3f}".format(mean(outq)) if outq else "NA",len(outq)))

print("\n[DONE]")
