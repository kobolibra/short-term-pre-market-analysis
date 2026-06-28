#!/usr/bin/env python3
"""
Job 0074 - 热度榜(level) vs 飙升榜(delta) 交互信号验证 v64

0073 confirmed via dataset_label:
  rank.hot_stock_day = 热度榜（日） field=hot_stock_day  -> 热度绝对水平(level)
  rank.rocket        = 飙升榜        field=skyrocket_hour -> 小时飙升量(delta)
Both captured premarket ~0925. Only 5 fields: rank/code/name/value/raw_rate
(value = 万-formatted raw_rate). So the real info = rank(=raw_rate ordering).

Hypothesis (level vs delta combo):
  A. 飙升top10 & 热度top10  = 已霸榜主线龙头 (low elasticity?)
  B. 飙升top10 & NOT 热度top20 = 新晋边际资金 (low base, fast rising -> 妥股早期?)
  C. 热度top10 & NOT 飙升top10 = 高热度但无飙升 (滞涨大票?)
Also: per-day spearman(rank, excess) for each board; qiangchou overlap for surge.
Premarket files only (stem<=093000). No leakage.
"""
import json, os, sys
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, spearman

PREOPEN = "093000"
CAPTURES = PROJECT_ROOT / "captures"
HOT = "rank.hot_stock_day"
SURGE = "rank.rocket"
QIANG = "auction.jjyd.qiangchou"
QXLIVE = "home.qxlive.top_metrics"

def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs)/len(xs) if xs else None
def std(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2: return 0.0
    m = mean(xs); return (sum((x-m)**2 for x in xs)/len(xs))**0.5
def pnum(s):
    if s is None: return None
    if isinstance(s,(int,float)): return float(s)
    t=str(s).replace(",","").replace("%","").replace("+","").strip()
    if t in ("","--","-","null","None"): return None
    m=1.0
    if t.endswith("\u4ebf"): m=1e8; t=t[:-1]
    elif t.endswith("\u4e07"): m=1e4; t=t[:-1]
    elif t.lower().endswith("w"): m=1e4; t=t[:-1]
    try: return float(t)*m
    except: return None
def _norm(c): s=str(c).split(".")[0]; return s[-6:].zfill(6)

def premarket_rows(dd, dsid):
    p = dd/dsid
    if not p.exists(): return []
    files = sorted(f for f in p.iterdir() if f.suffix==".json" and f.stem<=PREOPEN)
    if not files: return []
    with open(files[-1]) as f: d=json.load(f)
    if isinstance(d,list): return d
    return d.get("rows",[]) or d.get("data",[]) or []

daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())

# QX regime
qx_by_date={}
for dd in date_dirs:
    for r in premarket_rows(dd, QXLIVE):
        if isinstance(r,dict) and r.get("metric_key")=="QX":
            v=pnum(r.get("raw_value")) or pnum(r.get("value"))
            if v is not None: qx_by_date[dd.name]=v
            break
qv=sorted(qx_by_date.values()); qx_med=qv[len(qv)//2] if qv else None

# qiangchou universe (premarket)
qiang_by_date={}
for dd in date_dirs:
    codes=set()
    for r in premarket_rows(dd, QIANG):
        if isinstance(r,dict) and r.get("code"): codes.add(_norm(r["code"]))
    if codes: qiang_by_date[dd.name]=codes

A=[];B=[];C=[]  # pooled demeaned excess
Ad={};Bd={};Cd={}  # per-day means
surge_sp=[]; hot_sp=[]
surge_inq=[]; surge_outq=[]
hot_lvl_top=[]; hot_lvl_bottom=[]
n_days=0

for dd in date_dirs:
    hot_rows=premarket_rows(dd,HOT); surge_rows=premarket_rows(dd,SURGE)
    if not hot_rows or not surge_rows: continue
    hot_rank={}; surge_rank={}
    for r in hot_rows:
        if isinstance(r,dict) and r.get("code") and r.get("rank") is not None:
            hot_rank[_norm(r["code"])]=r["rank"]
    for r in surge_rows:
        if isinstance(r,dict) and r.get("code") and r.get("rank") is not None:
            surge_rank[_norm(r["code"])]=r["rank"]
    uni=set(hot_rank)|set(surge_rank)
    exc={c: daily.excess(c,dd.name) for c in uni}
    exc={c:v for c,v in exc.items() if v is not None}
    if len(exc)<5: continue
    dm=mean(exc.values()); n_days+=1
    hot_top10={c for c,r in hot_rank.items() if r<=10}
    hot_top20={c for c,r in hot_rank.items() if r<=20}
    surge_top10={c for c,r in surge_rank.items() if r<=10}
    ga=[exc[c]-dm for c in (surge_top10 & hot_top10) if c in exc]
    gb=[exc[c]-dm for c in (surge_top10 - hot_top20) if c in exc]
    gc=[exc[c]-dm for c in (hot_top10 - surge_top10) if c in exc]
    A+=ga; B+=gb; C+=gc
    if ga: Ad[dd.name]=mean(ga)
    if gb: Bd[dd.name]=mean(gb)
    if gc: Cd[dd.name]=mean(gc)
    # rank-excess spearman within day
    sc=[(surge_rank[c],exc[c]) for c in surge_rank if c in exc]
    if len(sc)>=8:
        xs,ys=zip(*sc); sp=spearman(list(xs),list(ys))
        if sp is not None: surge_sp.append(sp)
    hc=[(hot_rank[c],exc[c]) for c in hot_rank if c in exc]
    if len(hc)>=8:
        xs,ys=zip(*hc); sp=spearman(list(xs),list(ys))
        if sp is not None: hot_sp.append(sp)
    # surge x qiangchou
    qc=qiang_by_date.get(dd.name,set())
    for c in surge_top10:
        if c in exc:
            (surge_inq if c in qc else surge_outq).append(exc[c]-dm)
    # hot level: top10 vs rank41-100
    for c,r in hot_rank.items():
        if c not in exc: continue
        if r<=10: hot_lvl_top.append(exc[c]-dm)
        elif r>=41: hot_lvl_bottom.append(exc[c]-dm)

def rep(name, pooled, perday):
    m=mean(pooled); s=std(pooled)
    pdm=list(perday.values())
    wr=100*sum(1 for x in pdm if x>0)/len(pdm) if pdm else 0
    dmean=mean(pdm); dstd=std(pdm)
    print("  {:42s} pooled_mean={} n={:4d} | perday_mean={} ICIR={} win={:.0f}% days={}".format(
        name,
        "{:.3f}".format(m) if m is not None else "NA", len(pooled),
        "{:.3f}".format(dmean) if dmean is not None else "NA",
        "{:.3f}".format(dmean/dstd) if dstd and dstd>0 else "NA", wr, len(pdm)))

print("="*60)
print("热度榜(level) x 飙升榜(delta)  premarket days:", n_days, "QX_med:", qx_med)
print("="*60)
print("\n-- group demeaned excess --")
rep("A: 飙升top10 & 热度top10 (霸榜龙头)", A, Ad)
rep("B: 飙升top10 & NOT 热度top20 (新晋边际)", B, Bd)
rep("C: 热度top10 & NOT 飙升top10 (滞涨大票)", C, Cd)

print("\n-- per-day spearman(rank, excess) [neg=>rank1 best] --")
print("  飙升榜: mean={} n={}".format("{:.3f}".format(mean(surge_sp)) if surge_sp else "NA", len(surge_sp)))
print("  热度榜: mean={} n={}".format("{:.3f}".format(mean(hot_sp)) if hot_sp else "NA", len(hot_sp)))

print("\n-- 飙升top10 x 抢筹 --")
print("  IN qiangchou : mean={} n={}".format("{:.3f}".format(mean(surge_inq)) if surge_inq else "NA", len(surge_inq)))
print("  NOT qiangchou: mean={} n={}".format("{:.3f}".format(mean(surge_outq)) if surge_outq else "NA", len(surge_outq)))

print("\n-- 热度榜绝对水平 level --")
print("  rank1-10  : mean={} n={}".format("{:.3f}".format(mean(hot_lvl_top)) if hot_lvl_top else "NA", len(hot_lvl_top)))
print("  rank41-100: mean={} n={}".format("{:.3f}".format(mean(hot_lvl_bottom)) if hot_lvl_bottom else "NA", len(hot_lvl_bottom)))

print("\n[DONE]")
