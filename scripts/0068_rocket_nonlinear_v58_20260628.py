#!/usr/bin/env python3
"""
Job 0068 - rocket 非线性混收 v58
背景: 线性 IC≈0 不代表无价值; 页层结构:
  top10: excess >> 均値; r11-30: excess << 均値(陷阱区); r31+: 中性
目标:
  1. per-date 去均值 top10 binary IC
  2. r11-30 危险区确认
  3. top10 连续胜率(n_days top10 demeaned>0)
  4. rocket top10 与 qiangchou 重叠分析
  5. rocket top10 与 weimai 主力净额交互
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
    return d.get("rows", [])

daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAPTURES.iterdir() if p.is_dir())
print("Total date dirs:", len(date_dirs))

# ---------- load all data ----------
rocket_by_date = {}   # date -> list of {code, rank, raw_rate, excess}
qiang_by_date = {}    # date -> set of codes
weimai_by_date = {}   # date -> {code: main_net_over_turnover}

for dd in date_dirs:
    date_str = dd.name

    # rocket
    rrows = load_rows(dd, "rank.rocket")
    recs = []
    for row in rrows:
        code = code_of(row)
        if not code: continue
        exc = daily.excess(code, date_str)
        if exc is None: continue
        rk = pnum(row.get("rank"))
        rr = pnum(row.get("raw_rate"))
        recs.append({"code": code, "rank": rk, "raw_rate": rr, "excess": exc})
    if recs:
        rocket_by_date[date_str] = recs

    # qiangchou codes
    qrows = load_rows(dd, "auction.jjyd.qiangchou")
    qcodes = set()
    for row in qrows:
        c = code_of(row)
        if c: qcodes.add(c)
    if qcodes:
        qiang_by_date[date_str] = qcodes

    # weimai main_net_over_turnover
    wrows = load_rows(dd, "auction.jjyd.weimai")
    wmap = {}
    for row in wrows:
        c = code_of(row)
        if not c: continue
        main = pnum(row.get("main_net_inflow_wan") or row.get("main_net_inflow"))
        turn = pnum(row.get("auction_turnover_wan"))
        if main is not None and turn and turn != 0:
            wmap[c] = main / turn
    if wmap:
        weimai_by_date[date_str] = wmap

dates = sorted(rocket_by_date.keys())
print("Rocket dates:", len(dates), "->", dates)

# ============================================================
# 1. Per-date demeaned binary IC: top10 vs rest
# ============================================================
print("\n" + "="*60)
print("1. Per-date demeaned: top10 / r11_30 / r31plus")
print("="*60)

demeaned_top10 = []
demeaned_r11_30 = []
demeaned_r31plus = []
binary_ic_list = []   # per-day spearman(in_top10_binary, excess)

for date in dates:
    recs = rocket_by_date[date]
    recs_with_rank = [r for r in recs if r.get("rank") is not None]
    if len(recs_with_rank) < 10: continue
    day_mean = mean([r["excess"] for r in recs_with_rank])
    if day_mean is None: continue

    top10 = [r for r in recs_with_rank if r["rank"] <= 10]
    r11_30 = [r for r in recs_with_rank if 10 < r["rank"] <= 30]
    r31plus = [r for r in recs_with_rank if r["rank"] > 30]

    t10_dm = mean([r["excess"] - day_mean for r in top10])
    r11_dm = mean([r["excess"] - day_mean for r in r11_30])
    r31_dm = mean([r["excess"] - day_mean for r in r31plus])

    if t10_dm is not None: demeaned_top10.append(t10_dm)
    if r11_dm is not None: demeaned_r11_30.append(r11_dm)
    if r31_dm is not None: demeaned_r31plus.append(r31_dm)

    # binary IC: in_top10 (1/0)
    binary = [1 if r["rank"] <= 10 else 0 for r in recs_with_rank]
    excesses = [r["excess"] for r in recs_with_rank]
    if len(binary) >= 8:
        binary_ic_list.append(spearman(binary, excesses))

    print("  {} n={:3d} day_mean={:.3f} top10_dm={:.3f}(n={}) r11_30_dm={:.3f}(n={}) r31+_dm={:.3f}(n={})".format(
        date, len(recs_with_rank), day_mean,
        t10_dm if t10_dm is not None else 0, len(top10),
        r11_dm if r11_dm is not None else 0, len(r11_30),
        r31_dm if r31_dm is not None else 0, len(r31plus)
    ))

print("\n--- Summary demeaned means ---")
print("  top10  : mean_dm={:.3f} std={:.3f} n_days={} win_rate={:.1f}%".format(
    mean(demeaned_top10) or 0, std(demeaned_top10),
    len(demeaned_top10),
    100 * sum(1 for x in demeaned_top10 if x > 0) / len(demeaned_top10) if demeaned_top10 else 0
))
print("  r11_30 : mean_dm={:.3f} std={:.3f} n_days={} win_rate={:.1f}%".format(
    mean(demeaned_r11_30) or 0, std(demeaned_r11_30),
    len(demeaned_r11_30),
    100 * sum(1 for x in demeaned_r11_30 if x > 0) / len(demeaned_r11_30) if demeaned_r11_30 else 0
))
print("  r31plus: mean_dm={:.3f} std={:.3f} n_days={} win_rate={:.1f}%".format(
    mean(demeaned_r31plus) or 0, std(demeaned_r31plus),
    len(demeaned_r31plus),
    100 * sum(1 for x in demeaned_r31plus if x > 0) / len(demeaned_r31plus) if demeaned_r31plus else 0
))

m_bin = mean(binary_ic_list)
s_bin = std(binary_ic_list)
icir_bin = m_bin / s_bin if s_bin and s_bin > 0 else 0
print("\n  Binary IC (in_top10): mean={:.3f} ICIR={:.3f} n={}".format(
    m_bin or 0, icir_bin, len(binary_ic_list)))

# ============================================================
# 2. Top5 and Top3 thresholds
# ============================================================
print("\n" + "="*60)
print("2. Tighter thresholds: top3 / top5 / top10 / top20 demeaned")
print("="*60)

for topN in [3, 5, 10, 20]:
    dms = []
    for date in dates:
        recs = rocket_by_date[date]
        recs_r = [r for r in recs if r.get("rank") is not None]
        if len(recs_r) < topN: continue
        day_mean = mean([r["excess"] for r in recs_r])
        if day_mean is None: continue
        topN_recs = [r for r in recs_r if r["rank"] <= topN]
        dm = mean([r["excess"] - day_mean for r in topN_recs])
        if dm is not None: dms.append(dm)
    if dms:
        wr = 100 * sum(1 for x in dms if x > 0) / len(dms)
        print("  top{:<3d}: mean_dm={:.3f} ICIR={:.3f} win_rate={:.1f}% n_days={}".format(
            topN, mean(dms) or 0,
            (mean(dms) or 0) / std(dms) if std(dms) > 0 else 0,
            wr, len(dms)))

# ============================================================
# 3. Rocket top10 <-> qiangchou overlap
# ============================================================
print("\n" + "="*60)
print("3. Rocket top10 overlap with qiangchou")
print("="*60)

overlap_dms = []
non_overlap_dms = []

for date in dates:
    recs = rocket_by_date[date]
    recs_r = [r for r in recs if r.get("rank") is not None and r["rank"] <= 10]
    if not recs_r: continue
    day_mean = mean([r["excess"] for r in rocket_by_date[date]])
    if day_mean is None: continue
    qcodes = qiang_by_date.get(date, set())
    olap = [r for r in recs_r if r["code"] in qcodes]
    nolap = [r for r in recs_r if r["code"] not in qcodes]
    n_olap = len(olap); n_nolap = len(nolap)
    if n_olap > 0:
        dm_olap = mean([r["excess"] - day_mean for r in olap])
        if dm_olap is not None: overlap_dms.append(dm_olap)
    if n_nolap > 0:
        dm_nolap = mean([r["excess"] - day_mean for r in nolap])
        if dm_nolap is not None: non_overlap_dms.append(dm_nolap)
    print("  {} top10={} overlap_qiang={} non_overlap={}".format(
        date, len(recs_r), n_olap, n_nolap))

if overlap_dms:
    print("\n  rocket_top10 IN qiangchou  : mean_dm={:.3f} n_days={}".format(
        mean(overlap_dms) or 0, len(overlap_dms)))
if non_overlap_dms:
    print("  rocket_top10 NOT qiangchou : mean_dm={:.3f} n_days={}".format(
        mean(non_overlap_dms) or 0, len(non_overlap_dms)))

# ============================================================
# 4. Rocket top10 x weimai main_net direction
# ============================================================
print("\n" + "="*60)
print("4. Rocket top10 x weimai main_net direction")
print("="*60)

both_pos_dms = []
both_neg_dms = []
rocket_pos_weimai_neg = []

for date in dates:
    recs = rocket_by_date[date]
    recs_r = [r for r in recs if r.get("rank") is not None and r["rank"] <= 10]
    if not recs_r: continue
    day_mean = mean([r["excess"] for r in rocket_by_date[date]])
    if day_mean is None: continue
    wmap = weimai_by_date.get(date, {})
    for r in recs_r:
        c = r["code"]
        mot = wmap.get(c)
        dm = r["excess"] - day_mean
        if mot is None: continue
        if mot > 0: both_pos_dms.append(dm)
        else: rocket_pos_weimai_neg.append(dm)

if both_pos_dms:
    print("  rocket_top10 + weimai_main_pos: mean_dm={:.3f} n={}".format(
        mean(both_pos_dms) or 0, len(both_pos_dms)))
if rocket_pos_weimai_neg:
    print("  rocket_top10 + weimai_main_neg: mean_dm={:.3f} n={}".format(
        mean(rocket_pos_weimai_neg) or 0, len(rocket_pos_weimai_neg)))

# ============================================================
# 5. r11-30 danger zone confirmation
# ============================================================
print("\n" + "="*60)
print("5. r11-30 danger zone per-day confirmation")
print("="*60)

for date in dates:
    recs = rocket_by_date[date]
    recs_r = [r for r in recs if r.get("rank") is not None]
    if not recs_r: continue
    day_mean = mean([r["excess"] for r in recs_r])
    if day_mean is None: continue
    r11_30 = [r for r in recs_r if 10 < r["rank"] <= 30]
    if not r11_30: continue
    dm = mean([r["excess"] - day_mean for r in r11_30])
    print("  {} r11_30_dm={:.3f} n={}".format(date, dm if dm is not None else 0, len(r11_30)))

print("\n[DONE]")
