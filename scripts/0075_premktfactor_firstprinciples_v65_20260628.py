#!/usr/bin/env python3
# 0075_premktfactor_firstprinciples_v65_20260628.py
# First-principles validation of 4 disputed premarket-factor premises.
#   Q1: is 竞价换手率 redundant vs 竞价成交额? (orthogonalized residual IC)
#   Q2: is gap non-linear? (binned demeaned-excess curve + turning point) + regime
#   Q3: sentiment-regime DIRECTION -- does core alpha work better on COLD days?
#        (production gate is currently MORE aggressive when hot -- is it backwards?)
#   Q4: small-cap effect robustness (winsorize excess / drop highest-dispersion days)
#
# Pure stdlib + v10_optimize. Data: captures/<date>/auction.jjyd.qiangchou/<=093000.json
# Fields: auction_turnover_wan(amt), turnover_rate_pct(turn), latest_change_pct(gap).
# QX regime: home.qxlive.top_metrics metric_key=='QX' raw_value, split by median.
import os, sys, json, glob, math, statistics, importlib

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

v10 = importlib.import_module("v10_optimize")
Daily = v10.Daily
spearman = v10.spearman
mean_icir = v10.mean_icir
rankdata = v10.rankdata
code_of = getattr(v10, "code_of", None)
DEFAULT_PROJECT_ROOT = getattr(v10, "DEFAULT_PROJECT_ROOT", None)

PROJECT_ROOT = DEFAULT_PROJECT_ROOT or os.path.dirname(HERE)
CAPTURES = os.path.join(PROJECT_ROOT, "captures")
QIANGCHOU = "auction.jjyd.qiangchou"
QXDS = "home.qxlive.top_metrics"
MIN_N = 12
PREMARKET_MAX = "093000"

D = Daily(PROJECT_ROOT)

# ---------------- helpers ----------------
def _num(x):
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return None if x != x else float(x)
    s = str(x).strip().replace(",", "")
    if not s or s.lower() in ("-", "--", "none", "nan", "null"):
        return None
    m = 1.0
    for suf, mul in (("亿", 1e4), ("万", 1.0), ("w", 1.0), ("W", 1.0)):
        if s.endswith(suf):
            s = s[:-len(suf)]; m = mul; break
    s = s.replace("%", "")
    try:
        return float(s) * m
    except Exception:
        return None

def _extract_rows(obj):
    if isinstance(obj, list):
        return [r for r in obj if isinstance(r, dict)]
    if isinstance(obj, dict):
        for k in ("data", "rows", "list", "items", "result", "stocks", "records"):
            v = obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
            if isinstance(v, dict):
                for kk in ("data", "rows", "list", "items"):
                    vv = v.get(kk)
                    if isinstance(vv, list):
                        return [r for r in vv if isinstance(r, dict)]
        vals = list(obj.values())
        if vals and all(isinstance(x, dict) for x in vals):
            return vals
    return []

def latest_premarket_file(d, dsid):
    ddir = os.path.join(CAPTURES, d, dsid)
    if not os.path.isdir(ddir):
        return None
    best = None; bestkey = None
    for p in glob.glob(os.path.join(ddir, "*.json")):
        stem = os.path.splitext(os.path.basename(p))[0]
        if stem <= PREMARKET_MAX:
            if bestkey is None or stem > bestkey:
                bestkey = stem; best = p
    return best

def load_rows(d, dsid):
    p = latest_premarket_file(d, dsid)
    if not p:
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        return []
    return _extract_rows(obj)

def row_code(r):
    for k in ("code", "stock_code", "symbol", "ts_code", "secucode", "stockcode"):
        v = r.get(k)
        if v:
            return str(v).strip()
    return None

def norm_code(c):
    if code_of:
        try:
            x = code_of(c)
            if x:
                return x
        except Exception:
            pass
    return c

def safe_excess(c, d):
    try:
        return D.excess(c, d)
    except Exception:
        return None

def ss(a, b):
    try:
        if len(a) < 3:
            return None
        v = spearman(a, b)
        if v is None or v != v:
            return None
        return float(v)
    except Exception:
        return None

def pearson(a, b):
    n = len(a)
    if n < 3:
        return None
    ma = statistics.fmean(a); mb = statistics.fmean(b)
    sa = sum((x - ma) ** 2 for x in a); sb = sum((y - mb) ** 2 for y in b)
    if sa <= 0 or sb <= 0:
        return None
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    return cov / math.sqrt(sa * sb)

def pctrank(vals):
    n = len(vals)
    if n == 0:
        return []
    if n == 1:
        return [0.5]
    r = rankdata(vals)
    return [(x - 1.0) / (n - 1.0) for x in r]

def zmap(vals):
    n = len(vals)
    if n == 0:
        return []
    m = statistics.fmean(vals)
    sd = statistics.pstdev(vals) if n > 1 else 0.0
    if sd <= 0:
        return [0.0] * n
    return [(x - m) / sd for x in vals]

def ols_resid(x, y):
    n = len(x)
    if n < 2:
        return [0.0] * n
    mx = statistics.fmean(x); my = statistics.fmean(y)
    vx = sum((xi - mx) ** 2 for xi in x)
    if vx <= 0:
        return [yi - my for yi in y]
    b = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / vx
    a = my - b * mx
    return [y[i] - (a + b * x[i]) for i in range(n)]

def winsor(vals, lo=2.0, hi=98.0):
    if not vals:
        return vals
    s = sorted(vals); n = len(s)
    def q(p):
        idx = p / 100.0 * (n - 1)
        i = int(idx); f = idx - i
        if i + 1 < n:
            return s[i] * (1 - f) + s[i + 1] * f
        return s[i]
    a = q(lo); b = q(hi)
    return [min(max(v, a), b) for v in vals]

def agg(daily_vals):
    vals = [v for v in daily_vals if v is not None and v == v]
    if not vals:
        return (None, None, 0)
    try:
        out = mean_icir(vals)
        if isinstance(out, (list, tuple)) and len(out) >= 2:
            return (out[0], out[1], len(vals))
    except Exception:
        pass
    m = statistics.fmean(vals)
    sd = statistics.pstdev(vals) if len(vals) > 1 else 0.0
    return (m, (m / sd if sd > 0 else None), len(vals))

def fmt(x, nd=4):
    return "None" if x is None else ("%.*f" % (nd, x))

def pa(t):
    if not t:
        return "n/a"
    m, ic, n = t
    return "mean=%s ICIR=%s n=%d" % (fmt(m), fmt(ic, 3), n)

def m0(t):
    return t[0] if t else None

# ---------------- assemble per-day data ----------------
dates = []
if os.path.isdir(CAPTURES):
    for d in sorted(os.listdir(CAPTURES)):
        if os.path.isdir(os.path.join(CAPTURES, d, QIANGCHOU)):
            dates.append(d)

per_day = {}
qx_by_date = {}
total_rows = 0
for d in dates:
    recs = []
    for r in load_rows(d, QIANGCHOU):
        c = row_code(r)
        if not c:
            continue
        amt = _num(r.get("auction_turnover_wan"))
        turn = _num(r.get("turnover_rate_pct"))
        gap = _num(r.get("latest_change_pct"))
        if amt is None or turn is None or gap is None:
            continue
        ex = safe_excess(norm_code(c), d)
        if ex is None or ex != ex:
            continue
        recs.append({"code": c, "amt": amt, "turn": turn, "gap": gap, "ex": float(ex)})
    if len(recs) >= MIN_N:
        per_day[d] = recs
        total_rows += len(recs)
    qx = None
    for rr in load_rows(d, QXDS):
        if str(rr.get("metric_key", "")).upper() == "QX":
            qx = _num(rr.get("raw_value", rr.get("value")))
            break
    if qx is not None:
        qx_by_date[d] = qx

days = sorted(per_day.keys())
DAY = {}
for d in days:
    recs = per_day[d]
    amt = [r["amt"] for r in recs]; turn = [r["turn"] for r in recs]
    gap = [r["gap"] for r in recs]; ex = [r["ex"] for r in recs]
    DAY[d] = {"amt": amt, "turn": turn, "gap": gap, "ex": ex,
              "ar": pctrank(amt), "tr": pctrank(turn), "gr": pctrank(gap),
              "n": len(recs)}

# regime split by QX median
qx_days = [d for d in days if d in qx_by_date]
hot_days = set(); cold_days = set()
if len(qx_days) >= 4:
    med = statistics.median([qx_by_date[d] for d in qx_days])
    for d in qx_days:
        (hot_days if qx_by_date[d] > med else cold_days).add(d)

# ---------------- Q1 turnover redundancy ----------------
def Q1():
    ic_amt = []; ic_turn = []; ic_tres = []; ic_ares = []; ic_comp = []; corr_at = []
    for d in days:
        x = DAY[d]; ex = x["ex"]; ar = x["ar"]; tr = x["tr"]
        ic_amt.append(ss(x["amt"], ex))
        ic_turn.append(ss(x["turn"], ex))
        corr_at.append(ss(x["amt"], x["turn"]))
        ic_tres.append(ss(ols_resid(ar, tr), ex))   # turn after removing amt
        ic_ares.append(ss(ols_resid(tr, ar), ex))   # amt after removing turn
        za = zmap(ar); zt = zmap(tr)
        ic_comp.append(ss([za[i] + zt[i] for i in range(len(ar))], ex))
    return {"ic_amt": agg(ic_amt), "ic_turn": agg(ic_turn),
            "ic_turn_resid": agg(ic_tres), "ic_amt_resid": agg(ic_ares),
            "ic_comp": agg(ic_comp), "corr_amt_turn": agg(corr_at)}

# ---------------- Q2 gap non-linearity ----------------
def Q2():
    NB = 5
    ic_gap = []
    bin_ex = {i: [] for i in range(NB)}
    for d in days:
        x = DAY[d]; ex = x["ex"]; gr = x["gr"]; n = x["n"]
        ic_gap.append(ss(x["gap"], ex))
        mex = statistics.fmean(ex)
        for i in range(n):
            b = min(int(gr[i] * NB), NB - 1)
            bin_ex[b].append(ex[i] - mex)
    bins = [(statistics.fmean(bin_ex[i]) if bin_ex[i] else None) for i in range(NB)]
    hump = mono = None
    if all(b is not None for b in bins):
        hump = bins[2] - (bins[0] + bins[4]) / 2.0
        mono = bins[4] - bins[0]
    ic_gap_hot = agg([ss(DAY[d]["gap"], DAY[d]["ex"]) for d in hot_days])
    ic_gap_cold = agg([ss(DAY[d]["gap"], DAY[d]["ex"]) for d in cold_days])
    return {"ic_gap": agg(ic_gap), "bins": bins, "hump": hump, "mono": mono,
            "bin_counts": [len(bin_ex[i]) for i in range(NB)],
            "ic_gap_hot": ic_gap_hot, "ic_gap_cold": ic_gap_cold}

# ---------------- Q3 sentiment-regime direction ----------------
def core_vec(x):
    za = zmap(x["ar"]); zt = zmap(x["tr"]); zg = zmap(x["gr"])
    return [za[i] + zt[i] + zg[i] for i in range(x["n"])]

def Q3():
    rows = []
    for d in days:
        x = DAY[d]; ex = x["ex"]; n = x["n"]; core = core_vec(x)
        ic = ss(core, ex); mex = statistics.fmean(ex)
        order = sorted(range(n), key=lambda i: core[i], reverse=True)
        def topmean(k, demean):
            k = min(k, n); sel = order[:k]
            v = [ex[i] - (mex if demean else 0.0) for i in sel]
            return statistics.fmean(v) if v else None
        rows.append({"d": d, "ic": ic, "breadth": mex, "qx": qx_by_date.get(d),
                     "t5_raw": topmean(5, False), "t5_dm": topmean(5, True),
                     "t10_raw": topmean(10, False), "t10_dm": topmean(10, True)})
    def split(metric, dset):
        return agg([r[metric] for r in rows if r["d"] in dset])
    qrows = [r for r in rows if r["qx"] is not None]
    def corr(metric):
        pairs = [(r["qx"], r[metric]) for r in qrows if r[metric] is not None]
        if len(pairs) < 3:
            return None
        return pearson([p[0] for p in pairs], [p[1] for p in pairs])
    return {"ic_all": agg([r["ic"] for r in rows]),
            "ic_hot": split("ic", hot_days), "ic_cold": split("ic", cold_days),
            "t5dm_hot": split("t5_dm", hot_days), "t5dm_cold": split("t5_dm", cold_days),
            "t5raw_hot": split("t5_raw", hot_days), "t5raw_cold": split("t5_raw", cold_days),
            "t10dm_hot": split("t10_dm", hot_days), "t10dm_cold": split("t10_dm", cold_days),
            "breadth_hot": split("breadth", hot_days), "breadth_cold": split("breadth", cold_days),
            "corr_qx_ic": corr("ic"), "corr_qx_t5raw": corr("t5_raw"),
            "corr_qx_t5dm": corr("t5_dm"), "n_qx_days": len(qrows)}

# ---------------- Q4 small-cap robustness ----------------
def Q4():
    CAP_100E = 1e6  # 100亿 in 万
    ic_cap = []; small_full = []; mid_full = []; large_full = []
    small_abs = []; big_abs = []; small_winsor = []
    day_disp = []; day_small = {}
    for d in days:
        x = DAY[d]; ex = x["ex"]; n = x["n"]
        cap = [x["amt"][i] / (x["turn"][i] / 100.0) if x["turn"][i] > 0 else None for i in range(n)]
        idx = [i for i in range(n) if cap[i] is not None]
        if len(idx) < 6:
            continue
        capv = [cap[i] for i in idx]; exv = [ex[i] for i in idx]
        ic_cap.append(ss(capv, exv))
        mex = statistics.fmean(exv)
        order = sorted(range(len(idx)), key=lambda j: capv[j])
        t = len(order) // 3
        smiset = order[:t]; lgiset = order[-t:] if t > 0 else []
        midset = order[t:len(order) - t]
        sval = statistics.fmean([exv[j] - mex for j in smiset]) if smiset else None
        small_full.append(sval)
        mid_full.append(statistics.fmean([exv[j] - mex for j in midset]) if midset else None)
        large_full.append(statistics.fmean([exv[j] - mex for j in lgiset]) if lgiset else None)
        day_small[d] = sval
        sa = [exv[j] - mex for j in range(len(idx)) if capv[j] < CAP_100E]
        ba = [exv[j] - mex for j in range(len(idx)) if capv[j] >= CAP_100E]
        if sa:
            small_abs.append(statistics.fmean(sa))
        if ba:
            big_abs.append(statistics.fmean(ba))
        exw = winsor(exv); mexw = statistics.fmean(exw)
        small_winsor.append(statistics.fmean([exw[j] - mexw for j in smiset]) if smiset else None)
        day_disp.append((d, statistics.pstdev(exv) if len(exv) > 1 else 0.0))
    drop = set(d for d, _ in sorted(day_disp, key=lambda z: z[1], reverse=True)[:2])
    small_exout = [day_small[d] for d in day_small if d not in drop and day_small[d] is not None]
    return {"ic_cap": agg(ic_cap), "small_demean_full": agg(small_full),
            "mid_demean_full": agg(mid_full), "large_demean_full": agg(large_full),
            "small_lt100e": agg(small_abs), "big_ge100e": agg(big_abs),
            "small_demean_winsor": agg(small_winsor),
            "small_demean_exoutlier": agg(small_exout),
            "dropped_days": sorted(list(drop))}

# ---------------- run ----------------
P = []
def line(s=""):
    P.append(s)

line("=" * 72)
line("FIRST-PRINCIPLES FACTOR VALIDATION (v65 / job 0075)")
line("=" * 72)
line("coverage: %d usable days, %d stock-days (MIN_N=%d)" % (len(days), total_rows, MIN_N))
if days:
    line("date range: %s .. %s" % (days[0], days[-1]))
line("regime: hot(QX>med)=%d days  cold=%d days  qx_days=%d" % (len(hot_days), len(cold_days), len(qx_days)))

if not days:
    line("\nNO USABLE DATA -- aborting.")
    print("\n".join(P))
    sys.exit(0)

q1 = Q1(); q2 = Q2(); q3 = Q3(); q4 = Q4()

line("\n--- Q1  竞价换手率 redundancy vs 竞价成交额 ---")
line("ic_amt (raw)          : %s" % pa(q1["ic_amt"]))
line("ic_turn (raw)         : %s" % pa(q1["ic_turn"]))
line("ic_turn_resid|amt     : %s   <- turn AFTER removing amt" % pa(q1["ic_turn_resid"]))
line("ic_amt_resid|turn     : %s   <- amt AFTER removing turn" % pa(q1["ic_amt_resid"]))
line("ic_comp z(amt)+z(turn): %s" % pa(q1["ic_comp"]))
line("corr(amt,turn) daily  : %s" % pa(q1["corr_amt_turn"]))

line("\n--- Q2  gap non-linearity (pooled demeaned excess by gap quintile) ---")
line("ic_gap (linear)       : %s" % pa(q2["ic_gap"]))
bins = q2["bins"]; bc = q2["bin_counts"]
for i in range(len(bins)):
    line("  gap bin %d (low->high): demean_excess=%s  n=%d" % (i, fmt(bins[i]), bc[i]))
line("hump = b2-(b0+b4)/2   : %s  (>0 => inverted-U, mid gap best)" % fmt(q2["hump"]))
line("mono = b4-b0          : %s  (>0 => monotone increasing)" % fmt(q2["mono"]))
line("ic_gap hot regime     : %s" % pa(q2["ic_gap_hot"]))
line("ic_gap cold regime    : %s" % pa(q2["ic_gap_cold"]))

line("\n--- Q3  sentiment-regime DIRECTION (core = z(amt)+z(turn)+z(gap)) ---")
line("ic_all                : %s" % pa(q3["ic_all"]))
line("ic hot / cold         : %s  ||  %s" % (pa(q3["ic_hot"]), pa(q3["ic_cold"])))
line("top5 demean hot/cold  : %s  ||  %s" % (pa(q3["t5dm_hot"]), pa(q3["t5dm_cold"])))
line("top5 raw    hot/cold  : %s  ||  %s" % (pa(q3["t5raw_hot"]), pa(q3["t5raw_cold"])))
line("top10 demean hot/cold : %s  ||  %s" % (pa(q3["t10dm_hot"]), pa(q3["t10dm_cold"])))
line("breadth     hot/cold  : %s  ||  %s" % (pa(q3["breadth_hot"]), pa(q3["breadth_cold"])))
line("corr(QX, daily_IC)    : %s  (n_qx=%d)" % (fmt(q3["corr_qx_ic"]), q3["n_qx_days"]))
line("corr(QX, top5_raw)    : %s" % fmt(q3["corr_qx_t5raw"]))
line("corr(QX, top5_demean) : %s" % fmt(q3["corr_qx_t5dm"]))

line("\n--- Q4  small-cap effect robustness (cap_proxy = amt/(turn/100), 万) ---")
line("ic_cap (cap vs excess): %s  (>0 => big-cap better)" % pa(q4["ic_cap"]))
line("small tercile demean  : %s" % pa(q4["small_demean_full"]))
line("mid   tercile demean  : %s" % pa(q4["mid_demean_full"]))
line("large tercile demean  : %s" % pa(q4["large_demean_full"]))
line("abs <100亿 demean     : %s" % pa(q4["small_lt100e"]))
line("abs >=100亿 demean    : %s" % pa(q4["big_ge100e"]))
line("small demean WINSOR   : %s  (excess p2/p98)" % pa(q4["small_demean_winsor"]))
line("small demean EX-OUTLIER: %s  (drop top-2 dispersion days: %s)" % (pa(q4["small_demean_exoutlier"]), ",".join(q4["dropped_days"])))

# ---------------- verdicts ----------------
ta = m0(q1["ic_turn"]); tr = m0(q1["ic_turn_resid"]); cat = m0(q1["corr_amt_turn"])
q1_redundant = (ta is not None and tr is not None and cat is not None and abs(cat) >= 0.5 and abs(tr) < 0.4 * abs(ta))

q2_nonlinear = (q2["hump"] is not None and q2["mono"] is not None and abs(q2["hump"]) > 0.05 and abs(q2["hump"]) >= 0.3 * (abs(q2["mono"]) + 1e-9))

ch = m0(q3["t5dm_cold"]); hh = m0(q3["t5dm_hot"]); cq = q3["corr_qx_t5dm"]; ciq = q3["corr_qx_ic"]
q3_backwards = (ch is not None and hh is not None and ch > hh and ((cq is not None and cq < 0) or (ciq is not None and ciq < 0)))

sf = m0(q4["small_demean_full"]); sw = m0(q4["small_demean_winsor"]); se = m0(q4["small_demean_exoutlier"])
q4_artifact = False
if sf is not None and abs(sf) > 1e-9:
    shrink_w = (sw is not None and abs(sw) < 0.5 * abs(sf))
    shrink_e = (se is not None and abs(se) < 0.5 * abs(sf))
    q4_artifact = shrink_w or shrink_e

line("\n" + "=" * 72)
line("FINAL VERDICTS")
line("=" * 72)
line("Q1 turnover redundant vs amt : %s" % ("YES (residual IC collapses & corr high) -> use ONE or orthogonalize" if q1_redundant else "NO / partial -- turnover keeps independent residual IC"))
line("Q2 gap non-linear            : %s" % ("YES -- hump present, linear IC understates; use binned/turning-point" if q2_nonlinear else "mostly monotone -- linear treatment acceptable"))
line("Q3 regime gate BACKWARDS     : %s" % ("YES -- alpha is STRONGER on COLD days; current 'more aggressive when hot' gate is inverted" if q3_backwards else "NO -- hot days not worse; current gate not contradicted"))
line("Q4 small-cap effect artifact : %s" % ("YES -- small-cap signal halves under winsor/outlier-removal (supports NOT hard-rejecting small caps)" if q4_artifact else "NO -- small-cap signal survives robustness checks"))

print("\n".join(P))

# ---------------- persist report ----------------
report = {"coverage": {"days": len(days), "stock_days": total_rows,
                       "range": [days[0], days[-1]] if days else [],
                       "hot_days": len(hot_days), "cold_days": len(cold_days)},
          "Q1": q1, "Q2": q2, "Q3": q3, "Q4": q4,
          "verdicts": {"q1_turnover_redundant": q1_redundant,
                       "q2_gap_nonlinear": q2_nonlinear,
                       "q3_regime_gate_backwards": q3_backwards,
                       "q4_smallcap_artifact": q4_artifact}}
outdir = os.path.join(PROJECT_ROOT, "reports", "_audit")
try:
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "firstprinciples_v65.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    with open(os.path.join(outdir, "firstprinciples_v65.md"), "w", encoding="utf-8") as f:
        f.write("# First-principles factor validation (v65 / job 0075)\n\n")
        f.write("```\n" + "\n".join(P) + "\n```\n")
except Exception as e:
    print("WARN: could not write report: %r" % e)
