#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Job 0075 - 第一性原理四问验证 v65

针对 4 个质疑做实证（不预设结论，只看数据）：
  Q1 换手率是否只是成交额的归一化？两者正交后还剩多少独立 IC？
  Q2 gap(竞价涨幅) 对 excess 是线性还是非单调(驼峰/拐点)？分 regime 变吗？
  Q3 情绪 regime 方向：核心 alpha 到底是热市还是冷市更能选出赢家？(现行 gate 是热市更激进)
  Q4 小盘效应：去掉异常日/缩尾后，“小盘惩罚”是真实的还是肥尾伪象？

excess = (close - open) / preclose * 100。只用 <=09:30 的竞价快照，无泄露。
输出： reports/_audit/firstprinciples_v65.{json,md}
用法： python3 scripts/0075_premktfactor_firstprinciples_v65_20260628.py
"""
import json, os, sys, math, traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

WS = Path(os.environ.get("WORKSPACE", "/home/investmentofficehku/.openclaw/workspace"))
PROJECT_ROOT = WS / "projects" / "duanxianxia"
sys.path.insert(0, str(WS / "scripts"))
from v10_optimize import Daily, spearman, mean_icir, rankdata

PREOPEN = "093000"
CAP = PROJECT_ROOT / "captures"
QIANG = "auction.jjyd.qiangchou"
QXLIVE = "home.qxlive.top_metrics"
F_AMT = "auction_turnover_wan"
F_TURN = "turnover_rate_pct"
F_GAP = "latest_change_pct"


def pnum(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    t = str(s).replace(",", "").replace("%", "").replace("+", "").strip()
    if t in ("", "--", "-", "null", "None"):
        return None
    m = 1.0
    if t.endswith("\u4ebf"):
        m = 1e4; t = t[:-1]
    elif t.endswith("\u4e07"):
        m = 1.0; t = t[:-1]
    elif t.lower().endswith("w"):
        m = 1.0; t = t[:-1]
    try:
        return float(t) * m
    except Exception:
        return None


def _norm(c):
    s = str(c).split(".")[0]
    return s[-6:].zfill(6)


def code_of(r):
    for k in ("code", "\u4ee3\u7801", "symbol"):
        if r.get(k) not in (None, ""):
            return _norm(r.get(k))
    return None


def premarket_rows(dd, dsid):
    p = dd / dsid
    if not p.exists():
        return []
    files = sorted(f for f in p.iterdir() if f.suffix == ".json" and f.stem <= PREOPEN)
    if not files:
        return []
    try:
        d = json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(d, list):
        return [r for r in d if isinstance(r, dict)]
    rows = d.get("rows") or d.get("data") or d.get("list") or []
    return [r for r in rows if isinstance(r, dict)]


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def pstd(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return 0.0
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / len(xs)) ** 0.5


def pctrank(vals):
    n = len(vals)
    if n == 1:
        return [0.5]
    r = rankdata(vals)
    return [(x - 1.0) / (n - 1.0) for x in r]


def zlist(vals):
    n = len(vals)
    if n == 0:
        return []
    m = sum(vals) / n
    sd = (sum((v - m) ** 2 for v in vals) / n) ** 0.5
    if sd == 0:
        return [0.0] * n
    return [(v - m) / sd for v in vals]


def ols_resid(x, y):
    n = len(x)
    mx = sum(x) / n
    my = sum(y) / n
    vx = sum((xi - mx) ** 2 for xi in x)
    if vx == 0:
        return [yi - my for yi in y]
    b = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / vx
    a = my - b * mx
    return [y[i] - (a + b * x[i]) for i in range(n)]


def winsor_map(exmap, lo=0.02, hi=0.98):
    vals = sorted(exmap.values())
    n = len(vals)
    if n < 5:
        return dict(exmap)
    loq = vals[max(0, int(lo * n))]
    hiq = vals[min(n - 1, int(hi * n))]
    return {c: min(max(v, loq), hiq) for c, v in exmap.items()}


# ---------------- load all days ----------------
daily = Daily(PROJECT_ROOT)
date_dirs = sorted(p for p in CAP.iterdir() if p.is_dir()) if CAP.is_dir() else []

days = []  # {date, recs:[{code,amt,turn,gap,ex}], qx}
for dd in date_dirs:
    rows = premarket_rows(dd, QIANG)
    if not rows:
        continue
    recs = []
    seen = set()
    for r in rows:
        c = code_of(r)
        if not c or c in seen:
            continue
        ex = daily.excess(c, dd.name)
        if ex is None:
            continue
        seen.add(c)
        recs.append({
            "code": c,
            "amt": pnum(r.get(F_AMT)),
            "turn": pnum(r.get(F_TURN)),
            "gap": pnum(r.get(F_GAP)),
            "ex": ex,
        })
    if len(recs) < 12:
        continue
    qx = None
    for r in premarket_rows(dd, QXLIVE):
        if r.get("metric_key") == "QX":
            qx = pnum(r.get("raw_value")) or pnum(r.get("value"))
            break
    days.append({"date": dd.name, "recs": recs, "qx": qx})

qx_vals = sorted(d["qx"] for d in days if d["qx"] is not None)
qx_med = qx_vals[len(qx_vals) // 2] if qx_vals else None
N_DAYS = len(days)
report = {"generated_at": datetime.now().isoformat(timespec="seconds"),
          "job": "firstprinciples_v65", "n_days": N_DAYS, "qx_median": qx_med}
print("=" * 64)
print("Job 0075 first-principles v65 | days={} qx_med={}".format(N_DAYS, qx_med))
print("=" * 64)


# ================= Q1: turnover vs amount orthogonalization =================
ic_amt, ic_turn, ic_turn_resid, ic_amt_resid, ic_comp, corr_at = [], [], [], [], [], []
for d in days:
    rs = [r for r in d["recs"] if r["amt"] is not None and r["turn"] is not None]
    if len(rs) < 12:
        continue
    amt = [r["amt"] for r in rs]
    turn = [r["turn"] for r in rs]
    ex = [r["ex"] for r in rs]
    ar = pctrank(amt)
    tr = pctrank(turn)
    ic_amt.append(spearman(amt, ex))
    ic_turn.append(spearman(turn, ex))
    corr_at.append(spearman(amt, turn))
    rt = ols_resid(ar, tr)  # turn rank residual after removing amt rank
    ra = ols_resid(tr, ar)  # amt rank residual after removing turn rank
    ic_turn_resid.append(spearman(rt, ex))
    ic_amt_resid.append(spearman(ra, ex))
    za = zlist(ar); zt = zlist(tr)
    comp = [za[i] + zt[i] for i in range(len(rs))]
    ic_comp.append(spearman(comp, ex))


def summ(lst):
    m, icir, n = mean_icir([x for x in lst if x is not None])
    return {"mean_ic": m, "icir": icir, "n_days": n}


q1 = {
    "ic_amount": summ(ic_amt),
    "ic_turnrate": summ(ic_turn),
    "corr_amount_turnrate": summ(corr_at),
    "ic_turnrate_resid_after_amount": summ(ic_turn_resid),
    "ic_amount_resid_after_turnrate": summ(ic_amt_resid),
    "ic_composite_amt_plus_turn": summ(ic_comp),
}
report["Q1_orthogonalization"] = q1
print("\n[Q1] \u6362手率 vs 成交额 正交分解")
for k, v in q1.items():
    print("  {:34s} ic={} icir={} n={}".format(k, v["mean_ic"], v["icir"], v["n_days"]))


# ================= Q2: gap nonlinearity =================
NB = 5
bin_dm = [[] for _ in range(NB)]
bin_day = [[] for _ in range(NB)]
ic_gap_all, ic_gap_hot, ic_gap_cold = [], [], []
hump_list, mono_list = [], []
for d in days:
    rs = [r for r in d["recs"] if r["gap"] is not None]
    if len(rs) < 15:
        continue
    gap = [r["gap"] for r in rs]
    ex = [r["ex"] for r in rs]
    dm = mean(ex)
    ic = spearman(gap, ex)
    ic_gap_all.append(ic)
    if d["qx"] is not None and qx_med is not None:
        (ic_gap_hot if d["qx"] >= qx_med else ic_gap_cold).append(ic)
    gr = pctrank(gap)
    daymean = [None] * NB
    bd = [[] for _ in range(NB)]
    for i in range(len(rs)):
        b = min(NB - 1, int(gr[i] * NB))
        bin_dm[b].append(ex[i] - dm)
        bd[b].append(ex[i] - dm)
    for b in range(NB):
        if bd[b]:
            daymean[b] = mean(bd[b])
            bin_day[b].append(daymean[b])
    if all(x is not None for x in daymean):
        mono_list.append(daymean[NB - 1] - daymean[0])
        hump_list.append(daymean[NB // 2] - (daymean[0] + daymean[NB - 1]) / 2.0)

bins_out = []
for b in range(NB):
    m, icir, n = mean_icir(bin_day[b])
    bins_out.append({"bin": b, "pooled_mean_demeaned": round(mean(bin_dm[b]), 4) if bin_dm[b] else None,
                     "perday_mean": m, "perday_icir": icir, "n_obs": len(bin_dm[b])})
q2 = {
    "ic_gap_all": summ(ic_gap_all),
    "ic_gap_hot_regime": summ(ic_gap_hot),
    "ic_gap_cold_regime": summ(ic_gap_cold),
    "bin_curve_low_to_high": bins_out,
    "monotonic_top_minus_bottom": summ(mono_list),
    "hump_mid_minus_ends": summ(hump_list),
}
report["Q2_gap_nonlinearity"] = q2
print("\n[Q2] gap 非线性 (分 5 档, demeaned excess)")
print("  ic_all={} ic_hot={} ic_cold={}".format(
    q2["ic_gap_all"]["mean_ic"], q2["ic_gap_hot_regime"]["mean_ic"], q2["ic_gap_cold_regime"]["mean_ic"]))
for b in bins_out:
    print("  bin{} pooled_dm={} perday={} icir={} n={}".format(
        b["bin"], b["pooled_mean_demeaned"], b["perday_mean"], b["perday_icir"], b["n_obs"]))
print("  monotonic(top-bot)={} hump(mid-ends)={}".format(
    q2["monotonic_top_minus_bottom"]["mean_ic"], q2["hump_mid_minus_ends"]["mean_ic"]))


# ================= Q3: sentiment regime direction =================
core_ic_hot, core_ic_cold = [], []
top5_raw_hot, top5_raw_cold = [], []
top5_dm_hot, top5_dm_cold = [], []
breadth_hot, breadth_cold = [], []
pairs_qx_top5raw, pairs_qx_coreic = [], []
for d in days:
    rs = [r for r in d["recs"] if r["amt"] is not None and r["turn"] is not None and r["gap"] is not None]
    if len(rs) < 15 or d["qx"] is None or qx_med is None:
        continue
    ex = [r["ex"] for r in rs]
    dm = mean(ex)
    za = zlist(pctrank([r["amt"] for r in rs]))
    zt = zlist(pctrank([r["turn"] for r in rs]))
    zg = zlist(pctrank([r["gap"] for r in rs]))
    core = [za[i] + zt[i] + zg[i] for i in range(len(rs))]
    ic = spearman(core, ex)
    order = sorted(range(len(rs)), key=lambda i: core[i], reverse=True)[:5]
    t5raw = mean([ex[i] for i in order])
    t5dm = mean([ex[i] - dm for i in order])
    hot = d["qx"] >= qx_med
    (core_ic_hot if hot else core_ic_cold).append(ic)
    (top5_raw_hot if hot else top5_raw_cold).append(t5raw)
    (top5_dm_hot if hot else top5_dm_cold).append(t5dm)
    (breadth_hot if hot else breadth_cold).append(dm)
    pairs_qx_top5raw.append((d["qx"], t5raw))
    if ic is not None:
        pairs_qx_coreic.append((d["qx"], ic))


def corr_pairs(pairs):
    if len(pairs) < 6:
        return None
    xs, ys = zip(*pairs)
    return spearman(list(xs), list(ys))


q3 = {
    "core_ic_hot": summ(core_ic_hot), "core_ic_cold": summ(core_ic_cold),
    "top5_raw_excess_hot": round(mean(top5_raw_hot), 4) if top5_raw_hot else None,
    "top5_raw_excess_cold": round(mean(top5_raw_cold), 4) if top5_raw_cold else None,
    "top5_demeaned_hot": round(mean(top5_dm_hot), 4) if top5_dm_hot else None,
    "top5_demeaned_cold": round(mean(top5_dm_cold), 4) if top5_dm_cold else None,
    "breadth_mean_excess_hot": round(mean(breadth_hot), 4) if breadth_hot else None,
    "breadth_mean_excess_cold": round(mean(breadth_cold), 4) if breadth_cold else None,
    "corr_qx_top5raw": corr_pairs(pairs_qx_top5raw),
    "corr_qx_coreic": corr_pairs(pairs_qx_coreic),
    "n_hot": len(top5_raw_hot), "n_cold": len(top5_raw_cold),
}
report["Q3_regime_direction"] = q3
print("\n[Q3] 情绪 regime 方向 (核心 alpha comp_SD top5)")
print("  core_ic hot={} cold={}".format(q3["core_ic_hot"]["mean_ic"], q3["core_ic_cold"]["mean_ic"]))
print("  top5_raw_excess hot={} cold={}".format(q3["top5_raw_excess_hot"], q3["top5_raw_excess_cold"]))
print("  top5_demeaned   hot={} cold={}".format(q3["top5_demeaned_hot"], q3["top5_demeaned_cold"]))
print("  breadth(day mean ex) hot={} cold={}".format(q3["breadth_mean_excess_hot"], q3["breadth_mean_excess_cold"]))
print("  corr(QX,top5raw)={} corr(QX,coreIC)={}  [neg=>cold better]".format(
    q3["corr_qx_top5raw"], q3["corr_qx_coreic"]))


# ================= Q4: small-cap robustness =================
SMALL_WAN = 1e6  # 100亿 = 1,000,000 万
recs_q4 = []  # (date, cap_wan, ex, dm_raw, dm_w, tercile, abs_small)
day_disp = {}
ic_cap, ic_cap_w = [], []
for d in days:
    rs = [r for r in d["recs"] if r["amt"] is not None and r["turn"] and r["turn"] > 0]
    if len(rs) < 15:
        continue
    for r in rs:
        r["cap"] = r["amt"] / (r["turn"] / 100.0)
    exmap = {r["code"]: r["ex"] for r in rs}
    wmap = winsor_map(exmap)
    dm = mean([r["ex"] for r in rs])
    dmw = mean([wmap[r["code"]] for r in rs])
    day_disp[d["date"]] = pstd([r["ex"] for r in rs])
    caps = [r["cap"] for r in rs]
    cr = pctrank(caps)
    ic_cap.append(spearman(caps, [r["ex"] for r in rs]))
    ic_cap_w.append(spearman(caps, [wmap[r["code"]] for r in rs]))
    for i, r in enumerate(rs):
        terc = 0 if cr[i] < 1.0 / 3 else (1 if cr[i] < 2.0 / 3 else 2)
        recs_q4.append((d["date"], r["cap"], r["ex"], r["ex"] - dm, wmap[r["code"]] - dmw,
                        terc, r["cap"] < SMALL_WAN))

outlier_dates = set(sorted(day_disp, key=lambda k: day_disp[k], reverse=True)[:2])


def bucket_mean(pred, field, exclude=None):
    vals = [rec[field] for rec in recs_q4 if pred(rec) and (exclude is None or rec[0] not in exclude)]
    return (round(mean(vals), 4) if vals else None, len(vals))

# field index: 3=dm_raw, 4=dm_w
small_raw, n_sr = bucket_mean(lambda r: r[5] == 0, 3)
mid_raw, _ = bucket_mean(lambda r: r[5] == 1, 3)
large_raw, _ = bucket_mean(lambda r: r[5] == 2, 3)
small_w, _ = bucket_mean(lambda r: r[5] == 0, 4)
small_exout, n_seo = bucket_mean(lambda r: r[5] == 0, 3, exclude=outlier_dates)
abs_small_raw, n_as = bucket_mean(lambda r: r[6], 3)
abs_small_w, _ = bucket_mean(lambda r: r[6], 4)
abs_small_exout, _ = bucket_mean(lambda r: r[6], 3, exclude=outlier_dates)

q4 = {
    "ic_cap_raw": summ(ic_cap), "ic_cap_winsor": summ(ic_cap_w),
    "tercile_demeaned_raw": {"small": small_raw, "mid": mid_raw, "large": large_raw, "n_small": n_sr},
    "small_tercile_demeaned": {"raw": small_raw, "winsor": small_w, "ex_outlier_days": small_exout, "n": n_sr},
    "abs_small_lt_100yi_demeaned": {"raw": abs_small_raw, "winsor": abs_small_w,
                                     "ex_outlier_days": abs_small_exout, "n": n_as},
    "outlier_days_excluded": sorted(outlier_dates),
}
report["Q4_smallcap_robustness"] = q4
print("\n[Q4] 小盘效应稳健性 (cap代理=成交额/换手率, demeaned excess; neg=小盘差)")
print("  ic_cap raw={} winsor={}  [neg ic => 小市值 excess 更高]".format(
    q4["ic_cap_raw"]["mean_ic"], q4["ic_cap_winsor"]["mean_ic"]))
print("  tercile demeaned raw: small={} mid={} large={}".format(small_raw, mid_raw, large_raw))
print("  small tercile: raw={} winsor={} ex_outlier={} (n={})".format(small_raw, small_w, small_exout, n_sr))
print("  abs<100\u4ebf : raw={} winsor={} ex_outlier={} (n={})".format(
    abs_small_raw, abs_small_w, abs_small_exout, n_as))
print("  outlier days excluded: {}".format(sorted(outlier_dates)))


# ---------------- write report ----------------
audit = PROJECT_ROOT / "reports" / "_audit"
audit.mkdir(parents=True, exist_ok=True)
(audit / "firstprinciples_v65.json").write_text(
    json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
L = ["# 第一性原理四问验证 v65", "", "- 生成: " + report["generated_at"],
     "- days={} qx_med={}".format(N_DAYS, qx_med), "",
     "## Q1 换手率 vs 成交额", "", "| 指标 | ic | icir | n |", "|---|---|---|---|"]
for k, v in q1.items():
    L.append("| {} | {} | {} | {} |".format(k, v["mean_ic"], v["icir"], v["n_days"]))
L += ["", "## Q2 gap 非线性 (bin low->high)", "", "| bin | pooled_dm | perday | icir | n |", "|---|---|---|---|---|"]
for b in bins_out:
    L.append("| {} | {} | {} | {} | {} |".format(b["bin"], b["pooled_mean_demeaned"], b["perday_mean"], b["perday_icir"], b["n_obs"]))
L += ["", "ic_gap all/hot/cold = {} / {} / {}".format(
    q2["ic_gap_all"]["mean_ic"], q2["ic_gap_hot_regime"]["mean_ic"], q2["ic_gap_cold_regime"]["mean_ic"]),
    "monotonic={} hump={}".format(q2["monotonic_top_minus_bottom"]["mean_ic"], q2["hump_mid_minus_ends"]["mean_ic"])]
L += ["", "## Q3 情绪 regime", "",
      "core_ic hot/cold = {} / {}".format(q3["core_ic_hot"]["mean_ic"], q3["core_ic_cold"]["mean_ic"]),
      "top5_raw hot/cold = {} / {}".format(q3["top5_raw_excess_hot"], q3["top5_raw_excess_cold"]),
      "top5_demeaned hot/cold = {} / {}".format(q3["top5_demeaned_hot"], q3["top5_demeaned_cold"]),
      "breadth hot/cold = {} / {}".format(q3["breadth_mean_excess_hot"], q3["breadth_mean_excess_cold"]),
      "corr(QX,top5raw)={} corr(QX,coreIC)={}".format(q3["corr_qx_top5raw"], q3["corr_qx_coreic"])]
L += ["", "## Q4 小盘稳健性", "",
      "ic_cap raw/winsor = {} / {}".format(q4["ic_cap_raw"]["mean_ic"], q4["ic_cap_winsor"]["mean_ic"]),
      "small tercile raw/winsor/ex_outlier = {} / {} / {}".format(small_raw, small_w, small_exout),
      "abs<100\u4ebf raw/winsor/ex_outlier = {} / {} / {}".format(abs_small_raw, abs_small_w, abs_small_exout),
      "outlier days = {}".format(sorted(outlier_dates))]
(audit / "firstprinciples_v65.md").write_text("\n".join(L), encoding="utf-8")

print("\n" + "=" * 64)
print("FINAL VERDICTS (数据结论摘要)")
print("=" * 64)
print("Q1 换手率独立信息: corr(amt,turn)={} ; 去掉amt后 turn 残余IC={} ; 去掉turn后 amt 残余IC={} ; 合成IC={} vs 单amt={}".format(
    q1["corr_amount_turnrate"]["mean_ic"], q1["ic_turnrate_resid_after_amount"]["mean_ic"],
    q1["ic_amount_resid_after_turnrate"]["mean_ic"], q1["ic_composite_amt_plus_turn"]["mean_ic"], q1["ic_amount"]["mean_ic"]))
print("Q2 gap: ic_all={} monotonic={} hump={} (hump>0 且 |mono| 小 => 驼峰/拐点, 不能当线性正因子)".format(
    q2["ic_gap_all"]["mean_ic"], q2["monotonic_top_minus_bottom"]["mean_ic"], q2["hump_mid_minus_ends"]["mean_ic"]))
print("Q3 regime: corr(QX,top5raw)={} (neg=>冷市选股更赚, 现行热市激进 gate 可能反了)".format(q3["corr_qx_top5raw"]))
print("Q4 小盘: small tercile raw={} -> winsor={} -> ex_outlier={} (若去异常/缩尾后接近0 => 小盘惩罚是肥尾伪象)".format(
    small_raw, small_w, small_exout))
print("\n[DONE]")
