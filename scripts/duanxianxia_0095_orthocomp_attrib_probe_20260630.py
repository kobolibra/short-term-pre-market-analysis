#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0095 comp_SD 正交贡献归因 + 稳健确认探针(只读)。

0094: 候选池内 comp_SD 单因子 IC 0.165, 混入后 edge IC 0.163->0.18, ICIR 0.667->0.86
(10-11/15 天胜), 但自动推荐因 capture@30 闸门判 no_change ——而候选池中位仅 43≈TopN30,
capture 已饱和(~0.65)无区分度, 该闸门退化。本探针修正两点:
  1) 归因: A_turnover≈现行 amt_pct(均=竞价成交额), 全 comp_SD 会重复计 A。
     分别测 full_ABC / BC_orthogonal(只 B 换手率+C 跳空, 真正新信息) / A_only。
  2) 用非退化口径: 全候选池 + mean_excess@{10,20,30}(实际选股收益)与 IC/ICIR,
     不再用饱和的 capture 闸门。
推荐 BC_orthogonal(干净, 不双计) 若其稳健超 baseline; 否则 full; 都不达标则 no_change。
只读。输出 reports/_audit/orthocomp_attrib_v0095.json。
用法: python3 scripts/duanxianxia_0095_orthocomp_attrib_probe_20260630.py
"""
from __future__ import annotations
import json
import math
import statistics
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10

PREOPEN = "093000"
CODE_KEYS = ["code", "\u4ee3\u7801"]
QDATASET = "auction.jjyd.qiangchou"
QFIELD = {"A_turnover": "auction_turnover_wan", "B_turnrate": "turnover_rate_pct", "C_gap": "latest_change_pct"}
VARIANTS = {
    "full_ABC": ["A_turnover", "B_turnrate", "C_gap"],
    "BC_orthogonal": ["B_turnrate", "C_gap"],
    "A_only": ["A_turnover"],
}
LAMBDAS = [0.2, 0.3, 0.4, 0.5]
TOPK = [10, 20, 30]
MIN_ROWS = 30


def pnum(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("%", "").replace("+", "")
    if s in ("", "-", "--", "None"):
        return None
    mult = 1.0
    if s.endswith("\u4ebf"):
        mult, s = 1e4, s[:-1]
    elif s.endswith("\u4e07"):
        mult, s = 1.0, s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return None


def norm_code(v):
    s = str(v or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    if len(s) > 6:
        s = s[-6:]
    return s.zfill(6) if s.isdigit() else s


def code_of(r):
    for k in CODE_KEYS:
        if r.get(k) not in (None, ""):
            return norm_code(r.get(k))
    return ""


def latest_preopen_rows(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return []
    pre = [f for f in d.glob("*.json") if len(f.stem) == 6 and f.stem.isdigit() and f.stem <= PREOPEN]
    if not pre:
        return []
    f = sorted(pre)[-1]
    try:
        payload = json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def zmap(valmap, codes):
    vals = [valmap[c] for c in codes if c in valmap]
    if len(vals) < 3:
        return {}
    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / len(vals)
    sd = math.sqrt(var)
    if sd == 0:
        return {}
    return {c: (valmap[c] - m) / sd for c in codes if c in valmap}


def edge_score_row(r):
    return max(0.0, min(100.0, v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r.get("risk") or 0.0)))


def mean_excess_at(scores_by_code, ex, codes, k):
    order = sorted(codes, key=lambda c: scores_by_code[c], reverse=True)
    sel = order[:min(k, len(order))]
    if not sel:
        return None
    return sum(ex[c] for c in sel) / len(sel)


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    days = v10.load_days(root, daily)
    cap_root = root / "captures"

    base_ic = []
    base_me = {k: [] for k in TOPK}
    blend_ic = defaultdict(list)
    blend_me = {k: defaultdict(list) for k in TOPK}
    beat_ic = defaultdict(int)
    beat_me10 = defaultdict(int)
    comp_only_ic = defaultdict(list)
    cmp_days = 0
    coverage = []

    for day in days:
        date = day["date"]
        rows = day["rows"]
        cdir = cap_root / date
        qrows = latest_preopen_rows(cdir, QDATASET) if cdir.is_dir() else []
        qidx = {}
        for r in qrows:
            c = code_of(r)
            if c:
                qidx.setdefault(c, r)
        member_raw = {}
        for key, field in QFIELD.items():
            m = {}
            for c, r in qidx.items():
                v = pnum(r.get(field))
                if v is not None:
                    m[c] = v
            member_raw[key] = m

        edge_raw = {}
        ex = {}
        for r in rows:
            c = norm_code(r.get("code"))
            if not c:
                continue
            edge_raw[c] = edge_score_row(r)
            ex[c] = r["excess"]
        codes = list(ex.keys())
        if len(codes) < MIN_ROWS:
            continue
        ze = zmap(edge_raw, codes)
        if not ze:
            continue
        zeb = {c: ze.get(c, 0.0) for c in codes}
        member_z = {key: zmap(member_raw[key], codes) for key in QFIELD}
        ys = [ex[c] for c in codes]
        bic = v10.spearman([zeb[c] for c in codes], ys)
        if bic is None:
            continue
        cmp_days += 1
        coverage.append(sum(1 for c in codes if any(c in member_z[k] for k in QFIELD)))
        base_ic.append(bic)
        for k in TOPK:
            me = mean_excess_at(zeb, ex, codes, k)
            if me is not None:
                base_me[k].append(me)
        base_me10_day = mean_excess_at(zeb, ex, codes, 10)

        for vname, members in VARIANTS.items():
            comp = {}
            for c in codes:
                zs = [member_z[m][c] for m in members if c in member_z[m]]
                comp[c] = (sum(zs) / len(zs)) if zs else 0.0
            cic = v10.spearman([comp[c] for c in codes], ys)
            if cic is not None:
                comp_only_ic[vname].append(cic)
            for lam in LAMBDAS:
                sc = {c: (1.0 - lam) * zeb[c] + lam * comp[c] for c in codes}
                key = (vname, lam)
                ic = v10.spearman([sc[c] for c in codes], ys)
                if ic is not None:
                    blend_ic[key].append(ic)
                    if ic > bic:
                        beat_ic[key] += 1
                for k in TOPK:
                    me = mean_excess_at(sc, ex, codes, k)
                    if me is not None:
                        blend_me[k][key].append(me)
                me10 = mean_excess_at(sc, ex, codes, 10)
                if me10 is not None and base_me10_day is not None and me10 > base_me10_day:
                    beat_me10[key] += 1

    bm_ic, bm_icir, _ = v10.mean_icir(base_ic)
    base_me_mean = {k: (round(statistics.mean(base_me[k]), 3) if base_me[k] else None) for k in TOPK}
    base_me10 = base_me_mean[10]

    comp_only = {}
    for v in VARIANTS:
        m, icir, nd = v10.mean_icir(comp_only_ic[v])
        comp_only[v] = {"mean_ic": m, "icir": icir, "n_days": nd}

    schemes = {}
    for vname in VARIANTS:
        for lam in LAMBDAS:
            key = (vname, lam)
            m, icir, nd = v10.mean_icir(blend_ic.get(key, []))
            me_mean = {("me%d" % k): (round(statistics.mean(blend_me[k][key]), 3) if blend_me[k].get(key) else None) for k in TOPK}
            schemes["%s@lam%.1f" % (vname, lam)] = {
                "mean_ic": m,
                "icir": icir,
                "n_days": nd,
                "mean_excess_at": me_mean,
                "days_beat_ic": "%d/%d" % (beat_ic[key], cmp_days),
                "days_beat_me10": "%d/%d" % (beat_me10[key], cmp_days),
            }

    def qualifies(vname, lam):
        key = (vname, lam)
        m, icir, nd = v10.mean_icir(blend_ic.get(key, []))
        me10 = statistics.mean(blend_me[10][key]) if blend_me[10].get(key) else None
        if m is None or bm_ic is None or me10 is None or base_me10 is None:
            return None
        ok = (
            m > bm_ic
            and me10 >= base_me10
            and (bm_icir is None or (icir is not None and icir >= bm_icir))
            and beat_me10[key] * 2 >= cmp_days
        )
        return (me10, m) if ok else None

    rec = "edge_only_no_change"
    best = None
    for vname in ("BC_orthogonal", "full_ABC"):
        cand = []
        for lam in LAMBDAS:
            q = qualifies(vname, lam)
            if q:
                cand.append((vname, lam, q[0], q[1]))
        if cand:
            cand.sort(key=lambda t: (t[2], t[3]), reverse=True)
            best = cand[0]
            break
    if best:
        rec = "%s@lam%.1f" % (best[0], best[1])

    assert cmp_days >= 3, "too few cmp days: %d" % cmp_days
    assert bm_ic is not None, "baseline IC is None"
    assert base_me10 is not None, "baseline me10 is None"

    report = {
        "probe": "0095_orthocomp_attrib",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "cmp_days": cmp_days,
        "median_with_comp": int(statistics.median(coverage)) if coverage else None,
        "topk": TOPK,
        "baseline_edge": {"mean_ic": bm_ic, "icir": bm_icir, "mean_excess_at": base_me_mean},
        "comp_only_by_variant": comp_only,
        "schemes": schemes,
        "recommendation": rec,
        "note": "BC_orthogonal=\u53bb\u9664\u4e0e amt_pct \u91cd\u53e0\u7684 A \u540e\u7684\u51c0\u6b63\u4ea4\u4fe1\u53f7; \u63a8\u8350\u9700 IC>baseline \u4e14 mean_excess@10>=baseline \u4e14 ICIR>=baseline \u4e14\u8fc7\u534a\u5929\u6570 me10 \u80dc; \u4f18\u5148 BC_orthogonal\u3002",
    }

    try:
        audit = root / "reports" / "_audit"
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "orthocomp_attrib_v0095.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
