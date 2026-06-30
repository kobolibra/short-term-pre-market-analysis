#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0094 正交复合并入 edge 验证探针(只读)。

背景: 0093 证实在现有 7 个 CORE_FIELDS 上重配权重 = 噪音(OOS IC 0.1278->0.1290,
capture@30 不变 0.167)。真正的杠杆在正交信号: v44 的 comp_SD =
signed-z 平均{auction_turnover_wan, turnover_rate_pct, latest_change_pct}
(全体竞价股 IC 0.179)。但生产只对候选池打分, v44 的 IC 在全市场测得,
范围压缩到候选池后增量可能缩水。本探针在候选池内核验真实增量:

  每日: baseline edge = clip(v10.score(f,amt,V10AMT_W)-risk,0,100), 候选池横截面 z;
         comp_SD 从原始 capture(<=093000 的 auction.jjyd.qiangchou)取, 候选池横截面 z;
         blend = (1-lam)*z_edge + lam*z_comp, lam in {0,0.2,0.3,0.4,0.5,1.0};
  按 blend 排序算 IC/ICIR/capture@30, 逐日对比 lam=0(=现行 baseline 排序)。
  推荐整合当且仅当某 lam>0 的 OOS IC>baseline 且 capture>=baseline 且 ICIR>=0.9*baseline
  且过半天数胜; 否则推荐 edge_only_no_change。

excess=(close-open)/preclose*100。只读, 不改任何引擎; 输出 reports/_audit/orthocomp_blend_v0094.json。
用法: python3 scripts/duanxianxia_0094_orthocomp_blend_probe_20260630.py
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
QFIELDS = [
    ("A_turnover", "auction_turnover_wan"),
    ("B_turnrate", "turnover_rate_pct"),
    ("C_gap", "latest_change_pct"),
]
LAMBDAS = [0.0, 0.2, 0.3, 0.4, 0.5, 1.0]
TOPN = 30
MIN_ROWS = 8
MIN_CAP_ROWS = 30


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


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    days = v10.load_days(root, daily)  # 不 regen, 直接读已有 v9 分析
    cap_root = root / "captures"

    per_lambda_ic = defaultdict(list)
    per_lambda_cap = defaultdict(list)
    beat_ic = defaultdict(int)
    comp_only_ic = []
    coverage = []
    cmp_days = 0
    n_eval_days = 0

    for day in days:
        date = day["date"]
        rows = day["rows"]
        cdir = cap_root / date
        qrows = latest_preopen_rows(cdir, QDATASET) if cdir.is_dir() else []
        if not qrows:
            continue
        qidx = {}
        for r in qrows:
            c = code_of(r)
            if c:
                qidx.setdefault(c, r)
        member_raw = {}
        for key, field in QFIELDS:
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
            if not any(c in member_raw[k] for (k, _) in QFIELDS):
                continue
            edge_raw[c] = edge_score_row(r)
            ex[c] = r["excess"]
        codes = list(ex.keys())
        if len(codes) < MIN_ROWS:
            continue

        member_z = {k: zmap(member_raw[k], codes) for (k, _) in QFIELDS}
        comp = {}
        for c in codes:
            zs = [member_z[k][c] for (k, _) in QFIELDS if c in member_z[k]]
            if zs:
                comp[c] = sum(zs) / len(zs)
        ez = zmap(edge_raw, codes)
        both = [c for c in codes if c in ez and c in comp]
        if len(both) < MIN_ROWS:
            continue
        n_eval_days += 1
        coverage.append(len(both))
        ys = [ex[c] for c in both]

        base_ic = v10.spearman([ez[c] for c in both], ys)
        if base_ic is None:
            continue
        cic = v10.spearman([comp[c] for c in both], ys)
        if cic is not None:
            comp_only_ic.append(cic)

        do_cap = len(both) >= MIN_CAP_ROWS
        winners = set()
        if do_cap:
            order = sorted(range(len(both)), key=lambda i: ys[i], reverse=True)
            winners = set(order[:TOPN])

        def cap_of(scores):
            o = sorted(range(len(both)), key=lambda i: scores[i], reverse=True)
            return len(set(o[:TOPN]) & winners) / float(min(TOPN, len(winners)))

        cmp_days += 1
        for lam in LAMBDAS:
            sc = [(1.0 - lam) * ez[c] + lam * comp[c] for c in both]
            ic = v10.spearman(sc, ys)
            if ic is not None:
                per_lambda_ic[lam].append(ic)
            if do_cap:
                per_lambda_cap[lam].append(cap_of(sc))
            if lam > 0 and ic is not None and ic > base_ic:
                beat_ic[lam] += 1

    base_m, base_icir, _ = v10.mean_icir(per_lambda_ic.get(0.0, []))
    base_cap = round(statistics.mean(per_lambda_cap[0.0]), 4) if per_lambda_cap.get(0.0) else None
    comp_m, comp_icir, comp_nd = v10.mean_icir(comp_only_ic)

    out_lambdas = {}
    for lam in LAMBDAS:
        m, icir, nd = v10.mean_icir(per_lambda_ic.get(lam, []))
        capm = round(statistics.mean(per_lambda_cap[lam]), 4) if per_lambda_cap.get(lam) else None
        out_lambdas["lambda_%.1f" % lam] = {
            "mean_ic": m,
            "icir": icir,
            "n_days": nd,
            "capture_at_topn": capm,
            "days_beat_baseline_ic": ("%d/%d" % (beat_ic[lam], cmp_days)) if lam > 0 else None,
        }

    rec = "edge_only_no_change"
    best = None
    for lam in LAMBDAS:
        if lam <= 0:
            continue
        m, icir, nd = v10.mean_icir(per_lambda_ic.get(lam, []))
        capm = statistics.mean(per_lambda_cap[lam]) if per_lambda_cap.get(lam) else None
        if m is None or base_m is None:
            continue
        cond = (
            m > base_m
            and (base_cap is None or (capm is not None and capm >= base_cap))
            and (base_icir is None or (icir is not None and icir >= 0.9 * base_icir))
            and beat_ic[lam] * 2 >= cmp_days
        )
        if cond and (best is None or m > best[1]):
            best = (lam, m)
    if best is not None:
        rec = "blend_lambda_%.1f" % best[0]

    # 硬门槛: 不依赖某个胜出方案, 仅保证数据充分
    assert n_eval_days >= 3, "too few eval days: %d" % n_eval_days
    assert base_m is not None, "baseline (lambda=0) IC is None"
    assert comp_m is not None, "comp-only IC is None"

    report = {
        "probe": "0094_orthocomp_blend",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "n_eval_days": n_eval_days,
        "cmp_days": cmp_days,
        "median_candidate_pool": int(statistics.median(coverage)) if coverage else None,
        "top_n": TOPN,
        "composite": {"members": [k for (k, _) in QFIELDS], "source": QDATASET, "cutoff": PREOPEN},
        "comp_only_in_candidate_pool": {"mean_ic": comp_m, "icir": comp_icir, "n_days": comp_nd},
        "baseline_edge": {"mean_ic": base_m, "icir": base_icir, "capture_at_topn": base_cap},
        "blends": out_lambdas,
        "recommendation": rec,
        "note": "comp_only IC 是候选池内口径, 必然<=v44 全市场 0.179; 推荐整合需 IC>baseline 且 capture>=baseline 且 ICIR>=0.9*baseline 且过半天数胜.",
    }

    try:
        audit = root / "reports" / "_audit"
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "orthocomp_blend_v0094.json").write_text(
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
