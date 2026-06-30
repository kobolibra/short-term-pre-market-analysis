#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0096 orthocomp 生产路径字段映射验证探针(只读)。

背景: 0094/0095 在全候选池上证实 BC_orthogonal(turnover_rate_pct + latest_change_pct,
取自 qiangchou capture)以 lambda=0.4 混入 edge_score 可提升 mean_excess@10(0.78->1.135)
与 IC(0.0912->0.0981)。生产已将该混合接入 assemble_v9(commit d64f7138)。

但生产读取的是 compute_auction_strengths 产出的 auction_detail 字段:
  auction_turnover_pct = _first_pct(v_row -> qg_row -> ql_row -> n_row, turnover_keys)
  latest_change_pct    = _first_pct(n_row -> v_row -> qg_row -> ql_row -> f_row -> w_row, pct_keys)
即生产值来自跨 vratio/qiangchou/netamount 多数据集的回退链,而 0095 只从 qiangchou 单源
取 raw turnover_rate_pct/latest_change_pct。本探针验证该字段映射是否携带同一信号:
  1) 直接 import 生产 compute_auction_strengths, 用生产 cutoff(092900,earliest_before)加载
     真实 captures 跑出生产 auction_detail, 取 auction_turnover_pct/latest_change_pct。
  2) 复现已提交的混合逻辑(z(turn)+z(gap) 均值为 comp, (1-lam)*z_edge+lam*comp, 再标准化
     回 edge 均值/标准差), 测全候选池 IC/ICIR + mean_excess@{10,20,30} vs baseline edge。
  3) 同时比对生产派生 B/C 与 0095 raw qiangchou B/C 的 per-code 相关性(字段映射等价性)。
闸门通过 => PRODUCTION_PATH_CONFIRMED; 否则 PRODUCTION_PATH_REGRESSION(建议将
 edge_orthocomp_lambda 默认置 0 禁用)。只读。输出 reports/_audit/orthocomp_prodpath_v0096.json。
用法: python3 scripts/duanxianxia_0096_orthocomp_prodpath_probe_20260630.py
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
import duanxianxia_v7_2_auction_strength as v72as

# 生产 cutoff / dataset id(与 duanxianxia_v7_1_data_loader 一致)
PREMARKET_CUTOFF = "092900"
DS_VRATIO = "auction.jjyd.vratio"
DS_QIANGCHOU = "auction.jjyd.qiangchou"
DS_NETAMOUNT = "auction.jjyd.net_amount"
DS_FENGDAN = "auction.jjlive.fengdan"
DS_WEIMAI = "auction.jjyd.weimai"
CODE_KEYS = ["code", "\u4ee3\u7801"]
LAMBDA = 0.4
TOPK = [10, 20, 30]
MIN_ROWS = 30
MIN_Z = 3  # 与生产 _blend_zscores 一致: <3 个有效值不标准化


def pnum(x):
    """与生产 _blend_num 一致的数值解析(去 % 逗号)。"""
    if x in (None, "", "-", "None"):
        return None
    try:
        return float(str(x).replace("%", "").replace(",", "").strip())
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


def earliest_preopen_rows(date_dir, dsid, cutoff=PREMARKET_CUTOFF):
    """镜像生产 load_capture_at_time(pick='earliest_before', max_hhmmss=cutoff)。"""
    d = date_dir / dsid
    if not d.is_dir():
        return []
    elig = sorted(f for f in d.glob("*.json") if len(f.stem) == 6 and f.stem.isdigit() and f.stem <= cutoff)
    if not elig:
        return []
    try:
        payload = json.loads(elig[0].read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def zmap(valmap, codes):
    vals = [valmap[c] for c in codes if c in valmap and valmap[c] is not None]
    if len(vals) < MIN_Z:
        return {}
    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / len(vals)
    sd = math.sqrt(var)
    if sd == 0:
        return {}
    return {c: (valmap[c] - m) / sd for c in codes if c in valmap and valmap[c] is not None}


def edge_score_row(r):
    return max(0.0, min(100.0, v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r.get("risk") or 0.0)))


def mean_excess_at(scores_by_code, ex, codes, k):
    order = sorted(codes, key=lambda c: scores_by_code[c], reverse=True)
    sel = order[:min(k, len(order))]
    if not sel:
        return None
    return sum(ex[c] for c in sel) / len(sel)


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return None
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return cov / (sx * sy)


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    days = v10.load_days(root, daily)
    cap_root = root / "captures"

    base_ic = []
    base_me = {k: [] for k in TOPK}
    blend_ic = []
    blend_me = {k: [] for k in TOPK}
    beat_ic = 0
    beat_me10 = 0
    cmp_days = 0
    coverage = []
    # 字段映射等价性: 生产派生 vs 0095 raw qiangchou(pooled per-code)
    prodB_all, rawB_all, prodC_all, rawC_all = [], [], [], []
    match_b = {"both": 0, "prod_only": 0, "raw_only": 0}
    match_c = {"both": 0, "prod_only": 0, "raw_only": 0}

    for day in days:
        date = day["date"]
        rows = day["rows"]
        cdir = cap_root / date
        if not cdir.is_dir():
            continue
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
        ys = [ex[c] for c in codes]
        bic = v10.spearman([zeb[c] for c in codes], ys)
        if bic is None:
            continue

        # —— 生产路径: 加载 5 个 auction 数据集(生产 cutoff/pick) -> compute_auction_strengths ——
        vratio = earliest_preopen_rows(cdir, DS_VRATIO)
        qiangchou = earliest_preopen_rows(cdir, DS_QIANGCHOU)
        netamount = earliest_preopen_rows(cdir, DS_NETAMOUNT)
        fengdan = earliest_preopen_rows(cdir, DS_FENGDAN)
        weimai = earliest_preopen_rows(cdir, DS_WEIMAI)
        if not qiangchou:
            continue
        try:
            detail = v72as.compute_auction_strengths(codes, vratio, qiangchou, netamount, fengdan, {}, weimai_rows=weimai)
        except Exception:
            continue

        prod_turn = {}
        prod_gap = {}
        for c in codes:
            d = detail.get(c) or {}
            prod_turn[c] = pnum(d.get("auction_turnover_pct"))
            prod_gap[c] = pnum(d.get("latest_change_pct"))

        # 0095 raw qiangchou 单源(用于字段映射等价性比对)
        qidx = {}
        for r in qiangchou:
            c = code_of(r)
            if c:
                qidx.setdefault(c, r)
        raw_turn = {c: pnum(qidx[c].get("turnover_rate_pct")) for c in qidx}
        raw_gap = {c: pnum(qidx[c].get("latest_change_pct")) for c in qidx}
        for c in codes:
            pb, rb = prod_turn.get(c), raw_turn.get(c)
            if pb is not None and rb is not None:
                match_b["both"] += 1
                prodB_all.append(pb)
                rawB_all.append(rb)
            elif pb is not None:
                match_b["prod_only"] += 1
            elif rb is not None:
                match_b["raw_only"] += 1
            pc, rc = prod_gap.get(c), raw_gap.get(c)
            if pc is not None and rc is not None:
                match_c["both"] += 1
                prodC_all.append(pc)
                rawC_all.append(rc)
            elif pc is not None:
                match_c["prod_only"] += 1
            elif rc is not None:
                match_c["raw_only"] += 1

        # —— 复现生产混合(BC, lambda=0.4) 于生产派生字段 ——
        z_turn = zmap(prod_turn, codes)
        z_gap = zmap(prod_gap, codes)
        if not z_turn and not z_gap:
            continue
        comp = {}
        for c in codes:
            parts = [z for z in (z_turn.get(c), z_gap.get(c)) if z is not None]
            comp[c] = (sum(parts) / len(parts)) if parts else 0.0
        blended = {c: (1.0 - LAMBDA) * zeb[c] + LAMBDA * comp[c] for c in codes}
        # 重标准化回 edge 均值/标准差(与生产一致: 只改排序)
        bz = zmap(blended, codes)
        if not bz:
            continue
        es = [edge_raw[c] for c in codes]
        em = sum(es) / len(es)
        evar = sum((x - em) ** 2 for x in es) / len(es)
        esd = math.sqrt(evar)
        new_edge = {c: max(0.0, min(100.0, em + bz.get(c, 0.0) * esd)) for c in codes}

        nic = v10.spearman([new_edge[c] for c in codes], ys)
        if nic is None:
            continue
        cmp_days += 1
        coverage.append(sum(1 for c in codes if prod_turn.get(c) is not None or prod_gap.get(c) is not None))
        base_ic.append(bic)
        blend_ic.append(nic)
        if nic > bic:
            beat_ic += 1
        base_me10_day = mean_excess_at(zeb, ex, codes, 10)
        blend_me10_day = mean_excess_at(new_edge, ex, codes, 10)
        if blend_me10_day is not None and base_me10_day is not None and blend_me10_day > base_me10_day:
            beat_me10 += 1
        for k in TOPK:
            bm = mean_excess_at(zeb, ex, codes, k)
            nm = mean_excess_at(new_edge, ex, codes, k)
            if bm is not None:
                base_me[k].append(bm)
            if nm is not None:
                blend_me[k].append(nm)

    bm_ic, bm_icir, _ = v10.mean_icir(base_ic)
    nm_ic, nm_icir, _ = v10.mean_icir(blend_ic)
    base_me_mean = {("me%d" % k): (round(statistics.mean(base_me[k]), 3) if base_me[k] else None) for k in TOPK}
    blend_me_mean = {("me%d" % k): (round(statistics.mean(blend_me[k]), 3) if blend_me[k] else None) for k in TOPK}
    base_me10 = base_me_mean["me10"]
    blend_me10 = blend_me_mean["me10"]

    corr_b = pearson(prodB_all, rawB_all)
    corr_c = pearson(prodC_all, rawC_all)

    assert cmp_days >= 3, "too few cmp days: %d" % cmp_days
    assert bm_ic is not None and base_me10 is not None, "baseline metrics None"

    field_ok = (corr_b is not None and corr_b >= 0.9) or (corr_c is not None and corr_c >= 0.9)
    lift_ok = (
        nm_ic is not None and nm_ic > bm_ic
        and blend_me10 is not None and blend_me10 >= base_me10
        and (bm_icir is None or (nm_icir is not None and nm_icir >= bm_icir))
        and beat_me10 * 2 >= cmp_days
    )
    if lift_ok and field_ok:
        rec = "PRODUCTION_PATH_CONFIRMED"
    elif lift_ok and not field_ok:
        rec = "LIFT_OK_BUT_FIELD_MAPPING_DIVERGES_REVIEW"
    else:
        rec = "PRODUCTION_PATH_REGRESSION_SET_LAMBDA_0"

    report = {
        "probe": "0096_orthocomp_prodpath",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "lambda": LAMBDA,
        "cmp_days": cmp_days,
        "median_with_comp": int(statistics.median(coverage)) if coverage else None,
        "topk": TOPK,
        "baseline_edge": {"mean_ic": bm_ic, "icir": bm_icir, "mean_excess_at": base_me_mean},
        "prodpath_blend": {
            "mean_ic": nm_ic,
            "icir": nm_icir,
            "mean_excess_at": blend_me_mean,
            "days_beat_ic": "%d/%d" % (beat_ic, cmp_days),
            "days_beat_me10": "%d/%d" % (beat_me10, cmp_days),
        },
        "field_mapping_equivalence": {
            "turnover_corr_prod_vs_raw": corr_b,
            "gap_corr_prod_vs_raw": corr_c,
            "turnover_match": match_b,
            "gap_match": match_c,
        },
        "recommendation": rec,
        "note": "\u751f\u4ea7\u8def\u5f84\u9a8c\u8bc1: import compute_auction_strengths \u8dd1\u771f\u5b9e captures(cutoff 092900,earliest_before)\u53d6 auction_turnover_pct/latest_change_pct \u590d\u73b0\u5df2\u63a5\u5165\u7684 BC@lam0.4 \u6df7\u5408; \u95f8\u95e8\u987b IC>baseline \u4e14 me10>=baseline \u4e14 ICIR>=baseline \u4e14\u8fc7\u534a\u5929 me10 \u80dc \u4e14\u5b57\u6bb5\u6620\u5c04 corr>=0.9; \u4e0d\u8fbe\u6807 => \u5efa\u8bae edge_orthocomp_lambda \u9ed8\u8ba4\u7f6e 0 \u7981\u7528\u3002",
    }

    try:
        audit = root / "reports" / "_audit"
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "orthocomp_prodpath_v0096.json").write_text(
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
