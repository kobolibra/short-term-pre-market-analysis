#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v10_optimize.py — 盘前选股一体化优化管线(只读分析 + 数据补齐)。

一次跑完成:
  1) 数据吃满: 对有 captures 但缺 v9 分析的交易日,调用 v9 runner 补生成(只补缺,不覆盖)。
  2) 赢家倒推: 每日按真实 excess 取 Top-N 赢家,反推字段/衰生指标区分度。
  3) 逐字段 IC: 全样本每日横截面 Spearman IC。
  4) walk-forward: 仅用过去的天拟合权重,在未来的天上检验,得出真实出样本 IC。
  5) capture@N + 推荐权重 + 报告。

excess_ret = 收盘涨幅 - 竞价涨幅 = (close - open) / preclose * 100

输出:
  reports/_audit/premarket_master_report.json
  reports/_audit/premarket_master_report.md

用法:
  python3 scripts/v10_optimize.py [--top-n 30] [--min-train 5] [--no-regen]
"""
from __future__ import annotations
import argparse, csv, json, math, statistics, sys, traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
try:
    from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT
except Exception:
    DEFAULT_PROJECT_ROOT = SCRIPTS_DIR.parent / "projects" / "duanxianxia"

CORE_FIELDS = ["amt_pct", "auction_strength", "liquidity", "money", "pressure_score", "weimai_strength", "orderbook"]
RANK_FIELDS = {"net_amount_rank", "qiangchou_920_925_rank", "qiangchou_last_second_rank"}
V10AMT_W = {"amt_pct": 0.23, "auction_strength": 0.19, "liquidity": 0.18, "money": 0.14, "pressure_score": 0.14, "weimai_strength": 0.08, "orderbook": 0.05}


def fnum(x, d=None):
    try:
        if x in (None, "", "-", "None"):
            return d
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:
        return d


def rankdata(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def pearson(a, b):
    n = len(a)
    if n < 3:
        return None
    ma, mb = sum(a) / n, sum(b) / n
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    return cov / math.sqrt(va * vb)


def spearman(a, b):
    if len(a) < 3:
        return None
    return pearson(rankdata(a), rankdata(b))


def mean_icir(daily):
    vals = [x for x in daily if x is not None]
    if not vals:
        return None, None, 0
    m = statistics.mean(vals)
    sd = statistics.pstdev(vals) if len(vals) > 1 else 0.0
    icir = (m / sd) if sd > 0 else None
    return round(m, 4), (round(icir, 3) if icir is not None else None), len(vals)


class Daily:
    def __init__(self, root):
        self.dir = root / "dailyline" / "stocks"
        self.cache = {}

    def excess(self, code, d):
        code = str(code).zfill(6)
        if code not in self.cache:
            data = {}
            f = self.dir / f"{code}.csv"
            if f.exists():
                with open(f, newline="") as fh:
                    for r in csv.DictReader(fh):
                        data[r["date"]] = r
            self.cache[code] = data
        row = self.cache[code].get(d)
        if not row or str(row.get("tradestatus")) not in ("1", "1.0"):
            return None
        o, c, pc = fnum(row.get("open")), fnum(row.get("close")), fnum(row.get("preclose"))
        if not o or not c or not pc:
            return None
        return (c - o) / pc * 100.0


def extract(rec):
    full = rec.get("full") if isinstance(rec.get("full"), dict) else {}
    sub = (full.get("edge_components") or {}).get("sub") or {}
    ad = full.get("auction_detail") or {}
    f = {
        "auction_strength": fnum(sub.get("auction_strength")),
        "liquidity": fnum(sub.get("liquidity")),
        "money": fnum(sub.get("money")),
        "pressure_score": fnum(sub.get("pressure_score")),
        "weimai_strength": fnum(sub.get("weimai_strength")),
        "orderbook": fnum(sub.get("orderbook")),
        "low_cost": fnum(sub.get("low_cost")),
        "theme_strength_t0": fnum(sub.get("theme_strength_t0")),
        "market_env_score": fnum(sub.get("market_env_score")),
        "cashflow_continuity_score": fnum(sub.get("cashflow_continuity_score")),
        "longtou_score": fnum(sub.get("longtou_score")),
        "net_pressure": fnum(ad.get("net_pressure")),
        "latest_change_pct": fnum(ad.get("latest_change_pct")),
        "source_evidence_score": fnum(ad.get("source_evidence_score")),
        "auction_amount_wan": fnum(ad.get("auction_amount_wan")),
        "net_amount_rank": fnum(ad.get("net_amount_rank")),
        "qiangchou_920_925_rank": fnum(ad.get("qiangchou_920_925_rank")),
        "qiangchou_last_second_rank": fnum(ad.get("qiangchou_last_second_rank")),
    }
    return {"code": str(rec.get("code") or "").strip(), "f": f,
            "edge_old": fnum(rec.get("edge_score")), "final": fnum(full.get("final_score")),
            "risk": fnum((full.get("edge_components") or {}).get("risk_penalty"), 0.0)}


def pctl(idx_val):
    pres = sorted(idx_val, key=lambda t: t[1])
    m = len(pres)
    return {i: ((r / (m - 1) * 100.0) if m > 1 else 50.0) for r, (i, _) in enumerate(pres)}


def derived(f, amt):
    d, auc, liq, money, lcp = {}, f.get("auction_strength"), f.get("liquidity"), f.get("money"), f.get("latest_change_pct")
    if auc is not None and lcp is not None:
        d["deriv.auc_minus_8xopen"] = auc - 8.0 * lcp
        d["deriv.lowopen_strength"] = auc if lcp < 2.0 else 0.0
    if money is not None and liq is not None:
        d["deriv.money_x_liq"] = money / 100.0 * liq
    if amt is not None and auc is not None:
        d["deriv.amt_x_auc"] = amt / 100.0 * auc
    return d


def score(f, amt, w):
    g = lambda k: (amt if k == "amt_pct" else f.get(k))
    s = 0.0
    for k, wk in w.items():
        v = g(k)
        s += wk * (v if isinstance(v, (int, float)) else 50.0)
    return s


def regen_missing(root, daily_dummy):
    """对有 captures 但缺 v9 分析的交易日补生成 v9。只补缺,不覆盖;逐日 try。"""
    res = {"succeeded": [], "failed": [], "skipped_existing": 0}
    cap_dir = root / "captures"
    if not cap_dir.is_dir():
        res["failed"].append({"date": "*", "err": "no captures dir"})
        return res
    try:
        import duanxianxia_premarket_v9_runner as v9r
    except Exception as e:
        res["failed"].append({"date": "*", "err": f"import runner failed: {e}"})
        return res
    for dd in sorted(cap_dir.glob("20*-*-*")):
        date_str = dd.name
        pm = root / "reports" / date_str / "premarket"
        if pm.is_dir() and list(pm.glob("*_analysis_v9.json")):
            res["skipped_existing"] += 1
            continue
        try:
            v9r.run_v9(date_str, root, output_dir=None, no_write=False)
            res["succeeded"].append(date_str)
        except Exception as e:
            res["failed"].append({"date": date_str, "err": f"{type(e).__name__}: {e}"[:200]})
    return res


def load_days(root, daily):
    """返回 [{date, rows:[{code,f,amt,edge_old,final,risk,excess}]}],按日期升序。"""
    out = []
    rep = root / "reports"
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if not files:
            continue
        try:
            analysis = json.loads(files[-1].read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list) or not cands:
            continue
        rows = []
        for rec in cands:
            if not isinstance(rec, dict) or not rec.get("code"):
                continue
            ex = daily.excess(rec["code"], dd.name)
            if ex is None:
                continue
            e = extract(rec)
            e["excess"] = ex
            rows.append(e)
        if len(rows) < 30:
            continue
        amtp = [(i, rows[i]["f"]["auction_amount_wan"]) for i in range(len(rows)) if rows[i]["f"]["auction_amount_wan"] is not None]
        amap = pctl(amtp)
        for i, r in enumerate(rows):
            r["amt"] = amap.get(i, 50.0)
            r["d"] = derived(r["f"], r["amt"])
        out.append({"date": dd.name, "rows": rows})
    return out


def field_value(r, fld):
    if fld == "amt_pct":
        return r.get("amt")
    if fld.startswith("deriv."):
        return r["d"].get(fld)
    return r["f"].get(fld)


def daily_ic(rows, fld):
    xs, ys = [], []
    for r in rows:
        v = field_value(r, fld)
        if v is None:
            continue
        xs.append(-v if fld in RANK_FIELDS else v)
        ys.append(r["excess"])
    return spearman(xs, ys) if len(xs) >= 8 else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--min-train", type=int, default=5)
    ap.add_argument("--no-regen", action="store_true")
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = Daily(root)
    topN = args.top_n

    regen = {"attempted": False}
    if not args.no_regen:
        regen = {"attempted": True}
        regen.update(regen_missing(root, daily))

    days = load_days(root, daily)
    n_samples = sum(len(d["rows"]) for d in days)

    # 全字段列表(原始 + amt_pct + 衰生)
    raw_flds = ["amt_pct"] + [k for k in ["auction_strength", "liquidity", "money", "pressure_score", "weimai_strength",
        "orderbook", "low_cost", "theme_strength_t0", "market_env_score", "cashflow_continuity_score", "longtou_score",
        "net_pressure", "latest_change_pct", "source_evidence_score", "auction_amount_wan", "net_amount_rank",
        "qiangchou_920_925_rank", "qiangchou_last_second_rank"]]
    deriv_flds = ["deriv.auc_minus_8xopen", "deriv.lowopen_strength", "deriv.money_x_liq", "deriv.amt_x_auc"]

    # 3) 逐字段 IC
    field_ic = []
    for fld in raw_flds + deriv_flds:
        di = [daily_ic(d["rows"], fld) for d in days]
        m, icir, nd = mean_icir(di)
        if m is not None:
            field_ic.append({"field": fld, "mean_ic": m, "icir": icir, "n_days": nd})
    field_ic.sort(key=lambda x: abs(x["mean_ic"]), reverse=True)

    # 2) 赢家倒推
    sep_acc, hit_acc, ndays = defaultdict(list), defaultdict(list), defaultdict(int)
    for d in days:
        rows = d["rows"]
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = set(order[:topN])
        for fld in raw_flds + deriv_flds:
            iv = []
            for i, r in enumerate(rows):
                v = field_value(r, fld)
                if v is None:
                    continue
                iv.append((i, -v if fld in RANK_FIELDS else v))
            if len(iv) < max(20, topN):
                continue
            pm = pctl(iv)
            wp = [pm[i] for i in winners if i in pm]
            if len(wp) >= 5:
                sep_acc[fld].append(statistics.mean(wp) - 50.0)
            topf = set(i for i, _ in sorted(iv, key=lambda t: t[1], reverse=True)[:topN])
            hit_acc[fld].append(len(topf & winners) / float(min(topN, len(winners))))
            ndays[fld] += 1

    def winner_rows(flds):
        out = []
        for fld in flds:
            if not sep_acc.get(fld):
                continue
            seps = sep_acc[fld]
            out.append({"field": fld, "mean_sep": round(statistics.mean(seps), 2),
                "days_positive": sum(1 for s in seps if s > 0), "n_days": len(seps),
                "solo_hit_rate": round(statistics.mean(hit_acc[fld]), 3) if hit_acc.get(fld) else None})
        out.sort(key=lambda x: abs(x["mean_sep"]), reverse=True)
        return out

    # 4) walk-forward: 用训练日的正 IC 归一化作为权重,在测试日出样本检验
    def learn_weights(train_days):
        w = {}
        for fld in CORE_FIELDS:
            di = [daily_ic(d["rows"], fld) for d in train_days]
            m, _, nd = mean_icir(di)
            w[fld] = max(m, 0.0) if (m is not None) else 0.0
        tot = sum(w.values())
        if tot <= 0:
            return {k: V10AMT_W[k] for k in CORE_FIELDS}
        return {k: w[k] / tot for k in CORE_FIELDS}

    def edge_score_row(r, w):
        return max(0.0, min(100.0, score(r["f"], r["amt"], w) - (r["risk"] or 0.0)))

    wf_learned, wf_v10amt, wf_old, wf_final = [], [], [], []
    cap_learned, cap_v10amt, cap_old = [], [], []
    for ti in range(args.min_train, len(days)):
        train = days[:ti]
        test = days[ti]
        w = learn_weights(train)
        rows = test["rows"]
        ex = [r["excess"] for r in rows]
        wf_learned.append(spearman([edge_score_row(r, w) for r in rows], ex))
        wf_v10amt.append(spearman([edge_score_row(r, V10AMT_W) for r in rows], ex))
        wf_old.append(spearman([(r["edge_old"] if r["edge_old"] is not None else 0.0) for r in rows], ex))
        wf_final.append(spearman([(r["final"] if r["final"] is not None else 0.0) for r in rows], ex))
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = set(order[:topN])
        def cap(scores):
            o = sorted(range(len(rows)), key=lambda i: scores[i], reverse=True)
            return len(set(o[:topN]) & winners) / float(min(topN, len(winners)))
        cap_learned.append(cap([edge_score_row(r, w) for r in rows]))
        cap_v10amt.append(cap([edge_score_row(r, V10AMT_W) for r in rows]))
        cap_old.append(cap([(r["edge_old"] if r["edge_old"] is not None else 0.0) for r in rows]))

    # 全样本推荐权重(用于部署)
    rec_w = learn_weights(days)
    lm, licir, lnd = mean_icir(wf_learned)
    am, aicir, _ = mean_icir(wf_v10amt)
    om, oicir, _ = mean_icir(wf_old)
    fm, ficir, _ = mean_icir(wf_final)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "top_n": topN, "min_train": args.min_train,
        "n_days": len(days), "days_used": [d["date"] for d in days], "n_samples": n_samples,
        "data_regen": regen,
        "field_ic": field_ic,
        "winner_reverse_raw": winner_rows(raw_flds),
        "winner_reverse_derived": winner_rows(deriv_flds),
        "walk_forward": {
            "oos_days": lnd,
            "learned": {"mean_ic": lm, "icir": licir},
            "v10_amt_fixed": {"mean_ic": am, "icir": aicir},
            "edge_old_stored": {"mean_ic": om, "icir": oicir},
            "final_stored": {"mean_ic": fm, "icir": ficir},
            "capture_at_n": {
                "learned": round(statistics.mean(cap_learned), 3) if cap_learned else None,
                "v10_amt": round(statistics.mean(cap_v10amt), 3) if cap_v10amt else None,
                "edge_old": round(statistics.mean(cap_old), 3) if cap_old else None,
            },
        },
        "recommended_weights_fullsample": {k: round(v, 4) for k, v in rec_w.items()},
    }

    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_master_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # markdown
    L = ["# 盘前选股优化主报告", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples} ｜补生成: {regen.get('succeeded')} ｜失败: {regen.get('failed')}",
         f"- 出样本天数(walk-forward): {lnd}", "",
         "## walk-forward 出样本表现(主口径 excess_ret)", "",
         "| 模型 | mean_ic | icir | capture@%d |" % topN, "|---|---|---|---|",
         f"| 学到的权重 | {lm} | {licir} | {report['walk_forward']['capture_at_n']['learned']} |",
         f"| v10_amt(固定) | {am} | {aicir} | {report['walk_forward']['capture_at_n']['v10_amt']} |",
         f"| 现行 edge(stored) | {om} | {oicir} | {report['walk_forward']['capture_at_n']['edge_old']} |",
         f"| final(stored) | {fm} | {ficir} | - |", "",
         "## 推荐生产权重(全样本 IC 归一化)", ""]
    for k, v in rec_w.items():
        L.append(f"- `{k}`: {round(v,4)}")
    L += ["", "## 赢家倒推 Top-%d(原始字段,按|mean_sep|)" % topN, "", "| 字段 | mean_sep | 正向天数/总 | solo_hit |", "|---|---|---|---|"]
    for r in report["winner_reverse_raw"]:
        L.append(f"| {r['field']} | {r['mean_sep']} | {r['days_positive']}/{r['n_days']} | {r['solo_hit_rate']} |")
    L += ["", "## 赢家倒推(衰生指标)", "", "| 指标 | mean_sep | 正向天数/总 | solo_hit |", "|---|---|---|---|"]
    for r in report["winner_reverse_derived"]:
        L.append(f"| {r['field']} | {r['mean_sep']} | {r['days_positive']}/{r['n_days']} | {r['solo_hit_rate']} |")
    L += ["", "## 逐字段 IC(excess_ret)", "", "| 字段 | mean_ic | icir | n_days |", "|---|---|---|---|"]
    for r in field_ic:
        L.append(f"| {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} |")
    (audit / "premarket_master_report.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
