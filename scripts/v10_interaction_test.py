#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v10_interaction_test.py — 交互项掺入测试(只读)。

在现行 v10_amt 核心打分上,按凸组合掺入交互项:
    blend = (1/(1+λ)) * v10_amt_core + (λ/(1+λ)) * interaction
    edge  = clip(blend - risk)
扫描 λ,测量出样本(后半段交易日)的 excess_ret IC / icir / capture@N,
看交互项能否把 v10_amt 再往上抬。λ=0 即现行 v10_amt。

输出: reports/_audit/premarket_interaction_test.json
用法: python3 scripts/v10_interaction_test.py [--top-n 30] [--min-train 5]
"""
from __future__ import annotations
import argparse, csv, json, math, statistics, sys
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
try:
    from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT
except Exception:
    DEFAULT_PROJECT_ROOT = SCRIPTS_DIR.parent / "projects" / "duanxianxia"

V10AMT_W = {"amt_pct": 0.23, "auction_strength": 0.19, "liquidity": 0.18, "money": 0.14, "pressure_score": 0.14, "weimai_strength": 0.08, "orderbook": 0.05}
LAMBDAS = [0.0, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5]


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
    va = sum((x - ma) ** 2 for x in a); vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    return cov / math.sqrt(va * vb)


def spearman(a, b):
    return pearson(rankdata(a), rankdata(b)) if len(a) >= 8 else None


def mean_icir(daily):
    vals = [x for x in daily if x is not None]
    if not vals:
        return None, None, 0
    m = statistics.mean(vals); sd = statistics.pstdev(vals) if len(vals) > 1 else 0.0
    return round(m, 4), (round(m / sd, 3) if sd > 0 else None), len(vals)


class Daily:
    def __init__(self, root):
        self.dir = root / "dailyline" / "stocks"; self.cache = {}
    def excess(self, code, d):
        code = str(code).zfill(6)
        if code not in self.cache:
            data = {}; f = self.dir / f"{code}.csv"
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


def pctl(idx_val):
    pres = sorted(idx_val, key=lambda t: t[1]); m = len(pres)
    return {i: ((r / (m - 1) * 100.0) if m > 1 else 50.0) for r, (i, _) in enumerate(pres)}


def load_days(root, daily):
    out = []
    for dd in sorted((root / "reports").glob("20*-*-*")):
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
            full = rec.get("full") if isinstance(rec.get("full"), dict) else {}
            sub = (full.get("edge_components") or {}).get("sub") or {}
            ad = full.get("auction_detail") or {}
            rows.append({
                "f": {k: fnum(sub.get(k)) for k in ["auction_strength", "liquidity", "money", "pressure_score", "weimai_strength", "orderbook"]},
                "amt_wan": fnum(ad.get("auction_amount_wan")),
                "risk": fnum((full.get("edge_components") or {}).get("risk_penalty"), 0.0),
                "excess": ex,
            })
        if len(rows) < 30:
            continue
        amtp = [(i, rows[i]["amt_wan"]) for i in range(len(rows)) if rows[i]["amt_wan"] is not None]
        amap = pctl(amtp)
        for i, r in enumerate(rows):
            r["amt"] = amap.get(i, 50.0)
            f = r["f"]
            auc = f.get("auction_strength"); liq = f.get("liquidity"); money = f.get("money")
            r["amt_x_auc"] = (r["amt"] / 100.0 * auc) if auc is not None else None
            r["money_x_liq"] = (money / 100.0 * liq) if (money is not None and liq is not None) else None
        out.append({"date": dd.name, "rows": rows})
    return out


def core_score(r):
    g = lambda k: (r["amt"] if k == "amt_pct" else r["f"].get(k))
    s = 0.0
    for k, wk in V10AMT_W.items():
        v = g(k); s += wk * (v if isinstance(v, (int, float)) else 50.0)
    return s


def edge(r, lam, inter):
    base = core_score(r)
    iv = r.get(inter)
    iv = iv if isinstance(iv, (int, float)) else 50.0
    blend = (base + lam * iv) / (1.0 + lam)
    return max(0.0, min(100.0, blend - (r["risk"] or 0.0)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--min-train", type=int, default=5)
    a = ap.parse_args()
    root = Path(a.project_root); daily = Daily(root); topN = a.top_n
    days = load_days(root, daily)
    test_days = days[a.min_train:]

    def evaluate(lam, inter, day_set):
        ics, caps = [], []
        for d in day_set:
            rows = d["rows"]; ex = [r["excess"] for r in rows]
            sc = [edge(r, lam, inter) for r in rows]
            ic = spearman(sc, ex)
            if ic is not None:
                ics.append(ic)
            order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
            winners = set(order[:topN])
            o = sorted(range(len(rows)), key=lambda i: sc[i], reverse=True)
            caps.append(len(set(o[:topN]) & winners) / float(min(topN, len(winners))))
        m, icir, nd = mean_icir(ics)
        return {"oos_mean_ic": m, "oos_icir": icir, "capture": round(statistics.mean(caps), 3) if caps else None}

    result = {"generated_at": datetime.now().isoformat(timespec="seconds"),
              "n_days": len(days), "oos_days": len(test_days), "top_n": topN, "lambdas": LAMBDAS,
              "scans": {}}
    for inter in ["amt_x_auc", "money_x_liq"]:
        scan = []
        for lam in LAMBDAS:
            row = {"lambda": lam}
            row.update(evaluate(lam, inter, test_days))
            full = evaluate(lam, inter, days)
            row["full_mean_ic"] = full["oos_mean_ic"]
            scan.append(row)
        best = max(scan, key=lambda x: (x["oos_mean_ic"] if x["oos_mean_ic"] is not None else -9))
        result["scans"][inter] = {"scan": scan, "best_lambda": best["lambda"], "best_oos_ic": best["oos_mean_ic"]}

    audit = root / "reports" / "_audit"; audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_interaction_test.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
