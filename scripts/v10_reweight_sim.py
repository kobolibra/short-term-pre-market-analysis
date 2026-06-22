#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v10_reweight_sim.py — 用已落盘的 edge_components.sub 分量,在历史数据上
重算几套“新权重 edge”,并测它们对 excess_ret 的 IC。纯只读,不改任何生产打分。

目的:在动 compute_edge_v9.py 之前,先证明新权重的 excess IC > 现行 edge_score(≈0.0724)。

口径与 v10_field_ic.py 一致: excess_ret=收盘涨幅-竞价涨幅=(close-open)/preclose*100。
输出: <project_root>/reports/_audit/premarket_reweight_sim.json
用法: python3 scripts/v10_reweight_sim.py [--project-root PATH]
"""
from __future__ import annotations
import argparse, csv, json, math, statistics, sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
try:
    from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT
except Exception:
    DEFAULT_PROJECT_ROOT = SCRIPTS_DIR.parent / "projects" / "duanxianxia"

OUTCOMES = ["excess_ret", "day_pct", "next_day_pct"]


def fnum(x, default=None):
    try:
        if x in (None, "", "-", "None"):
            return default
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def g(x):
    return x if isinstance(x, (int, float)) else 0.0


def clip(x):
    return max(0.0, min(100.0, x))


class Daily:
    def __init__(self, root):
        self.dir = root / "dailyline" / "stocks"
        self.cache = {}

    def rows(self, code):
        code = str(code).zfill(6)
        if code in self.cache:
            return self.cache[code]
        f = self.dir / f"{code}.csv"
        data = {}
        if f.exists():
            with open(f, newline="") as fh:
                for r in csv.DictReader(fh):
                    data[r["date"]] = r
        self.cache[code] = data
        return data

    def outcomes(self, code, d):
        data = self.rows(code)
        if d not in data:
            return None
        dts = sorted(data)
        row = data[d]
        if str(row.get("tradestatus")) not in ("1", "1.0"):
            return None
        o, c, pc = fnum(row.get("open")), fnum(row.get("close")), fnum(row.get("preclose"))
        if not o or not c or not pc:
            return None
        auction_pct = (o - pc) / pc * 100.0
        day_pct = fnum(row.get("pctChg"))
        if day_pct is None:
            day_pct = (c - pc) / pc * 100.0
        nxt = None
        i = dts.index(d)
        if i + 1 < len(dts):
            nxt = fnum(data[dts[i + 1]].get("pctChg"))
        return {"excess_ret": day_pct - auction_pct, "day_pct": day_pct, "next_day_pct": nxt}


def comps(r):
    full = r.get("full") if isinstance(r.get("full"), dict) else {}
    ec = full.get("edge_components") or {}
    sub = ec.get("sub") or {}
    ad = full.get("auction_detail") or {}
    return {
        "code": r.get("code"),
        "auc": fnum(sub.get("auction_strength")),
        "liq": fnum(sub.get("liquidity")),
        "money": fnum(sub.get("money")),
        "pressure": fnum(sub.get("pressure_score")),
        "weimai": fnum(sub.get("weimai_strength")),
        "orderbook": fnum(sub.get("orderbook")),
        "low_cost": fnum(sub.get("low_cost")),
        "theme": fnum(sub.get("theme_strength_t0")),
        "env": fnum(sub.get("market_env_score")),
        "continuity": fnum(sub.get("cashflow_continuity_score")),
        "longtou": fnum(sub.get("longtou_score")),
        "source": fnum(ad.get("source_evidence_score")),
        "risk": fnum(ec.get("risk_penalty"), 0.0),
        "amt": fnum(ad.get("auction_amount_wan")),
        "edge_stored": fnum(r.get("edge_score")),
        "final_stored": fnum(full.get("final_score")),
    }


# ---- 几套待测 edge 变体(均 0-100,减 risk 后 clip) ----
def v_baseline_recon(c):
    main = 0.45 * g(c["auc"]) + 0.25 * max(g(c["money"]), g(c["pressure"])) + 0.20 * g(c["low_cost"]) + 0.10 * min(100.0, g(c["source"]) * 3)
    aux = 0.55 * g(c["weimai"]) + 0.30 * g(c["orderbook"]) + 0.15 * g(c["liq"])
    bg = 0.45 * g(c["theme"]) + 0.25 * g(c["env"]) + 0.20 * g(c["continuity"]) + 0.10 * g(c["longtou"])
    return clip(0.50 * main + 0.22 * aux + 0.28 * bg - g(c["risk"]))


def v10_icw(c):
    # 按 excess IC 归一化权重(不用新数据),剔除 low_cost/source/background,保留 risk
    blend = (0.24 * g(c["auc"]) + 0.24 * g(c["liq"]) + 0.18 * g(c["money"]) +
             0.17 * g(c["pressure"]) + 0.10 * g(c["weimai"]) + 0.07 * g(c["orderbook"]))
    return clip(blend - g(c["risk"]))


def v10_amt(c):
    # 加入竞价成交额的当日横截面百分位 amt_pct(缺失=50 中性)
    blend = (0.23 * g(c.get("amt_pct")) + 0.19 * g(c["auc"]) + 0.18 * g(c["liq"]) +
             0.14 * g(c["money"]) + 0.14 * g(c["pressure"]) + 0.08 * g(c["weimai"]) + 0.05 * g(c["orderbook"]))
    return clip(blend - g(c["risk"]))


VARIANTS = {
    "edge_stored": lambda c: c["edge_stored"],
    "final_stored": lambda c: c["final_stored"],
    "baseline_recon": v_baseline_recon,
    "v10_icw": v10_icw,
    "v10_amt": v10_amt,
}


def spearman(xs, ys):
    p = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(p) < 8:
        return None

    def rk(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                j += 1
            avg = (i + j) / 2.0
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r

    xr, yr = rk([a for a, _ in p]), rk([b for _, b in p])
    n = len(p)
    mx, my = sum(xr) / n, sum(yr) / n
    num = sum((a - mx) * (b - my) for a, b in zip(xr, yr))
    den = math.sqrt(sum((a - mx) ** 2 for a in xr) * sum((b - my) ** 2 for b in yr))
    return (num / den) if den else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    root = Path(args.project_root)
    reports = root / "reports"
    daily = Daily(root)

    daily_ic = defaultdict(lambda: defaultdict(list))   # variant -> outcome -> [per-day ic]
    pooled = defaultdict(lambda: defaultdict(lambda: ([], [])))
    days_used = []

    for dd in sorted(reports.glob("20*-*-*")):
        pm = dd / "premarket"
        if not pm.is_dir():
            continue
        files = sorted(pm.glob("*_analysis_v9.json"))
        if not files:
            continue
        try:
            analysis = json.loads(files[0].read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list) or not cands:
            continue
        rows = []
        for r in cands:
            if not isinstance(r, dict) or not r.get("code"):
                continue
            oc = daily.outcomes(r["code"], dd.name)
            if oc is None:
                continue
            c = comps(r)
            c["_oc"] = oc
            rows.append(c)
        if len(rows) < 8:
            continue
        # 当日 amt 百分位
        present = sorted([(i, rows[i]["amt"]) for i in range(len(rows)) if rows[i]["amt"] is not None], key=lambda t: t[1])
        m = len(present)
        for rank, (i, _) in enumerate(present):
            rows[i]["amt_pct"] = (rank / (m - 1) * 100.0) if m > 1 else 50.0
        for c in rows:
            c.setdefault("amt_pct", 50.0)
        days_used.append(dd.name)
        for vname, fn in VARIANTS.items():
            scores = [fn(c) for c in rows]
            for oc_key in OUTCOMES:
                ys = [c["_oc"].get(oc_key) for c in rows]
                ic = spearman(scores, ys)
                if ic is not None:
                    daily_ic[vname][oc_key].append(ic)
                px, py = pooled[vname][oc_key]
                for xv, yv in zip(scores, ys):
                    if xv is not None and yv is not None:
                        px.append(xv)
                        py.append(yv)

    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "n_days": len(days_used),
        "days_used": days_used,
        "baseline_target": {"edge_score_excess_ic": 0.0724, "final_score_excess_ic": 0.0689},
        "note": "比 edge_stored 高且更稳(icir)的变体才值得 port 进 compute_edge_v9.py。amt_pct=当日竞价成交额横截面百分位,缺失=50。",
        "variants": {},
    }
    for vname in VARIANTS:
        out["variants"][vname] = {}
        for oc_key in OUTCOMES:
            ics = daily_ic[vname][oc_key]
            if not ics:
                out["variants"][vname][oc_key] = None
                continue
            mean_ic = statistics.mean(ics)
            std_ic = statistics.pstdev(ics) if len(ics) > 1 else 0.0
            px, py = pooled[vname][oc_key]
            pic = spearman(px, py)
            out["variants"][vname][oc_key] = {
                "mean_daily_ic": round(mean_ic, 4),
                "ic_std": round(std_ic, 4),
                "icir": round(mean_ic / std_ic, 3) if std_ic else None,
                "pooled_ic": round(pic, 4) if pic is not None else None,
                "n_days": len(ics),
            }

    out_path = Path(args.out) if args.out else (reports / "_audit" / "premarket_reweight_sim.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
