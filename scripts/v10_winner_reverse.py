#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v10_winner_reverse.py — “赢家反推”分析(只读,不改打分)。

方法(按用户要求的尾部视角):
  每个交易日按真实 excess_ret(=收盘涨幅-竞价涨幅)排序,取 Top-N(默认 30)赢家;
  反推哪些盘前竞价字段/衰生指标能把赢家跟其余区分开。

指标:
  sep          = 赢家在该字段当日百分位的均值 - 50(正=赢家明显偏高;_rank 已方向校正)
  solo_hit     = 只按该字段选 Top-N 能命中多少真赢家 / N
  capture@N    = 现行 edge vs 新 v10 edge 各自 Top-N 能抓住多少真赢家(选股效果直接检验)

输出: <project_root>/reports/_audit/premarket_winner_reverse.json
用法: python3 scripts/v10_winner_reverse.py [--top-n 30] [--project-root PATH]
"""
from __future__ import annotations
import argparse, csv, json, statistics, sys
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

RANK_FIELDS = {"net_amount_rank", "qiangchou_920_925_rank", "qiangchou_last_second_rank"}


def fnum(x, default=None):
    try:
        if x in (None, "", "-", "None"):
            return default
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


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

    def excess(self, code, d):
        data = self.rows(code)
        if d not in data:
            return None
        row = data[d]
        if str(row.get("tradestatus")) not in ("1", "1.0"):
            return None
        o, c, pc = fnum(row.get("open")), fnum(row.get("close")), fnum(row.get("preclose"))
        if not o or not c or not pc:
            return None
        day_pct = fnum(row.get("pctChg"))
        if day_pct is None:
            day_pct = (c - pc) / pc * 100.0
        return day_pct - (o - pc) / pc * 100.0


def raw_fields(r):
    full = r.get("full") if isinstance(r.get("full"), dict) else {}
    sub = (full.get("edge_components") or {}).get("sub") or {}
    ad = full.get("auction_detail") or {}
    return {
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
        "money_intent_score": fnum(ad.get("money_intent_score")),
        "net_pressure": fnum(ad.get("net_pressure")),
        "latest_change_pct": fnum(ad.get("latest_change_pct")),
        "source_evidence_score": fnum(ad.get("source_evidence_score")),
        "auction_amount_wan": fnum(ad.get("auction_amount_wan")),
        "net_amount_rank": fnum(ad.get("net_amount_rank")),
        "qiangchou_920_925_rank": fnum(ad.get("qiangchou_920_925_rank")),
        "qiangchou_last_second_rank": fnum(ad.get("qiangchou_last_second_rank")),
        "edge_stored": fnum(r.get("edge_score")),
        "final_stored": fnum(full.get("final_score")),
        "risk": fnum((full.get("edge_components") or {}).get("risk_penalty"), 0.0),
    }


def derived_fields(f, amt_pct):
    d = {}
    auc, liq, money = f.get("auction_strength"), f.get("liquidity"), f.get("money")
    lcp = f.get("latest_change_pct")
    if auc is not None and lcp is not None:
        d["deriv.auc_minus_8xopen"] = auc - 8.0 * lcp
        d["deriv.lowopen_strength"] = auc if lcp < 2.0 else 0.0
    if money is not None and liq is not None:
        d["deriv.money_x_liq"] = money / 100.0 * liq
    if amt_pct is not None and auc is not None:
        d["deriv.amt_x_auc"] = amt_pct / 100.0 * auc
    return d


def pctl_map(idx_val):
    present = sorted(idx_val, key=lambda t: t[1])
    m = len(present)
    return {i: ((r / (m - 1) * 100.0) if m > 1 else 50.0) for r, (i, _) in enumerate(present)}


def v10_amt_score(f, amt_pct):
    g = lambda x: x if isinstance(x, (int, float)) else 0.0
    core = (0.23 * g(amt_pct) + 0.19 * g(f.get("auction_strength")) + 0.18 * g(f.get("liquidity")) +
            0.14 * g(f.get("money")) + 0.14 * g(f.get("pressure_score")) +
            0.08 * g(f.get("weimai_strength")) + 0.05 * g(f.get("orderbook")))
    return max(0.0, min(100.0, core - g(f.get("risk"))))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    root = Path(args.project_root)
    reports = root / "reports"
    daily = Daily(root)
    topN = args.top_n

    sep_acc = defaultdict(list)      # field -> [daily sep]
    hit_acc = defaultdict(list)      # field -> [daily solo hit rate]
    cover_days = defaultdict(int)
    capture = {"edge_stored": [], "v10_amt": []}
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
            ex = daily.excess(r["code"], dd.name)
            if ex is None:
                continue
            rows.append({"f": raw_fields(r), "excess": ex})
        if len(rows) < max(40, topN + 10):
            continue
        days_used.append(dd.name)
        # 当日成交额百分位
        amtpairs = [(i, rows[i]["f"]["auction_amount_wan"]) for i in range(len(rows)) if rows[i]["f"]["auction_amount_wan"] is not None]
        amtmap = pctl_map(amtpairs)
        for i, c in enumerate(rows):
            c["amt_pct"] = amtmap.get(i, 50.0)
            c["d"] = derived_fields(c["f"], c["amt_pct"])
        # 真赢家 Top-N
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = set(order[:topN])
        # 逐字段(原始 + amt_pct + 衰生)
        allfields = set()
        for c in rows:
            allfields.update(k for k, v in c["f"].items() if k not in ("edge_stored", "final_stored", "risk"))
            allfields.update(c["d"].keys())
        allfields.add("amt_pct")
        for fld in allfields:
            def getv(c):
                if fld == "amt_pct":
                    return c.get("amt_pct")
                if fld.startswith("deriv."):
                    return c["d"].get(fld)
                return c["f"].get(fld)
            idx_val = []
            for i, c in enumerate(rows):
                v = getv(c)
                if v is None:
                    continue
                idx_val.append((i, -v if fld in RANK_FIELDS else v))
            if len(idx_val) < max(20, topN):
                continue
            pm_map = pctl_map(idx_val)
            wp = [pm_map[i] for i in winners if i in pm_map]
            if len(wp) >= 5:
                sep_acc[fld].append(statistics.mean(wp) - 50.0)
            top_by_field = set(i for i, _ in sorted(idx_val, key=lambda t: t[1], reverse=True)[:topN])
            hit_acc[fld].append(len(top_by_field & winners) / float(min(topN, len(winners))))
            cover_days[fld] += 1
        # capture@N
        edge_order = sorted(range(len(rows)), key=lambda i: (rows[i]["f"]["edge_stored"] if rows[i]["f"]["edge_stored"] is not None else -1), reverse=True)
        capture["edge_stored"].append(len(set(edge_order[:topN]) & winners) / float(min(topN, len(winners))))
        v10_order = sorted(range(len(rows)), key=lambda i: v10_amt_score(rows[i]["f"], rows[i]["amt_pct"]), reverse=True)
        capture["v10_amt"].append(len(set(v10_order[:topN]) & winners) / float(min(topN, len(winners))))

    def agg(acc):
        out = []
        for fld, seps in acc.items():
            if not seps:
                continue
            mean_sep = statistics.mean(seps)
            out.append({
                "field": fld,
                "mean_sep": round(mean_sep, 2),
                "sep_std": round(statistics.pstdev(seps), 2) if len(seps) > 1 else 0.0,
                "days_positive": sum(1 for s in seps if s > 0),
                "n_days": len(seps),
                "solo_hit_rate": round(statistics.mean(hit_acc[fld]), 3) if hit_acc.get(fld) else None,
            })
        out.sort(key=lambda x: abs(x["mean_sep"]), reverse=True)
        return out

    all_rows = agg(sep_acc)
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "top_n": topN,
        "n_days": len(days_used),
        "days_used": days_used,
        "legend": {
            "mean_sep": "赢家在该字段当日百分位均值-50;正=赢家偏高(_rank 已方向校正,正=赢家秩偏小)",
            "solo_hit_rate": "只用该字段选 Top-N 命中真赢家的平均比例",
            "capture": "现行 edge vs 新 v10 edge 各自 Top-N 抓住真赢家的平均比例(越高越好)",
        },
        "winner_signals_raw": [r for r in all_rows if not r["field"].startswith("deriv.")],
        "winner_signals_derived": [r for r in all_rows if r["field"].startswith("deriv.")],
        "capture_at_n": {
            "edge_stored": round(statistics.mean(capture["edge_stored"]), 3) if capture["edge_stored"] else None,
            "v10_amt": round(statistics.mean(capture["v10_amt"]), 3) if capture["v10_amt"] else None,
            "n_days": len(capture["edge_stored"]),
            "edge_stored_per_day": [round(x, 3) for x in capture["edge_stored"]],
            "v10_amt_per_day": [round(x, 3) for x in capture["v10_amt"]],
        },
    }
    out_path = Path(args.out) if args.out else (reports / "_audit" / "premarket_winner_reverse.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
