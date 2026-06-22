#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v10_field_ic.py — 盘前竞价字段“有效性反推”分析（只读，不改任何打分逻辑）。

对每个交易日 reports/<date>/premarket/*_analysis_v9.json 的 all_candidates，
逐字段抽取盘前竞价指标，join dailyline/stocks/<code>.csv 的真实结果，
按“每日横截面 Spearman 秩相关(IC)”评估每个字段对前向收益的预测力。

结果口径（主口径 = 盘前选股超额）：
  excess_ret   = 当天收盘涨幅 - 当天竞价涨幅 = pctChg - (open-preclose)/preclose
               = (close-open)/preclose*100   ← 唯一正确的盘前选股验证口径
  day_pct      = pctChg                       ← 当日收盘涨幅（辅助）
  next_day_pct = 次日 pctChg                   ← 辅助

解读：|mean_daily_ic| 越大越有效；≈0=噪音(可降权/剔除)；
     *_rank 字段(秩，越小越好) IC 为负才算有效。

输出: <project_root>/reports/_audit/premarket_field_ic.json
用法: python3 scripts/v10_field_ic.py [--project-root PATH]
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

# 主口径 excess_ret 排第一；另两个为辅助
OUTCOMES = ["excess_ret", "day_pct", "next_day_pct"]


def fnum(x, default=None):
    try:
        if x in (None, "", "-", "None"):
            return default
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


class Daily:
    def __init__(self, root: Path):
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
        excess_ret = day_pct - auction_pct   # = (close-open)/preclose*100
        nxt = None
        i = dts.index(d)
        if i + 1 < len(dts):
            nxt = fnum(data[dts[i + 1]].get("pctChg"))
        return {
            "excess_ret": excess_ret,
            "day_pct": day_pct,
            "next_day_pct": nxt,
            "auction_pct": auction_pct,
        }


def fields_of(r):
    full = r.get("full") if isinstance(r.get("full"), dict) else {}
    sub = (full.get("edge_components") or {}).get("sub") or {}
    ad = full.get("auction_detail") or {}
    wd = full.get("weimai_detail") or {}
    td = full.get("theme_detail") or {}
    f = {
        "edge_score": fnum(r.get("edge_score")),
        "final_score": fnum(full.get("final_score")),
    }
    for k in ("auction_strength", "low_cost", "money", "pressure_score", "weimai_strength",
              "orderbook", "liquidity", "theme_strength_t0", "market_env_score",
              "cashflow_continuity_score", "longtou_score"):
        f["sub." + k] = fnum(sub.get(k))
    for k in ("money_intent_score", "net_amount_rank", "net_pressure", "latest_change_pct",
              "auction_amount_wan", "source_evidence_score", "orderbook_quality_score",
              "liquidity_score", "qiangchou_920_925_rank", "qiangchou_last_second_rank"):
        f["auction." + k] = fnum(ad.get(k))
    f["weimai.weimai_strength"] = fnum(wd.get("weimai_strength"))
    f["weimai.weimai_amount_wan"] = fnum(wd.get("weimai_amount_wan"))
    for k in ("t0_plate_inflow_wan", "t0_limitup_count", "plate_strength_rank", "plate_inflow_rank"):
        f["theme." + k] = fnum(td.get(k))
    return f


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

    daily_ic = defaultdict(lambda: defaultdict(list))      # field -> outcome -> [per-day ic]
    pooled = defaultdict(lambda: defaultdict(lambda: ([], [])))  # field -> outcome -> (xs, ys)
    coverage = defaultdict(int)
    days_used = []
    total_cands = 0
    matched_cands = 0
    money_total = 0
    money_fallback = 0

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
        day_rows = []
        for r in cands:
            if not isinstance(r, dict) or not r.get("code"):
                continue
            total_cands += 1
            f = fields_of(r)
            money_total += 1
            if (f["auction.money_intent_score"] in (None, 0, 0.0)) and (f["auction.net_amount_rank"] is not None):
                money_fallback += 1
            oc = daily.outcomes(r["code"], dd.name)
            if oc is None:
                continue
            matched_cands += 1
            day_rows.append((f, oc))
        if len(day_rows) < 8:
            continue
        days_used.append(dd.name)
        all_fields = set()
        for f, _ in day_rows:
            all_fields.update(f.keys())
        for fld in all_fields:
            for oc_key in OUTCOMES:
                xs = [f.get(fld) for f, _ in day_rows]
                ys = [oc.get(oc_key) for _, oc in day_rows]
                ic = spearman(xs, ys)
                if ic is not None:
                    daily_ic[fld][oc_key].append(ic)
                px, py = pooled[fld][oc_key]
                for xv, yv in zip(xs, ys):
                    if xv is not None and yv is not None:
                        px.append(xv)
                        py.append(yv)
            coverage[fld] += sum(1 for f, _ in day_rows if f.get(fld) is not None)

    field_ic = {}
    for oc_key in OUTCOMES:
        rows = []
        for fld in daily_ic:
            ics = daily_ic[fld][oc_key]
            if not ics:
                continue
            mean_ic = statistics.mean(ics)
            std_ic = statistics.pstdev(ics) if len(ics) > 1 else 0.0
            px, py = pooled[fld][oc_key]
            pooled_ic = spearman(px, py)
            rows.append({
                "field": fld,
                "mean_daily_ic": round(mean_ic, 4),
                "ic_std": round(std_ic, 4),
                "icir": round(mean_ic / std_ic, 3) if std_ic else None,
                "n_days": len(ics),
                "pooled_ic": round(pooled_ic, 4) if pooled_ic is not None else None,
                "coverage": coverage.get(fld, 0),
            })
        rows.sort(key=lambda x: abs(x["mean_daily_ic"]), reverse=True)
        field_ic[oc_key] = rows

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(root),
        "days_used": days_used,
        "n_days": len(days_used),
        "total_candidates_scanned": total_cands,
        "candidates_with_outcome": matched_cands,
        "money_bug_audit": {
            "total_candidates": money_total,
            "fallback_to_net_amount_rank": money_fallback,
            "fallback_pct": round(money_fallback / money_total * 100, 2) if money_total else None,
            "note": "money_intent_score 缺失/为0 时旧代码回退到 net_amount_rank(秩,方向相反、量纲不同)。",
        },
        "legend": {
            "excess_ret": "主口径=当天收盘涨幅-当天竞价涨幅=(close-open)/preclose*100",
            "reading": "|mean_daily_ic| 越大越有效; *_rank 字段负 IC 才算有效; icir=mean_ic/ic_std 越高越稳定",
        },
        "field_ic": field_ic,
    }

    out_path = Path(args.out) if args.out else (reports / "_audit" / "premarket_field_ic.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
