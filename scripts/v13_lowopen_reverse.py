#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v13_lowopen_reverse.py — 被丢弃的赢家(低开/风险位)alpha 反推(只读)。

v12 反思报告发现: 每日 Top-30 真实赢家绝大多数是 LOW_OPEN_WEAK / 低开票且
risk_flag=True, 被模型 DROP 或低排名(304 例排名问题)。本脚本量化这块被丢弃的超额:
  1) 低开 vs 高开 cohort 的 excess 分布; risk_flag True vs False cohort 对比。
  2) 每日 Top-N 赢家中低开票 / 风险位票占比。
  3) 低开 cohort 内部: 哪些字段能区分“会反包大涨”的低开票(per-day IC + 赢家倒推)。
  4) 用低开 cohort 内最有区分度的字段做简单选择器, 估算 capture@N 对比现行 edge。

low_open 定义: auction_detail.latest_change_pct < --low-open-max (默认 2.0)
excess = (close-open)/preclose*100

输出: reports/_audit/premarket_lowopen_reverse.{json,md}
用法: python3 scripts/v13_lowopen_reverse.py [--low-open-max 2.0] [--top-n 30]
"""
from __future__ import annotations
import argparse
import json
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
import v12_reflection as v12

FLDS = ["amt_pct", "auction_strength", "liquidity", "money", "pressure_score", "weimai_strength",
        "orderbook", "low_cost", "theme_strength_t0", "market_env_score", "cashflow_continuity_score",
        "longtou_score", "net_pressure", "source_evidence_score", "auction_amount_wan",
        "net_amount_rank", "qiangchou_920_925_rank", "qiangchou_last_second_rank",
        "deriv.money_x_liq", "deriv.amt_x_auc"]


def _stat(xs):
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "mean": round(statistics.mean(xs), 3), "median": round(statistics.median(xs), 3),
            "win_rate": round(sum(1 for e in xs if e > 0) / len(xs), 3),
            "p90": round(sorted(xs)[int(0.9 * (len(xs) - 1))], 2),
            "limitup_rate": round(sum(1 for e in xs if e >= 9.5) / len(xs), 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--low-open-max", type=float, default=2.0)
    ap.add_argument("--top-n", type=int, default=30)
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    thr = args.low_open_max
    topN = args.top_n
    days = v12.load_days_plus(root, daily)

    def is_lo(r):
        v = r["f"].get("latest_change_pct")
        return v is not None and v < thr

    lo_ex, hi_ex = [], []
    winner_lo_share, winner_rf_share = [], []
    rf_ex, nrf_ex = [], []
    for d in days:
        d["lo"] = [r for r in d["rows"] if is_lo(r)]
        lo_ex += [r["excess"] for r in d["lo"]]
        hi_ex += [r["excess"] for r in d["rows"] if not is_lo(r)]
        rf_ex += [r["excess"] for r in d["rows"] if r["risk_flag"]]
        nrf_ex += [r["excess"] for r in d["rows"] if not r["risk_flag"]]
        order = sorted(d["rows"], key=lambda r: r["excess"], reverse=True)[:topN]
        if order:
            winner_lo_share.append(sum(1 for r in order if is_lo(r)) / float(len(order)))
            winner_rf_share.append(sum(1 for r in order if r["risk_flag"]) / float(len(order)))

    field_ic = []
    for fld in FLDS:
        di = [v10.daily_ic(d["lo"], fld) for d in days if len(d["lo"]) >= 8]
        m, icir, nd = v10.mean_icir(di)
        if m is not None:
            field_ic.append({"field": fld, "mean_ic": m, "icir": icir, "n_days": nd})
    field_ic.sort(key=lambda x: abs(x["mean_ic"]), reverse=True)

    sep_acc, hit_acc = defaultdict(list), defaultdict(list)
    for d in days:
        lo = d["lo"]
        if len(lo) < max(15, topN // 2):
            continue
        k = min(topN, max(3, len(lo) // 5))
        order = sorted(range(len(lo)), key=lambda i: lo[i]["excess"], reverse=True)
        winners = set(order[:k])
        for fld in FLDS:
            iv = []
            for i, r in enumerate(lo):
                v = v10.field_value(r, fld)
                if v is None:
                    continue
                iv.append((i, -v if fld in v10.RANK_FIELDS else v))
            if len(iv) < 12:
                continue
            pm = v10.pctl(iv)
            wp = [pm[i] for i in winners if i in pm]
            if len(wp) >= 3:
                sep_acc[fld].append(statistics.mean(wp) - 50.0)
            topf = set(i for i, _ in sorted(iv, key=lambda t: t[1], reverse=True)[:k])
            hit_acc[fld].append(len(topf & winners) / float(min(k, len(winners))))
    winner_rev = []
    for fld in FLDS:
        if not sep_acc.get(fld):
            continue
        seps = sep_acc[fld]
        winner_rev.append({"field": fld, "mean_sep": round(statistics.mean(seps), 2),
                           "days_positive": sum(1 for s in seps if s > 0), "n_days": len(seps),
                           "hit_rate": round(statistics.mean(hit_acc[fld]), 3) if hit_acc.get(fld) else None})
    winner_rev.sort(key=lambda x: abs(x["mean_sep"]), reverse=True)

    top_flds = [w["field"] for w in winner_rev[:3]]
    cap_sel, cap_edge = [], []
    for d in days:
        lo = d["lo"]
        if len(lo) < topN:
            continue
        order = sorted(range(len(lo)), key=lambda i: lo[i]["excess"], reverse=True)
        winners = set(order[:topN])

        def pct_of(fld):
            iv = [(i, (-v10.field_value(lo[i], fld) if fld in v10.RANK_FIELDS else v10.field_value(lo[i], fld)))
                  for i in range(len(lo)) if v10.field_value(lo[i], fld) is not None]
            return v10.pctl(iv)
        pmaps = {f: pct_of(f) for f in top_flds}
        sel = []
        for i in range(len(lo)):
            s = sum(pmaps[f].get(i, 50.0) for f in top_flds)
            sel.append((i, s))
        sel_top = set(i for i, _ in sorted(sel, key=lambda t: t[1], reverse=True)[:topN])
        edge_top = set(sorted(range(len(lo)),
                              key=lambda i: (lo[i]["edge_old"] if lo[i]["edge_old"] is not None else -1.0),
                              reverse=True)[:topN])
        denom = float(min(topN, len(winners))) or 1.0
        cap_sel.append(len(sel_top & winners) / denom)
        cap_edge.append(len(edge_top & winners) / denom)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "low_open_max": thr, "top_n": topN,
        "n_days": len(days), "days": [d["date"] for d in days],
        "cohort_open": {"low_open": _stat(lo_ex), "high_open": _stat(hi_ex)},
        "cohort_risk": {"risk_true": _stat(rf_ex), "risk_false": _stat(nrf_ex)},
        "winner_lowopen_share_mean": round(statistics.mean(winner_lo_share), 3) if winner_lo_share else None,
        "winner_risk_share_mean": round(statistics.mean(winner_rf_share), 3) if winner_rf_share else None,
        "lowopen_field_ic": field_ic,
        "lowopen_winner_reverse": winner_rev,
        "selector_top_fields": top_flds,
        "selector_capture_at_n": round(statistics.mean(cap_sel), 3) if cap_sel else None,
        "edge_capture_at_n_lowopen": round(statistics.mean(cap_edge), 3) if cap_edge else None,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_lowopen_reverse.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    co = report["cohort_open"]
    cr = report["cohort_risk"]
    L = ["# 低开反包 / 风险位 alpha 反推", "",
         f"- 生成: {report['generated_at']} ｜交易日: {len(days)} ｜低开阈值: latest_change_pct<{thr}", "",
         "## cohort excess 对比", "", "| cohort | n | mean | median | win_rate | p90 | 涨停率 |", "|---|---|---|---|---|---|---|",
         f"| 低开 | {co['low_open'].get('n')} | {co['low_open'].get('mean')} | {co['low_open'].get('median')} | {co['low_open'].get('win_rate')} | {co['low_open'].get('p90')} | {co['low_open'].get('limitup_rate')} |",
         f"| 高开 | {co['high_open'].get('n')} | {co['high_open'].get('mean')} | {co['high_open'].get('median')} | {co['high_open'].get('win_rate')} | {co['high_open'].get('p90')} | {co['high_open'].get('limitup_rate')} |",
         f"| risk_flag=True | {cr['risk_true'].get('n')} | {cr['risk_true'].get('mean')} | {cr['risk_true'].get('median')} | {cr['risk_true'].get('win_rate')} | {cr['risk_true'].get('p90')} | {cr['risk_true'].get('limitup_rate')} |",
         f"| risk_flag=False | {cr['risk_false'].get('n')} | {cr['risk_false'].get('mean')} | {cr['risk_false'].get('median')} | {cr['risk_false'].get('win_rate')} | {cr['risk_false'].get('p90')} | {cr['risk_false'].get('limitup_rate')} |",
         "", f"- 每日 Top-{topN} 赢家中低开票占比(均值): **{report['winner_lowopen_share_mean']}**",
         f"- 每日 Top-{topN} 赢家中风险位票占比(均值): **{report['winner_risk_share_mean']}**",
         f"- 低开内: 现行 edge capture@{topN} = **{report['edge_capture_at_n_lowopen']}** ; 新选择器(字段={top_flds}) capture@{topN} = **{report['selector_capture_at_n']}**", "",
         "## 低开 cohort 内字段 IC", "", "| 字段 | mean_ic | icir | n_days |", "|---|---|---|---|"]
    for r in field_ic:
        L.append(f"| {r['field']} | {r['mean_ic']} | {r['icir']} | {r['n_days']} |")
    L += ["", "## 低开 cohort 赢家倒推(字段百分位 - 50)", "", "| 字段 | mean_sep | 正向天数/总 | hit_rate |", "|---|---|---|---|"]
    for r in winner_rev:
        L.append(f"| {r['field']} | {r['mean_sep']} | {r['days_positive']}/{r['n_days']} | {r['hit_rate']} |")
    (audit / "premarket_lowopen_reverse.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
