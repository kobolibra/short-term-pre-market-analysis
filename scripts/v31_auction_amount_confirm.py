#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v31_auction_amount_confirm.py — job 0040: 对 0039(v30 边际IC) 结论做决策层 walk-forward 复验 (只读)。

背景:
  job 0039 (v30_marginal_ic) 结论: 在 v10_amt 基础上, 唯一稳健(两个 lambda 均正、多数交易日改善)
  边际提升 OOS IC 的字段是 `auction_amount_wan` (mean_delta +0.0036, 改善 10/15 与 11/15 日);
  `qiangchou_920_925_rank` 两个 lambda 也为正但仅 5/15 日改善, 存疑。
  注意: auction_amount_wan 正是 amt_pct 的原始来源(amt_pct=其当日横截面百分位), 这与 HANDOFF §2.6
  结论2(原始幅度 > 逐日百分位)一致——百分位抹掉的量纲信息可能仍有残余 alpha。

按 HANDOFF §4.1 分支2: 某字段两个 lambda 都稳健正增量 -> 以小权重加入 edge 公式, 再走 walk-forward 复验。
但 §2.3 要求改线上公式前必须过 walk-forward 出样本验证, 且 v29 的上线门槛是: 仅当 OOS IC 与 Top3/Top5
实际超额同时不劣于 v10_amt 才推荐上线。

本作业 = 决策层复验:
  base   = v10_amt_raw = clip(score(CORE,V10AMT_W) - risk, 0, 100)   现行生产口径
  变体   = clip(base + w * z_day(field), 0, 100)
           field in {auction_amount_wan, qiangchou_920_925_rank}
           w     in {2,4,6,8}  (z 为当日横截面标准分, rank 字段方向翻转)
  walk-forward(min_train=5) 逐日出样本, 比较 OOS IC / capture@N / Top3 与 Top5 池化超额(均值/中位/胜率/跌停率)。

excess_ret = (close - open)/preclose*100  (唯一正确盘前口径)
推荐上线门槛: 变体在 OOS IC 与 Top3/Top5 实际超额上同时不劣于 v10_amt, 且不抬高 Top3 跌停率。
输出: reports/_audit/premarket_auction_confirm_v31.{json,md}
用法: python3 scripts/v31_auction_amount_confirm.py [--top-n 30] [--min-train 5]
"""
from __future__ import annotations
import argparse
import json
import statistics
import sys
import traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12

CAND_FIELDS = ["auction_amount_wan", "qiangchou_920_925_rank"]
WEIGHTS = [2.0, 4.0, 6.0, 8.0]


def zscore(pairs):
    vs = [v for _, v in pairs]
    if len(vs) < 2:
        return {i: 0.0 for i, _ in pairs}
    m = statistics.mean(vs)
    sd = statistics.pstdev(vs)
    if sd <= 0:
        return {i: 0.0 for i, _ in pairs}
    return {i: (v - m) / sd for i, v in pairs}


def day_z(rows, fld):
    pairs = []
    for i, r in enumerate(rows):
        v = v10.field_value(r, fld)
        if v is None:
            continue
        pairs.append((i, -v if fld in v10.RANK_FIELDS else v))
    return zscore(pairs)


def base_raw_scores(rows):
    out = []
    for r in rows:
        s = v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r["risk"] or 0.0)
        out.append(max(0.0, min(100.0, s)))
    return out


def variant_scores(base, rows, fld, w):
    z = day_z(rows, fld)
    return [max(0.0, min(100.0, base[i] + w * z.get(i, 0.0))) for i in range(len(rows))]


def topk_excess(scores, rows, k):
    order = sorted(range(len(rows)), key=lambda i: scores[i], reverse=True)[:k]
    return [rows[i]["excess"] for i in order]


def topk_stat(xs):
    if not xs:
        return {"n": 0}
    return {"n": len(xs),
            "mean_excess": round(statistics.mean(xs), 3),
            "median_excess": round(statistics.median(xs), 3),
            "win_rate": round(sum(1 for e in xs if e > 0) / len(xs), 3),
            "limitdown_rate": round(sum(1 for e in xs if e <= -9.5) / len(xs), 3)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--top-n", type=int, default=30)
    ap.add_argument("--min-train", type=int, default=5)
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    topN = args.top_n
    days = v12.load_days_plus(root, daily)
    n_samples = sum(len(d["rows"]) for d in days)

    strategies = ["v10_amt_raw"] + [f"{fld}|w{int(w)}" for fld in CAND_FIELDS for w in WEIGHTS]
    ic = {s: [] for s in strategies}
    cap = {s: [] for s in strategies}
    top3 = {s: [] for s in strategies}
    top5 = {s: [] for s in strategies}
    top3_dm = {s: [] for s in strategies}
    top5_dm = {s: [] for s in strategies}

    for ti in range(args.min_train, len(days)):
        test = days[ti]
        rows = test["rows"]
        ex = [r["excess"] for r in rows]
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = set(order[:topN])
        denom = float(min(topN, len(winners))) or 1.0
        base = base_raw_scores(rows)
        for s in strategies:
            if s == "v10_amt_raw":
                sc = base
            else:
                fld, wtag = s.split("|w")
                sc = variant_scores(base, rows, fld, float(wtag))
            ic[s].append(v10.spearman(sc, ex))
            o = sorted(range(len(rows)), key=lambda i: sc[i], reverse=True)
            cap[s].append(len(set(o[:topN]) & winners) / denom)
            e3 = topk_excess(sc, rows, 3)
            e5 = topk_excess(sc, rows, 5)
            top3[s].extend(e3)
            top5[s].extend(e5)
            if e3:
                top3_dm[s].append(statistics.mean(e3))
            if e5:
                top5_dm[s].append(statistics.mean(e5))

    summary = {}
    for s in strategies:
        m, icir, nd = v10.mean_icir(ic[s])
        summary[s] = {
            "oos_days": nd,
            "mean_ic": m, "icir": icir,
            "capture_at_n": round(statistics.mean(cap[s]), 3) if cap[s] else None,
            "top3_pooled": topk_stat(top3[s]),
            "top5_pooled": topk_stat(top5[s]),
            "top3_daily_mean_excess": round(statistics.mean(top3_dm[s]), 3) if top3_dm[s] else None,
            "top5_daily_mean_excess": round(statistics.mean(top5_dm[s]), 3) if top5_dm[s] else None,
        }

    base_s = summary["v10_amt_raw"]
    b_ic = base_s["mean_ic"] or 0.0
    b_t3 = base_s["top3_pooled"].get("mean_excess") or 0.0
    b_t3_win = base_s["top3_pooled"].get("win_rate") or 0.0
    b_t3_ld = base_s["top3_pooled"].get("limitdown_rate")
    b_t3_ld = b_t3_ld if b_t3_ld is not None else 1.0
    b_t5 = base_s["top5_pooled"].get("mean_excess") or 0.0

    evals = []
    for s in strategies:
        if s == "v10_amt_raw":
            continue
        d = summary[s]
        v_ic = d["mean_ic"] or 0.0
        v_t3 = d["top3_pooled"].get("mean_excess") or 0.0
        v_t3_win = d["top3_pooled"].get("win_rate") or 0.0
        v_t3_ld = d["top3_pooled"].get("limitdown_rate")
        v_t3_ld = v_t3_ld if v_t3_ld is not None else 1.0
        v_t5 = d["top5_pooled"].get("mean_excess") or 0.0
        checks = {
            "ic_ge_base": v_ic >= b_ic,
            "top3_mean_ge_base": v_t3 >= b_t3,
            "top3_win_ge_base": v_t3_win >= b_t3_win,
            "top3_limitdown_le_base": v_t3_ld <= b_t3_ld,
            "top5_mean_ge_base": v_t5 >= b_t5,
        }
        evals.append({
            "strategy": s,
            "mean_ic": d["mean_ic"], "ic_delta": round(v_ic - b_ic, 4),
            "top3_mean_excess": v_t3, "top3_mean_delta": round(v_t3 - b_t3, 3),
            "top5_mean_excess": v_t5,
            "checks": checks,
            "recommended": all(checks.values()),
        })
    evals.sort(key=lambda e: (e["recommended"], e["top3_mean_delta"], e["ic_delta"]), reverse=True)
    recommended = [e["strategy"] for e in evals if e["recommended"]]

    verdict = {
        "base_oos_ic": base_s["mean_ic"],
        "base_top3_mean_excess": b_t3,
        "base_oos_days": base_s["oos_days"],
        "any_variant_recommended": len(recommended) > 0,
        "recommended_variants": recommended,
        "best_variant": evals[0]["strategy"] if evals else None,
        "best_variant_recommended": evals[0]["recommended"] if evals else None,
    }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0040_premarket_auction_amount_confirm_v31",
        "top_n": topN, "min_train": args.min_train,
        "n_days": len(days), "days": [d["date"] for d in days], "n_samples": n_samples,
        "candidate_fields": CAND_FIELDS, "weights": WEIGHTS,
        "summary": summary, "evaluations": evals, "verdict": verdict,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_auction_confirm_v31.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前选股 v31 — auction_amount_wan 决策层 walk-forward 复验 (job 0040)", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples} ｜OOS天数: {base_s['oos_days']} ｜Top-N: {topN}",
         f"- base v10_amt_raw: OOS IC **{base_s['mean_ic']}**, Top3均超 **{b_t3}**, Top3胜率 {b_t3_win}", "",
         "## 各变体出样本对比 (walk-forward, 主口径 excess)", "",
         "| 策略 | OOS IC | ΔIC | cap@%d | Top3 均/中位/胜率/跌停 | Top5 均/胜率 | 推荐 |" % topN,
         "|---|---|---|---|---|---|---|"]
    for s in strategies:
        d = summary[s]
        t3 = d["top3_pooled"]; t5 = d["top5_pooled"]
        rec = ""
        dic = ""
        if s != "v10_amt_raw":
            ev = next((e for e in evals if e["strategy"] == s), None)
            if ev:
                rec = "YES" if ev["recommended"] else ""
                dic = f"{ev['ic_delta']:+}"
        L.append(f"| {s} | {d['mean_ic']} | {dic} | {d['capture_at_n']} | "
                 f"{t3.get('mean_excess')}/{t3.get('median_excess')}/{t3.get('win_rate')}/{t3.get('limitdown_rate')} | "
                 f"{t5.get('mean_excess')}/{t5.get('win_rate')} | {rec} |")
    L += ["", "## 结论", "",
          f"- 是否有变体在 IC 与 Top3/Top5 上同时不劣于生产: **{verdict['any_variant_recommended']}**",
          f"- 推荐变体: {verdict['recommended_variants'] or '无'}",
          f"- 最佳变体: **{verdict['best_variant']}** (推荐={verdict['best_variant_recommended']})", "",
          "> 门槛(v29 口径): 变体须 OOS IC>=base 且 Top3/Top5 实际超额>=base 且 Top3 跌停率<=base, 才推荐上线 v11。",
          "> auction_amount_wan 是 amt_pct 的原始来源; 若小权重变体稳健胜出, 即印证 §2.6『原始幅度>百分位』仍有残余 alpha。"]
    (audit / "premarket_auction_confirm_v31.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"verdict": verdict, "evaluations": evals[:6],
                      "base": {"mean_ic": base_s["mean_ic"], "top3_pooled": base_s["top3_pooled"]}},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
