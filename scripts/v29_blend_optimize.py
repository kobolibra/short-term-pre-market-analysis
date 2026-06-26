#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v29_blend_optimize.py — 盘前选股 v11 混合公式实验 (只读)。

动机 (数据驱动, 非猜测):
  - 现行 v10_amt 只用 7 个原始字段, 漏掉了两个最稳定的交互项:
      deriv.amt_x_auc (ICIR≈0.531, 全样本IC最稳) 与 deriv.money_x_liq (ICIR≈0.437)。
  - sparse_ic (用了这两个 deriv) 在 v28 Top5 OOS 上胜出v10_amt。
  => 把这两个 deriv 混入主 edge 公式, 走 walk-forward 验证是否同时括大
     OOS IC / capture@N 与决策相关的 Top5/Top3 当日实际超额。

五个策略 (均逐日横截面):
  v10_amt_raw   : clip(v10.score(CORE,V10AMT_W) - risk, 0, 100)            现行生产口径
  v10_amt_pctl  : CORE_FIELDS 百分位复合, 权重 V10AMT_W                  去量纲对照
  sparse_ic     : SPARSE_W 百分位复合 (Top5 现用)
  ext_fixed     : EXT_FIELDS 百分位复合, 手调权重 EXT_FIXED_W
  ext_learned   : EXT_FIELDS 百分位复合, 权重由训练期正IC归一化学得 (walk-forward)

excess_ret = (close - open) / preclose * 100  (唯一正确的盘前选股口径)

输出: reports/_audit/premarket_blend_v29.{json,md}
用法: python3 scripts/v29_blend_optimize.py [--top-n 30] [--min-train 5]
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
import v10_optimize as v10  # Daily/pctl/field_value/daily_ic/spearman/mean_icir/score/权重常量
import v12_reflection as v12  # load_days_plus

EXT_FIELDS = v10.CORE_FIELDS + ["deriv.amt_x_auc", "deriv.money_x_liq"]
EXT_FIXED_W = {"amt_pct": 0.15, "auction_strength": 0.16, "liquidity": 0.15, "money": 0.10,
               "pressure_score": 0.12, "weimai_strength": 0.05, "orderbook": 0.04,
               "deriv.amt_x_auc": 0.13, "deriv.money_x_liq": 0.10}
SPARSE_W = {"deriv.amt_x_auc": 0.24, "auction_strength": 0.22, "liquidity": 0.18,
            "pressure_score": 0.13, "deriv.money_x_liq": 0.12, "money": 0.11}

STRATEGIES = ["v10_amt_raw", "v10_amt_pctl", "sparse_ic", "ext_fixed", "ext_learned"]


def composite_pctl(rows, weights):
    """逐字段算当日百分位, 按权重加权合成 0..100 分。缺值按 50 位。"""
    n = len(rows)
    fp = {}
    for fld in weights:
        iv = []
        for i in range(n):
            v = v10.field_value(rows[i], fld)
            if v is None:
                continue
            iv.append((i, -v if fld in v10.RANK_FIELDS else v))
        fp[fld] = v10.pctl(iv)
    scores = []
    wsum = sum(weights.values()) or 1.0
    for i in range(n):
        s = 0.0
        for fld, w in weights.items():
            s += w * fp[fld].get(i, 50.0)
        scores.append(s / wsum)
    return scores


def v10_raw_scores(rows):
    out = []
    for r in rows:
        s = v10.score(r["f"], r["amt"], v10.V10AMT_W) - (r["risk"] or 0.0)
        out.append(max(0.0, min(100.0, s)))
    return out


def strat_scores(name, rows, learned_w=None):
    if name == "v10_amt_raw":
        return v10_raw_scores(rows)
    if name == "v10_amt_pctl":
        return composite_pctl(rows, v10.V10AMT_W)
    if name == "sparse_ic":
        return composite_pctl(rows, SPARSE_W)
    if name == "ext_fixed":
        return composite_pctl(rows, EXT_FIXED_W)
    if name == "ext_learned":
        return composite_pctl(rows, learned_w or EXT_FIXED_W)
    raise ValueError(name)


def learn_weights_ext(train_days, fields):
    """EXT_FIELDS 权重 = 训练期每日IC均值取正后归一化。"""
    w = {}
    for fld in fields:
        di = [v10.daily_ic(d["rows"], fld) for d in train_days]
        m, _, _ = v10.mean_icir(di)
        w[fld] = max(m, 0.0) if m is not None else 0.0
    tot = sum(w.values())
    if tot <= 0:
        return dict(EXT_FIXED_W)
    return {k: w[k] / tot for k in fields}


def topk_stat(excesses):
    if not excesses:
        return {"n": 0}
    return {"n": len(excesses),
            "mean_excess": round(statistics.mean(excesses), 3),
            "median_excess": round(statistics.median(excesses), 3),
            "win_rate": round(sum(1 for e in excesses if e > 0) / len(excesses), 3),
            "limitdown_rate": round(sum(1 for e in excesses if e <= -9.5) / len(excesses), 3)}


def topk_excess(scores, rows, k):
    order = sorted(range(len(rows)), key=lambda i: scores[i], reverse=True)[:k]
    return [rows[i]["excess"] for i in order]


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

    ic = {s: [] for s in STRATEGIES}
    cap = {s: [] for s in STRATEGIES}
    top5 = {s: [] for s in STRATEGIES}
    top3 = {s: [] for s in STRATEGIES}
    top5_daily_mean = {s: [] for s in STRATEGIES}
    top3_daily_mean = {s: [] for s in STRATEGIES}
    last_learned = None

    for ti in range(args.min_train, len(days)):
        train = days[:ti]
        test = days[ti]
        rows = test["rows"]
        ex = [r["excess"] for r in rows]
        learned_w = learn_weights_ext(train, EXT_FIELDS)
        last_learned = learned_w
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = set(order[:topN])
        denom = float(min(topN, len(winners))) or 1.0
        for s in STRATEGIES:
            sc = strat_scores(s, rows, learned_w)
            ic[s].append(v10.spearman(sc, ex))
            o = sorted(range(len(rows)), key=lambda i: sc[i], reverse=True)
            cap[s].append(len(set(o[:topN]) & winners) / denom)
            e5 = topk_excess(sc, rows, 5)
            e3 = topk_excess(sc, rows, 3)
            top5[s].extend(e5)
            top3[s].extend(e3)
            if e5:
                top5_daily_mean[s].append(statistics.mean(e5))
            if e3:
                top3_daily_mean[s].append(statistics.mean(e3))

    summary = {}
    for s in STRATEGIES:
        m, icir, nd = v10.mean_icir(ic[s])
        summary[s] = {
            "oos_days": nd,
            "mean_ic": m, "icir": icir,
            "capture_at_n": round(statistics.mean(cap[s]), 3) if cap[s] else None,
            "top5_pooled": topk_stat(top5[s]),
            "top3_pooled": topk_stat(top3[s]),
            "top5_daily_mean_excess": round(statistics.mean(top5_daily_mean[s]), 3) if top5_daily_mean[s] else None,
            "top3_daily_mean_excess": round(statistics.mean(top3_daily_mean[s]), 3) if top3_daily_mean[s] else None,
        }

    # 排名: 以 OOS mean_ic 主, Top5 pooled mean_excess 辅
    rank_ic = sorted(STRATEGIES, key=lambda s: (summary[s]["mean_ic"] or -9), reverse=True)
    rank_top5 = sorted(STRATEGIES, key=lambda s: (summary[s]["top5_pooled"].get("mean_excess", -9) or -9), reverse=True)
    rank_top3 = sorted(STRATEGIES, key=lambda s: (summary[s]["top3_pooled"].get("mean_excess", -9) or -9), reverse=True)

    base_ic = summary["v10_amt_raw"]["mean_ic"] or 0.0
    base_t5 = summary["v10_amt_raw"]["top5_pooled"].get("mean_excess") or 0.0
    verdict = {
        "best_by_ic": rank_ic[0],
        "best_by_top5": rank_top5[0],
        "best_by_top3": rank_top3[0],
        "ext_learned_beats_v10_ic": (summary["ext_learned"]["mean_ic"] or 0) > base_ic,
        "ext_fixed_beats_v10_ic": (summary["ext_fixed"]["mean_ic"] or 0) > base_ic,
        "ext_learned_beats_v10_top5": (summary["ext_learned"]["top5_pooled"].get("mean_excess") or 0) > base_t5,
        "ext_fixed_beats_v10_top5": (summary["ext_fixed"]["top5_pooled"].get("mean_excess") or 0) > base_t5,
    }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "top_n": topN, "min_train": args.min_train,
        "n_days": len(days), "days": [d["date"] for d in days], "n_samples": n_samples,
        "ext_fields": EXT_FIELDS,
        "ext_fixed_w": EXT_FIXED_W, "sparse_w": SPARSE_W,
        "ext_learned_w_fullsample": {k: round(v, 4) for k, v in (learn_weights_ext(days, EXT_FIELDS)).items()},
        "ext_learned_w_last_fold": {k: round(v, 4) for k, v in (last_learned or {}).items()},
        "summary": summary,
        "ranking_by_ic": rank_ic,
        "ranking_by_top5": rank_top5,
        "ranking_by_top3": rank_top3,
        "verdict": verdict,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_blend_v29.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前选股 v29 混合公式实验 (加入高ICIR交互项)", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples} ｜OOS天数: {summary['v10_amt_raw']['oos_days']} ｜Top-N: {topN}", "",
         "## 出样本对比 (walk-forward, 主口径 excess)", "",
         "| 策略 | mean_ic | icir | capture@%d | Top5均值 | Top5胜率 | Top3均值 | Top3胜率 |" % topN,
         "|---|---|---|---|---|---|---|---|"]
    for s in STRATEGIES:
        d = summary[s]
        t5 = d["top5_pooled"]; t3 = d["top3_pooled"]
        L.append(f"| {s} | {d['mean_ic']} | {d['icir']} | {d['capture_at_n']} | {t5.get('mean_excess')} | {t5.get('win_rate')} | {t3.get('mean_excess')} | {t3.get('win_rate')} |")
    L += ["", "## 结论", "",
          f"- OOS IC 最优: **{verdict['best_by_ic']}**",
          f"- Top5 实际超额最优: **{verdict['best_by_top5']}**",
          f"- Top3 实际超额最优: **{verdict['best_by_top3']}**",
          f"- ext_learned 在 IC 上超 v10_amt: **{verdict['ext_learned_beats_v10_ic']}**｜Top5: **{verdict['ext_learned_beats_v10_top5']}**",
          f"- ext_fixed 在 IC 上超 v10_amt: **{verdict['ext_fixed_beats_v10_ic']}**｜Top5: **{verdict['ext_fixed_beats_v10_top5']}**", "",
          "## ext_learned 全样本权重 (若推荐上线 v11)", ""]
    for k, v in report["ext_learned_w_fullsample"].items():
        L.append(f"- `{k}`: {v}")
    L += ["", "> 注: 复合均为逐日横截面百分位加权; v10_amt_raw 为现行生产口径(原始分-风险)。",
          "> 仅当 ext_* 在 OOS IC 与 Top5/Top3 实际超额上同时不劣于 v10_amt 时, 才推荐上线 v11。"]
    (audit / "premarket_blend_v29.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"n_days": len(days), "n_samples": n_samples, "summary": summary, "verdict": verdict}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
