#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_0093_factor_refit_probe_20260630.py -- Task 0093 因子权重重拟合验证探针 (只读).

目的: 在不盲改线上 edge_core 权重的前提下, 用历史全部有 v9 分析的交易日,
walk-forward 出样本比较 baseline(V10AMT_W = 现行生产权重) vs 多个证据驱动的
候选权重方案的 OOS Spearman IC / ICIR / capture@TopN, 选出稳健(非过拟合)最优方案.

证据来源(只读, reports/_audit):
  - premarket_clean_ic_v39 / premarket_marginal_ic_v30 / decorrelated_composite_v44
  - 结论: auction_amount(amt_pct) 是最强单因子(IC~0.16); source_evidence/auction_strength
    边际贡献最弱(marginal -0.012); 线性模型已近局部最优, 收益来自“重配”而非加新字段.

方法(低过拟合):
  复用 v10_optimize.load_days / score / Daily.excess / spearman / mean_icir.
  所有候选方案都只在现有 7 个 CORE_FIELDS 上重配权重(不引入新字段, 杜绝过拟合):
    CORE = [amt_pct, auction_strength, liquidity, money, pressure_score, weimai_strength, orderbook]
  - S0_baseline       = V10AMT_W (现行生产)
  - S1_ic_walkforward = 每个测试日用其之前所有训练日的正 IC 占比作权重 (自适应, 作参照)
  - S2..S5            = 固定手工方案(不按日拟合 -> 不可能过拟合), 依证据上调 amt_pct/liquidity,
                       下调 auction_strength/weimai/orderbook.
  评估: 每个方案在 ti>=MIN_TRAIN 的测试日算 edge_score=clip(score(f,amt,W)-risk,0,100) vs excess
        的横截面 IC, 汇总 mean_ic / icir / capture@TopN 以及“逐方案对 baseline 的逐日胜率”.

excess=(close-open)/preclose*100. 输出 stdout JSON 摘要. rc=0 = 探针成功运行.
用法: python3 scripts/duanxianxia_0093_factor_refit_probe_20260630.py
"""
from __future__ import annotations
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

CORE = list(v10.CORE_FIELDS)
BASELINE = dict(v10.V10AMT_W)
BASE_SUM = sum(BASELINE.values())
MIN_TRAIN = 5
TOPN = 30

# 证据驱动的固定候选方案(相对权重; 之后归一到 baseline 总和, 保持与 risk 罚分的相对尺度一致)
RAW_SCHEMES = {
    "S2_amt_tilt":     {"amt_pct": 0.30, "auction_strength": 0.10, "liquidity": 0.20, "money": 0.14, "pressure_score": 0.15, "weimai_strength": 0.05, "orderbook": 0.03},
    "S3_lean_amt":     {"amt_pct": 0.34, "auction_strength": 0.08, "liquidity": 0.22, "money": 0.15, "pressure_score": 0.14, "weimai_strength": 0.04, "orderbook": 0.03},
    "S4_mild_reweight": {"amt_pct": 0.28, "auction_strength": 0.12, "liquidity": 0.20, "money": 0.16, "pressure_score": 0.16, "weimai_strength": 0.05, "orderbook": 0.03},
    "S5_amt_liq_core": {"amt_pct": 0.32, "auction_strength": 0.09, "liquidity": 0.24, "money": 0.16, "pressure_score": 0.14, "weimai_strength": 0.03, "orderbook": 0.02},
}


def _norm_to(w, target):
    s = sum(w.values())
    if s <= 0:
        return {f: 0.0 for f in CORE}
    k = target / s
    return {f: round(w.get(f, 0.0) * k, 4) for f in CORE}


SCHEMES = {"S0_baseline": {f: round(BASELINE.get(f, 0.0), 4) for f in CORE}}
for _name, _w in RAW_SCHEMES.items():
    SCHEMES[_name] = _norm_to(_w, BASE_SUM)


def edge_score_row(r, w):
    return max(0.0, min(100.0, v10.score(r["f"], r["amt"], w) - (r["risk"] or 0.0)))


def learn_weights(train_days):
    w = {}
    for fld in CORE:
        di = [v10.daily_ic(d["rows"], fld) for d in train_days]
        m, _, _ = v10.mean_icir(di)
        w[fld] = max(m, 0.0) if (m is not None) else 0.0
    tot = sum(w.values())
    if tot <= 0:
        return dict(SCHEMES["S0_baseline"])
    return {k: round(w[k] / tot * BASE_SUM, 4) for k in CORE}


def cap_at_n(rows, scores, winners, topn):
    o = sorted(range(len(rows)), key=lambda i: scores[i], reverse=True)
    denom = float(min(topn, len(winners))) or 1.0
    return len(set(o[:topn]) & set(winners)) / denom


def run():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    days = v10.load_days(root, daily)
    n_days = len(days)
    n_samples = sum(len(d["rows"]) for d in days)

    # 全样本逐 CORE 字段 IC (透明度参照)
    field_ic = {}
    for fld in CORE:
        di = [v10.daily_ic(d["rows"], fld) for d in days]
        m, icir, nd = v10.mean_icir(di)
        field_ic[fld] = {"mean_ic": m, "icir": icir, "n_days": nd}

    all_names = list(SCHEMES.keys()) + ["S1_ic_walkforward"]
    oos = {n: [] for n in all_names}
    caps = {n: [] for n in all_names}
    wins_vs_base = {n: 0 for n in all_names if n != "S0_baseline"}
    cmp_days = 0
    last_learned = {}

    for ti in range(MIN_TRAIN, n_days):
        train = days[:ti]
        test = days[ti]
        rows = test["rows"]
        ex = [r["excess"] for r in rows]
        order = sorted(range(len(rows)), key=lambda i: rows[i]["excess"], reverse=True)
        winners = order[:TOPN]
        day_ic = {}
        for name, w in SCHEMES.items():
            sc = [edge_score_row(r, w) for r in rows]
            ic = v10.spearman(sc, ex)
            oos[name].append(ic)
            caps[name].append(cap_at_n(rows, sc, winners, TOPN))
            day_ic[name] = ic
        lw = learn_weights(train)
        sc = [edge_score_row(r, lw) for r in rows]
        ic = v10.spearman(sc, ex)
        oos["S1_ic_walkforward"].append(ic)
        caps["S1_ic_walkforward"].append(cap_at_n(rows, sc, winners, TOPN))
        day_ic["S1_ic_walkforward"] = ic
        if ti == n_days - 1:
            last_learned = lw
        base_ic = day_ic.get("S0_baseline")
        if base_ic is not None:
            cmp_days += 1
            for name in wins_vs_base:
                v = day_ic.get(name)
                if v is not None and v > base_ic:
                    wins_vs_base[name] += 1

    summary = {}
    for name in all_names:
        m, icir, nd = v10.mean_icir(oos[name])
        cvals = [c for c in caps[name] if c is not None]
        cap = round(statistics.mean(cvals), 3) if cvals else None
        summary[name] = {
            "oos_mean_ic": m,
            "oos_icir": icir,
            "oos_days": nd,
            "capture_at_topn": cap,
            "days_beat_baseline": (str(wins_vs_base[name]) + "/" + str(cmp_days)) if name != "S0_baseline" else None,
        }

    base = summary["S0_baseline"]

    def robust(name, s):
        if s["oos_mean_ic"] is None or base["oos_mean_ic"] is None:
            return False
        beat = wins_vs_base.get(name, 0)
        return (
            s["oos_mean_ic"] > base["oos_mean_ic"]
            and (s["oos_icir"] or 0.0) >= 0.9 * (base["oos_icir"] or 0.0)
            and beat * 2 >= cmp_days
        )

    ranked = sorted(
        [(n, s) for n, s in summary.items() if n != "S0_baseline"],
        key=lambda kv: (kv[1]["oos_mean_ic"] if kv[1]["oos_mean_ic"] is not None else -9.0),
        reverse=True,
    )
    robust_better = [n for n, s in ranked if robust(n, s)]
    recommendation = robust_better[0] if robust_better else "S0_baseline"

    out = {
        "probe": "0093_factor_refit",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "n_days": n_days,
        "n_samples": n_samples,
        "min_train": MIN_TRAIN,
        "top_n": TOPN,
        "core_fields": CORE,
        "schemes": SCHEMES,
        "field_ic_fullsample": field_ic,
        "oos_summary": summary,
        "robust_better_than_baseline": robust_better,
        "recommendation": recommendation,
        "last_walkforward_learned_weights": last_learned,
        "note": "只读探针; 候选方案仅在现有 CORE_FIELDS 上重配, 不引入新字段以杜绝过拟合; 推荐方案需 OOS IC>baseline 且 ICIR>=0.9*baseline 且过半天数跑赢.",
    }
    return out


def main():
    out = run()
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    # 硬闸门: 仅校验探针基础设施可用与可比, 不强行规定赢家(避免把过拟合写进闸门)
    assert out["n_days"] >= 3, "历史 v9 分析交易日不足(<3), 无法做 walk-forward"
    sm = out["oos_summary"]
    assert sm["S0_baseline"]["oos_days"] >= 1, "无 OOS 测试日"
    assert sm["S0_baseline"]["oos_mean_ic"] is not None, "baseline OOS IC 计算失败"
    for n in out["schemes"]:
        assert sm[n]["oos_mean_ic"] is not None, n + " OOS IC 缺失"
    assert sm["S1_ic_walkforward"]["oos_mean_ic"] is not None, "自适应方案 OOS IC 缺失"
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
