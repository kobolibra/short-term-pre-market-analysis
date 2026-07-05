#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0153: 因子共线/去相关/交互/顶档成分诊断 (全候选宇宙, 纯现有数据).

0152 证实: 线性重配 edge 7 权重对 mean_ic 无效(全距<0.004)且集中化反而腰斩顶档。
根因假设=7 子因子高度共线(相关性天花板)。本 job 量化:
  A) 7 因子日内横截面 Spearman 相关矩阵(共线程度)
  B) 各因子边际 IC(vs 次日超额)
  C) 偏 IC: 控制 liquidity 后 auction_strength 的偏相关(去相关后是否还剩独立 alpha)
  D) 交互项 IC: auction_strength x liquidity / x money / liquidity x money (秩乘积)
  E) baseline edge_score 顶/底档的因子画像 + risk_flag 命中率
不加新数据; production 口径; 真持久化输入。
"""
from __future__ import annotations
import json, math, statistics, sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from v10_optimize import Daily, DEFAULT_PROJECT_ROOT
from duanxianxia_v9_edge import compute_edge_v9
from duanxianxia_v9_output import _regime_label

FACTORS = ["auction_amount_pct", "auction_strength", "liquidity", "money", "pressure_score", "weimai_strength", "orderbook"]


def _rankdata(xs):
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


def _pearson(xs, ys):
    if len(xs) < 8:
        return None
    mx = statistics.mean(xs)
    my = statistics.mean(ys)
    num = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    dx = math.sqrt(sum((a - mx) ** 2 for a in xs))
    dy = math.sqrt(sum((b - my) ** 2 for b in ys))
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def _spearman(xs, ys):
    if len(xs) < 8:
        return None
    return _pearson(_rankdata(xs), _rankdata(ys))


def _decision(cand):
    full = cand.get("full") or {}
    return {
        "code": cand.get("code"),
        "auction_strength": cand.get("auction_strength"),
        "theme_strength_t0": cand.get("theme_strength_t0"),
        "auction_pct": cand.get("auction_pct"),
        "auction_detail": full.get("auction_detail") or cand.get("auction_detail") or {},
        "weimai_detail": full.get("weimai_detail") or {},
        "theme_detail": full.get("theme_detail") or {},
        "context_detail": full.get("context_detail") or {},
    }


def _mean(xs):
    return statistics.mean(xs) if xs else None


def main():
    root = Path(DEFAULT_PROJECT_ROOT)
    daily = Daily(root)
    rep = root / "reports"
    day_files = []
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        fs = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if fs:
            day_files.append((dd.name, fs[-1]))

    corr_acc = {f: {g: [] for g in FACTORS} for f in FACTORS}
    ic_acc = {f: [] for f in FACTORS}
    edge_ic_acc = []
    partial_as_liq = []
    inter_defs = {
        "auction_x_liquidity": ("auction_strength", "liquidity"),
        "auction_x_money": ("auction_strength", "money"),
        "liquidity_x_money": ("liquidity", "money"),
    }
    inter_ic = {k: [] for k in inter_defs}
    topdec_profile = {f: [] for f in FACTORS}
    botdec_profile = {f: [] for f in FACTORS}
    topdec_riskflag = []
    n_days = 0
    regime_days = {}

    for date, f in day_files:
        try:
            analysis = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list) or not cands:
            continue
        meta = analysis.get("meta") or {}
        market_env = meta.get("market_env") or analysis.get("market_env") or {}
        regime = _regime_label(market_env, meta) or "(unknown)"
        exs = []
        fvals = {ff: [] for ff in FACTORS}
        escores = []
        rflags = []
        for c in cands:
            if not isinstance(c, dict) or not c.get("code"):
                continue
            ex = daily.excess(c.get("code"), date)
            if ex is None:
                continue
            out = compute_edge_v9(_decision(c), market_env, {})
            sub = out.get("edge_components", {}).get("sub", {})
            exs.append(ex)
            for ff in FACTORS:
                fvals[ff].append(float(sub.get(ff, 0.0) or 0.0))
            escores.append(out.get("edge_score", 0.0))
            rflags.append(1.0 if out.get("risk_flag") else 0.0)
        if len(exs) < 20:
            continue
        n_days += 1
        regime_days[regime] = regime_days.get(regime, 0) + 1
        for fi in FACTORS:
            ic = _spearman(fvals[fi], exs)
            if ic is not None:
                ic_acc[fi].append(ic)
            for fj in FACTORS:
                cc = _spearman(fvals[fi], fvals[fj])
                if cc is not None:
                    corr_acc[fi][fj].append(cc)
        eic = _spearman(escores, exs)
        if eic is not None:
            edge_ic_acc.append(eic)
        r_ae = _spearman(fvals["auction_strength"], exs)
        r_al = _spearman(fvals["auction_strength"], fvals["liquidity"])
        r_le = _spearman(fvals["liquidity"], exs)
        if None not in (r_ae, r_al, r_le):
            denom = math.sqrt(max(1e-9, (1 - r_al ** 2) * (1 - r_le ** 2)))
            partial_as_liq.append((r_ae - r_al * r_le) / denom)
        rk = {ff: _rankdata(fvals[ff]) for ff in ("auction_strength", "liquidity", "money")}
        for name, (a, b) in inter_defs.items():
            prod = [x * y for x, y in zip(rk[a], rk[b])]
            ic = _spearman(prod, exs)
            if ic is not None:
                inter_ic[name].append(ic)
        idx = sorted(range(len(escores)), key=lambda k: escores[k])
        m = len(idx)
        top_idx = idx[m * 9 // 10:]
        bot_idx = idx[:max(1, m // 10)]
        for ff in FACTORS:
            if top_idx:
                topdec_profile[ff].append(statistics.mean([fvals[ff][k] for k in top_idx]))
            if bot_idx:
                botdec_profile[ff].append(statistics.mean([fvals[ff][k] for k in bot_idx]))
        if top_idx:
            topdec_riskflag.append(statistics.mean([rflags[k] for k in top_idx]))

    def rnd(x, n=4):
        return round(x, n) if x is not None else None

    corr_matrix = {fi: {fj: rnd(_mean(corr_acc[fi][fj]), 3) for fj in FACTORS} for fi in FACTORS}
    marginal_ic = {fi: rnd(_mean(ic_acc[fi])) for fi in FACTORS}
    report = {
        "job": "0153_factor_decorrelation_interaction",
        "n_days": n_days,
        "regime_days": regime_days,
        "edge_score_ic": rnd(_mean(edge_ic_acc)),
        "marginal_ic": marginal_ic,
        "corr_matrix": corr_matrix,
        "partial_ic_auction_given_liquidity": rnd(_mean(partial_as_liq)),
        "interaction_ic": {k: rnd(_mean(v)) for k, v in inter_ic.items()},
        "topdecile_profile": {fi: rnd(_mean(topdec_profile[fi]), 2) for fi in FACTORS},
        "botdecile_profile": {fi: rnd(_mean(botdec_profile[fi]), 2) for fi in FACTORS},
        "topdecile_riskflag_rate": rnd(_mean(topdec_riskflag), 3),
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "factor_decorrelation_0153.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = ["n_days=%s regime_days=%s edge_ic=%s" % (n_days, regime_days, report["edge_score_ic"])]
    lines.append("marginal_ic: " + ", ".join("%s=%s" % (k, marginal_ic[k]) for k in FACTORS))
    lines.append("partial_ic auction|liquidity=%s (marginal auction=%s)" % (report["partial_ic_auction_given_liquidity"], marginal_ic["auction_strength"]))
    lines.append("interaction_ic: " + ", ".join("%s=%s" % (k, report["interaction_ic"][k]) for k in inter_defs))
    lines.append("CORR matrix (Spearman):")
    lines.append("           " + " ".join("%8s" % ff[:8] for ff in FACTORS))
    for fi in FACTORS:
        lines.append("%-11s" % fi[:11] + " ".join("%8s" % corr_matrix[fi][fj] for fj in FACTORS))
    lines.append("topdec_profile: " + ", ".join("%s=%s" % (k, report["topdecile_profile"][k]) for k in FACTORS))
    lines.append("botdec_profile: " + ", ".join("%s=%s" % (k, report["botdecile_profile"][k]) for k in FACTORS))
    lines.append("topdec_riskflag_rate=%s" % report["topdecile_riskflag_rate"])
    print(chr(10).join(lines))


if __name__ == "__main__":
    main()
