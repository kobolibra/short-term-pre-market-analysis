#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v35_feature_audit.py — job 0044: 下一代模型地基 (只读特征审计)。

动机: 现役 edge 只用 7 个 *预聚合的* 0-100 分; 真正有逻辑含义的原始比率
(委买/封单比、封单/流通比、大单占比、净流入压强) 被揉进 weimai_strength、
以 0.08 权重几乎被忽略; 且头号因子 amt_pct 用的是 *绝对* 竞价成交额百分位,
被大盘股主导。本作业不改任何生产逻辑, 纯描述性回答两问:

  (1) 被埋掉的原始比率 + 「换手强度 = 竞价成交额/流通市值」作为独立因子,
      横截面 IC/ICIR 谁高谁低? 覆盖率多少? vs 现役头号 amt。
  (2) amt 的 IC 是否随开盘位置反转: 低开/平开 = 抢筹(看多), 高开 = 出货(看空)?
      (按 latest_change_pct 分桶的池化 rank 相关, 描述性探针, 小样本谨慎)

excess = (close - open)/preclose*100
输出: reports/_audit/premarket_feature_audit_v35.{json,md}
用法: python3 scripts/v35_feature_audit.py
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
import v10_optimize as v10  # Daily/fnum/spearman/mean_icir

# 秩字段: 越小越强, IC 需翻转符号
RANK_NEG = {"qiangchou_920_925_rank", "qiangchou_last_second_rank"}

NEW_FEATURES = [
    "auction_amount_wan",      # 现役头号(绝对额, rank-等价于 amt_pct)
    "turnover_intensity",      # 新: 竞价成交额/流通市值 (换手强度)
    "weimai_to_seal_ratio",    # 委买/封单比
    "seal_to_mcap_ratio",      # 封单/流通比
    "big_order_share",         # 大单占比
    "wm_net_pressure",         # 净流入/流通压强
    "net_inflow_pct",          # 净流入百分位
    "weimai_amount_pct",       # 委买额百分位
    "qiangchou_920_925_rank",  # 抢筹时序(早)
    "qiangchou_last_second_rank",  # 抢筹时序(尾)
]


def load(root):
    daily = v10.Daily(root)
    days = []
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
            ad = full.get("auction_detail") or {}
            wm = full.get("weimai_detail") or {}
            amt = v10.fnum(ad.get("auction_amount_wan"))
            mcap = v10.fnum(wm.get("market_cap_wan"))
            feat = {
                "auction_amount_wan": amt,
                "market_cap_wan": mcap,
                "turnover_intensity": (amt / mcap) if (amt and mcap and mcap > 0) else None,
                "weimai_to_seal_ratio": v10.fnum(wm.get("weimai_to_seal_ratio")),
                "seal_to_mcap_ratio": v10.fnum(wm.get("seal_to_mcap_ratio")),
                "big_order_share": v10.fnum(wm.get("big_order_share")),
                "wm_net_pressure": v10.fnum(wm.get("net_pressure")),
                "net_inflow_pct": v10.fnum(wm.get("net_inflow_pct")),
                "weimai_amount_pct": v10.fnum(wm.get("weimai_amount_pct")),
                "qiangchou_920_925_rank": v10.fnum(ad.get("qiangchou_920_925_rank")),
                "qiangchou_last_second_rank": v10.fnum(ad.get("qiangchou_last_second_rank")),
                "latest_change_pct": v10.fnum(ad.get("latest_change_pct")),
            }
            rows.append({"code": rec["code"], "excess": ex, "feat": feat})
        if len(rows) < 30:
            continue
        days.append({"date": dd.name, "rows": rows})
    return days


def daily_ic(rows, fld):
    xs, ys = [], []
    for r in rows:
        v = r["feat"].get(fld)
        if v is None:
            continue
        xs.append(-v if fld in RANK_NEG else v)
        ys.append(r["excess"])
    return v10.spearman(xs, ys) if len(xs) >= 8 else None


def coverage(days, fld):
    tot = sum(len(d["rows"]) for d in days)
    have = sum(1 for d in days for r in d["rows"] if r["feat"].get(fld) is not None)
    return round(have / tot, 3) if tot else 0.0


def open_bucket(p):
    if p is None:
        return None
    if p < 0:
        return "1_low_open(<0)"
    if p < 3:
        return "2_flat_open(0-3)"
    if p < 7:
        return "3_high_open(3-7)"
    return "4_veryhigh_open(>=7)"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    days = load(root)
    n_samples = sum(len(d["rows"]) for d in days)

    # (1) 逐特征横截面 IC
    feat_ic = []
    for fld in NEW_FEATURES:
        di = [daily_ic(d["rows"], fld) for d in days]
        m, icir, nd = v10.mean_icir(di)
        if m is not None:
            feat_ic.append({"field": fld, "mean_ic": m, "icir": icir,
                            "n_days": nd, "coverage": coverage(days, fld)})
    feat_ic.sort(key=lambda x: abs(x["mean_ic"]), reverse=True)

    # (2) 条件 IC: amt / turnover 在开盘位置各桶内的池化 rank 相关
    cond = {}
    for key in ("auction_amount_wan", "turnover_intensity"):
        buckets = defaultdict(lambda: {"x": [], "y": []})
        for d in days:
            for r in d["rows"]:
                b = open_bucket(r["feat"].get("latest_change_pct"))
                v = r["feat"].get(key)
                if b is None or v is None:
                    continue
                buckets[b]["x"].append(v)
                buckets[b]["y"].append(r["excess"])
        cond[key] = {b: {"pooled_spearman": (round(v10.spearman(o["x"], o["y"]), 4)
                                              if len(o["x"]) >= 8 and v10.spearman(o["x"], o["y"]) is not None
                                              else None),
                         "n": len(o["x"])}
                    for b, o in sorted(buckets.items())}

    top = feat_ic[0] if feat_ic else None
    turnover = next((f for f in feat_ic if f["field"] == "turnover_intensity"), None)
    amt = next((f for f in feat_ic if f["field"] == "auction_amount_wan"), None)
    verdict = {
        "strongest_feature": top,
        "turnover_vs_amt": {
            "turnover_intensity": turnover,
            "auction_amount_wan": amt,
            "turnover_better": (turnover and amt and abs(turnover["mean_ic"]) > abs(amt["mean_ic"]))
                                if (turnover and amt) else None,
        },
        "buried_ratios_with_signal": [f for f in feat_ic
            if f["field"] in ("weimai_to_seal_ratio", "seal_to_mcap_ratio",
                              "big_order_share", "wm_net_pressure")
            and abs(f["mean_ic"]) >= 0.04 and f["coverage"] >= 0.3],
        "amt_sign_flips_by_open": cond["auction_amount_wan"],
        "turnover_sign_by_open": cond["turnover_intensity"],
    }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0044_premarket_feature_audit_v35",
        "n_days": len(days), "n_samples": n_samples,
        "feature_ic": feat_ic, "conditional_ic_by_open": cond, "verdict": verdict,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_feature_audit_v35.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    L = ["# 盘前特征审计 v35 — next-level 地基 (job 0044)", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples}",
         f"- 口径: excess=(close-open)/preclose*100; 不改生产逻辑, 纯描述性", "",
         "## 1. 原始/新因子横截面 IC (按 |IC| 排序)", "",
         "| 因子 | mean_ic | icir | 覆盖率 | n_days |", "|---|---|---|---|---|"]
    for f in feat_ic:
        L.append(f"| {f['field']} | {f['mean_ic']} | {f['icir']} | {f['coverage']} | {f['n_days']} |")
    L += ["", "## 2. 条件 IC: 成交额×开盘位置 (池化 spearman, 描述性)", "",
          "### auction_amount_wan", "", "| 开盘桶 | pooled_spearman | n |", "|---|---|---|"]
    for b, o in cond["auction_amount_wan"].items():
        L.append(f"| {b} | {o['pooled_spearman']} | {o['n']} |")
    L += ["", "### turnover_intensity (换手强度)", "", "| 开盘桶 | pooled_spearman | n |", "|---|---|---|"]
    for b, o in cond["turnover_intensity"].items():
        L.append(f"| {b} | {o['pooled_spearman']} | {o['n']} |")
    tb = verdict["turnover_vs_amt"]["turnover_better"]
    L += ["", "## 结论", "",
          f"- 最强因子: **{(top or {}).get('field')}** (IC {(top or {}).get('mean_ic')}, 覆盖 {(top or {}).get('coverage')})",
          f"- 换手强度是否优于绝对成交额: **{tb}** "
          f"(turnover IC {turnover['mean_ic'] if turnover else None} vs amt IC {amt['mean_ic'] if amt else None})",
          f"- 有信号的被埋比率: {[f['field'] for f in verdict['buried_ratios_with_signal']]}",
          "- amt 是否随开盘位置变号: 见上表(低开桶 vs 高开桶 符号/强度差异)", "",
          "> 谨慎: 15 天小样本; 条件 IC 为池化(未逐日去均值), 仅作方向探针。几何意义均需后续 walk-forward 复验。"]
    (audit / "premarket_feature_audit_v35.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"verdict": verdict, "top6_ic": feat_ic[:6]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
