#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v28_risk_filter_validation.py — sparse_ic Top5 风险过滤验证(只读)。

v27 当天实盘影子结果显示: sparse Top5 中 risk=Y 两只表现明显差于 risk=False。
本脚本回到全历史/OOS 验证, 判断 risk_flag / 高开过滤是否应正式叠加到 sparse_ic Top5。

比较策略(同日 excess, 每天等权):
  - sparse_top5_all: 原始 sparse_ic Top5
  - sparse_top5_no_risk: 只在 risk_flag=False 内取 Top5
  - sparse_top5_open_lt8: latest_change_pct < 8 内取 Top5
  - sparse_top5_no_risk_open_lt8: risk=False 且 latest_change_pct < 8 内取 Top5
  - sparse_top5_no_drop: action != DROP 内取 Top5
  - v10_top3 / v10_top5 基准

输出: reports/_audit/premarket_risk_filter_v28.{json,md}
用法: python3 scripts/v28_risk_filter_validation.py [--min-train 5]
"""
from __future__ import annotations
import argparse, json, statistics, sys, traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
import v12_reflection as v12

SPARSE_W = {"deriv.amt_x_auc": 0.24, "auction_strength": 0.22, "liquidity": 0.18,
            "pressure_score": 0.13, "deriv.money_x_liq": 0.12, "money": 0.11}


def q(xs, p):
    xs = sorted([x for x in xs if x is not None])
    if not xs:
        return None
    return xs[int(p * (len(xs)-1))]


def stat(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return {"n": 0}
    pos = [x for x in xs if x > 0]
    neg = [x for x in xs if x < 0]
    ap = statistics.mean(pos) if pos else 0.0
    an = statistics.mean(neg) if neg else 0.0
    return {"n": len(xs), "mean": round(statistics.mean(xs), 4), "median": round(statistics.median(xs), 4),
            "win_rate": round(len(pos)/len(xs), 4), "odds": round(ap/abs(an), 3) if neg and an else None,
            "sum": round(sum(xs), 4), "p10": round(q(xs,0.1),4), "p90": round(q(xs,0.9),4),
            "min": round(min(xs),4), "max": round(max(xs),4)}


def build_days(root):
    days = v12.load_days_plus(root, v10.Daily(root))
    out = []
    for d in days:
        rows = d["rows"]
        for r in rows:
            r["_v10_amt"] = v10.score(r["f"], r["amt"], v10.V10AMT_W)
        xr = {}
        for fld in SPARSE_W:
            iv = [(i, v10.field_value(rows[i], fld)) for i in range(len(rows)) if v10.field_value(rows[i], fld) is not None]
            xr[fld] = v10.pctl(iv) if iv else {}
        for i, r in enumerate(rows):
            r["_sparse_ic"] = sum(SPARSE_W[f] * xr[f].get(i, 50.0) for f in SPARSE_W)
        out.append({"date": d["date"], "rows": rows})
    return out


def latest_change(r):
    return v10.field_value(r, "latest_change_pct")


def select(rows, key, n, pred=lambda r: True):
    pool = [r for r in rows if pred(r)]
    return sorted(pool, key=lambda r: r.get(key, -1e9), reverse=True)[:n]


def eval_strategy(days, name, fn):
    vals, details = [], []
    for d in days:
        picks = fn(d["rows"])
        if not picks:
            ret = 0.0
        else:
            ret = statistics.mean([r["excess"] for r in picks])
        vals.append(ret)
        details.append({"date": d["date"], "ret": round(ret, 4), "n": len(picks),
                        "codes": [r["code"] for r in picks],
                        "risk_n": sum(1 for r in picks if r.get("risk_flag")),
                        "open_mean": round(statistics.mean([latest_change(r) for r in picks if latest_change(r) is not None]), 3) if any(latest_change(r) is not None for r in picks) else None})
    return {"name": name, "stat": stat(vals), "details": details}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--min-train", type=int, default=5)
    args = ap.parse_args()
    root = Path(args.project_root)
    days = build_days(root)
    oos = days[args.min_train:]

    strategies = {
        "sparse_top5_all": lambda rows: select(rows, "_sparse_ic", 5),
        "sparse_top5_no_risk": lambda rows: select(rows, "_sparse_ic", 5, lambda r: not r.get("risk_flag")),
        "sparse_top5_open_lt8": lambda rows: select(rows, "_sparse_ic", 5, lambda r: (latest_change(r) is None or latest_change(r) < 8.0)),
        "sparse_top5_no_risk_open_lt8": lambda rows: select(rows, "_sparse_ic", 5, lambda r: (not r.get("risk_flag")) and (latest_change(r) is None or latest_change(r) < 8.0)),
        "sparse_top5_no_drop": lambda rows: select(rows, "_sparse_ic", 5, lambda r: r.get("action") != "DROP"),
        "v10_top3": lambda rows: select(rows, "_v10_amt", 3),
        "v10_top5": lambda rows: select(rows, "_v10_amt", 5),
    }
    all_res = {name: eval_strategy(days, name, fn) for name, fn in strategies.items()}
    oos_res = {name: eval_strategy(oos, name, fn) for name, fn in strategies.items()}
    ranked_oos = sorted(oos_res.keys(), key=lambda n: (oos_res[n]["stat"].get("mean", -999), oos_res[n]["stat"].get("win_rate", -999)), reverse=True)

    report = {"generated_at": datetime.now().isoformat(timespec="seconds"), "n_days": len(days),
              "oos_days": len(oos), "min_train": args.min_train,
              "all_sample": all_res, "oos": oos_res, "ranked_oos": ranked_oos}
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_risk_filter_v28.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def table(title, block):
        L = ["", f"## {title}", "", "| 策略 | n | 均值 | 中位 | 胜率 | 赔率 | 累计 | p10 | p90 | min | max |",
             "|---|---|---|---|---|---|---|---|---|---|---|"]
        order = sorted(block.keys(), key=lambda n: (block[n]["stat"].get("mean", -999), block[n]["stat"].get("win_rate", -999)), reverse=True)
        for name in order:
            s = block[name]["stat"]
            L.append(f"| {name} | {s.get('n')} | {s.get('mean')} | {s.get('median')} | {s.get('win_rate')} | {s.get('odds')} | {s.get('sum')} | {s.get('p10')} | {s.get('p90')} | {s.get('min')} | {s.get('max')} |")
        return L

    L = ["# v28 sparse_ic Top5 风险过滤验证", "",
         f"- 生成: {report['generated_at']} ｜总天数: {len(days)} ｜OOS天: {len(oos)}",
         "- 目的: 验证 v27 当天发现的 risk=Y 拖累是否是历史稳健规律。"]
    L += table("OOS 同日 excess", oos_res)
    L += table("全样本 同日 excess", all_res)
    L += ["", "## OOS 逐日明细(核心策略)", "", "| 日期 | sparse_all | no_risk | open_lt8 | no_risk_open_lt8 | v10_top3 |",
          "|---|---|---|---|---|---|"]
    for i, d in enumerate(oos):
        def ret(name): return oos_res[name]["details"][i]["ret"]
        L.append(f"| {d['date']} | {ret('sparse_top5_all')} | {ret('sparse_top5_no_risk')} | {ret('sparse_top5_open_lt8')} | {ret('sparse_top5_no_risk_open_lt8')} | {ret('v10_top3')} |")
    L += ["", "> 判定: 若 no_risk 或 open_lt8 在 OOS 均值/胜率/p10 同时优于 sparse_all, 则应把该过滤加入影子/生产候选;",
          "> 若只改善单日但历史不稳, 则保留 risk 信息为提示, 不硬过滤。"]
    (audit / "premarket_risk_filter_v28.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"oos_days": len(oos), "ranked_oos": ranked_oos,
                      "top": {k: oos_res[k]["stat"] for k in ranked_oos[:4]}}, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc(); sys.exit(1)
