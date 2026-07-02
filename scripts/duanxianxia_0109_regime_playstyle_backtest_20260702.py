#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_0109_regime_playstyle_backtest_20260702.py — Task 0109 只读回测

目的: 用历史 v9 分析文件 + dailyline 真实收益, 量化
  "情绪周期(regime) × 打法(play-style)" 的 Top-K 超额/胜率/跌停率矩阵,
  为"多打法按情绪周期自适应路由"提供数据依据(回答: 什么周期用什么打法最有效)。

只读: 仅读 reports/<date>/premarket/*_analysis_v9.json 与 dailyline/stocks/<code>.csv,
  缺失交易日可选从 captures 补生成 v9 分析文件(--regen, 与 v10_optimize 同口径),
  产出 reports/_audit/regime_playstyle_backtest.{json,md}。
  不 import duanxianxia_batch, 不触发任何 webhook / 多维表 / 生产输出修改。

收益口径(沿用 0040/0093/v10_optimize 既有 harness):
  excess_ret = (close - open) / preclose * 100   # 当日开盘买入到收盘的日内超额
  limitdown  = (close - preclose) / preclose * 100 <= -9.5

regime: 直接取 v9 分析文件 meta.action_gate.regime(cold/cold_to_warming/warming/
  normal/hot), 由 qxlive 市场环境层驱动(封板率/情绪/涨跌停等), 无需另造代理。

打法(play-style, 基于每行 full.context_detail / auction_detail 派生, 防御式解析):
  S1_first_board 首板打板:   T-1 连板<=1 且 竞价涨幅>=0
  S2_low_relay 低位连板接力: 2<=T-1 连板<=4 且 龙头梯队区间涨幅<60
  S3_dip_buy 低吸反弹:       竞价涨幅<=0 且 T-1 连板<=3
  S4_high_leader 高标龙头:   T-1 连板>=5 或 (在龙头梯队 且 区间涨幅>=45)
每种打法内按 auction_amount_wan(竞价成交额, 已验证最强资金信号)降序取 TopK。

baseline: 当前 v9 动作层 BUY 行(action_type==BUY), 同口径统计, 按 regime 分组。

注意: T-1 连板标签常因跌停股掉出涨停池而为空(项目 bug #10), 故 S1/S3 池偏宽;
  这是当前数据现实, 结果解读时需结合 n_picks_pool 与跌停率一起看。
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
import traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
try:
    from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT
except Exception:
    DEFAULT_PROJECT_ROOT = SCRIPTS_DIR.parent / "projects" / "duanxianxia"

SUMMARY_JSON_BEGIN = "===SUMMARY_JSON_BEGIN==="
SUMMARY_JSON_END = "===SUMMARY_JSON_END==="

STYLES = ["S1_first_board", "S2_low_relay", "S3_dip_buy", "S4_high_leader"]
STYLE_LABEL = {
    "S1_first_board": "S1首板打板",
    "S2_low_relay": "S2低位连板接力",
    "S3_dip_buy": "S3低吸反弹",
    "S4_high_leader": "S4高标龙头",
    "BASELINE_v9_BUY": "baseline(v9 BUY)",
}


def fnum(x, d=None):
    try:
        if x in (None, "", "-", "None"):
            return d
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:
        return d


def parse_board(*vals):
    """从 t1_zt_board_label / weimai board_label 之类文本解析连板数。空/无 -> 0。"""
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if s == "首板":
            return 1
        m = re.search(r"(\d+)\s*连\s*板", s)
        if m:
            return int(m.group(1))
        m = re.search(r"(\d+)\s*进\s*(\d+)", s)
        if m:
            return int(m.group(1))
        m = re.match(r"^\s*(\d+)\s*$", s)
        if m:
            return int(m.group(1))
    return 0


class Daily:
    def __init__(self, root):
        self.dir = root / "dailyline" / "stocks"
        self.cache = {}

    def _rows(self, code):
        code = str(code).zfill(6)
        if code not in self.cache:
            data = {}
            f = self.dir / (code + ".csv")
            if f.exists():
                try:
                    with open(f, newline="") as fh:
                        for r in csv.DictReader(fh):
                            data[r["date"]] = r
                except Exception:
                    data = {}
            self.cache[code] = data
        return self.cache[code]

    def metrics(self, code, d):
        row = self._rows(code).get(d)
        if not row or str(row.get("tradestatus")) not in ("1", "1.0"):
            return None
        o, c, pc = fnum(row.get("open")), fnum(row.get("close")), fnum(row.get("preclose"))
        if not o or not c or not pc:
            return None
        day_ret = (c - pc) / pc * 100.0
        return {"excess": (c - o) / pc * 100.0, "day_ret": day_ret, "limitdown": day_ret <= -9.5}


def regen_missing(root):
    """对有 captures 但缺 v9 分析的交易日补生成(只补缺, 不覆盖)。同 v10_optimize。"""
    res = {"attempted": True, "succeeded": [], "failed": [], "skipped_existing": 0}
    cap_dir = root / "captures"
    if not cap_dir.is_dir():
        res["failed"].append({"date": "*", "err": "no captures dir"})
        return res
    try:
        import duanxianxia_premarket_v9_runner as v9r
    except Exception as e:
        res["failed"].append({"date": "*", "err": "import runner failed: %r" % e})
        return res
    for dd in sorted(cap_dir.glob("20*-*-*")):
        date_str = dd.name
        pm = root / "reports" / date_str / "premarket"
        if pm.is_dir() and list(pm.glob("*_analysis_v9.json")):
            res["skipped_existing"] += 1
            continue
        try:
            v9r.run_v9(date_str, root, output_dir=None, no_write=False)
            res["succeeded"].append(date_str)
        except Exception as e:
            res["failed"].append({"date": date_str, "err": ("%s: %s" % (type(e).__name__, e))[:200]})
    return res


def _regime_of(analysis):
    meta = analysis.get("meta") or {}
    gate = meta.get("action_gate") or {}
    reg = gate.get("regime")
    if reg and reg != "(unknown)":
        return str(reg)
    me = analysis.get("market_env") or {}
    r = me.get("regime")
    if isinstance(r, dict):
        return str(r.get("regime") or r.get("label") or "unknown")
    if isinstance(r, str) and r:
        return r
    return "unknown"


def _row_fields(rec):
    full = rec.get("full") if isinstance(rec.get("full"), dict) else {}
    ad = full.get("auction_detail") or {}
    ctx = full.get("context_detail") or {}
    wd = full.get("weimai_detail") or {}
    board = parse_board(ctx.get("t1_zt_board_label"), wd.get("board_label"), rec.get("weimai_board_label"))
    lg = fnum(ctx.get("t1_ltgd_range_gain_pct"))
    return {
        "code": str(rec.get("code") or "").strip(),
        "edge": fnum(rec.get("edge_score"), 0.0),
        "action_type": rec.get("action_type"),
        "board": board,
        "auction_pct": fnum(rec.get("auction_pct"), fnum(ad.get("latest_change_pct"))),
        "amt": fnum(rec.get("auction_amount_wan"), fnum(ad.get("auction_amount_wan"))),
        "ltgd_leader": bool(ctx.get("t1_ltgd_leader")),
        "ltgd_gain": lg,
    }


def _styles_of(f):
    out = []
    board = f["board"]
    apct = f["auction_pct"]
    lg = f["ltgd_gain"] if f["ltgd_gain"] is not None else 0.0
    if board <= 1 and (apct is None or apct >= 0):
        out.append("S1_first_board")
    if 2 <= board <= 4 and lg < 60:
        out.append("S2_low_relay")
    if (apct is not None and apct <= 0) and board <= 3:
        out.append("S3_dip_buy")
    if board >= 5 or (f["ltgd_leader"] and lg >= 45):
        out.append("S4_high_leader")
    return out


def _rank_pick(rows, k):
    with_amt = [r for r in rows if r["amt"] is not None]
    if with_amt:
        with_amt.sort(key=lambda r: r["amt"], reverse=True)
        return with_amt[:k]
    return sorted(rows, key=lambda r: r["edge"], reverse=True)[:k]


def load_days(root, daily):
    out = []
    rep = root / "reports"
    if not rep.is_dir():
        return out
    for dd in sorted(rep.glob("20*-*-*")):
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
        regime = _regime_of(analysis)
        rows = []
        for rec in cands:
            if not isinstance(rec, dict) or not rec.get("code"):
                continue
            m = daily.metrics(rec["code"], dd.name)
            if m is None:
                continue
            f = _row_fields(rec)
            f.update(m)
            f["styles"] = _styles_of(f)
            rows.append(f)
        if len(rows) < 20:
            continue
        out.append({"date": dd.name, "regime": regime, "rows": rows})
    return out


def summarize(days):
    agg = {}

    def ensure(reg, st):
        key = (reg, st)
        if key not in agg:
            agg[key] = {"k3": [], "k5": [], "pool": [], "days": set()}
        return agg[key]

    regimes = set()
    for d in days:
        reg = d["regime"]
        regimes.add(reg)
        for st in STYLES:
            pool_rows = [r for r in d["rows"] if st in r["styles"]]
            if not pool_rows:
                continue
            p3 = _rank_pick(pool_rows, 3)
            p5 = _rank_pick(pool_rows, 5)
            for rk in (reg, "ALL"):
                a = ensure(rk, st)
                if p3:
                    a["k3"].append(statistics.mean([r["excess"] for r in p3]))
                if p5:
                    a["k5"].append(statistics.mean([r["excess"] for r in p5]))
                    a["pool"].extend(p5)
                a["days"].add(d["date"])
        buys = [r for r in d["rows"] if str(r.get("action_type")) == "BUY"]
        if buys:
            for rk in (reg, "ALL"):
                a = ensure(rk, "BASELINE_v9_BUY")
                a["k3"].append(statistics.mean([r["excess"] for r in buys[:3]]))
                a["k5"].append(statistics.mean([r["excess"] for r in buys[:5]]))
                a["pool"].extend(buys[:5])
                a["days"].add(d["date"])

    rows_out = []
    for (reg, st), a in agg.items():
        pool = a["pool"]
        n = len(pool)
        rows_out.append({
            "regime": reg,
            "style": st,
            "style_label": STYLE_LABEL.get(st, st),
            "n_days": len(a["days"]),
            "top3_excess": round(statistics.mean(a["k3"]), 3) if a["k3"] else None,
            "top5_excess": round(statistics.mean(a["k5"]), 3) if a["k5"] else None,
            "win_rate": round(sum(1 for r in pool if r["excess"] > 0) / n, 3) if n else None,
            "limitdown_rate": round(sum(1 for r in pool if r["limitdown"]) / n, 3) if n else None,
            "n_picks_pool": n,
        })
    rows_out.sort(key=lambda x: (x["regime"], x["style"]))
    return rows_out, sorted(regimes)


def best_by_regime(rows_out):
    by = {}
    for r in rows_out:
        if r["style"].startswith("BASELINE") or r["regime"] == "ALL":
            continue
        if r["top5_excess"] is None or r["n_picks_pool"] < 5:
            continue
        cur = by.get(r["regime"])
        if cur is None or r["top5_excess"] > cur["top5_excess"]:
            by[r["regime"]] = r
    return {reg: {"style": r["style"], "style_label": r["style_label"],
                  "top5_excess": r["top5_excess"], "limitdown_rate": r["limitdown_rate"]}
            for reg, r in by.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    ap.add_argument("--regen", action="store_true", help="缺失交易日从 captures 补生成 v9 分析文件")
    args = ap.parse_args()
    root = Path(args.project_root)

    summary = {"job": "0109_regime_playstyle_backtest",
               "generated_at": datetime.now().isoformat(timespec="seconds")}

    if args.regen:
        summary["data_regen"] = regen_missing(root)

    daily = Daily(root)
    days = load_days(root, daily)
    summary["n_days"] = len(days)
    summary["days_used"] = [d["date"] for d in days]
    regime_days = {}
    for d in days:
        regime_days[d["regime"]] = regime_days.get(d["regime"], 0) + 1
    summary["regime_days"] = regime_days

    matrix, regimes = summarize(days)
    summary["regimes"] = regimes
    summary["matrix"] = matrix
    summary["best_style_per_regime"] = best_by_regime(matrix)
    summary["ok"] = len(days) > 0
    summary["note_return_caliber"] = ("excess=(close-open)/preclose*100 当日日内; "
                                      "regime=v9 已标定标签; 打法内按 auction_amount_wan 排序; "
                                      "pool 为各日 Top5 汇总")

    try:
        audit = root / "reports" / "_audit"
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "regime_playstyle_backtest.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        L = ["# 情绪周期 × 打法 回测矩阵 (Task 0109, 只读)", "",
             "- 生成: " + summary["generated_at"],
             "- 有效交易日: %d ｜ regime 分布: %s" % (len(days), regime_days),
             "- 收益口径: " + summary["note_return_caliber"], "",
             "## 矩阵 (regime × style)", "",
             "| regime | 打法 | 交易日 | Top3超额 | Top5超额 | 胜率 | 跌停率 | 池样本 |",
             "|---|---|---|---|---|---|---|---|"]
        for r in matrix:
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                r["regime"], r["style_label"], r["n_days"], r["top3_excess"],
                r["top5_excess"], r["win_rate"], r["limitdown_rate"], r["n_picks_pool"]))
        L += ["", "## 各 regime 最优打法(按 Top5 超额, 池样本>=5)", ""]
        for reg, b in summary["best_style_per_regime"].items():
            L.append("- **%s** -> %s (Top5超额 %s, 跌停率 %s)" % (
                reg, b["style_label"], b["top5_excess"], b["limitdown_rate"]))
        (audit / "regime_playstyle_backtest.md").write_text("\n".join(L), encoding="utf-8")
        summary["report_written"] = True
    except Exception as e:
        summary["report_written"] = False
        summary["report_error"] = "%s: %s" % (type(e).__name__, e)

    print(SUMMARY_JSON_BEGIN)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(SUMMARY_JSON_END)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        raise
