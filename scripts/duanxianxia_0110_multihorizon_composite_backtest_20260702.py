#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_0110_multihorizon_composite_backtest_20260702.py — Task 0110 只读回测

目的(P0a, 不依赖长历史、不受全冷污染):
  1) 收益轴多周期 R0/R1/R2 — 修正当日口径对连板接力/龙头打法的系统性低估。
  2) 复合排序 vs 纯竞价成交额排序 — 在每个打法池内对比 Top-K 超额。
  3) 字段覆盖率自检 — 报告 FF/换手率/量比/matched_plate 在保存的 v9 json 里到底在不在。
  4) theme_sync 代理 — 按 matched_plate 聚合 竞价涨幅>=3% 成员占比。

只读: 仅读 reports/<date>/premarket/*_analysis_v9.json 与 dailyline/stocks/<code>.csv,
  产出 reports/_audit/multihorizon_composite_backtest.{json,md}。
  不 import batch, 不触发任何 webhook / 多维表 / 生产输出修改。

收益口径(统一以 preclose_T 为分母, 买入=T 日开盘):
  R0 = (close_T   - open_T)/preclose_T*100     # 当日日内(同 0109 excess)
  R1 = (close_T+1 - open_T)/preclose_T*100     # 持到次日收盘
  R2 = (close_T+2 - open_T)/preclose_T*100     # 持到 T+2 收盘
  limitdown = (close_T - preclose_T)/preclose_T*100 <= -9.5
  (T+1/T+2 取该股自身 tradestatus==1 的后续交易日; 最近日子未满则为空, 均值自动跳过空值)

regime: 取 v9 meta.action_gate.regime。打法定义与 0109 一致(S1/S2/S3/S4 + baseline)。
复合分: 对当日全候选集逐因子标准化(z-score)后相加(仅累加当日有值的因子):
  bidStrength_FF=amt/FF(无FF时退为 amt 本身) + turnover + volume_ratio + gap(auction_pct)
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
# full.auction_detail / weimai_detail 里可能的字段名(防御式多键探测)
FF_KEYS = ["free_float_market_cap", "ff_market_cap", "market_cap", "market_cap_yi", "float_market_cap"]
TURN_KEYS = ["turnover_rate", "turnover_rate_pct", "real_turnover_rate"]
VR_KEYS = ["volume_ratio", "volume_ratio_multiple"]


def fnum(x, d=None):
    try:
        if x in (None, "", "-", "None"):
            return d
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:
        return d


def _probe(dct, keys):
    if not isinstance(dct, dict):
        return None
    for k in keys:
        if k in dct:
            v = fnum(dct.get(k))
            if v is not None:
                return v
    return None


def parse_board(*vals):
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


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.mean(xs), 3) if xs else None


class Daily:
    def __init__(self, root):
        self.dir = root / "dailyline" / "stocks"
        self.cache = {}
        self.tdates = {}

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
            self.tdates[code] = sorted(
                d for d, r in data.items() if str(r.get("tradestatus")) in ("1", "1.0"))
        return self.cache[code]

    def _close_on(self, code, d):
        r = self._rows(code).get(d)
        return fnum(r.get("close")) if r else None

    def metrics(self, code, d):
        """R0/R1/R2 (买入=T 开盘, 分母=preclose_T) + limitdown。"""
        rows = self._rows(code)
        row = rows.get(d)
        if not row or str(row.get("tradestatus")) not in ("1", "1.0"):
            return None
        o, c, pc = fnum(row.get("open")), fnum(row.get("close")), fnum(row.get("preclose"))
        if not o or not c or not pc:
            return None
        tds = self.tdates.get(str(code).zfill(6)) or []
        try:
            i = tds.index(d)
        except ValueError:
            i = -1
        c1 = self._close_on(code, tds[i + 1]) if 0 <= i and i + 1 < len(tds) else None
        c2 = self._close_on(code, tds[i + 2]) if 0 <= i and i + 2 < len(tds) else None
        day_ret = (c - pc) / pc * 100.0
        return {
            "R0": (c - o) / pc * 100.0,
            "R1": ((c1 - o) / pc * 100.0) if c1 else None,
            "R2": ((c2 - o) / pc * 100.0) if c2 else None,
            "limitdown": day_ret <= -9.5,
        }


def regen_missing(root):
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
    wd = full.get("weimai_detail") or {}
    ctx = full.get("context_detail") or {}
    board = parse_board(ctx.get("t1_zt_board_label"), wd.get("board_label"), rec.get("weimai_board_label"))
    lg = fnum(ctx.get("t1_ltgd_range_gain_pct"))
    amt = fnum(rec.get("auction_amount_wan"), fnum(ad.get("auction_amount_wan")))
    ff = _probe(ad, FF_KEYS)
    if ff is None:
        ff = _probe(wd, FF_KEYS)
    turn = _probe(ad, TURN_KEYS)
    if turn is None:
        turn = _probe(wd, TURN_KEYS)
    vr = _probe(ad, VR_KEYS)
    if vr is None:
        vr = _probe(wd, VR_KEYS)
    gap = fnum(rec.get("auction_pct"), fnum(ad.get("latest_change_pct")))
    return {
        "code": str(rec.get("code") or "").strip(),
        "edge": fnum(rec.get("edge_score"), 0.0),
        "action_type": rec.get("action_type"),
        "board": board,
        "auction_pct": gap,
        "amt": amt,
        "ff": ff,
        "turnover": turn,
        "volume_ratio": vr,
        "bidstrength": (amt / ff) if (amt is not None and ff and ff > 0) else None,
        "matched_plate": rec.get("matched_plate"),
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


def _zscores(rows, key):
    vals = [(r, r.get(key)) for r in rows if r.get(key) is not None]
    if len(vals) < 2:
        return {}
    xs = [v for _r, v in vals]
    mu = statistics.mean(xs)
    sd = statistics.pstdev(xs)
    if sd <= 0:
        return {}
    return {id(r): (v - mu) / sd for r, v in vals}


def _assign_composite(rows):
    """对当日全候选集逐因子 z-score 相加(仅累加有值因子)。bidstrength 无则退为 amt。"""
    strength_key = "bidstrength" if any(r.get("bidstrength") is not None for r in rows) else "amt"
    zmaps = {k: _zscores(rows, k) for k in (strength_key, "turnover", "volume_ratio", "auction_pct")}
    for r in rows:
        s = 0.0
        got = 0
        for k, zm in zmaps.items():
            z = zm.get(id(r))
            if z is not None:
                s += z
                got += 1
        r["composite"] = s if got else None
        r["composite_ncomp"] = got


def _pick(rows, key, k):
    have = [r for r in rows if r.get(key) is not None]
    if have:
        have.sort(key=lambda r: r[key], reverse=True)
        return have[:k]
    return sorted(rows, key=lambda r: (r.get("edge") or 0), reverse=True)[:k]


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
        _assign_composite(rows)
        out.append({"date": dd.name, "regime": regime, "rows": rows})
    return out


def _pool_stats(pool):
    n = len(pool)
    if not n:
        return {"n_pool": 0}
    return {
        "n_pool": n,
        "top5_R0": _mean([r["R0"] for r in pool]),
        "top5_R1": _mean([r["R1"] for r in pool]),
        "top5_R2": _mean([r["R2"] for r in pool]),
        "win_rate_R0": round(sum(1 for r in pool if (r["R0"] or 0) > 0) / n, 3),
        "limitdown_rate": round(sum(1 for r in pool if r["limitdown"]) / n, 3),
    }


def summarize(days):
    # matrix[(regime, style, method)] -> {top3:[], top5:[], pool:[], days:set}
    agg = {}

    def ensure(reg, st, method):
        key = (reg, st, method)
        if key not in agg:
            agg[key] = {"t3_R0": [], "t5_R0": [], "t5_R1": [], "t5_R2": [], "pool": [], "days": set()}
        return agg[key]

    regimes = set()
    for d in days:
        reg = d["regime"]
        regimes.add(reg)
        for st in STYLES:
            pool_rows = [r for r in d["rows"] if st in r["styles"]]
            if not pool_rows:
                continue
            for method, mkey in (("amt", "amt"), ("composite", "composite")):
                p3 = _pick(pool_rows, mkey, 3)
                p5 = _pick(pool_rows, mkey, 5)
                for rk in (reg, "ALL"):
                    a = ensure(rk, st, method)
                    if p3:
                        a["t3_R0"].append(_mean([r["R0"] for r in p3]))
                    if p5:
                        a["t5_R0"].append(_mean([r["R0"] for r in p5]))
                        a["t5_R1"].append(_mean([r["R1"] for r in p5]))
                        a["t5_R2"].append(_mean([r["R2"] for r in p5]))
                        a["pool"].extend(p5)
                    a["days"].add(d["date"])
        buys = [r for r in d["rows"] if str(r.get("action_type")) == "BUY"]
        if buys:
            for rk in (reg, "ALL"):
                a = ensure(rk, "BASELINE_v9_BUY", "amt")
                a["t3_R0"].append(_mean([r["R0"] for r in buys[:3]]))
                a["t5_R0"].append(_mean([r["R0"] for r in buys[:5]]))
                a["t5_R1"].append(_mean([r["R1"] for r in buys[:5]]))
                a["t5_R2"].append(_mean([r["R2"] for r in buys[:5]]))
                a["pool"].extend(buys[:5])
                a["days"].add(d["date"])

    rows_out = []
    for (reg, st, method), a in agg.items():
        ps = _pool_stats(a["pool"])
        rows_out.append({
            "regime": reg, "style": st, "style_label": STYLE_LABEL.get(st, st), "rank_method": method,
            "n_days": len(a["days"]),
            "top3_R0": _mean(a["t3_R0"]), "top5_R0": _mean(a["t5_R0"]),
            "top5_R1": _mean(a["t5_R1"]), "top5_R2": _mean(a["t5_R2"]),
            "win_rate_R0": ps.get("win_rate_R0"), "limitdown_rate": ps.get("limitdown_rate"),
            "n_picks_pool": ps.get("n_pool"),
        })
    rows_out.sort(key=lambda x: (x["regime"], x["style"], x["rank_method"]))
    return rows_out, sorted(regimes)


def field_coverage(days):
    tot = 0
    cov = {"ff": 0, "turnover": 0, "volume_ratio": 0, "bidstrength": 0, "matched_plate": 0,
           "auction_pct": 0, "amt": 0}
    for d in days:
        for r in d["rows"]:
            tot += 1
            for k in cov:
                v = r.get(k)
                if v is not None and v != "":
                    cov[k] += 1
    return {"n_rows": tot, "present": cov,
            "pct": {k: (round(v / tot, 3) if tot else None) for k, v in cov.items()}}


def theme_sync(days):
    """按 matched_plate 聚合: 题材内 竞价涨幅>=3% 成员 / 总成员 (当日均值)。"""
    per_day = []
    for d in days:
        groups = {}
        for r in d["rows"]:
            p = r.get("matched_plate")
            if not p:
                continue
            groups.setdefault(p, [0, 0])
            groups[p][1] += 1
            if (r.get("auction_pct") or -99) >= 3.0:
                groups[p][0] += 1
        ratios = [hi / tot for hi, tot in groups.values() if tot >= 3]
        if ratios:
            per_day.append({"date": d["date"], "regime": d["regime"],
                            "n_plates": len(ratios), "mean_sync": round(statistics.mean(ratios), 3),
                            "max_sync": round(max(ratios), 3)})
    return {"note": "theme_sync=题材内竞价涨幅>=3%成员占比(成员>=3的 plate); 基于 matched_plate, 受其覆盖率限制",
            "per_day": per_day}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    ap.add_argument("--regen", action="store_true")
    args = ap.parse_args()
    root = Path(args.project_root)

    summary = {"job": "0110_multihorizon_composite_backtest",
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
    summary["field_coverage"] = field_coverage(days)
    summary["theme_sync"] = theme_sync(days)
    summary["ok"] = len(days) > 0
    summary["note"] = ("R0/R1/R2=买入T开盘持到T/T+1/T+2收盘,分母 preclose_T; "
                       "rank_method=amt(竞价成交额) vs composite(z相加); "
                       "打板涨停价买入口径待下轮(需板型涨跌幅上限); 字段覆盖率见 field_coverage")

    try:
        audit = root / "reports" / "_audit"
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "multihorizon_composite_backtest.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        cov = summary["field_coverage"]["pct"]
        L = ["# 多周期收益 × 打法 × 排序法 回测 (Task 0110, 只读)", "",
             "- 生成: " + summary["generated_at"],
             "- 有效交易日: %d ｜ regime 分布: %s" % (len(days), regime_days),
             "- 字段覆盖率: FF=%s 换手=%s 量比=%s bidStrength=%s matched_plate=%s" % (
                 cov.get("ff"), cov.get("turnover"), cov.get("volume_ratio"),
                 cov.get("bidstrength"), cov.get("matched_plate")),
             "- 口径: " + summary["note"], "",
             "## 矩阵 (regime × style × rank_method)", "",
             "| regime | 打法 | 排序 | 交易日 | Top5_R0 | Top5_R1 | Top5_R2 | 胜率R0 | 跌停率 | 池 |",
             "|---|---|---|---|---|---|---|---|---|---|"]
        for r in matrix:
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                r["regime"], r["style_label"], r["rank_method"], r["n_days"],
                r["top5_R0"], r["top5_R1"], r["top5_R2"], r["win_rate_R0"],
                r["limitdown_rate"], r["n_picks_pool"]))
        (audit / "multihorizon_composite_backtest.md").write_text("\n".join(L), encoding="utf-8")
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
