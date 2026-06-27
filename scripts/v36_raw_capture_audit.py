#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v36_raw_capture_audit.py — job 0045: 全量盘前数据的第一性原理审计 (只读)。

动机: 生产 edge 只用 7 个预聚合 0-100 分; 但盘前实际下载 9 个数据集,
其中大量原始微观结构字段 (竞价量比、昨竞额、竞价换手率、抢筹幅度、
封单 9:15/9:20/9:25 时序、主力净买/流通压强、热度/飙升榜) 被压缩掉或根本
未进排序层。本作业 直接从原始 capture (captures/<date>/<dataset_id>/) 读全部 9 个
数据集, 按代码 join, 按第一性原理构造衰减特征, 测每个特征对当日超额的
每日横截面 Spearman IC/ICIR/覆盖率。纯描述性, 不改生产逻辑, 不过拟合。

同时输出每个数据集的「字段名并集 + 覆盖率 + 样本行」——用实际数据经验性
描述 schema, 避免猜测字段名。

excess = (close - open)/preclose*100
输出: reports/_audit/premarket_raw_capture_audit_v36.{json,md}
用法: python3 scripts/v36_raw_capture_audit.py
"""
from __future__ import annotations
import argparse
import json
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10
try:
    import duanxianxia_v9_weimai as v9wm
except Exception:
    v9wm = None

PREMARKET_DATASETS = [
    "rank.rocket",
    "rank.hot_stock_day",
    "auction.jjyd.vratio",
    "auction.jjyd.qiangchou",
    "auction.jjyd.net_amount",
    "auction.jjlive.fengdan",
    "auction.jjyd.weimai",
    "home.kaipan.plate.summary",
    "home.qxlive.top_metrics",
]
# 个股级可 join 的数据集 (用于构造 code 全集)
STOCK_LEVEL = [
    "auction.jjyd.vratio", "auction.jjyd.qiangchou", "auction.jjyd.net_amount",
    "auction.jjlive.fengdan", "auction.jjyd.weimai", "rank.rocket", "rank.hot_stock_day",
]

# 特征 -> 越小越好 (IC 需翻转符号)
NEG = {"rocket_rank", "hot_rank"}


def _norm(v):
    s = str(v or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:].zfill(6) if len(s) >= 6 else s


def _g(row, keys):
    if not isinstance(row, dict):
        return None
    for k in keys:
        if k in row and row.get(k) not in (None, "", "-", "None"):
            val = v10.fnum(row.get(k))
            if val is not None:
                return val
    return None


def latest_rows(date_dir, dsid):
    d = date_dir / dsid
    if not d.is_dir():
        return []
    files = sorted(d.glob("*.json"))
    if not files:
        return []
    try:
        payload = json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def index_by_code(rows):
    out = {}
    for r in rows:
        c = _norm(r.get("code") or r.get("\u4ee3\u7801"))
        if c and c not in out:
            out[c] = r
    return out


def build_features(idx, weimai_feat, code):
    vr = idx.get("auction.jjyd.vratio", {}).get(code)
    qc = idx.get("auction.jjyd.qiangchou", {}).get(code)
    na = idx.get("auction.jjyd.net_amount", {}).get(code)
    fd = idx.get("auction.jjlive.fengdan", {}).get(code)
    rk = idx.get("rank.rocket", {}).get(code)
    ht = idx.get("rank.hot_stock_day", {}).get(code)
    wm = weimai_feat.get(code, {}) if weimai_feat else {}

    f = {}
    # --- vratio: 竞价量比 / 竞价换手率 / 昨竞额 → 竞额增长 ---
    f["turnover_rate_pct"] = _g(vr, ["turnover_rate_pct", "\u7ade\u4ef7\u6362\u624b", "\u6362\u624b\u7387", "\u6362\u624b"]) or _g(qc, ["turnover_rate_pct"]) or _g(na, ["turnover_rate_pct"])
    f["volume_ratio_multiple"] = _g(vr, ["volume_ratio_multiple", "\u7ade\u4ef7\u91cf\u6bd4", "\u91cf\u6bd4"])
    auc_amt = _g(vr, ["auction_turnover_wan", "\u7ade\u989d"]) or _g(qc, ["auction_turnover_wan"]) or _g(na, ["auction_turnover_wan"])
    y_auc = _g(vr, ["yesterday_auction_turnover_wan", "\u6628\u7ade\u989d"])
    f["auction_amt_growth"] = (auc_amt / y_auc) if (auc_amt and y_auc and y_auc > 0) else None
    # --- qiangchou: 抢筹幅度 (连续量) ---
    f["grab_strength"] = _g(qc, ["grab_strength", "\u62a2\u7b79\u5e45\u5ea6"])
    # --- net_amount: 主力净买 / 流通压强 (真实归一) ---
    net_in = _g(na, ["main_net_inflow_wan", "\u4e3b\u529b\u51c0\u4e70", "\u4e3b\u529b\u51c0\u6d41\u5165"])
    mcap_yi = _g(na, ["market_cap_yi", "\u6d41\u901a\u503c"])
    f["main_net_inflow_wan"] = net_in
    f["net_inflow_pressure"] = (net_in / (mcap_yi * 10000.0)) if (net_in is not None and mcap_yi and mcap_yi > 0) else None
    # --- fengdan: 封单 9:15/9:20/9:25 时序 (增厚 vs 消耗) ---
    a915 = _g(fd, ["amount_915", "9:15"])
    a920 = _g(fd, ["amount_920", "9:20"])
    a925 = _g(fd, ["amount_925", "9:25"])
    f["fengdan_build_slope"] = ((a925 - a915) / a915) if (a915 and a915 > 0 and a925 is not None) else None
    f["fengdan_late_change"] = ((a925 - a920) / a920) if (a920 and a920 > 0 and a925 is not None) else None
    f["fengdan_925_wan"] = a925
    # --- weimai 原始比率 (复用真实 builder) ---
    f["weimai_to_seal_ratio"] = wm.get("weimai_to_seal_ratio")
    f["seal_to_mcap_ratio"] = wm.get("seal_to_mcap_ratio")
    f["big_order_share"] = wm.get("big_order_share")
    f["wm_net_pressure"] = wm.get("net_pressure")
    mcap_wan = wm.get("market_cap_wan")
    f["turnover_intensity"] = (auc_amt / mcap_wan) if (auc_amt and mcap_wan and mcap_wan > 0) else None
    # --- 热度/飙升榜 (注意力) ---
    f["rocket_rank"] = _g(rk, ["rank", "\u6392\u540d"])
    f["hot_rank"] = _g(ht, ["rank", "\u6392\u540d"])
    # 开盘位置 (条件分桶用)
    f["latest_change_pct"] = _g(vr, ["latest_change_pct", "\u6da8\u5e45"]) or _g(na, ["latest_change_pct"]) or _g(qc, ["latest_change_pct"])
    return f


FEATURES = [
    "turnover_rate_pct", "volume_ratio_multiple", "auction_amt_growth", "grab_strength",
    "main_net_inflow_wan", "net_inflow_pressure", "fengdan_build_slope", "fengdan_late_change",
    "fengdan_925_wan", "weimai_to_seal_ratio", "seal_to_mcap_ratio", "big_order_share",
    "wm_net_pressure", "turnover_intensity", "rocket_rank", "hot_rank",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(v10.DEFAULT_PROJECT_ROOT))
    args = ap.parse_args()
    root = Path(args.project_root)
    daily = v10.Daily(root)
    cap_root = root / "captures"

    schema = {ds: {"keys": defaultdict(int), "rows": 0, "sample": None} for ds in PREMARKET_DATASETS}
    days = []
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []
    for dd in date_dirs:
        idx = {}
        raw = {}
        for ds in PREMARKET_DATASETS:
            rows = latest_rows(dd, ds)
            raw[ds] = rows
            for r in rows:
                schema[ds]["rows"] += 1
                if schema[ds]["sample"] is None:
                    schema[ds]["sample"] = {k: str(v)[:40] for k, v in list(r.items())[:30]}
                for k, v in r.items():
                    if v not in (None, "", "-"):
                        schema[ds]["keys"][k] += 1
            if ds in STOCK_LEVEL:
                idx[ds] = index_by_code(rows)
        codes = set()
        for ds in STOCK_LEVEL:
            codes |= set(idx.get(ds, {}).keys())
        if not codes:
            continue
        weimai_feat = {}
        if v9wm is not None:
            try:
                weimai_feat = v9wm.compute_weimai_features(list(codes), raw.get("auction.jjyd.weimai", []), {})
            except Exception:
                weimai_feat = {}
        drows = []
        for code in codes:
            ex = daily.excess(code, dd.name)
            if ex is None:
                continue
            drows.append({"code": code, "excess": ex, "f": build_features(idx, weimai_feat, code)})
        if len(drows) >= 8:
            days.append({"date": dd.name, "rows": drows})

    # per-feature daily cross-sectional IC
    def daily_ic(rows, fld):
        xs, ys = [], []
        for r in rows:
            v = r["f"].get(fld)
            if v is None:
                continue
            xs.append(-v if fld in NEG else v)
            ys.append(r["excess"])
        return v10.spearman(xs, ys) if len(xs) >= 8 else None

    n_samples = sum(len(d["rows"]) for d in days)
    feat_ic = []
    for fld in FEATURES:
        di = [daily_ic(d["rows"], fld) for d in days]
        m, icir, nd = v10.mean_icir(di)
        have = sum(1 for d in days for r in d["rows"] if r["f"].get(fld) is not None)
        cov = round(have / n_samples, 3) if n_samples else 0.0
        if m is not None:
            feat_ic.append({"field": fld, "mean_ic": m, "icir": icir, "n_days": nd, "coverage": cov})
        else:
            feat_ic.append({"field": fld, "mean_ic": None, "icir": None, "n_days": nd, "coverage": cov})
    feat_ic.sort(key=lambda x: abs(x["mean_ic"]) if x["mean_ic"] is not None else -1, reverse=True)

    schema_out = {}
    for ds, s in schema.items():
        total = s["rows"]
        schema_out[ds] = {
            "total_rows": total,
            "field_coverage": {k: round(c / total, 3) for k, c in sorted(s["keys"].items(), key=lambda kv: -kv[1])} if total else {},
            "sample_row": s["sample"],
        }

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0045_premarket_raw_capture_audit_v36",
        "n_days": len(days), "n_samples": n_samples,
        "dataset_schema": schema_out,
        "feature_ic": feat_ic,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_raw_capture_audit_v36.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    L = ["# 全量盘前原始数据审计 v36 (job 0045)", "",
         f"- 生成: {report['generated_at']}",
         f"- 有效交易日: **{len(days)}** ｜样本: {n_samples} ｜数据源: 原始 captures (9 个盘前数据集)", "",
         "## 1. 第一性原理特征横截面 IC (按 |IC| 排序)", "",
         "| 特征 | mean_ic | icir | 覆盖率 | n_days |", "|---|---|---|---|---|"]
    for r in feat_ic:
        L.append(f"| {r['field']} | {r['mean_ic']} | {r['icir']} | {r['coverage']} | {r['n_days']} |")
    L += ["", "## 2. 每个下载数据集的实际 schema (字段覆盖率)", ""]
    for ds in PREMARKET_DATASETS:
        so = schema_out[ds]
        L.append(f"### {ds} — {so['total_rows']} rows")
        if so["field_coverage"]:
            L.append("字段: " + ", ".join(f"{k}({v})" for k, v in list(so["field_coverage"].items())[:24]))
        if so["sample_row"]:
            L.append("样本: " + json.dumps(so["sample_row"], ensure_ascii=False)[:400])
        L.append("")
    L += ["> 谨慎: 15 天小样本, 纯描述性。几何意义特征须后续 walk-forward 复验。",
          "> 覆盖率低的特征 = 该原始字段在 capture 里键名与此脚本猜的不一致, 下轮按 schema 表纠正键名重跑。"]
    (audit / "premarket_raw_capture_audit_v36.md").write_text("\n".join(L), encoding="utf-8")

    print(json.dumps({"n_days": len(days), "n_samples": n_samples,
                      "top_ic": feat_ic[:8],
                      "dataset_rows": {ds: schema_out[ds]["total_rows"] for ds in PREMARKET_DATASETS}},
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
