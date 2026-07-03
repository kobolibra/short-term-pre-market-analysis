#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_0118_pit_enriched_backtest.py -- Task 0118 (additive, read-only).

在 0110 多周期复合回测之上, 把数据层(ff/换手/量比/amt/bidstrength)从
v9 json 的嵌套探测(coverage≈0)切换为 PIT 面板(canonical, point-in-time)。
策略层(候选宇宙 all_candidates / edge / action_type / board / matched_plate /
ltgd / styles)仍来自 v9, 不变。

只读: 读 reports/<date>/premarket/*_analysis_v9.json + captures/<date>/<ds>/*.json
  + dailyline/stocks/<code>.csv; 产出 reports/_audit/pit_enriched_backtest.{json,md}。
  复用 0110 的 Daily/metrics/summarize/field_coverage/theme_sync/_styles_of/_assign_composite。
"""
from __future__ import annotations
import argparse
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import duanxianxia_0110_multihorizon_composite_backtest_20260702 as bt  # noqa: E402
from duanxianxia_pit_enrich import build_pit_index, enrich_fields  # noqa: E402
try:
    from duanxianxia_master_indicators import _norm_code  # noqa: E402
except Exception:  # noqa: BLE001
    def _norm_code(x):
        s = str(x or "").strip()
        return s.zfill(6) if s.isdigit() else s

SUMMARY_JSON_BEGIN = bt.SUMMARY_JSON_BEGIN
SUMMARY_JSON_END = bt.SUMMARY_JSON_END


def _pit_lookup(pit, code):
    for k in (_norm_code(code), str(code).strip(), str(code).strip().zfill(6)):
        if k in pit:
            return pit[k]
    return None


def load_days_enriched(root, daily, as_of_slot="premarket", cutoff="09:29"):
    captures_root = root / "captures"
    out = []
    pit_stats = []
    rep = root / "reports"
    if not rep.is_dir():
        return out, pit_stats
    for dd in sorted(rep.glob("20*-*-*")):
        pm = dd / "premarket"
        files = sorted(pm.glob("*_analysis_v9.json")) if pm.is_dir() else []
        if not files:
            continue
        try:
            analysis = json.loads(files[-1].read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        cands = analysis.get("all_candidates")
        if not isinstance(cands, list) or not cands:
            continue
        regime = bt._regime_of(analysis)
        try:
            pit, psum = build_pit_index(captures_root, dd.name,
                                        as_of_slot=as_of_slot, cutoff=cutoff)
        except Exception as e:  # noqa: BLE001
            pit, psum = {}, {"error": "%s: %s" % (type(e).__name__, e)}
        rows = []
        hits = 0
        for rec in cands:
            if not isinstance(rec, dict) or not rec.get("code"):
                continue
            m = daily.metrics(rec["code"], dd.name)
            if m is None:
                continue
            f = bt._row_fields(rec)
            enrich_fields(f, _pit_lookup(pit, f["code"]))
            if f.get("_pit"):
                hits += 1
            f.update(m)
            f["styles"] = bt._styles_of(f)
            rows.append(f)
        if len(rows) < 20:
            continue
        bt._assign_composite(rows)
        out.append({"date": dd.name, "regime": regime, "rows": rows})
        pit_stats.append({"date": dd.name, "n_rows": len(rows), "pit_hits": hits,
                          "pit_hit_rate": round(hits / len(rows), 3) if rows else None,
                          "universe_size": psum.get("universe_size"),
                          "pit_error": psum.get("error")})
    return out, pit_stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-root", default=str(bt.DEFAULT_PROJECT_ROOT))
    ap.add_argument("--asof", default="premarket")
    ap.add_argument("--cutoff", default="09:29")
    args = ap.parse_args()
    root = Path(args.project_root)

    summary = {"job": "0118_pit_enriched_backtest",
               "generated_at": datetime.now().isoformat(timespec="seconds"),
               "as_of_slot": args.asof, "cutoff": args.cutoff}
    daily = bt.Daily(root)
    days, pit_stats = load_days_enriched(root, daily, as_of_slot=args.asof, cutoff=args.cutoff)
    summary["n_days"] = len(days)
    summary["days_used"] = [d["date"] for d in days]
    regime_days = {}
    for d in days:
        regime_days[d["regime"]] = regime_days.get(d["regime"], 0) + 1
    summary["regime_days"] = regime_days
    summary["pit_join"] = pit_stats

    matrix, regimes = bt.summarize(days)
    summary["regimes"] = regimes
    summary["matrix"] = matrix
    summary["field_coverage"] = bt.field_coverage(days)
    summary["theme_sync"] = bt.theme_sync(days)
    summary["ok"] = len(days) > 0
    summary["note"] = ("数据层=PIT canonical(元/百分比, point-in-time premarket as-of); "
                       "策略层=v9(all_candidates/edge/action_type/board/matched_plate/ltgd); "
                       "R0/R1/R2 口径同 0110; rank=amt vs composite")

    try:
        audit = root / "reports" / "_audit"
        audit.mkdir(parents=True, exist_ok=True)
        (audit / "pit_enriched_backtest.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        cov = summary["field_coverage"]["pct"]
        hr = [p["pit_hit_rate"] for p in pit_stats if p.get("pit_hit_rate") is not None]
        mean_hit = round(sum(hr) / len(hr), 3) if hr else None
        L = ["# PIT 增强多周期回测 (Task 0118, 只读)", "",
             "- 生成: " + summary["generated_at"],
             "- 有效交易日: %d ｜ regime: %s" % (len(days), regime_days),
             "- PIT join 命中率(均值): %s" % mean_hit,
             "- 字段覆盖率(PIT后): FF=%s 换手=%s 量比=%s bidStrength=%s amt=%s matched_plate=%s" % (
                 cov.get("ff"), cov.get("turnover"), cov.get("volume_ratio"),
                 cov.get("bidstrength"), cov.get("amt"), cov.get("matched_plate")),
             "- 口径: " + summary["note"], "",
             "## 矩阵 (regime × style × rank_method)", "",
             "| regime | 打法 | 排序 | 交易日 | Top5_R0 | Top5_R1 | Top5_R2 | 胜率R0 | 跌停率 | 池 |",
             "|---|---|---|---|---|---|---|---|---|---|"]
        for r in matrix:
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                r["regime"], r["style_label"], r["rank_method"], r["n_days"],
                r["top5_R0"], r["top5_R1"], r["top5_R2"], r["win_rate_R0"],
                r["limitdown_rate"], r["n_picks_pool"]))
        (audit / "pit_enriched_backtest.md").write_text("\n".join(L), encoding="utf-8")
        summary["report_written"] = True
    except Exception as e:  # noqa: BLE001
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
