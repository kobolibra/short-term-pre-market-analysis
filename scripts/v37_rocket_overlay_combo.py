#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v37_rocket_overlay_combo.py — job 0048: 把飙升榜注意力因子叠加进生产选股并 walk-forward 验证.

v36 审计发现 rocket_rank(飙升榜排名) 是迄今最强单因子(IC 0.222, ICIR 1.03),
而现行生产 edge 完全没用它. 本作业测试: 在生产打分基础上叠加 rocket 注意力分位,
是否提升当日横截面 IC 与 Top3 选股收益. 纯只读验证, 不改生产.

excess = (close-open)/preclose*100
输出: reports/_audit/premarket_rocket_overlay_v37.{json,md}
用法: python3 scripts/v37_rocket_overlay_combo.py
"""
from __future__ import annotations
import json
import sys
import statistics
import traceback
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10


def _norm(v):
    s = str(v or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:].zfill(6) if len(s) >= 6 else s


def rocket_map(cap_root, date):
    d = cap_root / date / "rank.rocket"
    if not d.is_dir():
        return {}
    files = sorted(d.glob("*.json"))
    if not files:
        return {}
    try:
        payload = json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    out = {}
    if isinstance(rows, list):
        for r in rows:
            if not isinstance(r, dict):
                continue
            c = _norm(r.get("code"))
            rk = v10.fnum(r.get("rank"))
            if c and rk is not None and c not in out:
                out[c] = rk
    return out


def pct_map(pairs):
    pres = sorted(pairs, key=lambda t: t[1])
    m = len(pres)
    return {k: ((i / (m - 1) * 100.0) if m > 1 else 50.0) for i, (k, _) in enumerate(pres)}


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    days = v10.load_days(root, daily)

    BLEND = 0.30
    recs = []
    for d in days:
        rows = d["rows"]
        rmap = rocket_map(cap_root, d["date"])
        base = []
        for r in rows:
            s = r.get("edge_old")
            if s is None:
                s = r.get("final")
            base.append((r["code"], s if s is not None else 0.0))
        base_pct = pct_map(base)
        att_pairs = [(c, -rmap[c]) for c in rmap]
        att_pct = pct_map(att_pairs) if att_pairs else {}
        comp = []
        for r in rows:
            c = r["code"]
            bp = base_pct.get(c, 50.0)
            if c in att_pct:
                cp = (1 - BLEND) * bp + BLEND * att_pct[c]
            else:
                cp = bp
            comp.append((c, cp, bp, r["excess"]))
        if len(comp) < 8:
            continue
        ex = [x[3] for x in comp]
        ic_base = v10.spearman([x[2] for x in comp], ex)
        ic_comp = v10.spearman([x[1] for x in comp], ex)

        def top3_mean(scored):
            top = sorted(scored, key=lambda t: t[0], reverse=True)[:3]
            return statistics.mean([t[1] for t in top]) if top else None

        t3_base = top3_mean([(x[2], x[3]) for x in comp])
        t3_comp = top3_mean([(x[1], x[3]) for x in comp])
        recs.append({"date": d["date"], "n": len(comp),
                     "ic_base": ic_base, "ic_comp": ic_comp,
                     "t3_base": t3_base, "t3_comp": t3_comp,
                     "rocket_codes": len(rmap)})

    def agg_ic(key):
        return v10.mean_icir([r[key] for r in recs])

    bm, bicir, bnd = agg_ic("ic_base")
    cm, cicir, cnd = agg_ic("ic_comp")
    t3b = [r["t3_base"] for r in recs if r["t3_base"] is not None]
    t3c = [r["t3_comp"] for r in recs if r["t3_comp"] is not None]
    cum_b = round(sum(t3b), 2) if t3b else None
    cum_c = round(sum(t3c), 2) if t3c else None

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "0048_rocket_overlay_v37",
        "blend": BLEND,
        "n_days": len(recs),
        "ic_baseline": {"mean_ic": bm, "icir": bicir, "n_days": bnd},
        "ic_composite": {"mean_ic": cm, "icir": cicir, "n_days": cnd},
        "top3_cum_excess": {"baseline": cum_b, "composite": cum_c,
                            "mean_base": round(statistics.mean(t3b), 3) if t3b else None,
                            "mean_comp": round(statistics.mean(t3c), 3) if t3c else None},
        "daily": recs,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "premarket_rocket_overlay_v37.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    L = ["# 飙升榜注意力叠加验证 v37 (job 0048)", "",
         f"- 生成: {report['generated_at']} ｜叠加权重: {BLEND} ｜有效天: {len(recs)}", "",
         "| 口径 | mean_ic | icir | n_days |", "|---|---|---|---|",
         f"| 生产基线 edge | {bm} | {bicir} | {bnd} |",
         f"| +飙升叠加 | {cm} | {cicir} | {cnd} |", "",
         f"- Top3 累计超额: 基线 {cum_b} vs 叠加 {cum_c}",
         f"- Top3 单日均值: 基线 {report['top3_cum_excess']['mean_base']} vs 叠加 {report['top3_cum_excess']['mean_comp']}", ""]
    (audit / "premarket_rocket_overlay_v37.md").write_text("\n".join(L), encoding="utf-8")
    print(json.dumps({"ic_baseline": report["ic_baseline"], "ic_composite": report["ic_composite"],
                      "top3_cum": report["top3_cum_excess"], "n_days": len(recs)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
