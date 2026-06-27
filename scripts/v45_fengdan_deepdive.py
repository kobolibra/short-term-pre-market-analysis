#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v45_fengdan_deepdive.py

Deep-dive on the premarket 封单 table (auction.jjlive.fengdan) ONLY.
0050 clean IC already showed numeric fields are weak/negative (amount_915 IC -0.065 ICIR -0.52,
amount_920 -0.077 n12, amount_925/latest_change_pct no signal). This job fills the gaps the
cross-sectional IC scans miss:
  1) field coverage: % non-empty for amount_915/920/925 (sparsity check).
  2) seal-time decay ratios r920=amount_920/amount_915, r925=amount_925/amount_915 (撤单/sincerity)
     -> daily Spearman IC vs excess.
  3) board_label buckets (板位): mean forward excess per label, raw + coarse(today_1st/today_multi/
     yest_board/none), both raw mean and day-demeaned (relative) mean.
  4) section sentiment (seal_total/yizi_count/t15/t20/t25, same for all rows that day): daily series
     vs that day's mean excess of fengdan stocks -> timing corr (small n, caveated).
  5) reconfirm amount_915 IC with our own n.
Premarket snapshot only (HHMMSS<=093000). excess=(close-open)/preclose*100.
Output reports/_audit/fengdan_deepdive_v45.{json,md}
"""
from __future__ import annotations
import json
import sys
import math
import traceback
from collections import defaultdict
from pathlib import Path
from datetime import datetime

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import v10_optimize as v10

DSID = "auction.jjlive.fengdan"
PREOPEN = "093000"
CODE_KEYS = ["code", "\u4ee3\u7801"]
YI = "\u4ebf"
WAN = "\u4e07"
ZUO = "\u6628"  # 昨
SHOUBAN = "\u9996\u677f"  # 首板


def pnum(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "").replace("%", "").replace("+", "")
    if s in ("", "-", "--", "None"):
        return None
    mult = 1.0
    if s.endswith(YI):
        mult, s = 1e4, s[:-1]
    elif s.endswith(WAN):
        mult, s = 1.0, s[:-1]
    try:
        return float(s) * mult
    except Exception:
        return None


def _norm(v):
    s = str(v or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:].zfill(6) if len(s) >= 6 else s


def code_of(r):
    for k in CODE_KEYS:
        if r.get(k) not in (None, ""):
            return _norm(r.get(k))
    return ""


def coarse(lbl):
    if not lbl:
        return "none"
    if lbl.startswith(ZUO):
        return "yest_board"
    if lbl == SHOUBAN:
        return "today_1st"
    return "today_multi"


def latest_preopen_rows(date_dir):
    d = date_dir / DSID
    if not d.is_dir():
        return []
    pre = [f for f in d.glob("*.json") if len(f.stem) == 6 and f.stem.isdigit() and f.stem <= PREOPEN]
    if not pre:
        return []
    try:
        payload = json.loads(sorted(pre)[-1].read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def main():
    root = Path(v10.DEFAULT_PROJECT_ROOT)
    daily = v10.Daily(root)
    cap_root = root / "captures"
    date_dirs = sorted([d for d in cap_root.glob("20*-*-*") if d.is_dir()]) if cap_root.is_dir() else []

    cov = {"amount_915": [0, 0], "amount_920": [0, 0], "amount_925": [0, 0]}
    ic_fields = defaultdict(list)  # field/ratio -> daily IC list
    board_raw = defaultdict(list)        # label -> [excess]
    board_raw_dm = defaultdict(list)     # label -> [day-demeaned excess]
    board_coarse = defaultdict(list)
    board_coarse_dm = defaultdict(list)
    sect_series = {"seal_total": [], "yizi_count": [], "t15": [], "t20": [], "t25": [], "mkt_excess": []}
    n_dates = 0

    for dd in date_dirs:
        D = dd.name
        rows = latest_preopen_rows(dd)
        if not rows:
            continue
        n_dates += 1
        # excess map for rows present
        exrows = []
        for r in rows:
            c = code_of(r)
            if not c:
                continue
            e = daily.excess(c, D)
            if e is None:
                continue
            exrows.append((c, r, e))
        if len(exrows) < 8:
            continue
        day_mean_ex = mean([e for _, _, e in exrows])
        # coverage
        for _, r, _ in exrows:
            for f in cov:
                cov[f][1] += 1
                if pnum(r.get(f)) is not None:
                    cov[f][0] += 1
        # field & ratio IC
        def field_ic(getter, key):
            xs, ys = [], []
            for c, r, e in exrows:
                v = getter(r)
                if v is not None:
                    xs.append(v)
                    ys.append(e)
            if len(xs) >= 8:
                ic = v10.spearman(xs, ys)
                if ic is not None:
                    ic_fields[key].append(ic)
        field_ic(lambda r: pnum(r.get("amount_915")), "amount_915")
        field_ic(lambda r: pnum(r.get("amount_920")), "amount_920")
        field_ic(lambda r: pnum(r.get("amount_925")), "amount_925")
        field_ic(lambda r: pnum(r.get("latest_change_pct")), "latest_change_pct")

        def ratio(r, a, b):
            va, vb = pnum(r.get(a)), pnum(r.get(b))
            if va is not None and vb not in (None, 0):
                return va / vb
            return None
        field_ic(lambda r: ratio(r, "amount_920", "amount_915"), "r920_over_915")
        field_ic(lambda r: ratio(r, "amount_925", "amount_915"), "r925_over_915")
        # board buckets
        for c, r, e in exrows:
            lbl = str(r.get("board_label") or "").strip()
            dm = e - day_mean_ex
            board_raw[lbl or "(empty)"].append(e)
            board_raw_dm[lbl or "(empty)"].append(dm)
            board_coarse[coarse(lbl)].append(e)
            board_coarse_dm[coarse(lbl)].append(dm)
        # section sentiment (from first row)
        r0 = rows[0]
        sect_series["seal_total"].append(pnum(r0.get("section_seal_total")))
        yc = r0.get("section_yizi_count")
        sect_series["yizi_count"].append(float(yc) if isinstance(yc, (int, float)) else None)
        sect_series["t15"].append(pnum(r0.get("section_t15_total")))
        sect_series["t20"].append(pnum(r0.get("section_t20_total")))
        sect_series["t25"].append(pnum(r0.get("section_t25_total")))
        sect_series["mkt_excess"].append(day_mean_ex)

    field_out = []
    for k in ["amount_915", "amount_920", "amount_925", "latest_change_pct", "r920_over_915", "r925_over_915"]:
        m, icir, nd = v10.mean_icir(ic_fields.get(k, []))
        field_out.append({"field": k, "mean_ic": m, "icir": icir, "n_days": nd})
    cov_out = {f: {"present": cov[f][0], "total": cov[f][1],
                   "pct": round(cov[f][0] / cov[f][1], 3) if cov[f][1] else None} for f in cov}

    def bucket_out(raw, dm):
        out = []
        for lbl in raw:
            out.append({"label": lbl, "n": len(raw[lbl]),
                        "mean_excess": round(mean(raw[lbl]), 3) if raw[lbl] else None,
                        "mean_excess_demeaned": round(mean(dm[lbl]), 3) if dm[lbl] else None})
        out.sort(key=lambda x: x["n"], reverse=True)
        return out

    # section timing corr
    sect_corr = []
    me = sect_series["mkt_excess"]
    for k in ["seal_total", "yizi_count", "t15", "t20", "t25"]:
        xs, ys = [], []
        for a, b in zip(sect_series[k], me):
            if a is not None and b is not None:
                xs.append(a)
                ys.append(b)
        c = v10.spearman(xs, ys) if len(xs) >= 8 else None
        sect_corr.append({"metric": k, "spearman_vs_mkt_excess": round(c, 3) if c is not None else None, "n_days": len(xs)})

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "job": "fengdan_deepdive_v45",
        "dataset": DSID,
        "n_dates": n_dates,
        "coverage": cov_out,
        "field_ic": field_out,
        "board_coarse": bucket_out(board_coarse, board_coarse_dm),
        "board_raw": bucket_out(board_raw, board_raw_dm),
        "section_sentiment_timing": sect_corr,
    }
    audit = root / "reports" / "_audit"
    audit.mkdir(parents=True, exist_ok=True)
    (audit / "fengdan_deepdive_v45.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
