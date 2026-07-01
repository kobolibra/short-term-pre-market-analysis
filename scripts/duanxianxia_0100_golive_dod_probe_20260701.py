#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_0100_golive_dod_probe_20260701.py  --  v11 M5 上线校验 (go-live DoD gate).

机械核对 docs/rebuild-plan-v11.md §8 的 5 条 DoD + QX-live 9:25 pin
(docs/HANDOFF.md §5.5 marketSealRate)。READ-ONLY：import 生产模块并读取
captures/ 与 M3 backfill summary，只把 JSON 报告打到 stdout，不改任何文件。
rc=0 iff 每一项都通过，否则 rc=2。

说明（任务编号）：0094-0096 已被 orthocomp blend/attrib/prodpath 探针占用，
0097-0099 已被 M3/S5 工作占用，因此这个 go-live gate 取下一个空闲 id 0100，
它是 plan 里 “0094 上线校验 / M5” 里程碑的落地实现。
"""
from __future__ import annotations

import importlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
WS = Path.cwd()
PROJECT = WS / "projects" / "duanxianxia"
CAPTURES = PROJECT / "captures"
M3_SUMMARY = PROJECT / "reports" / "_audit" / "m3_backfill_20260701.json"
M3_CSV = PROJECT / "_all_candidates_flat_v11.csv"

S5 = {"amt_pct": 0.3232, "auction_strength": 0.0909, "liquidity": 0.2424,
      "money": 0.1616, "pressure_score": 0.1414, "weimai_strength": 0.0303,
      "orderbook": 0.0202}
# v9_edge param key -> S5 canonical key
EDGE_W = {"edge_w_amt": "amt_pct", "edge_w_auction": "auction_strength",
          "edge_w_liquidity": "liquidity", "edge_w_money": "money",
          "edge_w_pressure": "pressure_score", "edge_w_weimai": "weimai_strength",
          "edge_w_orderbook": "orderbook"}

checks = []


def check(name, ok, detail=None):
    checks.append({"check": name, "ok": bool(ok), "detail": detail})
    return bool(ok)


# --- DoD1: canonical single-source-of-truth import + self-test guard ---
try:
    import duanxianxia_canonical as C
    import duanxianxia_canonical_routing as R  # noqa: F401 (import runs self-test)
    check("DoD1_canonical_import_selftest", True,
          {"n_registry_datasets": len(C.REGISTRY),
           "datasets": sorted(C.REGISTRY.keys())})
except Exception as e:
    check("DoD1_canonical_import_selftest", False, {"error": repr(e)})

# --- DoD2: feature builder self-test + T0 time isolation enforced ---
try:
    import duanxianxia_feature_builder as FB
    t_post, _ = FB._file_time_seconds("x_100400.json", {"fetched_at": "10:04:00"})
    t_pre, _ = FB._file_time_seconds("x_092500.json", {"fetched_at": "09:25:00"})
    cut = FB._cutoff_seconds(FB.T0_DEFAULT_CUTOFF)
    iso_ok = (t_post is not None and t_post > cut and t_pre is not None and t_pre <= cut)
    check("DoD2_feature_builder_time_isolation", iso_ok,
          {"cutoff": FB.T0_DEFAULT_CUTOFF, "t_post": t_post, "t_pre": t_pre})
except Exception as e:
    check("DoD2_feature_builder_time_isolation", False, {"error": repr(e)})

# --- DoD3: historical CSV all re-derived from raw via canonical, 0 errors ---
try:
    summ = json.loads(M3_SUMMARY.read_text(encoding="utf-8"))
    csv_ok = M3_CSV.exists()
    n_rows = summ.get("n_rows_total", 0)
    errs = summ.get("total_canonical_errors", None)
    ok3 = bool(csv_ok and n_rows > 0 and errs == 0)
    check("DoD3_m3_backfill_from_raw", ok3,
          {"csv_exists": csv_ok, "n_rows": n_rows, "canonical_errors": errs,
           "n_dates_ok": summ.get("n_dates_ok")})
except Exception as e:
    check("DoD3_m3_backfill_from_raw", False, {"error": repr(e)})

# --- DoD4: edge_core refit S5 persisted (v9_edge defaults + v10 V10AMT_W) ---
try:
    v9 = importlib.import_module("duanxianxia_v9_edge")
    v9_src = Path(v9.__file__).read_text(encoding="utf-8")
    v9_defaults = {}
    v9_ok = True
    for pkey, skey in EDGE_W.items():
        m = re.search(r'p\.get\("' + re.escape(pkey) + r'",\s*([0-9.]+)\)', v9_src)
        val = float(m.group(1)) if m else None
        v9_defaults[pkey] = val
        if val is None or abs(val - S5[skey]) > 1e-9:
            v9_ok = False
    check("DoD4_v9_edge_S5_defaults", v9_ok, {"defaults": v9_defaults})
    v10 = importlib.import_module("v10_optimize")
    v10w = dict(getattr(v10, "V10AMT_W", {}) or {})
    v10_ok = bool(v10w) and all(
        (k in v10w and v10w[k] is not None and abs(v10w[k] - S5[k]) < 1e-9) for k in S5)
    check("DoD4_v10_V10AMT_W_S5", v10_ok, {"V10AMT_W": v10w})
except Exception as e:
    check("DoD4_edge_core_S5", False, {"error": repr(e)})

# --- DoD5: QX-live 9:25 pin on real captures ---
try:
    import duanxianxia_qxlive_loader as QX
    date_dirs = (sorted([d for d in CAPTURES.iterdir()
                         if d.is_dir() and d.name[:4].isdigit()])
                 if CAPTURES.is_dir() else [])
    per_date = []
    n_with_qx = n_pinned_ok = n_only_post = 0
    for dd in date_dirs:
        if QX.find_qxlive_dir(dd) is None:
            continue
        n_with_qx += 1
        res = QX.load_pinned_metrics(dd)
        meta = res["capture_meta"]
        fellback = bool(meta.get("fellback_all_post_cutoff"))
        if fellback:
            n_only_post += 1
        else:
            n_pinned_ok += 1
        per_date.append({
            "date": dd.name,
            "chosen": meta.get("chosen"),
            "chosen_time_secs": meta.get("chosen_time_secs"),
            "n_skipped_post_cutoff": meta.get("n_skipped_post_cutoff"),
            "fellback_all_post_cutoff": fellback,
            "PB": res.get("marketSealRate"),
            "PB_time_point": res["metric_time_points"].get("PB"),
            "QX": res.get("sentimentSignal"),
        })
    dod5_ok = bool(n_with_qx > 0 and n_pinned_ok > 0)
    check("DoD5_qxlive_925_pin", dod5_ok,
          {"n_dates_with_qxlive": n_with_qx, "n_pinned_ok": n_pinned_ok,
           "n_only_post_cutoff": n_only_post, "per_date": per_date[:30]})
except Exception as e:
    check("DoD5_qxlive_925_pin", False, {"error": repr(e)})

all_ok = all(c["ok"] for c in checks)
report = {
    "probe": "0100_golive_dod",
    "generated_at": datetime.now(TZ).isoformat(timespec="seconds"),
    "all_ok": all_ok,
    "n_checks": len(checks),
    "n_passed": sum(1 for c in checks if c["ok"]),
    "checks": checks,
    "dod_source": "docs/rebuild-plan-v11.md 8 + HANDOFF 5.5",
}
print(json.dumps(report, ensure_ascii=False, indent=2))
sys.exit(0 if all_ok else 2)
