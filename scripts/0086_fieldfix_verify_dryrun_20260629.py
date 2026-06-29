#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0086 fieldfix verify + dry-run.

NO writes to git-tracked files, NO git push. Only:
  A) live-probe upstream structure (hotlist.json keys, hot/surge raw item arrays,
     qt.gtimg.cn realtime-quote brace bug) so field-fix replacements are grounded;
  B) static dry-run over duanxianxia_fetcher.py: assert each candidate replacement
     string occurs the expected number of times + dump context.
Writes its own report artifact under reports/_audit (published to agent-results).
"""
from __future__ import annotations
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

WS = Path("/home/investmentofficehku/.openclaw/workspace")
SCRIPTS = WS / "scripts"
REPORT_DIR = WS / "projects" / "duanxianxia" / "reports" / "_audit" / "agent_jobs"
TZ = ZoneInfo("Asia/Shanghai")
sys.path.insert(0, str(SCRIPTS))

report = {
    "job": "0086_fieldfix_verify_dryrun",
    "ts": datetime.now(TZ).isoformat(timespec="seconds"),
    "probes": {},
    "dryrun": {},
    "errors": [],
}


def _err(where, e):
    report["errors"].append(f"{where}: {type(e).__name__}: {e}")


# ---------- A. live upstream structure probes ----------
try:
    from duanxianxia_fetcher import DuanxianxiaFetcher, BASE, X_BASE
    f = DuanxianxiaFetcher()
    try:
        data = f._get_json(f"{X_BASE}/vendor/stockdata/hotlist.json")
        listkeys = {}
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, list):
                    sample = v[0] if v else None
                    listkeys[k] = {
                        "len": len(v),
                        "sample_keys": sorted(sample.keys()) if isinstance(sample, dict) else None,
                        "sample": ({kk: sample[kk] for kk in list(sample)[:8]} if isinstance(sample, dict) else sample),
                    }
        report["probes"]["hotlist_json"] = {
            "top_keys": sorted(data.keys()) if isinstance(data, dict) else None,
            "list_keys": listkeys,
        }
    except Exception as e:
        _err("hotlist_json", e)
    try:
        d = f._post_json(f"{BASE}/data/getFxPoolData/")
        items = d.get("list", []) or []
        first = items[0] if items else None
        report["probes"]["hot_getFxPoolData"] = {
            "n": len(items),
            "first_len": (len(first) if isinstance(first, list) else None),
            "first_item_indexed": ({str(i): first[i] for i in range(len(first))} if isinstance(first, list) else None),
        }
    except Exception as e:
        _err("hot_getFxPoolData", e)
    try:
        d = f._post_json(f"{BASE}/data/getCzPoolData/")
        items = d.get("list", []) or []
        first = items[0] if items else None
        report["probes"]["surge_getCzPoolData"] = {
            "n": len(items),
            "first_len": (len(first) if isinstance(first, list) else None),
            "first_item_indexed": ({str(i): first[i] for i in range(len(first))} if isinstance(first, list) else None),
        }
    except Exception as e:
        _err("surge_getCzPoolData", e)
    try:
        q = f._fetch_realtime_quotes(["600519"])
        report["probes"]["realtime_quotes"] = {"ok": True, "sample": q}
    except Exception as e:
        report["probes"]["realtime_quotes"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
except Exception as e:
    _err("import_fetcher", e)
    traceback.print_exc()

# ---------- B. static dry-run over fetcher.py ----------
try:
    src = (SCRIPTS / "duanxianxia_fetcher.py").read_text(encoding="utf-8")
    checks = [
        ("hotlist_day_key", 'data.get("hot_stock_day", []) or []'),
        ("hotlist_day_meta_field", '"field": "hot_stock_day",'),
        ("ztpool_source_plus", "f'{page_url} + /vendor/stockdata/jinjidata.json'"),
        ("qtimg_brace_url", 'url = f"{{https://qt.gtimg.cn/q={symbols}}}"'),
        ("hot_concept_item6", 'concept_1, concept_2 = self._split_concepts(item[6] if len(item) > 6 else "")'),
        ("hot_floatcap", 'float_cap = self._format_amount(item[9] if len(item) > 9 else None, digits=0)'),
        ("surge_turnover_recompute", 'turnover_ratio = f"{(float(item[8]) / float(item[9]) * 100):.2f}%"'),
        ("surge_floatcap_label", '"float_market_cap": float_cap,'),
        ("vratio_avr_label", '"auction_volume_ratio": item[2],'),
        ("netamount_mktcap_label", '"market_cap_yi": item[6],'),
    ]
    for name, needle in checks:
        idx = src.find(needle)
        report["dryrun"][name] = {
            "count": src.count(needle),
            "context": (src[max(0, idx - 90): idx + len(needle) + 90] if idx >= 0 else ""),
        }
except Exception as e:
    _err("dryrun_fetcher", e)

# ---------- write artifact + compact stdout ----------
try:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "0086_fieldfix_verify_dryrun.report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
except Exception as e:
    _err("write_report", e)

summary = {
    "errors": report["errors"],
    "hotlist_top_keys": report["probes"].get("hotlist_json", {}).get("top_keys"),
    "hotlist_list_keys": list((report["probes"].get("hotlist_json", {}).get("list_keys") or {}).keys()),
    "hot_n": report["probes"].get("hot_getFxPoolData", {}).get("n"),
    "hot_first_len": report["probes"].get("hot_getFxPoolData", {}).get("first_len"),
    "hot_item": report["probes"].get("hot_getFxPoolData", {}).get("first_item_indexed"),
    "surge_n": report["probes"].get("surge_getCzPoolData", {}).get("n"),
    "surge_first_len": report["probes"].get("surge_getCzPoolData", {}).get("first_len"),
    "surge_item": report["probes"].get("surge_getCzPoolData", {}).get("first_item_indexed"),
    "realtime_quotes": report["probes"].get("realtime_quotes"),
    "dryrun_counts": {k: v["count"] for k, v in report["dryrun"].items()},
}
print(json.dumps(summary, ensure_ascii=False, indent=2))
