#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0086_fieldfix_verify_dryrun_20260629.py

READ-ONLY verification probe + static dry-run for the field-fix work.
- NO writes to any tracked source file.
- NO git operations / NO push.
- Writes a detailed report under projects/duanxianxia/reports/_audit/agent_jobs/
  (which agent_job_runner.sh publishes to the agent-results branch) and prints a
  compact summary to stdout (captured by the worker as stdout_tail, last 16000 chars).

Goals:
  A) Live probe (reuse the fetcher's EXACT request logic via a Session.request tee):
     - hotlist.json real top-level keys + list lengths + samples
       (confirm the day-list key used by fetch_hotlist_day / fetch_rocket)
     - hot pool item arrays, fully index-annotated (confirm item[7] board state, item[9])
     - surge pool item arrays, index-annotated (confirm item[7], item[9], item[10])
     - _fetch_realtime_quotes(['600519']) - confirm the qt.gtimg URL brace bug
  B) Static needle scan of scripts/duanxianxia_fetcher.py: count + line + context
     for each string we intend to patch, so the apply step can assert exact hit counts.
"""
from __future__ import annotations
import os
import sys
import json
import traceback
import datetime
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
if not WS.exists():
    WS = Path(os.getcwd())
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

REPORT_DIR = WS / "projects" / "duanxianxia" / "reports" / "_audit" / "agent_jobs"
REPORT_PATH = REPORT_DIR / "0086_fieldfix_verify_dryrun.report.json"

report = {
    "job": "0086_fieldfix_verify_dryrun",
    "generated_at": datetime.datetime.now().isoformat(),
    "workspace": str(WS),
    "script_dir": str(SCRIPT_DIR),
    "live_probe": {},
    "static_needles": {},
    "module_introspection": {},
    "errors": [],
}


def _err(label, e):
    report["errors"].append({
        "label": label,
        "error": repr(e),
        "tb": traceback.format_exc()[-1200:],
    })


def safe(label, fn, default=None):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        _err(label, e)
        return default


def describe(obj, max_depth=2, _d=0):
    if _d >= max_depth:
        return type(obj).__name__
    if isinstance(obj, dict):
        return {str(k): describe(v, max_depth, _d + 1) for k, v in list(obj.items())[:40]}
    if isinstance(obj, list):
        return {"__list_len__": len(obj),
                "__elem0__": describe(obj[0], max_depth, _d + 1) if obj else None}
    if isinstance(obj, str):
        return "str[%d]:%s" % (len(obj), obj[:40])
    return obj


def find_item_rows(obj, minlen=8, _depth=0):
    if _depth > 6:
        return None
    if isinstance(obj, list):
        if obj and isinstance(obj[0], list) and len(obj[0]) >= minlen:
            return obj
        for el in obj[:5]:
            r = find_item_rows(el, minlen, _depth + 1)
            if r:
                return r
    elif isinstance(obj, dict):
        for v in obj.values():
            r = find_item_rows(v, minlen, _depth + 1)
            if r:
                return r
    return None


def index_rows(rows, n=2):
    out = []
    for row in rows[:n]:
        out.append({str(i): row[i] for i in range(len(row))})
    return out


CAP = []
MARKERS = ("getFxPoolData", "getCzPoolData", "hotlist.json", "qt.gtimg")


def _install_tee():
    import requests.sessions as _s
    if getattr(_s.Session, "_teed", False):
        return
    orig = _s.Session.request

    def tee(self, method, url, *a, **k):
        rec = {"method": method, "url": str(url)}
        try:
            resp = orig(self, method, url, *a, **k)
        except Exception as e:  # noqa: BLE001
            rec["exception"] = repr(e)
            CAP.append(rec)
            raise
        rec["status"] = getattr(resp, "status_code", None)
        u = str(url)
        if any(m in u for m in MARKERS):
            try:
                rec["json"] = resp.json()
            except Exception:  # noqa: BLE001
                try:
                    rec["text_head"] = (resp.text or "")[:1500]
                except Exception:  # noqa: BLE001
                    rec["text_head"] = None
        CAP.append(rec)
        return resp

    _s.Session.request = tee
    _s.Session._teed = True


safe("install_tee", _install_tee)

M = safe("import_fetcher", lambda: __import__("duanxianxia_fetcher"))
if M is not None:
    report["module_introspection"] = {
        "module_attrs": sorted([n for n in dir(M) if not n.startswith("__")])[:200],
        "BASE": getattr(M, "BASE", None),
        "X_BASE": getattr(M, "X_BASE", None),
    }

fetcher = None
if M is not None:
    cls = getattr(M, "DuanxianxiaFetcher", None)
    if cls is not None:
        fetcher = safe("init_fetcher", lambda: cls())
        if fetcher is not None:
            report["module_introspection"]["instance_attrs"] = sorted(
                [n for n in dir(fetcher) if not n.startswith("__")])[:300]
    else:
        report["errors"].append({"label": "find_class", "error": "DuanxianxiaFetcher not found"})


def _find_cap(marker, need_json=True):
    for r in CAP:
        if marker in r.get("url", ""):
            if not need_json or "json" in r:
                return r
    for r in CAP:
        if marker in r.get("url", ""):
            return r
    return None


def _probe_hotlist():
    for meth in ("fetch_hotlist_day", "fetch_rocket"):
        fn = getattr(fetcher, meth, None)
        if fn:
            safe("call_" + meth, lambda fn=fn: fn())
    rec = _find_cap("hotlist.json")
    if rec and "json" in rec:
        j = rec["json"]
        info = {
            "url": rec["url"],
            "top_level": describe(j, 2),
            "top_level_keys": list(j.keys()) if isinstance(j, dict) else None,
        }
        if isinstance(j, dict):
            ksum = {}
            for k, v in j.items():
                if isinstance(v, list):
                    ksum[k] = {"len": len(v), "elem0": describe(v[0], 2) if v else None}
            info["list_keys"] = ksum
        report["live_probe"]["hotlist_json"] = info
    else:
        report["live_probe"]["hotlist_json"] = {
            "captured": False,
            "caps": [{"url": r.get("url"), "status": r.get("status"), "exc": r.get("exception")}
                     for r in CAP if "hotlist" in r.get("url", "")],
        }


def _probe_pool(meth, marker, key):
    fn = getattr(fetcher, meth, None)
    if fn:
        safe("call_" + meth, lambda: fn())
    rec = _find_cap(marker)
    if rec and "json" in rec:
        rows = find_item_rows(rec["json"])
        report["live_probe"][key] = {
            "url": rec["url"],
            "shape": describe(rec["json"], 2),
            "row0_len": len(rows[0]) if rows else None,
            "rows_indexed": index_rows(rows, 2) if rows else None,
        }
    elif rec:
        report["live_probe"][key] = {"url": rec.get("url"), "status": rec.get("status"),
                                       "text_head": rec.get("text_head"), "exc": rec.get("exception")}
    else:
        report["live_probe"][key] = {"captured": False}


def _probe_rt():
    fn = getattr(fetcher, "_fetch_realtime_quotes", None)
    if fn is None:
        report["live_probe"]["realtime_quotes"] = {"method_present": False}
        return
    res = safe("call_realtime", lambda: fn(["600519"]))
    rec = _find_cap("qt.gtimg", need_json=False)
    report["live_probe"]["realtime_quotes"] = {
        "method_present": True,
        "returned": describe(res, 2) if res is not None else None,
        "captured_url": rec.get("url") if rec else None,
        "captured_exception": rec.get("exception") if rec else None,
        "note": "if captured_url contains literal { } braces, the f-string URL bug is confirmed",
    }


if fetcher is not None:
    safe("probe_hotlist", _probe_hotlist)
    safe("probe_hot", lambda: _probe_pool("fetch_hot", "getFxPoolData", "hot"))
    safe("probe_surge", lambda: _probe_pool("fetch_surge", "getCzPoolData", "surge"))
    safe("probe_rt", _probe_rt)
else:
    report["live_probe"]["skipped"] = "fetcher instance unavailable"

NEEDLES = [
    'data.get("hot_stock_day"',
    "data.get('hot_stock_day'",
    'hot_stock_day',
    'data.get("skyrocket_hour"',
    'skyrocket_hour',
    'qt.gtimg.cn/q=',
    'float(item[8])/float(item[9])',
    'float(item[8]) / float(item[9])',
    'getFxPoolData',
    'getCzPoolData',
    'jinjidata.json',
    '+ /vendor/stockdata/jinjidata.json',
    'item[7]',
    'def fetch_hot',
    'def fetch_surge',
    'def fetch_hotlist_day',
    'def fetch_rocket',
    'def _fetch_realtime_quotes',
]


def _static():
    p = SCRIPT_DIR / "duanxianxia_fetcher.py"
    text = p.read_text(encoding="utf-8")
    lines = text.splitlines()
    report["static_needles"]["file"] = str(p)
    report["static_needles"]["file_lines"] = len(lines)
    report["static_needles"]["file_bytes"] = len(text)
    res = {}
    for nd in NEEDLES:
        hits = []
        start = 0
        while True:
            i = text.find(nd, start)
            if i == -1:
                break
            ln = text.count("\n", 0, i) + 1
            hits.append(ln)
            start = i + len(nd)
            if len(hits) > 50:
                break
        ctxs = []
        for ln in hits[:6]:
            lo = max(0, ln - 3)
            hi = min(len(lines), ln + 2)
            ctxs.append({"line": ln, "context": "\n".join(lines[lo:hi])})
        res[nd] = {"count": len(hits), "lines": hits[:50], "contexts": ctxs}
    report["static_needles"]["needles"] = res


safe("static_scan", _static)


def _write_report():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


safe("write_report", _write_report)

lp = report["live_probe"]
compact = {
    "job": report["job"],
    "report_file": str(REPORT_PATH),
    "errors": report["errors"][:10],
    "module": {
        "BASE": report["module_introspection"].get("BASE"),
        "X_BASE": report["module_introspection"].get("X_BASE"),
        "has_fetcher": fetcher is not None,
    },
    "hotlist_keys": (lp.get("hotlist_json", {}) or {}).get("top_level_keys"),
    "hotlist_list_keys": (lp.get("hotlist_json", {}) or {}).get("list_keys"),
    "hot_row0": (lp.get("hot", {}) or {}).get("rows_indexed"),
    "surge_row0": (lp.get("surge", {}) or {}).get("rows_indexed"),
    "realtime": lp.get("realtime_quotes"),
    "needle_counts": {k: v["count"] for k, v in report["static_needles"].get("needles", {}).items()},
}
s = json.dumps(compact, ensure_ascii=False, default=str, indent=2)
if len(s) > 14000:
    compact.pop("hot_row0", None)
    compact.pop("surge_row0", None)
    s = json.dumps(compact, ensure_ascii=False, default=str, indent=2)
print(s)
