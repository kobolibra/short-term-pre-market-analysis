#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0107: pin the MINIMAL fix for the 002674 STILL_BUY blind spot.

0106 proved: the T-1 (6/30) bundle is fully loaded; 002674 is simply ABSENT from
the 6/30 涨停池 (home.ztpool) because it limit-DOWNED that day, so every
ztpool-derived context field is null and the 0105 gate cannot fire.

This READ-ONLY probe finds which OTHER T-1 dataset still carries 002674's
high-board / leader signal (so the gate can key off it WITHOUT depending on the
limit-up pool):
  - review.ltgd.range   (龙头梯队/高度)  <- prime candidate
  - review.fupan.plate  (涨停复盘, 板数)
  - cashflow.stock.today / 3day
  - home.ztpool board history over ALL captured days (reconstruct recent max board)
Dumps full target rows + schemas. No writes / webhook / bitable.
"""
import json
import sys
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WS / "projects" / "duanxianxia"
SCRIPTS = WS / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
CAP = PROJECT_ROOT / "captures"
TARGET = "002674"
NAME = "\u5174\u4e1a\u79d1\u6280"


def _norm(c):
    s = str(c or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _latest(date, ds):
    d = CAP / date / ds
    if not d.exists():
        return None
    files = sorted(d.glob("*.json"))
    return files[-1] if files else None


def _rows(obj):
    if isinstance(obj, list):
        return [r for r in obj if isinstance(r, dict)]
    if isinstance(obj, dict):
        for k in ("rows", "data", "list", "pool", "items", "result"):
            v = obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
            if isinstance(v, dict):
                for k2 in ("rows", "data", "list", "items"):
                    v2 = v.get(k2)
                    if isinstance(v2, list):
                        return [r for r in v2 if isinstance(r, dict)]
    return []


def _find_target(rows):
    for r in rows:
        code = _norm(r.get("code") or r.get("\u4ee3\u7801") or r.get("symbol") or r.get("c"))
        nm = str(r.get("\u540d\u79f0") or r.get("name") or "")
        if code == TARGET or (NAME and NAME in nm):
            return r
    return None


def _dump_dataset(date, ds):
    p = _latest(date, ds)
    info = {"date": date, "dataset": ds, "file": p.name if p else None}
    if not p:
        info["note"] = "no capture dir/file"
        return info
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        info["error"] = repr(e)
        return info
    info["top_level_type"] = type(obj).__name__
    if isinstance(obj, dict):
        info["top_level_keys"] = sorted(list(obj.keys()))[:40]
    rows = _rows(obj)
    info["row_count"] = len(rows)
    info["schema_keys"] = sorted(list(rows[0].keys())) if rows else []
    info["target_row"] = _find_target(rows)
    return info


def main():
    out = {"job": "0107_ltgd_leader_signal_probe", "target": TARGET, "name": NAME,
           "datasets": [], "ztpool_history": [], "bundle_counts": None, "errors": []}

    for date in ("2026-06-30", "2026-06-29"):
        for ds in ("review.ltgd.range", "review.fupan.plate",
                   "cashflow.stock.today", "cashflow.stock.3day"):
            out["datasets"].append(_dump_dataset(date, ds))

    if CAP.exists():
        for d in sorted(x.name for x in CAP.iterdir() if x.is_dir()):
            zt = CAP / d / "home.ztpool"
            if not zt.exists():
                continue
            for f in sorted(zt.glob("*.json")):
                try:
                    obj = json.loads(f.read_text(encoding="utf-8"))
                except Exception as e:
                    out["errors"].append("%s/%s: %r" % (d, f.name, e))
                    continue
                row = _find_target(_rows(obj))
                if row is not None:
                    out["ztpool_history"].append({"date": d, "file": f.name, "row": row})

    try:
        from duanxianxia_v7_1_data_loader import load_premarket_bundle
        b = load_premarket_bundle("2026-07-01", PROJECT_ROOT)
        out["bundle_counts"] = b.to_summary_dict().get("counts")
    except Exception as e:
        out["errors"].append("bundle: %r" % e)

    print("[0107] SUMMARY_JSON_BEGIN")
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print("[0107] SUMMARY_JSON_END")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
