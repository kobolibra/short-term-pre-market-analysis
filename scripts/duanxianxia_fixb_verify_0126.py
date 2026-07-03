#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0126 -- verify Fix B (auction_change_pct fallback) end-to-end.

Importing canonical/routing/master runs their import-time self-tests (so a broken
edit fails this job loudly). Then re-derive today's vratio+qiangchou
auction_change_pct coverage THROUGH the canonical routing layer to prove the
fallback recovers the value from the text field on real captures.
"""
from __future__ import annotations
import os, json, sys, pathlib

WS = pathlib.Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace")).resolve()
SCRIPTS = WS / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
CAP = WS / "projects" / "duanxianxia" / "captures"
TARGET = os.environ.get("DXX_TARGET", "2026-07-03")

out = {"job": "0126", "target": TARGET}
try:
    import duanxianxia_canonical as C
    import duanxianxia_canonical_routing as R
    import duanxianxia_master_indicators as M  # noqa: F401
    out["imports"] = "ok (canonical/routing/master self-tests passed on import)"
    out["n_datasets"] = len(C.REGISTRY)
except Exception as e:
    out["imports_error"] = f"{type(e).__name__}: {e}"
    print(json.dumps(out, ensure_ascii=False, indent=2))
    raise SystemExit(1)


def load_first(ds):
    d = CAP / TARGET / ds
    if not d.is_dir():
        return None
    fs = sorted(d.glob("*.json"))
    return json.loads(fs[0].read_text(encoding="utf-8")) if fs else None


def rows_of(o):
    r = o.get("rows") if isinstance(o, dict) else None
    return r if isinstance(r, list) else []


res = {}
for ds in ("auction.jjyd.vratio", "auction.jjyd.qiangchou"):
    obj = load_first(ds)
    if not obj:
        res[ds] = {"error": "no snapshot"}
        continue
    rows = rows_of(obj)
    canon = R.canonicalize_rows_by_id(ds, rows)
    n = len(canon)
    nonnull = sum(1 for c in canon if isinstance(c, dict) and c.get("auction_change_pct") is not None)
    sample = [{"code": c.get("code"), "name": c.get("name"),
               "auction_change_pct": c.get("auction_change_pct")}
              for c in canon[:5] if isinstance(c, dict)]
    res[ds] = {"n": n, "auction_change_pct_nonnull": nonnull,
               "cov_pct": round(100.0 * nonnull / n, 1) if n else None,
               "sample": sample}
out["auction_change_pct_after_fix"] = res
print(json.dumps(out, ensure_ascii=False, indent=2))
