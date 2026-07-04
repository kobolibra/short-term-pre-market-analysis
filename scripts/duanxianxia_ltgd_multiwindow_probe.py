#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_ltgd_multiwindow_probe.py -- Task 0141 (read-only, additive).

Evidence for item A "ltgd range_period 多窗口(5/10/20/50) 覆盖问题".
0139 confirmed the FETCHER emits all 4 windows (LTGD_RANGE_WINDOWS=[5,10,20,50])
when fetch_review_ltgd_range is called with no range_expr. This probe checks the
DOWNSTREAM reality on a REAL committed/on-disk capture:
  1) Does a real review.ltgd.range capture actually store all 4 window groups?
  2) How many distinct codes, and across how many windows does each code appear?
     (ltgd is a per-window RANK table -> codes may NOT overlap across windows;
      this decides whether a per-window pivot yields dense or sparse fields.)
  3) Prove the by_code last-wins collapse used by pit_panel._load_map and
     master.build_master_panel: after canonicalize + code-keyed merge, which
     range_period survives per code, and how many rows are lost to the overwrite.
Read-only. Small single-purpose output to avoid stdout_tail truncation.
"""
from __future__ import annotations
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
WS = HERE.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from duanxianxia_master_indicators import _rows_of, _row_code, _norm_code  # noqa: E402
from duanxianxia_canonical_routing import canonicalize_rows_by_id  # noqa: E402

DSID = "review.ltgd.range"
PERIOD_KEY = "\u5468\u671f"   # 周期
CODE_KEY = "\u4ee3\u7801"     # 代码
CAP = WS / "projects" / "duanxianxia" / "captures"


def _find_captures():
    if not CAP.is_dir():
        return []
    hits = []
    for datef in sorted(CAP.iterdir()):
        dd = datef / DSID
        if dd.is_dir():
            for p in sorted(dd.glob("*.json")):
                hits.append(p)
    return hits


def _extract_rows(payload):
    rows = list(_rows_of(payload))
    if rows:
        return rows
    # defensive fallbacks if _rows_of does not recognise this payload shape
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("rows", "data", "list", "result"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict) and isinstance(v.get("rows"), list):
                return v["rows"]
    return []


def main():
    out = {"task": "0141_ltgd_multiwindow_probe", "dataset": DSID,
           "captures_root": str(CAP)}
    hits = _find_captures()
    out["total_capture_files"] = len(hits)
    out["recent_capture_files"] = [str(h.relative_to(CAP)) for h in hits[-6:]]
    if not hits:
        out["error"] = "no review.ltgd.range capture found on disk"
        print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
        return
    chosen = hits[-1]
    out["chosen_capture"] = str(chosen.relative_to(CAP))
    try:
        payload = json.loads(chosen.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        out["error"] = f"read/parse failed: {type(e).__name__}: {e}"
        print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
        return
    rows = _extract_rows(payload)
    out["raw_row_count"] = len(rows)
    if isinstance(payload, dict):
        meta = payload.get("meta")
        if isinstance(meta, dict):
            out["capture_meta_windows"] = meta.get("windows")
            out["capture_meta_latest_date"] = meta.get("latest_date")

    # 1) raw 周期 distribution
    raw_periods = Counter(str(r.get(PERIOD_KEY)) for r in rows if isinstance(r, dict))
    out["raw_period_distribution"] = dict(raw_periods)

    # 2) per-code window coverage
    code_windows = defaultdict(set)
    for r in rows:
        if not isinstance(r, dict):
            continue
        c = _row_code(r) or _norm_code(r.get(CODE_KEY))
        if c:
            code_windows[c].add(str(r.get(PERIOD_KEY)))
    cov_dist = Counter(len(w) for w in code_windows.values())
    out["distinct_codes"] = len(code_windows)
    out["per_code_window_count_dist"] = dict(sorted(cov_dist.items()))
    out["sample_codes_windows"] = {
        c: sorted(w) for c, w in list(code_windows.items())[:6]
    }

    # 3) prove the by_code last-wins collapse (same merge as pit_panel/master)
    canon = canonicalize_rows_by_id(DSID, rows)
    by_code = {}
    for r, c in zip(rows, canon):
        code = _row_code(r) or _norm_code(c.get("code"))
        if code:
            by_code[code] = c
    survivor_periods = Counter(str(v.get("range_period")) for v in by_code.values())
    out["canonical_total_rows"] = len(canon)
    out["bycode_survivor_count"] = len(by_code)
    out["bycode_survivor_period_dist"] = dict(survivor_periods)
    out["rows_lost_to_collapse"] = len(canon) - len(by_code)
    out["collapse_confirmed"] = (len(canon) - len(by_code)) > 0

    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    main()
