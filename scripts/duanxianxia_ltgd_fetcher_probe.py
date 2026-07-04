#!/usr/bin/env python3
"""
Read-only fetcher-side ltgd probe (job 0139).

0138 confirmed the ltgd usage, but its combined output was tail-truncated and
the FETCHER section was dropped -- only the batch renderer hits survived. This
probe reads ONLY the fetcher, dumps the FULL body of every def whose name
contains 'ltgd' (esp. fetch_review_ltgd_range), plus a bounded line-index of
ltgd / range_period / window-literal hits. Small output so nothing truncates.
Touches nothing.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
FETCHER = WS / "scripts" / "duanxianxia_fetcher.py"


def _rel(p) -> str:
    try:
        return os.path.relpath(p, WS)
    except Exception:
        return str(p)


def _indent(s: str) -> int:
    return len(s) - len(s.lstrip(" "))


def _extract_def(lines, start: int) -> dict:
    base = _indent(lines[start])
    end = len(lines)
    for j in range(start + 1, len(lines)):
        ln = lines[j]
        if ln.strip() == "":
            continue
        ind = _indent(ln)
        if ind <= base and re.match(r"\s*(def|class)\s+", ln):
            end = j
            break
        if ind < base:
            end = j
            break
    body = [{"n": k + 1, "t": lines[k][:240]} for k in range(start, end)]
    return {"start_line": start + 1, "end_line": end, "body": body}


def main() -> int:
    try:
        lines = FETCHER.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"}))
        return 1
    out = {"file": _rel(FETCHER), "total_lines": len(lines)}
    ltgd_defs = {}
    for i, ln in enumerate(lines):
        m = re.match(r"\s*def\s+([A-Za-z0-9_]*ltgd[A-Za-z0-9_]*)\b", ln)
        if m:
            ltgd_defs[m.group(1)] = _extract_def(lines, i)
    out["ltgd_defs"] = ltgd_defs
    rx = re.compile(r"ltgd|range_period|range_window|周期|日期区间|区间涨幅|LTGD", re.IGNORECASE)
    hits = [{"n": i + 1, "t": ln[:200]} for i, ln in enumerate(lines) if rx.search(ln)]
    out["ltgd_hits_count"] = len(hits)
    out["ltgd_hits"] = hits[:80]
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
