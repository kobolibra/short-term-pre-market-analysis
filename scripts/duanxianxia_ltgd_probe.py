#!/usr/bin/env python3
"""
Read-only focused ltgd probe (job 0138).

Job 0136's combined output was tail-truncated by the worker and dropped the
ltgd grep (it was first in the JSON). This probe greps ONLY the fetcher + batch
for ltgd / range_period / window literals WITH a few context lines each, and
prints a small bounded JSON so nothing is truncated. Goal: pinpoint where the
multi-window (5/10/20/50) range_period collapses onto a single field. Touches
nothing.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
SCRIPTS = WS / "scripts"


def _rel(p) -> str:
    try:
        return os.path.relpath(p, WS)
    except Exception:
        return str(p)


def _grep_ctx(path: Path, pattern: str, ctx: int = 3, max_hits: int = 50) -> dict:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}
    rx = re.compile(pattern, re.IGNORECASE)
    blocks = []
    for i, ln in enumerate(lines):
        if rx.search(ln):
            lo = max(0, i - ctx)
            hi = min(len(lines), i + ctx + 1)
            block = [{"n": j + 1, "t": lines[j][:180]} for j in range(lo, hi)]
            blocks.append({"match_line": i + 1, "ctx": block})
            if len(blocks) >= max_hits:
                break
    return {"total_lines": len(lines), "block_count": len(blocks), "blocks": blocks}


def main() -> int:
    out = {"task": "ltgd_probe"}
    pat = r"ltgd|range_period|range_window|区间涨幅|区间振幅|复盘.*区间"
    for key, fname in (("fetcher", "duanxianxia_fetcher.py"), ("batch", "duanxianxia_batch.py")):
        p = SCRIPTS / fname
        out[key] = {"file": _rel(p), **_grep_ctx(p, pat)}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
