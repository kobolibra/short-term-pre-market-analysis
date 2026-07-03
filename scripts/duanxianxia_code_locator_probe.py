#!/usr/bin/env python3
"""
Read-only code locator probe (job 0136).

Gathers ground truth for two backlog items WITHOUT editing anything (server-side
grep avoids the get_file_contents truncation on the 105KB fetcher / 142KB batch):

  A) ltgd range_period multi-window (5/10/20/50) overwrite: grep fetcher + batch
     for ltgd / range_period / window literals + the matching source lines, to
     pinpoint where multiple windows collapse onto one field.
  B) 0118 backtest re-run + 0050/0055 IC + v9 edge weights: scan scripts/ by
     name+content to locate the backtest/IC/weight scripts, and dump any queue
     file that references those ids so the exact script+args can be re-queued.

Output: single JSON blob on stdout. Touches nothing.
"""
from __future__ import annotations

import glob
import json
import os
import re
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
SCRIPTS = WS / "scripts"
QUEUE = SCRIPTS / "agent_jobs" / "queue"


def _rel(p) -> str:
    try:
        return os.path.relpath(p, WS)
    except Exception:
        return str(p)


def _grep_file(path: Path, pattern: str, max_hits: int = 80) -> dict:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}
    rx = re.compile(pattern, re.IGNORECASE)
    hits = []
    for i, ln in enumerate(lines, 1):
        if rx.search(ln):
            hits.append({"n": i, "t": ln.strip()[:240]})
            if len(hits) >= max_hits:
                break
    return {"total_lines": len(lines), "hit_count": len(hits), "hits": hits}


def _locate_scripts() -> list:
    name_rx = re.compile(r"backtest|回测|\bic\b|_ic|ic_|weight|edge|scorecard|v9|0118|0050|0055", re.IGNORECASE)
    content_rx = re.compile(r"backtest|回测|edge.?weight|\bv9\b|information.?coef|信息系数|\bIC\b|0118|0050|0055", re.IGNORECASE)
    out = []
    for f in sorted(glob.glob(str(SCRIPTS / "*.py"))):
        name = os.path.basename(f)
        try:
            txt = Path(f).read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        name_hit = bool(name_rx.search(name))
        c_hits = []
        for i, ln in enumerate(txt.splitlines(), 1):
            if content_rx.search(ln):
                c_hits.append({"n": i, "t": ln.strip()[:200]})
        if name_hit or len(c_hits) >= 3:
            out.append({
                "file": _rel(f),
                "name_hit": name_hit,
                "content_hit_count": len(c_hits),
                "sample": c_hits[:8],
            })
        if len(out) >= 30:
            break
    return out


def _locate_queue() -> list:
    out = []
    base_rx = re.compile(r"0118|0050|0055|backtest|回测|\bic\b|weight", re.IGNORECASE)
    for f in sorted(glob.glob(str(QUEUE / "*.json"))):
        base = os.path.basename(f)
        if base_rx.search(base):
            try:
                content = Path(f).read_text(encoding="utf-8", errors="replace")[:2000]
            except Exception as exc:  # noqa: BLE001
                content = f"<read error: {type(exc).__name__}: {exc}>"
            out.append({"file": _rel(f), "content": content})
    return out


def main() -> int:
    out = {"task": "code_locator_probe"}
    fetcher = SCRIPTS / "duanxianxia_fetcher.py"
    batch = SCRIPTS / "duanxianxia_batch.py"
    ltgd_pat = r"ltgd|range_period|range_window|龙虎|区间|window|窗口|(?:\b5\b.*\b10\b.*\b20\b.*\b50\b)"
    out["ltgd_fetcher"] = {"file": _rel(fetcher), **_grep_file(fetcher, ltgd_pat)}
    out["ltgd_batch"] = {"file": _rel(batch), **_grep_file(batch, ltgd_pat)}
    out["candidate_scripts"] = _locate_scripts()
    out["candidate_queue_files"] = _locate_queue()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
