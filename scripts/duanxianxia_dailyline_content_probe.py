#!/usr/bin/env python3
"""
Read-only dailyline CONTENT probe (job 0135).

WHY: job 0133 confirmed via file mtimes that the dailyline capture RAN today
(~18:27 Asia/Shanghai) and rewrote thousands of CSVs. This probe closes the
remaining gap -- did today's actual trading bar land? -- by reading today's
dailyline report summary (already-have / newly-added counts, success/fail) and
sampling stock CSV tails (latest date + row count). Touches nothing.

Output: single JSON blob on stdout.
"""
from __future__ import annotations

import csv
import glob
import json
import os
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
PROJ = WS / "projects/duanxianxia"
DL_STOCKS = PROJ / "dailyline/stocks"


def _rel(p: str) -> str:
    try:
        return os.path.relpath(p, WS)
    except Exception:
        return str(p)


def _newest(paths):
    best = None
    best_m = -1.0
    for p in paths:
        try:
            m = os.path.getmtime(p)
        except OSError:
            continue
        if m > best_m:
            best_m = m
            best = p
    return best


def _read_report() -> dict:
    cand = glob.glob(str(PROJ / "reports" / "*" / "dailyline" / "*.json"))
    newest = _newest(cand)
    out = {"report_path": None, "report": None, "report_count": len(cand)}
    if newest:
        out["report_path"] = _rel(newest)
        try:
            out["report"] = json.loads(Path(newest).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            out["report"] = f"<read error: {type(exc).__name__}: {exc}>"
    # also surface today's manifest capture if present
    man = glob.glob(str(PROJ / "captures" / "*" / "dailyline.stock.manifest" / "*.json"))
    newest_man = _newest(man)
    if newest_man:
        out["manifest_path"] = _rel(newest_man)
        try:
            mtime = os.path.getmtime(newest_man)
            out["manifest_mtime_epoch"] = int(mtime)
        except OSError:
            pass
    return out


def _csv_tail(path: str, n: int = 2) -> dict:
    try:
        with open(path, newline="", encoding="utf-8") as fp:
            rows = list(csv.reader(fp))
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}
    if not rows:
        return {"row_count": 0}
    header = rows[0]
    data = rows[1:]
    return {
        "header": header,
        "row_count": len(data),
        "tail": data[-n:] if data else [],
    }


def _sample_csvs(k: int = 10) -> dict:
    files = sorted(glob.glob(str(DL_STOCKS / "*.csv")))
    total = len(files)
    picks = []
    if files:
        step = max(1, total // k)
        picks = files[::step][:k]
    samples = []
    for f in picks:
        info = _csv_tail(f)
        info["file"] = _rel(f)
        try:
            info["mtime_epoch"] = int(os.path.getmtime(f))
        except OSError:
            pass
        samples.append(info)
    return {"stock_csv_total": total, "samples": samples}


def main() -> int:
    result = {"task": "dailyline_content_probe"}
    result.update(_read_report())
    result.update(_sample_csvs())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
