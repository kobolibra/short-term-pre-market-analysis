#!/usr/bin/env python3
"""Read-only diagnostic: is dailyline (日线) data being downloaded recently?

Dailyline is produced by the postmarket chain (run_dailyline ->
duanxianxia_batch.py dailyline). It does NOT run through the agent job worker,
so there is no result JSON to inspect. This probe:
  1) greps the (server-side, untruncated) batch source for dailyline output
     path literals + the code that fetches/writes dailyline,
  2) walks the duanxianxia project dir and reports the most recent dailyline
     files and the newest files overall, with size + mtime, so we can see
     whether dailyline capture actually landed on recent trading days.
Prints a single JSON object to stdout. Makes no changes.
"""
import json
import os
import re
import time

WS = "/home/investmentofficehku/.openclaw/workspace"
PROJ = os.path.join(WS, "projects", "duanxianxia")
BATCH = os.path.join(WS, "scripts", "duanxianxia_batch.py")


def rel(p):
    try:
        return os.path.relpath(p, WS)
    except Exception:
        return p


def fmt(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


result = {"probe": "dailyline", "now": fmt(time.time())}

# 1) grep batch.py for dailyline-related lines + path literals
batch_hits = []
path_literals = set()
try:
    with open(BATCH, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    kw = re.compile(r"dailyline|daily_line|day_line|kline|\u65e5\u7ebf", re.I)
    lit = re.compile(
        r"[\"']([^\"']*(?:dailyline|daily_line|day_line|kline)[^\"']*)[\"']", re.I
    )
    for i, ln in enumerate(lines):
        if kw.search(ln):
            t = ln.rstrip()[:240]
            batch_hits.append({"n": i + 1, "t": t})
            for m in lit.finditer(ln):
                path_literals.add(m.group(1))
    result["batch_hits_count"] = len(batch_hits)
    result["batch_hits"] = batch_hits[:80]
    result["path_literals"] = sorted(path_literals)
except Exception as e:
    result["batch_read_error"] = repr(e)

# 2) walk project dir: newest dailyline files + newest files overall
dl = []
allf = []
try:
    for root, dirs, files in os.walk(PROJ):
        if os.sep + ".git" in root:
            continue
        for fn in files:
            fp = os.path.join(root, fn)
            try:
                st = os.stat(fp)
            except Exception:
                continue
            rec = {"path": rel(fp), "size": st.st_size, "mtime": fmt(st.st_mtime)}
            low = fp.lower()
            if (
                "dailyline" in low
                or "daily_line" in low
                or "day_line" in low
                or "kline" in low
            ):
                dl.append((st.st_mtime, rec))
            allf.append((st.st_mtime, rec))
    dl.sort(key=lambda x: x[0], reverse=True)
    allf.sort(key=lambda x: x[0], reverse=True)
    result["dailyline_files_count"] = len(dl)
    result["dailyline_files_recent"] = [r for _, r in dl[:40]]
    result["project_newest_files"] = [r for _, r in allf[:40]]
except Exception as e:
    result["walk_error"] = repr(e)

print(json.dumps(result, ensure_ascii=False))
