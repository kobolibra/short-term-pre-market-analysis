#!/usr/bin/env python3
"""0081: ztpool deep-dive -- can ztpool be parsed as a positional array?
A) grep every ztpool reference across scripts/*.py (find the real upstream fetcher)
B) print full ztpool parse code blocks in fetcher.py
C) dump a real ztpool capture (does rows[0] carry a positional 'raw' array? column order?)
D) probe the upstream endpoint (JSON positional array vs rendered HTML)
Decisive sections C/D printed LAST so they survive the 16000-char stdout tail.
"""
import os, json, glob

def find_ws():
    for c in [os.getcwd(), os.environ.get("WORKSPACE", ""),
              os.path.expanduser("~/.openclaw/workspace"),
              "/home/investmentofficehku/.openclaw/workspace"]:
        if c and os.path.isdir(os.path.join(c, "scripts")):
            return c
    return os.getcwd()

WS = find_ws()
SCRIPTS = os.path.join(WS, "scripts")
FETCHER = os.path.join(SCRIPTS, "duanxianxia_fetcher.py")
CAP = os.path.join(WS, "projects", "duanxianxia", "captures")

print("=== 0081 ZTPOOL DEEP DIVE ===")
print("WS:", WS)

# A) all ztpool references across scripts
print("\n--- A) ztpool references across scripts/*.py ---")
for pf in sorted(glob.glob(os.path.join(SCRIPTS, "*.py"))):
    try:
        fl = open(pf, encoding="utf-8").read().splitlines()
    except Exception:
        continue
    rel = os.path.relpath(pf, WS)
    for i, l in enumerate(fl):
        if "ztpool" in l.lower() or "\u6da8\u505c\u6c60" in l:
            print("%s:%d: %s" % (rel, i + 1, l.strip()[:160]))

# B) full ztpool code blocks in fetcher
print("\n--- B) fetcher.py ztpool code blocks ---")
try:
    lines = open(FETCHER, encoding="utf-8").read().splitlines()
    hits = [i for i, l in enumerate(lines) if "ztpool" in l.lower() or "\u6da8\u505c\u6c60" in l]
    wins = []
    for i in hits:
        s, e = max(0, i - 22), min(len(lines), i + 23)
        if wins and s <= wins[-1][1]:
            wins[-1][1] = max(wins[-1][1], e)
        else:
            wins.append([s, e])
    for s, e in wins:
        print("\n#### fetcher.py lines %d-%d" % (s + 1, e))
        for n in range(s, e):
            print("%5d  %s" % (n + 1, lines[n]))
except Exception as ex:
    print("fetcher read err:", ex)

# C) real ztpool capture
print("\n--- C) newest ztpool capture ---")
cap = None
cz = None
if os.path.isdir(CAP):
    cands = []
    for date in sorted(os.listdir(CAP)):
        dd = os.path.join(CAP, date)
        if not os.path.isdir(dd):
            continue
        for dsid in os.listdir(dd):
            if "ztpool" in dsid.lower():
                fs = sorted(glob.glob(os.path.join(dd, dsid, "*.json")))
                if fs:
                    cands.append((date, dsid, fs[-1]))
    if cands:
        cz = cands[-1]
if not cz:
    print("no ztpool capture found")
else:
    date, dsid, path = cz
    print("date=%s dsid=%s file=%s" % (date, dsid, os.path.basename(path)))
    cap = json.load(open(path, encoding="utf-8"))
    print("source_url:", cap.get("source_url"))
    print("dataset_label:", cap.get("dataset_label"))
    meta = cap.get("meta") or {}
    print("meta keys:", list(meta.keys()))
    for k, v in meta.items():
        sv = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
        print("  meta[%s] (%d chars): %s" % (k, len(sv), sv[:500]))
    rows = cap.get("rows") or []
    print("headers:", json.dumps(cap.get("headers"), ensure_ascii=False))
    print("n_rows:", len(rows))
    if rows and isinstance(rows[0], dict):
        print("row0 keys:", list(rows[0].keys()))
        print("row0:", json.dumps(rows[0], ensure_ascii=False)[:1500])
        print("row0 has 'raw'?:", "raw" in rows[0])
        if "raw" in rows[0]:
            print("row0.raw:", json.dumps(rows[0]["raw"], ensure_ascii=False)[:1500])

# D) upstream probe (LAST)
print("\n--- D) upstream probe ---")
url = cap.get("source_url") if cap else None
print("url:", url)
if url:
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            ct = resp.headers.get("Content-Type")
            body = resp.read(4000).decode("utf-8", "replace")
        st = body.lstrip()
        print("content-type:", ct)
        print("json_array?:", st.startswith("["), "json_obj?:", st.startswith("{"),
              "html?:", st.startswith("<") or "<table" in body.lower())
        print("body[:1800]:", body[:1800])
    except Exception as ex:
        print("probe failed:", repr(ex))
print("\n=== END 0081 ===")
