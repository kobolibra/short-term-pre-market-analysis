#!/usr/bin/env python3
"""0080: field-caliber audit dump for under-verified datasets.
Dumps newest capture (headers + sample named rows + raw arrays) for:
qxlive (plate_summary/top_metrics), ztpool, ltgd, cashflow, review_daily, kaipan/plate.
Purpose: verify field semantics against the site before the rebuild.
"""
import os, json, glob, datetime

def find_root():
    cands = [os.getcwd(),
             os.environ.get("WORKSPACE", ""),
             os.path.expanduser("~/.openclaw/workspace"),
             "/home/investmentofficehku/.openclaw/workspace"]
    for c in cands:
        if c and os.path.isdir(os.path.join(c, "projects", "duanxianxia", "captures")):
            return os.path.join(c, "projects", "duanxianxia")
    return os.path.join(os.getcwd(), "projects", "duanxianxia")

PROJECT_ROOT = find_root()
CAP = os.path.join(PROJECT_ROOT, "captures")
OUT_DIR = os.path.join(PROJECT_ROOT, "reports", "_audit")
os.makedirs(OUT_DIR, exist_ok=True)

TARGET_KEYS = ["qxlive", "ztpool", "ltgd", "cashflow", "review_daily", "plate", "kaipan"]

def recent_dates(n=8):
    if not os.path.isdir(CAP):
        return []
    ds = sorted(d for d in os.listdir(CAP) if os.path.isdir(os.path.join(CAP, d)))
    return ds[-n:]

report = {"generated_at": datetime.datetime.now().isoformat(),
          "project_root": PROJECT_ROOT, "datasets": {}}
seen = set()
for date in reversed(recent_dates(8)):
    datedir = os.path.join(CAP, date)
    if not os.path.isdir(datedir):
        continue
    for dsid in sorted(os.listdir(datedir)):
        dsdir = os.path.join(datedir, dsid)
        if not os.path.isdir(dsdir):
            continue
        if dsid in seen:
            continue
        if not any(k in dsid.lower() for k in TARGET_KEYS):
            continue
        files = sorted(glob.glob(os.path.join(dsdir, "*.json")))
        if not files:
            continue
        newest = files[-1]
        try:
            with open(newest, encoding="utf-8") as f:
                cap = json.load(f)
        except Exception as e:
            report["datasets"][dsid] = {"error": str(e)}
            seen.add(dsid)
            continue
        seen.add(dsid)
        rows = cap.get("rows") or []
        r0 = rows[0] if rows and isinstance(rows[0], dict) else {}
        report["datasets"][dsid] = {
            "date": date,
            "file": os.path.basename(newest),
            "dataset_label": cap.get("dataset_label"),
            "dataset_kind": cap.get("dataset_kind"),
            "source_url": cap.get("source_url"),
            "n_rows": len(rows),
            "headers": cap.get("headers"),
            "row_keys": list(r0.keys()),
            "sample_rows": rows[:2],
            "meta": cap.get("meta"),
        }

outpath = os.path.join(OUT_DIR, "0080_field_caliber_dump.json")
with open(outpath, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

print("=== 0080 FIELD CALIBER DUMP ===")
print("project_root:", PROJECT_ROOT)
print("found:", list(report["datasets"].keys()))
for dsid, e in report["datasets"].items():
    print("\n### " + dsid + " [" + str(e.get("date")) + "] label=" + str(e.get("dataset_label")) + " n=" + str(e.get("n_rows")))
    print("source_url:", e.get("source_url"))
    print("headers:", json.dumps(e.get("headers"), ensure_ascii=False))
    print("row_keys:", json.dumps(e.get("row_keys"), ensure_ascii=False))
    sr = e.get("sample_rows") or []
    if sr:
        print("row0:", json.dumps(sr[0], ensure_ascii=False)[:1500])
print("\nFULL REPORT:", outpath)
