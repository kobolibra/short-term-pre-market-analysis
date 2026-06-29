#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
0076 — Authoritative field census on the SERVER.
Goals:
  1. Enumerate EVERY dataset directory actually present in the latest captures
     (so we know exactly which tables exist on disk, not from contract docs).
  2. For each dataset: dump headers + the positional `raw` payload of a sample
     row, and flag any market-cap-like field (by name AND by scanning raw),
     so we can definitively confirm whether 流通市值 exists per table.
  3. Dump meta.count_meta.limit_up_count / limit_down_count for the last
     several trading days from net_amount (verify 封板率/炸板/连板 decomposition).
Outputs: prints a compact report to stdout and writes a full JSON to
  projects/duanxianxia/reports/_audit/field_census_0076.json
"""
import os, json, glob, datetime

WS = "/home/investmentofficehku/.openclaw/workspace"
PROJECT_ROOT = os.path.join(WS, "projects/duanxianxia")
CAP = os.path.join(PROJECT_ROOT, "captures")
OUT_DIR = os.path.join(PROJECT_ROOT, "reports/_audit")
os.makedirs(OUT_DIR, exist_ok=True)

MCAP_HINTS = ["cap", "市值", "流通", "ltsz", "circ", "mktcap", "mc_", "流值", "流通z", "ltz", "fbmc", "总市"]


def list_dates():
    if not os.path.isdir(CAP):
        return []
    out = []
    for d in os.listdir(CAP):
        p = os.path.join(CAP, d)
        if os.path.isdir(p) and len(d) == 10 and d[4] == '-' and d[7] == '-':
            out.append(d)
    return sorted(out)


def latest_premarket(dsdir):
    files = sorted(glob.glob(os.path.join(dsdir, "*.json")))
    pre = [f for f in files if os.path.basename(f).split('.')[0] <= "093000"]
    if pre:
        return pre[-1]
    return files[-1] if files else None


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def looks_mcap(key):
    k = str(key).lower()
    return any(h in k for h in MCAP_HINTS)


dates = list_dates()
report = {
    "generated_at": datetime.datetime.now().isoformat(),
    "captures_root": CAP,
    "n_dates": len(dates),
    "last_dates": dates[-10:],
    "datasets": {},
    "limit_up_count_by_date": {},
}

print("=== CAPTURE DATES: total %d ===" % len(dates))
print("last 12:", dates[-12:])

latest = dates[-1] if dates else None
print("\n=== LATEST CAPTURE DATE:", latest, "===")

if latest:
    latest_dir = os.path.join(CAP, latest)
    dsids = sorted([d for d in os.listdir(latest_dir) if os.path.isdir(os.path.join(latest_dir, d))])
    print("DATASET DIRECTORIES PRESENT (%d):" % len(dsids))
    for d in dsids:
        print("   -", d)
    print()
    for dsid in dsids:
        dsdir = os.path.join(latest_dir, dsid)
        f = latest_premarket(dsdir)
        if not f:
            print("### %s : NO FILES\n" % dsid)
            report["datasets"][dsid] = {"error": "no files"}
            continue
        try:
            j = load_json(f)
        except Exception as e:
            print("### %s : LOAD ERROR %s\n" % (dsid, e))
            report["datasets"][dsid] = {"error": str(e)}
            continue
        headers = j.get("headers")
        rows = j.get("rows")
        sample = rows[0] if isinstance(rows, list) and rows else None
        mcap_by_name = []
        if isinstance(sample, dict):
            for k, v in sample.items():
                if looks_mcap(k):
                    mcap_by_name.append({"key": k, "value": v})
        info = {
            "file": os.path.basename(f),
            "label": j.get("dataset_label"),
            "source_url": j.get("source_url"),
            "source_path": j.get("source_path"),
            "row_count": j.get("row_count"),
            "top_level_keys": sorted(list(j.keys())),
            "headers": headers,
            "sample_row": sample,
            "mcap_fields_by_name": mcap_by_name,
            "meta_keys": sorted(list((j.get("meta") or {}).keys())) if isinstance(j.get("meta"), dict) else None,
        }
        report["datasets"][dsid] = info
        print("### %s" % dsid)
        print("   label      :", j.get("dataset_label"))
        print("   source_url :", j.get("source_url"))
        print("   row_count  :", j.get("row_count"))
        print("   headers    :", headers)
        if mcap_by_name:
            print("   >>> MCAP-LIKE FIELD(S):", mcap_by_name)
        else:
            print("   >>> NO mcap-like header. sample raw =", (sample.get("raw") if isinstance(sample, dict) else sample))
        print()

print("\n=== limit_up_count / limit_down_count (net_amount.meta.count_meta) by date ===")
for d in dates[-8:]:
    dsdir = os.path.join(CAP, d, "auction.jjyd.net_amount")
    if not os.path.isdir(dsdir):
        print(d, ": no net_amount dir")
        continue
    f = latest_premarket(dsdir)
    if not f:
        print(d, ": no net_amount file")
        continue
    try:
        j = load_json(f)
    except Exception as e:
        print(d, ": load error", e)
        continue
    cm = (j.get("meta") or {}).get("count_meta") or {}
    luc = cm.get("limit_up_count")
    ldc = cm.get("limit_down_count")
    report["limit_up_count_by_date"][d] = {
        "file": os.path.basename(f),
        "zt": cm.get("zt"), "lb": cm.get("lb"), "zb": cm.get("zb"), "dt": cm.get("dt"), "cz": cm.get("cz"),
        "limit_up_count": luc,
        "limit_down_count": ldc,
    }
    print(d, "capture=", os.path.basename(f))
    print("    zt/lb/zb/dt/cz:", cm.get("zt"), cm.get("lb"), cm.get("zb"), cm.get("dt"), cm.get("cz"))
    print("    limit_up_count  :", json.dumps(luc, ensure_ascii=False))
    print("    limit_down_count:", json.dumps(ldc, ensure_ascii=False))

outpath = os.path.join(OUT_DIR, "field_census_0076.json")
with open(outpath, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
print("\nWROTE", outpath)
print("DONE")
