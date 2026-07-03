#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0124 -- two-in-one forensic diagnostic (READ-ONLY, no rewrites).

Part A. WHY do postmarket overnight tables (fupan / ltgd / daily) sometimes
        land at ~01:20 next calendar day (folder=content+1) instead of ~17:20
        same day (folder=content)?
  - dump the LIVE crontab (source of truth for the schedule)
  - walk EVERY capture date for the overnight datasets and, per snapshot,
    record: folder date, snapshot HHMMSS, top-level fetched_at, the row's
    INTERNAL content date, and folder_minus_content (the off-by-one signal).
  - contrast with cashflow.stock.today (same-day EOD) as a control.

Part B. Auction VR/QC correctness the user flagged:
  B1. many '竞价涨幅' (auction_change_pct) were blank in my earlier top5 --
      is it MISSING in source or did my presentation script fail to read it?
      -> report per-group non-null coverage AND read auction_change_pct from
         each stock's OWN subject row.
  B2. grab_strength MIXES two different calibers. The qiangchou capture is
      {list.grab, list.qiangchou} -> the SAME code can appear in BOTH groups
      with DIFFERENT grab_strength. Keep the groups SEPARATE; also list the
      codes present in both to quantify the mixing on today's data.

Prints a compact JSON summary (worker keeps ~16KB stdout tail) and also writes
the full report to reports/_audit for /pull.
"""
from __future__ import annotations
import os, json, subprocess, datetime, pathlib

WS = pathlib.Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace")).resolve()
CAP = WS / "projects" / "duanxianxia" / "captures"
AUDIT = WS / "projects" / "duanxianxia" / "reports" / "_audit"
TARGET = os.environ.get("DXX_TARGET", "2026-07-03")
OVERNIGHT = ["review.fupan.plate", "review.ltgd.range", "review.daily.top_metrics"]
DATE_FIELDS = ("\u65e5\u671f", "date", "content_date")


def load(p):
    try:
        return json.loads(pathlib.Path(p).read_text(encoding="utf-8"))
    except Exception as e:
        return {"_err": f"{type(e).__name__}: {e}"}


def rows_of(obj):
    r = obj.get("rows") if isinstance(obj, dict) else None
    return r if isinstance(r, list) else []


def content_date(obj):
    for r in rows_of(obj):
        if isinstance(r, dict):
            for k in DATE_FIELDS:
                v = r.get(k)
                if v:
                    return str(v)[:10]
    return None


def _num(v):
    try:
        if v is None or (isinstance(v, str) and v.strip().lower() in ("", "none", "null", "-", "\u2014", "nan")):
            return None
        return float(str(v).replace(",", "").replace("%", ""))
    except Exception:
        return None


def date_folders():
    if not CAP.is_dir():
        return []
    out = []
    for d in sorted(CAP.glob("*")):
        if d.is_dir() and len(d.name) == 10 and d.name[4] == "-":
            out.append(d)
    return out


report = {"job": "0124", "ws": str(WS), "target": TARGET,
          "generated": datetime.datetime.now().isoformat(timespec="seconds")}

# ----- Part A.1 live crontab -------------------------------------------------
cr = subprocess.run(["crontab", "-l"], text=True, capture_output=True)
report["crontab"] = [l for l in (cr.stdout or "").splitlines() if l.strip()]
report["crontab_rc"] = cr.returncode
report["crontab_stderr"] = (cr.stderr or "")[-300:]

# ----- Part A.2 overnight cadence across all dates ---------------------------
cad = {}
for ds in OVERNIGHT + ["cashflow.stock.today"]:
    recs = []
    for d in date_folders():
        dsdir = d / ds
        if not dsdir.is_dir():
            continue
        for s in sorted(dsdir.glob("*.json")):
            obj = load(s)
            cd = content_date(obj)
            delta = None
            if cd:
                try:
                    delta = (datetime.date.fromisoformat(d.name) - datetime.date.fromisoformat(cd)).days
                except Exception:
                    delta = None
            recs.append({"folder": d.name, "snap": s.stem,
                          "fetched_at": str(obj.get("fetched_at", ""))[:19],
                          "content_date": cd, "folder_minus_content": delta,
                          "row_count": obj.get("row_count", len(rows_of(obj)))})
    cad[ds] = recs
report["cadence"] = cad

# summarise the off-by-one pattern per dataset
summ = {}
for ds, recs in cad.items():
    deltas = [r["folder_minus_content"] for r in recs if r["folder_minus_content"] is not None]
    snaps = [r["snap"] for r in recs]
    overnight_snaps = [s for s in snaps if s.isdigit() and int(s) < 60000]      # before 06:00
    evening_snaps = [s for s in snaps if s.isdigit() and 150000 <= int(s) <= 235959]
    summ[ds] = {"n": len(recs),
                 "delta_counts": {str(x): deltas.count(x) for x in sorted(set(deltas))},
                 "n_overnight_before_0600": len(overnight_snaps),
                 "n_evening_1500_2359": len(evening_snaps)}
report["cadence_summary"] = summ


# ----- Part B auction VR / QC correctness ------------------------------------
def premkt_snapshot(ds):
    dsdir = CAP / TARGET / ds
    if not dsdir.is_dir():
        return None, None
    snaps = sorted(dsdir.glob("*.json"))
    pick = None
    for s in snaps:
        if s.stem.isdigit() and 91500 <= int(s.stem) <= 93500:
            pick = s
            break
    if pick is None and snaps:
        pick = snaps[0]
    return pick, [s.stem for s in snaps]


def dump_structure(obj):
    grp = {}
    for r in rows_of(obj):
        if isinstance(r, dict):
            g = r.get("group", "<none>")
            grp[g] = grp.get(g, 0) + 1
    return {"headers": obj.get("headers"), "meta": obj.get("meta"),
            "row_count": obj.get("row_count", len(rows_of(obj))),
            "group_counts": grp}


auc = {}
for ds, metric in (("auction.jjyd.vratio", "auction_volume_ratio"),
                    ("auction.jjyd.qiangchou", "grab_strength")):
    snap, allsnaps = premkt_snapshot(ds)
    entry = {"snapshot": (snap.stem if snap else None), "all_snaps": allsnaps}
    if not snap:
        auc[ds] = {**entry, "error": "no premarket snapshot for target"}
        continue
    obj = load(snap)
    entry["structure"] = dump_structure(obj)
    # group rows
    by_group = {}
    for r in rows_of(obj):
        if isinstance(r, dict):
            by_group.setdefault(r.get("group", "<none>"), []).append(r)
    grp_out = {}
    for g, rws in by_group.items():
        cov_apct = sum(1 for r in rws if _num(r.get("auction_change_pct")) is not None)
        def keyf(r):
            v = _num(r.get(metric))
            return v if v is not None else -1e18
        top = sorted(rws, key=keyf, reverse=True)[:5]
        grp_out[g] = {
            "n": len(rws),
            "auction_change_pct_nonnull": cov_apct,
            "auction_change_pct_cov_pct": round(100.0 * cov_apct / len(rws), 1) if rws else None,
            "top5": [{"code": r.get("code"), "name": r.get("name"),
                       metric: r.get(metric),
                       "auction_change_pct": r.get("auction_change_pct"),
                       "turnover_rate_pct": r.get("turnover_rate_pct")} for r in top],
        }
    entry["by_group"] = grp_out
    auc[ds] = entry

# B2 mixing proof: codes in BOTH grab and qiangchou groups (qiangchou dataset)
qsnap, _ = premkt_snapshot("auction.jjyd.qiangchou")
mixing = []
if qsnap:
    qobj = load(qsnap)
    g_map, q_map = {}, {}
    for r in rows_of(qobj):
        if not isinstance(r, dict):
            continue
        if r.get("group") == "grab":
            g_map[r.get("code")] = r.get("grab_strength")
        elif r.get("group") == "qiangchou":
            q_map[r.get("code")] = r.get("grab_strength")
    for code in sorted(set(g_map) & set(q_map)):
        mixing.append({"code": code,
                        "grab_group_grab_strength": g_map[code],
                        "qiangchou_group_grab_strength": q_map[code]})
report["auction"] = auc
report["grab_qiangchou_same_code_both_groups"] = mixing
report["grab_qiangchou_mixing_count"] = len(mixing)

# ----- persist + print -------------------------------------------------------
AUDIT.mkdir(parents=True, exist_ok=True)
(AUDIT / "overnight_auction_diag_0124.json").write_text(
    json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

print(json.dumps(report, ensure_ascii=False, indent=2))
