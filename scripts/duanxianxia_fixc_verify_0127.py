#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_fixc_verify_0127.py -- Task 0127 (Fix C verification, additive read-only).

Verifies on real capture data:
  C1. canonical_routing.canonicalize_row preserves row['group'] onto canonical dict.
  C2. qiangchou grab/qiangchou groups separated; shared code -> different grab_strength.
  C3. master.build_master_panel maps grab_strength -> grp_grab and
      grab_strength_qiangchou -> grp_qiangchou (no cross-caliber overwrite); both covered.
  B.  auction_change_pct coverage on vratio + qiangchou (Fix B regression guard).
Writes nothing to git; prints a JSON report.
"""
from __future__ import annotations
import json
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WS = Path(os.environ.get("DXX_WS", HERE.parent))
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

report = {"task": "0127_fixc_verify", "ok": True, "checks": {}, "errors": []}


def _fail(msg):
    report["ok"] = False
    report["errors"].append(msg)


try:
    import duanxianxia_canonical as _C  # noqa: F401  (self-test at import)
    import duanxianxia_canonical_routing as R
    import duanxianxia_master_indicators as M
    report["checks"]["imports"] = "ok"
except Exception as e:  # noqa: BLE001
    _fail(f"import failed: {type(e).__name__}: {e}")
    print(json.dumps(report, ensure_ascii=False, indent=1, default=str))
    sys.exit(0)


def _norm(c):
    s = "".join(ch for ch in str(c or "") if ch.isdigit())
    return s.zfill(6)[-6:] if s else ""


def _num(x):
    try:
        return float(x)
    except Exception:  # noqa: BLE001
        return float("-inf")


cap_root = WS / "projects" / "duanxianxia" / "captures"
target = os.environ.get("DXX_TARGET")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
if target:
    date_dir = cap_root / target
else:
    dates = sorted(p.name for p in cap_root.iterdir()
                   if p.is_dir() and DATE_RE.match(p.name)) if cap_root.is_dir() else []
    date_dir = cap_root / (dates[-1] if dates else "NONE")
report["date_dir"] = str(date_dir)


def _rows_of(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("rows", "items", "data", "list"):
            if isinstance(payload.get(k), list):
                return payload[k]
    return []


def _load_last(ds):
    d = date_dir / ds
    if not d.is_dir():
        return None
    files = sorted(d.glob("*.json"))
    if not files:
        return None
    return json.loads(files[-1].read_text(encoding="utf-8"))


# ---- C1/C2: qiangchou group separation via canonicalize_row ----
q_payload = _load_last("auction.jjyd.qiangchou")
if not q_payload:
    report["checks"]["qiangchou"] = "missing_capture"
else:
    grp = {"grab": {}, "qiangchou": {}, "other": 0}
    apct_have = apct_tot = 0
    for row in _rows_of(q_payload):
        g = row.get("group") if isinstance(row, dict) else None
        c = R.canonicalize_row("auction.jjyd.qiangchou", row)
        if not isinstance(c, dict) or c.get("_canonical_error"):
            continue
        cg = c.get("group")
        if cg != g:
            _fail(f"group not preserved: row={g!r} canonical={cg!r}")
        apct_tot += 1
        if c.get("auction_change_pct") is not None:
            apct_have += 1
        if cg not in ("grab", "qiangchou"):
            grp["other"] += 1
            continue
        code = _norm(row.get("code") if isinstance(row, dict) else None)
        if code:
            grp[cg][code] = {"name": c.get("name"), "grab_strength": c.get("grab_strength")}
    report["checks"]["group_counts"] = {
        "grab": len(grp["grab"]), "qiangchou": len(grp["qiangchou"]), "other": grp["other"]}

    def _top5(d):
        items = [{"code": k, **v} for k, v in d.items()]
        return sorted(items, key=lambda r: _num(r.get("grab_strength")), reverse=True)[:5]

    report["checks"]["grab_top5"] = _top5(grp["grab"])
    report["checks"]["qiangchou_top5"] = _top5(grp["qiangchou"])
    shared = []
    for code in sorted(set(grp["grab"]) & set(grp["qiangchou"])):
        a = grp["grab"][code]["grab_strength"]
        b = grp["qiangchou"][code]["grab_strength"]
        shared.append({"code": code, "name": grp["grab"][code]["name"],
                       "grab": a, "qiangchou": b, "differ": str(a) != str(b)})
    report["checks"]["shared_codes"] = shared
    if shared and not any(s["differ"] for s in shared):
        _fail("shared codes exist but none differ across groups -- caliber still mixed?")
    report["checks"]["qiangchou_auction_change_pct_cov"] = (
        f"{apct_have}/{apct_tot}" if apct_tot else "0/0")
    if apct_tot and apct_have < apct_tot:
        report["checks"]["qiangchou_apct_incomplete"] = True

# ---- B: vratio auction_change_pct coverage ----
v_payload = _load_last("auction.jjyd.vratio")
if v_payload:
    have = tot = 0
    for row in _rows_of(v_payload):
        c = R.canonicalize_row("auction.jjyd.vratio", row)
        if not isinstance(c, dict) or c.get("_canonical_error"):
            continue
        tot += 1
        if c.get("auction_change_pct") is not None:
            have += 1
    report["checks"]["vratio_auction_change_pct_cov"] = f"{have}/{tot}" if tot else "0/0"

# ---- C3: master table mapping + panel coverage ----
try:
    tf_g = M.tables_for("grab_strength")
    tf_q = M.tables_for("grab_strength_qiangchou")
    report["checks"]["tables_for"] = {"grab_strength": tf_g, "grab_strength_qiangchou": tf_q}
    if tf_g != ["auction.jjyd.qiangchou.grp_grab"]:
        _fail(f"grab_strength maps wrong: {tf_g}")
    if tf_q != ["auction.jjyd.qiangchou.grp_qiangchou"]:
        _fail(f"grab_strength_qiangchou maps wrong: {tf_q}")
except Exception as e:  # noqa: BLE001
    _fail(f"tables_for failed: {type(e).__name__}: {e}")

try:
    res = M.build_master_panel(date_dir)
    s = res["summary"]
    report["checks"]["load_report_grp"] = {
        k: val for k, val in s["load_report"].items() if "grp_" in k}
    report["checks"]["coverage"] = {
        "grab_strength": s["coverage_pct"].get("grab_strength"),
        "grab_strength_qiangchou": s["coverage_pct"].get("grab_strength_qiangchou"),
        "auction_change_pct": s["coverage_pct"].get("auction_change_pct"),
        "volume_ratio": s["coverage_pct"].get("volume_ratio"),
    }
    sample = None
    for code, r in res["panel"].items():
        if r.get("grab_strength") is not None and r.get("grab_strength_qiangchou") is not None:
            sample = {"code": code, "name": r.get("name"),
                      "grab_strength": r.get("grab_strength"),
                      "grab_strength_qiangchou": r.get("grab_strength_qiangchou")}
            break
    report["checks"]["panel_shared_sample"] = sample
except Exception as e:  # noqa: BLE001
    _fail(f"build_master_panel failed: {type(e).__name__}: {e}")

print("=== FIX C VERIFY 0127 ===")
print(json.dumps(report, ensure_ascii=False, indent=1, default=str))
