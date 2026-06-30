#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_feature_builder_probe_20260630.py -- v11 milestone M1 validation gate.

Runs the new canonical-first feature builder (duanxianxia_feature_builder) on the
REAL persisted premarket captures and prints a JSON summary, proving on live data:
  * on-disk capture rows actually carry raw[] (positional canonical_error ~ 0)
  * the legacy mislabel (auction_volume_ratio) NEVER leaks into features
  * free_float_mktcap is FF-caliber-tagged and money is in 元
  * time-isolation excludes any post-09:29 re-fetch capture file

It also peeks one on-disk row per auction dataset to expose the real row schema
(keys / raw[] presence / raw length), which is the one fact we could not verify
from the sandbox. This empirically settles \"does each row carry raw[]?\".

READ-ONLY: builds in memory and prints to stdout (captured by agent_job_worker).
No writes, no push. rc=0 = pass; a mislabel leak or missing FF tag fails the gate.
"""
from __future__ import annotations
import json
from pathlib import Path

import duanxianxia_feature_builder as FB

WS = Path.cwd()
CAPTURES = WS / "projects" / "duanxianxia" / "captures"


def _latest_date_dir():
    if not CAPTURES.is_dir():
        return None
    dated = [d for d in CAPTURES.iterdir() if d.is_dir()]
    return sorted(dated, key=lambda d: d.name)[-1] if dated else None


def _diag():
    out = {"captures_root": str(CAPTURES), "exists": CAPTURES.is_dir()}
    if not CAPTURES.is_dir():
        return out
    dates = sorted(d.name for d in CAPTURES.iterdir() if d.is_dir())
    out["date_dirs_tail"] = dates[-8:]
    if not dates:
        return out
    latest = CAPTURES / dates[-1]
    out["latest_date"] = dates[-1]
    out["latest_datasets"] = sorted(p.name for p in latest.iterdir() if p.is_dir())
    peek = {}
    for dsid in FB.AUCTION_DATASETS:
        dsdir = latest / dsid
        if not dsdir.is_dir():
            peek[dsid] = {"present": False}
            continue
        files = sorted(dsdir.glob("*.json"))
        info = {"present": True, "n_files": len(files),
                "files_tail": [f.name for f in files[-3:]]}
        if files:
            try:
                payload = json.loads(files[-1].read_text(encoding="utf-8"))
                rows = FB._rows_of(payload)
                info["payload_top_keys"] = (
                    sorted(payload.keys()) if isinstance(payload, dict) else "list")
                info["n_rows"] = len(rows)
                if rows:
                    r0 = rows[0]
                    if isinstance(r0, dict):
                        info["row0_keys"] = sorted(r0.keys())
                        info["row0_has_raw"] = "raw" in r0
                        raw = r0.get("raw")
                        info["row0_raw_len"] = (
                            len(raw) if isinstance(raw, (list, tuple)) else None)
                    elif isinstance(r0, (list, tuple)):
                        info["row0_is_positional_list"] = True
                        info["row0_len"] = len(r0)
            except Exception as e:
                info["peek_error"] = f"{type(e).__name__}: {e}"
        peek[dsid] = info
    out["dataset_peek"] = peek
    return out


def run():
    date_dir = _latest_date_dir()
    summary = {"version": FB.VERSION, "diag": _diag()}
    if date_dir is None:
        summary["status"] = "NO_CAPTURES"
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return
    res = FB.build_feature_table(date_dir)
    feats = res["features"]
    checks = {
        "no_mislabel_leak": all("auction_volume_ratio" not in f for f in feats),
        "all_ff_caliber": all(
            f.get("free_float_mktcap_caliber") == "FF" for f in feats),
        "canonical_error_total": sum(
            c.get("canonical_error", 0) for c in res["coverage"].values()),
        "n_multi_source": sum(1 for f in feats if f["source_hit_count"] >= 2),
    }
    keep = ("code", "name", "free_float_mktcap", "bidAmount", "bidStrength",
            "volumeRatio", "grabStrength", "changeRate", "mainNetInflow",
            "sealAmount", "boardLabel", "source_hit_count", "_field_sources")
    summary.update({
        "status": "OK",
        "date": res["date"],
        "n_features": res["n_features"],
        "coverage": res["coverage"],
        "capture_meta": res["capture_meta"],
        "checks": checks,
        "sample_top": [{k: f.get(k) for k in keep} for f in feats[:12]],
    })
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    # hard gates (after printing, so the summary is always visible)
    assert checks["no_mislabel_leak"], "auction_volume_ratio leaked into features"
    assert checks["all_ff_caliber"], "FF caliber tag missing on a feature row"


run()

if __name__ == "__main__":
    print("duanxianxia_feature_builder_probe_20260630: done")
