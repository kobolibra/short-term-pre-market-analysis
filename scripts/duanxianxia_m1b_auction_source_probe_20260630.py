#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_m1b_auction_source_probe_20260630.py -- v11 milestone M1b validation gate.

Validates, on the REAL persisted premarket captures via the TRUE production
entrypoint build_v72_decisions (batch -> premarket_v7_3_runner -> run_v7_2),
that the canonical-first auction scoring is actually taking effect in production:
  * meta.auction_source == "canonical_feature_builder" (the canonical raw[]
    rebuilt source rows fed compute_auction_strengths, not the legacy named rows)
  * auction_strengths is populated and non-degenerate
  * NO ranking regression vs the legacy named-field path: we independently
    recompute the legacy auction_strengths from the same bundle and report the
    Spearman rank correlation + a top-N side-by-side of both scores per code.

This is read-only: it builds in memory and prints a JSON summary to stdout
(captured by agent_job_worker). No writes, no push. rc=0 = pass.
"""
from __future__ import annotations
import json
import traceback
from pathlib import Path

import duanxianxia_premarket_v7_2_runner as RUN
from duanxianxia_v7_2_auction_strength import compute_auction_strengths

WS = Path.cwd()
PROJECT_ROOT = WS / "projects" / "duanxianxia"
CAPTURES = PROJECT_ROOT / "captures"


def _latest_date():
    if not CAPTURES.is_dir():
        return None
    dates = sorted(d.name for d in CAPTURES.iterdir() if d.is_dir())
    return dates[-1] if dates else None


def _score(v):
    """Pull a single numeric auction strength out of whatever shape the engine returns."""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, dict):
        for k in ("auction_strength", "alpha", "strength", "score"):
            x = v.get(k)
            if isinstance(x, (int, float)):
                return float(x)
        return None
    for k in ("auction_strength", "alpha", "strength", "score"):
        if hasattr(v, k):
            x = getattr(v, k)
            if isinstance(x, (int, float)):
                return float(x)
    return None


def _spearman(xs, ys):
    n = len(xs)
    if n < 3:
        return None

    def ranks(a):
        order = sorted(range(n), key=lambda i: a[i])
        r = [0] * n
        for rank, i in enumerate(order):
            r[i] = rank
        return r

    rx, ry = ranks(xs), ranks(ys)
    d2 = sum((rx[i] - ry[i]) ** 2 for i in range(n))
    return 1.0 - 6.0 * d2 / (n * (n * n - 1))


def run():
    summary = {"probe": "m1b_auction_source_20260630", "project_root": str(PROJECT_ROOT)}
    date = _latest_date()
    summary["date"] = date
    if date is None:
        summary["status"] = "NO_CAPTURES"
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return
    try:
        ctx = RUN.build_v72_decisions(date, PROJECT_ROOT)
    except Exception as e:
        summary["status"] = "BUILD_ERROR"
        summary["error"] = f"{type(e).__name__}: {e}"
        summary["traceback"] = traceback.format_exc()
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        raise

    meta = ctx.get("meta") or {}
    bundle = ctx["bundle"]
    v71 = bundle.v71
    params = ctx.get("params") or {}
    candidates = ctx.get("candidates") or []
    codes = [c["code"] for c in candidates if c.get("code")]
    canonical = ctx.get("auction_strengths") or {}
    weimai_rows = getattr(bundle, "auction_weimai_rows", None) or []

    # Independently recompute the legacy named-field path on the same bundle.
    legacy = compute_auction_strengths(
        codes,
        v71.auction_vratio,
        v71.auction_qiangchou,
        v71.auction_netamount,
        v71.auction_fengdan,
        params,
        weimai_rows=weimai_rows,
    )

    pairs = []
    for code in codes:
        cs = _score(canonical.get(code))
        ls = _score(legacy.get(code))
        if cs is not None and ls is not None:
            pairs.append((code, cs, ls))
    cs_list = [p[1] for p in pairs]
    ls_list = [p[2] for p in pairs]
    rho = _spearman(cs_list, ls_list)

    # canonical source-row coverage (how many rows the canonical layer rebuilt)
    rows_by_dataset = {
        RUN.DS_AUCTION_VRATIO: list(getattr(v71, "auction_vratio", []) or []),
        RUN.DS_AUCTION_QIANGCHOU: list(getattr(v71, "auction_qiangchou", []) or []),
        RUN.DS_AUCTION_NETAMOUNT: list(getattr(v71, "auction_netamount", []) or []),
        RUN.DS_AUCTION_WEIMAI: list(weimai_rows),
    }
    canon_src, used_canonical = RUN._canonical_auction_source_rows(rows_by_dataset)
    src_cov = {}
    if isinstance(canon_src, dict):
        for k in ("vratio_rows", "qiangchou_rows", "netamount_rows", "weimai_rows", "fengdan_rows"):
            v = canon_src.get(k)
            src_cov[k] = len(v) if isinstance(v, list) else None

    top = sorted(pairs, key=lambda p: p[1], reverse=True)[:15]
    summary.update({
        "status": "OK",
        "auction_source": meta.get("auction_source"),
        "used_canonical_helper": bool(used_canonical),
        "candidate_count": len(candidates),
        "n_codes": len(codes),
        "n_canonical_strengths": len(canonical),
        "n_legacy_strengths": len(legacy),
        "n_comparable": len(pairs),
        "n_canonical_nonzero": sum(1 for x in cs_list if x and x > 0),
        "spearman_canonical_vs_legacy": rho,
        "canonical_source_row_coverage": src_cov,
        "legacy_input_rows": {
            "vratio": len(getattr(v71, "auction_vratio", []) or []),
            "qiangchou": len(getattr(v71, "auction_qiangchou", []) or []),
            "netamount": len(getattr(v71, "auction_netamount", []) or []),
            "fengdan": len(getattr(v71, "auction_fengdan", []) or []),
            "weimai": len(weimai_rows),
        },
        "top15_canonical_vs_legacy": [
            {"code": c, "canonical": round(cs, 3), "legacy": round(ls, 3), "delta": round(cs - ls, 3)}
            for (c, cs, ls) in top
        ],
        "warnings": list(getattr(bundle, "warnings", []) or [])[:20],
    })
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))

    # hard gates (after printing, so the summary is always visible)
    assert meta.get("auction_source") == "canonical_feature_builder", (
        "production v7.2 is NOT on the canonical-first path: auction_source="
        + repr(meta.get("auction_source"))
    )
    assert len(canonical) > 0, "canonical auction_strengths is empty"
    assert any(x and x > 0 for x in cs_list), "all canonical auction strengths are zero"


run()

if __name__ == "__main__":
    print("duanxianxia_m1b_auction_source_probe_20260630: done")
