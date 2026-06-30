#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_feature_builder.py  --  v11 milestone M1 (L3 feature/loader rebuild).

Canonical-first replacement for the legacy transform-2 loader.

The old loader (duanxianxia_premarket_v6/_v7 -> _dataset_rows / _merge_candidates)
consumed the fetcher's NAMED rows, which carry historical mislabels
(e.g. vratio/qiangchou raw[2] "auction_volume_ratio" is really FF market cap).
This builder NEVER reads those named fields. It re-derives every value from the
stored raw[] array via the canonical layer (duanxianxia_canonical[_routing]), so
mislabels cannot leak into features. It then merges the 4 premarket auction
sources into one flat, time-isolated feature table keyed by stock code, exposing
the v10 factor primitives (FINAL semantics, see docs/rebuild-plan-v11.md +
docs/HANDOFF.md §5.2) with canonical names and money in 元.

Time isolation (v10 KEEP rule): T0 premarket auction captures are taken in the
09:25 cron window. This builder additionally refuses any capture file whose
embedded wall-clock timestamp is confidently AFTER the T0 cutoff (default
09:29), which is exactly how an occasional 10:04 re-fetch would otherwise leak
look-ahead data into T0 features.

Public API:
    VERSION
    AUCTION_DATASETS
    DATASET_KINDS
    canonical_rows_for_dataset(rows, dataset_id) -> (canon_rows, n_errors)
    build_from_datasets(datasets, *, date=None, cutoff='09:29', capture_meta=None) -> dict
    build_feature_table(capture_dir, *, cutoff='09:29') -> dict
    _self_test()

CLI:
    python3 duanxianxia_feature_builder.py captures/<YYYY-MM-DD> [--cutoff HH:MM] [--out path]

Importing this module runs _self_test() on the real job-0089 sample rows; any
routing / merge / unit / time-isolation regression raises AssertionError and
blocks import (same guard pattern as duanxianxia_canonical).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from duanxianxia_canonical import REGISTRY
from duanxianxia_canonical_routing import canonicalize_row, KIND_TO_DATASET

VERSION = "feature_builder_v11.0"

# The 4 premarket auction sources merged into the T0 feature table.
AUCTION_DATASETS: Tuple[str, ...] = (
    "auction.jjyd.vratio",
    "auction.jjyd.qiangchou",
    "auction.jjyd.net_amount",
    "auction.jjyd.weimai",
)

# dataset_id -> fetcher FetchResult.kind (reverse of routing map), for reference.
DATASET_KINDS: Dict[str, str] = {v: k for k, v in KIND_TO_DATASET.items()}

# Short source tags used in source_hits / provenance.
_SRC_TAG = {
    "auction.jjyd.vratio": "vratio",
    "auction.jjyd.qiangchou": "qiangchou",
    "auction.jjyd.net_amount": "net_amount",
    "auction.jjyd.weimai": "weimai",
}

# Per-canonical-field source priority (first present non-null wins). Rationale:
#  - free_float_mktcap: weimai is exact 元; net_amount/vratio are 亿-rounded.
#  - auction_turnover (bidAmount): all four carry it; prefer vratio then net_amount.
#  - main_net_inflow: net_amount + weimai both 元 after canonical; prefer net_amount.
#  - auction_change_pct: weimai stores the same quantity under auction_change.
_MERGE_PRIORITY: Dict[str, Sequence[str]] = {
    "free_float_mktcap": ("auction.jjyd.weimai", "auction.jjyd.net_amount",
                          "auction.jjyd.vratio", "auction.jjyd.qiangchou"),
    "auction_turnover": ("auction.jjyd.vratio", "auction.jjyd.net_amount",
                         "auction.jjyd.qiangchou", "auction.jjyd.weimai"),
    "auction_change_pct": ("auction.jjyd.net_amount", "auction.jjyd.vratio",
                           "auction.jjyd.qiangchou"),
    "auction_change": ("auction.jjyd.weimai",),
    "latest_change_pct": ("auction.jjyd.vratio", "auction.jjyd.net_amount",
                          "auction.jjyd.qiangchou", "auction.jjyd.weimai"),
    "turnover_rate": ("auction.jjyd.vratio", "auction.jjyd.net_amount",
                      "auction.jjyd.qiangchou", "auction.jjyd.weimai"),
    "main_net_inflow": ("auction.jjyd.net_amount", "auction.jjyd.weimai"),
    "seal_amount": ("auction.jjyd.weimai", "auction.jjyd.vratio"),
    "volume_ratio": ("auction.jjyd.vratio",),
    "grab_strength": ("auction.jjyd.qiangchou",),
    "concept": ("auction.jjyd.vratio", "auction.jjyd.net_amount",
                "auction.jjyd.qiangchou", "auction.jjyd.weimai"),
    "name": ("auction.jjyd.vratio", "auction.jjyd.net_amount",
             "auction.jjyd.qiangchou", "auction.jjyd.weimai"),
    "price": ("auction.jjyd.weimai",),
    "main_net_inflow_full": ("auction.jjyd.weimai",),
    "super_large_order": ("auction.jjyd.weimai",),
    "large_order": ("auction.jjyd.weimai",),
    "board_label": ("auction.jjyd.weimai",),
    "yesterday_auction_turnover": ("auction.jjyd.vratio", "auction.jjyd.qiangchou"),
}

T0_DEFAULT_CUTOFF = "09:29"
_TS_KEYS = ("fetched_at", "captured_at", "capture_time", "timestamp",
            "ts", "time", "asof", "datetime")

# Fail fast if a dataset id drifts away from the canonical registry.
assert all(d in REGISTRY for d in AUCTION_DATASETS), \
    "AUCTION_DATASETS out of sync with duanxianxia_canonical.REGISTRY"


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _norm_code(code: Any) -> str:
    s = str(code or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s
    if len(digits) > 6:
        digits = digits[-6:]
    return digits.zfill(6)


def _cutoff_seconds(cutoff: str) -> int:
    try:
        hh, mm = str(cutoff).split(":")[:2]
        return int(hh) * 3600 + int(mm) * 60
    except Exception:
        return 9 * 3600 + 29 * 60


def _parse_hhmm_seconds(text: Any) -> Optional[int]:
    """Extract a wall-clock time-of-day (seconds) from a string, or None."""
    if text is None:
        return None
    m = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", str(text))
    if not m:
        return None
    h, mi, se = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
    if 0 <= h < 24 and 0 <= mi < 60:
        return h * 3600 + mi * 60 + se
    return None


def _file_time_seconds(path: Any, payload: Any) -> Tuple[Optional[int], str]:
    """Best-effort wall-clock seconds for a capture.
    Prefers a payload timestamp key; only uses filename digits when they clearly
    look like a trailing HHMMSS, to avoid misreading date digits as a time."""
    if isinstance(payload, Mapping):
        for container, prefix in ((payload, "payload"), (payload.get("meta"), "meta")):
            if isinstance(container, Mapping):
                for k in _TS_KEYS:
                    if k in container:
                        t = _parse_hhmm_seconds(container.get(k))
                        if t is not None:
                            return t, f"{prefix}.{k}"
    stem = Path(str(path)).stem
    digits = "".join(ch for ch in stem if ch.isdigit())
    if len(digits) >= 6:
        tail = digits[-6:]
        h, mi, se = int(tail[0:2]), int(tail[2:4]), int(tail[4:6])
        if h < 24 and mi < 60 and se < 60:
            return h * 3600 + mi * 60 + se, "filename.HHMMSS"
    return None, "unknown"


def _safe_load(path: Path) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _rows_of(payload: Any) -> List[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, Mapping):
        for k in ("rows", "items", "data", "list"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


def _pick_capture_file(dataset_dir: Path, cutoff_secs: int):
    """Choose the latest capture file at/under the T0 cutoff. Files with a
    confident timestamp AFTER the cutoff are excluded (look-ahead guard)."""
    files = sorted(Path(dataset_dir).glob("*.json"))
    if not files:
        return None, {"n_files": 0}
    annotated = []
    for f in files:
        payload = _safe_load(f)
        t, src = _file_time_seconds(f, payload)
        annotated.append((f, payload, t, src))
    eligible = [a for a in annotated if a[2] is None or a[2] <= cutoff_secs]
    skipped = [a for a in annotated if a[2] is not None and a[2] > cutoff_secs]
    pool = eligible or annotated
    chosen = max(pool, key=lambda a: (a[2] if a[2] is not None else -1, a[0].name))
    meta = {
        "n_files": len(files),
        "chosen": chosen[0].name,
        "chosen_time_secs": chosen[2],
        "time_source": chosen[3],
        "n_skipped_post_cutoff": len(skipped),
        "skipped": [s[0].name for s in skipped],
        "fellback_all_post_cutoff": not eligible,
    }
    return (chosen[0], chosen[1]), meta


# --------------------------------------------------------------------------- #
# Canonicalisation + merge
# --------------------------------------------------------------------------- #
def canonical_rows_for_dataset(rows: Sequence[Any], dataset_id: str) -> Tuple[List[Dict[str, Any]], int]:
    """Canonicalise every fetcher row; count (never silently drop) rows whose
    raw[] is missing."""
    out: List[Dict[str, Any]] = []
    errors = 0
    for row in rows or []:
        c = canonicalize_row(dataset_id, row)
        if isinstance(c, Mapping) and c.get("_canonical_error"):
            errors += 1
            continue
        out.append(c)
    return out, errors


def _pick(field: str, srcmap: Mapping[str, Mapping[str, Any]]):
    pri = _MERGE_PRIORITY.get(field) or tuple(srcmap.keys())
    for dsid in pri:
        c = srcmap.get(dsid)
        if c is not None and c.get(field) is not None:
            return c.get(field), dsid
    for dsid, c in srcmap.items():
        if c.get(field) is not None:
            return c.get(field), dsid
    return None, None


def _assemble(code: str, srcmap: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    prov: Dict[str, str] = {}

    def take(field: str):
        v, dsid = _pick(field, srcmap)
        if dsid is not None:
            prov[field] = _SRC_TAG.get(dsid, dsid)
        return v

    name = take("name")
    ff = take("free_float_mktcap")
    bid_amount = take("auction_turnover")
    change_rate = take("auction_change_pct")
    if change_rate is None:
        change_rate = take("auction_change")  # weimai's name for the same quantity
    turnover_rate = take("turnover_rate")
    main_net = take("main_net_inflow")
    seal_amount = take("seal_amount")
    volume_ratio = take("volume_ratio")
    grab_strength = take("grab_strength")
    concept = take("concept")
    price = take("price")
    latest = take("latest_change_pct")
    main_net_full = take("main_net_inflow_full")
    super_large = take("super_large_order")
    large = take("large_order")
    board = take("board_label")

    bid_strength = None
    if bid_amount is not None and ff not in (None, 0):
        bid_strength = bid_amount / ff * 1e4

    sources = sorted(_SRC_TAG.get(d, d) for d in srcmap.keys())
    return {
        "code": code,
        "name": name or "",
        "free_float_mktcap": ff,
        "free_float_mktcap_caliber": "FF",
        "bidAmount": bid_amount,            # 竞价成交额 (元)
        "bidStrength": bid_strength,        # bidAmount / FF * 1e4
        "volumeRatio": volume_ratio,        # vratio raw[11]
        "grabStrength": grab_strength,      # qiangchou raw[11]
        "changeRate": change_rate,          # 竞价涨幅 (%)
        "latestChangePct": latest,
        "turnoverRate": turnover_rate,
        "mainNetInflow": main_net,
        "mainNetInflowFull": main_net_full,
        "superLargeOrder": super_large,
        "largeOrder": large,
        "sealAmount": seal_amount,
        "boardLabel": board,
        "price": price,
        "concept": concept,
        "source_hits": sources,
        "source_hit_count": len(sources),
        "_field_sources": prov,
    }


def _merge(datasets_canon: Mapping[str, Sequence[Mapping[str, Any]]]) -> List[Dict[str, Any]]:
    by_code: Dict[str, Dict[str, Mapping[str, Any]]] = {}
    for dsid in AUCTION_DATASETS:
        for c in datasets_canon.get(dsid) or []:
            code = _norm_code(c.get("code"))
            if not code:
                continue
            by_code.setdefault(code, {})[dsid] = c
    return [_assemble(code, srcmap) for code, srcmap in by_code.items()]


def build_from_datasets(datasets: Mapping[str, Sequence[Any]], *,
                        date: Optional[str] = None,
                        cutoff: str = T0_DEFAULT_CUTOFF,
                        capture_meta: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Pure core: dataset_id -> fetcher rows  ->  merged canonical feature table."""
    datasets_canon: Dict[str, List[Dict[str, Any]]] = {}
    coverage: Dict[str, Dict[str, int]] = {}
    for dsid in AUCTION_DATASETS:
        rows = datasets.get(dsid) or []
        canon, errors = canonical_rows_for_dataset(rows, dsid)
        datasets_canon[dsid] = canon
        coverage[dsid] = {"rows_in": len(rows), "canonical_ok": len(canon),
                          "canonical_error": errors}
    features = _merge(datasets_canon)
    features.sort(key=lambda f: (-f["source_hit_count"], f["code"]))
    return {
        "version": VERSION,
        "date": date,
        "t0_cutoff": cutoff,
        "n_features": len(features),
        "coverage": coverage,
        "capture_meta": dict(capture_meta or {}),
        "features": features,
    }


def build_feature_table(capture_dir: Any, *, cutoff: str = T0_DEFAULT_CUTOFF) -> Dict[str, Any]:
    """Disk entry: read captures/<date>/<dataset_id>/*.json (time-isolated) and
    build the T0 feature table."""
    capture_dir = Path(capture_dir)
    cutoff_secs = _cutoff_seconds(cutoff)
    datasets: Dict[str, List[Any]] = {}
    capture_meta: Dict[str, Any] = {}
    for dsid in AUCTION_DATASETS:
        dsdir = capture_dir / dsid
        if not dsdir.is_dir():
            capture_meta[dsid] = {"present": False}
            continue
        picked, meta = _pick_capture_file(dsdir, cutoff_secs)
        capture_meta[dsid] = {"present": True, **meta}
        if picked is not None:
            datasets[dsid] = _rows_of(picked[1])
    return build_from_datasets(datasets, date=capture_dir.name, cutoff=cutoff,
                               capture_meta=capture_meta)


# --------------------------------------------------------------------------- #
# Self-test -- real job-0089 sample rows (shared with duanxianxia_canonical)
# --------------------------------------------------------------------------- #
def _self_test() -> bool:
    v = ["002407", "多氟多", 462, 32740, "none", "10.0",
         "1779", "氢氟酸", "10.0", "1779", "15", 6.1, 0.52]
    w = ["002407", "多氟多", 45.66, 10, 2339609266, "none",
         144416464, 0.56, 258717139, 1016893860, 258717139,
         "氢氟酸、电解液",
         46177984662, 144416464, 203217386, -58800922, "首板", 208089]
    n = ["002407", "多氟多", 10, 10, 14442, 25872, 461.8,
         "氢氟酸|电解液", 0.56]
    q = ["300279", "和晶科技", 22, None, "none", "1.01",
         "189", "机器人", "1.01", "189", None, "11.93", 0.09]
    datasets = {
        "auction.jjyd.vratio": [{"code": "002407", "raw": v}],
        "auction.jjyd.weimai": [{"code": "002407", "raw": w}],
        "auction.jjyd.net_amount": [{"code": "002407", "raw": n}],
        "auction.jjyd.qiangchou": [{"code": "300279", "raw": q}],
    }
    res = build_from_datasets(datasets, date="2026-06-29")
    feats = {f["code"]: f for f in res["features"]}
    assert "002407" in feats and "300279" in feats, list(feats)

    a = feats["002407"]
    # FF preferred from weimai (exact 元), not the 亿-rounded sources
    assert a["free_float_mktcap"] == 46177984662, a["free_float_mktcap"]
    assert a["_field_sources"]["free_float_mktcap"] == "weimai"
    # bidAmount from vratio raw[6]=1779万 -> 1.779e7 元
    assert a["bidAmount"] == 17_790_000, a["bidAmount"]
    # _field_sources uses canonical field name "auction_turnover" (not output key "bidAmount")
    assert a["_field_sources"]["auction_turnover"] == "vratio"
    # bidStrength = bidAmount / FF * 1e4
    assert abs(a["bidStrength"] - 17_790_000 / 46177984662 * 1e4) < 1e-9, a["bidStrength"]
    # volumeRatio only from vratio raw[11]
    assert a["volumeRatio"] == 6.1, a["volumeRatio"]
    # mainNetInflow preferred from net_amount raw[4]=14442万 -> 1.4442e8 元
    assert a["mainNetInflow"] == 144_420_000, a["mainNetInflow"]
    assert a["_field_sources"]["main_net_inflow"] == "net_amount"
    # weimai-only primitives
    assert a["mainNetInflowFull"] == 144416464, a["mainNetInflowFull"]
    assert a["superLargeOrder"] == 203217386, a["superLargeOrder"]
    assert a["largeOrder"] == -58800922, a["largeOrder"]
    assert a["boardLabel"] == "首板", a["boardLabel"]
    assert a["price"] == 45.66, a["price"]
    # seal_amount preferred from weimai raw[17]=208089万 -> x1e4
    assert a["sealAmount"] == 208089 * 10000, a["sealAmount"]
    assert a["changeRate"] == 10.0, a["changeRate"]
    assert a["turnoverRate"] == 0.52, a["turnoverRate"]
    # 3 sources hit; qiangchou stock is a different code
    assert a["source_hit_count"] == 3, a["source_hits"]
    assert set(a["source_hits"]) == {"vratio", "weimai", "net_amount"}, a["source_hits"]
    # mislabel guard: the legacy named field must never leak through
    assert "auction_volume_ratio" not in a

    b = feats["300279"]
    assert b["grabStrength"] == 11.93, b["grabStrength"]
    assert b["volumeRatio"] is None, b["volumeRatio"]
    assert b["source_hit_count"] == 1, b["source_hits"]

    # coverage: a positional row missing raw[] is counted, not crashed/dropped silently
    res2 = build_from_datasets({"auction.jjyd.vratio": [{"code": "x"}]})
    assert res2["coverage"]["auction.jjyd.vratio"]["canonical_error"] == 1, res2["coverage"]

    # time-isolation: confident post-cutoff timestamp excluded; pre-cutoff kept
    cutoff_secs = _cutoff_seconds(T0_DEFAULT_CUTOFF)
    t_post, _src_post = _file_time_seconds("snap_100400.json", {"fetched_at": "10:04:00"})
    assert t_post is not None and t_post > cutoff_secs
    t_pre, _src_pre = _file_time_seconds("snap_092500.json", {"fetched_at": "09:25:00"})
    assert t_pre is not None and t_pre <= cutoff_secs
    return True


_self_test()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Build the v11 T0 canonical feature table from a captures/<date> dir")
    ap.add_argument("capture_dir", help="path to captures/<YYYY-MM-DD>")
    ap.add_argument("--cutoff", default=T0_DEFAULT_CUTOFF,
                    help="T0 time-isolation cutoff HH:MM (default 09:29)")
    ap.add_argument("--out", help="write feature table JSON here (default: stdout)")
    args = ap.parse_args(argv)
    res = build_feature_table(args.capture_dir, cutoff=args.cutoff)
    payload = json.dumps(res, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
