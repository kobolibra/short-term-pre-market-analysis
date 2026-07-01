#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_qxlive_loader.py  --  v11 M5 (go-live) QX-live 9:25 pin.

QX-live (home.qxlive.top_metrics) carries premarket top metrics, notably
metric_key='QX' (sentimentSignal) and metric_key='PB' (今日封板率 =
marketSealRate). The premarket cron captures it in the 09:25 window, but an
intraday re-fetch (~10:01, fetched_at ~10:04) also lands in the SAME
captures/<date>/<dataset_id>/ directory. A naive "latest file" picker would pick
the 10:04 snapshot and leak post-open (look-ahead) state into T0 premarket
features.

This loader pins QX-live consumption to the T0 auction window by REUSING the
exact time-isolation selection already proven in duanxianxia_feature_builder
(_pick_capture_file / _file_time_seconds via each capture's `fetched_at`): it
picks the latest capture at/under the cutoff (default 09:29) and refuses any
capture whose confident wall-clock timestamp is after the cutoff. We deliberately
reuse rather than reimplement, to keep a single source of truth for the T0 rule
(docs/rebuild-plan-v11.md KEEP: T0<=9:29) and avoid drift.

Public API:
    VERSION
    find_qxlive_dir(capture_date_dir) -> Path | None
    pick_qxlive_capture(capture_date_dir, *, cutoff='09:29') -> (payload, meta)
    metric_value(payload, key) / metric_time_point(payload, key)
    load_pinned_metrics(capture_date_dir, *, cutoff='09:29', keys=('QX','PB')) -> dict
    _self_test()

Importing this module runs _self_test(); any regression raises AssertionError and
blocks import (same guard pattern as duanxianxia_canonical / _feature_builder).
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

import duanxianxia_feature_builder as FB

VERSION = "qxlive_loader_v11.0"


def find_qxlive_dir(capture_date_dir: Any) -> Optional[Path]:
    """Locate the QX-live dataset dir under a captures/<date> dir."""
    base = Path(capture_date_dir)
    exact = base / "home.qxlive.top_metrics"
    if exact.is_dir():
        return exact
    if base.is_dir():
        for d in sorted(base.iterdir()):
            if d.is_dir() and any(h in d.name.lower() for h in ("qxlive", "top_metrics")):
                return d
    return None


def pick_qxlive_capture(capture_date_dir: Any, *,
                        cutoff: str = FB.T0_DEFAULT_CUTOFF) -> Tuple[Any, Dict[str, Any]]:
    """Return (payload, meta) for the T0-pinned QX-live capture, reusing the
    feature builder's time-isolation picker (fetched_at <= cutoff wins;
    post-cutoff snapshots excluded)."""
    dsdir = find_qxlive_dir(capture_date_dir)
    if dsdir is None:
        return None, {"present": False}
    cutoff_secs = FB._cutoff_seconds(cutoff)
    picked, meta = FB._pick_capture_file(dsdir, cutoff_secs)
    meta = {"present": True, "dir": dsdir.name, **meta}
    payload = picked[1] if picked is not None else None
    return payload, meta


def _rows(payload: Any) -> Sequence[Any]:
    return FB._rows_of(payload) if payload is not None else []


def metric_value(payload: Any, key: str):
    for r in _rows(payload):
        if isinstance(r, Mapping) and r.get("metric_key") == key:
            return r.get("value")
    return None


def metric_time_point(payload: Any, key: str):
    for r in _rows(payload):
        if isinstance(r, Mapping) and r.get("metric_key") == key:
            return r.get("time_point")
    return None


def load_pinned_metrics(capture_date_dir: Any, *,
                        cutoff: str = FB.T0_DEFAULT_CUTOFF,
                        keys: Sequence[str] = ("QX", "PB")) -> Dict[str, Any]:
    """Load QX-live metrics from the 9:25-pinned capture. Exposes the FINAL
    aliases marketSealRate (PB) and sentimentSignal (QX) per HANDOFF §5.5."""
    payload, meta = pick_qxlive_capture(capture_date_dir, cutoff=cutoff)
    metrics: Dict[str, Any] = {}
    tps: Dict[str, Any] = {}
    for k in keys:
        metrics[k] = metric_value(payload, k)
        tps[k] = metric_time_point(payload, k)
    return {
        "version": VERSION,
        "cutoff": cutoff,
        "capture_meta": meta,
        "metrics": metrics,
        "metric_time_points": tps,
        "marketSealRate": metrics.get("PB"),
        "sentimentSignal": metrics.get("QX"),
    }


def _self_test() -> bool:
    with tempfile.TemporaryDirectory() as td:
        # date A: both a 09:25 and a 10:04 capture exist -> must pin to 09:25
        date_a = Path(td) / "2026-06-30"
        qa = date_a / "home.qxlive.top_metrics"
        qa.mkdir(parents=True)
        early = {"fetched_at": "09:25:07",
                 "rows": [{"metric_key": "PB", "value": 63.0, "time_point": "09:25"},
                          {"metric_key": "QX", "value": 41.0, "time_point": "09:25"}]}
        late = {"fetched_at": "10:04:11",
                "rows": [{"metric_key": "PB", "value": 88.0, "time_point": "10:04"},
                         {"metric_key": "QX", "value": 70.0, "time_point": "10:04"}]}
        (qa / "snap_092507.json").write_text(json.dumps(early), encoding="utf-8")
        (qa / "snap_100411.json").write_text(json.dumps(late), encoding="utf-8")
        res = load_pinned_metrics(date_a, cutoff="09:29")
        assert res["marketSealRate"] == 63.0, res
        assert res["sentimentSignal"] == 41.0, res
        assert res["capture_meta"]["chosen"] == "snap_092507.json", res["capture_meta"]
        assert res["capture_meta"]["n_skipped_post_cutoff"] == 1, res["capture_meta"]
        assert res["capture_meta"]["fellback_all_post_cutoff"] is False, res["capture_meta"]

        # date B: ONLY a 10:04 capture exists -> loader falls back but flags it
        date_b = Path(td) / "2026-06-29"
        qb = date_b / "home.qxlive.top_metrics"
        qb.mkdir(parents=True)
        (qb / "snap_100411.json").write_text(json.dumps(late), encoding="utf-8")
        res2 = load_pinned_metrics(date_b, cutoff="09:29")
        assert res2["capture_meta"]["fellback_all_post_cutoff"] is True, res2["capture_meta"]

        # date C: no qxlive dir at all -> present False, no crash
        date_c = Path(td) / "2026-06-28"
        (date_c / "auction.jjyd.vratio").mkdir(parents=True)
        res3 = load_pinned_metrics(date_c)
        assert res3["capture_meta"].get("present") is False, res3["capture_meta"]
        assert res3["marketSealRate"] is None and res3["sentimentSignal"] is None, res3
    return True


_self_test()


if __name__ == "__main__":  # pragma: no cover
    print("duanxianxia_qxlive_loader self-test: PASS")
    print("VERSION:", VERSION)
