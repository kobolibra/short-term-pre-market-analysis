#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Duanxianxia auction premarket self-check + best-effort backfill + alert.

Runs shortly after the 09:25 premarket capture cron. Verifies today's auction
datasets exist with rows>0; if any are missing it runs fetch_retry --backfill
for the missing ones (auction summary tables are typically still queryable after
the 09:15-09:25 window), re-checks, writes a status report, and appends an alert
line if still missing.

FAIL-SAFE: this script is fully isolated from the capture pipeline and never
lets an error escape -- if anything goes wrong it just logs and exits 0, so it
can never affect the premarket capture itself.
"""
from __future__ import annotations

import glob
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

WS = Path("/home/investmentofficehku/.openclaw/workspace")

# auction dataset_id -> fetch_retry --backfill kind (method names confirmed by job 0160)
AUCTION_DATASETS = {
    "auction.jjyd.vratio": "auction_vratio",
    "auction.jjyd.qiangchou": "auction_qiangchou",
    "auction.jjyd.net_amount": "auction_net_amount",
    "auction.jjyd.weimai": "auction_weimai",
    "auction.jjlive.fengdan": "auction_fengdan",
}


def _log(msg: str) -> None:
    sys.stderr.write(f"[auction_selfcheck] {msg}\n")


def _project_root() -> Path:
    try:
        from v10_optimize import DEFAULT_PROJECT_ROOT  # type: ignore

        return Path(str(DEFAULT_PROJECT_ROOT))
    except Exception as exc:  # noqa: BLE001
        _log(f"DEFAULT_PROJECT_ROOT import failed: {exc}")
    for c in (WS / "projects" / "duanxianxia", WS):
        if (c / "captures").is_dir():
            return c
    return WS / "projects" / "duanxianxia"


def _rows_for(cap_root: Path, date: str, dataset_id: str):
    d = cap_root / date / dataset_id
    if not d.is_dir():
        return None  # absent
    files = sorted(glob.glob(str(d / "*.json")))
    if not files:
        return 0
    try:
        with open(files[-1], encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return len(data)
        if isinstance(data, dict):
            return len(data.get("rows", []) or [])
    except Exception:  # noqa: BLE001
        return -1  # read error
    return 0


def _check(cap_root: Path, date: str):
    status = {}
    missing = []
    for ds in AUCTION_DATASETS:
        n = _rows_for(cap_root, date, ds)
        status[ds] = n
        if not n or (isinstance(n, int) and n < 0):
            missing.append(ds)
    return status, missing


def _emit(root: Path, rec: dict) -> None:
    date = rec.get("date", "unknown")
    try:
        rep_dir = root / "reports" / str(date) / "premarket"
        rep_dir.mkdir(parents=True, exist_ok=True)
        (rep_dir / "auction_capture_selfcheck.json").write_text(
            json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as exc:  # noqa: BLE001
        _log(f"write report failed: {exc}")
    if not rec.get("ok", False):
        try:
            audit = root / "reports" / "_audit"
            audit.mkdir(parents=True, exist_ok=True)
            with open(audit / "auction_capture_alerts.log", "a", encoding="utf-8") as fh:
                fh.write(
                    f"{rec.get('checked_at')}\tdate={rec.get('date')}\t"
                    f"missing_final={rec.get('missing_final')}\n"
                )
        except Exception as exc:  # noqa: BLE001
            _log(f"append alert failed: {exc}")
    print(json.dumps(rec, ensure_ascii=False))


def main() -> int:
    now = datetime.now(timezone(timedelta(hours=8)))
    date = now.strftime("%Y-%m-%d")
    root = _project_root()
    cap_root = root / "captures" if (root / "captures").is_dir() else None
    rec: dict = {
        "job": "auction_selfcheck",
        "date": date,
        "checked_at": now.isoformat(timespec="seconds"),
        "project_root": str(root),
        "captures_root": str(cap_root) if cap_root else None,
    }
    if cap_root is None:
        rec["ok"] = False
        rec["error"] = "captures_root_not_found"
        rec["missing_final"] = list(AUCTION_DATASETS.keys())
        _emit(root, rec)
        return 0

    before, missing = _check(cap_root, date)
    rec["before"] = before
    rec["missing_initial"] = missing

    if missing:
        kinds = [AUCTION_DATASETS[m] for m in missing]
        _log(f"missing {missing}; backfilling kinds {kinds}")
        try:
            proc = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "duanxianxia_fetch_retry.py"), "--backfill", *kinds],
                cwd=str(WS),
                text=True,
                capture_output=True,
                timeout=600,
            )
            rec["backfill_rc"] = proc.returncode
            rec["backfill_stdout_tail"] = (proc.stdout or "")[-2000:]
            rec["backfill_stderr_tail"] = (proc.stderr or "")[-1000:]
        except Exception as exc:  # noqa: BLE001
            rec["backfill_error"] = f"{type(exc).__name__}: {exc}"
        after, still = _check(cap_root, date)
        rec["after"] = after
        rec["missing_final"] = still
    else:
        rec["missing_final"] = []

    rec["ok"] = len(rec.get("missing_final", [])) == 0
    _emit(root, rec)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        _log(f"fatal (ignored): {exc}")
        raise SystemExit(0)
