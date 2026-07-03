#!/usr/bin/env python3
"""
Fetch-retry entry + one-off capture backfill for duanxianxia.

WHY: batch download receipts show per-table TRANSIENT network failures
(Playwright TimeoutError on cashflow 3/5/10day & home.ztpool; requests
SSLError on rank.rocket). The per-table fetchers in duanxianxia_fetcher.py
have little/no retry, so a single upstream blip drops a table to 0 rows and
marks it missing.

WHAT: monkey-patch DuanxianxiaFetcher.fetch_* to retry transient network
errors with exponential backoff, then either
  (a) forward argv to duanxianxia_batch.main()   [production capture entry], or
  (b) --backfill <kind...>  re-fetch specific datasets and persist  [one-off].

This mirrors the existing monkey-patch pattern in
duanxianxia_premarket_v7_runner.py (import batch, patch a symbol, call main),
and keeps the 142KB batch / 105KB fetcher untouched (too large to edit safely).

ROLLBACK: point duanxianxia_cron_runner.sh (non-premarket branch) and the
duanxianxia_postmarket_chain_runner.sh capture call back at
scripts/duanxianxia_batch.py directly. Nothing else imports this file.

Usage:
    # production (from runners):
    python3 scripts/duanxianxia_fetch_retry.py intraday_cashflow
    python3 scripts/duanxianxia_fetch_retry.py postmarket_cashflow --capture-only --webhook-url '' --json
    # one-off backfill:
    python3 scripts/duanxianxia_fetch_retry.py --backfill cashflow_3d cashflow_5d cashflow_10d home_ztpool rocket
"""
from __future__ import annotations

import functools
import json
import sys
import time
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import duanxianxia_fetcher as fx  # noqa: E402

MAX_ATTEMPTS = 3
BACKOFF_BASE = 4.0  # seconds; waits ~4s then ~12s between attempts


def _retryable_exceptions() -> tuple:
    excs: list = [TimeoutError, ConnectionError]
    try:
        from playwright.sync_api import TimeoutError as PWTimeout  # type: ignore

        excs.append(PWTimeout)
    except Exception:
        pass
    try:
        from requests.exceptions import RequestException  # type: ignore

        excs.append(RequestException)
    except Exception:
        pass
    return tuple(excs)


RETRYABLE = _retryable_exceptions()


def _with_retry(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return method(*args, **kwargs)
            except RETRYABLE as exc:
                last_exc = exc
                if attempt >= MAX_ATTEMPTS:
                    break
                sys.stderr.write(
                    f"[fetch_retry] {getattr(method, '__name__', 'fetch')} "
                    f"attempt {attempt}/{MAX_ATTEMPTS} failed: "
                    f"{type(exc).__name__}: {exc}; backing off\n"
                )
                time.sleep(BACKOFF_BASE * (3 ** (attempt - 1)))
        assert last_exc is not None
        raise last_exc

    return wrapper


def install_retry() -> int:
    """Patch every public fetch_* method on DuanxianxiaFetcher with retry.
    Never let a patching error break the capture run."""
    patched = 0
    try:
        cls = fx.DuanxianxiaFetcher
        for name in dir(cls):
            if not name.startswith("fetch_"):
                continue
            orig = getattr(cls, name)
            if callable(orig) and not getattr(orig, "_retry_wrapped", False):
                wrapped = _with_retry(orig)
                wrapped._retry_wrapped = True  # type: ignore[attr-defined]
                setattr(cls, name, wrapped)
                patched += 1
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[fetch_retry] retry patch skipped: {exc}\n")
    return patched


def _run_backfill(kinds: list) -> int:
    fetcher = fx.DuanxianxiaFetcher()
    out = {"task": "capture_retry_backfill", "filled": [], "failed": []}
    for kind in kinds:
        method = getattr(fetcher, f"fetch_{kind}", None)
        if not callable(method):
            out["failed"].append({"kind": kind, "error": "unknown dataset kind"})
            continue
        try:
            result = method()
            payload = fx.build_capture_payload(result)
            path = fx.persist_capture(payload)
            out["filled"].append(
                {
                    "kind": kind,
                    "dataset_id": payload["dataset_id"],
                    "rows": payload["row_count"],
                    "path": str(path),
                }
            )
        except Exception as exc:  # noqa: BLE001
            out["failed"].append({"kind": kind, "error": f"{type(exc).__name__}: {exc}"})
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if not out["failed"] else 1


def _forward_to_batch() -> int:
    import duanxianxia_batch  # noqa: E402 (imported after patch; class is shared)

    main_fn = getattr(duanxianxia_batch, "main", None)
    if callable(main_fn):
        try:
            rc = main_fn(sys.argv[1:])
        except TypeError:
            rc = main_fn()
        return rc if isinstance(rc, int) else 0
    # Fallback: execute batch under __main__ (patched fetcher persists via sys.modules cache)
    src_path = SCRIPTS_DIR / "duanxianxia_batch.py"
    ns = {"__name__": "__main__", "__file__": str(src_path)}
    exec(compile(src_path.read_text(encoding="utf-8"), str(src_path), "exec"), ns)  # noqa: S102
    return 0


def main() -> int:
    install_retry()
    argv = sys.argv[1:]
    if argv and argv[0] == "--backfill":
        kinds = argv[1:]
        if not kinds:
            sys.stderr.write("usage: --backfill <kind> [<kind> ...]\n")
            return 2
        return _run_backfill(kinds)
    return _forward_to_batch()


if __name__ == "__main__":
    raise SystemExit(main())
