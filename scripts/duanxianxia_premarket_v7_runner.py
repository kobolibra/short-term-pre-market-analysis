#!/usr/bin/env python3
"""
Premarket cron entry (production). Preserve the original batch-main production
architecture for premarket: fetch + analysis + webhook/bitable still happen
inside `duanxianxia_batch.py`, while we monkey-patch its legacy premarket
analyzer to the ACTIVE premarket decision engine.

Active engine: v9 full-data decision engine (build_premarket_analysis_v9).
ROLLBACK: set ACTIVE_ENGINE = build_premarket_analysis_v7_3 below (one line),
then re-pull on the server.

The file name is kept ("..._v7_runner.py") because the crontab / cron_runner.sh
already point at it; switching engines here avoids any crontab change.

Usage (from cron):
    python3 scripts/duanxianxia_premarket_v7_runner.py premarket

The argv after the script name is forwarded verbatim to batch.py's main().
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# Stage 1+2: import batch (no main runs because __name__ != '__main__')
import duanxianxia_batch  # type: ignore  # noqa: E402

# Stage 2b: table-level fetch retry + Playwright timeout floor for the PREMARKET
# path. Root cause of missing auction.jjyd.* / auction.jjlive.* tables on some
# trading days: cron_runner.sh routes premarket through THIS runner, which --
# unlike the non-premarket branch that goes via duanxianxia_fetch_retry.py --
# applied NO retry at all. A single transient auction fetch blip at the 09:25
# cron (Playwright networkidle 60s timeout / requests SSL EOF) therefore
# dropped the whole auction group to 0 rows for the day with no second attempt,
# while the faster same-group tables (qxlive / rank) still succeeded. Reuse the
# exact, already-tested retry monkey-patch here so the fragile auction fetchers
# get 3x exponential-backoff retries + a 60s Playwright wait-timeout floor too.
# The patch is class-level on DuanxianxiaFetcher, so it applies to the shared
# class object that batch.py already imported. Never let hardening break capture.
try:
    import duanxianxia_fetch_retry as _fetch_retry  # noqa: E402

    _n_retry = _fetch_retry.install_retry()
    _n_tmo = _fetch_retry.install_pw_timeout_floor()
    sys.stderr.write(
        f"[premarket_v7_runner] fetch retry patched={_n_retry} "
        f"pw_timeout_floored={_n_tmo}\n"
    )
except Exception as _exc:  # noqa: BLE001 -- hardening must never break capture
    sys.stderr.write(f"[premarket_v7_runner] fetch retry install skipped: {_exc}\n")

# Stage 3: pull premarket-for-batch adapters (all engines importable).
from duanxianxia_premarket_v7_3_runner import build_premarket_analysis_v7_3  # noqa: E402
from duanxianxia_premarket_v9_runner import build_premarket_analysis_v9  # noqa: E402
from duanxianxia_v4_2_runner import build_premarket_analysis_v4_2  # noqa: E402
from duanxianxia_v5_0_runner import build_premarket_analysis_v5_0  # noqa: E402

# Active premarket decision engine. Switch to v4_2 / v7_3 / v9 to roll back.
ACTIVE_ENGINE = build_premarket_analysis_v5_0

# Stage 4: monkey-patch
duanxianxia_batch.build_premarket_analysis = ACTIVE_ENGINE


def _try_clean_main() -> bool:
    """Try to invoke duanxianxia_batch.main(argv) directly. Returns True if dispatched."""
    main_fn = getattr(duanxianxia_batch, "main", None)
    if not callable(main_fn):
        return False
    try:
        rc = main_fn(sys.argv[1:])
    except TypeError:
        # Some main() signatures take no args and read sys.argv directly.
        rc = main_fn()
    sys.exit(rc if isinstance(rc, int) else 0)


def _fallback_reexec() -> None:
    """Re-execute batch.py under __name__='__main__' with the active engine still wired."""
    src_path = SCRIPTS_DIR / "duanxianxia_batch.py"
    src = src_path.read_text(encoding="utf-8")
    needle = 'if __name__ == "__main__":'
    shim = (
        "# premarket-runner late-binding shim (inserted by duanxianxia_premarket_v7_runner.py)\n"
        "build_premarket_analysis = ACTIVE_ENGINE\n"
    )
    if needle in src:
        src = src.replace(needle, shim + needle, 1)
    ns = {
        "__name__": "__main__",
        "__file__": str(src_path),
        "ACTIVE_ENGINE": ACTIVE_ENGINE,
        "build_premarket_analysis_v7_3": build_premarket_analysis_v7_3,
        "build_premarket_analysis_v9": build_premarket_analysis_v9,
        "build_premarket_analysis_v4_2": build_premarket_analysis_v4_2,
        "build_premarket_analysis_v5_0": build_premarket_analysis_v5_0,
    }
    code = compile(src, str(src_path), "exec")
    exec(code, ns)  # noqa: S102 — trusted local source


if __name__ == "__main__":
    if not _try_clean_main():
        _fallback_reexec()
