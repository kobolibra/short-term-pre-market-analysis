#!/usr/bin/env python3
"""
v7.3 cron entry. Preserve the original batch-main production architecture for
premarket: fetch + analysis + webhook/bitable still happen inside
`duanxianxia_batch.py`, while we monkey-patch its legacy premarket analyzer to
v7.3.

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

# Stage 3: pull v7.3-for-batch adapter
from duanxianxia_premarket_v7_3_runner import build_premarket_analysis_v7_3  # noqa: E402

# Stage 4: monkey-patch
duanxianxia_batch.build_premarket_analysis = build_premarket_analysis_v7_3


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
    """Re-execute batch.py under __name__='__main__' with v7.3 still wired."""
    src_path = SCRIPTS_DIR / "duanxianxia_batch.py"
    src = src_path.read_text(encoding="utf-8")
    needle = 'if __name__ == "__main__":'
    shim = (
        "# v7.3-runner late-binding shim (inserted by duanxianxia_premarket_v7_runner.py)\n"
        "build_premarket_analysis = build_premarket_analysis_v7_3\n"
    )
    if needle in src:
        src = src.replace(needle, shim + needle, 1)
    ns = {
        "__name__": "__main__",
        "__file__": str(src_path),
        "build_premarket_analysis_v7_3": build_premarket_analysis_v7_3,
    }
    code = compile(src, str(src_path), "exec")
    exec(code, ns)  # noqa: S102 — trusted local source


if __name__ == "__main__":
    if not _try_clean_main():
        _fallback_reexec()
