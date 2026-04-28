#!/usr/bin/env python3
"""
v7 cron entry. Replaces `python scripts/duanxianxia_batch.py premarket` so the
09:25 cron uses v7 setup-classifier instead of v5 inline scoring, WITHOUT
rewriting the 96KB duanxianxia_batch.py.

Strategy (in order):
  1. Add scripts/ to sys.path so module imports resolve correctly.
  2. Import duanxianxia_batch as a regular module — this runs all top-level
     defs but NOT the `if __name__ == \"__main__\":` block.
  3. Monkey-patch its `build_premarket_analysis` symbol to v7.
  4. Invoke main() if available (clean path).
  5. Fallback: re-exec the source file with `__name__=\"__main__\"` while
     pre-injecting a late-binding shim that overrides the inline def AFTER
     it runs but BEFORE main() executes.

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

# Stage 3: pull v7
from duanxianxia_premarket_v7 import build_premarket_analysis_v7  # noqa: E402

# Stage 4: monkey-patch
duanxianxia_batch.build_premarket_analysis = build_premarket_analysis_v7


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
    """Re-execute the batch.py source under __name__='__main__' with v7 still wired.

    Heuristic: insert one line right before `if __name__ == \"__main__\":` that
    rebinds `build_premarket_analysis` to the v7 implementation. Since the
    `def build_premarket_analysis(...)` earlier in the file would otherwise
    overwrite our pre-injected globals entry, this late-binding shim ensures
    v7 wins by the time main() runs.
    """
    src_path = SCRIPTS_DIR / "duanxianxia_batch.py"
    src = src_path.read_text(encoding="utf-8")
    needle = 'if __name__ == "__main__":'
    shim = (
        "# v7-runner late-binding shim (inserted by duanxianxia_premarket_v7_runner.py)\n"
        "build_premarket_analysis = build_premarket_analysis_v7\n"
    )
    if needle in src:
        src = src.replace(needle, shim + needle, 1)
    ns = {
        "__name__": "__main__",
        "__file__": str(src_path),
        "build_premarket_analysis_v7": build_premarket_analysis_v7,
    }
    code = compile(src, str(src_path), "exec")
    exec(code, ns)  # noqa: S102 — trusted local source


if __name__ == "__main__":
    if not _try_clean_main():
        _fallback_reexec()
