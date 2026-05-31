"""Deprecated compatibility module.

The previous implementation monkey-patched v7.3 output at import time, which made
runtime behavior hard to reason about.  It has been replaced by the explicit
`duanxianxia_v8_premarket_engine` and the compatibility runner
`duanxianxia_premarket_v7_3_runner.py` now calls that engine directly.

This module intentionally has no side effects.  It remains only so any stale
imports fail softly during deployment transition.
"""

from __future__ import annotations

VERSION = "deprecated_use_duanxianxia_v8_premarket_engine"


def apply() -> None:
    """No-op compatibility shim."""
    return None
