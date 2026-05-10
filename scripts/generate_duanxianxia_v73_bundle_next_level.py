#!/usr/bin/env python3
"""Next-level wrapper for v7.3 bundle generation.

Use this wrapper instead of generate_duanxianxia_v73_bundle.py when you want the
next-level recall overlay (BROAD_REPAIR_MOMENTUM / HIGH_COST_REPAIR_WATCH) to be
applied before review recomputation.
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import duanxianxia_v7_3_next_level_patch  # noqa: F401 - applies patch

if __name__ == "__main__":
    runpy.run_path(str(SCRIPTS_DIR / "generate_duanxianxia_v73_bundle.py"), run_name="__main__")
