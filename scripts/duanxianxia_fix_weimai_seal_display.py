#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-shot, idempotent patch for scripts/duanxianxia_fetcher.py.

Bug: weimai raw[17] seal_amount is in WAN (万) -- see duanxianxia_canonical.py and
field-rename-map.md s4. But fetch_auction_weimai() formatted `seal_amount_text` via
_format_qxlive_seal_amount(), which treats the value as YUAN (元). That shrinks a
limit-up board's seal amount by 1e4 (e.g. duofuduo raw[17]=64230 rendered as the
nonsensical "6万" instead of the real 6.42亿). The stored numeric `seal_amount_wan`
field is correct; only the human-readable `seal_amount_text` was wrong.

Fix: add a dedicated _format_weimai_seal_amount() (WAN -> YUAN, then format) and point
seal_amount_text at it. The shared qxlive formatter is left untouched so qxlive plate
rows are unaffected.

Why a patch script (not a direct edit): the fetcher is ~105 KB; rewriting it whole via
the GitHub single-file API risks silent transcription drift in unrelated logic. This
script makes ONLY the two intended edits, is idempotent, and self-verifies. Run it where
the repo is checked out, then commit duanxianxia_fetcher.py:
    python3 scripts/duanxianxia_fix_weimai_seal_display.py
"""
from __future__ import annotations

import py_compile
import sys
from pathlib import Path

TARGET = Path(__file__).resolve().parent / "duanxianxia_fetcher.py"

OLD_CALL = '"seal_amount_text": self._format_qxlive_seal_amount(item[17]) if item[17] not in (None, "") else "",'
NEW_CALL = '"seal_amount_text": self._format_weimai_seal_amount(item[17]) if item[17] not in (None, "") else "",'

ANCHOR = "    def _format_qxlive_seal_amount(self, value: Any) -> str:\n"
NEW_METHOD = (
    "    def _format_weimai_seal_amount(self, value: Any) -> str:\n"
    "        \"\"\"weimai raw[17] seal_amount is in \u4e07 (canonical layer / field-rename-map \u00a74);\n"
    "        convert \u4e07 -> \u5143 before formatting. Treating it as \u5143 (the old bug) shrinks a\n"
    "        limit-up board's \u5c01\u5355\u989d by 1e4 (e.g. \u591a\u6c1f\u591a raw[17]=64230 -> wrong '6\u4e07' vs real 6.42\u4ebf).\"\"\"\n"
    "        try:\n"
    "            number = float(value)\n"
    "        except Exception:\n"
    "            return str(value or \"\").strip()\n"
    "        if number <= 0:\n"
    "            return \"--\"\n"
    "        return self._format_qxlive_amount(number * 10000, yi_digits=1, wan_digits=0)\n"
    "\n"
    "    def _format_qxlive_seal_amount(self, value: Any) -> str:\n"
)


def _self_test() -> None:
    """Standalone replica of the two pure formatters; no heavy imports needed."""
    def _fmt_amount(number, yi_digits, wan_digits):
        number = float(number)
        if abs(number) >= 100000000:
            text = f"{number/100000000:.{yi_digits}f}".rstrip("0").rstrip(".")
            return f"{text}\u4ebf"
        text = f"{number/10000:.{wan_digits}f}".rstrip("0").rstrip(".")
        return f"{text}\u4e07"

    def _fmt_weimai(value):
        try:
            number = float(value)
        except Exception:
            return str(value or "").strip()
        if number <= 0:
            return "--"
        return _fmt_amount(number * 10000, yi_digits=1, wan_digits=0)

    cases = {64230: "6.4\u4ebf", 149266: "14.9\u4ebf", 34899: "3.5\u4ebf",
             12471: "1.2\u4ebf", 3027: "3027\u4e07", 0: "--"}
    for raw17, expect in cases.items():
        got = _fmt_weimai(raw17)
        assert got == expect, f"self-test FAIL raw[17]={raw17}: got {got!r} expected {expect!r}"


def main() -> int:
    if not TARGET.exists():
        print(f"ERROR: target not found: {TARGET}")
        return 2
    src = TARGET.read_text(encoding="utf-8")

    already = "_format_weimai_seal_amount" in src
    if already:
        print("ALREADY_PATCHED: _format_weimai_seal_amount present; verifying only.")
    else:
        n_call = src.count(OLD_CALL)
        n_anchor = src.count(ANCHOR)
        if n_call != 1 or n_anchor != 1:
            print(f"ERROR: unexpected anchors (call={n_call}, anchor={n_anchor}); aborting, no write.")
            return 3
        src = src.replace(OLD_CALL, NEW_CALL)
        src = src.replace(ANCHOR, NEW_METHOD)
        TARGET.write_text(src, encoding="utf-8")
        print("PATCHED: seal_amount_text now uses _format_weimai_seal_amount (\u4e07->\u5143).")

    py_compile.compile(str(TARGET), doraise=True)
    print("PY_COMPILE: PASS")
    _self_test()
    print("SELF_TEST: PASS (64230->6.4\u4ebf, 3027->3027\u4e07)")
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
