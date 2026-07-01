#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0103 READ-ONLY recon: dump duanxianxia_batch.py premarket argparse switches,
9:25 time-gate logic, and webhook/push call sites, so we can safely run a
premarket backfill NOW (bypassing the 9:25 gate) with or without live push.

Pure read + grep. No captures, no pushes, no writes to tracked files.
Critical categories (TIME_GATE, ARGPARSE) are printed LAST so they survive the
worker's last-16000-chars stdout tail.
"""
import re
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
SCRIPTS = WS / "scripts"


def dump(title, text, pattern, cap=45):
    rx = re.compile(pattern, re.I)
    lines = text.splitlines()
    print(f"\n===== {title} ({len(lines)} lines total) =====")
    n = 0
    for i, ln in enumerate(lines, 1):
        if rx.search(ln):
            print(f"{i}: {ln.strip()[:200]}")
            n += 1
            if n >= cap:
                print("... (capped)")
                break


def main() -> int:
    bp = SCRIPTS / "duanxianxia_batch.py"
    src = bp.read_text(encoding="utf-8", errors="replace")
    print(f"batch.py path={bp} chars={len(src)} lines={len(src.splitlines())}")

    # Printed first -> may be truncated away; least critical.
    dump("PREMARKET_DEFS", src, r"def [a-z_]*premarket|premarket[a-z_]*\(|build_premarket")
    dump("PUSH_SITES", src, r"webhook|requests\.post|bitable|feishu|lark|notify|open\.feishu|def .*push|send_")
    dump("MAIN", src, r"^def main|def main\(|def run\(|sys\.argv|dispatch|group ==|args\.group")
    # Printed last -> survives stdout tail; most critical for the backfill.
    dump("TIME_GATE", src, r"9:25|09:25|'925'|\"925\"|cutoff|window|time.?gate|--force|force|--target|target.?time|datetime\.now|now\(|strftime|is_trading|trading.?day|skip|dry.?run|allow|snapshot.?time")
    dump("ARGPARSE", src, r"add_argument|ArgumentParser|add_parser|set_defaults|dest=")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
