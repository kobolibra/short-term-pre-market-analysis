#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0079_rename_blast_radius_audit_20260629.py

Blast-radius audit for the field-rename refactor. Greps the whole repo
(code/config/docs) for every identifier involved in the rename, and probes for
any transformed/persisted 'table' layer beyond the raw captures. Read-only:
writes a full report to reports/_audit/0079_blast_radius.json and prints a
bounded summary (critical fields last so they survive the stdout tail).
"""
from __future__ import annotations
import json
import os
import re
from pathlib import Path

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
OUT_DIR = WORKSPACE / "projects" / "duanxianxia" / "reports" / "_audit"

EXCLUDE_PARTS = (
    "/.git/", "/captures/", "/node_modules/", "/.venv/", "/__pycache__/",
    "/reports/_audit/agent_jobs/",
)
CODE_EXT = {".py", ".json", ".yaml", ".yml"}
DOC_EXT = {".md"}
ALL_EXT = CODE_EXT | DOC_EXT

ASCII_TERMS = [
    "auction_volume_ratio", "float_market_cap", "free_float",
    "market_cap_yi", "market_cap", "hot_stock_day", "hot_stock_hour",
    "skyrocket_hour", "volume_ratio_multiple", "grab_strength",
    "turnover_rate", "seal_amount_wan",
]
CN_TERMS = ["\u6d41\u901a", "\u81ea\u7531\u6d41\u901a", "\u677f\u6001"]

PERSIST_RE = re.compile(
    r"(to_parquet|to_csv|to_feather|to_pickle|read_parquet|read_csv|"
    r"read_feather|sqlite3|duckdb|create\s+table|insert\s+into|\.db['\"]|"
    r"\.sqlite|\.parquet|normaliz|build_table|to_frame|DataFrame\()",
    re.IGNORECASE,
)
DATA_SUFFIX = {".db", ".sqlite", ".sqlite3", ".parquet", ".feather",
               ".csv", ".pkl", ".pickle", ".h5", ".duckdb"}


def excluded(p: str) -> bool:
    return any(x in p for x in EXCLUDE_PARTS)


def rel(p: Path) -> str:
    try:
        return str(p.relative_to(WORKSPACE))
    except Exception:
        return str(p)


def main() -> int:
    term_hits = {t: [] for t in ASCII_TERMS + CN_TERMS}
    persist_hits = []
    data_artifacts = []

    for root, dirs, files in os.walk(WORKSPACE):
        dirs[:] = [d for d in dirs if not excluded(root + "/" + d + "/")]
        for fn in files:
            fp = Path(root) / fn
            sp = str(fp)
            if excluded(sp):
                continue
            suf = fp.suffix.lower()
            rp = rel(fp)
            if suf in DATA_SUFFIX:
                try:
                    data_artifacts.append({"path": rp, "size": fp.stat().st_size})
                except Exception:
                    data_artifacts.append({"path": rp, "size": None})
                continue
            if suf not in ALL_EXT:
                continue
            try:
                text = fp.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            for i, line in enumerate(text.splitlines(), 1):
                for t in ASCII_TERMS:
                    if t in line:
                        term_hits[t].append(f"{rp}:{i}: {line.strip()[:200]}")
                if suf in CODE_EXT:
                    for t in CN_TERMS:
                        if t in line:
                            term_hits[t].append(f"{rp}:{i}: {line.strip()[:200]}")
                if suf == ".py" and PERSIST_RE.search(line):
                    persist_hits.append(f"{rp}:{i}: {line.strip()[:200]}")

    proj = WORKSPACE / "projects" / "duanxianxia"
    proj_dirs = []
    if proj.exists():
        for d in sorted(proj.iterdir()):
            if d.is_dir():
                proj_dirs.append(d.name)

    full = {
        "term_counts": {t: len(v) for t, v in term_hits.items()},
        "term_hits": term_hits,
        "persist_hits": persist_hits,
        "data_artifacts": data_artifacts,
        "project_dirs": proj_dirs,
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "0079_blast_radius.json").write_text(
        json.dumps(full, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "term_hits_capped": {t: v[:25] for t, v in term_hits.items()},
        "persist_hits": persist_hits[:60],
        "data_artifacts": data_artifacts[:60],
        "project_dirs": proj_dirs,
        "term_counts": full["term_counts"],
        "report_file": "projects/duanxianxia/reports/_audit/0079_blast_radius.json",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
