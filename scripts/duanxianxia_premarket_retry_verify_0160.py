#!/usr/bin/env python3
"""0160: verify the premarket runner now installs table-level fetch retry.

Importing duanxianxia_premarket_v7_runner runs its Stage 2b block, which calls
duanxianxia_fetch_retry.install_retry() + install_pw_timeout_floor(). We then
inspect DuanxianxiaFetcher to confirm the fetch_* methods (incl. the auction
fetchers) carry the _retry_wrapped marker. This is an import/wiring sanity check
-- it does NOT fetch auction data (the 09:15-09:25 window is a one-shot and is
long closed by job-run time); it only proves the hardening loads cleanly and
will be active at the next 09:25 premarket cron.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

out: dict = {"job": "0160_premarket_retry_verify"}

try:
    import duanxianxia_premarket_v7_runner as _r  # noqa: F401  (runs Stage 2b at import)
    out["v7_runner_import"] = "ok"
except Exception as exc:  # noqa: BLE001
    out["v7_runner_import"] = f"{type(exc).__name__}: {exc}"

try:
    import duanxianxia_fetcher as fx

    cls = fx.DuanxianxiaFetcher
    fetch_methods = [n for n in dir(cls) if n.startswith("fetch_")]
    wrapped = [
        n for n in fetch_methods
        if getattr(getattr(cls, n), "_retry_wrapped", False)
    ]
    auction_related = [
        n for n in fetch_methods
        if any(k in n.lower() for k in ("auction", "jjyd", "jjlive", "weimai", "jingjia"))
    ]
    out["fetch_methods_total"] = len(fetch_methods)
    out["retry_wrapped_total"] = len(wrapped)
    out["all_fetch_methods"] = sorted(fetch_methods)
    out["auction_related_methods"] = sorted(auction_related)
    out["all_wrapped"] = len(wrapped) == len(fetch_methods) and len(fetch_methods) > 0
except Exception as exc:  # noqa: BLE001
    out["fetcher_inspect"] = f"{type(exc).__name__}: {exc}"

print(json.dumps(out, ensure_ascii=False))
