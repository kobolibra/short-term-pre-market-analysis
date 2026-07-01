#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Task 0099 — 将 S5_amt_liq_core 权重持久化到 git main (source of truth).

背景: 0098 已把 S5 权重写入服务器工作副本的 duanxianxia_v9_edge.py /
v10_optimize.py 并自检通过 (production 已用 S5)。但 git main 仍是 baseline
权重, 下次 `git pull` / 重新部署会把 S5 覆盖回 baseline, 且 git 作为
source-of-truth 与运行时漂移。

本 job 幂等地重新套用 S5 到这两个文件, 自验证 (regex 断言 + import +
样本打分 + V10AMT_W 校验) 通过后, git add/commit/push 到 main, 让 git
成为持久事实源。无论服务器是否在每次运行前 reset 工作副本, 都能保证
git main 承载 S5。

S5 (Task 0093 walk-forward 推荐 -> 0098 上线; OOS IC 0.129 vs baseline
0.1278, beats 8/12 days):
  amt_pct 0.3232 / auction_strength 0.0909 / liquidity 0.2424 / money 0.1616 /
  pressure_score 0.1414 / weimai_strength 0.0303 / orderbook 0.0202
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
V9 = SCRIPTS_DIR / "duanxianxia_v9_edge.py"
V10 = SCRIPTS_DIR / "v10_optimize.py"

S5 = {
    "amt_pct": 0.3232,
    "auction_strength": 0.0909,
    "liquidity": 0.2424,
    "money": 0.1616,
    "pressure_score": 0.1414,
    "weimai_strength": 0.0303,
    "orderbook": 0.0202,
}
V9_KEYS = {
    "edge_w_amt": "amt_pct",
    "edge_w_auction": "auction_strength",
    "edge_w_liquidity": "liquidity",
    "edge_w_money": "money",
    "edge_w_pressure": "pressure_score",
    "edge_w_weimai": "weimai_strength",
    "edge_w_orderbook": "orderbook",
}

report = {
    "probe": "0099_s5_persist_git",
    "generated_at": datetime.now().isoformat(timespec="seconds"),
    "s5_weights": S5,
    "v9_edge": {"file": str(V9), "changes": []},
    "v10_optimize": {"file": str(V10)},
    "verify": {},
    "git": {},
}


def _run(cmd, cwd):
    p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    return {
        "cmd": " ".join(cmd),
        "rc": p.returncode,
        "stdout": p.stdout.strip()[-3000:],
        "stderr": p.stderr.strip()[-3000:],
    }


def _fail(msg):
    report["ok"] = False
    report["error"] = msg
    print(json.dumps(report, ensure_ascii=False, indent=2))
    sys.exit(2)


# --- 1) patch v9_edge.py edge_core defaults ---
t9 = V9.read_text(encoding="utf-8")
for key, fac in V9_KEYS.items():
    newv = repr(S5[fac])
    pat = re.compile(r'(p\.get\(\s*"' + re.escape(key) + r'"\s*,\s*)([0-9]*\.?[0-9]+)(\s*\))')
    m = pat.search(t9)
    if not m:
        _fail("v9_edge: default pattern for " + key + " not found")
    old = m.group(2)
    t9, n = pat.subn(lambda mm: mm.group(1) + newv + mm.group(3), t9, count=1)
    report["v9_edge"]["changes"].append(
        {"key": key, "old": old, "new": S5[fac], "occurrences": n}
    )
V9.write_text(t9, encoding="utf-8")

# --- 2) patch v10_optimize.py V10AMT_W ---
t10 = V10.read_text(encoding="utf-8")
s5_dict = "{" + ", ".join('"' + k + '": ' + str(v) for k, v in S5.items()) + "}"
pat10 = re.compile(r'(V10AMT_W\s*=\s*)\{[^}]*\}')
if not pat10.search(t10):
    _fail("v10_optimize: V10AMT_W assignment not found")
t10, n10 = pat10.subn(lambda mm: mm.group(1) + s5_dict, t10, count=1)
report["v10_optimize"]["occurrences"] = n10
report["v10_optimize"]["new_V10AMT_W"] = s5_dict
V10.write_text(t10, encoding="utf-8")

# --- 3) verify: re-read v9 defaults ---
t9v = V9.read_text(encoding="utf-8")
bad = []
for key, fac in V9_KEYS.items():
    m = re.search(r'p\.get\(\s*"' + re.escape(key) + r'"\s*,\s*([0-9]*\.?[0-9]+)\s*\)', t9v)
    got = float(m.group(1)) if m else None
    if got != S5[fac]:
        bad.append({"key": key, "want": S5[fac], "got": got})
report["verify"]["v9_defaults_ok"] = not bad
if bad:
    report["verify"]["v9_bad"] = bad
    _fail("v9_edge verify failed")

# import v9_edge fresh, run compute_edge_v9 on a sample
spec = importlib.util.spec_from_file_location("_v9edge_0099", str(V9))
mod = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(mod)
except Exception as e:
    _fail("v9_edge import failed: " + type(e).__name__ + ": " + str(e))
sample = {
    "code": "600000", "auction_strength": 78, "theme_strength_t0": 70,
    "auction_detail": {"latest_change_pct": 3.0, "money_intent_score": 70,
                       "net_pressure": 0.0015, "orderbook_quality_score": 60,
                       "liquidity_score": 70, "source_evidence_score": 25,
                       "auction_amount_pct": 80},
    "theme_detail": {"theme_strength_t0": 70},
    "weimai_detail": {"weimai_strength": 65},
    "context_detail": {"cashflow_continuity": "strong", "market_longtou_height": 6},
}
out = mod.compute_edge_v9(sample, {"market_env_score": 68, "risk_flags": []}, {})
report["verify"]["v9_sample_edge_score"] = out.get("edge_score")
report["verify"]["v9_alpha_type"] = out.get("alpha_type")
if not isinstance(out.get("edge_score"), (int, float)):
    _fail("v9_edge sample score invalid")

# import v10_optimize, check V10AMT_W
spec10 = importlib.util.spec_from_file_location("_v10opt_0099", str(V10))
mod10 = importlib.util.module_from_spec(spec10)
try:
    spec10.loader.exec_module(mod10)
except Exception as e:
    _fail("v10_optimize import failed: " + type(e).__name__ + ": " + str(e))
report["verify"]["v10_V10AMT_W"] = mod10.V10AMT_W
report["verify"]["v10_ok"] = (mod10.V10AMT_W == S5)
if not report["verify"]["v10_ok"]:
    _fail("v10_optimize V10AMT_W verify failed")

# --- 4) git commit + push to main ---
top = _run(["git", "rev-parse", "--show-toplevel"], SCRIPTS_DIR)
report["git"]["toplevel"] = top
if top["rc"] != 0 or not top["stdout"]:
    _fail("git toplevel detection failed")
root = top["stdout"].splitlines()[0].strip()
rel9 = os.path.relpath(str(V9), root)
rel10 = os.path.relpath(str(V10), root)

report["git"]["status_before"] = _run(["git", "status", "--porcelain", rel9, rel10], root)
report["git"]["add"] = _run(["git", "add", rel9, rel10], root)
if report["git"]["add"]["rc"] != 0:
    _fail("git add failed")
commit = _run([
    "git", "-c", "user.name=agent-job-0099",
    "-c", "user.email=agent-job@duanxianxia.local",
    "commit", "-m",
    "0099: persist S5_amt_liq_core edge weights to main (0093 refit -> 0098 apply)",
], root)
report["git"]["commit"] = commit
nothing = "nothing to commit" in (commit["stdout"] + commit["stderr"]).lower()
report["git"]["nothing_to_commit"] = nothing
if commit["rc"] != 0 and not nothing:
    _fail("git commit failed")
report["git"]["head"] = _run(["git", "rev-parse", "HEAD"], root)
push = _run(["git", "push", "origin", "HEAD:main"], root)
report["git"]["push"] = push
if push["rc"] != 0:
    _fail("git push to main failed")

report["ok"] = True
report["source"] = "0093 rec S5_amt_liq_core; 0098 applied to working copy; 0099 persists to git main"
print(json.dumps(report, ensure_ascii=False, indent=2))
