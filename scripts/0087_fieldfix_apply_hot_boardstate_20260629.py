#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""0087_fieldfix_apply_hot_boardstate_20260629.py

Apply ONE evidence-based, non-destructive fix to scripts/duanxianxia_fetcher.py:
  fetch_hot() drops item[7] (board state / 连板状态, e.g. "4天2板") and stores no
  raw upstream item array, unlike fetch_surge() which already stores raw. This
  loses the board-state signal and prevents historical re-derivation.

Fix: in fetch_hot()'s row dict, append "板态" (item[7]) and "raw": item. Additive.

Safety:
  - Assertion-anchored: the exact anchor must appear EXACTLY ONCE, else abort (no write/push).
  - After patching, the file must still compile() as valid Python, else abort + revert.
  - Does NOT push to main. Builds a commit (parent=HEAD) via a temp git index and
    pushes it to FEATURE_BRANCH for PR review. Local refs/HEAD untouched; working
    tree reverted after push.
  - Emits the unified diff + status to the results dir (published to agent-results)
    and to stdout.
"""
from __future__ import annotations
import os
import sys
import json
import tempfile
import subprocess
import difflib
import datetime
import py_compile
from pathlib import Path

WS = Path("/home/investmentofficehku/.openclaw/workspace")
if not WS.exists():
    WS = Path(os.getcwd())
SCRIPT_DIR = Path(__file__).resolve().parent
FETCHER = SCRIPT_DIR / "duanxianxia_fetcher.py"
REPORT_DIR = WS / "projects" / "duanxianxia" / "reports" / "_audit" / "agent_jobs"
REPORT_PATH = REPORT_DIR / "0087_fieldfix_apply_hot_boardstate.report.json"
FEATURE_BRANCH = "fix/hot-board-state-raw"
COMMIT_MSG = "fix(fetcher): fetch_hot capture board-state item[7] + raw array (additive, non-destructive)"

IND = " " * 20
CLOSE = " " * 16
OLD = (
    IND + '"概念": "+".join([x for x in [concept_1, concept_2] if x]),\n'
    + CLOSE + '}'
)
NEW = (
    IND + '"概念": "+".join([x for x in [concept_1, concept_2] if x]),\n'
    + IND + '"板态": item[7] if len(item) > 7 and item[7] is not None else "",\n'
    + IND + '"raw": item,\n'
    + CLOSE + '}'
)

report = {
    "job": "0087_fieldfix_apply_hot_boardstate",
    "generated_at": datetime.datetime.now().isoformat(),
    "feature_branch": FEATURE_BRANCH,
    "steps": [],
    "ok": False,
}


def log(**kw):
    report["steps"].append(kw)


def _flush():
    try:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def fail(stage, err):
    report["error"] = {"stage": stage, "msg": str(err)}
    _flush()
    print(json.dumps({"ok": False, "stage": stage, "error": str(err), "report_file": str(REPORT_PATH)}, ensure_ascii=False))
    raise SystemExit(1)


def git(args, env=None, check=True):
    r = subprocess.run(["git", *args], cwd=str(WS), capture_output=True, text=True, env=env)
    if check and r.returncode != 0:
        raise RuntimeError("git %s failed rc=%s: %s" % (" ".join(args), r.returncode, (r.stderr or r.stdout).strip()))
    return r


# 1) read + assert anchor count == 1
try:
    original = FETCHER.read_text(encoding="utf-8")
except Exception as e:  # noqa: BLE001
    fail("read", e)

cnt = original.count(OLD)
log(stage="anchor_count", count=cnt)
if cnt != 1:
    fail("anchor_count", "expected exactly 1 occurrence of anchor, found %d" % cnt)

# 2) apply in memory
patched = original.replace(OLD, NEW, 1)
if patched == original or NEW not in patched:
    fail("apply", "replacement produced no change")
report["bytes_before"] = len(original)
report["bytes_after"] = len(patched)

# 3) write to disk + compile check (revert on failure)
try:
    FETCHER.write_text(patched, encoding="utf-8")
except Exception as e:  # noqa: BLE001
    fail("write", e)
try:
    py_compile.compile(str(FETCHER), doraise=True)
    log(stage="py_compile", ok=True)
except Exception as e:  # noqa: BLE001
    try:
        FETCHER.write_text(original, encoding="utf-8")
    except Exception:
        pass
    fail("py_compile", e)

# 4) unified diff for review
diff = "".join(difflib.unified_diff(
    original.splitlines(keepends=True), patched.splitlines(keepends=True),
    fromfile="a/scripts/duanxianxia_fetcher.py", tofile="b/scripts/duanxianxia_fetcher.py"))
report["diff"] = diff

# 5) commit via temp index (parent=HEAD) + push to FEATURE_BRANCH (NOT main)
try:
    head = git(["rev-parse", "HEAD"]).stdout.strip()
    report["parent_head"] = head
    tmpidx = tempfile.mktemp(prefix="idx0087_")
    env = dict(os.environ)
    env["GIT_INDEX_FILE"] = tmpidx
    git(["read-tree", head], env=env)
    git(["add", "--", "scripts/duanxianxia_fetcher.py"], env=env)
    tree = git(["write-tree"], env=env).stdout.strip()
    report["tree"] = tree
    env2 = dict(os.environ)
    env2.setdefault("GIT_AUTHOR_NAME", "agent-job")
    env2.setdefault("GIT_AUTHOR_EMAIL", "agent-job@local")
    env2.setdefault("GIT_COMMITTER_NAME", "agent-job")
    env2.setdefault("GIT_COMMITTER_EMAIL", "agent-job@local")
    ct = subprocess.run(["git", "commit-tree", tree, "-p", head, "-m", COMMIT_MSG], cwd=str(WS), capture_output=True, text=True, env=env2)
    if ct.returncode != 0:
        raise RuntimeError("commit-tree failed: " + (ct.stderr or ct.stdout).strip())
    commit = ct.stdout.strip()
    report["commit"] = commit
    push = subprocess.run(["git", "push", "origin", "%s:refs/heads/%s" % (commit, FEATURE_BRANCH)], cwd=str(WS), capture_output=True, text=True)
    report["push_rc"] = push.returncode
    report["push_out"] = (push.stdout or "")[-1000:]
    report["push_err"] = (push.stderr or "")[-1000:]
    if push.returncode != 0:
        raise RuntimeError("push failed: " + (push.stderr or push.stdout).strip())
    try:
        os.remove(tmpidx)
    except Exception:
        pass
except Exception as e:  # noqa: BLE001
    try:
        FETCHER.write_text(original, encoding="utf-8")
    except Exception:
        pass
    fail("git_push", e)

# 6) revert working tree (commit already pushed); keep main working tree clean
try:
    FETCHER.write_text(original, encoding="utf-8")
    log(stage="revert_worktree", ok=True)
except Exception as e:  # noqa: BLE001
    log(stage="revert_worktree", ok=False, err=str(e))

report["ok"] = True
_flush()
print(json.dumps({
    "ok": True,
    "feature_branch": FEATURE_BRANCH,
    "commit": report.get("commit"),
    "parent_head": report.get("parent_head"),
    "anchor_count": cnt,
    "bytes_before": report["bytes_before"],
    "bytes_after": report["bytes_after"],
    "diff": diff,
    "report_file": str(REPORT_PATH),
}, ensure_ascii=False, indent=2))
