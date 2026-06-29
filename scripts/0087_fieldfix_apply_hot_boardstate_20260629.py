#!/usr/bin/env python3
"""
0087 fieldfix apply: add "板态" (board-state, item[7]) and "raw" to fetch_hot() row dict.

This is the ONLY confirmed field bug (verified by job 0086 + full fetcher read):
fetch_hot() drops item[7] (board-state / consecutive-limit-up label, e.g. "4天2板")
and stores no raw, while fetch_surge() already stores raw. This makes the hot-pool
board-state signal permanently unrecoverable.

This patch is purely ADDITIVE: it inserts two keys right after the existing
"概念" key in the hot row dict. No other behavior changes.

Safety:
- Auto-discovers the unique .py file containing the anchor; asserts exactly 1 file,
  exactly 1 occurrence. Aborts (raises) otherwise -- never writes/pushes on ambiguity.
- Validates with py_compile after edit; restores working tree and aborts on failure.
- Does NOT push to main. Creates a commit via a temporary git index (only the
  patched file differs from HEAD) and pushes it to refs/heads/fix/hot-board-state-raw.
- Restores the working tree afterwards (next cron reset --hard also cleans it).
- Writes a unified diff to the report and stdout for review.
"""
import os, sys, subprocess, tempfile, difflib, json, py_compile, datetime

REPORT_DIR = "projects/duanxianxia/reports/_audit/agent_jobs"
REPORT_PATH = os.path.join(REPORT_DIR, "0087_fieldfix_apply_hot_boardstate.report.json")
BRANCH = "fix/hot-board-state-raw"
COMMIT_MSG = "fieldfix(hot): preserve item[7] board-state and raw in fetch_hot() row dict"

IND = " " * 20
CLOSE = " " * 16
OLD = IND + '"概念": "+".join([x for x in [concept_1, concept_2] if x]),\n' + CLOSE + '}'
NEW = (
    IND + '"概念": "+".join([x for x in [concept_1, concept_2] if x]),\n'
    + IND + '"板态": item[7] if len(item) > 7 and item[7] is not None else "",\n'
    + IND + '"raw": item,\n'
    + CLOSE + '}'
)

def run(cmd, env=None, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if check and r.returncode != 0:
        raise RuntimeError("cmd failed rc=%d: %s\nstdout=%s\nstderr=%s" % (r.returncode, " ".join(cmd), r.stdout, r.stderr))
    return r

report = {"id": "0087", "started_at": datetime.datetime.now().isoformat(), "ok": False}

try:
    matches = []
    for root, dirs, files in os.walk("."):
        if ".git" in dirs:
            dirs.remove(".git")
        for fn in files:
            if not fn.endswith(".py"):
                continue
            p = os.path.join(root, fn)
            try:
                with open(p, "r", encoding="utf-8") as f:
                    txt = f.read()
            except Exception:
                continue
            if OLD in txt:
                matches.append((p, txt))
    report["matched_files"] = [m[0] for m in matches]
    if len(matches) != 1:
        raise RuntimeError("expected exactly 1 file containing anchor, found %d: %s" % (len(matches), [m[0] for m in matches]))
    path, original = matches[0]
    report["target"] = path
    cnt = original.count(OLD)
    report["anchor_count"] = cnt
    if cnt != 1:
        raise RuntimeError("expected exactly 1 anchor occurrence in %s, found %d" % (path, cnt))

    patched = original.replace(OLD, NEW, 1)
    if patched == original:
        raise RuntimeError("no change produced")

    diff = "".join(difflib.unified_diff(
        original.splitlines(keepends=True),
        patched.splitlines(keepends=True),
        fromfile="a/" + path, tofile="b/" + path,
    ))
    report["diff"] = diff

    with open(path, "w", encoding="utf-8") as f:
        f.write(patched)
    try:
        py_compile.compile(path, doraise=True)
    except Exception as e:
        with open(path, "w", encoding="utf-8") as f:
            f.write(original)
        raise RuntimeError("py_compile failed, reverted: %s" % e)

    env = dict(os.environ)
    tmp_index = tempfile.NamedTemporaryFile(delete=False, suffix=".gitindex")
    tmp_index.close()
    env["GIT_INDEX_FILE"] = tmp_index.name
    env.setdefault("GIT_AUTHOR_NAME", "duanxianxia-agent")
    env.setdefault("GIT_AUTHOR_EMAIL", "agent@duanxianxia.local")
    env.setdefault("GIT_COMMITTER_NAME", "duanxianxia-agent")
    env.setdefault("GIT_COMMITTER_EMAIL", "agent@duanxianxia.local")

    run(["git", "read-tree", "HEAD"], env=env)
    run(["git", "add", "--", path], env=env)
    tree = run(["git", "write-tree"], env=env).stdout.strip()
    report["tree"] = tree
    parent = run(["git", "rev-parse", "HEAD"], env=env).stdout.strip()
    report["parent"] = parent
    commit = run(["git", "commit-tree", tree, "-p", parent, "-m", COMMIT_MSG], env=env).stdout.strip()
    report["commit"] = commit

    push = run(["git", "push", "origin", "%s:refs/heads/%s" % (commit, BRANCH)], env=env, check=False)
    report["push_rc"] = push.returncode
    report["push_stdout"] = push.stdout
    report["push_stderr"] = push.stderr
    if push.returncode != 0:
        raise RuntimeError("push failed rc=%d stderr=%s" % (push.returncode, push.stderr))

    try:
        os.unlink(tmp_index.name)
    except Exception:
        pass

    report["ok"] = True
finally:
    try:
        run(["git", "checkout", "--", "."], check=False)
    except Exception:
        pass
    report["finished_at"] = datetime.datetime.now().isoformat()
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("=== 0087 REPORT ===")
    print(json.dumps({k: v for k, v in report.items() if k != "diff"}, ensure_ascii=False, indent=2))
    print("=== UNIFIED DIFF ===")
    print(report.get("diff", "(no diff)"))
