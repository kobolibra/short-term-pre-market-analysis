#!/usr/bin/env python3
"""
0088 fieldfix: apply the SAFE, zero-rename canonical bug fixes to duanxianxia_fetcher.py.

Per projects/duanxianxia/docs/field-rename-map.md §10 (CODE BUGS) and
canonical-field-dictionary.md "Corrections summary" -- ONLY the corrections that
do NOT rename any consumed field (so no downstream consumer change is required):

  1. hotlist_day: reads nonexistent key "hot_stock_day" -> 0 rows.
     Fix: read "hot_stock_hour" (real key), keep old key as fallback.
  2. hotlist_day meta "field" label -> "hot_stock_hour" (honesty).
  3. surge turnover_ratio: was RECOMPUTED = item[8]/item[9]*100 (disagrees with
     site). Fix: use SITE item[10] directly.
  4. surge board_state: item[7] was dropped -> add "board_state".
  5. home_ztpool source_url: malformed literal " + " -> clean URL.

Field RENAMES (free_float_mktcap etc.) are intentionally NOT done here; they
require a coordinated fetcher+consumer+history-rederive change and ship separately.

Safety (mirrors 0087):
- Operates ONLY on scripts/duanxianxia_fetcher.py.
- Each edit asserted to match EXACTLY once; aborts (raises) on any mismatch,
  writing/pushing nothing.
- py_compile after edits; reverts working tree and aborts on failure.
- Does NOT push to main. Commits via a temp git index and pushes to
  refs/heads/fix/canonical-safe-bugs. Restores working tree afterwards.
- Writes a unified diff to the report and stdout for review.
"""
import os, subprocess, tempfile, difflib, json, py_compile, datetime

TARGET = "scripts/duanxianxia_fetcher.py"
REPORT_DIR = "projects/duanxianxia/reports/_audit/agent_jobs"
REPORT_PATH = os.path.join(REPORT_DIR, "0088_fieldfix_canonical_safe_bugs.report.json")
BRANCH = "fix/canonical-safe-bugs"
COMMIT_MSG = "fieldfix(safe): hotlist_hour key, surge site-turnover + board_state, ztpool source_url (no renames)"

EDITS = [
    (
        '        items = data.get("hot_stock_day", []) or []',
        '        items = data.get("hot_stock_hour", []) or data.get("hot_stock_day", []) or []',
    ),
    (
        '                "field": "hot_stock_day",',
        '                "field": "hot_stock_hour",',
    ),
    (
        '            turnover_ratio = ""\n'
        '            if len(item) > 9 and item[9]:\n'
        '                try:\n'
        '                    turnover_ratio = f"{(float(item[8]) / float(item[9]) * 100):.2f}%"\n'
        '                except Exception:\n'
        '                    turnover_ratio = ""',
        '            turnover_ratio = ""\n'
        '            if len(item) > 10 and item[10] is not None and str(item[10]).strip() != "":\n'
        '                turnover_ratio = f"{item[10]}%"',
    ),
    (
        '                    "float_market_cap": float_cap,\n'
        '                    "concept": "+".join([x for x in [concept_1, concept_2] if x]),',
        '                    "float_market_cap": float_cap,\n'
        '                    "board_state": item[7] if len(item) > 7 and item[7] is not None else "",\n'
        '                    "concept": "+".join([x for x in [concept_1, concept_2] if x]),',
    ),
    (
        "                'source': f'{page_url} + /vendor/stockdata/jinjidata.json',",
        "                'source': f'{page_url}/vendor/stockdata/jinjidata.json',",
    ),
]


def run(cmd, env=None, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if check and r.returncode != 0:
        raise RuntimeError("cmd failed rc=%d: %s\nstdout=%s\nstderr=%s" % (r.returncode, " ".join(cmd), r.stdout, r.stderr))
    return r


report = {"id": "0088", "started_at": datetime.datetime.now().isoformat(), "ok": False, "edits": []}

try:
    if not os.path.exists(TARGET):
        raise RuntimeError("target not found: %s" % TARGET)
    with open(TARGET, "r", encoding="utf-8") as f:
        original = f.read()

    patched = original
    for i, (old, new) in enumerate(EDITS, start=1):
        cnt = patched.count(old)
        report["edits"].append({"edit": i, "count": cnt})
        if cnt != 1:
            raise RuntimeError("edit %d expected exactly 1 occurrence, found %d" % (i, cnt))
        patched = patched.replace(old, new, 1)

    if patched == original:
        raise RuntimeError("no change produced")

    diff = "".join(difflib.unified_diff(
        original.splitlines(keepends=True),
        patched.splitlines(keepends=True),
        fromfile="a/" + TARGET, tofile="b/" + TARGET,
    ))
    report["diff"] = diff

    with open(TARGET, "w", encoding="utf-8") as f:
        f.write(patched)
    try:
        py_compile.compile(TARGET, doraise=True)
    except Exception as e:
        with open(TARGET, "w", encoding="utf-8") as f:
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
    run(["git", "add", "--", TARGET], env=env)
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
    print("=== 0088 REPORT ===")
    print(json.dumps({k: v for k, v in report.items() if k != "diff"}, ensure_ascii=False, indent=2))
    print("=== UNIFIED DIFF ===")
    print(report.get("diff", "(no diff)"))
