#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""agent_job_worker.py — deterministic git-as-queue worker.

由 cron 经 scripts/agent_job_runner.sh 调用（先 git 同步 main，再跑本脚本）。
扫描 scripts/agent_jobs/queue/*.json，对每个尚未产出结果的 job 执行其
whitelisted 脚本，把结果写到 reports/_audit/agent_jobs/<id>.result.json。

设计要点：
- 幂等：结果文件已存在则跳过（同一份工作不重跑）。要重跑请用新的 id。
- 单向：worker 只读 queue、只写 reports/_audit（经 /pull 可读），不需 git 写权限。
- 安全：只允许 scripts/ 下的 .py 脚本，路径必须解析在 workspace 内。
"""
from __future__ import annotations
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace").resolve()
QUEUE_DIR = WORKSPACE / "scripts" / "agent_jobs" / "queue"
RESULT_DIR = WORKSPACE / "projects" / "duanxianxia" / "reports" / "_audit" / "agent_jobs"
TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_TIMEOUT = 1800
TAIL = 16000
MAX_TOTAL_RUNTIME = 300  # 5 分钟总运行时间上限，防止锁被长期占用


def _now() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


def _write(path: Path, rec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_jobs() -> list:
    if not QUEUE_DIR.is_dir():
        return []
    jobs = []
    for f in sorted(QUEUE_DIR.glob("*.json")):
        try:
            spec = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            jobs.append({"_bad": f.name, "_err": f"parse error: {e}"})
            continue
        spec["_file"] = f.name
        jobs.append(spec)
    # 优先级排序: feishu > rerun > premarket > backtest > daily > hourly > 其他
    def _priority(spec):
        jid = str(spec.get("id", "")).lower()
        if "feishu" in jid:
            return 0
        if "rerun" in jid:
            return 1
        if "premarket" in jid:
            return 2
        if "backtest" in jid:
            return 3
        if jid.startswith("daily_"):
            return 4
        if jid.startswith("hourly_"):
            return 5
        return 6
    jobs.sort(key=_priority)
    return jobs


def _validate(spec: dict):
    jid = str(spec.get("id") or "").strip()
    script = str(spec.get("script") or "").strip()
    if not jid:
        return None, "missing id"
    if not script.startswith("scripts/") or not script.endswith(".py"):
        return None, f"script not allowed (must be scripts/*.py): {script}"
    target = (WORKSPACE / script).resolve()
    if WORKSPACE not in target.parents or not target.exists():
        return None, f"script not found under workspace: {script}"
    return target, None


def _run_one(spec: dict) -> str:
    jid = str(spec.get("id") or "").strip()
    result_path = RESULT_DIR / f"{jid}.result.json"
    if result_path.exists():
        return "skipped_done"
    rec = {
        "id": jid,
        "script": spec.get("script"),
        "args": spec.get("args") or [],
        "queued_file": spec.get("_file"),
        "note": spec.get("note"),
        "worker_time": _now(),
    }
    target, err = _validate(spec)
    if err:
        rec.update({"ok": False, "rc": None, "error": err, "ended": _now()})
        _write(result_path, rec)
        return "error"
    args = [str(a) for a in (spec.get("args") or [])]
    timeout = int(spec.get("timeout") or DEFAULT_TIMEOUT)
    cmd = ["python3", str(target), *args]
    started = _now()
    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd, cwd=str(WORKSPACE), text=True, capture_output=True, timeout=timeout
        )
        rec.update({
            "ok": proc.returncode == 0,
            "rc": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-TAIL:],
            "stderr_tail": (proc.stderr or "")[-TAIL:],
        })
    except subprocess.TimeoutExpired as e:
        rec.update({
            "ok": False, "rc": None, "error": f"timeout after {timeout}s",
            "stdout_tail": (e.stdout or "")[-TAIL:] if isinstance(e.stdout, str) else "",
            "stderr_tail": (e.stderr or "")[-TAIL:] if isinstance(e.stderr, str) else "",
        })
    except Exception as e:
        rec.update({"ok": False, "rc": None, "error": f"{type(e).__name__}: {e}"})
    rec.update({
        "started": started, "ended": _now(),
        "duration_s": round(time.time() - t0, 1), "cmd": cmd,
    })
    _write(result_path, rec)
    return "ok" if rec.get("ok") else "failed"


def main() -> int:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    jobs = _load_jobs()
    summary = {"scanned": len(jobs), "ran": [], "skipped": [], "errors": []}
    start_time = time.time()
    for spec in jobs:
        if spec.get("_bad"):
            summary["errors"].append({spec["_bad"]: spec["_err"]})
            continue
        status = _run_one(spec)
        jid = spec.get("id")
        if status == "skipped_done":
            summary["skipped"].append(jid)
        elif status in ("ok", "failed"):
            summary["ran"].append({jid: status})
        else:
            summary["errors"].append({jid: status})
        # 检查总运行时间，防止超时卡住锁
        if time.time() - start_time > MAX_TOTAL_RUNTIME:
            break
    _write(RESULT_DIR / "_worker_heartbeat.json", {"last_run": _now(), **summary})
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
