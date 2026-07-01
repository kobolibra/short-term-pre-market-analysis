#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''0101 qxlive capture recon -- READ-ONLY. Dumps fetcher qxlive capture code,
scans today captures, tails runner.log 09:25, prints crontab. Prints JSON. Exit 0.'''
from __future__ import annotations
import json, re, sys, subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')
WS = Path.cwd()
PROJECT = WS / 'projects' / 'duanxianxia'
CAPTURES = PROJECT / 'captures'
SCRIPTS = WS / 'scripts'
RUNNER_LOG = PROJECT / 'reports' / '_audit' / 'agent_jobs' / 'runner.log'
TODAY = datetime.now(TZ).strftime('%Y-%m-%d')

report = {'probe': '0101_qxlive_capture_recon',
          'generated_at': datetime.now(TZ).isoformat(timespec='seconds'),
          'today': TODAY}

PAT = re.compile('qxlive|top_metrics|qx_live', re.I)
ORCH = re.compile('premarket|pre_market|盘前|def capture|def fetch|def _fetch|def _capture', re.I)

def dump_windows(path, ctx=45, cap=1000):
    try:
        lines = path.read_text(encoding='utf-8', errors='replace').splitlines()
    except Exception as e:
        return {'error': repr(e)}
    hits = sorted({i for i, ln in enumerate(lines) if PAT.search(ln) or ORCH.search(ln)})
    wins = []
    for i in hits:
        lo, hi = max(0, i - ctx), min(len(lines), i + ctx + 1)
        if wins and lo <= wins[-1][1]:
            wins[-1][1] = max(wins[-1][1], hi)
        else:
            wins.append([lo, hi])
    out, total = [], 0
    for lo, hi in wins:
        if total >= cap:
            out.append('... [truncated] ...')
            break
        chunk = [str(j + 1) + ': ' + lines[j] for j in range(lo, hi)]
        total += len(chunk)
        out.append('\n'.join(chunk))
    return {'n_lines': len(lines), 'n_hit_lines': len(hits), 'windows': out}

try:
    grep = {}
    for p in sorted(SCRIPTS.glob('*.py')):
        try:
            n = len(PAT.findall(p.read_text(encoding='utf-8', errors='replace')))
        except Exception:
            n = 0
        if n:
            grep[p.name] = n
    report['scripts_mentioning_qxlive'] = grep
    targets = list(SCRIPTS.glob('*fetcher*.py')) + list(SCRIPTS.glob('*batch*.py'))
    for name in grep:
        if 'loader' not in name and (SCRIPTS / name) not in targets:
            targets.append(SCRIPTS / name)
    report['fetcher_qxlive_windows'] = {p.name: dump_windows(p) for p in sorted(set(targets))}
except Exception as e:
    report['fetcher_qxlive_windows'] = {'error': repr(e)}

try:
    d = CAPTURES / TODAY
    if d.is_dir():
        ds = {}
        for sub in sorted(d.iterdir()):
            if sub.is_dir():
                files = sorted(f.name for f in sub.glob('*.json'))
                ds[sub.name] = {'n_files': len(files), 'earliest': files[0] if files else None, 'all': files[:60]}
        report['captures_today'] = {'exists': True, 'datasets': ds}
    else:
        report['captures_today'] = {'exists': False, 'path': str(d)}
    if CAPTURES.is_dir():
        report['capture_dates'] = sorted(x.name for x in CAPTURES.iterdir() if x.is_dir())[-15:]
except Exception as e:
    report['captures_today'] = {'error': repr(e)}

try:
    size = RUNNER_LOG.stat().st_size
    with RUNNER_LOG.open('r', encoding='utf-8', errors='replace') as fh:
        fh.seek(max(0, size - 400000))
        tail = fh.read().splitlines()
    kw = re.compile('09:2|盘前|premarket|qxlive', re.I)
    matched = [ln for ln in tail if (TODAY in ln or '09:2' in ln) and kw.search(ln)]
    report['runner_log'] = {'size': size, 'tail_scanned': len(tail), 'matched_last_40': matched[-40:]}
except Exception as e:
    report['runner_log'] = {'error': repr(e)}

try:
    cp = subprocess.run(['crontab', '-l'], capture_output=True, text=True, timeout=20)
    report['crontab'] = {'rc': cp.returncode, 'lines': [l for l in cp.stdout.splitlines() if l.strip() and not l.strip().startswith('#')]}
except Exception as e:
    report['crontab'] = {'error': repr(e)}

print(json.dumps(report, ensure_ascii=False, indent=2))
sys.exit(0)
