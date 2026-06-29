#!/usr/bin/env python3
# 0084: locate a COUNT-based premarket-auction 封板率 source.
# (a) home.qxlive.top_metrics: PB/封板率 rows + distinct time_point
# (b) review_daily / core11 (getChartByQingxu): find 封板率 series + time points
# (c) any capture mentioning 封板率 with a time field
import os, json, glob
from collections import Counter

def find_ws():
    for c in [os.getcwd(), os.environ.get('WORKSPACE', ''),
              os.path.expanduser('~/.openclaw/workspace'),
              '/home/investmentofficehku/.openclaw/workspace']:
        if c and os.path.isdir(os.path.join(c, 'projects', 'duanxianxia', 'captures')):
            return c
    return os.getcwd()

WS = find_ws()
CAP = os.path.join(WS, 'projects', 'duanxianxia', 'captures')

def newest_for(substr):
    res = []
    for date in sorted(os.listdir(CAP)):
        dd = os.path.join(CAP, date)
        if not os.path.isdir(dd):
            continue
        for dsid in os.listdir(dd):
            if substr in dsid.lower():
                fs = sorted(glob.glob(os.path.join(dd, dsid, '*.json')))
                if fs:
                    res.append((date, dsid, fs[-1]))
    return res[-1] if res else None

def dump_cap(label, t):
    if not t:
        print('\n###', label, '-> none')
        return
    cap = json.load(open(t[2], encoding='utf-8'))
    rows = cap.get('rows') or []
    print('\n###', label, t[0], t[1], 'n=', len(rows))
    print('cap keys:', list(cap.keys()))
    if rows and isinstance(rows[0], dict):
        print('row0 keys:', list(rows[0].keys()))
        print('row0:', json.dumps(rows[0], ensure_ascii=False)[:600])
        if 'time_point' in rows[0]:
            print('distinct time_point:', dict(Counter(str(r.get('time_point')) for r in rows)))
    hits = [r for r in rows if isinstance(r, dict) and '封板' in json.dumps(r, ensure_ascii=False)]
    print('rows mentioning 封板:', len(hits))
    for r in hits[:8]:
        print('   ', json.dumps(r, ensure_ascii=False)[:400])

print('=== 0084 auction sealrate source ===')
print('WS:', WS)
dump_cap('qxlive.top_metrics', newest_for('top_metrics') or newest_for('qxlive'))
dump_cap('review_daily', newest_for('review_daily') or newest_for('qingxu'))
dump_cap('review_daily_core11', newest_for('core11'))

# (c) broad scan over last 2 dates for 封板率 with a time-ish field
print('\n--- (c) broad scan for 封板率 + time field ---')
seen = 0
if os.path.isdir(CAP):
    for date in sorted(os.listdir(CAP))[-2:]:
        dd = os.path.join(CAP, date)
        if not os.path.isdir(dd):
            continue
        for dsid in sorted(os.listdir(dd)):
            fs = sorted(glob.glob(os.path.join(dd, dsid, '*.json')))
            if not fs:
                continue
            try:
                cap = json.load(open(fs[-1], encoding='utf-8'))
            except Exception:
                continue
            rows = cap.get('rows') or []
            blob = json.dumps(rows[:3], ensure_ascii=False) if rows else ''
            if '封板率' in blob:
                keys = list(rows[0].keys()) if isinstance(rows[0], dict) else []
                print('%s/%s keys=%s' % (date, dsid, json.dumps(keys, ensure_ascii=False)[:200]))
                seen += 1
                if seen >= 10:
                    break
        if seen >= 10:
            break
print('=== END 0084 ===')
