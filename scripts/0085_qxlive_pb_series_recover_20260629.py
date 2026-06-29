#!/usr/bin/env python3
# 0085: can we recover the 9:25 PB 今日封板率 from the existing QX-live capture?
# Dump full PB row (incl *_series), cap meta, fetched_at/source_path, and whether
# series fields hold an intraday array with a 9:25 point. Also check capture timing.
import os, json, glob

def find_ws():
    for c in [os.getcwd(), os.environ.get('WORKSPACE', ''),
              os.path.expanduser('~/.openclaw/workspace'),
              '/home/investmentofficehku/.openclaw/workspace']:
        if c and os.path.isdir(os.path.join(c, 'projects', 'duanxianxia', 'captures')):
            return c
    return os.getcwd()

WS = find_ws()
CAP = os.path.join(WS, 'projects', 'duanxianxia', 'captures')

def all_for(substr):
    res = []
    for date in sorted(os.listdir(CAP)):
        dd = os.path.join(CAP, date)
        if not os.path.isdir(dd):
            continue
        for dsid in os.listdir(dd):
            if substr in dsid.lower():
                for f in sorted(glob.glob(os.path.join(dd, dsid, '*.json'))):
                    res.append((date, dsid, f))
    return res

print('=== 0085 qxlive PB series recover ===')
files = all_for('top_metrics') or all_for('qxlive')
if not files:
    print('no qxlive capture'); print('=== END 0085 ==='); raise SystemExit

# (1) timing across ALL qxlive captures today: list fetched_at + time_point of PB
print('\n--- all qxlive captures: fetched_at + PB time_point/value ---')
for (date, dsid, f) in files[-12:]:
    try:
        cap = json.load(open(f, encoding='utf-8'))
    except Exception as e:
        print(date, os.path.basename(f), 'ERR', e); continue
    rows = cap.get('rows') or []
    pb = next((r for r in rows if isinstance(r, dict) and r.get('metric_key') == 'PB'), None)
    print('%s %s fetched_at=%s pb_tp=%s pb_val=%s' % (
        date, os.path.basename(f), cap.get('fetched_at'),
        (pb or {}).get('time_point'), (pb or {}).get('value')))

# (2) deep-dump newest capture
date, dsid, f = files[-1]
cap = json.load(open(f, encoding='utf-8'))
print('\n--- newest capture deep dump ---')
print('file:', f)
print('fetched_at:', cap.get('fetched_at'), '| utc:', cap.get('fetched_at_utc'))
print('source_path:', cap.get('source_path'))
print('source_url:', cap.get('source_url'))
print('meta:', json.dumps(cap.get('meta'), ensure_ascii=False)[:800])
print('headers:', json.dumps(cap.get('headers'), ensure_ascii=False)[:400])
rows = cap.get('rows') or []
pb = next((r for r in rows if isinstance(r, dict) and r.get('metric_key') == 'PB'), None)
if pb:
    print('\nPB row FULL:')
    print(json.dumps(pb, ensure_ascii=False))
    for k in ['source_series', 'display_series', 'compare_series',
              'raw_value', 'raw_chart_tail_value', 'raw_compare_value']:
        v = pb.get(k)
        print('  field %s: type=%s' % (k, type(v).__name__),
              ('len=%d' % len(v)) if isinstance(v, (list, dict, str)) else '',
              ('sample=%s' % json.dumps(v, ensure_ascii=False)[:300]) if v not in (None, '') else '(empty)')
print('=== END 0085 ===')
