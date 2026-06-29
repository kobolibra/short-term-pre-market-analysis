#!/usr/bin/env python3
# 0083: dump fengdan auction-seal breadth aggregates (section_t15/t20/t25_total, seal_total, yizi_count)
# to define the premarket-auction limitUpSealRate; + amount_925 sealed-vs-open counts.
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

print('=== 0083 fengdan auction-seal breadth ===')
t = newest_for('fengdan') or newest_for('jjlive')
if not t:
    print('no fengdan capture')
else:
    cap = json.load(open(t[2], encoding='utf-8'))
    rows = cap.get('rows') or []
    print('date/dsid:', t[0], t[1], 'n=', len(rows))
    r0 = rows[0] if rows else {}
    for k in ['section_date', 'section_kind', 'section_yizi_count', 'section_seal_total',
              'section_t15_total', 'section_t20_total', 'section_t25_total', 'section_has_change_pct']:
        print('  ', k, '=', r0.get(k))
    print('  board_label distribution:', dict(Counter(str(r.get('board_label')) for r in rows)))
    def sealed925(r):
        v = r.get('amount_925')
        return v not in (None, '', '-')
    print('  amount_925 numeric (sealed@9:25):', sum(1 for r in rows if sealed925(r)))
    print('  amount_925 == dash (not sealed@9:25):', sum(1 for r in rows if str(r.get('amount_925')) in ('-', 'None', '')))
    print('  distinct section_kind:', dict(Counter(str(r.get('section_kind')) for r in rows)))
    print('  distinct section_t15_total:', dict(Counter(str(r.get('section_t15_total')) for r in rows)))
    print('  distinct section_t20_total:', dict(Counter(str(r.get('section_t20_total')) for r in rows)))
    print('  distinct section_t25_total:', dict(Counter(str(r.get('section_t25_total')) for r in rows)))
    print('  distinct section_seal_total:', dict(Counter(str(r.get('section_seal_total')) for r in rows)))
    print('  distinct section_yizi_count:', dict(Counter(str(r.get('section_yizi_count')) for r in rows)))
print('=== END 0083 ===')
