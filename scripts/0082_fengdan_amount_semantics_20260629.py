#!/usr/bin/env python3
# 0082: (A) fengdan amount_915/920/925 = 竞价成交额 vs 涨停价委买/封单额?
#       cross-check vs weimai 竞价成交额 for overlapping stocks (DECISIVE, printed last)
#       (B) per-stock 开板 count in review.fupan.plate (prevOpenNum source)
#       (C) locate prev-day sealed/touched limit-up breadth (num/hist/open / 封板率)
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
    if os.path.isdir(CAP):
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

def load(t):
    return json.load(open(t[2], encoding='utf-8')) if t else None

print('=== 0082 fengdan/openNum/breadth verify ===')
print('WS:', WS)

# C) breadth candidates (printed first; decisive A printed last to survive stdout tail)
print('\n--- C) limit-up breadth candidates (封板/封板率/触及/炸板/开板) ---')
hits = 0
if os.path.isdir(CAP):
    for date in sorted(os.listdir(CAP))[-3:]:
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
            keys = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
            kjoin = ''.join(keys)
            lab = cap.get('dataset_label') or ''
            if any(x in kjoin for x in ['封板', '触及', '炸板', '开板']) or any(x in lab for x in ['封板', '情绪', '晋级']):
                print('%s/%s label=%s keys=%s' % (date, dsid, lab, json.dumps(keys, ensure_ascii=False)[:300]))
                if rows:
                    print('   row0:', json.dumps(rows[0], ensure_ascii=False)[:400])
                hits += 1
                if hits >= 12:
                    break
        if hits >= 12:
            break

# B) fupan 开板 distribution
print('\n--- B) review.fupan.plate 开板 (prevOpenNum) ---')
fp = newest_for('fupan')
cap = load(fp)
if not cap:
    print('no fupan capture')
else:
    rows = cap.get('rows') or []
    print('date/dsid:', fp[0], fp[1], 'n=', len(rows))
    print('has 开板?:', ('开板' in rows[0]) if rows else None)
    c = Counter(str(r.get('开板')) for r in rows if isinstance(r, dict))
    print('开板 distribution:', dict(c))
    for r in rows[:5]:
        print('   ', r.get('名称'), '开板=', r.get('开板'), '连板=', r.get('连板'), '涨停类型=', r.get('涨停类型'))

# A) fengdan vs weimai (DECISIVE -- last)
print('\n--- A) fengdan amount_915/920/925 vs weimai 竞价成交额 ---')
fd = newest_for('fengdan') or newest_for('jjlive')
wm = newest_for('weimai') or newest_for('daban')
capfd = load(fd)
capwm = load(wm)
if not capfd:
    print('no fengdan capture')
else:
    print('fengdan date/dsid:', fd[0], fd[1])
    rfd = capfd.get('rows') or []
    print('fengdan headers:', json.dumps(capfd.get('headers'), ensure_ascii=False)[:500])
    for r in rfd[:3]:
        if isinstance(r, dict):
            print('   ', r.get('name') or r.get('名称'), r.get('code') or r.get('代码'),
                  '915=', r.get('amount_915'), '920=', r.get('amount_920'), '925=', r.get('amount_925'),
                  'board=', r.get('board_label'))
    if capwm:
        rwm = capwm.get('rows') or []
        wmmap = {}
        for r in rwm:
            if isinstance(r, dict):
                wmmap[str(r.get('code') or r.get('代码') or '')] = r
        print('weimai date/dsid:', wm[0], wm[1], 'n=', len(rwm))
        print('   CROSS-CHECK fengdan vs weimai (auction_amount/auction_turnover):')
        n = 0
        for r in rfd:
            if not isinstance(r, dict):
                continue
            code = str(r.get('code') or r.get('代码') or '')
            w = wmmap.get(code)
            if not w:
                continue
            print('   %s %s | FD 915=%s 920=%s 925=%s | WM auction_amount=%s auction_turnover=%s main_net=%s' % (
                code, (r.get('name') or r.get('名称')),
                r.get('amount_915'), r.get('amount_920'), r.get('amount_925'),
                w.get('auction_amount'), w.get('auction_turnover'), w.get('main_net_inflow')))
            n += 1
            if n >= 8:
                break
        if n == 0:
            print('   (no overlapping codes between fengdan and weimai)')
    else:
        print('no weimai capture for cross-check')
print('\n=== END 0082 ===')
