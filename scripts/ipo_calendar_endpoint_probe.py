#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Decisive 9fzt stockApply data endpoint finder.
# Strategy: download the page-specific Next.js chunk + app/framework chunks,
# extract every quoted path/url literal, then probe candidates across the THREE
# 9fzt hosts (stock / api / api+/gw) with GET and POST, ALWAYS capturing the
# response body so auth errors (missing appcode/sign) are visible. A hit is a
# JSON response containing IPO-ish content. All output is implementation detail
# of the user-provided 9fzt stockApply page (same source).
from __future__ import annotations
import json, re, traceback, urllib.parse, urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
ROOT=Path('/home/investmentofficehku/.openclaw/workspace')
PAGE='https://stock.9fzt.com/dataCenter/stockApply.html'
AUDIT=ROOT/'projects'/'ipo_calendar'/'reports'/'_audit'
DXX=ROOT/'projects'/'duanxianxia'/'reports'/'_audit'/'ipo_calendar_debug'
# Chunks most likely to contain the page data fetch call.
CHUNKS=[
  'https://stock.9fzt.com/_next/static/chunks/pages/dataCenter/stockApply.html-1077f1617483e17c7c17.js',
  'https://stock.9fzt.com/_next/static/chunks/cd592ad928dfdb26299ca34f5f9919693ea0755f.689d45b23d01fd250955.js',
  'https://stock.9fzt.com/_next/static/chunks/6156bbab0d794c3f9f5ca993fbf2cdedbc758d36.e985cfe87ee523bc111b.js',
  'https://stock.9fzt.com/_next/static/chunks/cd82629a7060093aab1c2ea578ef9aabcd3c0df9.8b87cb69d7a14e46c47e.js',
  'https://stock.9fzt.com/_next/static/chunks/pages/_app-1ca0fb962a65272bdf73.js',
]
HOSTS=['https://api.9fzt.com/gw','https://api.9fzt.com','https://stock.9fzt.com']
TODAY=datetime.now(TZ).strftime('%Y-%m-%d')
HIT_WORDS=['申购','上市','证券代码','申购代码','申购日期','上市日期','发行价','中签','xgsg','stockApply']


def fetch(url, method='GET', data=None, timeout=15, extra_headers=None):
    headers={
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
        'Accept':'application/json,text/plain,*/*',
        'Accept-Language':'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer':PAGE,
        'Origin':'https://stock.9fzt.com',
    }
    if extra_headers: headers.update(extra_headers)
    body=None
    if data is not None:
        body=json.dumps(data).encode('utf-8'); headers['Content-Type']='application/json'
    req=urllib.request.Request(url,data=body,headers=headers,method=method)
    with urllib.request.urlopen(req,timeout=timeout) as r:
        raw=r.read(); h=dict(r.headers.items()); st=r.status
    return st,h,raw.decode('utf-8','ignore')


def extract_literals(txt):
    strings=re.findall(r'(?<!\\\\)["\']((?:\\\\.|[^"\'])*?)(?<!\\\\)["\']',txt)
    paths=set(); urls=set()
    for s in strings:
        ss=s.encode('utf-8','ignore').decode('unicode_escape','ignore') if '\\\\' in s else s
        if ss.startswith('http') and '9fzt' in ss:
            urls.add(ss)
        if re.match(r'^/[A-Za-z0-9]', ss) and len(ss)<160 and ' ' not in ss:
            paths.add(ss)
    # keep paths that look API-ish or IPO-ish
    keep=[]
    for p in paths:
        if re.search(r'(api|gw|stock|apply|ipo|xg|new|page|list|data|quote|server|market|finance|dc|dataCenter|public)',p,re.I):
            keep.append(p)
    return sorted(keep), sorted(urls)


def looks_like_hit(txt):
    if not txt: return False
    if any(w in txt for w in HIT_WORDS): return True
    # array of objects with many 6-digit codes
    if txt.lstrip().startswith(('{','[')) and len(re.findall(r'\b\d{6}\b',txt))>=5:
        return True
    return False


def build_candidates(paths):
    cands=[]
    param_sets=['', '?pageNum=1&pageSize=30', '?page=1&pageSize=30', '?current=1&size=30',
                '?pageNo=1&pageSize=30', f'?date={TODAY}', f'?tradeDate={TODAY}&pageSize=30',
                '?type=1&pageNum=1&pageSize=30']
    for host in HOSTS:
        for p in paths:
            base=host+p
            for q in param_sets:
                if '?' in base and q.startswith('?'): continue
                cands.append(base+q)
    # de-dupe preserving order
    seen=set(); out=[]
    for c in cands:
        if c not in seen:
            seen.add(c); out.append(c)
    return out


def main():
    AUDIT.mkdir(parents=True,exist_ok=True); DXX.mkdir(parents=True,exist_ok=True)
    out={'generated_at':datetime.now(TZ).isoformat(timespec='seconds'),'page':PAGE,'chunks':{},'paths':[],'urls':[],'tries':[],'hits':[]}
    all_paths=set(); all_urls=set()
    for cu in CHUNKS:
        rec={}
        try:
            st,h,txt=fetch(cu,timeout=30)
            paths,urls=extract_literals(txt)
            rec={'ok':True,'status':st,'len':len(txt),'n_paths':len(paths),'n_urls':len(urls),'paths':paths[:120],'urls':urls[:60]}
            all_paths.update(paths); all_urls.update(urls)
        except Exception as e:
            rec={'ok':False,'error':repr(e)}
        out['chunks'][cu]=rec
    # also include absolute 9fzt api urls directly as candidates
    out['paths']=sorted(all_paths); out['urls']=sorted(all_urls)
    candidates=build_candidates(sorted(all_paths))
    # prepend any absolute urls discovered
    candidates=[u for u in sorted(all_urls) if u.startswith('http')]+candidates
    appcode_variants=[None,{'appcode':'web'},{'appcode':'wwwgw'},{'appcode':'gw'}]
    tried=0
    for u in candidates:
        if tried>=240: break
        low=u.lower()
        if any(x in low for x in ['.js','.css','.png','.jpg','.gif','.ico','.svg','.woff','.map','.html','sensors','baidu','hm.js']): continue
        for hv in appcode_variants:
            if tried>=240: break
            for method,data in (('GET',None),('POST',{})):
                if tried>=240: break
                tried+=1
                rec={'url':u,'method':method,'appcode':hv}
                try:
                    st,h,txt=fetch(u,method=method,data=data,timeout=10,extra_headers=hv)
                    ct=h.get('Content-Type') or h.get('content-type') or ''
                    rec.update({'status':st,'ct':ct,'len':len(txt),'head':txt[:600]})
                    if looks_like_hit(txt):
                        rec['HIT']=True
                        out['hits'].append(rec)
                except Exception as e:
                    msg=repr(e)
                    rec.update({'error':msg})
                    # capture HTTP error bodies (e.g. 400 with auth message)
                    try:
                        import urllib.error
                        if isinstance(e,urllib.error.HTTPError):
                            rec['err_body']=e.read().decode('utf-8','ignore')[:600]; rec['status']=e.code
                    except Exception: pass
                # only keep informative tries to limit size
                if rec.get('HIT') or (rec.get('status') and rec.get('status')!=404) or rec.get('err_body'):
                    out['tries'].append(rec)
                # if hit on GET no-auth, no need to try more variants for this url
                if rec.get('HIT'): break
            if out['hits'] and out['hits'][-1].get('url')==u: break
    out['n_candidates']=len(candidates); out['n_tried']=tried; out['n_hits']=len(out['hits'])
    for p in [AUDIT/'latest_9fzt_endpoint.json',DXX/'latest_9fzt_endpoint.json']:
        p.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'paths':len(out['paths']),'urls':len(out['urls']),'candidates':out['n_candidates'],'tried':tried,'hits':out['n_hits'],'hit_urls':[h['url'] for h in out['hits'][:10]]},ensure_ascii=False))

if __name__=='__main__':
    try:
        main()
    except Exception:
        traceback.print_exc(); raise
