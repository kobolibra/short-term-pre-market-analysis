#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, re, traceback, urllib.parse, urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
ROOT=Path('/home/investmentofficehku/.openclaw/workspace')
URL='https://stock.9fzt.com/dataCenter/stockApply.html'
BASE='https://stock.9fzt.com'
AUDIT=ROOT/'projects'/'ipo_calendar'/'reports'/'_audit'
DXX=ROOT/'projects'/'duanxianxia'/'reports'/'_audit'/'ipo_calendar_debug'
KEYWORDS=['stockApply','xgsg','新股','申购','上市日期','网上申购','IPO','ipo','pageSize','pageNum','current','pagination','dataCenter','query','list','apply','stock_apply']


def fetch(url, timeout=25):
    req=urllib.request.Request(url,headers={
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
        'Accept':'*/*',
        'Accept-Language':'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer':URL,
        'Origin':'https://stock.9fzt.com',
        'Cache-Control':'no-cache',
    })
    with urllib.request.urlopen(req,timeout=timeout) as r:
        raw=r.read(); h=dict(r.headers.items()); st=r.status
    return st,h,raw.decode('utf-8','ignore')

def abs_url(src):
    if src.startswith('//'): return 'https:'+src
    return urllib.parse.urljoin(BASE,src)

def extract_scripts(html):
    vals=[]
    for m in re.finditer(r'<script[^>]+src=["\']([^"\']+)["\']',html,re.I): vals.append(abs_url(m.group(1)))
    for m in re.finditer(r'<link[^>]+href=["\']([^"\']+\.js[^"\']*)["\']',html,re.I): vals.append(abs_url(m.group(1)))
    out=[]; seen=set()
    for u in vals:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def context(txt,pos,n=350):
    return txt[max(0,pos-n):min(len(txt),pos+n)]

def scan_js(name,txt):
    rec={'url':name,'len':len(txt),'keyword_hits':{},'strings':[],'url_like':[],'api_like':[],'contexts':[]}
    for k in KEYWORDS:
        hits=[m.start() for m in re.finditer(re.escape(k),txt,re.I)]
        if hits:
            rec['keyword_hits'][k]=len(hits)
            for p in hits[:6]: rec['contexts'].append({'keyword':k,'pos':p,'context':context(txt,p)})
    # quoted strings, including escaped slash strings common in minified bundles
    strings=re.findall(r'(?<!\\)["\']((?:\\.|[^"\'])*?)(?<!\\)["\']',txt)
    interesting=[]
    for s in strings:
        ss=s.encode('utf-8','ignore').decode('unicode_escape','ignore') if '\\' in s else s
        if any(k.lower() in ss.lower() for k in KEYWORDS) or re.search(r'/(api|server|quote|data|stock|new|xg|ipo|public|market|finance)[A-Za-z0-9_./?=&%-]*',ss,re.I):
            interesting.append(ss[:500])
    rec['strings']=interesting[:300]
    url_like=sorted(set(re.findall(r'https?://[^"\'\\<>\s]+',txt)))
    rec['url_like']=[u for u in url_like if any(k.lower() in u.lower() for k in KEYWORDS+['api','server','quote','data','stock'])][:200]
    paths=[]
    for s in strings:
        ss=s.encode('utf-8','ignore').decode('unicode_escape','ignore') if '\\' in s else s
        if re.match(r'^/(api|server|quote|data|stock|new|xg|ipo|public|market|finance|dc|datacenter)',ss,re.I) or re.search(r'(stockApply|xgsg|ipo|申购|上市日期|pageSize|pageNum)',ss,re.I):
            paths.append(ss)
    rec['api_like']=paths[:300]
    return rec

def try_get_json(candidates):
    tried=[]; seen=set()
    extras=[]
    # Expand relative candidates with common pagination params if no query.
    for c in candidates:
        if not c: continue
        u=abs_url(c) if c.startswith('/') else c
        if not u.startswith('http'): continue
        extras.append(u)
        if '?' not in u:
            for q in ['?pageNum=1&pageSize=20','?page=1&pageSize=20','?current=1&pageSize=20','?currentPage=1&pageSize=20','?limit=20&page=1']:
                extras.append(u+q)
    for u in extras:
        if u in seen: continue
        seen.add(u)
        if len(tried)>=180: break
        low=u.lower()
        if any(x in low for x in ['.js','.css','.png','.jpg','.gif','.ico','.svg','.woff','.map','.html']): continue
        rec={'url':u}
        try:
            st,h,txt=fetch(u,timeout=12)
            rec.update({'ok':True,'status':st,'content_type':h.get('Content-Type') or h.get('content-type'),'len':len(txt),'code_count':len(re.findall(r'\b\d{6}\b',txt)),'head':txt[:1000]})
            if txt.lstrip().startswith(('{','[')):
                try:
                    js=json.loads(txt); rec['json_type']=type(js).__name__; rec['json_keys']=list(js.keys())[:50] if isinstance(js,dict) else None
                except Exception as e: rec['json_error']=repr(e)
        except Exception as e:
            rec.update({'ok':False,'error':repr(e)})
        tried.append(rec)
    return tried

def main():
    AUDIT.mkdir(parents=True,exist_ok=True); DXX.mkdir(parents=True,exist_ok=True)
    out={'generated_at':datetime.now(TZ).isoformat(timespec='seconds'),'source_url':URL}
    try:
        st,h,html=fetch(URL)
        out['html']={'status':st,'content_type':h.get('Content-Type') or h.get('content-type'),'len':len(html),'code_count':len(re.findall(r'\b\d{6}\b',html))}
        scripts=extract_scripts(html)
        out['script_count']=len(scripts); out['scripts']=scripts
        scans=[]; allc=[]
        for u in scripts:
            rec={'url':u}
            try:
                st,h,txt=fetch(u,timeout=30)
                rec.update({'status':st,'content_type':h.get('Content-Type') or h.get('content-type'),'len':len(txt),'ok':True})
                srec=scan_js(u,txt); rec.update(srec)
                allc.extend(rec.get('url_like') or []); allc.extend(rec.get('api_like') or []); allc.extend(rec.get('strings') or [])
            except Exception as e:
                rec.update({'ok':False,'error':repr(e)})
            scans.append(rec)
        out['chunk_scans']=scans
        # prioritize candidates containing target words or endpoint-looking paths
        cand=[]; seen=set()
        for c in allc:
            if not isinstance(c,str): continue
            if any(k.lower() in c.lower() for k in ['stock','apply','ipo','xg','new','page','list','data','server','api','申购','上市']):
                if c not in seen:
                    seen.add(c); cand.append(c)
        out['candidate_count']=len(cand); out['candidates']=cand[:500]
        out['api_tries']=try_get_json(cand)
    except Exception:
        out['error']=traceback.format_exc()
    for p in [AUDIT/'latest_9fzt_api_discovery.json',DXX/'latest_9fzt_api_discovery.json']:
        p.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'generated_at':out.get('generated_at'),'scripts':out.get('script_count'),'candidates':out.get('candidate_count'),'api_tries':len(out.get('api_tries') or []),'error':out.get('error')},ensure_ascii=False))
if __name__=='__main__': main()
