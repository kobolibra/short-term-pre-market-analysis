#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, re, traceback, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
ROOT=Path('/home/investmentofficehku/.openclaw/workspace')
URL='https://stock.9fzt.com/dataCenter/stockApply.html'
AUDIT=ROOT/'projects'/'ipo_calendar'/'reports'/'_audit'
DXX=ROOT/'projects'/'duanxianxia'/'reports'/'_audit'/'ipo_calendar_debug'
TERMS=['stockApply','新股','申购','上市日期','网上申购','IPO','ipo','apply','Apply','dataCenter','fetch','axios','request','api','接口','pageNum','pageSize','sort','xgsg','reportName','RPT','secucode','SECURITY_CODE']


def fetch(url, timeout=35):
    req=urllib.request.Request(url,headers={
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
        'Accept':'text/html,application/javascript,text/javascript,*/*',
        'Accept-Language':'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer':URL,
        'Cache-Control':'no-cache',
    })
    with urllib.request.urlopen(req,timeout=timeout) as resp:
        raw=resp.read(); headers=dict(resp.headers.items()); status=resp.status
    txt=None
    for enc in ('utf-8','gb18030','gbk'):
        try:
            txt=raw.decode(enc); break
        except Exception: pass
    if txt is None: txt=raw.decode('utf-8','ignore')
    return status,headers,txt


def abs_url(src):
    return urllib.parse.urljoin(URL, src)


def contexts(txt, terms=TERMS, n=600, max_each=8):
    out=[]
    for term in terms:
        c=0
        for m in re.finditer(re.escape(term),txt,re.I):
            s=max(0,m.start()-n); e=min(len(txt),m.end()+n)
            out.append({'term':term,'pos':m.start(),'context':txt[s:e]})
            c+=1
            if c>=max_each: break
    return out


def interesting_strings(txt):
    pats=[]
    # quoted strings likely to be endpoints / report names / field names
    for m in re.finditer(r"['\"]([^'\"]{3,220})['\"]", txt):
        s=m.group(1)
        if any(k.lower() in s.lower() for k in ['api','stock','apply','ipo','dataCenter','xg','申购','新股','RPT','report','page','sort','security','上市']):
            pats.append(s)
            if len(pats)>=300: break
    urls=sorted(set(re.findall(r'https?://[^\"\'\s<>]+|/(?:api|server|stock|dataCenter|quote|gateway|jf|new)[^\"\'\s<>]{1,200}', txt, flags=re.I)))[:300]
    return {'strings':pats[:300],'urls':urls[:300]}


def main():
    AUDIT.mkdir(parents=True,exist_ok=True); DXX.mkdir(parents=True,exist_ok=True)
    rec={'generated_at':datetime.now(TZ).isoformat(timespec='seconds'),'url':URL,'chunks':[]}
    try:
        status,headers,html=fetch(URL)
        rec['page']={'status':status,'len':len(html),'content_type':headers.get('Content-Type') or headers.get('content-type')}
        srcs=[]
        for m in re.finditer(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.I):
            src=m.group(1)
            if '_next/static/chunks' in src or 'dataCenter/stockApply' in src:
                srcs.append(abs_url(src))
        # include preload script hrefs too
        for m in re.finditer(r'<link[^>]+href=["\']([^"\']+\.js)["\']', html, flags=re.I):
            src=m.group(1)
            if '_next/static/chunks' in src or 'dataCenter/stockApply' in src:
                u=abs_url(src)
                if u not in srcs: srcs.append(u)
        rec['script_count']=len(srcs)
        combined_hits=[]
        for u in srcs:
            item={'url':u}
            try:
                st,h,txt=fetch(u,timeout=45)
                item.update({'ok':True,'status':st,'content_type':h.get('Content-Type') or h.get('content-type'),'len':len(txt),'code_count':len(re.findall(r'\b\d{6}\b',txt))})
                item['contexts']=contexts(txt)
                item['interesting']=interesting_strings(txt)
                if item['contexts'] or item['interesting']['strings'] or item['interesting']['urls']:
                    combined_hits.append({'url':u,'contexts':item['contexts'][:20],'interesting':item['interesting']})
                # Save only page chunk and chunks with terms to keep repo small
                if item['contexts'] or 'stockApply.html' in u:
                    safe=re.sub(r'[^A-Za-z0-9_.-]+','_',u)[-160:]
                    (AUDIT/f'chunk_{safe}.js').write_text(txt,encoding='utf-8')
            except Exception as e:
                item.update({'ok':False,'error':repr(e),'trace':traceback.format_exc()[-1000:]})
            rec['chunks'].append(item)
        rec['combined_hits']=combined_hits[:80]
    except Exception:
        rec['error']=traceback.format_exc()
    for p in [AUDIT/'latest_9fzt_api_inspect.json', DXX/'latest_9fzt_api_inspect.json']:
        p.write_text(json.dumps(rec,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'generated_at':rec.get('generated_at'),'script_count':rec.get('script_count'),'hit_chunks':len(rec.get('combined_hits',[])),'error':rec.get('error')},ensure_ascii=False))

if __name__=='__main__': main()
