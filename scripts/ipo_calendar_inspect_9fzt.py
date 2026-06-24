#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import html, json, re, urllib.request, shutil, subprocess, traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
ROOT=Path('/home/investmentofficehku/.openclaw/workspace')
URL='https://stock.9fzt.com/dataCenter/stockApply.html'
AUDIT=ROOT/'projects'/'ipo_calendar'/'reports'/'_audit'
DXX=ROOT/'projects'/'duanxianxia'/'reports'/'_audit'/'ipo_calendar_debug'


def fetch():
    req=urllib.request.Request(URL,headers={
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language':'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control':'no-cache',
        'Referer':URL,
    })
    with urllib.request.urlopen(req,timeout=30) as resp:
        raw=resp.read(); headers=dict(resp.headers.items()); status=resp.status
    txt=None
    for enc in ('utf-8','gb18030','gbk'):
        try: txt=raw.decode(enc); break
        except Exception: pass
    if txt is None: txt=raw.decode('utf-8','ignore')
    return status,headers,txt


def render():
    out={'attempts':[]}
    for exe in ['chromium','chromium-browser','google-chrome','google-chrome-stable','chrome']:
        p=shutil.which(exe); out['attempts'].append({'exe':exe,'path':p})
        if not p: continue
        cmd=[p,'--headless=new','--disable-gpu','--no-sandbox','--disable-dev-shm-usage','--virtual-time-budget=12000','--run-all-compositor-stages-before-draw','--dump-dom',URL]
        try:
            proc=subprocess.run(cmd,text=True,capture_output=True,timeout=90)
            out['attempts'].append({'exe':exe,'rc':proc.returncode,'stdout_len':len(proc.stdout or ''),'stderr_tail':(proc.stderr or '')[-1000:]})
            if proc.returncode==0 and len(proc.stdout or '')>1000:
                out['ok_exe']=exe
                return proc.stdout,out
        except Exception as e:
            out['attempts'].append({'exe':exe,'error':repr(e)})
    return '',out


def contexts(txt, terms, n=1000):
    arr=[]
    for term in terms:
        for m in re.finditer(re.escape(term), txt):
            s=max(0,m.start()-n); e=min(len(txt),m.end()+n)
            arr.append({'term':term,'pos':m.start(),'context':txt[s:e]})
            if len([x for x in arr if x['term']==term])>=5: break
    return arr


def scripts_info(txt):
    infos=[]
    for m in re.finditer(r'<script([^>]*)>([\s\S]*?)</script>',txt,re.I):
        attrs=m.group(1); body=m.group(2)
        info={'pos':m.start(),'attrs':attrs[:500],'len':len(body),'has_code':bool(re.search(r'\b\d{6}\b',body)),'has_apply':('申购日期' in body or 'stockApply' in body or 'APPLY_DATE' in body)}
        if info['has_code'] or info['has_apply'] or '__NEXT_DATA__' in attrs:
            info['head']=body[:1500]
        infos.append(info)
    return infos


def next_data_summary(txt):
    m=re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>([\s\S]*?)</script>',txt,re.I)
    if not m: return {'found':False}
    raw=html.unescape(m.group(1))
    res={'found':True,'len':len(raw),'head':raw[:1000]}
    try:
        js=json.loads(raw)
        res['top_keys']=list(js.keys())
        hits=[]
        def walk(x,path=''):
            if len(hits)>=50: return
            if isinstance(x,dict):
                blob=json.dumps(x,ensure_ascii=False)[:5000]
                if re.search(r'\b\d{6}\b',blob) or '申购日期' in blob or 'APPLY_DATE' in blob:
                    hits.append({'path':path,'keys':list(x.keys())[:60],'sample':blob[:1500]})
                for k,v in x.items(): walk(v,path+'/'+str(k))
            elif isinstance(x,list):
                if x:
                    blob=json.dumps(x[:2],ensure_ascii=False)[:5000]
                    if re.search(r'\b\d{6}\b',blob) or '申购日期' in blob or 'APPLY_DATE' in blob:
                        hits.append({'path':path,'len':len(x),'sample':blob[:2000]})
                for i,v in enumerate(x[:20]): walk(v,path+f'[{i}]')
        walk(js)
        res['hits']=hits
    except Exception as e:
        res['json_error']=repr(e)
    return res


def main():
    AUDIT.mkdir(parents=True,exist_ok=True); DXX.mkdir(parents=True,exist_ok=True)
    rec={'generated_at':datetime.now(TZ).isoformat(timespec='seconds'),'url':URL}
    try:
        status,headers,direct=fetch()
        rec['direct']={'status':status,'content_type':headers.get('Content-Type') or headers.get('content-type'),'len':len(direct),'tr_count':len(re.findall(r'<tr[^>]*>',direct,re.I)),'code_count':len(re.findall(r'\b\d{6}\b',direct))}
        rec['direct_contexts']=contexts(direct,['申购日期','网上申购缴款日期','上市日期','__NEXT_DATA__','stockApply','\u7533\u8d2d','301583','托伦斯'],800)
        rec['direct_scripts']=scripts_info(direct)[:80]
        rec['direct_next_data']=next_data_summary(direct)
        (AUDIT/'latest_9fzt_direct.html').write_text(direct,encoding='utf-8')
        rendered,rdebug=render()
        rec['render_debug']=rdebug
        rec['rendered']={'len':len(rendered),'tr_count':len(re.findall(r'<tr[^>]*>',rendered,re.I)),'code_count':len(re.findall(r'\b\d{6}\b',rendered))}
        if rendered:
            rec['rendered_contexts']=contexts(rendered,['申购日期','网上申购缴款日期','上市日期','__NEXT_DATA__','stockApply','301583','托伦斯'],800)
            rec['rendered_scripts']=scripts_info(rendered)[:80]
            rec['rendered_next_data']=next_data_summary(rendered)
            (AUDIT/'latest_9fzt_rendered.html').write_text(rendered,encoding='utf-8')
    except Exception:
        rec['error']=traceback.format_exc()
    for p in [AUDIT/'latest_9fzt_inspect.json', DXX/'latest_9fzt_inspect.json']:
        p.write_text(json.dumps(rec,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(rec,ensure_ascii=False)[:12000])
if __name__=='__main__': main()
