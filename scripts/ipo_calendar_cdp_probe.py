#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, os, re, shutil, subprocess, tempfile, time, traceback, urllib.request, urllib.parse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
ROOT=Path('/home/investmentofficehku/.openclaw/workspace')
URL='https://stock.9fzt.com/dataCenter/stockApply.html'
AUDIT=ROOT/'projects'/'ipo_calendar'/'reports'/'_audit'
DXX=ROOT/'projects'/'duanxianxia'/'reports'/'_audit'/'ipo_calendar_debug'


def http_json(url, timeout=5):
    with urllib.request.urlopen(url,timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8','ignore'))

def cdp(port, method, params=None, sid=None, timeout=8):
    payload=json.dumps({'id':int(time.time()*1000000)%1000000000,'method':method,'params':params or {}},ensure_ascii=False).encode()
    url=f'http://127.0.0.1:{port}/json/protocol'
    # placeholder; websocket not available in stdlib. Use chrome remote debugging HTTP endpoints + performance log via --log-net-log instead.
    return None

def fetch_url(u, timeout=25):
    req=urllib.request.Request(u,headers={
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
        'Accept':'application/json,text/plain,*/*',
        'Referer':URL,
        'Origin':'https://stock.9fzt.com',
    })
    with urllib.request.urlopen(req,timeout=timeout) as resp:
        raw=resp.read(); headers=dict(resp.headers.items()); status=resp.status
    txt=raw.decode('utf-8','ignore')
    return status,headers,txt

def launch_and_netlog():
    rec={'attempts':[]}
    for exe in ['google-chrome','google-chrome-stable','chromium','chromium-browser']:
        p=shutil.which(exe)
        rec['attempts'].append({'exe':exe,'path':p})
        if not p: continue
        with tempfile.TemporaryDirectory(prefix='ipo_chrome_') as td:
            netlog=str(Path(td)/'netlog.json')
            cmd=[p,'--headless=new','--no-sandbox','--disable-gpu','--disable-dev-shm-usage','--disable-extensions','--disable-background-networking','--disable-sync','--metrics-recording-only','--disable-default-apps','--mute-audio',f'--user-data-dir={td}',f'--log-net-log={netlog}','--net-log-capture-mode=IncludeSensitive',URL]
            try:
                proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                time.sleep(35)
                proc.terminate()
                try: proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill(); proc.wait(timeout=5)
                stderr=''
                try: stderr=(proc.stderr.read() if proc.stderr else '')[-3000:]
                except Exception: pass
                item={'exe':exe,'rc':proc.returncode,'netlog_exists':Path(netlog).exists(),'stderr_tail':stderr}
                if Path(netlog).exists():
                    txt=Path(netlog).read_text(encoding='utf-8',errors='ignore')
                    item['netlog_len']=len(txt)
                    urls=sorted(set(re.findall(r'https?://[^"\\\s<>]+',txt)))
                    item['url_count']=len(urls)
                    item['interesting_urls']=[u for u in urls if any(k.lower() in u.lower() for k in ['stock','apply','ipo','data','new','page','api','xg','ajax','list'])][:300]
                    rec['attempts'].append(item)
                    rec['ok_exe']=exe
                    rec['netlog_urls']=urls[:1000]
                    rec['interesting_urls']=item['interesting_urls']
                    return rec
                rec['attempts'].append(item)
            except Exception as e:
                rec['attempts'].append({'exe':exe,'error':repr(e),'trace':traceback.format_exc()[-1000:]})
    return rec

def try_candidate_apis(urls):
    out=[]
    # Try exact interesting URLs that look like XHR/API first; skip static assets.
    for u in urls:
        low=u.lower()
        if any(x in low for x in ['.js','.css','.png','.jpg','.gif','.ico','.woff','.map','.svg']):
            continue
        if not any(k in low for k in ['api','server','stock','apply','ipo','data','list','page']):
            continue
        rec={'url':u}
        try:
            st,h,txt=fetch_url(u,timeout=12)
            rec.update({'ok':True,'status':st,'content_type':h.get('Content-Type') or h.get('content-type'),'len':len(txt),'head':txt[:800],'code_count':len(re.findall(r'\b\d{6}\b',txt))})
            if txt.lstrip().startswith(('{','[')):
                try:
                    js=json.loads(txt); rec['json_type']=type(js).__name__; rec['json_keys']=list(js.keys()) if isinstance(js,dict) else None
                except Exception as e: rec['json_error']=repr(e)
        except Exception as e:
            rec.update({'ok':False,'error':repr(e)})
        out.append(rec)
        if len(out)>=120: break
    return out

def main():
    AUDIT.mkdir(parents=True,exist_ok=True); DXX.mkdir(parents=True,exist_ok=True)
    rec={'generated_at':datetime.now(TZ).isoformat(timespec='seconds'),'url':URL}
    try:
        net=launch_and_netlog(); rec['netlog']=net
        rec['api_tries']=try_candidate_apis(net.get('interesting_urls') or net.get('netlog_urls') or [])
    except Exception:
        rec['error']=traceback.format_exc()
    for p in [AUDIT/'latest_9fzt_cdp_probe.json',DXX/'latest_9fzt_cdp_probe.json']:
        p.write_text(json.dumps(rec,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'generated_at':rec.get('generated_at'),'interesting_urls':len(rec.get('netlog',{}).get('interesting_urls',[])),'api_tries':len(rec.get('api_tries',[])),'error':rec.get('error')},ensure_ascii=False))
if __name__=='__main__': main()
