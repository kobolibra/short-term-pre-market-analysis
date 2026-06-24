#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import json, re, urllib.request, urllib.error, traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
ROOT=Path('/home/investmentofficehku/.openclaw/workspace')
AUDIT=ROOT/'projects'/'ipo_calendar'/'reports'/'_audit'
URLS=[
 ('9fzt', 'https://stock.9fzt.com/dataCenter/stockApply.html'),
 ('eastmoney_ipoapply_all', 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=APPLY_DATE,SECURITY_CODE&sortTypes=-1,-1&pageSize=20&pageNumber=1&reportName=RPTA_APP_IPOAPPLY&columns=ALL'),
 ('eastmoney_ipoapply_cols', 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=APPLY_DATE,SECURITY_CODE&sortTypes=-1,-1&pageSize=20&pageNumber=1&reportName=RPTA_APP_IPOAPPLY&columns=SECURITY_CODE,SECURITY_NAME,APPLY_CODE,APPLY_DATE,PAYMENT_DATE,LISTING_DATE,ISSUE_PRICE,ONLINE_ISSUE_NUM,APPLY_LIMIT,PE_RATIO_A,INDUSTRY_PE_RATIO,BALLOT_NUM,NET_RAISE_FUNDS'),
 ('eastmoney_ipo_info_allnew', 'https://datacenter-web.eastmoney.com/api/data/v1/get?sortColumns=APPLY_DATE,SECURITY_CODE&sortTypes=-1,-1&pageSize=20&pageNumber=1&reportName=RPT_IPO_INFOALLNEW&columns=ALL'),
]

def fetch(url):
    req=urllib.request.Request(url,headers={
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36',
        'Accept':'text/html,application/json,text/plain,*/*',
        'Accept-Language':'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer':'https://stock.9fzt.com/dataCenter/stockApply.html',
    })
    with urllib.request.urlopen(req,timeout=25) as resp:
        raw=resp.read()
        headers=dict(resp.headers.items())
        status=resp.status
    txt=None
    for enc in ('utf-8','gb18030','gbk'):
        try:
            txt=raw.decode(enc); break
        except Exception: pass
    if txt is None: txt=raw.decode('utf-8','ignore')
    return status,headers,txt

def main():
    AUDIT.mkdir(parents=True,exist_ok=True)
    out={'generated_at':datetime.now(TZ).isoformat(timespec='seconds'),'items':[]}
    for name,url in URLS:
        rec={'name':name,'url':url}
        try:
            status,headers,txt=fetch(url)
            rec.update({'ok':True,'status':status,'content_type':headers.get('Content-Type') or headers.get('content-type'),'len':len(txt),'snippet':txt[:1500]})
            rec['tr_count']=len(re.findall(r'<tr[^>]*>',txt,re.I))
            rec['code_count']=len(re.findall(r'\b\d{6}\b',txt))
            if txt.lstrip().startswith('{'):
                try:
                    js=json.loads(txt)
                    rec['json_keys']=list(js.keys())
                    data=js.get('result',{}).get('data') or js.get('data')
                    rec['json_data_len']=len(data) if isinstance(data,list) else None
                    rec['json_first']=data[0] if isinstance(data,list) and data else None
                except Exception as e:
                    rec['json_error']=repr(e)
        except Exception as e:
            rec.update({'ok':False,'error':f'{type(e).__name__}: {e}','trace':traceback.format_exc()[-2000:]})
        out['items'].append(rec)
    p=AUDIT/'ipo_calendar_probe.json'
    p.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(out,ensure_ascii=False,indent=2))
if __name__=='__main__': main()
