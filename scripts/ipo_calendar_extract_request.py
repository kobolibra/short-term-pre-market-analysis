#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, os, ssl, urllib.request, urllib.error, datetime

WS = os.environ.get("WS") or os.path.expanduser("~/.openclaw/workspace")
AUDIT = os.path.join(WS, "projects/ipo_calendar/reports/_audit")
os.makedirs(AUDIT, exist_ok=True)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
REFERER = "https://stock.9fzt.com/dataCenter/stockApply.html"

CHUNKS = [
  "https://stock.9fzt.com/_next/static/chunks/pages/dataCenter/stockApply.html-1077f1617483e17c7c17.js",
  "https://stock.9fzt.com/_next/static/chunks/cd592ad928dfdb26299ca34f5f9919693ea0755f.689d45b23d01fd250955.js",
  "https://stock.9fzt.com/_next/static/chunks/6156bbab0d794c3f9f5ca993fbf2cdedbc758d36.e985cfe87ee523bc111b.js",
  "https://stock.9fzt.com/_next/static/chunks/cd82629a7060093aab1c2ea578ef9aabcd3c0df9.8b87cb69d7a14e46c47e.js",
  "https://upload.9fzt.com/production/9fzt-stock-pc-ssr/globalLogin.js",
]

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch(url, method="GET", headers=None, data=None, timeout=25):
    h = {"User-Agent": UA, "Referer": REFERER, "Origin": "https://stock.9fzt.com"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, method=method, headers=h, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            body = r.read().decode("utf-8", "replace")
            return {"ok": True, "status": getattr(r, "status", 200), "body": body}
    except urllib.error.HTTPError as e:
        try:
            eb = e.read().decode("utf-8", "replace")
        except Exception:
            eb = ""
        return {"ok": False, "status": e.code, "body": eb}
    except Exception as e:
        return {"ok": False, "status": None, "body": "", "error": str(e)}

KEYS = ["ipo/list", "appcode", "baseURL", "baseUrl", "sign", "api.9fzt", "chongneng", "/gw", "/api/1/stock", "interceptors", "headers:", "X-"]

def snippets(text):
    out = {}
    for k in KEYS:
        ms = []
        start = 0
        for _ in range(5):
            i = text.find(k, start)
            if i < 0:
                break
            ms.append(text[max(0, i-220):i+220])
            start = i + len(k)
        if ms:
            out[k] = ms
    return out

result = {"generated_at": datetime.datetime.now().isoformat(), "chunks": {}, "extract": {}, "probes": []}

for u in CHUNKS:
    r = fetch(u)
    result["chunks"][u] = {"ok": r["ok"], "status": r["status"], "len": len(r.get("body", ""))}
    if r["ok"] and r.get("body"):
        result["extract"][u] = snippets(r["body"])

PATH = "/api/1/stock/a/ipo/list"
BASES = [
  "https://api.9fzt.com",
  "https://api.9fzt.com/gw",
  "https://stock.9fzt.com",
  "https://stock.9fzt.com/gw",
  "https://hq.9fzt.com",
  "https://hq.9fzt.com/gw",
  "https://api-hq.chongnengjihua.com",
  "https://api-hq.chongnengjihua.com/gw",
  "https://www.9fzt.com/gw",
]
APPCODES = [None, "web", "wwwgw", "gw", "stock", "hq"]
for base in BASES:
    for ac in APPCODES:
        url = base + PATH + "?pageNum=1&pageSize=30"
        hdr = {"appcode": ac} if ac else None
        r = fetch(url, headers=hdr)
        body = r.get("body") or ""
        hit = bool(r["status"] == 200 and any(x in body for x in ["申购", "上市", "证券代码", "ipo", "\"data\"", "code"]))
        result["probes"].append({"url": url, "appcode": ac, "status": r["status"], "body": body[:240], "hit": hit})

out = os.path.join(AUDIT, "latest_9fzt_request_extract.json")
with open(out, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("wrote", out, "chunks", len(result["chunks"]), "probes", len(result["probes"]), "hits", sum(1 for p in result["probes"] if p.get("hit")))
