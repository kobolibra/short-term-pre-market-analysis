#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, os, ssl, re, urllib.request, datetime

WS = os.environ.get("WS") or os.path.expanduser("~/.openclaw/workspace")
AUDIT = os.path.join(WS, "projects/ipo_calendar/reports/_audit")
os.makedirs(AUDIT, exist_ok=True)

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
REFERER = "https://stock.9fzt.com/dataCenter/stockApply.html"
PAGE = "https://stock.9fzt.com/dataCenter/stockApply.html"
BASE = "https://stock.9fzt.com"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": REFERER})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""

def win(text, idx, before=200, after=8000):
    return text[max(0, idx-before):idx+after]

html = fetch(PAGE)
chunks = set()
for m in re.findall(r'src="([^"]+\.js)"', html):
    if m.startswith("http"):
        chunks.add(m)
    elif m.startswith("/"):
        chunks.add(BASE + m)
KNOWN = [
  "https://stock.9fzt.com/_next/static/chunks/pages/dataCenter/stockApply.html-1077f1617483e17c7c17.js",
  "https://stock.9fzt.com/_next/static/chunks/cd592ad928dfdb26299ca34f5f9919693ea0755f.689d45b23d01fd250955.js",
]
for k in KNOWN:
    chunks.add(k)

result = {"generated_at": datetime.datetime.now().isoformat(), "page_html_len": len(html), "chunks": [], "dg18": [], "domain_map": [], "apiserver": [], "sign_code": []}

for u in sorted(chunks):
    code = fetch(u)
    result["chunks"].append({"url": u, "len": len(code)})
    if not code:
        continue
    for kw in ["DG18:function", '"DG18":function']:
        i = code.find(kw)
        if i >= 0:
            result["dg18"].append({"url": u, "snippet": win(code, i, 200, 9000)})
    for kw in ["chongnengjihua", "apiHqDomain", "hqdomain", "apiDomain", "rjhy"]:
        s = 0
        for _ in range(3):
            i = code.find(kw, s)
            if i < 0:
                break
            result["domain_map"].append({"url": u, "kw": kw, "snippet": win(code, i, 500, 700)})
            s = i + len(kw)
    for kw in ["apiServer", "requestKey"]:
        i = code.find(kw)
        if i >= 0:
            result["apiserver"].append({"url": u, "kw": kw, "snippet": win(code, i, 200, 700)})
    for kw in ["createSign", "getSign", "makeSign", "signature", "x-sign", "X-Sign", "timestamp", "appcode", "secret", "HmacSHA", ".md5", "Md5", "MD5", "nonce"]:
        i = code.find(kw)
        if i >= 0:
            result["sign_code"].append({"url": u, "kw": kw, "snippet": win(code, i, 200, 500)})

out = os.path.join(AUDIT, "latest_9fzt_dg18.json")
with open(out, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("wrote", out, "chunks", len(result["chunks"]), "dg18", len(result["dg18"]), "domain_map", len(result["domain_map"]), "sign", len(result["sign_code"]))
