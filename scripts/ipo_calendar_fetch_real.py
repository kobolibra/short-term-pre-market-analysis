#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch the real 9fzt IPO list using the EXACT params read from the page's
getData() call site (not guessed):

  t = { listedSector: plateIndex,   // 0=all A, 1=SH main, 2=SZ main, 3=STAR, 4=ChiNext
        pageNum: pageNo,
        pageSize: 10,
        sortField: sortObj.key,      // initial "onlineStartDate" (listing date)
        sortType: sortObj.order }    // initial 0
  getIpoList(t) -> { count, ipoList: [...] }

Signature: header signature=md5(SECRET+sortedParamValues+ts), timestamp=ts.
URL: https://api-hq.chongnengjihua.com/news/api/1/stock/a/ipo/list
"""
import hashlib, time, json, os, ssl, datetime
import urllib.request, urllib.parse, urllib.error

SECRET = "sjdxfnqogbzoun13d971ckh8p"
HOST = "https://api-hq.chongnengjihua.com"
PREFIX = "/news"
PATH = "/api/1/stock/a/ipo/list"
PAGE = "https://stock.9fzt.com/dataCenter/stockApply.html"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
OUT_DIR = "/home/investmentofficehku/.openclaw/workspace/projects/ipo_calendar/reports/_audit"
OUT = os.path.join(OUT_DIR, "latest_9fzt_signed.json")


def _ctx():
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c


def make_sign(params):
    keys = sorted(params.keys())
    vals = "".join(str(params[k]) for k in keys)
    ts = int(time.time() * 1000)
    raw = SECRET + vals + str(ts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest(), ts


def call(params):
    sig, ts = make_sign(params)
    qs = urllib.parse.urlencode(params)
    url = HOST + PREFIX + PATH + "?" + qs
    h = {"signature": sig, "timestamp": str(ts), "User-Agent": UA,
         "Accept": "application/json, text/plain, */*",
         "Referer": PAGE, "Origin": "https://stock.9fzt.com"}
    rec = {"params": dict(params), "url": url}
    body = ""
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=25, context=_ctx()) as r:
            rec["status"] = r.status
            body = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        rec["status"] = e.code
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            pass
    except Exception as e:
        rec["status"] = None
        rec["error"] = repr(e)
    try:
        rec["json"] = json.loads(body)
    except Exception as ex:
        rec["parse_err"] = str(ex)
        rec["body_head"] = body[:400]
    return rec


def main():
    out = {"generated_at": datetime.datetime.now().isoformat(), "calls": [], "ipoList": []}
    base = {"listedSector": 0, "pageSize": 50, "sortField": "onlineStartDate", "sortType": 0}
    all_rows = []
    count = None
    for page in range(1, 11):
        p = dict(base)
        p["pageNum"] = page
        rec = call(p)
        j = rec.get("json") if isinstance(rec.get("json"), dict) else {}
        data = j.get("data") if isinstance(j, dict) else None
        rows = []
        if isinstance(data, dict):
            rows = data.get("ipoList") or []
            count = data.get("count", count)
        elif isinstance(data, list):
            rows = data
        out["calls"].append({
            "params": p, "status": rec.get("status"),
            "code": (j.get("code") if isinstance(j, dict) else None),
            "message": (j.get("message") if isinstance(j, dict) else None),
            "rows": len(rows), "count": count,
            "body_head": rec.get("body_head"), "parse_err": rec.get("parse_err"),
            "error": rec.get("error"),
        })
        all_rows.extend(rows)
        if not rows or (count is not None and len(all_rows) >= count):
            break
        time.sleep(0.3)
    out["total_rows"] = len(all_rows)
    out["count"] = count
    if all_rows:
        out["field_names"] = sorted(all_rows[0].keys())
        out["sample_rows"] = all_rows[:5]
    out["ipoList"] = all_rows
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("wrote", OUT, "total_rows", len(all_rows), "count", count)
    if all_rows:
        print("fields:", sorted(all_rows[0].keys()))


if __name__ == "__main__":
    main()
