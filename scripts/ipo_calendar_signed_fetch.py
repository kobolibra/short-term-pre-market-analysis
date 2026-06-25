#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Signed fetch for 9fzt IPO list.

Reverse-engineered from DG18 webpack module on stock.9fzt.com:
  t.sign && headers = { signature: md5(SECRET + sortedValues.join('') + ts), timestamp: ts }
  baseURL = domain[apiHqDomain] + apiServer[news] = https://api-hq.chongnengjihua.com + /news
  url = /api/1/stock/a/ipo/list ; method GET ; sign true
  SECRET = sjdxfnqogbzoun13d971ckh8p
Signature values are params values ordered by sorted param keys, concatenated, then + timestamp(ms).
"""
import hashlib, time, json, os, ssl, sys, datetime
import urllib.request, urllib.parse, urllib.error

SECRET = "sjdxfnqogbzoun13d971ckh8p"
HOST = "https://api-hq.chongnengjihua.com"
PREFIXES = ["/news", ""]
PATH = "/api/1/stock/a/ipo/list"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

OUT_DIR = "/home/investmentofficehku/.openclaw/workspace/projects/ipo_calendar/reports/_audit"
OUT = os.path.join(OUT_DIR, "latest_9fzt_signed.json")


def make_sign(params):
    keys = sorted(params.keys())
    vals = "".join(str(params[k]) for k in keys)
    ts = int(time.time() * 1000)
    raw = SECRET + vals + str(ts)
    sig = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return sig, ts, raw


def fetch(prefix, params):
    sig, ts, raw = make_sign(params)
    qs = urllib.parse.urlencode(params)
    url = HOST + prefix + PATH + (("?" + qs) if qs else "")
    headers = {
        "signature": sig,
        "timestamp": str(ts),
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://stock.9fzt.com/dataCenter/stockApply.html",
        "Origin": "https://stock.9fzt.com",
    }
    rec = {"url": url, "prefix": prefix, "params": params,
           "sign_raw_head": raw[:60], "sig": sig, "ts": ts}
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    body = ""
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=25, context=ctx) as r:
            rec["status"] = r.status
            body = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        rec["status"] = e.code
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
    except Exception as e:
        rec["status"] = None
        rec["error"] = repr(e)
    rec["body_head"] = body[:1200]
    parsed = None
    try:
        parsed = json.loads(body)
    except Exception as ex:
        rec["parse_err"] = str(ex)
    ok = False
    if isinstance(parsed, dict):
        rec["code"] = parsed.get("code")
        rec["message"] = parsed.get("message")
        data = parsed.get("data")
        rec["data_type"] = type(data).__name__
        if isinstance(data, list):
            rec["data_len"] = len(data)
            rec["first_row"] = data[0] if data else None
            ok = len(data) > 0
        elif isinstance(data, dict):
            rec["data_keys"] = list(data.keys())[:30]
            for k, v in data.items():
                if isinstance(v, list) and v:
                    rec["nested_list_key"] = k
                    rec["nested_len"] = len(v)
                    rec["nested_first"] = v[0]
                    ok = True
                    break
            if not ok and data:
                ok = True
    rec["ok"] = ok
    return rec, (parsed if ok else None)


def main():
    param_sets = [
        {},
        {"pageNum": 1, "pageSize": 30},
        {"pageNum": 1, "pageSize": 50},
        {"pageNum": 1, "pageSize": 200},
        {"page": 1, "size": 30},
        {"type": 1, "pageNum": 1, "pageSize": 30},
        {"market": "all", "pageNum": 1, "pageSize": 50},
    ]
    results = []
    hit = None
    hit_full = None
    for prefix in PREFIXES:
        for ps in param_sets:
            rec, full = fetch(prefix, ps)
            results.append(rec)
            if rec.get("ok") and hit is None:
                hit = rec
                hit_full = full
            time.sleep(0.4)
    out = {
        "generated_at": datetime.datetime.now().isoformat(),
        "host": HOST,
        "path": PATH,
        "secret_head": SECRET[:6] + "...",
        "n_tried": len(results),
        "hit": hit,
        "sample": hit_full if hit_full else None,
        "results": results,
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("wrote", OUT, "n_tried", len(results), "hit", bool(hit))
    if hit:
        print("HIT url:", hit.get("url"), "data_len:", hit.get("data_len") or hit.get("nested_len"))


if __name__ == "__main__":
    main()
