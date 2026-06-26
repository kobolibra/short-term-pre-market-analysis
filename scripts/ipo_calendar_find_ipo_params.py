#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Find the correct params for the 9fzt IPO list endpoint.

Signature is already accepted: with /news prefix the endpoint returns
  {"code":20001,"message":"\u53c2\u6570\u975e\u6cd5"}  (illegal params)
so the signature + host + path + /news prefix are all correct; we only need
the right query params. This script (a) extracts the JS call site for
getIpoList to read real param names, and (b) brute-forces common param sets.
"""
import hashlib, time, json, os, ssl, re, calendar, datetime
import urllib.request, urllib.parse, urllib.error

SECRET = "sjdxfnqogbzoun13d971ckh8p"
HOST = "https://api-hq.chongnengjihua.com"
PREFIX = "/news"
PATH = "/api/1/stock/a/ipo/list"
PAGE = "https://stock.9fzt.com/dataCenter/stockApply.html"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
OUT_DIR = "/home/investmentofficehku/.openclaw/workspace/projects/ipo_calendar/reports/_audit"
OUT = os.path.join(OUT_DIR, "latest_9fzt_ipo_params.json")


def _ctx():
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c


def get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout, context=_ctx()) as r:
        return r.read().decode("utf-8", "replace")


def find_chunks(html):
    urls = set()
    for m in re.findall(r'src="([^"]+\.js)"', html):
        urls.add(m)
    for m in re.findall(r'"(/_next/static/[^"]+\.js)"', html):
        urls.add(m)
    out = []
    for u in urls:
        if u.startswith("//"):
            u = "https:" + u
        elif u.startswith("/"):
            u = "https://stock.9fzt.com" + u
        out.append(u)
    return out


def make_sign(params):
    keys = sorted(params.keys())
    vals = "".join(str(params[k]) for k in keys)
    ts = int(time.time() * 1000)
    raw = SECRET + vals + str(ts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest(), ts


def call(params):
    sig, ts = make_sign(params)
    qs = urllib.parse.urlencode(params)
    url = HOST + PREFIX + PATH + (("?" + qs) if qs else "")
    h = {"signature": sig, "timestamp": str(ts), "User-Agent": UA,
         "Accept": "application/json, text/plain, */*",
         "Referer": PAGE, "Origin": "https://stock.9fzt.com"}
    rec = {"params": params, "url": url}
    body = ""
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=20, context=_ctx()) as r:
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
    rec["body_head"] = body[:600]
    try:
        j = json.loads(body)
        rec["code"] = j.get("code")
        rec["message"] = j.get("message")
        d = j.get("data")
        if isinstance(d, list):
            rec["data_len"] = len(d)
            rec["first"] = d[0] if d else None
            rec["ok"] = bool(d)
        elif isinstance(d, dict):
            rec["data_keys"] = list(d.keys())[:30]
            found = False
            for k, v in d.items():
                if isinstance(v, list) and v:
                    rec["nested_key"] = k
                    rec["nested_len"] = len(v)
                    rec["nested_first"] = v[0]
                    rec["ok"] = True
                    found = True
                    break
            if not found:
                rec["ok"] = bool(d)
        else:
            rec["ok"] = False
    except Exception as ex:
        rec["parse_err"] = str(ex)
        rec["ok"] = False
    return rec


def main():
    out = {"generated_at": datetime.datetime.now().isoformat(), "js": {}, "probes": []}
    try:
        html = get(PAGE)
        out["page_len"] = len(html)
        chunks = find_chunks(html)
        out["chunk_count"] = len(chunks)
        kws = ["getIpoList", "ipo/list", "kpAY", "Ipo", "beginDate", "endDate",
               "startDate", "applyDate", "tradeDate", "month", "queryDate",
               "pageNum", "pageSize"]
        for cu in chunks:
            if "stockApply" not in cu:
                continue
            try:
                js = get(cu)
            except Exception as e:
                out["js"][cu] = "ERR " + repr(e)
                continue
            snips = []
            for kw in kws:
                i = 0
                while len(snips) < 60:
                    idx = js.find(kw, i)
                    if idx < 0:
                        break
                    snips.append({"kw": kw, "snippet": js[max(0, idx - 220):idx + 320]})
                    i = idx + len(kw)
            out["js"][cu] = {"len": len(js), "snips": snips}
    except Exception as e:
        out["js_err"] = repr(e)

    today = datetime.date.today()
    first = today.replace(day=1)
    last = today.replace(day=calendar.monthrange(today.year, today.month)[1])
    d1 = first.strftime("%Y-%m-%d")
    d2 = last.strftime("%Y-%m-%d")
    n1 = first.strftime("%Y%m%d")
    n2 = last.strftime("%Y%m%d")
    ym = today.strftime("%Y-%m")
    td = today.strftime("%Y-%m-%d")
    param_sets = [
        {"beginDate": d1, "endDate": d2},
        {"startDate": d1, "endDate": d2},
        {"beginDate": n1, "endDate": n2},
        {"startTime": d1, "endTime": d2},
        {"month": ym},
        {"date": td},
        {"tradeDate": td},
        {"applyDate": td},
        {"type": "1"}, {"type": "0"}, {"type": "2"},
        {"status": "1"}, {"listStatus": "1"}, {"queryType": "1"},
        {"market": "1"}, {"market": "0"},
        {"beginDate": d1, "endDate": d2, "pageNum": 1, "pageSize": 50},
        {"beginDate": d1, "endDate": d2, "type": 1},
        {"pageNum": "1", "pageSize": "30"},
        {"year": today.year, "month": today.month},
    ]
    hit = None
    for ps in param_sets:
        rec = call(ps)
        out["probes"].append(rec)
        if rec.get("ok") and not hit:
            hit = rec
        time.sleep(0.3)
    out["hit"] = hit
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("wrote", OUT, "probes", len(out["probes"]), "hit", bool(hit))
    if hit:
        print("HIT params:", hit.get("params"), "len:", hit.get("data_len") or hit.get("nested_len"))


if __name__ == "__main__":
    main()
