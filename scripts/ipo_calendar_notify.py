#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ipo_calendar_notify.py — 独立新股日历抓取 + 飞书推送。

数据源严格使用用户指定页面：
  https://stock.9fzt.com/dataCenter/stockApply.html

每天交易日上午 8 点运行：
  - 抓取/解析九方智投新股申购表；
  - 筛选“今天”为 申购日期 / 网上申购缴款日期 / 上市日期 的股票；
  - 保存独立数据到 projects/ipo_calendar/；
  - 通过飞书机器人推送摘要。

与 duanxianxia 隔离：数据、报告都落在 projects/ipo_calendar/。

注意：9fzt 页面在不同环境可能返回 HTML 表格，也可能返回已渲染文本/脚本骨架。
本脚本只围绕该 URL 解析：先解析 HTML <tr>，失败后解析页面文本中的管道表格。
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import html as html_lib
import json
import os
import re
import sys
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

DEFAULT_URL = "https://stock.9fzt.com/dataCenter/stockApply.html"
TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parent.parent
OPENCLAW_ROOT = ROOT.parent
PROJECT = ROOT / "projects" / "ipo_calendar"
DATA_DIR = PROJECT / "data"
AUDIT_DIR = PROJECT / "reports" / "_audit"

HEADERS = [
    "序号", "代码", "股票名称", "发行总数(万股)", "网上发行(万股)", "申购上限(万股)",
    "发行价(元)", "首日收盘价(元)", "申购日期", "中签率公告日", "网上申购缴款日期", "上市日期",
    "筹集资金(万元)", "实际筹集资金", "发行市盈率", "行业市盈率", "中签率", "询价累积报价倍数",
    "有效报价配售对象家数", "招股书",
]
DATE_FIELDS = ["申购日期", "网上申购缴款日期", "上市日期"]


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            value = value.strip()
            if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ.setdefault(key, value)
    except Exception:
        return


load_env_file(OPENCLAW_ROOT / ".env")
load_env_file(ROOT / ".env")


def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


def fetch_html(url: str, timeout: int = 25) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Referer": url,
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    for enc in ("utf-8", "gb18030", "gbk"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="ignore")


def clean_cell(s: str) -> str:
    s = re.sub(r"<script[\s\S]*?</script>", "", s, flags=re.I)
    s = re.sub(r"<style[\s\S]*?</style>", "", s, flags=re.I)
    s = re.sub(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>[\s\S]*?</a>", r"\1", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html_lib.unescape(s)
    s = s.replace("\\--", "--").replace("\u00a0", " ")
    return re.sub(r"\s+", " ", s).strip()


def parse_html_table(page_html: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", page_html, flags=re.I):
        cells_raw = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr, flags=re.I)
        cells = [clean_cell(c) for c in cells_raw]
        if len(cells) < 12:
            continue
        if not re.fullmatch(r"\d+", cells[0] or ""):
            continue
        if not re.fullmatch(r"\d{6}", cells[1] or ""):
            continue
        vals = (cells + [""] * len(HEADERS))[: len(HEADERS)]
        rows.append(dict(zip(HEADERS, vals)))
    return rows


def parse_pipe_text(text: str) -> List[Dict[str, Any]]:
    """Parse rendered markdown-like pipe table text, e.g. web-rendered 9fzt output."""
    rows: List[Dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("|") or not line.endswith("|"):
            continue
        if "---" in line:
            continue
        cells = [c.strip().replace("\\.", ".").replace("\\--", "--") for c in line.strip("|").split("|")]
        if len(cells) < 12:
            continue
        if not re.fullmatch(r"\d+", cells[0] or ""):
            continue
        if not re.fullmatch(r"\d{6}", cells[1] or ""):
            continue
        vals = (cells + [""] * len(HEADERS))[: len(HEADERS)]
        rows.append(dict(zip(HEADERS, vals)))
    return rows


def parse_stock_apply_page(page: str) -> List[Dict[str, Any]]:
    rows = parse_html_table(page)
    if rows:
        return rows
    text = html_lib.unescape(page)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</tr>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return parse_pipe_text(text)


def normalize_md(mmdd: str, year: int) -> Optional[str]:
    s = (mmdd or "").strip()
    if not s or s in {"--", "-", "—"}:
        return None
    m = re.search(r"(?:(\d{4})[-/])?(\d{1,2})[-/](\d{1,2})", s)
    if not m:
        return None
    y = int(m.group(1) or year)
    mo = int(m.group(2))
    d = int(m.group(3))
    return f"{y:04d}-{mo:02d}-{d:02d}"


def match_events(rows: List[Dict[str, Any]], target_date: str) -> List[Dict[str, Any]]:
    year = int(target_date[:4])
    out: List[Dict[str, Any]] = []
    for r in rows:
        events = []
        for f in DATE_FIELDS:
            full = normalize_md(str(r.get(f, "")), year)
            if full == target_date:
                events.append(f)
        if events:
            item = dict(r)
            item["事件"] = events
            item["事件标签"] = "+".join(events)
            out.append(item)
    pri = {"上市日期": 0, "申购日期": 1, "网上申购缴款日期": 2}
    out.sort(key=lambda x: (min(pri.get(e, 9) for e in x["事件"]), x.get("代码", "")))
    return out


def first_env(names: List[str]) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def webhook_config(cli_url: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    url = cli_url or first_env([
        "IPO_FEISHU_WEBHOOK_URL", "FEISHU_WEBHOOK_URL", "LARK_WEBHOOK_URL",
        "IPO_FEISHU_WEBHOOK", "FEISHU_WEBHOOK", "LARK_WEBHOOK", "WEBHOOK_URL",
        "FEISHU_BOT_WEBHOOK", "LARK_BOT_WEBHOOK", "DXX_FEISHU_WEBHOOK_URL",
    ])
    secret = first_env(["IPO_FEISHU_SIGN_SECRET", "FEISHU_SIGN_SECRET", "LARK_SIGN_SECRET"])
    return url, secret


def feishu_sign(secret: str, timestamp: str) -> str:
    string_to_sign = f"{timestamp}\n{secret}".encode("utf-8")
    digest = hmac.new(string_to_sign, b"", hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def post_feishu(text: str, webhook_url: str, secret: Optional[str] = None, timeout: int = 15) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
    if secret:
        ts = str(int(time.time()))
        payload["timestamp"] = ts
        payload["sign"] = feishu_sign(secret, ts)
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json; charset=utf-8", "User-Agent": "ipo-calendar-bot/1.0"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return {"ok": 200 <= resp.status < 300, "status": resp.status, "body": body[:1000]}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore") if hasattr(e, "read") else ""
        return {"ok": False, "status": e.code, "body": body[:1000]}
    except Exception as e:
        return {"ok": False, "status": None, "body": f"{type(e).__name__}: {e}"}


def fmt_num(x: Any) -> str:
    s = str(x or "").strip()
    return "--" if not s or s == "\\--" else s


def build_message(target_date: str, events: List[Dict[str, Any]], source_url: str, total_rows: int) -> str:
    title = f"【新股日历】{target_date} 申购/缴款/上市提醒"
    if not events:
        return f"{title}\n\n今日未匹配到：申购日期 / 网上申购缴款日期 / 上市日期。\n数据源：九方智投新股申购（抓取行数 {total_rows}）\n{source_url}"
    lines = [title, "", f"共 {len(events)} 只："]
    for i, r in enumerate(events, 1):
        lines += [
            f"\n{i}. {r.get('代码')} {r.get('股票名称')} ｜{r.get('事件标签')}",
            f"   发行价: {fmt_num(r.get('发行价(元)'))} 元 ｜发行PE: {fmt_num(r.get('发行市盈率'))} ｜行业PE: {fmt_num(r.get('行业市盈率'))}",
            f"   申购: {fmt_num(r.get('申购日期'))} ｜缴款: {fmt_num(r.get('网上申购缴款日期'))} ｜上市: {fmt_num(r.get('上市日期'))}",
            f"   网上发行: {fmt_num(r.get('网上发行(万股)'))} 万股 ｜上限: {fmt_num(r.get('申购上限(万股)'))} 万股 ｜中签率: {fmt_num(r.get('中签率'))}",
        ]
    lines += ["", f"数据源：九方智投新股申购（抓取行数 {total_rows}）", source_url]
    text = "\n".join(lines)
    return text[:3600] + ("\n…(已截断)" if len(text) > 3600 else "")


def write_outputs(target_date: str, rows: List[Dict[str, Any]], events: List[Dict[str, Any]], msg: str, send_result: Dict[str, Any], source_url: str, parse_debug: Dict[str, Any]) -> Dict[str, str]:
    day_dir = DATA_DIR / target_date
    day_dir.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = day_dir / "stock_apply_raw.json"
    events_path = day_dir / "stock_apply_events.json"
    report_json = AUDIT_DIR / f"{target_date}_ipo_calendar.json"
    report_md = AUDIT_DIR / f"{target_date}_ipo_calendar.md"
    latest_json = AUDIT_DIR / "latest_ipo_calendar.json"
    latest_md = AUDIT_DIR / "latest_ipo_calendar.md"

    raw_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    events_path.write_text(json.dumps(events, ensure_ascii=False, indent=2), encoding="utf-8")
    rec = {"generated_at": datetime.now(TZ).isoformat(timespec="seconds"), "date": target_date, "source_url": source_url, "raw_rows": len(rows), "event_count": len(events), "events": events, "message": msg, "send_result": send_result, "parse_debug": parse_debug}
    report_json.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    md_lines = [f"# 新股日历 {target_date}", "", f"- 生成: {rec['generated_at']}", f"- 抓取行数: {len(rows)} ｜匹配事件: {len(events)}", f"- 飞书发送: {send_result}", "", "## 消息正文", "", "```", msg, "```"]
    report_md.write_text("\n".join(md_lines), encoding="utf-8")
    latest_md.write_text("\n".join(md_lines), encoding="utf-8")
    return {"raw": str(raw_path), "events": str(events_path), "report_json": str(report_json), "report_md": str(report_md)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=today_str(), help="目标日期 YYYY-MM-DD，默认今天(Asia/Shanghai)")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--webhook-url", default=None)
    ap.add_argument("--no-send", action="store_true", help="只抓取和落盘，不发送飞书")
    ap.add_argument("--send-empty", action="store_true", help="没有事件也发送‘今日无’消息")
    ap.add_argument("--run-weekends", action="store_true", help="周末也运行；默认周末跳过")
    args = ap.parse_args()

    target_date = args.date
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    if dt.weekday() >= 5 and not args.run_weekends:
        print(json.dumps({"date": target_date, "skipped": "weekend"}, ensure_ascii=False))
        return 0

    page = fetch_html(args.url)
    rows = parse_stock_apply_page(page)
    parse_debug = {"page_len": len(page), "html_tr_count": len(re.findall(r"<tr[^>]*>", page, re.I)), "code_like_count": len(re.findall(r"\b\d{6}\b", page)), "snippet": page[:500]}
    if not rows:
        # 落盘 debug，明确说明是该 URL 在服务器返回未渲染内容，而非换数据源。
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        (AUDIT_DIR / "latest_ipo_calendar_parse_failed.html").write_text(page, encoding="utf-8")
        (AUDIT_DIR / "latest_ipo_calendar_parse_failed.json").write_text(json.dumps(parse_debug, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError("failed to parse stockApply rows from provided 9fzt URL on VM; raw page saved for debug")
    events = match_events(rows, target_date)
    msg = build_message(target_date, events, args.url, len(rows))

    webhook_url, secret = webhook_config(args.webhook_url)
    if args.no_send:
        send_result = {"ok": None, "skipped": "--no-send"}
    elif (not events) and not args.send_empty:
        send_result = {"ok": None, "skipped": "no events and --send-empty not set"}
    elif not webhook_url:
        send_result = {"ok": False, "error": "missing webhook url env IPO_FEISHU_WEBHOOK_URL/FEISHU_WEBHOOK_URL/LARK_WEBHOOK_URL/..."}
    else:
        send_result = post_feishu(msg, webhook_url, secret)

    paths = write_outputs(target_date, rows, events, msg, send_result, args.url, parse_debug)
    print(json.dumps({"date": target_date, "raw_rows": len(rows), "event_count": len(events), "events": events, "send_result": send_result, "paths": paths}, ensure_ascii=False, indent=2))
    return 0 if (send_result.get("ok") is not False or args.no_send or ((not events) and not args.send_empty)) else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)
