#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ipo_calendar_notify.py — 独立新股日历抓取 + 飞书推送。

数据源严格使用用户指定页面：
  https://stock.9fzt.com/dataCenter/stockApply.html

每天交易日上午 8 点运行：抓取九方智投新股申购表，筛选今天为
申购日期 / 网上申购缴款日期 / 上市日期 的股票，并推送飞书。

注意：该页面在不同环境可能返回：HTML 表格、Next/React DOM、或无 table 的渲染文本。
本脚本只使用这一个 9fzt URL，但解析方式会兼容这些返回形态。
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
import shutil
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

DEFAULT_URL = "https://stock.9fzt.com/dataCenter/stockApply.html"
TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parent.parent
OPENCLAW_ROOT = ROOT.parent
PROJECT = ROOT / "projects" / "ipo_calendar"
DATA_DIR = PROJECT / "data"
AUDIT_DIR = PROJECT / "reports" / "_audit"
DXX_IPO_DEBUG_DIR = ROOT / "projects" / "duanxianxia" / "reports" / "_audit" / "ipo_calendar_debug"

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


def render_with_chromium(url: str, timeout: int = 90) -> Tuple[Optional[str], Dict[str, Any]]:
    debug: Dict[str, Any] = {"method": "chromium_dump_dom", "attempts": []}
    for exe in ["chromium", "chromium-browser", "google-chrome", "google-chrome-stable", "chrome"]:
        path = shutil.which(exe)
        debug["attempts"].append({"exe": exe, "path": path})
        if not path:
            continue
        cmd = [
            path, "--headless=new", "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage",
            "--virtual-time-budget=15000", "--run-all-compositor-stages-before-draw", "--dump-dom", url,
        ]
        try:
            proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
            debug["attempts"].append({"exe": exe, "rc": proc.returncode, "stdout_len": len(proc.stdout or ""), "stderr_tail": (proc.stderr or "")[-1000:]})
            if proc.returncode == 0 and proc.stdout and len(proc.stdout) > 1000:
                debug["ok_exe"] = exe
                return proc.stdout, debug
        except Exception as e:
            debug["attempts"].append({"exe": exe, "error": f"{type(e).__name__}: {e}"})
    return None, debug


def clean_cell(s: str) -> str:
    s = re.sub(r"<script[\s\S]*?</script>", "", s, flags=re.I)
    s = re.sub(r"<style[\s\S]*?</style>", "", s, flags=re.I)
    s = re.sub(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>[\s\S]*?</a>", r"\1", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html_lib.unescape(s)
    s = s.replace("\\--", "--").replace("\u00a0", " ").replace("&nbsp;", " ")
    return re.sub(r"\s+", " ", s).strip()


def row_from_cells(cells: List[str]) -> Optional[Dict[str, Any]]:
    cells = [clean_cell(c) for c in cells]
    cells = [c if c else "--" for c in cells]
    # 有些渲染结果会多出一个全空占位行，或者首列为空。
    if cells and cells[0] in {"", "--"} and len(cells) > 1 and re.fullmatch(r"\d+", cells[1] or ""):
        cells = cells[1:]
    if len(cells) < 12:
        return None
    if not re.fullmatch(r"\d+", cells[0] or ""):
        return None
    if not re.fullmatch(r"\d{6}", cells[1] or ""):
        return None
    vals = (cells + [""] * len(HEADERS))[: len(HEADERS)]
    return dict(zip(HEADERS, vals))


def parse_html_table(page_html: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", page_html, flags=re.I):
        cells_raw = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr, flags=re.I)
        row = row_from_cells(cells_raw)
        if row:
            rows.append(row)
    return rows


def parse_pipe_text(text: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("|") or not line.endswith("|"):
            continue
        if "---" in line:
            continue
        cells = [c.strip().replace("\\.", ".").replace("\\--", "--") for c in line.strip("|").split("|")]
        row = row_from_cells(cells)
        if row:
            rows.append(row)
    return rows


def html_to_loose_text(page: str) -> str:
    text = page
    text = re.sub(r"<script[\s\S]*?</script>", "\n", text, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", "\n", text, flags=re.I)
    # 保留单元格/块边界。React DOM 通常不是 table，但 div/span 边界足够恢复文本 token。
    text = re.sub(r"</(td|th|span|p|li|a|button)>", " | ", text, flags=re.I)
    text = re.sub(r"</(tr|div|section|ul|ol|table|tbody|thead)>", "\n", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>", r" \1 ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    text = text.replace("\\--", "--").replace("\u00a0", " ").replace("&nbsp;", " ")
    return re.sub(r"[ \t\r\f\v]+", " ", text)


def parse_loose_text(text: str) -> List[Dict[str, Any]]:
    """Parse rows from whitespace/pipe text, e.g. '1 301583 托伦斯 4636.84 ...'."""
    rows: List[Dict[str, Any]] = []
    # 先按行试；如果整页被压成一行，再用 serial+code 边界切分。
    candidates: List[str] = []
    for line in text.splitlines():
        line = line.strip(" |\t")
        if re.search(r"(^|\s)\d{1,4}\s+\d{6}\s+", line):
            candidates.append(line)
    flat = re.sub(r"\s+", " ", text).strip()
    starts = list(re.finditer(r"(?<!\d)(\d{1,4})\s+(\d{6})\s+", flat))
    for i, m in enumerate(starts):
        end = starts[i + 1].start() if i + 1 < len(starts) else min(len(flat), m.start() + 1500)
        seg = flat[m.start():end].strip(" |")
        candidates.append(seg)

    seen = set()
    for seg in candidates:
        parts = [p.strip(" |") for p in re.split(r"\s+|\s*\|\s*", seg) if p.strip(" |")]
        if len(parts) < 12:
            continue
        # 找 serial+code 起点，前面可能混有表头文字。
        start = None
        for i in range(0, min(8, len(parts) - 2)):
            if re.fullmatch(r"\d{1,4}", parts[i]) and re.fullmatch(r"\d{6}", parts[i + 1]):
                start = i
                break
        if start is None:
            continue
        parts = parts[start:]
        row = row_from_cells(parts[: len(HEADERS)])
        if not row:
            continue
        key = row.get("代码")
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def parse_next_data(page: str) -> List[Dict[str, Any]]:
    """If Next.js embeds row data, recover it. Generic and 9fzt-only."""
    m = re.search(r"<script[^>]+id=['\"]__NEXT_DATA__['\"][^>]*>([\s\S]*?)</script>", page, flags=re.I)
    if not m:
        return []
    try:
        data = json.loads(html_lib.unescape(m.group(1)))
    except Exception:
        return []
    rows: List[Dict[str, Any]] = []
    seen = set()

    def maybe_row(obj: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(obj, dict):
            return None
        blob = json.dumps(obj, ensure_ascii=False)
        if not re.search(r"\b\d{6}\b", blob):
            return None
        # 常见中英文字段兜底映射。
        code = obj.get("代码") or obj.get("SECURITY_CODE") or obj.get("APPLY_CODE") or obj.get("stockCode") or obj.get("code")
        name = obj.get("股票名称") or obj.get("SECURITY_NAME") or obj.get("SECURITY_NAME_ABBR") or obj.get("stockName") or obj.get("name")
        if not code or not re.fullmatch(r"\d{6}", str(code)):
            return None
        r = {h: "" for h in HEADERS}
        r["序号"] = str(len(rows) + 1)
        r["代码"] = str(code)
        r["股票名称"] = str(name or "")
        mapping = {
            "发行总数(万股)": ["发行总数(万股)", "ISSUE_NUM", "TOTAL_ISSUE_NUM"],
            "网上发行(万股)": ["网上发行(万股)", "ONLINE_ISSUE_NUM"],
            "申购上限(万股)": ["申购上限(万股)", "ONLINE_APPLY_UPPER", "TOP_APPLY_MARKETCAP"],
            "发行价(元)": ["发行价(元)", "ISSUE_PRICE", "ONLINE_APPLY_PRICE"],
            "首日收盘价(元)": ["首日收盘价(元)", "CLOSE_PRICE"],
            "申购日期": ["申购日期", "APPLY_DATE", "ONLINE_ISSUE_DATE"],
            "中签率公告日": ["中签率公告日", "BALLOT_NUM_DATE", "ASSIGN_DATE"],
            "网上申购缴款日期": ["网上申购缴款日期", "BALLOT_PAY_DATE", "ONLINE_PAY_DATE", "START_DATE"],
            "上市日期": ["上市日期", "LISTING_DATE", "OPEN_DATE", "SELECT_LISTING_DATE"],
            "筹集资金(万元)": ["筹集资金(万元)", "TOTAL_RAISE_FUNDS"],
            "实际筹集资金": ["实际筹集资金", "NET_RAISE_FUNDS"],
            "发行市盈率": ["发行市盈率", "AFTER_ISSUE_PE", "PREDICT_PE_THREE"],
            "行业市盈率": ["行业市盈率", "INDUSTRY_PE", "INDUSTRY_PE_RATIO"],
            "中签率": ["中签率", "BALLOT_NUM"],
            "询价累积报价倍数": ["询价累积报价倍数", "OFFLINE_VAS_MULTIPLE"],
            "有效报价配售对象家数": ["有效报价配售对象家数", "OFFLINE_VAP_OBJECT"],
            "招股书": ["招股书"],
        }
        for out_k, keys in mapping.items():
            for k in keys:
                if obj.get(k) not in (None, ""):
                    r[out_k] = str(obj.get(k))
                    break
        return r

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            r = maybe_row(x)
            if r and r["代码"] not in seen:
                seen.add(r["代码"]); rows.append(r)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(data)
    return rows


def parse_stock_apply_page(page: str) -> List[Dict[str, Any]]:
    parsers = [
        ("html_table", lambda p: parse_html_table(p)),
        ("pipe_raw", lambda p: parse_pipe_text(html_lib.unescape(p))),
        ("next_data", lambda p: parse_next_data(p)),
        ("loose_text", lambda p: parse_loose_text(html_to_loose_text(p))),
    ]
    for _name, fn in parsers:
        rows = fn(page)
        if rows:
            return rows
    return []


def fetch_and_parse_9fzt(url: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    debug: Dict[str, Any] = {"source_url": url, "stages": []}
    raw = fetch_html(url)
    for stage_name, page in [("direct_http", raw)]:
        rows = parse_stock_apply_page(page)
        debug["stages"].append({"stage": stage_name, "page_len": len(page), "rows": len(rows), "tr_count": len(re.findall(r"<tr[^>]*>", page, re.I)), "code_count": len(re.findall(r"\b\d{6}\b", page)), "has_next_data": "__NEXT_DATA__" in page, "snippet": page[:800]})
        if rows:
            return rows, debug, page
    rendered, rdebug = render_with_chromium(url)
    debug["stages"].append({"stage": "headless_render", **rdebug})
    if rendered:
        rows = parse_stock_apply_page(rendered)
        debug["stages"].append({"stage": "parse_rendered", "page_len": len(rendered), "rows": len(rows), "tr_count": len(re.findall(r"<tr[^>]*>", rendered, re.I)), "code_count": len(re.findall(r"\b\d{6}\b", rendered)), "has_next_data": "__NEXT_DATA__" in rendered, "snippet": rendered[:800]})
        if rows:
            return rows, debug, rendered
    return [], debug, rendered or raw


def normalize_md(mmdd: str, year: int) -> Optional[str]:
    s = (mmdd or "").strip()
    if not s or s in {"--", "-", "—", "None", "null"}:
        return None
    m = re.search(r"(?:(\d{4})[-/年])?(\d{1,2})[-/月](\d{1,2})", s)
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
    for p in [report_json, latest_json]:
        p.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    md_lines = [f"# 新股日历 {target_date}", "", f"- 生成: {rec['generated_at']}", f"- 抓取行数: {len(rows)} ｜匹配事件: {len(events)}", f"- 飞书发送: {send_result}", "", "## 消息正文", "", "```", msg, "```"]
    for p in [report_md, latest_md]:
        p.write_text("\n".join(md_lines), encoding="utf-8")
    return {"raw": str(raw_path), "events": str(events_path), "report_json": str(report_json), "report_md": str(report_md)}


def write_failure_debug(page: str, parse_debug: Dict[str, Any]) -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    DXX_IPO_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    for base in [AUDIT_DIR, DXX_IPO_DEBUG_DIR]:
        (base / "latest_ipo_calendar_parse_failed.html").write_text(page, encoding="utf-8")
        (base / "latest_ipo_calendar_parse_failed.json").write_text(json.dumps(parse_debug, ensure_ascii=False, indent=2), encoding="utf-8")


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

    rows, parse_debug, page = fetch_and_parse_9fzt(args.url)
    if not rows:
        write_failure_debug(page, parse_debug)
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
