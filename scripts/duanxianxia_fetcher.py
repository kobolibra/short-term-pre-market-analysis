#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import requests
from requests.exceptions import ReadTimeout
from bs4 import BeautifulSoup
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from playwright.sync_api import sync_playwright

BASE = "https://duanxianxia.com"
X_BASE = "https://x.duanxianxia.cn"
TIMEOUT = 20
CASHFLOW_BASE = "https://stock.9fzt.com"
CASHFLOW_DEFAULT_LIMIT = 150
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

WORKSPACE_ROOT = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE_ROOT / "projects" / "duanxianxia"
CAPTURE_ROOT = PROJECT_ROOT / "captures"
TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")

JJLIVE_AES_KEY = b"secretkey322yes!!aaaaaaaaaaaaaaa"
JJLIVE_AES_IV = b"fixediv_16valued"

DATASET_REGISTRY: Dict[str, Dict[str, str]] = {
    "rocket": {
        "id": "rank.rocket",
        "label": "飙升榜",
        "path": "搜索/榜单",
    },
    "hot": {
        "id": "pool.hot",
        "label": "热门",
        "path": "股票池/热门",
    },
    "surge": {
        "id": "pool.surge",
        "label": "冲涨",
        "path": "股票池/冲涨",
    },
    "hotlist_day": {
        "id": "rank.hot_stock_day",
        "label": "热度榜（日）",
        "path": "热度榜/日",
    },
    "review_daily": {
        "id": "review.daily.top_metrics",
        "label": "每日复盘顶部指标",
        "path": "复盘/每日复盘",
    },
    "review_daily_core11": {
        "id": "review.daily.top_metrics.core11",
        "label": "每日复盘顶部指标（11项，不含量能）",
        "path": "复盘/每日复盘/11项参考",
    },
    "review_ltgd_range": {
        "id": "review.ltgd.range",
        "label": "龙头高度区间涨幅",
        "path": "复盘/龙头高度/区间涨幅",
    },
    "review_plate": {
        "id": "review.fupan.plate",
        "label": "涨停复盘（按概念）",
        "path": "复盘/涨停复盘（按概念）",
    },
    "home_qxlive_plate_summary": {
        "id": "home.kaipan.plate.summary",
        "label": "主页板块强度全主标签汇总表",
        "path": "主页/qxlive/全主标签/主标签字段+子标签列表",
    },
    "home_qxlive_top_metrics": {
        "id": "home.qxlive.top_metrics",
        "label": "主页 qxlive 顶部指标按钮组",
        "path": "主页/qxlive/顶部指标按钮组",
    },
    "home_ztpool": {
        "id": "home.ztpool",
        "label": "主页涨停股票池",
        "path": "主页/涨停股票池",
    },
    "auction_vratio": {
        "id": "auction.jjyd.vratio",
        "label": "竞价异动/竞价爆量",
        "path": "竞价/竞价异动/竞价爆量",
    },
    "auction_qiangchou": {
        "id": "auction.jjyd.qiangchou",
        "label": "竞价异动/竞价抢筹",
        "path": "竞价/竞价异动/竞价抢筹",
    },
    "auction_net_amount": {
        "id": "auction.jjyd.net_amount",
        "label": "竞价异动/竞价净额",
        "path": "竞价/竞价异动/竞价净额",
    },
    "auction_fengdan": {
        "id": "auction.jjlive.fengdan",
        "label": "竞价封单/当日封单表",
        "path": "竞价/竞价封单",
    },
    "cashflow_today": {
        "id": "cashflow.stock.today",
        "label": "个股资金流向/今日排行",
        "path": "资金流向/个股资金流向/今日排行",
    },
    "cashflow_3d": {
        "id": "cashflow.stock.3day",
        "label": "个股资金流向/3日排行",
        "path": "资金流向/个股资金流向/3日排行",
    },
    "cashflow_5d": {
        "id": "cashflow.stock.5day",
        "label": "个股资金流向/5日排行",
        "path": "资金流向/个股资金流向/5日排行",
    },
    "cashflow_10d": {
        "id": "cashflow.stock.10day",
        "label": "个股资金流向/10日排行",
        "path": "资金流向/个股资金流向/10日排行",
    },
}

REVIEW_METRIC_DEFS = [
    ("QX", "情绪指标"),
    ("ZT", "涨停家数"),
    ("DT", "跌停家数"),
    ("KQXY", "亏钱效应"),
    ("HSLN", "量能"),
    ("LBGD", "连板高度"),
    ("SZ", "上涨家数"),
    ("XD", "下跌家数"),
    ("PB", "今日封板率"),
    ("ZTBX", "昨涨停表现"),
    ("LBBX", "昨连板表现"),
    ("PBBX", "连板晋级率"),
]

REVIEW_CORE11_EXCLUDED_KEYS = {
    "HSLN",
    "PBBX_TOP",
    "PBBX_1_2",
    "PBBX_2_3",
    "PBBX_3_4",
    "PBBX_4P",
}

QXLIVE_TOP_METRIC_DEFS = [
    {"order": 1, "metric_key": "QX", "metric_label": "情绪指标", "button_id": "QX_btn", "source_series": "QX", "value_type": "number"},
    {"order": 2, "metric_key": "ZT", "metric_label": "涨停家数", "button_id": "ZT_btn", "source_series": "ZT", "value_type": "number"},
    {"order": 3, "metric_key": "DT", "metric_label": "跌停家数", "button_id": "DT_btn", "source_series": "DT", "value_type": "number"},
    {"order": 4, "metric_key": "KQXY", "metric_label": "亏钱效应", "button_id": "KQXY_btn", "source_series": "KQXY", "value_type": "number"},
    {"order": 5, "metric_key": "HSLN", "metric_label": "主力流入", "button_id": "HSLN_btn", "source_series": "HSLN", "value_type": "signed"},
    {"order": 6, "metric_key": "LBGD", "metric_label": "连板高度", "button_id": "LBGD_btn", "source_series": "LBGD", "value_type": "number"},
    {"order": 7, "metric_key": "SZ", "metric_label": "上涨家数", "button_id": "SZ_btn", "source_series": "SZ", "value_type": "number"},
    {"order": 8, "metric_key": "XD", "metric_label": "下跌家数", "button_id": "XD_btn", "source_series": "XD", "value_type": "number"},
    {"order": 9, "metric_key": "PB", "metric_label": "今日封板率", "button_id": "PB_btn", "source_series": "PB", "value_type": "percent"},
    {"order": 10, "metric_key": "ZTBX", "metric_label": "昨涨停表现", "button_id": "ZTBX_btn", "source_series": "ZTBX", "value_type": "percent"},
    {"order": 11, "metric_key": "LBBX", "metric_label": "昨连板表现", "button_id": "LBBX_btn", "source_series": "LBBX", "value_type": "percent"},
    {
        "order": 12,
        "metric_key": "PBBX",
        "metric_label": "沪深5分钟量能",
        "button_id": "PBBX_btn",
        "source_series": "JRLN",
        "display_series": "PBBX",
        "compare_series": "ZRLN",
        "value_type": "number",
    },
]

LTGD_RANGE_WINDOWS = [5, 10, 20, 50]
LTGD_BOARD_ORDER = ["主板", "创业科创板", "北交所"]
REVIEW_PLATE_FINE_TAGS = [
    "数据中心",
    "液冷服务器",
    "算力租赁",
    "GPUNAS",
    "GPU",
    "NAS",
    "英伟达概念",
    "训推一体机",
    "东数西算",
    "云计算",
    "算力调度",
    "国资云",
    "液冷",
    "算力",
    "服务器",
]


@dataclass
class FetchResult:
    kind: str
    rows: List[Dict[str, Any]]
    meta: Dict[str, Any]


class DuanxianxiaFetcher:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": UA,
                "Referer": BASE,
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

    def _get_json(self, url: str) -> Any:
        resp = self.session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def _get_text(self, url: str) -> str:
        resp = self.session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def _post_json(self, url: str, data: Dict[str, Any] | None = None) -> Any:
        last_exc: Exception | None = None
        for attempt in range(2):
            try:
                resp = self.session.post(url, data=data or {}, timeout=TIMEOUT)
                resp.raise_for_status()
                return resp.json()
            except ReadTimeout as exc:
                last_exc = exc
                if attempt == 0:
                    time.sleep(1.0)
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"POST failed without response: {url}")

    def _post_json_via_browser(self, page_url: str, endpoint: str, data: Dict[str, Any] | None = None) -> Any:
        data = data or {}
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                executable_path='/usr/bin/google-chrome',
                args=['--no-sandbox', '--disable-dev-shm-usage'],
            )
            page = browser.new_page()
            try:
                page.goto(page_url, wait_until='networkidle', timeout=60000)
                result = page.evaluate(
                    """async ({ endpoint, payload }) => {
                      const r = await fetch(endpoint, {
                        method: 'POST',
                        credentials: 'include',
                        headers: {
                          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                        },
                        body: new URLSearchParams(payload).toString(),
                      });
                      const text = await r.text();
                      return { ok: r.ok, status: r.status, text };
                    }""",
                    {"endpoint": endpoint, "payload": data},
                )
                if not result.get('ok'):
                    raise RuntimeError(
                        f"browser fallback failed for {endpoint}: status={result.get('status')} body={str(result.get('text', ''))[:500]}"
                    )
                return json.loads(result['text'])
            finally:
                browser.close()

    def _fetch_home_ztpool_snapshot(self, page_url: str) -> Dict[str, Any]:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                executable_path='/usr/bin/google-chrome',
                args=['--no-sandbox', '--disable-dev-shm-usage'],
            )
            page = browser.new_page()
            try:
                page.goto(page_url, wait_until='networkidle', timeout=60000)
                result = page.evaluate(
                    """async () => {
                        const resp = await fetch('/vendor/stockdata/jinjidata.json', { credentials: 'include' });
                        const text = await resp.text();
                        const dateText = (document.querySelector('#jinjidate')?.innerText || '').trim();
                        return {
                            date_text: dateText,
                            title_text: `${dateText}涨停股票池`.trim(),
                            payload_text: text,
                        };
                    }"""
                )
                if not isinstance(result, dict):
                    raise RuntimeError('invalid home ztpool snapshot')
                payload_text = str(result.get('payload_text', '') or '').strip()
                if not payload_text:
                    raise RuntimeError('home ztpool payload missing')
                payload = json.loads(payload_text)
                if not isinstance(payload, dict):
                    raise RuntimeError('home ztpool payload is not json object')
                return {
                    'date_text': result.get('date_text', ''),
                    'title_text': result.get('title_text', ''),
                    'payload': payload,
                }
            finally:
                browser.close()

    @staticmethod
    def _normalize_ztpool_trade_date(value: Any) -> str:
        text = str(value or '').strip().replace('【', '').replace('】', '')
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
            return text
        if re.fullmatch(r'\d{8}', text):
            return f'{text[:4]}-{text[4:6]}-{text[6:]}'
        return datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d')

    @staticmethod
    def _normalize_ztpool_html(html: Any) -> str:
        text = str(html or '')
        replacements = {
            '<@>': "<div class='jjgn'>",
            "<#'": "<span class='kline' code='",
            '<Aa>': "<span class='change'>",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text

    @staticmethod
    def _parse_ztpool_rate_text(text: Any) -> Dict[str, Any]:
        raw = str(text or '').strip()
        match = re.search(r'(\d+)\s*/\s*(\d+)\s*=\s*([+-]?\d+(?:\.\d+)?)%?', raw)
        if not match:
            return {
                '晋级率文本': raw,
                '晋级数': '',
                '样本数': '',
                '晋级率': raw,
            }
        rate_value = match.group(3)
        if '.' in rate_value:
            rate_display = f'{float(rate_value):.2f}'.rstrip('0').rstrip('.')
        else:
            rate_display = rate_value
        return {
            '晋级率文本': raw,
            '晋级数': int(match.group(1)),
            '样本数': int(match.group(2)),
            '晋级率': f'{rate_display}%',
        }

    def fetch_home_ztpool(self) -> FetchResult:
        page_url = BASE
        snapshot = self._fetch_home_ztpool_snapshot(page_url)
        payload = snapshot.get('payload', {}) if isinstance(snapshot, dict) else {}
        raw_html = payload.get('html', '') if isinstance(payload, dict) else ''
        normalized_html = self._normalize_ztpool_html(raw_html)
        soup = BeautifulSoup(f'<table><tbody>{normalized_html}</tbody></table>', 'html.parser')

        trade_date = self._normalize_ztpool_trade_date(snapshot.get('date_text', ''))
        rows: List[Dict[str, Any]] = []
        group_summaries: List[Dict[str, Any]] = []

        for group_index, tr in enumerate(soup.select('tr'), start=1):
            cells = tr.find_all('td', recursive=False)
            if len(cells) < 3:
                continue
            group_name = cells[0].get_text(strip=True)
            rate_text = cells[1].get_text(strip=True)
            rate_info = self._parse_ztpool_rate_text(rate_text)
            stock_nodes = cells[2].select('.jjgn')

            group_summary = {
                '分组序号': group_index,
                '分组名称': group_name,
                '晋级率文本': rate_info['晋级率文本'],
                '晋级数': rate_info['晋级数'],
                '样本数': rate_info['样本数'],
                '晋级率': rate_info['晋级率'],
                '股票数': len(stock_nodes),
            }
            group_summaries.append(group_summary)

            for stock_index, node in enumerate(stock_nodes, start=1):
                stock_name_node = node.select_one('.kline')
                status_node = node.find(['b', 'i'], class_=re.compile(r'^(success|zha|fail)$'))
                topic_node = node.find('u')
                change_node = None
                for span in node.find_all('span'):
                    classes = span.get('class') or []
                    if 'kline' in classes:
                        continue
                    change_node = span
                    break

                market = ''
                for content in node.contents:
                    if isinstance(content, str):
                        market = content.strip()
                        if market:
                            break
                market = market.split()[0] if market else ''

                rows.append(
                    {
                        '日期': trade_date,
                        '分组序号': group_index,
                        '分组名称': group_name,
                        '组内序号': stock_index,
                        '晋级率文本': rate_info['晋级率文本'],
                        '晋级数': rate_info['晋级数'],
                        '样本数': rate_info['样本数'],
                        '晋级率': rate_info['晋级率'],
                        '市场': market,
                        '代码': str(stock_name_node.get('code', '') or '').strip() if stock_name_node else '',
                        '名称': stock_name_node.get_text(strip=True) if stock_name_node else '',
                        '状态': status_node.get_text(strip=True) if status_node else '',
                        '状态样式': (status_node.get('class') or [''])[0] if status_node else '',
                        '涨幅': change_node.get_text(strip=True) if change_node else '',
                        '题材': topic_node.get_text(strip=True) if topic_node else '',
                    }
                )

        return FetchResult(
            kind='home_ztpool',
            rows=rows,
            meta={
                'source': f'{page_url} + /vendor/stockdata/jinjidata.json',
                'field': 'jinjidata.html',
                'count': len(rows),
                'date': trade_date,
                'title': str(snapshot.get('title_text', '') or '').strip(),
                'group_count': len(group_summaries),
                'groups': group_summaries,
                'complete': True,
            },
        )

    def fetch_rocket(self) -> FetchResult:
        data = self._get_json(f"{X_BASE}/vendor/stockdata/hotlist.json")
        items = data.get("skyrocket_hour", [])
        rows = [
            {
                "rank": idx + 1,
                "code": str(item.get("code", "")),
                "name": item.get("name", ""),
                "value": "+" + self._format_hot_rate(item.get("rate")),
                "raw_rate": item.get("rate"),
            }
            for idx, item in enumerate(items)
        ]
        return FetchResult(
            kind="rocket",
            rows=rows,
            meta={
                "source": f"{X_BASE}/vendor/stockdata/hotlist.json",
                "field": "skyrocket_hour",
                "count": len(rows),
            },
        )

    def fetch_hot(self, sort: str = "") -> FetchResult:
        data = self._post_json(f"{BASE}/data/getFxPoolData/{sort}")
        items = data.get("list", []) or []
        rows: List[Dict[str, Any]] = []
        for idx, item in enumerate(items, start=1):
            concept_1, concept_2 = self._split_concepts(item[6] if len(item) > 6 else "")
            amount = self._format_amount(item[8] if len(item) > 8 else None, digits=1)
            float_cap = self._format_amount(item[9] if len(item) > 9 else None, digits=0)
            main_force_signed = ""
            if len(item) > 10 and item[10] is not None:
                try:
                    v = float(item[10])
                    if v > 0:
                        main_force_signed = "+" + self._format_amount(abs(v), digits=1)
                    elif v < 0:
                        main_force_signed = "-" + self._format_amount(abs(v), digits=1)
                    else:
                        main_force_signed = self._format_amount(0, digits=1)
                except Exception:
                    main_force_signed = str(item[10])
            rows.append(
                {
                    "代码": str(item[0]) if len(item) > 0 else "",
                    "名称": item[1] if len(item) > 1 else "",
                    "涨幅": f"{item[2]}%" if len(item) > 2 and item[2] is not None else "",
                    "主力": main_force_signed,
                    "实际换手": f"{item[11]}%" if len(item) > 11 and item[11] is not None else "",
                    "成交": amount,
                    "流通": float_cap,
                    "概念": "+".join([x for x in [concept_1, concept_2] if x]),
                }
            )
        return FetchResult(
            kind="hot",
            rows=rows,
            meta={
                "source": f"{BASE}/data/getFxPoolData/{sort}",
                "field": "list",
                "count": len(rows),
                "sort": sort,
            },
        )

    def fetch_hotlist_day(self) -> FetchResult:
        data = self._get_json(f"{X_BASE}/vendor/stockdata/hotlist.json")
        items = data.get("hot_stock_day", []) or []
        rows = [
            {
                "rank": idx + 1,
                "code": str(item.get("code", "")),
                "name": item.get("name", ""),
                "value": self._format_hot_rate(item.get("rate")),
                "raw_rate": item.get("rate"),
            }
            for idx, item in enumerate(items)
        ]
        return FetchResult(
            kind="hotlist_day",
            rows=rows,
            meta={
                "source": f"{X_BASE}/vendor/stockdata/hotlist.json",
                "field": "hot_stock_day",
                "count": len(rows),
            },
        )

    def fetch_surge(self, sort: str = "") -> FetchResult:
        data = self._post_json(f"{BASE}/data/getCzPoolData/{sort}")
        items = data.get("list", []) or []
        rows: List[Dict[str, Any]] = []
        for idx, item in enumerate(items, start=1):
            concept_1, concept_2 = self._split_concepts(item[6] if len(item) > 6 else "")
            amount = self._format_amount(item[8] if len(item) > 8 else None, digits=2)
            float_cap = self._format_amount(item[9] if len(item) > 9 else None, digits=0)
            turnover_ratio = ""
            if len(item) > 9 and item[9]:
                try:
                    turnover_ratio = f"{(float(item[8]) / float(item[9]) * 100):.2f}%"
                except Exception:
                    turnover_ratio = ""

            rows.append(
                {
                    "rank": idx,
                    "code": str(item[0]) if len(item) > 0 else "",
                    "name": item[1] if len(item) > 1 else "",
                    "change_pct": f"{item[2]}%" if len(item) > 2 and item[2] is not None else "",
                    "turnover_ratio": turnover_ratio,
                    "amount": amount,
                    "float_market_cap": float_cap,
                    "concept": "+".join([x for x in [concept_1, concept_2] if x]),
                    "concept_1": concept_1,
                    "concept_2": concept_2,
                    "raw": item,
                }
            )

        return FetchResult(
            kind="surge",
            rows=rows,
            meta={
                "source": f"{BASE}/data/getCzPoolData/{sort}",
                "field": "list",
                "count": len(rows),
                "sort": sort,
            },
        )

    def fetch_review_daily(self, date: str = "") -> FetchResult:
        chart_data = self._post_json(f"{BASE}/api/getChartByQingxu")
        aaxis = chart_data.get("Aaxis", []) or []
        if not aaxis:
            raise RuntimeError("review daily chart data missing Aaxis")

        target_date = date.strip() if date else ""
        if target_date:
            normalized = self._post_json(
                f"{BASE}/api/getFupanDate",
                {"date": target_date, "type": "choose"},
            )
            target_date = normalized.get("date", target_date)
        else:
            target_date = str(aaxis[-1])

        try:
            idx = aaxis.index(target_date)
        except ValueError as exc:
            raise RuntimeError(f"target review date not found in series: {target_date}") from exc

        series = chart_data.get("series", {}) or {}
        rows: List[Dict[str, Any]] = []
        raw_pbbx = ""
        for order, (key, label) in enumerate(REVIEW_METRIC_DEFS, start=1):
            values = series.get(key, []) or []
            raw_value = values[idx] if idx < len(values) else ""
            if key == "PBBX":
                raw_pbbx = str(raw_value) if raw_value is not None else ""
                pbbx_summary, pbbx_layers = self._build_review_jinji_rows(series, target_date, order, raw_pbbx)
                rows.extend([pbbx_summary, *pbbx_layers])
                continue
            rows.append(
                {
                    "order": order,
                    "metric_key": key,
                    "metric_label": label,
                    "date": target_date,
                    "value": raw_value,
                }
            )

        return FetchResult(
            kind="review_daily",
            rows=rows,
            meta={
                "source": f"{BASE}/api/getChartByQingxu",
                "field": "series",
                "count": len(rows),
                "date": target_date,
                "raw_pbbx": raw_pbbx,
                "series_length": len(aaxis),
                "available_series_keys": sorted(series.keys()),
            },
        )

    def fetch_review_daily_core11(self, date: str = "") -> FetchResult:
        base_result = self.fetch_review_daily(date=date)
        rows = [
            row
            for row in base_result.rows
            if str(row.get("metric_key") or "").strip() not in REVIEW_CORE11_EXCLUDED_KEYS
        ]
        meta = dict(base_result.meta)
        meta.update(
            {
                "derived_from": "review_daily",
                "excluded_metric_keys": sorted(REVIEW_CORE11_EXCLUDED_KEYS),
                "count": len(rows),
            }
        )
        return FetchResult(
            kind="review_daily_core11",
            rows=rows,
            meta=meta,
        )

    def fetch_home_qxlive_top_metrics(self) -> FetchResult:
        page_url = f"{BASE}/web/qxlive"
        snapshot = self._fetch_qxlive_top_metrics_snapshot(page_url)
        qxlive = snapshot.get("qxlive", {}) if isinstance(snapshot, dict) else {}
        qxlast = snapshot.get("qxlast", {}) if isinstance(snapshot, dict) else {}
        buttons = snapshot.get("buttons", {}) if isinstance(snapshot, dict) else {}

        aaxis = qxlive.get("Aaxis", []) or []
        if not aaxis:
            raise RuntimeError("qxlive top metrics missing Aaxis")

        time_point_raw = aaxis[-1]
        time_point = self._format_qxlive_axis(time_point_raw)
        trade_date = self._normalize_qxlive_trade_date(qxlive.get("from"))
        series = qxlive.get("series", {}) or {}

        rows: List[Dict[str, Any]] = []
        for spec in QXLIVE_TOP_METRIC_DEFS:
            metric_key = spec["metric_key"]
            source_series_key = spec["source_series"]
            display_series_key = spec.get("display_series", metric_key)
            compare_series_key = spec.get("compare_series", "")
            button_text = str(buttons.get(spec["button_id"], "") or "").strip()
            button_display_value = self._extract_button_display_value(button_text)

            if metric_key == "PBBX":
                current_raw = self._series_last_value(qxlast.get(display_series_key, []))
                compare_raw = self._series_last_value(qxlast.get(compare_series_key, [])) if compare_series_key else ""
                display_raw = self._series_last_value(series.get(source_series_key, []))
            else:
                current_raw = button_display_value
                compare_raw = ""
                display_raw = self._series_last_value(series.get(source_series_key, []))
                if current_raw in (None, ""):
                    current_raw = display_raw

            rows.append(
                {
                    "order": spec["order"],
                    "metric_key": metric_key,
                    "metric_label": spec["metric_label"],
                    "date": trade_date,
                    "time_point": time_point,
                    "value": self._format_qxlive_metric_value(current_raw, spec["value_type"]),
                    "button_display_value": button_display_value,
                    "chart_tail_value": self._format_qxlive_metric_value(display_raw, spec["value_type"]),
                    "compare_value": self._format_qxlive_metric_value(compare_raw, spec["value_type"]),
                    "source_series": source_series_key,
                    "display_series": display_series_key,
                    "compare_series": compare_series_key,
                    "button_id": spec["button_id"],
                    "button_text": button_text,
                    "raw_value": current_raw,
                    "raw_chart_tail_value": display_raw,
                    "raw_compare_value": compare_raw,
                }
            )

        return FetchResult(
            kind="home_qxlive_top_metrics",
            rows=rows,
            meta={
                "source": page_url,
                "field": "/vendor/stockdata/platechart1.json(qxlive.series) + /api/getLastQxlive(qxlast) + button.chart",
                "count": len(rows),
                "date": trade_date,
                "time_point": time_point,
                "time_point_raw": time_point_raw,
                "button_count": len(buttons),
                "available_series_keys": sorted(series.keys()),
                "available_last_keys": sorted(qxlast.keys()) if isinstance(qxlast, dict) else [],
                "mapping_notes": {
                    "PBBX": "主页按钮 PBBX_btn 对应沪深5分钟量能，按钮显示值取 qxlast.PBBX，比对序列为 qxlive.series.JRLN 与 qxlast.ZRLN。"
                },
            },
        )

    def _build_review_jinji_rows(
        self,
        series: Dict[str, Any],
        target_date: str,
        order: int,
        raw_pbbx: str,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        jinji_series = series.get("jinji") or {}
        jinji_series_item = jinji_series.get(target_date, {}) if isinstance(jinji_series, dict) else {}
        jinji_data = series.get("jinji_data") or {}
        jinji_data_item = jinji_data.get(target_date, {}) if isinstance(jinji_data, dict) else {}
        if isinstance(jinji_data_item, str):
            try:
                jinji_data_item = json.loads(jinji_data_item)
            except Exception:
                jinji_data_item = {}
        if not isinstance(jinji_data_item, dict):
            jinji_data_item = {}

        summary_bucket = self._review_jinji_bucket(jinji_data_item.get("lb"), fallback_rate=jinji_series_item.get("val2"))
        summary_row = {
            "order": order,
            "metric_key": "PBBX",
            "metric_label": "连板晋级率",
            "date": target_date,
            "value": self._format_review_percent(summary_bucket.get("rate")),
            "metric_group": "连板晋级率",
            "metric_category": "总体",
            "display_label": "连板总体晋级率",
            "display_rate": self._format_review_percent(summary_bucket.get("rate")),
            "raw_value": raw_pbbx,
            "ratio": self._format_review_ratio(summary_bucket.get("jinji"), summary_bucket.get("all")),
            "jinji_count": summary_bucket.get("jinji"),
            "sample_count": summary_bucket.get("all"),
        }

        layer_specs = [
            ("PBBX_TOP", "最高板晋级率", "top"),
            ("PBBX_1_2", "1进2晋级率", "1"),
            ("PBBX_2_3", "2进3晋级率", "2"),
            ("PBBX_3_4", "3进4晋级率", "3"),
            ("PBBX_4P", "4板+晋级率", "other"),
        ]
        layer_rows: List[Dict[str, Any]] = []
        for offset, (metric_key, metric_label, bucket_key) in enumerate(layer_specs, start=1):
            bucket = self._review_jinji_bucket(jinji_data_item.get(bucket_key))
            layer_rows.append(
                {
                    "order": order + offset / 10,
                    "metric_key": metric_key,
                    "metric_label": metric_label,
                    "date": target_date,
                    "value": self._format_review_percent(bucket.get("rate")),
                    "metric_group": "连板晋级率",
                    "metric_category": "分层",
                    "display_label": metric_label,
                    "display_rate": self._format_review_percent(bucket.get("rate")),
                    "ratio": self._format_review_ratio(bucket.get("jinji"), bucket.get("all")),
                    "jinji_count": bucket.get("jinji"),
                    "sample_count": bucket.get("all"),
                }
            )

        return summary_row, layer_rows

    def fetch_review_ltgd_range(self, range_expr: str = "") -> FetchResult:
        dates_data = self._post_json(f"{BASE}/api/getDatesByLongtou")
        full_dates = dates_data.get("dates", []) or dates_data.get("Bdate", []) or []
        aaxis = dates_data.get("Aaxis", []) or []
        if not aaxis:
            raise RuntimeError("ltgd date axis missing")

        latest_date = str(full_dates[-1] if full_dates else aaxis[-1])
        if range_expr.strip():
            requested_windows = [0]
            explicit_ranges = {0: range_expr.strip()}
        else:
            requested_windows = LTGD_RANGE_WINDOWS
            explicit_ranges = {}

        rows: List[Dict[str, Any]] = []
        section_summaries: List[Dict[str, Any]] = []

        for window in requested_windows:
            if window == 0:
                final_range = explicit_ranges[0]
                window_label = "自定义"
            else:
                base_dates = full_dates if full_dates else aaxis
                start_idx = max(0, len(base_dates) - window - 1)
                start = str(base_dates[start_idx])
                end = latest_date
                final_range = f"{start} - {end}"
                window_label = f"{window}日"

            html_payload = self._post_json(f"{BASE}/api/getZfByDate", {"date": final_range})
            html = html_payload.get("html", "") if isinstance(html_payload, dict) else str(html_payload)
            if not html:
                raise RuntimeError(f"ltgd range response missing html for {final_range}")

            soup = BeautifulSoup(f"<table><tbody>{html}</tbody></table>", "lxml")
            parsed_rows: List[Dict[str, Any]] = []
            for idx, tr in enumerate(soup.select("tr"), start=1):
                tds = tr.select("td")
                if len(tds) < 3:
                    continue
                stock_td = tds[0]
                change_td = tds[1]
                concept_td = tds[2]
                code = str(stock_td.get("code") or stock_td.get("title") or "")
                board = self._classify_board(code)
                parsed_rows.append(
                    {
                        "周期": window_label,
                        "板块": board,
                        "板块顺序": self._board_sort_key(board),
                        "排名": idx,
                        "代码": code,
                        "名称": stock_td.get_text(" ", strip=True),
                        "区间涨幅": change_td.get_text(" ", strip=True),
                        "概念": concept_td.get_text(" ", strip=True),
                        "概念键": concept_td.get("gn", ""),
                        "日期区间": final_range,
                    }
                )

            parsed_rows.sort(key=lambda row: (self._window_sort_key(row["周期"]), row["板块顺序"], row["排名"]))
            section_summaries.append(
                {
                    "周期": window_label,
                    "日期区间": final_range,
                    "条数": len(parsed_rows),
                    "板块分布": {
                        board: sum(1 for row in parsed_rows if row["板块"] == board)
                        for board in LTGD_BOARD_ORDER
                    },
                }
            )
            rows.extend(parsed_rows)

        return FetchResult(
            kind="review_ltgd_range",
            rows=rows,
            meta={
                "source": f"{BASE}/api/getZfByDate",
                "field": "html",
                "count": len(rows),
                "latest_date": latest_date,
                "available_axis_points": len(aaxis),
                "axis_tail": aaxis[-10:],
                "sections": section_summaries,
                "windows": requested_windows,
            },
        )

    def fetch_review_plate(self, date: str = "") -> FetchResult:
        chart_data = self._post_json(f"{BASE}/api/getChartByQingxu")
        aaxis = chart_data.get("Aaxis", []) or []
        if not aaxis:
            raise RuntimeError("review plate chart data missing Aaxis")

        target_date = date.strip() if date else ""
        if target_date:
            normalized = self._post_json(
                f"{BASE}/api/getFupanDate",
                {"date": target_date, "type": "choose"},
            )
            target_date = normalized.get("date", target_date)
        else:
            target_date = str(aaxis[-1])

        payload = {
            "type": "plate",
            "date": target_date,
        }
        try:
            html_payload = self._post_json(f"{BASE}/api/getFupanByYidong", payload)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                html_payload = self._post_json_via_browser(BASE, "/api/getFupanByYidong", payload)
            else:
                raise

        html = html_payload.get("html", "") if isinstance(html_payload, dict) else ""
        if not html:
            raise RuntimeError(f"review plate response missing html for {target_date}")

        rows, topics = self._parse_review_plate_html(html, target_date)
        htmlcopy = html_payload.get("htmlcopy", "") if isinstance(html_payload, dict) else ""
        htmlcopy_metrics = self._parse_review_plate_htmlcopy_metrics(htmlcopy)
        fine_tag_summary = self._build_review_plate_fine_tag_summary(rows)

        return FetchResult(
            kind="review_plate",
            rows=rows,
            meta={
                "source": f"{BASE}/api/getFupanByYidong",
                "field": "html",
                "count": len(rows),
                "date": target_date,
                "type": "plate",
                "topic_count": len(topics),
                "topics": topics,
                "htmlcopy_metrics": htmlcopy_metrics,
                "fine_tag_summary": fine_tag_summary,
            },
        )

    @staticmethod
    def _parse_review_plate_htmlcopy_metrics(htmlcopy: str) -> Dict[str, str]:
        if not htmlcopy:
            return {}
        soup = BeautifulSoup(htmlcopy, "lxml")
        metrics: Dict[str, str] = {}
        for button in soup.select("button.platetype"):
            text = button.get_text(" ", strip=True).replace("\xa0", " ")
            if ":" not in text:
                continue
            key, value = text.split(":", 1)
            key = key.strip()
            value = value.strip()
            if key:
                metrics[key] = value
        return metrics

    def _parse_review_plate_html(self, html: str, target_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        soup = BeautifulSoup(html, "lxml")
        topic_items = soup.select("div.list-group-item.ztitem")
        if not topic_items:
            raise RuntimeError("review plate html missing topic blocks")

        rows: List[Dict[str, Any]] = []
        topics: List[Dict[str, Any]] = []

        for topic_index, item in enumerate(topic_items, start=1):
            title_node = item.select_one("b")
            topic_name = title_node.get_text(" ", strip=True) if title_node else ""

            left_block = item.find("div")
            left_text = left_block.get_text(" ", strip=True).replace("\xa0", " ") if left_block else item.get_text(" ", strip=True)
            topic_desc = left_text
            if topic_name:
                topic_desc = re.sub(rf"^{re.escape(topic_name)}\s*[：:]\s*", "", topic_desc).strip()

            ztnum = item.select_one(".ztnum")
            ztnum_text = ztnum.get_text(" ", strip=True) if ztnum else ""
            ztnum_match = re.search(r"(\d+)", ztnum_text)
            topic_limit_up_count: Any = int(ztnum_match.group(1)) if ztnum_match else ztnum_text

            table_wrap = item.find_next_sibling(lambda tag: tag.name == "div" and "zt" in (tag.get("class") or []))
            table = table_wrap.select_one("table.ztlist") if table_wrap else None
            if table is None:
                continue

            topic_start = len(rows)
            last_stock_row: Dict[str, Any] | None = None
            for tr in table.select("tr")[1:]:
                classes = tr.get("class") or []
                if "explain" in classes:
                    if last_stock_row is not None:
                        detail_text = tr.get_text(" ", strip=True).replace("\xa0", " ")
                        last_stock_row["异动原因详情"] = detail_text
                        search_text = " ".join(
                            [
                                str(last_stock_row.get("题材名称", "")),
                                str(last_stock_row.get("题材说明", "")),
                                str(last_stock_row.get("异动原因", "")),
                                detail_text,
                            ]
                        )
                        fine_tags = self._extract_review_plate_fine_tags(search_text)
                        last_stock_row["细标签"] = "|".join(fine_tags)
                        last_stock_row["细标签列表"] = fine_tags
                    continue

                tds = tr.select("td")
                if len(tds) < 18:
                    continue

                name_node = tds[0].select_one(".kline")
                stock_name = name_node.get_text(" ", strip=True) if name_node else tds[0].get_text(" ", strip=True)
                reason_summary = tds[16].get_text(" ", strip=True).replace("\xa0", " ")
                reason_detail = ""
                search_text = " ".join([topic_name, topic_desc, reason_summary])
                fine_tags = self._extract_review_plate_fine_tags(search_text)

                last_stock_row = {
                    "日期": target_date,
                    "题材序号": topic_index,
                    "题材名称": topic_name,
                    "题材说明": topic_desc,
                    "题材涨停数": topic_limit_up_count,
                    "题材内序号": len(rows) - topic_start + 1,
                    "名称": stock_name,
                    "代码": tds[1].get_text(" ", strip=True),
                    "股价": tds[2].get_text(" ", strip=True),
                    "涨幅": tds[3].get_text(" ", strip=True),
                    "涨停类型": tds[4].get_text(" ", strip=True),
                    "板数": tds[5].get_text(" ", strip=True),
                    "连板": tds[6].get_text(" ", strip=True),
                    "首次封板": tds[7].get_text(" ", strip=True),
                    "最后封板": tds[8].get_text(" ", strip=True),
                    "开板": tds[9].get_text(" ", strip=True),
                    "封单额": tds[10].get_text(" ", strip=True),
                    "成交额": tds[11].get_text(" ", strip=True),
                    "换手率": tds[12].get_text(" ", strip=True),
                    "实际流通": tds[13].get_text(" ", strip=True),
                    "流通市值": tds[14].get_text(" ", strip=True),
                    "总市值": tds[15].get_text(" ", strip=True),
                    "异动原因": reason_summary,
                    "异动原因详情": reason_detail,
                    "细标签": "|".join(fine_tags),
                    "细标签列表": fine_tags,
                    "龙虎榜": tds[17].get_text(" ", strip=True),
                }
                rows.append(last_stock_row)

            topic_row_count = len(rows) - topic_start
            for row in rows[topic_start:]:
                row["题材股票数"] = topic_row_count

            topics.append(
                {
                    "题材序号": topic_index,
                    "题材名称": topic_name,
                    "题材说明": topic_desc,
                    "题材涨停数": topic_limit_up_count,
                    "题材股票数": topic_row_count,
                }
            )

        if not rows:
            raise RuntimeError("review plate parsed zero rows")
        return rows, topics

    @staticmethod
    def _extract_review_plate_fine_tags(text: str) -> List[str]:
        text = str(text or "")
        hits: List[str] = []
        for tag in REVIEW_PLATE_FINE_TAGS:
            if tag in text and tag not in hits:
                hits.append(tag)
        return hits

    @staticmethod
    def _build_review_plate_fine_tag_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        summary: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            tags = row.get("细标签列表") or []
            for tag in tags:
                bucket = summary.setdefault(tag, {"count": 0, "topics": [], "stocks": []})
                bucket["count"] += 1
                topic_name = str(row.get("题材名称", ""))
                if topic_name and topic_name not in bucket["topics"]:
                    bucket["topics"].append(topic_name)
                stock_info = {
                    "名称": row.get("名称", ""),
                    "代码": row.get("代码", ""),
                    "题材名称": topic_name,
                }
                if stock_info not in bucket["stocks"]:
                    bucket["stocks"].append(stock_info)
        return summary

    def _classify_board(self, code: str) -> str:
        code = str(code or "")
        if code.startswith(("430", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "920")):
            return "北交所"
        if code.startswith(("300", "301", "688", "689")):
            return "创业科创板"
        return "主板"

    def _board_sort_key(self, board: str) -> int:
        try:
            return LTGD_BOARD_ORDER.index(board)
        except ValueError:
            return len(LTGD_BOARD_ORDER)

    def _window_sort_key(self, label: str) -> int:
        if label == "自定义":
            return 999
        m = re.search(r"(\d+)", label)
        return int(m.group(1)) if m else 999

    def fetch_home_qxlive_plate_summary(self) -> FetchResult:
        page_url = f"{BASE}/web/qxlive"
        rows: List[Dict[str, Any]] = []
        failed_items: List[Dict[str, Any]] = []
        top_plate_summaries: List[Dict[str, Any]] = []
        all_subplate_summaries: List[Dict[str, Any]] = []

        headers = [
            {'序号': 1, '名称': '主标签名称', '排序类型': ''},
            {'序号': 2, '名称': '主标签代码', '排序类型': ''},
            {'序号': 3, '名称': '板块强度', '排序类型': ''},
            {'序号': 4, '名称': '主力流入', '排序类型': ''},
            {'序号': 5, '名称': '涨停数量', '排序类型': ''},
            {'序号': 6, '名称': '子标签数量', '排序类型': ''},
            {'序号': 7, '名称': '子标签列表', '排序类型': ''},
        ]

        def direct_qxlive_json(url: str, payload: Dict[str, Any], timeout: int = TIMEOUT) -> Dict[str, Any]:
            resp = self.session.post(url, data=payload, timeout=timeout)
            resp.raise_for_status()
            return resp.json()

        def direct_qxlive_json_with_retry(
            url: str,
            payload: Dict[str, Any],
            *,
            attempts: int = 4,
            base_timeout: int = TIMEOUT,
            retry_wait: float = 0.8,
        ) -> Dict[str, Any]:
            last_exc: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    timeout = base_timeout + (attempt - 1) * 10
                    return direct_qxlive_json(url, payload, timeout=timeout)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < attempts:
                        time.sleep(retry_wait * attempt)
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(f'qxlive request failed with no exception: {url}')

        def fetch_top_payload(platetype: str) -> Dict[str, Any]:
            # 1) 先直连请求，最多4次
            try:
                result = direct_qxlive_json_with_retry(
                    'https://duanxianxia.cn/api/getLiveByStrong',
                    {'platetype': platetype, 'platelist': ''},
                    attempts=4,
                    base_timeout=TIMEOUT,
                )
                plates = result.get('plates', {})
                if not plates:
                    raise RuntimeError(f'empty plates for {platetype}')
                if platetype == 'money':
                    values = [
                        float(str(item.get('val', '') or '0').strip() or '0')
                        for item in (plates.values() if isinstance(plates, dict) else plates)
                    ]
                    if not any(abs(v) > 0.1 for v in values):
                        raise RuntimeError(f'all zero val for {platetype}, retry via browser')
                return result
            except Exception as direct_exc:
                # 2) 直连失败 → 浏览器方式，同样校验
                try:
                    browser_result = self._post_json_via_browser(
                        page_url,
                        'https://duanxianxia.cn/api/getLiveByStrong',
                        {'platetype': platetype, 'platelist': ''},
                    )
                    plates = browser_result.get('plates', {})
                    if not plates:
                        raise RuntimeError(f'browser fallback empty plates for {platetype}')
                    if platetype == 'money':
                        values = [
                            float(str(item.get('val', '') or '0').strip() or '0')
                            for item in (plates.values() if isinstance(plates, dict) else plates)
                        ]
                        if not any(abs(v) > 0.1 for v in values):
                            raise RuntimeError(f'browser fallback all zero val for {platetype}')
                    return browser_result
                except Exception as browser_exc:
                    # 3) 两级都失败 → 明确抛异常
                    raise RuntimeError(
                        f'fetch_top_payload FAILED for {platetype}: direct_err={direct_exc}; browser_err={browser_exc}'
                    )


        def payload_to_plate_values(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
            raw_plates = payload.get('plates', {})
            if isinstance(raw_plates, dict):
                return list(raw_plates.values())
            return list(raw_plates or [])

        def fetch_subplates_for_top_plate(top_plate: Dict[str, Any]) -> Dict[str, Any]:
            top_code = str(top_plate.get('主页题材代码', '') or '').strip()
            top_name = str(top_plate.get('主页题材名称', '') or '').strip()
            if not top_code:
                return {
                    'top_plate': top_plate,
                    'subplates': [],
                    'error': 'missing top plate code',
                }
            try:
                subplate_payload = direct_qxlive_json_with_retry(
                    'https://duanxianxia.cn/data/getKaipanSubPlate',
                    {'plateCode': top_code},
                    attempts=4,
                    base_timeout=TIMEOUT,
                )
                subplates = self._parse_qxlive_subplates_html(str(subplate_payload.get('result', '') or ''))
                return {
                    'top_plate': top_plate,
                    'subplates': subplates,
                    'error': '',
                }
            except Exception as exc:  # noqa: BLE001
                return {
                    'top_plate': top_plate,
                    'subplates': [],
                    'error': f'{top_name}({top_code}): {exc}',
                }

        strong_payload = fetch_top_payload('strong')
        money_payload = fetch_top_payload('money')

        strong_values = payload_to_plate_values(strong_payload)
        money_values = payload_to_plate_values(money_payload)
        strong_by_code = {
            str(item.get('code', '') or '').strip(): item
            for item in strong_values
            if str(item.get('code', '') or '').strip()
        }
        money_by_code = {
            str(item.get('code', '') or '').strip(): item
            for item in money_values
            if str(item.get('code', '') or '').strip()
        }

        # 按板块强度排序，取前20个主标签
        def strength_val(item: Dict[str, Any]) -> float:
            try:
                return float(str(item.get('val', '') or '0').strip())
            except Exception:
                return 0.0

        sorted_strong = sorted(strong_values, key=strength_val, reverse=True)
        TOP_N = 20
        ordered_codes: List[str] = []
        for item in sorted_strong:
            code = str(item.get('code', '') or '').strip()
            if code and code not in ordered_codes:
                ordered_codes.append(code)
            if len(ordered_codes) >= TOP_N:
                break
        # 不再合并 money_values 的全量，仅按强度前20抓取

        top_plates = []
        for idx, code in enumerate(ordered_codes, start=1):
            strong_item = strong_by_code.get(code, {})
            money_item = money_by_code.get(code, {})
            top_plates.append(
                {
                    '主页题材序号': idx,
                    '主页题材名称': str(
                        strong_item.get('name', '')
                        or money_item.get('name', '')
                        or ''
                    ).strip(),
                    '主页题材代码': code,
                    '主页题材值': str(strong_item.get('val', '') or '').strip(),
                    '板块强度原值': str(strong_item.get('val', '') or '').strip(),
                    '主力流入原值': str(money_item.get('val', '') or '').strip(),
                    '涨停数量': str(
                        strong_item.get('ztcount', '')
                        if strong_item.get('ztcount', '') not in [None, '']
                        else money_item.get('ztcount', '')
                    ).strip(),
                    '主页题材显示文本': f"{str(strong_item.get('name', '') or money_item.get('name', '') or '').strip()}({str(strong_item.get('val', '') or '').strip()})",
                }
            )
        if not top_plates:
            raise RuntimeError('qxlive getLiveByStrong returned no top plates')

        sub_results: List[Dict[str, Any]] = []
        worker_count = min(4, max(1, len(top_plates)))
        if worker_count == 1:
            for top_plate in top_plates:
                sub_results.append(fetch_subplates_for_top_plate(top_plate))
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(fetch_subplates_for_top_plate, top_plate) for top_plate in top_plates]
                for future in as_completed(futures):
                    sub_results.append(future.result())

        sub_map = {
            str(item['top_plate'].get('主页题材代码', '') or '').strip(): item
            for item in sub_results
        }

        missing_money_codes = [code for code in ordered_codes if code not in money_by_code]
        for top_plate in top_plates:
            top_code = str(top_plate.get('主页题材代码', '') or '').strip()
            top_name = str(top_plate.get('主页题材名称', '') or '').strip()
            sub_info = sub_map.get(top_code, {})
            subplates = sub_info.get('subplates', []) or []
            if sub_info.get('error'):
                failed_items.append(
                    {
                        'level': 'subplate_list',
                        'name': top_name,
                        'code': top_code,
                        'reason': sub_info.get('error', ''),
                    }
                )

            subplate_summaries = []
            for subplate in subplates:
                summary = {
                    **subplate,
                    'top_plate_name': top_name,
                    'top_plate_code': top_code,
                }
                subplate_summaries.append(summary)
                all_subplate_summaries.append(summary)

            strength_raw = str(top_plate.get('板块强度原值', '') or '').strip()
            inflow_raw = str(top_plate.get('主力流入原值', '') or '').strip()
            inflow_amount = self._parse_qxlive_money_raw_to_yuan(inflow_raw)
            rows.append(
                {
                    '主标签序号': top_plate.get('主页题材序号', ''),
                    '主标签名称': top_name,
                    '主标签代码': top_code,
                    '板块强度': strength_raw,
                    '板块强度原值': strength_raw,
                    '主力流入': self._format_qxlive_money_display(inflow_raw) if inflow_raw else '',
                    '主力流入原值': inflow_raw,
                    '主力流入真实金额': inflow_amount,
                    '涨停数量': str(top_plate.get('涨停数量', '') or '0'),
                    '子标签数量': len(subplates),
                    '子标签列表': '、'.join(
                        str(subplate.get('子题材名称', '') or '').strip()
                        for subplate in subplates
                        if str(subplate.get('子题材名称', '') or '').strip()
                    ),
                }
            )
            top_plate_summaries.append(
                {
                    **top_plate,
                    '板块强度': strength_raw,
                    '主力流入': self._format_qxlive_money_display(inflow_raw) if inflow_raw else '',
                    '主力流入真实金额': inflow_amount,
                    '子题材数': len(subplate_summaries),
                    'subplates': subplate_summaries,
                }
            )

        return FetchResult(
            kind='home_qxlive_plate_summary',
            rows=rows,
            meta={
                'source': page_url,
                'field': '/api/getLiveByStrong?platetype=strong + /api/getLiveByStrong?platetype=money + /data/getKaipanSubPlate',
                'count': len(rows),
                'table_headers': headers,
                'money_value_semantics': 'platetype=money 返回的 val 原值按万元理解；展示值按万/亿换算，主力流入真实金额字段按元落盘。',
                'top_plate_count': len(top_plates),
                'selected_top_plate': top_plate_summaries[0] if top_plate_summaries else {},
                'top_plates': top_plate_summaries,
                'subplate_count': len(all_subplate_summaries),
                'top_plate_summaries': top_plate_summaries,
                'subplates': all_subplate_summaries,
                'missing_items': [
                    {
                        'level': 'money_field',
                        'code': code,
                        'reason': 'missing platetype=money data',
                    }
                    for code in missing_money_codes
                ],
                'failed_items': failed_items,
                'complete': len(failed_items) == 0,
            },
        )

    def _fetch_qxlive_top_metrics_snapshot(self, page_url: str) -> Dict[str, Any]:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                executable_path='/usr/bin/google-chrome',
                args=['--no-sandbox', '--disable-dev-shm-usage'],
            )
            page = browser.new_page()
            try:
                page.goto(page_url, wait_until='networkidle', timeout=60000)
                result = page.evaluate(
                    """async () => {
                        const chartResp = await fetch('/vendor/stockdata/platechart1.json', { credentials: 'include' });
                        const chartText = await chartResp.text();
                        const lastResp = await fetch('/api/getLastQxlive', {
                            method: 'POST',
                            credentials: 'include',
                            headers: {
                                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
                            },
                            body: ''
                        });
                        const lastJson = await lastResp.json();
                        const buttons = {};
                        document.querySelectorAll('button.chart').forEach((el) => {
                            const id = (el.id || '').trim();
                            if (!id) return;
                            buttons[id] = (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();
                        });
                        return {
                            qxlive: JSON.parse(decryptData(chartText)).qxlive || {},
                            qxlast: lastJson.qxlast || {},
                            buttons,
                        };
                    }"""
                )
                if not isinstance(result, dict):
                    raise RuntimeError('invalid qxlive top metrics snapshot')
                return result
            finally:
                browser.close()

    @staticmethod
    def _series_last_value(values: Any) -> Any:
        if not isinstance(values, list) or not values:
            return ''
        return values[-1]

    @staticmethod
    def _extract_button_display_value(button_text: str) -> str:
        text = str(button_text or '').strip()
        if '：' in text:
            return text.split('：', 1)[1].strip()
        if ':' in text:
            return text.split(':', 1)[1].strip()
        return ''

    @staticmethod
    def _format_qxlive_axis(value: Any) -> str:
        text = str(value or '').strip()
        if not text:
            return ''
        digits = re.sub(r'\D', '', text)
        if len(digits) == 4:
            return f'{digits[:2]}:{digits[2:]}'
        if len(digits) == 3:
            return f'0{digits[0]}:{digits[1:]}'
        return text

    @staticmethod
    def _normalize_qxlive_trade_date(value: Any) -> str:
        text = str(value or '').strip()
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
            return text
        if re.fullmatch(r'\d{8}', text):
            return f'{text[:4]}-{text[4:6]}-{text[6:]}'
        return datetime.now(TZ_SHANGHAI).strftime('%Y-%m-%d')

    @staticmethod
    def _format_qxlive_metric_value(value: Any, value_type: str) -> str:
        if value in (None, ''):
            return ''
        text = str(value).strip()
        if value_type == 'percent':
            return text if text.endswith('%') else f'{text}%'
        if value_type == 'signed':
            if text.startswith(('+', '-')):
                return text
            try:
                number = float(text)
            except Exception:
                return text
            display = f'{int(number)}' if number.is_integer() else f'{number}'
            return f'+{display}' if number > 0 else display
        return text

    @staticmethod
    def _to_float_or_none(value: Any) -> float | None:
        try:
            if value in (None, ''):
                return None
            return float(str(value).strip())
        except Exception:
            return None

    def _parse_qxlive_money_raw_to_yuan(self, value: Any) -> float | None:
        amount_wan = self._to_float_or_none(value)
        if amount_wan is None:
            return None
        return amount_wan * 10000

    def _format_qxlive_money_display(self, value: Any) -> str:
        amount_wan = self._to_float_or_none(value)
        if amount_wan is None:
            return str(value or '').strip()
        sign = '-' if amount_wan < 0 else ''
        abs_amount_wan = abs(amount_wan)
        if abs_amount_wan >= 10000:
            text = f'{abs_amount_wan / 10000:.1f}'.rstrip('0').rstrip('.')
            return f'{sign}{text}亿'
        if abs_amount_wan >= 100:
            text = f'{abs_amount_wan:.0f}'.rstrip('0').rstrip('.')
        else:
            text = f'{abs_amount_wan:.1f}'.rstrip('0').rstrip('.')
        if not text:
            text = '0'
        return f'{sign}{text}万'

    @staticmethod
    def _parse_qxlive_subplates_html(html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(f"<div>{html}</div>", 'html.parser')
        rows: List[Dict[str, Any]] = []
        for idx, node in enumerate(soup.select('.subplate'), start=1):
            rows.append(
                {
                    '子题材序号': idx,
                    '子题材名称': node.get_text(strip=True),
                    '子题材代码': node.get('platecode') or node.get('plateCode') or '',
                }
            )
        return rows

    @staticmethod
    def _parse_qxlive_plate_text(item: Dict[str, Any]) -> Dict[str, Any]:
        text = str(item.get('显示文本', '') or '').strip()
        match = re.match(r'^(.*?)(?:\(([^()]*)\))?$', text)
        plate_name = text
        plate_value = ''
        if match:
            plate_name = (match.group(1) or '').strip() or text
            plate_value = (match.group(2) or '').strip()
        return {
            '主页题材序号': item.get('题材序号', ''),
            '主页题材名称': plate_name,
            '主页题材代码': item.get('题材代码', ''),
            '主页题材值': plate_value,
            '主页题材显示文本': text,
        }

    def _normalize_qxlive_stock_row(
        self,
        primary_plate: Dict[str, Any],
        subplate: Dict[str, Any] | None,
        stock_index: int | str,
        item: List[Any],
        stock_url: str,
        record_type: str,
        main_stock_index: int | str,
    ) -> Dict[str, Any]:
        def pick(index: int, default: Any = '') -> Any:
            return item[index] if len(item) > index else default

        tag_text = str(pick(13, '') or '').strip()
        return {
            '记录类型': record_type,
            '主页题材序号': primary_plate.get('主页题材序号', ''),
            '主页题材名称': primary_plate.get('主页题材名称', ''),
            '主页题材代码': primary_plate.get('主页题材代码', ''),
            '主页题材值': primary_plate.get('主页题材值', ''),
            '主标签内序号': main_stock_index,
            '子题材序号': subplate.get('子题材序号', '') if subplate else '',
            '子题材名称': subplate.get('子题材名称', '') if subplate else '',
            '子题材代码': subplate.get('子题材代码', '') if subplate else '',
            '子题材内序号': stock_index,
            '名称': str(pick(1, '') or ''),
            '代码': str(pick(0, '') or ''),
            '涨幅': self._format_qxlive_pct(pick(2, '')),
            '成交': self._format_qxlive_amount(pick(8, ''), yi_digits=1, wan_digits=0),
            '流通': self._format_qxlive_amount(pick(7, ''), yi_digits=0, wan_digits=0),
            '板数': str(pick(12, '') or '--'),
            '竞涨': self._format_qxlive_pct(pick(3, '')),
            '竞额': self._format_qxlive_amount(pick(16, ''), yi_digits=2, wan_digits=0),
            '竞量': str(pick(15, '') or ''),
            '买成比': self._format_qxlive_pct(pick(14, ''), with_sign=False),
            '封单': self._format_qxlive_seal_amount(pick(17, '')),
            '标签': tag_text,
            '龙头标签': tag_text if '龙' in tag_text else '',
            '破板标签': tag_text if '破' in tag_text else '',
            '行情源': stock_url,
        }

    @staticmethod
    def _format_qxlive_pct(value: Any, with_sign: bool = True) -> str:
        try:
            number = float(value)
        except Exception:
            text = str(value or '').strip()
            if not text:
                return ''
            return text if text.endswith('%') else f'{text}%'

        normalized = f'{number:.2f}'.rstrip('0').rstrip('.')
        if normalized == '-0':
            normalized = '0'
        return f'{normalized}%' if with_sign else f'{normalized}%'

    @staticmethod
    def _format_qxlive_amount(value: Any, yi_digits: int, wan_digits: int) -> str:
        try:
            number = float(value)
        except Exception:
            return str(value or '').strip()
        if abs(number) >= 100000000:
            text = f'{number / 100000000:.{yi_digits}f}'.rstrip('0').rstrip('.')
            return f'{text}亿'
        text = f'{number / 10000:.{wan_digits}f}'.rstrip('0').rstrip('.')
        return f'{text}万'

    def _format_qxlive_seal_amount(self, value: Any) -> str:
        try:
            number = float(value)
        except Exception:
            return str(value or '').strip()
        if number <= 100:
            return '--'
        return self._format_qxlive_amount(number, yi_digits=1, wan_digits=0)

    def fetch_auction_vratio(self) -> FetchResult:
        url = f"{BASE}/data/getVratioData/11"
        try:
            data = self._post_json(url)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                data = self._post_json_via_browser(f"{BASE}/mob/jjyd", "/data/getVratioData/11")
            else:
                raise
        items = data.get("list", []) or []
        rows = []
        for idx, item in enumerate(items, start=1):
            rows.append(
                {
                    "rank": idx,
                    "code": str(item[0]),
                    "name": item[1],
                    "auction_volume_ratio": item[2],
                    "seal_amount_wan": item[3],
                    "auction_change_pct": item[4],
                    "latest_change_pct": item[5],
                    "auction_turnover_wan": item[6],
                    "concept": item[7],
                    "auction_change_pct_text": item[8],
                    "auction_turnover_wan_text": item[9],
                    "yesterday_auction_turnover_wan": item[10],
                    "volume_ratio_multiple": item[11],
                    "turnover_rate_pct": item[12],
                    "raw": item,
                }
            )
        return FetchResult(
            kind="auction_vratio",
            rows=rows,
            meta={
                "source": url,
                "field": "list",
                "count": len(rows),
            },
        )

    def fetch_auction_qiangchou(self) -> FetchResult:
        url = f"{BASE}/data/getQiangchouData/11"
        try:
            data = self._post_json(url)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                data = self._post_json_via_browser(f"{BASE}/mob/jjyd", "/data/getQiangchouData/11")
            else:
                raise
        groups = data.get("list", {}) or {}
        rows = []
        for group_name in ["grab", "qiangchou"]:
            items = groups.get(group_name, []) or []
            for idx, item in enumerate(items, start=1):
                rows.append(
                    {
                        "group": group_name,
                        "rank": idx,
                        "code": str(item[0]),
                        "name": item[1],
                        "auction_volume_ratio": item[2],
                        "seal_amount_wan": item[3],
                        "auction_change_pct": item[4],
                        "latest_change_pct": item[5],
                        "auction_turnover_wan": item[6],
                        "concept": item[7],
                        "auction_change_pct_text": item[8],
                        "auction_turnover_wan_text": item[9],
                        "yesterday_auction_turnover_wan": item[10],
                        "grab_strength": item[11],
                        "turnover_rate_pct": item[12],
                        "raw": item,
                    }
                )
        return FetchResult(
            kind="auction_qiangchou",
            rows=rows,
            meta={
                "source": url,
                "field": "list.grab + list.qiangchou",
                "count": len(rows),
                "groups": {k: len(v or []) for k, v in groups.items()},
            },
        )

    def fetch_auction_net_amount(self) -> FetchResult:
        url = f"{BASE}/vendor/stockdata/jjzhuli.json"
        encrypted = self.session.get(
            url,
            timeout=TIMEOUT,
            headers={
                "User-Agent": UA,
                "Referer": f"{BASE}/mob/jjyd",
            },
        ).text
        decrypted = self._decrypt_jjlive_payload(encrypted)
        data = json.loads(decrypted)
        items = data.get("list", []) or []
        rows = []
        for idx, item in enumerate(items, start=1):
            concept_1, concept_2 = self._split_pipe_concepts(item[7] if len(item) > 7 else "")
            rows.append(
                {
                    "rank": idx,
                    "code": str(item[0]),
                    "name": item[1],
                    "auction_change_pct": item[2],
                    "latest_change_pct": item[3],
                    "main_net_inflow_wan": item[4],
                    "auction_turnover_wan": item[5],
                    "market_cap_yi": item[6],
                    "concept": item[7],
                    "turnover_rate_pct": item[8],
                    "concept_1": concept_1,
                    "concept_2": concept_2,
                    "raw": item,
                }
            )
        return FetchResult(
            kind="auction_net_amount",
            rows=rows,
            meta={
                "source": url,
                "field": "list",
                "count": len(rows),
                "count_meta": data.get("count", {}),
                "source_tab": "mob/jjyd -> zhuli",
            },
        )

    def fetch_auction_fengdan(self) -> FetchResult:
        datasource = self._get_json(f"{BASE}/vendor/stockdata/datasource.json")
        if datasource.get("istrade") == 1:
            live_base = datasource.get("data_url", BASE)
        else:
            live_base = (datasource.get("base_url") or [BASE])[0]
        live_url = f"{live_base}/vendor/stockdata/jjlive.json"
        live_encrypted = self._get_text(live_url)
        live_decrypted = self._decrypt_jjlive_payload(live_encrypted)
        live_data = json.loads(live_decrypted)

        live_summary = self._parse_fengdan_head(str(live_data.get("th", "")))
        live_summary.update(
            {
                "date": live_summary.get("date") or datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d"),
                "t15": live_data.get("t15", ""),
                "t20": live_data.get("t20", ""),
                "t25": live_data.get("t25", ""),
                "source": live_url,
                "section_kind": "live",
            }
        )

        rows = self._parse_fengdan_table(str(live_data.get("table", "")), live_summary)
        live_summary["row_count"] = len(rows)
        summaries = [live_summary]

        # 页面展示中的涨幅列会被前端用 qt.gtimg.cn 实时行情覆盖；
        # vendor/stockdata/jjlive.json 里的旧值不能直接当最终涨幅使用。
        live_codes = [r.get("code", "") for r in rows if r.get("code")]
        live_quotes = self._fetch_realtime_quotes(live_codes)
        for row in rows:
            quote = live_quotes.get(row.get("code", ""), {})
            if quote.get("latest_change_pct"):
                row["latest_change_pct"] = quote["latest_change_pct"]
                row["latest_change_pct_source"] = "qt.gtimg.cn"
            else:
                row["latest_change_pct_source"] = "jjlive.json"

        return FetchResult(
            kind="auction_fengdan",
            rows=rows,
            meta={
                "source": live_url,
                "field": "live vendor/stockdata/jjlive.json + qt.gtimg.cn realtime quotes",
                "count": len(rows),
                "section_count": len(summaries),
                "sections": summaries,
            },
        )

    def fetch_cashflow_today(self) -> FetchResult:
        return self._fetch_cashflow_rank("today", "今日排行")

    def fetch_cashflow_3d(self) -> FetchResult:
        return self._fetch_cashflow_rank("3d", "3日排行")

    def fetch_cashflow_5d(self) -> FetchResult:
        return self._fetch_cashflow_rank("5d", "5日排行")

    def fetch_cashflow_10d(self) -> FetchResult:
        return self._fetch_cashflow_rank("10d", "10日排行")

    def _fetch_cashflow_rank(self, period: str, label: str) -> FetchResult:
        page_url = f"{CASHFLOW_BASE}/cashFlow/stock.html"
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                executable_path='/usr/bin/google-chrome',
                args=['--no-sandbox', '--disable-dev-shm-usage'],
            )
            page = browser.new_page(user_agent=UA)
            try:
                page.goto(page_url, wait_until='networkidle', timeout=60000)
                label_map = {
                    "today": "今日排行",
                    "3d": "3日排行",
                    "5d": "5日排行",
                    "10d": "10日排行",
                }
                tab_label = label_map[period]
                if period != "today":
                    page.get_by_text(tab_label, exact=True).click()
                    page.wait_for_timeout(1800)

                first_page = self._extract_cashflow_rows_from_page(page)
                total_pages = first_page["total_pages"]
                rows = list(first_page["rows"])
                page_counts = [
                    {
                        "page": 1,
                        "row_count": len(first_page["rows"]),
                    }
                ]

                if len(rows) < CASHFLOW_DEFAULT_LIMIT:
                    for page_no in range(2, total_pages + 1):
                        page.locator(f'[name="whj_page"][data-page="{page_no}"]').click()
                        page.wait_for_timeout(1800)
                        page_data = self._extract_cashflow_rows_from_page(page)
                        rows.extend(page_data["rows"])
                        page_counts.append(
                            {
                                "page": page_no,
                                "row_count": len(page_data["rows"]),
                            }
                        )
                        if len(rows) >= CASHFLOW_DEFAULT_LIMIT:
                            break

                rows = rows[:CASHFLOW_DEFAULT_LIMIT]
            finally:
                browser.close()

        kind_map = {
            "today": "cashflow_today",
            "3d": "cashflow_3d",
            "5d": "cashflow_5d",
            "10d": "cashflow_10d",
        }
        return FetchResult(
            kind=kind_map[period],
            rows=rows,
            meta={
                "source": page_url,
                "field": "table rows",
                "count": len(rows),
                "period": period,
                "period_label": label,
                "total_pages": total_pages,
                "page_counts": page_counts,
                "default_limit": CASHFLOW_DEFAULT_LIMIT,
                "complete": True,
            },
        )

    def _extract_cashflow_rows_from_page(self, page) -> Dict[str, Any]:
        payload = page.evaluate(
            """() => {
              const rows = [...document.querySelectorAll('table tbody tr')].map(tr => {
                const tds = [...tr.querySelectorAll('td')].map(td => (td.innerText || '').trim());
                return {
                  rank: tds[0] || '',
                  name: tds[1] || '',
                  code: tds[2] || '',
                  stock_circle: tds[3] || '',
                  latest_price: tds[4] || '',
                  change_pct: tds[5] || '',
                  main_net_inflow: tds[6] || '',
                  super_net_inflow: tds[7] || '',
                  large_net_inflow: tds[8] || '',
                  medium_net_inflow: tds[9] || '',
                  little_net_inflow: tds[10] || ''
                };
              }).filter(row => row.code);
              const pages = [...document.querySelectorAll('[name="whj_page"]')]
                .map(el => Number(el.getAttribute('data-page') || '0'))
                .filter(Boolean);
              return {
                rows,
                totalPages: pages.length ? Math.max(...pages) : 1
              };
            }"""
        )
        normalized_rows = []
        for row in payload.get("rows", []):
            normalized_rows.append(
                {
                    "排名": int(row["rank"]) if str(row.get("rank", "")).isdigit() else row.get("rank", ""),
                    "名称": row.get("name", ""),
                    "代码": row.get("code", ""),
                    "股圈": row.get("stock_circle", ""),
                    "最新价": row.get("latest_price", ""),
                    "涨跌幅": row.get("change_pct", ""),
                    "主力净流入": row.get("main_net_inflow", ""),
                    "特大单净流入": row.get("super_net_inflow", ""),
                    "大单净流入": row.get("large_net_inflow", ""),
                    "中单净流入": row.get("medium_net_inflow", ""),
                    "小单净流入": row.get("little_net_inflow", ""),
                }
            )
        return {
            "rows": normalized_rows,
            "total_pages": int(payload.get("totalPages") or 1),
        }

    @staticmethod
    def _decode_padded_base64(value: str) -> bytes:
        value = value.rstrip("=")
        while len(value) % 4:
            value += "="
        return base64.b64decode(value)

    def _decrypt_jjlive_payload(self, encrypted_text: str) -> str:
        encrypted_bytes = base64.b64decode(encrypted_text)
        cipher = AES.new(JJLIVE_AES_KEY, AES.MODE_CBC, JJLIVE_AES_IV)
        decrypted = unpad(cipher.decrypt(encrypted_bytes), AES.block_size)
        return decrypted.decode("utf-8")

    @staticmethod
    def _parse_fengdan_head(head_html: str) -> Dict[str, Any]:
        text = BeautifulSoup(head_html, "lxml").get_text("\n", strip=True)
        text = text.replace("\xa0", " ")
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
        yizi_match = re.search(r"一字\s*:?\s*(\d+)个", text)
        seal_match = re.search(r"封单\s*:?\s*([^\n|]+)", text)
        return {
            "date": date_match.group(1) if date_match else "",
            "yizi_count": int(yizi_match.group(1)) if yizi_match else None,
            "seal_total": seal_match.group(1).strip() if seal_match else "",
            "has_change_pct": "涨幅" in text,
            "header_text": text,
        }

    @staticmethod
    def _parse_fengdan_table(table_html: str, summary: Dict[str, Any]) -> List[Dict[str, Any]]:
        def is_board_label(text: str) -> bool:
            text = text.strip()
            if not text:
                return False
            return bool(re.fullmatch(r"(?:昨)?(?:首板|\d+板|\d+天\d+板)", text))

        soup = BeautifulSoup(table_html, "lxml")
        rows: List[Dict[str, Any]] = []
        for idx, td in enumerate(soup.select("td.fd"), start=1):
            code = td.get("code", "")
            name = ""
            b = td.find("b")
            if b and b.contents:
                first_text = b.find(string=True, recursive=False)
                name = first_text.strip() if first_text else b.get_text(" ", strip=True)

            direct_ps = [p.get_text(" ", strip=True) for p in td.find_all("p", recursive=False) if p.get_text(" ", strip=True)]
            board_label = ""
            concept_tags = direct_ps
            if direct_ps and is_board_label(direct_ps[-1]):
                board_label = direct_ps[-1]
                concept_tags = direct_ps[:-1]

            direct_spans = td.select(":scope > span")
            span_texts = [s.get_text(" ", strip=True) for s in direct_spans]
            amount_915 = span_texts[0] if len(span_texts) > 0 else ""
            amount_920 = span_texts[1] if len(span_texts) > 1 else ""
            amount_925 = span_texts[2] if len(span_texts) > 2 else ""
            latest_change_pct = span_texts[3] if len(span_texts) > 3 else ""

            rows.append(
                {
                    "section_date": summary.get("date", ""),
                    "section_kind": summary.get("section_kind", ""),
                    "section_yizi_count": summary.get("yizi_count"),
                    "section_seal_total": summary.get("seal_total", ""),
                    "section_t15_total": summary.get("t15", ""),
                    "section_t20_total": summary.get("t20", ""),
                    "section_t25_total": summary.get("t25", ""),
                    "section_has_change_pct": summary.get("has_change_pct", False),
                    "rank": idx,
                    "code": code,
                    "name": name,
                    "tag_1": concept_tags[0] if len(concept_tags) > 0 else "",
                    "tag_2": concept_tags[1] if len(concept_tags) > 1 else "",
                    "tag_3": concept_tags[2] if len(concept_tags) > 2 else "",
                    "board_label": board_label,
                    "amount_915": amount_915,
                    "amount_920": amount_920,
                    "amount_925": amount_925,
                    "latest_change_pct": latest_change_pct,
                    "latest_change_pct_source": "jjlive.json",
                    "tags": direct_ps,
                }
            )
        return rows

    def _fetch_realtime_quotes(self, codes: List[str]) -> Dict[str, Dict[str, str]]:
        unique_codes: List[str] = []
        seen = set()
        for code in codes:
            code = str(code).strip()
            if not code or code in seen:
                continue
            seen.add(code)
            unique_codes.append(code)

        result: Dict[str, Dict[str, str]] = {}
        if not unique_codes:
            return result

        def market_prefix(code: str) -> str:
            if code.startswith(("60", "68")):
                return "sh"
            return "sz"

        for i in range(0, len(unique_codes), 60):
            batch = unique_codes[i : i + 60]
            symbols = ",".join(f"{market_prefix(code)}{code}" for code in batch)
            url = f"https://qt.gtimg.cn/q={symbols}"
            resp = self.session.get(
                url,
                timeout=TIMEOUT,
                headers={
                    "User-Agent": UA,
                    "Referer": f"{BASE}/web/jjlive",
                },
            )
            resp.raise_for_status()
            for line in resp.text.splitlines():
                line = line.strip().rstrip(";")
                if not line or "=\"" not in line:
                    continue
                _, payload = line.split('="', 1)
                payload = payload.rstrip('"')
                parts = payload.split("~")
                if len(parts) < 33:
                    continue
                code = parts[2].strip()
                latest_change_pct = parts[32].strip()
                if code:
                    result[code] = {
                        "latest_change_pct": f"{latest_change_pct}%" if latest_change_pct else ""
                    }
        return result

    @staticmethod
    def _format_hot_rate(value: Any) -> str:
        try:
            n = float(value)
        except Exception:
            return str(value) if value is not None else ""
        if abs(n) >= 10000:
            return f"{n / 10000:.0f}w"
        if n.is_integer():
            return str(int(n))
        return str(n)

    @staticmethod
    def _format_amount(value: Any, digits: int = 1) -> str:
        try:
            n = float(value)
        except Exception:
            return ""
        if abs(n) >= 100000000:
            return f"{n / 100000000:.{digits}f}亿"
        return f"{round(n / 10000)}万"

    @staticmethod
    def _split_concepts(value: Any) -> tuple[str, str]:
        if value is None:
            return "", ""
        text = str(value)
        parts = [p.strip() for p in text.split("+") if p.strip()]
        if not parts:
            return "", ""
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], parts[1]

    @staticmethod
    def _split_pipe_concepts(value: Any) -> tuple[str, str]:
        if value is None:
            return "", ""
        text = str(value)
        parts = [p.strip() for p in text.split("|") if p.strip()]
        if not parts:
            return "", ""
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], parts[1]

    @staticmethod
    def _review_jinji_bucket(bucket: Any, fallback_rate: Any = None) -> Dict[str, Any]:
        if not isinstance(bucket, dict):
            bucket = {}
        all_count = DuanxianxiaFetcher._to_int_or_none(bucket.get("all"))
        jinji_count = DuanxianxiaFetcher._to_int_or_none(bucket.get("jinji"))
        rate = bucket.get("jinjilv")
        if rate in (None, ""):
            rate = fallback_rate
        return {
            "all": all_count,
            "jinji": 0 if all_count is not None and jinji_count is None else jinji_count,
            "rate": rate,
        }

    @staticmethod
    def _to_int_or_none(value: Any) -> int | None:
        try:
            if value in (None, ""):
                return None
            return int(float(value))
        except Exception:
            return None

    @staticmethod
    def _format_review_percent(value: Any) -> str:
        try:
            if value in (None, ""):
                return ""
            return f"{float(value):.1f}%"
        except Exception:
            text = str(value) if value is not None else ""
            return text if not text or text.endswith("%") else text + "%"

    @staticmethod
    def _format_review_ratio(jinji_count: Any, sample_count: Any) -> str:
        if jinji_count is None or sample_count is None:
            return ""
        return f"{jinji_count}/{sample_count}"


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch reusable duanxianxia rank data")
    parser.add_argument(
        "dataset",
        choices=[
            "rocket",
            "hot",
            "surge",
            "hotlist_day",
            "review_daily",
            "review_daily_core11",
            "review_ltgd_range",
            "review_plate",
            "home_qxlive_plate_summary",
            "home_qxlive_top_metrics",
            "home_ztpool",
            "auction_vratio",
            "auction_qiangchou",
            "auction_net_amount",
            "auction_fengdan",
            "cashflow_today",
            "cashflow_3d",
            "cashflow_5d",
            "cashflow_10d",
        ],
        help="rocket=飙升榜, hot=热门, surge=冲涨, hotlist_day=热度榜（日）, review_daily=复盘/每日复盘顶部指标, review_daily_core11=每日复盘顶部指标（11项，不含量能）, review_ltgd_range=龙头高度区间涨幅, review_plate=涨停复盘（按概念/题材标签）, home_qxlive_plate_summary=主页板块强度全主标签汇总表, home_qxlive_top_metrics=主页qxlive顶部指标按钮组, home_ztpool=主页涨停股票池, auction_vratio=竞价爆量, auction_qiangchou=竞价抢筹, auction_net_amount=竞价净额, auction_fengdan=竞价封单, cashflow_today/3d/5d/10d=个股资金流向排行（默认前100名）",
    )
    parser.add_argument("--format", choices=["json", "jsonl"], default="json")
    parser.add_argument("--limit", type=int, default=0, help="Only output first N rows (0 = all)")
    parser.add_argument("--sort", default="", help="Optional server-side sort suffix for pool endpoints")
    parser.add_argument("--date", default="", help="Optional trade date for review_daily, e.g. 2026-04-09")
    parser.add_argument(
        "--range",
        default="",
        help="Optional date range for review_ltgd_range, e.g. '2026-03-11 - 2026-04-09'",
    )
    parser.add_argument(
        "--stdout-only",
        action="store_true",
        help="Do not persist capture to disk (debug only; default behavior persists every fetch)",
    )
    return parser


def dataset_meta(kind: str) -> Dict[str, str]:
    try:
        return DATASET_REGISTRY[kind]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset kind: {kind}") from exc


def infer_headers(rows: List[Dict[str, Any]]) -> List[str]:
    headers: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                headers.append(key)
                seen.add(key)
    return headers


def build_capture_payload(result: FetchResult) -> Dict[str, Any]:
    ds = dataset_meta(result.kind)
    now_utc = datetime.now(timezone.utc)
    now_cn = now_utc.astimezone(TZ_SHANGHAI)
    rows = result.rows
    return {
        "project": "duanxianxia",
        "dataset_kind": result.kind,
        "dataset_id": ds["id"],
        "dataset_label": ds["label"],
        "source_path": ds["path"],
        "source_url": result.meta.get("source", ""),
        "fetched_at": now_cn.isoformat(timespec="seconds"),
        "fetched_at_utc": now_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "timezone": "Asia/Shanghai",
        "row_count": len(rows),
        "headers": infer_headers(rows),
        "rows": rows,
        "meta": result.meta,
    }


def persist_capture(payload: Dict[str, Any]) -> Path:
    fetched_at = datetime.fromisoformat(payload["fetched_at"])
    date_part = fetched_at.strftime("%Y-%m-%d")
    time_part = fetched_at.strftime("%H%M%S")
    dataset_id = payload["dataset_id"]
    out_dir = CAPTURE_ROOT / date_part / dataset_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{time_part}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    fetcher = DuanxianxiaFetcher()

    if args.dataset == "rocket":
        result = fetcher.fetch_rocket()
    elif args.dataset == "hot":
        result = fetcher.fetch_hot(sort=args.sort)
    elif args.dataset == "surge":
        result = fetcher.fetch_surge(sort=args.sort)
    elif args.dataset == "hotlist_day":
        result = fetcher.fetch_hotlist_day()
    elif args.dataset == "review_daily":
        result = fetcher.fetch_review_daily(date=args.date)
    elif args.dataset == "review_daily_core11":
        result = fetcher.fetch_review_daily_core11(date=args.date)
    elif args.dataset == "review_ltgd_range":
        result = fetcher.fetch_review_ltgd_range(range_expr=args.range)
    elif args.dataset == "review_plate":
        result = fetcher.fetch_review_plate(date=args.date)
    elif args.dataset == "home_qxlive_plate_summary":
        result = fetcher.fetch_home_qxlive_plate_summary()
    elif args.dataset == "home_qxlive_top_metrics":
        result = fetcher.fetch_home_qxlive_top_metrics()
    elif args.dataset == "home_ztpool":
        result = fetcher.fetch_home_ztpool()
    elif args.dataset == "auction_vratio":
        result = fetcher.fetch_auction_vratio()
    elif args.dataset == "auction_qiangchou":
        result = fetcher.fetch_auction_qiangchou()
    elif args.dataset == "auction_net_amount":
        result = fetcher.fetch_auction_net_amount()
    elif args.dataset == "auction_fengdan":
        result = fetcher.fetch_auction_fengdan()
    elif args.dataset == "cashflow_today":
        result = fetcher.fetch_cashflow_today()
    elif args.dataset == "cashflow_3d":
        result = fetcher.fetch_cashflow_3d()
    elif args.dataset == "cashflow_5d":
        result = fetcher.fetch_cashflow_5d()
    elif args.dataset == "cashflow_10d":
        result = fetcher.fetch_cashflow_10d()
    else:
        parser.error(f"Unsupported dataset: {args.dataset}")
        return 2

    capture_payload = build_capture_payload(result)
    capture_path = None
    if not args.stdout_only:
        capture_path = persist_capture(capture_payload)

    rows = result.rows[: args.limit] if args.limit and args.limit > 0 else result.rows
    output_meta = dict(capture_payload["meta"])
    output_meta.update(
        {
            "dataset_id": capture_payload["dataset_id"],
            "dataset_label": capture_payload["dataset_label"],
            "source_path": capture_payload["source_path"],
            "fetched_at": capture_payload["fetched_at"],
            "timezone": capture_payload["timezone"],
            "saved": not args.stdout_only,
            "capture_path": str(capture_path) if capture_path else "",
            "stored_row_count": capture_payload["row_count"],
            "returned_row_count": len(rows),
        }
    )
    payload = {
        "dataset": result.kind,
        "dataset_id": capture_payload["dataset_id"],
        "meta": output_meta,
        "rows": rows,
    }

    if args.format == "jsonl":
        for row in rows:
            print(json.dumps(row, ensure_ascii=False))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
