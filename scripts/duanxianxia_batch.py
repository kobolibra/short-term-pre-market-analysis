#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import signal
import sys
import time
import traceback
import urllib.request
from collections import Counter
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

from duanxianxia_fetcher import (
    DATASET_REGISTRY,
    DuanxianxiaFetcher,
    build_capture_payload,
    infer_headers,
    persist_capture,
)
from feishu_bitable_cli import create_record, feishu_request, load_meta

WORKSPACE_ROOT = Path("/home/investmentofficehku/.openclaw/workspace")
PROJECT_ROOT = WORKSPACE_ROOT / "projects" / "duanxianxia"
REPORT_ROOT = PROJECT_ROOT / "reports"
CAPTURE_ROOT = PROJECT_ROOT / "captures"
DAILYLINE_ROOT = PROJECT_ROOT / "dailyline"
DAILYLINE_STOCK_ROOT = DAILYLINE_ROOT / "stocks"
TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILYLINE_START_DATE = "2026-01-01"
DAILYLINE_MANIFEST_DATASET_ID = "dailyline.stock.manifest"
DAILYLINE_MANIFEST_LABEL = "复盘日线下载清单"
DAILYLINE_FIELDS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "pctChg",
    "isST",
]
DAILYLINE_CAPTURE_EXCLUDED = {
    "home.kaipan.plate.summary",
    DAILYLINE_MANIFEST_DATASET_ID,
}


def load_workspace_env(env_path: Path) -> None:
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            value = value.strip()
            if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            os.environ.setdefault(key, value)
    except Exception:
        return


load_workspace_env(WORKSPACE_ROOT / ".env")

GROUPS: Dict[str, Dict[str, Any]] = {
    "premarket": {
        "label": "盘前",
        "datasets": [
            "auction_vratio",
            "auction_qiangchou",
            "auction_net_amount",
            "auction_fengdan",
            "home_qxlive_plate_summary",
            "home_qxlive_top_metrics",
        ],
    },
    "intraday": {
        "label": "盘中",
        "datasets": [
            "rocket",
            "hotlist_day",
            "hot",
            "surge",
            "home_qxlive_top_metrics",
        ],
    },
    "postmarket": {
        "label": "盘后",
        "datasets": [
            "review_daily",
            "review_ltgd_range",
            "review_plate",
            "home_ztpool",
            "rocket",
            "hotlist_day",
        ],
    },
    "postmarket_cashflow": {
        "label": "盘后+资金净流入",
        "datasets": [
            "review_daily",
            "review_ltgd_range",
            "review_plate",
            "home_ztpool",
            "rocket",
            "hotlist_day",
            "cashflow_today",
            "cashflow_3d",
            "cashflow_5d",
            "cashflow_10d",
        ],
    },
    "cashflow": {
        "label": "资金流向",
        "datasets": [
            "cashflow_today",
            "cashflow_3d",
            "cashflow_5d",
            "cashflow_10d",
        ],
    },
    "intraday_cashflow": {
        "label": "盘中+资金流向",
        "datasets": [
            "rocket",
            "hotlist_day",
            "hot",
            "surge",
            "home_qxlive_top_metrics",
            "cashflow_today",
            "cashflow_3d",
            "cashflow_5d",
            "cashflow_10d",
        ],
    },
    "dailyline": {
        "label": "复盘日线下载",
        "datasets": [],
    },
}

SEQUENCE = {
    "rocket": 1,
    "hot": 2,
    "surge": 3,
    "hotlist_day": 4,
    "review_ltgd_range": 5,
    "review_daily": 6,
    "review_daily_core11": 17,
    "home_qxlive_top_metrics": 17,
    "home_ztpool": 18,
    "review_plate": 15,
    "home_qxlive_plate_summary": 16,
    "auction_vratio": 7,
    "auction_qiangchou": 8,
    "auction_net_amount": 9,
    "auction_fengdan": 10,
    "cashflow_today": 11,
    "cashflow_3d": 12,
    "cashflow_5d": 13,
    "cashflow_10d": 14,
}

TABLE_SPECS: Dict[str, Dict[str, Any]] = {
    "rank.rocket": {
        "columns": [("排名", "rank"), ("代码", "code"), ("名称", "name"), ("飙升值", "value")],
        "webhook_row_limit": 30,
    },
    "rank.hot_stock_day": {
        "columns": [("排名", "rank"), ("代码", "code"), ("名称", "name"), ("热度值", "value")],
        "webhook_row_limit": 30,
    },
    "pool.hot": {
        "columns": [("代码", "代码"), ("名称", "名称"), ("涨幅", "涨幅"), ("主力", "主力"), ("实际换手", "实际换手"), ("成交", "成交"), ("流通", "流通"), ("概念", "概念")],
    },
    "pool.surge": {
        "columns": [("排名", "rank"), ("代码", "code"), ("名称", "name"), ("涨幅", "change_pct"), ("换手比", "turnover_ratio"), ("成交", "amount"), ("流通市值", "float_market_cap"), ("概念1", "concept_1"), ("概念2", "concept_2")],
    },
    "auction.jjyd.vratio": {
        "columns": [("排名", "rank"), ("名称", "name"), ("代码", "code"), ("涨幅", "latest_change_pct"), ("竞额", "auction_turnover_wan"), ("昨竞额", "yesterday_auction_turnover_wan"), ("竞价换手", "turnover_rate_pct"), ("竞价量比", "volume_ratio_multiple"), ("概念", "concept")],
    },
    "auction.jjyd.qiangchou": {
        "columns": [("排名", "rank"), ("名称", "name"), ("代码", "code"), ("涨幅", "latest_change_pct"), ("竞额", "auction_turnover_wan"), ("抢筹幅度", "grab_strength"), ("竞价换手", "turnover_rate_pct"), ("概念", "concept")],
        "group_titles": {
            "qiangchou": "9:20-9:25 抢筹幅度",
            "grab": "竞价最后1秒 抢筹幅度",
        },
    },
    "auction.jjyd.net_amount": {
        "columns": [("排名", "rank"), ("名称", "name"), ("代码", "code"), ("涨幅", "latest_change_pct"), ("竞价换手", "turnover_rate_pct"), ("主力净买", "main_net_inflow_wan"), ("竞额", "auction_turnover_wan"), ("流通值", "market_cap_yi"), ("概念1", "concept_1"), ("概念2", "concept_2")],
    },
    "auction.jjlive.fengdan": {
        "columns": [("排名", "rank"), ("名称", "name"), ("代码", "code"), ("题材1", "tag_1"), ("题材2", "tag_2"), ("连板标签", "board_label"), ("9:15", "amount_915"), ("9:20", "amount_920"), ("9:25", "amount_925"), ("涨幅", "latest_change_pct")],
    },
    "review.daily.top_metrics": {
        "columns": [("序号", "order"), ("指标键", "metric_key"), ("指标名称", "metric_label"), ("指标分组", "metric_group"), ("分类", "metric_category"), ("展示名称", "display_label"), ("日期", "date"), ("数值", "value"), ("晋级率", "display_rate"), ("晋级数", "jinji_count"), ("样本数", "sample_count"), ("比值", "ratio"), ("原值", "raw_value")],
    },
    "review.daily.top_metrics.core11": {
        "columns": [("序号", "order"), ("指标键", "metric_key"), ("指标名称", "metric_label"), ("指标分组", "metric_group"), ("分类", "metric_category"), ("展示名称", "display_label"), ("日期", "date"), ("数值", "value"), ("晋级率", "display_rate"), ("晋级数", "jinji_count"), ("样本数", "sample_count"), ("比值", "ratio"), ("原值", "raw_value")],
    },
    "home.qxlive.top_metrics": {
        "columns": [("序号", "order"), ("指标名称", "metric_label"), ("日期", "date"), ("时间点", "time_point"), ("当前值", "value")],
        "webhook_row_limit": 20,
        "card_chunk_size": 20,
    },
    "home.ztpool": {
        "columns": [
            ("日期", "日期"),
            ("分组序号", "分组序号"),
            ("分组名称", "分组名称"),
            ("组内序号", "组内序号"),
            ("晋级率文本", "晋级率文本"),
            ("晋级数", "晋级数"),
            ("样本数", "样本数"),
            ("晋级率", "晋级率"),
            ("市场", "市场"),
            ("代码", "代码"),
            ("名称", "名称"),
            ("状态", "状态"),
            ("涨幅", "涨幅"),
            ("题材", "题材"),
        ],
        "card_columns": [
            ("分组名称", "分组名称"),
            ("组内序号", "组内序号"),
            ("代码", "代码"),
            ("名称", "名称"),
            ("状态", "状态"),
            ("涨幅", "涨幅"),
            ("题材", "题材"),
        ],
        "webhook_row_limit": 500,
        "card_chunk_size": 45,
        "webhook_max_rows": 500,
    },
    "review.ltgd.range": {
        "columns": [("周期", "周期"), ("板块", "板块"), ("排名", "排名"), ("代码", "代码"), ("名称", "名称"), ("区间涨幅", "区间涨幅"), ("概念", "概念"), ("日期区间", "日期区间")],
    },
    "review.fupan.plate": {
        "columns": [
            ("日期", "日期"),
            ("题材序号", "题材序号"),
            ("题材名称", "题材名称"),
            ("题材说明", "题材说明"),
            ("题材涨停数", "题材涨停数"),
            ("题材内序号", "题材内序号"),
            ("名称", "名称"),
            ("代码", "代码"),
            ("涨幅", "涨幅"),
            ("板数", "板数"),
            ("连板", "连板"),
            ("封单额", "封单额"),
            ("成交额", "成交额"),
            ("异动原因", "异动原因"),
        ],
        "webhook_row_limit": 50,
        "card_chunk_size": 20,
    },
    "home.kaipan.plate.summary": {
        "columns": [
            ("主标签序号", "主标签序号"),
            ("主标签名称", "主标签名称"),
            ("主标签代码", "主标签代码"),
            ("板块强度", "板块强度"),
            ("主力流入", "主力流入"),
            ("涨停数量", "涨停数量"),
            ("子标签数量", "子标签数量"),
            ("子标签列表", "子标签列表"),
        ],
        "card_columns": [
            ("主标签序号", "主标签序号"),
            ("主标签名称", "主标签名称"),
            ("主标签代码", "主标签代码"),
            ("板块强度", "板块强度"),
            ("主力流入", "主力流入"),
            ("涨停数量", "涨停数量"),
        ],
        "webhook_row_limit": 20,
        "card_chunk_size": 20,
    },
    "cashflow.stock.today": {
        "columns": [("排名", "排名"), ("名称", "名称"), ("代码", "代码"), ("最新价", "最新价"), ("涨跌幅", "涨跌幅"), ("主力净流入", "主力净流入"), ("特大单净流入", "特大单净流入")],
        "webhook_row_limit": 50,
        "card_chunk_size": 25,
    },
    "cashflow.stock.3day": {
        "columns": [("排名", "排名"), ("名称", "名称"), ("代码", "代码"), ("最新价", "最新价"), ("涨跌幅", "涨跌幅"), ("主力净流入", "主力净流入"), ("特大单净流入", "特大单净流入")],
        "webhook_row_limit": 50,
        "card_chunk_size": 25,
    },
    "cashflow.stock.5day": {
        "columns": [("排名", "排名"), ("名称", "名称"), ("代码", "代码"), ("最新价", "最新价"), ("涨跌幅", "涨跌幅"), ("主力净流入", "主力净流入"), ("特大单净流入", "特大单净流入")],
        "webhook_row_limit": 50,
        "card_chunk_size": 25,
    },
    "cashflow.stock.10day": {
        "columns": [("排名", "排名"), ("名称", "名称"), ("代码", "代码"), ("最新价", "最新价"), ("涨跌幅", "涨跌幅"), ("主力净流入", "主力净流入"), ("特大单净流入", "特大单净流入")],
        "webhook_row_limit": 50,
        "card_chunk_size": 25,
    },
    DAILYLINE_MANIFEST_DATASET_ID: {
        "columns": [
            ("股票代码", "股票代码"),
            ("股票名称", "股票名称"),
            ("baostock代码", "baostock代码"),
            ("来源数据集", "来源数据集"),
            ("前一交易日正式推荐", "前一交易日正式推荐"),
            ("已有日线数", "已有日线数"),
            ("新增日线数", "新增日线数"),
            ("最新日期", "最新日期"),
            ("状态", "状态"),
            ("错误", "错误"),
        ],
        "webhook_row_limit": 50,
        "card_chunk_size": 20,
    },
}

MAX_WEBHOOK_TABLE_ROWS = 30
MAX_FEISHU_CARD_PAYLOAD_BYTES = 40000

CARD_TEMPLATES: Dict[str, str] = {
    "premarket": "orange",
    "intraday": "blue",
    "intraday_cashflow": "blue",
    "cashflow": "wathet",
    "postmarket": "green",
    "postmarket_cashflow": "green",
    "dailyline": "green",
}

QXLIVE_LABEL_SCORE = {
    "龙一": 12,
    "龙二": 10,
    "龙三": 8,
    "龙四": 7,
    "龙五": 6,
    "龙六": 5,
    "龙七": 4,
    "龙八": 3,
    "龙九": 2,
    "龙十": 1,
}


def normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"(\d{6})", text)
    return match.group(1) if match else text


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def safe_date(value: Any) -> date_cls | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def zero_pad_stock_code(value: Any) -> str:
    code = normalize_code(value)
    if not code:
        return ""
    digits = re.sub(r"\D", "", code)
    if not digits:
        return ""
    if len(digits) > 6:
        digits = digits[-6:]
    return digits.zfill(6)


def is_supported_a_share_code(value: Any) -> bool:
    code = zero_pad_stock_code(value)
    return bool(code) and code[0] in {"0", "3", "6"}


def to_baostock_code(value: Any) -> str:
    code = zero_pad_stock_code(value)
    if not code:
        return ""
    if code.startswith("6"):
        return f"sh.{code}"
    if code.startswith(("0", "3")):
        return f"sz.{code}"
    return ""


def parse_pct_value(value: Any, default: float = 0.0) -> float:
    text = str(value or "").strip().rstrip("%")
    if not text:
        return default
    return safe_float(text, default)


def infer_price_limit_pct(code: Any, name: Any = "") -> float:
    norm_code = normalize_code(code)
    name_text = str(name or "").upper()
    if "ST" in name_text:
        return 5.0
    if norm_code.startswith(("300", "301", "688")):
        return 20.0
    if norm_code.startswith("8") or norm_code.startswith("92"):
        return 30.0
    return 10.0


def is_untradable_auction_candidate(row: Dict[str, Any]) -> bool:
    code = normalize_code(row.get("code"))
    if not code:
        return False
    limit_pct = infer_price_limit_pct(code, row.get("name", ""))
    auction_pct = parse_pct_value(
        row.get("auction_change_pct")
        or row.get("auction_change_pct_text")
        or row.get("竞价涨幅")
    )
    return auction_pct >= max(0.0, limit_pct - 0.2)


SHORT_THEME_TOKENS = {
    "AI",
    "AR",
    "VR",
    "MR",
    "ST",
    "5G",
    "6G",
    "CPO",
    "MPO",
    "OCS",
    "PCB",
    "CPC",
    "GPU",
    "IP",
}

THEME_GRAYLIST = {
    "数字经济",
    "大科技",
    "新质生产力",
    "国企改革",
    "专精特新",
    "新能源",
    "大消费",
}


def normalize_theme_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", "", text)
    for suffix in ["概念股", "概念", "板块", "题材"]:
        if text.endswith(suffix) and len(text) > len(suffix):
            text = text[: -len(suffix)]
            break
    return text.strip("|-_/，,、；;")


def is_noise_theme_token(token: str) -> bool:
    if not token or token in {"-", "暂无", "无"}:
        return True
    if token in {"首板", "反包", "连板"}:
        return True
    if re.fullmatch(r"\d+板", token):
        return True
    if re.fullmatch(r"\d+天\d+板", token):
        return True
    return False


def split_theme_tokens(*values: Any) -> List[str]:
    tokens: List[str] = []
    seen = set()
    for value in values:
        if value is None:
            continue
        parts = value if isinstance(value, list) else [value]
        for part in parts:
            for raw_piece in re.split(r"[|、,/，；;]+", str(part or "")):
                token = normalize_theme_token(raw_piece)
                if is_noise_theme_token(token):
                    continue
                if len(token) < 2 and token not in SHORT_THEME_TOKENS:
                    continue
                if token in seen:
                    continue
                seen.add(token)
                tokens.append(token)
    return tokens


def theme_token_matches(left: str, right: str) -> bool:
    a = normalize_theme_token(left)
    b = normalize_theme_token(right)
    if not a or not b:
        return False
    if a == b:
        return True
    if a in b or b in a:
        if min(len(a), len(b)) >= 3:
            return True
        if a in SHORT_THEME_TOKENS or b in SHORT_THEME_TOKENS:
            return True
    return False


def extract_candidate_theme_tokens(row: Dict[str, Any]) -> List[str]:
    return split_theme_tokens(
        row.get("concept"),
        row.get("concept_1"),
        row.get("concept_2"),
        row.get("tag_1"),
        row.get("tag_2"),
        row.get("tag_3"),
        row.get("tags"),
    )


def build_qxlive_theme_catalog(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def to_theme_score(row: Dict[str, Any]) -> float:
        return safe_float(row.get("板块强度原值") or row.get("板块强度"), 0.0)

    def to_theme_inflow(row: Dict[str, Any]) -> float:
        return safe_float(row.get("主力流入原值") or row.get("主力流入"), 0.0)

    sorted_rows = sorted(
        rows,
        key=lambda row: (
            -to_theme_score(row),
            -to_theme_inflow(row),
            safe_int(row.get("主标签序号"), 9999),
        ),
    )
    catalog: List[Dict[str, Any]] = []
    for rank, row in enumerate(sorted_rows, start=1):
        main_name = str(row.get("主标签名称", "") or "").strip()
        subthemes = split_theme_tokens(row.get("子标签列表"))
        catalog.append(
            {
                "theme_rank": rank,
                "主标签序号": row.get("主标签序号", ""),
                "主标签名称": main_name,
                "主标签代码": row.get("主标签代码", ""),
                "板块强度": row.get("板块强度", ""),
                "板块强度原值": row.get("板块强度原值", row.get("板块强度", "")),
                "主力流入": row.get("主力流入", ""),
                "主力流入原值": row.get("主力流入原值", row.get("主力流入", "")),
                "涨停数量": row.get("涨停数量", ""),
                "子标签数量": row.get("子标签数量", ""),
                "子标签列表": row.get("子标签列表", ""),
                "main_token": normalize_theme_token(main_name),
                "is_gray_theme": normalize_theme_token(main_name) in THEME_GRAYLIST,
                "subtheme_tokens": subthemes,
                "strength_value": to_theme_score(row),
                "inflow_value": to_theme_inflow(row),
                "ztcount_value": safe_int(row.get("涨停数量"), 0),
            }
        )
    return catalog


def estimate_risk_penalty(matches: List[Dict[str, Any]]) -> float:
    penalty = 0.0
    for match in matches[:2]:
        inflow_value = safe_float(match.get("主力流入"), 0.0)
        ztcount_value = safe_int(match.get("涨停数量"), 0)
        if inflow_value < -80000 or (inflow_value < 0 and ztcount_value <= 0):
            penalty = max(penalty, 0.15)
        elif inflow_value < -30000:
            penalty = max(penalty, 0.10)
        elif inflow_value < 0:
            penalty = max(penalty, 0.05)
    return penalty


def evaluate_theme_overlay(candidate_tokens: List[str], theme_catalog: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidate_tokens:
        return {
            "theme_score": 0.0,
            "theme_matches": [],
            "theme_reasons": [],
            "theme_risks": [],
            "ranking_penalty": 0.0,
        }

    matches: List[Dict[str, Any]] = []
    for theme in theme_catalog:
        main_hits = [token for token in candidate_tokens if theme_token_matches(token, theme.get("main_token", ""))]
        sub_hits: List[str] = []
        for subtheme in theme.get("subtheme_tokens", []):
            if any(theme_token_matches(token, subtheme) for token in candidate_tokens):
                sub_hits.append(subtheme)

        if not main_hits and not sub_hits:
            continue

        theme_rank = safe_int(theme.get("theme_rank"), 9999)
        strength_value = safe_float(theme.get("strength_value"), 0.0)
        inflow_value = safe_float(theme.get("inflow_value"), 0.0)
        ztcount_value = safe_int(theme.get("ztcount_value"), 0)
        is_gray_theme = bool(theme.get("is_gray_theme", False))

        if not main_hits and theme_rank > 25:
            continue
        if theme_rank > 80 and strength_value <= 0 and inflow_value <= 0 and ztcount_value <= 0:
            continue

        raw_score = 0.0
        if main_hits:
            raw_score += 2.0 if is_gray_theme else 5.0
        raw_score += min(3.0 if is_gray_theme else 4.0, (1.0 if is_gray_theme else 2.0) * len(sub_hits))

        if not is_gray_theme:
            if theme_rank <= 3:
                raw_score += 3.0
            elif theme_rank <= 5:
                raw_score += 2.0
            elif theme_rank <= 10:
                raw_score += 1.0

        if inflow_value > 0:
            raw_score += 1.0
        if ztcount_value >= 5:
            raw_score += 1.0

        if theme_rank > 120:
            raw_score -= 8.0
        elif theme_rank > 80:
            raw_score -= 5.0
        elif theme_rank > 50:
            raw_score -= 3.0
        elif theme_rank > 20:
            raw_score -= 1.0

        if strength_value <= 0:
            raw_score -= 2.0
        if inflow_value < 0:
            raw_score -= 1.0
        if ztcount_value <= 0:
            raw_score -= 1.0
        if is_gray_theme:
            raw_score -= 2.0

        if raw_score <= 0:
            continue

        match = {
            "主标签名称": theme.get("主标签名称", ""),
            "主标签代码": theme.get("主标签代码", ""),
            "主题排名": theme_rank,
            "板块强度": theme.get("板块强度", ""),
            "主力流入": theme.get("主力流入", ""),
            "涨停数量": theme.get("涨停数量", ""),
            "灰名单主题": is_gray_theme,
            "命中主标签": bool(main_hits),
            "命中子标签": sub_hits[:5],
            "匹配词": sorted(set(main_hits + sub_hits)),
            "match_score": round(min(10.0, raw_score), 1),
        }
        matches.append(match)

    matches.sort(key=lambda item: (-safe_float(item.get("match_score"), 0.0), safe_int(item.get("主题排名"), 9999), str(item.get("主标签名称", ""))))
    top_matches = matches[:2]

    total_theme_score = 0.0
    if top_matches:
        total_theme_score += safe_float(top_matches[0].get("match_score"), 0.0)
    if len(top_matches) > 1:
        total_theme_score += min(4.0, round(safe_float(top_matches[1].get("match_score"), 0.0) * 0.5, 1))
    total_theme_score = round(min(12.0, total_theme_score), 1)

    theme_reasons: List[str] = []
    theme_risks: List[str] = []
    for match in top_matches:
        main_name = str(match.get("主标签名称", "") or "")
        sub_hits = match.get("命中子标签", []) or []
        rank = safe_int(match.get("主题排名"), 9999)
        score = safe_float(match.get("match_score"), 0.0)
        gray_prefix = "灰名单主题，" if match.get("灰名单主题") else ""
        if sub_hits:
            reason = f"主题匹配 {main_name}（{gray_prefix}子标签：{'/'.join(sub_hits[:3])}，主题前{rank}，加分{score:g}）"
        else:
            reason = f"主题匹配 {main_name}（{gray_prefix}主题前{rank}，加分{score:g}）"
        theme_reasons.append(reason)
        inflow_text = str(match.get("主力流入", "") or "").strip()
        inflow_value = safe_float(inflow_text, 0.0)
        if inflow_text and inflow_value < 0:
            theme_risks.append(f"命中主题 {main_name}，但主力流入为负（{inflow_text}）")
        if match.get("灰名单主题"):
            theme_risks.append(f"命中泛化主题 {main_name}，参考意义已降权")

    ranking_penalty = estimate_risk_penalty(top_matches)

    return {
        "theme_score": total_theme_score,
        "theme_matches": top_matches,
        "theme_reasons": theme_reasons,
        "theme_risks": theme_risks,
        "ranking_penalty": ranking_penalty,
    }


def build_premarket_analysis(report: Dict[str, Any]) -> Dict[str, Any]:
    items_by_id = {item.get("dataset_id"): item for item in report.get("items", [])}
    required_ids = [
        "auction.jjyd.vratio",
        "auction.jjyd.qiangchou",
        "auction.jjyd.net_amount",
        "auction.jjlive.fengdan",
        "home.kaipan.plate.summary",
    ]
    missing = [dataset_id for dataset_id in required_ids if dataset_id not in items_by_id]
    if missing:
        return {
            "enabled": False,
            "version": "premarket_5table_v5",
            "reason": f"missing datasets: {', '.join(missing)}",
            "top_candidates": [],
        }

    vratio_rows = load_capture_rows(items_by_id["auction.jjyd.vratio"].get("capture_path", ""))
    qiangchou_rows = load_capture_rows(items_by_id["auction.jjyd.qiangchou"].get("capture_path", ""))
    net_rows = load_capture_rows(items_by_id["auction.jjyd.net_amount"].get("capture_path", ""))
    fengdan_rows = load_capture_rows(items_by_id["auction.jjlive.fengdan"].get("capture_path", ""))
    qxlive_rows = load_capture_rows(items_by_id["home.kaipan.plate.summary"].get("capture_path", ""))

    qxlive_themes = build_qxlive_theme_catalog(qxlive_rows)

    untradable_codes = {
        normalize_code(row.get("code"))
        for row in [*vratio_rows, *qiangchou_rows, *net_rows]
        if is_untradable_auction_candidate(row)
    }

    candidates: Dict[str, Dict[str, Any]] = {}

    def ensure_candidate(code: Any, name: Any) -> Dict[str, Any]:
        norm_code = normalize_code(code)
        item = candidates.get(norm_code)
        if item is None:
            item = {
                "code": norm_code,
                "name": str(name or "").strip(),
                "score": 0.0,
                "signals": [],
                "risks": [],
                "concept_tokens": [],
                "concept_token_set": set(),
                "source_hits": set(),
            }
            candidates[norm_code] = item
        if not item.get("name"):
            item["name"] = str(name or "").strip()
        return item

    def attach_theme_tokens(candidate: Dict[str, Any], row: Dict[str, Any]) -> None:
        for token in extract_candidate_theme_tokens(row):
            if token in candidate["concept_token_set"]:
                continue
            candidate["concept_token_set"].add(token)
            candidate["concept_tokens"].append(token)

    for row in vratio_rows:
        cand = ensure_candidate(row.get("code"), row.get("name"))
        attach_theme_tokens(cand, row)
        rank = safe_int(row.get("rank"), 999)
        cand["score"] += max(0, 18 - rank)
        cand["signals"].append(f"竞价爆量第{rank}")
        cand["source_hits"].add("竞价爆量")

    for row in qiangchou_rows:
        cand = ensure_candidate(row.get("code"), row.get("name"))
        attach_theme_tokens(cand, row)
        rank = safe_int(row.get("rank"), 999)
        group = str(row.get("group", "") or "").strip()
        if group == "grab":
            cand["score"] += max(0, 22 - rank)
            cand["signals"].append(f"末秒抢筹第{rank}")
        else:
            cand["score"] += max(0, 16 - rank)
            cand["signals"].append(f"区间抢筹第{rank}")
        cand["source_hits"].add("竞价抢筹")

    for row in net_rows:
        cand = ensure_candidate(row.get("code"), row.get("name"))
        attach_theme_tokens(cand, row)
        rank = safe_int(row.get("rank"), 999)
        cand["score"] += max(0, 20 - rank)
        cand["signals"].append(f"竞价净额第{rank}")
        cand["source_hits"].add("竞价净额")

    for row in fengdan_rows:
        section_kind = str(row.get("section_kind", "") or "").strip()
        if section_kind != "live":
            continue
        cand = ensure_candidate(row.get("code"), row.get("name"))
        attach_theme_tokens(cand, row)
        rank = safe_int(row.get("rank"), 999)
        cand["score"] += max(0, 16 - rank)
        cand["signals"].append(f"当日封单第{rank}")
        cand["source_hits"].add("竞价封单")

    output: List[Dict[str, Any]] = []
    for cand in candidates.values():
        if cand["code"] in untradable_codes:
            continue
        source_hit_count = len(cand["source_hits"])
        if source_hit_count >= 4:
            cand["score"] += 10
        elif source_hit_count == 3:
            cand["score"] += 6
        elif source_hit_count == 2:
            cand["score"] += 2

        overlay = evaluate_theme_overlay(cand.get("concept_tokens", []), qxlive_themes)
        cand["score"] += safe_float(overlay.get("theme_score"), 0.0)
        cand["theme_score"] = overlay.get("theme_score", 0.0)
        cand["theme_matches"] = overlay.get("theme_matches", [])
        cand["theme_reasons"] = overlay.get("theme_reasons", [])
        cand["ranking_penalty"] = safe_float(overlay.get("ranking_penalty"), 0.0)
        cand["risks"].extend(overlay.get("theme_risks", []))

        unique_signals = []
        seen = set()
        for signal in cand["signals"]:
            if signal in seen:
                continue
            seen.add(signal)
            unique_signals.append(signal)
        unique_risks = []
        seen_risks = set()
        for risk in cand["risks"]:
            if risk in seen_risks:
                continue
            seen_risks.add(risk)
            unique_risks.append(risk)
        ordered_reasons = unique_signals[:2] + [reason for reason in cand.get("theme_reasons", []) if reason] + unique_signals[2:6]
        output.append(
            {
                "name": cand["name"],
                "code": cand["code"],
                "score": round(cand["score"], 1),
                "ranking_score": round(cand["score"] * (1.0 - safe_float(cand.get("ranking_penalty"), 0.0)), 2),
                "ranking_penalty": round(safe_float(cand.get("ranking_penalty"), 0.0), 2),
                "theme_score": round(safe_float(cand.get("theme_score"), 0.0), 1),
                "source_hit_count": source_hit_count,
                "source_hits": sorted(cand["source_hits"]),
                "concept_tokens": cand.get("concept_tokens", [])[:8],
                "theme_matches": cand.get("theme_matches", []),
                "reasons": ordered_reasons[:6],
                "risks": unique_risks,
            }
        )

    output.sort(key=lambda item: (-safe_float(item.get("ranking_score"), 0.0), -item["score"], -item["source_hit_count"], item["code"]))
    for idx, item in enumerate(output, start=1):
        item["rank"] = idx

    return {
        "enabled": True,
        "version": "premarket_5table_v5",
        "candidate_count": len(output),
        "top_candidates": output[:10],
        "market_themes": [
            {
                "主题排名": row.get("theme_rank", ""),
                "主标签序号": row.get("主标签序号", ""),
                "主标签名称": row.get("主标签名称", ""),
                "主标签代码": row.get("主标签代码", ""),
                "板块强度": row.get("板块强度", ""),
                "主力流入": row.get("主力流入", ""),
                "涨停数量": row.get("涨停数量", ""),
                "子标签数量": row.get("子标签数量", ""),
                "子标签列表": row.get("子标签列表", ""),
            }
            for row in qxlive_themes[:10]
        ],
        "notes": [
            "候选池当前仍以盘前4表命中的股票为主。",
            "第5表已替换为 qxlive 全主标签汇总表，不再按个股代码参与匹配，而是作为 market theme overlay 给候选股提供主题一致性加分。",
            "theme overlay 的输入来自前4表中的概念/题材字段，优先匹配主标签名称，其次匹配子标签名称，并对前排强主题给予额外加分。",
            "已加入泛化主题灰名单、后排弱主题更强过滤，以及基于主题风险的 ranking_score 排序降权。",
            "竞价封单现只使用 section_kind=live 的当日封单数据，history 历史封单不再参与盘前分析。",
            "若个股竞价涨幅已接近对应涨停阈值，则视为盘前难买候选，直接不进入排序结果。",
        ],
    }


def parse_chinese_amount(value: Any) -> float:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return 0.0
    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 100000000.0
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 10000.0
        text = text[:-1]
    return safe_float(text, 0.0) * multiplier


def build_intraday_analysis(report: Dict[str, Any]) -> Dict[str, Any]:
    items_by_id = {item.get("dataset_id"): item for item in report.get("items", [])}
    required_ids = [
        "rank.rocket",
        "rank.hot_stock_day",
        "pool.hot",
        "pool.surge",
        "home.qxlive.top_metrics",
    ]
    missing = [dataset_id for dataset_id in required_ids if dataset_id not in items_by_id]
    if missing:
        return {
            "enabled": False,
            "version": "intraday_multisignal_v1",
            "reason": f"missing datasets: {', '.join(missing)}",
            "top_candidates": [],
        }

    rocket_rows = load_capture_rows(items_by_id["rank.rocket"].get("capture_path", ""))
    hotlist_rows = load_capture_rows(items_by_id["rank.hot_stock_day"].get("capture_path", ""))
    hot_rows = load_capture_rows(items_by_id["pool.hot"].get("capture_path", ""))
    surge_rows = load_capture_rows(items_by_id["pool.surge"].get("capture_path", ""))
    qxlive_rows = load_capture_rows(items_by_id["home.qxlive.top_metrics"].get("capture_path", ""))

    cashflow_sources = [
        ("cashflow.stock.today", "今日资金流入", 14.0),
        ("cashflow.stock.3day", "3日资金流入", 10.0),
        ("cashflow.stock.5day", "5日资金流入", 8.0),
        ("cashflow.stock.10day", "10日资金流入", 6.0),
    ]

    candidates: Dict[str, Dict[str, Any]] = {}

    def ensure_candidate(code: Any, name: Any) -> Dict[str, Any] | None:
        norm_code = zero_pad_stock_code(code)
        if not norm_code:
            return None
        item = candidates.get(norm_code)
        if item is None:
            item = {
                "code": norm_code,
                "name": str(name or "").strip(),
                "score": 0.0,
                "hits": set(),
                "reasons": [],
                "risks": [],
                "change_pct": None,
                "main_force": 0.0,
            }
            candidates[norm_code] = item
        if not item.get("name"):
            item["name"] = str(name or "").strip()
        return item

    for row in hotlist_rows:
        cand = ensure_candidate(row.get("code"), row.get("name"))
        if cand is None:
            continue
        rank = safe_int(row.get("rank"), 999)
        cand["score"] += max(0, 35 - rank)
        cand["reasons"].append(f"热度榜日内第{rank}")
        cand["hits"].add("hotlist_day")

    for row in rocket_rows:
        cand = ensure_candidate(row.get("code"), row.get("name"))
        if cand is None:
            continue
        rank = safe_int(row.get("rank"), 999)
        cand["score"] += max(0, 18 - rank)
        cand["reasons"].append(f"飙升榜第{rank}")
        cand["hits"].add("rocket")

    for row in hot_rows:
        cand = ensure_candidate(row.get("代码"), row.get("名称"))
        if cand is None:
            continue
        cand["score"] += 18
        cand["reasons"].append("热门池命中")
        cand["hits"].add("hot")
        cand["change_pct"] = parse_pct_value(row.get("涨幅"))
        cand["main_force"] = parse_chinese_amount(row.get("主力"))
        if cand["main_force"] > 0:
            cand["score"] += min(10.0, cand["main_force"] / 50000000.0)
            cand["reasons"].append("热门池主力净流入为正")
        else:
            cand["risks"].append("热门池主力净流入为负")

    for row in surge_rows:
        cand = ensure_candidate(row.get("code"), row.get("name"))
        if cand is None:
            continue
        rank = safe_int(row.get("rank"), 999)
        cand["score"] += max(0, 16 - rank)
        cand["reasons"].append(f"冲涨池第{rank}")
        cand["hits"].add("surge")
        if parse_pct_value(row.get("change_pct")) >= 8:
            cand["score"] += 4

    for dataset_id, label, boost in cashflow_sources:
        item = items_by_id.get(dataset_id)
        if not item:
            continue
        for row in load_capture_rows(item.get("capture_path", "")):
            cand = ensure_candidate(row.get("代码"), row.get("名称"))
            if cand is None:
                continue
            rank = safe_int(row.get("排名"), 999)
            if rank <= 30:
                cand["score"] += max(0.0, boost - rank / 6.0)
                cand["reasons"].append(f"{label}前{rank}")
                cand["hits"].add(dataset_id)

    output: List[Dict[str, Any]] = []
    for cand in candidates.values():
        hit_count = len(cand["hits"])
        if hit_count >= 4:
            cand["score"] += 8
        elif hit_count == 3:
            cand["score"] += 4
        if cand.get("change_pct") is not None and safe_float(cand.get("change_pct"), 0.0) >= 9.5:
            cand["score"] += 3
        if "hot" in cand["hits"] and "hotlist_day" in cand["hits"]:
            cand["score"] += 5
        if "hot" in cand["hits"] and any(key in cand["hits"] for key in {"cashflow.stock.today", "cashflow.stock.3day"}):
            cand["score"] += 4

        unique_reasons = []
        seen_reasons = set()
        for reason in cand["reasons"]:
            if reason in seen_reasons:
                continue
            seen_reasons.add(reason)
            unique_reasons.append(reason)

        unique_risks = []
        seen_risks = set()
        for risk in cand["risks"]:
            if risk in seen_risks:
                continue
            seen_risks.add(risk)
            unique_risks.append(risk)

        output.append(
            {
                "code": cand["code"],
                "name": cand["name"],
                "score": round(cand["score"], 2),
                "source_hit_count": hit_count,
                "source_hits": sorted(cand["hits"]),
                "change_pct": round(safe_float(cand.get("change_pct"), 0.0), 2) if cand.get("change_pct") is not None else None,
                "main_force": round(safe_float(cand.get("main_force"), 0.0), 2),
                "reasons": unique_reasons[:6],
                "risks": unique_risks[:3],
            }
        )

    output.sort(key=lambda item: (-safe_float(item.get("score"), 0.0), -safe_int(item.get("source_hit_count"), 0), item.get("code", "")))
    for idx, item in enumerate(output, start=1):
        item["rank"] = idx

    market_snapshot = {
        str(row.get("metric_label") or row.get("metric_key") or "").strip(): str(row.get("value") or row.get("button_display_value") or "").strip()
        for row in qxlive_rows
        if str(row.get("metric_label") or row.get("metric_key") or "").strip()
    }

    return {
        "enabled": True,
        "version": "intraday_multisignal_v1",
        "candidate_count": len(output),
        "market_snapshot": market_snapshot,
        "top_candidates": output[:10],
        "notes": [
            "盘中候选当前基于 热度榜、飙升榜、热门池、冲涨池、qxlive 顶部指标 与可用的资金流向表做多信号排序。",
            "盘中场景坚持宁缺毋滥，不使用同日旧 capture 冒充最新实时数据；若实时抓取失败，应真实暴露失败。",
        ],
    }


def parse_percent_value(value: Any) -> float:
    text = str(value or "").strip().replace("%", "")
    return safe_float(text, 0.0)


def infer_target_board(group_name: str) -> int:
    name = str(group_name or "").strip()
    if not name:
        return 0
    if name == "首板":
        return 1
    match = re.search(r"(\d+)进(\d+)", name)
    if match:
        return safe_int(match.group(2), 0)
    match = re.search(r"(\d+)板", name)
    if match:
        return safe_int(match.group(1), 0)
    return 0


def build_postmarket_analysis(report: Dict[str, Any]) -> Dict[str, Any]:
    items_by_id = {item.get("dataset_id"): item for item in report.get("items", [])}
    generated_at = str(report.get("generated_at") or "").strip()
    report_date = generated_at[:10] if len(generated_at) >= 10 else ""

    def resolve_rows(dataset_id: str, extra_dates: List[str] | None = None) -> Tuple[List[Dict[str, Any]], bool]:
        item = items_by_id.get(dataset_id) or {}
        capture_path = str(item.get("capture_path") or "").strip()
        if capture_path:
            rows = load_capture_rows(capture_path)
            if rows:
                return rows, False
        ordered_dates: List[str] = []
        for candidate_date in [report_date, *(extra_dates or [])]:
            if candidate_date and candidate_date not in ordered_dates:
                ordered_dates.append(candidate_date)
        for candidate_date in ordered_dates:
            fallback_path = resolve_latest_capture_path_for_date(dataset_id, candidate_date)
            if fallback_path:
                rows = load_capture_rows(fallback_path)
                if rows:
                    return rows, True
        return [], False

    fallback_used_by: List[str] = []
    required_ids = [
        "review.daily.top_metrics",
        "review.ltgd.range",
        "review.fupan.plate",
        "home.ztpool",
    ]
    required_rows: Dict[str, List[Dict[str, Any]]] = {}
    unavailable: List[str] = []
    for dataset_id in required_ids:
        rows, used_fallback = resolve_rows(dataset_id)
        if used_fallback:
            fallback_used_by.append(dataset_id)
        if not rows:
            unavailable.append(dataset_id)
        required_rows[dataset_id] = rows
    if unavailable:
        return {
            "enabled": False,
            "version": "postmarket_ztpool_v1",
            "reason": f"required rows unavailable: {', '.join(unavailable)}",
            "strong_up_candidates": [],
            "weak_to_strong_candidates": [],
            "risk_watch": [],
            "market_snapshot": {
                "情绪指标": "",
                "涨停家数": "",
                "跌停家数": "",
                "连板高度": "",
                "上涨家数": "",
                "下跌家数": "",
            },
        }

    review_daily_rows = required_rows["review.daily.top_metrics"]
    review_ltgd_rows = required_rows["review.ltgd.range"]
    review_plate_rows = required_rows["review.fupan.plate"]
    ztpool_rows = required_rows["home.ztpool"]

    analysis_trade_date = ""
    for rows, key in [
        (review_plate_rows, "日期"),
        (review_daily_rows, "date"),
        (ztpool_rows, "日期"),
    ]:
        if analysis_trade_date:
            break
        for row in rows:
            parsed = safe_date(row.get(key))
            if parsed is not None:
                analysis_trade_date = parsed.isoformat()
                break

    capture_trade_date = ""
    for row in ztpool_rows:
        parsed = safe_date(row.get("日期"))
        if parsed is not None:
            capture_trade_date = parsed.isoformat()
            break

    popularity_dates = [capture_trade_date, report_date, analysis_trade_date]

    def build_rank_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            code = normalize_code(row.get("代码") or row.get("code"))
            if not code or code in result:
                continue
            result[code] = {
                "rank": safe_int(row.get("排名") or row.get("rank"), 999),
                "value": str(row.get("value") or row.get("热度值") or row.get("飙升值") or "").strip(),
                "raw_rate": safe_float(row.get("raw_rate"), 0.0),
            }
        return result

    def build_presence_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            code = normalize_code(row.get("代码") or row.get("code"))
            if not code or code in result:
                continue
            result[code] = row
        return result

    rocket_rows, used_fallback = resolve_rows("rank.rocket", extra_dates=popularity_dates)
    if used_fallback:
        fallback_used_by.append("rank.rocket")
    hotlist_rows, used_fallback = resolve_rows("rank.hot_stock_day", extra_dates=popularity_dates)
    if used_fallback:
        fallback_used_by.append("rank.hot_stock_day")
    hot_pool_rows, used_fallback = resolve_rows("pool.hot", extra_dates=popularity_dates)
    if used_fallback:
        fallback_used_by.append("pool.hot")
    surge_pool_rows, used_fallback = resolve_rows("pool.surge", extra_dates=popularity_dates)
    if used_fallback:
        fallback_used_by.append("pool.surge")

    rocket_map = build_rank_map(rocket_rows)
    hotlist_map = build_rank_map(hotlist_rows)
    hot_pool_map = build_presence_map(hot_pool_rows)
    surge_pool_map = build_presence_map(surge_pool_rows)

    ztpool_codes = {
        normalize_code(row.get("代码"))
        for row in ztpool_rows
        if normalize_code(row.get("代码"))
    }
    popularity_coverage = {
        "ztpool_count": len(ztpool_codes),
        "rocket_overlap": sum(1 for code in ztpool_codes if code in rocket_map),
        "hot_stock_day_overlap": sum(1 for code in ztpool_codes if code in hotlist_map),
        "pool_hot_overlap": sum(1 for code in ztpool_codes if code in hot_pool_map),
        "pool_surge_overlap": sum(1 for code in ztpool_codes if code in surge_pool_map),
    }

    def coverage_scale(overlap: int, target_ratio: float, min_scale: float) -> float:
        total = max(1, popularity_coverage["ztpool_count"])
        ratio = overlap / total
        if target_ratio <= 0:
            return 1.0
        return round(min(1.0, max(min_scale, ratio / target_ratio)), 2)

    popularity_weights = {
        "rocket": coverage_scale(popularity_coverage["rocket_overlap"], 0.08, 0.15),
        "hot_stock_day": coverage_scale(popularity_coverage["hot_stock_day_overlap"], 0.12, 0.65),
        "pool_hot": coverage_scale(popularity_coverage["pool_hot_overlap"], 0.08, 0.35),
        "pool_surge": coverage_scale(popularity_coverage["pool_surge_overlap"], 0.05, 0.25),
    }

    dailyline_cache: Dict[str, Dict[str, Any]] = {}

    def get_dailyline_snapshot(code: str) -> Dict[str, Any]:
        code = zero_pad_stock_code(code)
        if not code:
            return {}
        cached = dailyline_cache.get(code)
        if cached is not None:
            return cached
        if not analysis_trade_date:
            dailyline_cache[code] = {}
            return {}

        path = DAILYLINE_STOCK_ROOT / f"{code}.csv"
        if not path.exists():
            dailyline_cache[code] = {}
            return {}

        try:
            with path.open("r", encoding="utf-8", newline="") as fp:
                rows = list(csv.DictReader(fp))
        except Exception:
            dailyline_cache[code] = {}
            return {}

        idx = next((i for i, row in enumerate(rows) if str(row.get("date") or "") == analysis_trade_date), -1)
        if idx < 0:
            dailyline_cache[code] = {}
            return {}

        row = rows[idx]
        closes = [safe_float(item.get("close"), 0.0) for item in rows]
        highs = [safe_float(item.get("high"), 0.0) for item in rows]
        lows = [safe_float(item.get("low"), 0.0) for item in rows]
        volumes = [safe_float(item.get("volume"), 0.0) for item in rows]

        def rolling_mean(series: List[float], end_idx: int, window: int) -> float | None:
            start = max(0, end_idx - window + 1)
            chunk = series[start : end_idx + 1]
            if len(chunk) < window:
                return None
            return sum(chunk) / len(chunk)

        def avg(series: List[float]) -> float | None:
            if not series:
                return None
            return sum(series) / len(series)

        close = safe_float(row.get("close"), 0.0)
        open_price = safe_float(row.get("open"), 0.0)
        high = safe_float(row.get("high"), 0.0)
        low = safe_float(row.get("low"), 0.0)
        preclose = safe_float(row.get("preclose"), 0.0)
        volume = safe_float(row.get("volume"), 0.0)
        amount = safe_float(row.get("amount"), 0.0)
        turn = safe_float(row.get("turn"), 0.0)
        pct_chg = safe_float(row.get("pctChg"), 0.0)

        ma5 = rolling_mean(closes, idx, 5)
        ma10 = rolling_mean(closes, idx, 10)
        ma20 = rolling_mean(closes, idx, 20)
        prev5_vol = avg(volumes[max(0, idx - 5) : idx]) if idx > 0 else None
        recent20_high = max(highs[max(0, idx - 19) : idx + 1]) if highs[max(0, idx - 19) : idx + 1] else high
        recent20_low = min(lows[max(0, idx - 19) : idx + 1]) if lows[max(0, idx - 19) : idx + 1] else low
        close_near_high = high > 0 and (high - close) / high <= 0.015
        upper_shadow_pct = round(((high - max(open_price, close)) / preclose) * 100, 2) if preclose > 0 else 0.0
        body_pct = round(((close - open_price) / preclose) * 100, 2) if preclose > 0 else 0.0
        volume_ratio_5 = round(volume / prev5_vol, 2) if prev5_vol and prev5_vol > 0 else None

        snapshot = {
            "trade_date": analysis_trade_date,
            "pct_chg": round(pct_chg, 2),
            "turn": round(turn, 2),
            "amount": round(amount, 2),
            "ma5": round(ma5, 4) if ma5 is not None else None,
            "ma10": round(ma10, 4) if ma10 is not None else None,
            "ma20": round(ma20, 4) if ma20 is not None else None,
            "volume_ratio_5": volume_ratio_5,
            "recent20_high": round(recent20_high, 4),
            "recent20_low": round(recent20_low, 4),
            "close_near_high": close_near_high,
            "upper_shadow_pct": upper_shadow_pct,
            "body_pct": body_pct,
            "breakout_20": close >= recent20_high - 1e-9,
            "above_ma5": ma5 is not None and close >= ma5,
            "above_ma10": ma10 is not None and close >= ma10,
            "above_ma20": ma20 is not None and close >= ma20,
        }
        dailyline_cache[code] = snapshot
        return snapshot

    def evaluate_dailyline_signal(snapshot: Dict[str, Any]) -> Tuple[float, List[str], List[str]]:
        if not snapshot:
            return 0.0, [], []
        score = 0.0
        reasons: List[str] = []
        risks: List[str] = []

        if snapshot.get("breakout_20"):
            score += 1.8
            reasons.append("日线收盘创20日新高")

        if snapshot.get("above_ma5") and snapshot.get("above_ma10") and snapshot.get("above_ma20"):
            score += 1.8
            reasons.append("日线站上5/10/20日线")
        elif snapshot.get("above_ma10") and snapshot.get("above_ma20"):
            score += 1.0
            reasons.append("日线仍在10/20日线上方")
        else:
            if snapshot.get("above_ma10") is False:
                score -= 0.8
                risks.append("日线尚未稳住10日线")
            if snapshot.get("above_ma20") is False:
                score -= 1.1
                risks.append("日线尚未站稳20日线")

        volume_ratio_5 = snapshot.get("volume_ratio_5")
        if volume_ratio_5 is not None:
            if volume_ratio_5 >= 1.8:
                score += 1.6
                reasons.append(f"日线显著放量 {volume_ratio_5}x")
            elif volume_ratio_5 >= 1.3:
                score += 0.9
                reasons.append(f"日线温和放量 {volume_ratio_5}x")
            elif volume_ratio_5 < 0.75:
                score -= 0.9
                risks.append("量能较5日均量明显不足")

        if snapshot.get("close_near_high"):
            score += 0.7
            reasons.append("收盘接近全天高点")

        body_pct = safe_float(snapshot.get("body_pct"), 0.0)
        if body_pct >= 3.0:
            score += 0.8
            reasons.append("实体阳线较扎实")
        elif body_pct <= 0:
            score -= 0.8
            risks.append("实体偏弱")

        upper_shadow_pct = safe_float(snapshot.get("upper_shadow_pct"), 0.0)
        if upper_shadow_pct >= 3.0:
            score -= 1.0
            risks.append("上影偏长，追涨性价比下降")
        elif upper_shadow_pct <= 1.0 and body_pct > 0:
            score += 0.4

        turn = safe_float(snapshot.get("turn"), 0.0)
        if 6.0 <= turn <= 28.0:
            score += 0.5
            reasons.append(f"换手 {turn:.2f}%")
        elif 0 < turn < 2.0:
            score -= 0.4
            risks.append("换手偏低，筹码换手不够")

        return round(score, 2), reasons[:3], risks[:3]

    def evaluate_popularity_signal(code: str) -> Tuple[float, List[str], Dict[str, Any]]:
        score = 0.0
        reasons: List[str] = []
        detail = {
            "rocket_rank": None,
            "hot_rank": None,
            "hot_pool": code in hot_pool_map,
            "surge_pool": code in surge_pool_map,
            "weights": popularity_weights,
        }

        rocket = rocket_map.get(code)
        if rocket:
            rank = safe_int(rocket.get("rank"), 999)
            detail["rocket_rank"] = rank
            rocket_score = 0.0
            if rank <= 10:
                rocket_score = 1.8
            elif rank <= 30:
                rocket_score = 1.2
            elif rank <= 50:
                rocket_score = 0.6
            score += rocket_score * popularity_weights["rocket"]
            if rank <= 50 and rocket_score > 0:
                reasons.append(f"飙升榜前{rank}")

        hot_rank_item = hotlist_map.get(code)
        if hot_rank_item:
            rank = safe_int(hot_rank_item.get("rank"), 999)
            detail["hot_rank"] = rank
            hot_score = 0.0
            if rank <= 10:
                hot_score = 2.2
            elif rank <= 30:
                hot_score = 1.6
            elif rank <= 50:
                hot_score = 0.9
            score += hot_score * popularity_weights["hot_stock_day"]
            if rank <= 50 and hot_score > 0:
                reasons.append(f"热度榜前{rank}")

        if code in hot_pool_map:
            score += 0.8 * popularity_weights["pool_hot"]
            reasons.append("热门池活跃")
        if code in surge_pool_map:
            score += 1.0 * popularity_weights["pool_surge"]
            reasons.append("冲涨池活跃")

        return round(score, 2), reasons[:3], detail

    def infer_missing_confirmations(
        target_board: int,
        cashflow_hits: List[str],
        dailyline_snapshot: Dict[str, Any],
        dailyline_score: float,
        popularity_detail: Dict[str, Any],
    ) -> List[str]:
        missing: List[str] = []
        if not cashflow_hits:
            missing.append("资金确认")
        hot_rank = safe_int(popularity_detail.get("hot_rank"), 999) if popularity_detail.get("hot_rank") is not None else 999
        if hot_rank > 50 and not popularity_detail.get("hot_pool") and not popularity_detail.get("surge_pool"):
            missing.append("人气确认")
        if target_board in {2, 3} and dailyline_score < 5.0:
            missing.append("更强日线结构")
        if dailyline_snapshot and not dailyline_snapshot.get("breakout_20") and target_board in {2, 3}:
            missing.append("突破确认")
        return missing[:3]

    cashflow_ids = [
        "cashflow.stock.today",
        "cashflow.stock.3day",
        "cashflow.stock.5day",
        "cashflow.stock.10day",
    ]
    cashflow_maps: Dict[str, List[str]] = {}
    for dataset_id in cashflow_ids:
        rows, used_fallback = resolve_rows(dataset_id)
        if used_fallback:
            fallback_used_by.append(dataset_id)
        for row in rows:
            code = normalize_code(row.get("代码") or row.get("code"))
            if not code:
                continue
            label = dataset_id.replace("cashflow.stock.", "")
            labels = cashflow_maps.setdefault(code, [])
            if label not in labels:
                labels.append(label)

    ltgd_map: Dict[str, List[str]] = {}
    for row in review_ltgd_rows:
        code = normalize_code(row.get("代码") or row.get("code"))
        if not code:
            continue
        desc = f"{row.get('周期', '')}/{row.get('板块', '')}/涨幅{row.get('区间涨幅', '')}"
        refs = ltgd_map.setdefault(code, [])
        if desc not in refs:
            refs.append(desc)

    hot_topic_scores: Dict[str, float] = {}
    for row in review_plate_rows:
        topic = str(row.get("题材名称") or "").strip()
        if not topic:
            continue
        score = hot_topic_scores.get(topic, 0.0)
        score += max(1.0, safe_float(row.get("题材涨停数"), 0.0) * 0.2)
        score += max(0.0, safe_float(row.get("板数") or 0.0, 0.0) * 0.1)
        hot_topic_scores[topic] = round(score, 2)

    daily_metric_map: Dict[str, str] = {}
    for row in review_daily_rows:
        label = str(row.get("display_label") or row.get("metric_label") or row.get("metric_key") or "").strip()
        if label and label not in daily_metric_map:
            daily_metric_map[label] = str(row.get("display_rate") or row.get("value") or "").strip()

    market_snapshot = {
        "情绪指标": daily_metric_map.get("情绪指标", ""),
        "涨停家数": daily_metric_map.get("涨停家数", ""),
        "跌停家数": daily_metric_map.get("跌停家数", ""),
        "连板高度": daily_metric_map.get("连板高度", ""),
        "上涨家数": daily_metric_map.get("上涨家数", ""),
        "下跌家数": daily_metric_map.get("下跌家数", ""),
    }

    def find_topic_score(theme_text: str) -> tuple[float, List[str]]:
        text = str(theme_text or "").strip()
        if not text:
            return 0.0, []
        matched: List[str] = []
        total = 0.0
        for topic, score in hot_topic_scores.items():
            if topic and topic in text:
                matched.append(topic)
                total += score
        return round(min(6.0, total), 2), matched[:3]

    strong_up: List[Dict[str, Any]] = []
    weak_to_strong: List[Dict[str, Any]] = []
    risk_watch: List[Dict[str, Any]] = []

    for row in ztpool_rows:
        code = normalize_code(row.get("代码"))
        if not code:
            continue
        name = str(row.get("名称") or "").strip()
        status = str(row.get("状态") or "").strip()
        group_name = str(row.get("分组名称") or "").strip()
        target_board = infer_target_board(group_name)
        gain = parse_percent_value(row.get("涨幅"))
        advance_rate = parse_percent_value(row.get("晋级率"))
        theme_text = str(row.get("题材") or "").strip()
        cashflow_hits = cashflow_maps.get(code, [])
        ltgd_refs = ltgd_map.get(code, [])
        topic_score, topic_matches = find_topic_score(theme_text)
        dailyline_snapshot = get_dailyline_snapshot(code)
        dailyline_score, dailyline_reasons, dailyline_risks = evaluate_dailyline_signal(dailyline_snapshot)
        popularity_score, popularity_reasons, popularity_detail = evaluate_popularity_signal(code)

        reasons = [
            f"分组 {group_name}（晋级率 {row.get('晋级率文本', '')}）",
        ]
        if theme_text:
            reasons.append(f"题材 {theme_text}")
        if cashflow_hits:
            reasons.append(f"资金榜命中 {'/'.join(cashflow_hits)}")
        if ltgd_refs:
            reasons.append(f"龙头区间参考 {ltgd_refs[0]}")
        if topic_matches:
            reasons.append(f"复盘热点命中 {'/'.join(topic_matches)}")
        reasons.extend(dailyline_reasons)
        reasons.extend(popularity_reasons)

        risks: List[str] = []
        if target_board >= 5:
            risks.append("连板位置已高，次日更容易高开分歧")
        if status in {"炸", "败"} and gain <= 0:
            risks.append("当日未封住且收盘转弱，次日修复难度更高")
        if not cashflow_hits:
            risks.append("未命中资金流向榜，资金确认偏弱")
        risks.extend(dailyline_risks)
        if status == "成" and target_board <= 1 and not cashflow_hits:
            risks.append("首板更多依赖题材延续，若次日无增量资金接力，持续性容易打折")
        if status == "成" and target_board in {2, 3, 4} and not cashflow_hits:
            risks.append("连板晋级虽然成立，但缺少资金榜确认，次日溢价持续性仍要打问号")
        if status in {"炸", "败"} and gain > 0 and not cashflow_hits:
            risks.append("虽然尾盘仍有承接，但缺少资金榜确认，弱转强确定性一般")
        if popularity_detail.get("hot_rank") and not cashflow_hits and dailyline_score <= 0:
            risks.append("有人气但日线量价形态未同步强化，容易先手兑现")

        base = {
            "name": name,
            "code": code,
            "status": status,
            "group_name": group_name,
            "target_board": target_board,
            "gain": round(gain, 2),
            "advance_rate": row.get("晋级率", ""),
            "advance_rate_text": row.get("晋级率文本", ""),
            "theme": theme_text,
            "cashflow_hits": cashflow_hits,
            "ltgd_refs": ltgd_refs[:2],
            "topic_matches": topic_matches,
            "dailyline_snapshot": dailyline_snapshot,
            "popularity": popularity_detail,
            "reasons": reasons[:7],
            "risks": risks[:3],
            "signal_profile": [],
            "missing_confirmations": infer_missing_confirmations(
                target_board,
                cashflow_hits,
                dailyline_snapshot,
                dailyline_score,
                popularity_detail,
            ),
        }

        if status == "成":
            score = 10.0
            score += max(0.0, 6.0 - abs(target_board - 3) * 1.5)
            score += min(4.0, max(0.0, gain) / 2.5)
            score += min(4.0, advance_rate / 20.0)
            score += min(4.0, len(cashflow_hits) * 1.2)
            score += topic_score
            score += dailyline_score
            score += popularity_score
            if target_board <= 1 and advance_rate >= 70:
                score -= 1.0
            if target_board in {2, 3, 4}:
                score += 1.2
            if target_board in {2, 3} and advance_rate <= 15:
                score += 1.2
            elif target_board in {2, 3} and advance_rate <= 25:
                score += 0.6
            if target_board in {2, 3} and dailyline_score >= 2.5:
                score += 0.8
            if target_board in {2, 3} and dailyline_score >= 4.0 and not cashflow_hits and popularity_score <= 1.0:
                score += 0.8
            if target_board in {2, 3} and dailyline_score >= 6.0 and not cashflow_hits and popularity_score <= 0.5:
                score += 1.0
                reasons.append("低关注但日线结构极强")
            if target_board in {2, 3} and (
                cashflow_hits
                or (popularity_detail.get("hot_rank") and safe_int(popularity_detail.get("hot_rank"), 999) <= 30)
                or popularity_detail.get("hot_pool")
                or popularity_detail.get("surge_pool")
            ):
                score += 0.8
            if target_board in {2, 3, 4} and popularity_detail.get("hot_rank") and safe_int(popularity_detail.get("hot_rank"), 999) <= 20:
                score += 0.6
            if target_board in {2, 3, 4} and not cashflow_hits:
                score -= 1.4
            if target_board in {2, 3, 4} and not cashflow_hits and not ltgd_refs:
                score -= 0.8
            if target_board <= 1:
                score -= 0.5
            if target_board <= 1 and not popularity_detail.get("hot_rank") and not popularity_detail.get("rocket_rank"):
                score -= 0.5
            if target_board <= 1 and dailyline_snapshot and not dailyline_snapshot.get("breakout_20"):
                score -= 0.6
            if target_board <= 1 and not cashflow_hits:
                score -= 2.6
            if target_board <= 1 and not cashflow_hits and not ltgd_refs:
                score -= 1.2
            if target_board <= 1 and popularity_score > 0 and dailyline_score <= 0 and not cashflow_hits:
                score -= 0.8
            signal_profile: List[str] = []
            if target_board in {2, 3, 4}:
                signal_profile.append("连板成功")
            elif target_board <= 1:
                signal_profile.append("首板成功")
            if advance_rate <= 15 and target_board in {2, 3}:
                signal_profile.append("低晋级率突围")
            if dailyline_snapshot.get("breakout_20"):
                signal_profile.append("20日突破")
            if dailyline_score >= 5.0:
                signal_profile.append("日线结构强")
            elif dailyline_score >= 2.5:
                signal_profile.append("日线结构良好")
            if cashflow_hits:
                signal_profile.append("资金确认")
            if popularity_detail.get("hot_rank") and safe_int(popularity_detail.get("hot_rank"), 999) <= 30:
                signal_profile.append("热度确认")
            elif popularity_detail.get("hot_pool") or popularity_detail.get("surge_pool"):
                signal_profile.append("活跃池确认")
            if not cashflow_hits and popularity_score <= 0.5 and dailyline_score >= 6.0 and target_board in {2, 3}:
                signal_profile.append("低关注高结构")

            item = {**base, "score": round(score, 2), "signal_profile": signal_profile[:5]}
            strong_up.append(item)
            if target_board >= 4:
                risk_watch.append({**item, "watch_reason": "高位成功板，次日更像强上强观察而不是无脑追"})
            elif target_board in {2, 3, 4} and not cashflow_hits:
                risk_watch.append({**item, "watch_reason": "连板晋级成立但资金确认不足，更适合等次日承接与回流确认"})
            elif target_board <= 1 and not cashflow_hits:
                risk_watch.append({**item, "watch_reason": "首板成功但资金确认不足，更适合观察次日是否有接力回流"})
            continue

        if status in {"炸", "败"}:
            score = 6.0 if status == "炸" else 4.5
            score += max(0.0, 5.0 - abs(target_board - 2) * 1.6)
            if gain > 0:
                score += min(4.0, gain / 2.0)
            elif gain > -2:
                score += 1.0
            score += min(4.0, len(cashflow_hits) * 1.2)
            score += topic_score
            score += max(-1.5, round(dailyline_score * 0.8, 2))
            score += round(popularity_score * 0.85, 2)
            if target_board in {2, 3} and dailyline_score >= 2.0:
                score += 0.6
            if popularity_detail.get("hot_rank") and safe_int(popularity_detail.get("hot_rank"), 999) <= 20:
                score += 0.4
            if target_board <= 1 and not cashflow_hits:
                score -= 1.8
            if gain <= 0:
                score -= 1.2
            signal_profile = []
            if target_board in {2, 3, 4}:
                signal_profile.append("连板尝试")
            elif target_board <= 1:
                signal_profile.append("首板尝试")
            if advance_rate <= 15 and target_board in {2, 3}:
                signal_profile.append("低晋级率环境")
            if dailyline_score >= 5.0:
                signal_profile.append("日线结构强")
            elif dailyline_score >= 2.5:
                signal_profile.append("日线结构良好")
            if cashflow_hits:
                signal_profile.append("资金确认")
            if popularity_detail.get("hot_rank") and safe_int(popularity_detail.get("hot_rank"), 999) <= 30:
                signal_profile.append("热度确认")
            elif popularity_detail.get("hot_pool") or popularity_detail.get("surge_pool"):
                signal_profile.append("活跃池确认")
            item = {**base, "score": round(score, 2), "signal_profile": signal_profile[:5]}
            weak_to_strong.append(item)
            if gain <= 0 or not cashflow_hits:
                risk_watch.append({**item, "watch_reason": "炸板/失败后承接不够，优先放入风险观察"})

    strong_up.sort(key=lambda item: (-safe_float(item.get("score"), 0.0), -safe_float(item.get("gain"), 0.0), item.get("code", "")))
    weak_to_strong.sort(key=lambda item: (-safe_float(item.get("score"), 0.0), -safe_float(item.get("gain"), 0.0), item.get("code", "")))
    risk_watch.sort(key=lambda item: (-safe_float(item.get("target_board"), 0.0), -abs(safe_float(item.get("gain"), 0.0)), item.get("code", "")))

    strong_up_top = strong_up[:8]
    strong_up_codes = {item.get("code") for item in strong_up_top}
    strong_up_cutoff_score = safe_float(strong_up_top[-1].get("score"), 0.0) if strong_up_top else 0.0
    missed_continuation_watch = [
        {
            "name": item.get("name"),
            "code": item.get("code"),
            "group_name": item.get("group_name"),
            "score": item.get("score"),
            "gap_to_strong_up": round(max(0.0, strong_up_cutoff_score - safe_float(item.get("score"), 0.0)), 2),
            "signal_profile": item.get("signal_profile", []),
            "missing_confirmations": item.get("missing_confirmations", []),
            "reasons": item.get("reasons", [])[:5],
            "risks": item.get("risks", [])[:3],
        }
        for item in strong_up
        if item.get("target_board") in {2, 3, 4}
        and item.get("code") not in strong_up_codes
    ][:5]
    quasi_strong_up_watch = [
        item
        for item in missed_continuation_watch
        if safe_float(item.get("gap_to_strong_up"), 999.0) <= 0.5
        and all(conf in {"资金确认", "人气确认"} for conf in item.get("missing_confirmations", []))
    ][:3]
    quasi_codes = {str(item.get("code") or "") for item in quasi_strong_up_watch}
    residual_missed_continuation_watch = [
        item for item in missed_continuation_watch
        if str(item.get("code") or "") not in quasi_codes
    ]

    dedup_watch: List[Dict[str, Any]] = []
    seen_watch = set()
    for item in risk_watch:
        code = item.get("code")
        if code in seen_watch:
            continue
        seen_watch.add(code)
        dedup_watch.append(item)

    notes = [
        "盘后分析当前以 home.ztpool 为主候选池，并分成 强上强 / 弱转强 / 风险观察 三段输出。",
        "强上强优先看 状态=成 的晋级成功票，再结合 晋级率、收盘涨幅、资金榜命中、日线量价形态 与复盘热点强度排序。",
        "弱转强优先看 状态 in {炸, 败} 但收盘承接未明显走坏、且仍有资金/热点/人气确认的票。",
        "高位成功板、炸板后走弱票、以及缺少资金确认的票，会额外进入 风险观察。",
        "本轮新增把 `rank.rocket`、`rank.hot_stock_day`、可用的 `pool.hot/pool.surge` 与本地日线数据一起并入打分，不再只看板位和资金流。",
    ]
    ordered_fallbacks: List[str] = []
    seen_fallbacks = set()
    for dataset_id in fallback_used_by:
        if dataset_id in seen_fallbacks:
            continue
        seen_fallbacks.add(dataset_id)
        ordered_fallbacks.append(dataset_id)

    if ordered_fallbacks:
        notes.append(
            "本次分析使用了同日最新成功 capture 兜底：" + ", ".join(ordered_fallbacks) + "，避免单次抓取超时导致盘后分析整体失真或为空。"
        )
    if popularity_coverage["rocket_overlap"] <= max(1, popularity_coverage["ztpool_count"] // 50):
        notes.append(
            "本轮盘后涨停池与飙升榜重合度较低，因此 `rank.rocket` 当前更多作为补充信号，主人气确认仍以 `rank.hot_stock_day` 及可用的 `pool.hot/pool.surge` 为主。"
        )
    notes.append(
        "near-miss 字段说明：`quasi_strong_up_watch` 为几乎上位的准强上强层；`missed_continuation_watch` 为去重后的剩余 near-miss；`all_missed_continuation_watch` 为完整 near-miss 全集。"
    )

    return {
        "enabled": True,
        "version": "postmarket_ztpool_v2",
        "analysis_trade_date": analysis_trade_date,
        "capture_trade_date": capture_trade_date,
        "fallback_datasets": ordered_fallbacks,
        "popularity_coverage": popularity_coverage,
        "popularity_weights": popularity_weights,
        "market_snapshot": market_snapshot,
        "strong_up_cutoff_score": round(strong_up_cutoff_score, 2),
        "strong_up_candidates": strong_up_top,
        "weak_to_strong_candidates": weak_to_strong[:8],
        "risk_watch": dedup_watch[:8],
        "quasi_strong_up_watch": quasi_strong_up_watch,
        "missed_continuation_watch": residual_missed_continuation_watch,
        "all_missed_continuation_watch": missed_continuation_watch,
        "notes": notes,
    }


def load_capture_payload(capture_path: str) -> Dict[str, Any]:
    if not capture_path:
        return {}
    path = Path(capture_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_latest_capture_path_for_date(dataset_id: str, target_date: str) -> str:
    dataset_dir = CAPTURE_ROOT / target_date / dataset_id
    if not dataset_dir.exists():
        return ""
    files = sorted(dataset_dir.glob("*.json"))
    if not files:
        return ""
    return str(files[-1])


def iter_report_files_for_date(target_date: str) -> List[Path]:
    root = REPORT_ROOT / target_date
    if not root.exists():
        return []
    files: List[Path] = []
    for group_dir in sorted(root.iterdir()):
        if not group_dir.is_dir():
            continue
        files.extend(sorted(group_dir.glob("*.json")))
    return files


def parse_report_generated_at(report: Dict[str, Any], path: Path) -> datetime:
    text = str(report.get("generated_at") or "").strip()
    if text:
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass
    return datetime.fromtimestamp(path.stat().st_mtime, tz=TZ_SHANGHAI)


def collect_official_capture_paths_for_date(target_date: str) -> List[str]:
    latest_by_group: Dict[str, Tuple[datetime, List[str]]] = {}
    for report_path in iter_report_files_for_date(target_date):
        try:
            report = load_report(str(report_path))
        except Exception:
            continue
        group = str(report.get("group") or "").strip()
        if not group:
            continue
        capture_paths = []
        for item in report.get("items", []):
            capture_path = str(item.get("capture_path") or "").strip()
            dataset_id = str(item.get("dataset_id") or "").strip()
            if not capture_path or dataset_id in DAILYLINE_CAPTURE_EXCLUDED:
                continue
            if "/_quarantine" in capture_path or "_quarantine_" in capture_path:
                continue
            capture_paths.append(capture_path)
        generated_at = parse_report_generated_at(report, report_path)
        prev = latest_by_group.get(group)
        if prev is None or generated_at >= prev[0]:
            latest_by_group[group] = (generated_at, capture_paths)

    ordered: List[str] = []
    seen = set()
    for _, paths in sorted(latest_by_group.values(), key=lambda item: item[0]):
        for capture_path in paths:
            if capture_path in seen:
                continue
            seen.add(capture_path)
            ordered.append(capture_path)
    return ordered


def build_dailyline_stock_pool_from_captures(target_date: str) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
    stock_map: Dict[str, Dict[str, Any]] = {}
    capture_sources: Dict[str, List[str]] = {}
    capture_paths = collect_official_capture_paths_for_date(target_date)

    for capture_path in capture_paths:
        payload = load_capture_payload(capture_path)
        dataset_id = str(payload.get("dataset_id") or "").strip()
        if not dataset_id or dataset_id in DAILYLINE_CAPTURE_EXCLUDED:
            continue
        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw_code = row.get("code") if "code" in row else row.get("代码")
            code = zero_pad_stock_code(raw_code)
            if not is_supported_a_share_code(code):
                continue
            name = str(row.get("name") or row.get("名称") or "").strip()
            item = stock_map.get(code)
            if item is None:
                item = {
                    "股票代码": code,
                    "股票名称": name,
                    "baostock代码": to_baostock_code(code),
                    "来源数据集": [],
                    "前一交易日正式推荐": "否",
                }
                stock_map[code] = item
            elif not item.get("股票名称") and name:
                item["股票名称"] = name

            src_list = capture_sources.setdefault(code, [])
            if dataset_id not in src_list:
                src_list.append(dataset_id)

    for code, sources in capture_sources.items():
        stock_map[code]["来源数据集"] = sources

    rows = sorted(stock_map.values(), key=lambda item: (item["股票代码"], item.get("股票名称", "")))
    return rows, capture_sources


def fetch_all_bitable_records(meta_name: str = "duanxianxia_review") -> List[Dict[str, Any]]:
    meta = load_meta(meta_name)
    app_token = meta["app_token"]
    table_id = meta["table_id"]
    page_token = ""
    items: List[Dict[str, Any]] = []
    while True:
        query = {"page_size": 500}
        if page_token:
            query["page_token"] = page_token
        res = feishu_request("GET", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records", query=query)
        data = res.get("data") or {}
        batch = data.get("items") or []
        if isinstance(batch, list):
            items.extend(batch)
        if not data.get("has_more"):
            break
        page_token = str(data.get("page_token") or "").strip()
        if not page_token:
            break
    return items


def extract_record_date(fields: Dict[str, Any]) -> str:
    explicit = safe_date(fields.get("日期"))
    if explicit:
        return explicit.isoformat()
    text = str(fields.get("推荐时间") or "").strip()
    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    return match.group(1) if match else ""


def format_report_generated_time(report: Dict[str, Any], include_tz: bool = True) -> str:
    text = str(report.get("generated_at") or "").strip()
    if text:
        try:
            dt = datetime.fromisoformat(text)
            base = dt.strftime("%Y-%m-%d %H:%M")
            return f"{base} Asia/Shanghai" if include_tz else base
        except ValueError:
            pass
    now_cn = datetime.now(TZ_SHANGHAI)
    base = now_cn.strftime("%Y-%m-%d %H:%M")
    return f"{base} Asia/Shanghai" if include_tz else base


def report_natural_date(report: Dict[str, Any]) -> str:
    text = str(report.get("generated_at") or "").strip()
    if len(text) >= 10:
        parsed = safe_date(text[:10])
        if parsed is not None:
            return parsed.isoformat()
    return datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d")


def build_premarket_bitable_records(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    analysis = report.get("analysis", {}) if isinstance(report, dict) else {}
    top_candidates = analysis.get("top_candidates", []) if isinstance(analysis, dict) else []
    if not top_candidates:
        return []

    rec_date = report_natural_date(report)
    rec_time = format_report_generated_time(report, include_tz=True)
    rows: List[Dict[str, Any]] = []
    for cand in top_candidates[:10]:
        rows.append(
            {
                "日期": rec_date,
                "推荐时间": rec_time,
                "推荐场景": "盘前推荐",
                "股票代码": zero_pad_stock_code(cand.get("code")),
                "股票名称": str(cand.get("name") or "").strip(),
                "推荐分级": f"盘前Top{cand.get('rank')}",
                "推荐理由": "；".join((cand.get("reasons") or [])[:4]),
            }
        )
    return rows


def build_postmarket_bitable_records(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    analysis = report.get("analysis", {}) if isinstance(report, dict) else {}
    if not isinstance(analysis, dict):
        return []

    trade_date = str(analysis.get("analysis_trade_date") or "").strip() or report_natural_date(report)
    strong_up = analysis.get("strong_up_candidates", []) or []
    risk_watch = analysis.get("risk_watch", []) or []
    if not strong_up and not risk_watch:
        return []

    grade_cycle = ["盘后首选", "盘后次选", "盘后第三选择", "弹性备选", "弹性备选"]
    rows: List[Dict[str, Any]] = []
    used_codes = set()
    for idx, cand in enumerate(strong_up[:5]):
        code = zero_pad_stock_code(cand.get("code"))
        if not code:
            continue
        used_codes.add(code)
        reasons = list(cand.get("reasons") or [])[:5]
        rows.append(
            {
                "日期": trade_date,
                "推荐时间": f"{trade_date} 盘后",
                "推荐场景": "盘后复盘选股",
                "股票代码": code,
                "股票名称": str(cand.get("name") or "").strip(),
                "推荐分级": grade_cycle[min(idx, len(grade_cycle) - 1)],
                "推荐理由": "；".join(reasons),
            }
        )

    for cand in risk_watch:
        code = zero_pad_stock_code(cand.get("code"))
        if not code or code in used_codes:
            continue
        reason_parts = list(cand.get("reasons") or [])[:3]
        watch_reason = str(cand.get("watch_reason") or "").strip()
        if watch_reason:
            reason_parts.append(watch_reason)
        rows.append(
            {
                "日期": trade_date,
                "推荐时间": f"{trade_date} 盘后",
                "推荐场景": "盘后复盘选股",
                "股票代码": code,
                "股票名称": str(cand.get("name") or "").strip(),
                "推荐分级": "不建议追高",
                "推荐理由": "；".join(reason_parts[:5]),
            }
        )
        break

    return rows


def build_analysis_bitable_records(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    group = str(report.get("group") or "").strip()
    if group == "premarket":
        return build_premarket_bitable_records(report)
    if group in {"intraday", "intraday_cashflow"}:
        analysis = report.get("analysis", {}) if isinstance(report, dict) else {}
        top_candidates = analysis.get("top_candidates", []) if isinstance(analysis, dict) else []
        rec_date = report_natural_date(report)
        rec_time = format_report_generated_time(report, include_tz=True)
        rows: List[Dict[str, Any]] = []
        for cand in top_candidates[:5]:
            rank = safe_int(cand.get("rank"), 999)
            grade = "盘中首选"
            if rank == 2:
                grade = "盘中次选"
            elif rank == 3:
                grade = "盘中第三选择"
            elif rank >= 4:
                risk_text = "；".join((cand.get("risks") or [])[:1])
                grade = "谨慎观察" if risk_text else "强势观察"
            rows.append(
                {
                    "日期": rec_date,
                    "推荐时间": rec_time,
                    "推荐场景": "盘中联动推荐",
                    "股票代码": zero_pad_stock_code(cand.get("code")),
                    "股票名称": str(cand.get("name") or "").strip(),
                    "推荐分级": grade,
                    "推荐理由": "；".join((cand.get("reasons") or [])[:4]),
                }
            )
        return rows
    if group in {"postmarket", "postmarket_cashflow"}:
        return build_postmarket_bitable_records(report)
    return []


def sync_analysis_to_bitable(report: Dict[str, Any], meta_name: str = "duanxianxia_review") -> Dict[str, Any]:
    rows = build_analysis_bitable_records(report)
    if not rows:
        return {
            "enabled": False,
            "meta_name": meta_name,
            "created_count": 0,
            "skipped_existing_count": 0,
            "records": [],
            "reason": "no supported analysis records",
        }

    meta = load_meta(meta_name)
    records: List[Dict[str, Any]] = []
    created_count = 0
    skipped_existing_count = 0
    for fields in rows:
        key = (
            str(fields.get("日期") or "").strip(),
            str(fields.get("推荐场景") or "").strip(),
            zero_pad_stock_code(fields.get("股票代码")),
        )
        payload = dict(fields)
        payload["来源会话"] = payload.get("来源会话") or f"agent:main:feishu:direct:{os.getenv('OPENCLAW_USER_ID', '')}".strip(":")
        rec = create_record(meta["app_token"], meta["table_id"], payload)
        created_count += 1
        records.append(
            {
                "scene": key[1],
                "code": key[2],
                "name": fields.get("股票名称"),
                "record_id": rec.get("record_id"),
                "status": "created",
            }
        )

    return {
        "enabled": True,
        "meta_name": meta_name,
        "created_count": created_count,
        "skipped_existing_count": skipped_existing_count,
        "records": records,
    }


def load_previous_formal_recommendation_codes(prev_trade_date: str, meta_name: str = "duanxianxia_review") -> Dict[str, Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    for item in fetch_all_bitable_records(meta_name=meta_name):
        fields = item.get("fields") or {}
        if not isinstance(fields, dict):
            continue
        if extract_record_date(fields) != prev_trade_date:
            continue
        scene = str(fields.get("推荐场景") or "").strip()
        grade = str(fields.get("推荐分级") or "").strip()
        if not scene:
            continue
        if "不建议" in grade:
            continue
        code = zero_pad_stock_code(fields.get("股票代码"))
        if not is_supported_a_share_code(code):
            continue
        rows[code] = {
            "股票代码": code,
            "股票名称": str(fields.get("股票名称") or "").strip(),
            "推荐场景": scene,
            "推荐分级": grade,
            "推荐时间": str(fields.get("推荐时间") or "").strip(),
            "record_id": item.get("record_id", ""),
        }
    return rows


def get_trade_day_pair(target_date: str) -> Tuple[str, str]:
    try:
        import baostock as bs  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"BAOSTOCK_IMPORT_ERR {type(exc).__name__}: {exc}") from exc

    parsed = safe_date(target_date)
    if parsed is None:
        raise ValueError(f"invalid target date: {target_date}")

    login_res = bs.login()
    if getattr(login_res, "error_code", "0") != "0":
        raise RuntimeError(f"BAOSTOCK_LOGIN_ERR {login_res.error_code} {login_res.error_msg}")
    try:
        start = (parsed - timedelta(days=10)).isoformat()
        end = parsed.isoformat()
        rs = bs.query_trade_dates(start_date=start, end_date=end)
        if rs.error_code != "0":
            raise RuntimeError(f"BAOSTOCK_TRADE_DATES_ERR {rs.error_code} {rs.error_msg}")
        trading_days: List[str] = []
        while rs.next():
            row = rs.get_row_data()
            if len(row) >= 2 and row[1] == "1":
                trading_days.append(row[0])
        if not trading_days:
            raise RuntimeError(f"No trading day found up to {target_date}")
        effective = max(day for day in trading_days if day <= parsed.isoformat())
        prev_candidates = [day for day in trading_days if day < effective]
        if not prev_candidates:
            raise RuntimeError(f"No previous trading day found before {effective}")
        return effective, prev_candidates[-1]
    finally:
        bs.logout()


def read_existing_dailyline_csv(path: Path) -> Tuple[List[Dict[str, str]], str]:
    if not path.exists():
        return [], ""
    rows: List[Dict[str, str]] = []
    latest_date = ""
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            clean = {field: str(row.get(field, "") or "") for field in DAILYLINE_FIELDS}
            rows.append(clean)
            row_date = clean.get("date", "")
            if row_date and row_date > latest_date:
                latest_date = row_date
    return rows, latest_date


def append_dailyline_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=DAILYLINE_FIELDS)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in DAILYLINE_FIELDS})


class StockQueryTimeoutError(RuntimeError):
    pass


def _stock_query_timeout_handler(signum: int, frame: Any) -> None:  # noqa: ARG001
    raise StockQueryTimeoutError("baostock single-stock query timed out")


def download_dailyline_for_stock(bs: Any, stock: Dict[str, Any], end_date: str, start_date: str, retries: int = 2) -> Dict[str, Any]:
    code = str(stock.get("股票代码") or "")
    bs_code = str(stock.get("baostock代码") or "")
    path = DAILYLINE_STOCK_ROOT / f"{code}.csv"
    existing_rows, latest_existing_date = read_existing_dailyline_csv(path)

    fetch_start = start_date
    if latest_existing_date:
        next_date = safe_date(latest_existing_date)
        if next_date is not None:
            fetch_start = max(start_date, (next_date + timedelta(days=1)).isoformat())

    result = {
        "股票代码": code,
        "股票名称": stock.get("股票名称", ""),
        "baostock代码": bs_code,
        "来源数据集": ", ".join(stock.get("来源数据集", [])),
        "前一交易日正式推荐": stock.get("前一交易日正式推荐", "否"),
        "已有日线数": len(existing_rows),
        "新增日线数": 0,
        "最新日期": latest_existing_date,
        "状态": "跳过",
        "错误": "",
        "文件": str(path),
    }

    if not bs_code:
        result["状态"] = "失败"
        result["错误"] = "unsupported code"
        return result

    if fetch_start > end_date:
        result["状态"] = "已是最新"
        return result

    last_error = ""
    for attempt in range(retries + 1):
        previous_handler = signal.getsignal(signal.SIGALRM)
        try:
            signal.signal(signal.SIGALRM, _stock_query_timeout_handler)
            signal.alarm(25)
            rs = bs.query_history_k_data_plus(
                bs_code,
                ",".join(DAILYLINE_FIELDS),
                start_date=fetch_start,
                end_date=end_date,
                frequency="d",
                adjustflag="3",
            )
            if rs.error_code != "0":
                raise RuntimeError(f"{rs.error_code} {rs.error_msg}")
            new_rows: List[Dict[str, str]] = []
            while rs.next():
                row_data = rs.get_row_data()
                row = {field: row_data[idx] if idx < len(row_data) else "" for idx, field in enumerate(DAILYLINE_FIELDS)}
                if row.get("date"):
                    new_rows.append(row)
            if new_rows:
                append_dailyline_rows(path, new_rows)
                result["新增日线数"] = len(new_rows)
                result["最新日期"] = new_rows[-1].get("date", latest_existing_date)
                result["状态"] = "成功"
            else:
                result["状态"] = "无新增"
            return result
        except Exception as exc:  # noqa: BLE001
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < retries:
                time.sleep(1.0 + attempt)
                continue
        finally:
            try:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, previous_handler)
            except Exception:
                pass
    result["状态"] = "失败"
    result["错误"] = last_error
    return result


def build_dailyline_capture_payload(target_date: str, rows: List[Dict[str, Any]], meta: Dict[str, Any]) -> Dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    now_cn = now_utc.astimezone(TZ_SHANGHAI)
    return {
        "project": "duanxianxia",
        "dataset_kind": "dailyline_manifest",
        "dataset_id": DAILYLINE_MANIFEST_DATASET_ID,
        "dataset_label": DAILYLINE_MANIFEST_LABEL,
        "source_path": f"复盘/日线下载/{target_date}",
        "source_url": "baostock + Feishu Bitable",
        "fetched_at": now_cn.isoformat(timespec="seconds"),
        "fetched_at_utc": now_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "timezone": "Asia/Shanghai",
        "row_count": len(rows),
        "headers": infer_headers(rows),
        "rows": rows,
        "meta": meta,
    }


def run_dailyline_group(target_date: str, start_date: str, review_meta_name: str = "duanxianxia_review") -> Dict[str, Any]:
    effective_trade_date, prev_trade_date = get_trade_day_pair(target_date)
    pool_rows, _ = build_dailyline_stock_pool_from_captures(effective_trade_date)
    previous_formal = load_previous_formal_recommendation_codes(prev_trade_date, meta_name=review_meta_name)

    stock_map: Dict[str, Dict[str, Any]] = {row["股票代码"]: dict(row) for row in pool_rows}
    for code, info in previous_formal.items():
        item = stock_map.get(code)
        if item is None:
            item = {
                "股票代码": code,
                "股票名称": info.get("股票名称", ""),
                "baostock代码": to_baostock_code(code),
                "来源数据集": [],
                "前一交易日正式推荐": "是",
            }
            stock_map[code] = item
        else:
            item["前一交易日正式推荐"] = "是"
            if not item.get("股票名称") and info.get("股票名称"):
                item["股票名称"] = info["股票名称"]

    ordered_stocks = sorted(stock_map.values(), key=lambda item: item["股票代码"])

    try:
        import baostock as bs  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"BAOSTOCK_IMPORT_ERR {type(exc).__name__}: {exc}") from exc

    login_res = bs.login()
    if getattr(login_res, "error_code", "0") != "0":
        raise RuntimeError(f"BAOSTOCK_LOGIN_ERR {login_res.error_code} {login_res.error_msg}")
    try:
        manifest_rows = [download_dailyline_for_stock(bs, stock, effective_trade_date, start_date) for stock in ordered_stocks]
    finally:
        bs.logout()

    success_count = sum(1 for row in manifest_rows if row.get("状态") in {"成功", "无新增", "已是最新"})
    failure_rows = [row for row in manifest_rows if row.get("状态") == "失败"]
    new_bar_count = sum(safe_int(row.get("新增日线数"), 0) for row in manifest_rows)
    previous_formal_in_pool = sum(1 for row in manifest_rows if row.get("前一交易日正式推荐") == "是")

    payload = build_dailyline_capture_payload(
        effective_trade_date,
        manifest_rows,
        {
            "effective_trade_date": effective_trade_date,
            "previous_trade_date": prev_trade_date,
            "start_date": start_date,
            "pool_count": len(manifest_rows),
            "new_bar_count": new_bar_count,
            "failure_count": len(failure_rows),
            "previous_formal_recommendation_count": previous_formal_in_pool,
        },
    )
    capture_path = persist_capture(payload)

    item = {
        "seq": 17,
        "dataset": "dailyline_manifest",
        "dataset_id": DAILYLINE_MANIFEST_DATASET_ID,
        "dataset_label": DAILYLINE_MANIFEST_LABEL,
        "source_path": payload["source_path"],
        "source_url": payload["source_url"],
        "fetched_at": payload["fetched_at"],
        "row_count": payload["row_count"],
        "capture_path": str(capture_path),
        "saved": True,
        "success": len(failure_rows) == 0,
        "complete": True,
        "failed_items": [row["股票代码"] for row in failure_rows],
        "missing_items": [],
        "headers": payload.get("headers", []),
    }

    return {
        "project": "duanxianxia",
        "group": "dailyline",
        "group_label": GROUPS["dailyline"]["label"],
        "trigger": "manual_or_cron",
        "generated_at": payload["fetched_at"],
        "generated_at_utc": payload["fetched_at_utc"],
        "timezone": "Asia/Shanghai",
        "expected_count": 1,
        "success_count": 1 if item["success"] else 0,
        "failure_count": 0 if item["success"] else 1,
        "success": item["success"],
        "complete": True,
        "saved": True,
        "failed_items": item["failed_items"],
        "incomplete_items": [],
        "missing_items": [],
        "items": [item],
        "analysis": {
            "enabled": True,
            "version": "dailyline_v1",
            "effective_trade_date": effective_trade_date,
            "previous_trade_date": prev_trade_date,
            "pool_count": len(manifest_rows),
            "new_bar_count": new_bar_count,
            "failure_count": len(failure_rows),
            "previous_formal_recommendation_count": previous_formal_in_pool,
            "notes": [
                "股票池来自当日正式报告引用的盘前/盘中/盘后/资金流向 capture 去重汇总。",
                "盘后组里若已刷新 `rank.rocket` 与 `rank.hot_stock_day`，则默认使用盘后最终快照参与当日日线池构建。",
                "home.kaipan.plate.summary 与隔离的旧 qxlive 股票覆盖残留不会贡献股票代码。",
                "额外并入上一交易日飞书多维表中识别出的所有正式推荐股票（过滤‘不建议’类记录），不再只限盘后推荐。",
                "日线来源使用 baostock，frequency=d，adjustflag=3，不复权。",
            ],
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run duanxianxia grouped batch fetches")
    parser.add_argument("group", choices=sorted(GROUPS.keys()))
    parser.add_argument("--json", action="store_true", help="Output JSON only")
    parser.add_argument(
        "--target-date",
        default="",
        help="Target trade date for dailyline mode, defaults to today in Asia/Shanghai",
    )
    parser.add_argument(
        "--dailyline-start-date",
        default=DAILYLINE_START_DATE,
        help="Inclusive start date for baostock daily-line backfill",
    )
    parser.add_argument(
        "--review-meta-name",
        default="duanxianxia_review",
        help="Feishu Bitable meta name for prior postmarket recommendation lookup",
    )
    parser.add_argument(
        "--report-path",
        default="",
        help="Reuse an existing report JSON and rebuild analysis from its saved capture paths without fetching",
    )
    parser.add_argument(
        "--capture-only",
        action="store_true",
        help="Fetch and persist raw report items only, without building analysis, syncing bitable, or posting webhook",
    )
    parser.add_argument(
        "--save-analysis-copy",
        action="store_true",
        help="When used with --report-path, persist an updated analysis-only report copy instead of only printing",
    )
    parser.add_argument(
        "--webhook-url",
        default=os.getenv("DUANXIANXIA_WEBHOOK_URL", "").strip(),
        help="Optional external webhook URL for structured POST",
    )
    parser.add_argument(
        "--webhook-bearer",
        default=os.getenv("DUANXIANXIA_WEBHOOK_BEARER", "").strip(),
        help="Optional bearer token for webhook Authorization header",
    )
    parser.add_argument(
        "--webhook-secret",
        default=os.getenv("DUANXIANXIA_WEBHOOK_SECRET", "").strip(),
        help="Optional shared secret sent in X-Webhook-Secret header",
    )
    return parser


def run_dataset(fetcher: DuanxianxiaFetcher, dataset: str) -> Dict[str, Any]:
    if dataset == "rocket":
        result = fetcher.fetch_rocket()
    elif dataset == "hot":
        result = fetcher.fetch_hot()
    elif dataset == "surge":
        result = fetcher.fetch_surge()
    elif dataset == "hotlist_day":
        result = fetcher.fetch_hotlist_day()
    elif dataset == "review_daily":
        result = fetcher.fetch_review_daily()
    elif dataset == "review_daily_core11":
        result = fetcher.fetch_review_daily_core11()
    elif dataset == "home_qxlive_top_metrics":
        result = fetcher.fetch_home_qxlive_top_metrics()
    elif dataset == "home_ztpool":
        result = fetcher.fetch_home_ztpool()
    elif dataset == "review_ltgd_range":
        result = fetcher.fetch_review_ltgd_range()
    elif dataset == "review_plate":
        result = fetcher.fetch_review_plate()
    elif dataset == "home_qxlive_plate_summary":
        result = fetcher.fetch_home_qxlive_plate_summary()
    elif dataset == "auction_vratio":
        result = fetcher.fetch_auction_vratio()
    elif dataset == "auction_qiangchou":
        result = fetcher.fetch_auction_qiangchou()
    elif dataset == "auction_net_amount":
        result = fetcher.fetch_auction_net_amount()
    elif dataset == "auction_fengdan":
        result = fetcher.fetch_auction_fengdan()
    elif dataset == "cashflow_today":
        result = fetcher.fetch_cashflow_today()
    elif dataset == "cashflow_3d":
        result = fetcher.fetch_cashflow_3d()
    elif dataset == "cashflow_5d":
        result = fetcher.fetch_cashflow_5d()
    elif dataset == "cashflow_10d":
        result = fetcher.fetch_cashflow_10d()
    else:
        raise ValueError(f"Unsupported dataset: {dataset}")

    capture_payload = build_capture_payload(result)
    capture_path = persist_capture(capture_payload)
    meta = capture_payload.get("meta", {}) if isinstance(capture_payload, dict) else {}
    item_failed = meta.get("failed_items", []) if isinstance(meta, dict) else []
    item_missing = meta.get("missing_items", []) if isinstance(meta, dict) else []
    item_complete = bool(meta.get("complete", True)) if isinstance(meta, dict) else True
    return {
        "seq": SEQUENCE[dataset],
        "dataset": dataset,
        "dataset_id": capture_payload["dataset_id"],
        "dataset_label": capture_payload["dataset_label"],
        "source_path": capture_payload["source_path"],
        "source_url": capture_payload["source_url"],
        "fetched_at": capture_payload["fetched_at"],
        "row_count": capture_payload["row_count"],
        "capture_path": str(capture_path),
        "saved": True,
        "success": True,
        "complete": item_complete,
        "failed_items": item_failed,
        "missing_items": item_missing,
        "headers": capture_payload.get("headers", []),
    }


def persist_report(report: Dict[str, Any]) -> Path:
    now_cn = datetime.now(TZ_SHANGHAI)
    out_dir = REPORT_ROOT / now_cn.strftime("%Y-%m-%d") / report["group"]
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{now_cn.strftime('%H%M%S')}.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path


def send_webhook_request(url: str, webhook_payload: Dict[str, Any], bearer: str = "", secret: str = "") -> Dict[str, Any]:
    data = json.dumps(webhook_payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "duanxianxia-batch/1.0",
        "X-Webhook-Source": "duanxianxia",
    }
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    if secret:
        headers["X-Webhook-Secret"] = secret

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return {
                "enabled": True,
                "status": "ok",
                "success": True,
                "http_status": resp.status,
                "response_excerpt": body[:500],
            }
    except Exception as exc:  # noqa: BLE001
        status = getattr(exc, "code", None)
        body = ""
        if hasattr(exc, "read"):
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001
                body = ""
        return {
            "enabled": True,
            "status": "error",
            "success": False,
            "http_status": status,
            "response_excerpt": body[:500] or str(exc),
        }


def post_webhook(url: str, payload: Dict[str, Any], bearer: str = "", secret: str = "") -> Dict[str, Any]:
    if not url:
        return {
            "enabled": False,
            "status": "disabled",
            "success": False,
            "http_status": None,
            "response_excerpt": "",
        }

    is_feishu_bot_hook = "open.feishu.cn/open-apis/bot/v2/hook/" in url
    if not is_feishu_bot_hook:
        return send_webhook_request(url, payload, bearer=bearer, secret=secret)

    messages = build_feishu_webhook_messages(payload)
    last_result: Dict[str, Any] = {
        "enabled": True,
        "status": "ok",
        "success": True,
        "http_status": 200,
        "response_excerpt": "",
        "sent_messages": 0,
    }
    for message in messages:
        result = send_webhook_request(url, message, bearer=bearer, secret=secret)
        last_result = result
        last_result["sent_messages"] = last_result.get("sent_messages", 0) + 1
        if not result.get("success"):
            return last_result
    last_result["sent_messages"] = len(messages)
    return last_result


def load_capture_rows(capture_path: str) -> List[Dict[str, Any]]:
    if not capture_path:
        return []
    path = Path(capture_path)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows", [])
    return rows if isinstance(rows, list) else []


def build_fallback_item_from_capture(dataset: str, target_date: str, now_cn: datetime) -> Dict[str, Any] | None:
    ds_meta = DATASET_REGISTRY[dataset]
    capture_path = resolve_latest_capture_path_for_date(ds_meta["id"], target_date)
    if not capture_path:
        return None
    payload = load_capture_payload(capture_path)
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list) or not rows:
        return None
    meta = payload.get("meta", {}) if isinstance(payload, dict) else {}
    item_failed = meta.get("failed_items", []) if isinstance(meta, dict) else []
    item_missing = meta.get("missing_items", []) if isinstance(meta, dict) else []
    item_complete = bool(meta.get("complete", True)) if isinstance(meta, dict) else True
    return {
        "seq": SEQUENCE[dataset],
        "dataset": dataset,
        "dataset_id": payload.get("dataset_id", ds_meta["id"]),
        "dataset_label": payload.get("dataset_label", ds_meta["label"]),
        "source_path": payload.get("source_path", ds_meta["path"]),
        "source_url": payload.get("source_url", ""),
        "fetched_at": now_cn.isoformat(timespec="seconds"),
        "row_count": len(rows),
        "capture_path": capture_path,
        "saved": True,
        "success": True,
        "complete": item_complete,
        "failed_items": item_failed,
        "missing_items": item_missing,
        "headers": payload.get("headers", []),
        "fallback_used": True,
        "fallback_reason": "reused latest same-day successful capture after live fetch failure",
        "fallback_source_fetched_at": payload.get("fetched_at", ""),
    }


def load_report(report_path: str) -> Dict[str, Any]:
    if not report_path:
        raise ValueError("report_path is empty")
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"report not found: {report_path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid report payload: {report_path}")
    return payload


def stringify_cell(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text if text else "-"


def escape_lark_md(text: str) -> str:
    raw = stringify_cell(text)
    return raw.replace("|", "¦").replace("\n", " ")


def format_markdown_table_row(values: List[str]) -> str:
    return "| " + " | ".join(values) + " |"


def rows_to_pipe_table(headers: List[str], rows: List[Dict[str, Any]], columns: List[tuple[str, str]]) -> List[str]:
    display_headers = [header for header, _ in columns]
    lines = [format_markdown_table_row(display_headers)]
    lines.append(format_markdown_table_row(["---"] * len(display_headers)))
    for row in rows:
        values = []
        for _, key in columns:
            values.append(escape_lark_md(row.get(key)))
        lines.append(format_markdown_table_row(values))
    if len(lines) == 2:
        lines.append("暂无")
    return lines


def build_dataset_table_blocks(item: Dict[str, Any]) -> List[str]:
    rows = load_capture_rows(item.get("capture_path", ""))
    if not rows:
        return [f"【{item['dataset_label']}】\n暂无数据"]

    spec = TABLE_SPECS.get(item.get("dataset_id"), {})
    row_limit = spec.get("webhook_row_limit")
    if isinstance(row_limit, int) and row_limit > 0:
        rows = rows[:row_limit]
    columns = spec.get("card_columns") or spec.get("columns", [(header, header) for header in item.get("headers", [])])
    title = f"【{item['dataset_label']}｜共 {len(rows)} 行】"

    if item.get("dataset_id") == "auction.jjyd.qiangchou":
        group_titles = spec.get("group_titles", {})
        blocks: List[str] = []
        for group_key in ["qiangchou", "grab"]:
            subset = [row for row in rows if str(row.get("group", "")).strip() == group_key]
            if not subset:
                continue
            sub_title = group_titles.get(group_key, group_key)
            table_lines = rows_to_pipe_table(item.get("headers", []), subset, columns)
            blocks.append(f"{title}\n{sub_title}\n" + "\n".join(table_lines))
        return blocks or [f"{title}\n暂无数据"]

    table_lines = rows_to_pipe_table(item.get("headers", []), rows, columns)
    return [title + "\n" + "\n".join(table_lines)]

def chunk_text_block(text: str, max_chars: int = 7000) -> List[str]:
    lines = text.splitlines()
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    for line in lines:
        add_len = len(line) + 1
        if current and current_len + add_len > max_chars:
            chunks.append("\n".join(current))
            current = [line]
            current_len = add_len
        else:
            current.append(line)
            current_len += add_len
    if current:
        chunks.append("\n".join(current))
    return chunks


def render_external_detail_texts(report: Dict[str, Any]) -> List[str]:
    texts: List[str] = []
    intro = f"duanxianxia｜{report['group_label']}明细数据表格"
    for item in iter_webhook_items(report):
        if not item.get("success"):
            continue
        for block in build_dataset_table_blocks(item):
            block_text = intro + "\n\n" + block
            texts.extend(chunk_text_block(block_text))
    return texts


def build_card_table_lines(item: Dict[str, Any], rows: List[Dict[str, Any]], columns: List[tuple[str, str]]) -> List[str]:
    title = f"**{item['dataset_label']}**"
    lines = [title]
    header_line = format_markdown_table_row([header for header, _ in columns])
    separator_line = format_markdown_table_row(["---"] * len(columns))
    lines.append(header_line)
    lines.append(separator_line)
    for row in rows:
        values = [escape_lark_md(row.get(key)) for _, key in columns]
        lines.append(format_markdown_table_row(values))
    if len(lines) == 3:
        lines.append("暂无")
    return lines


def build_dataset_card_markdowns(item: Dict[str, Any], max_rows_per_card: int = 12) -> List[str]:
    rows = load_capture_rows(item.get("capture_path", ""))
    spec = TABLE_SPECS.get(item.get("dataset_id"), {})
    row_limit = spec.get("webhook_row_limit")
    if isinstance(row_limit, int) and row_limit > 0:
        rows = rows[:row_limit]
    columns = spec.get("card_columns") or spec.get("columns", [(header, header) for header in item.get("headers", [])])
    if not rows:
        return [f"**{item['dataset_label']}**\n暂无数据"]

    if item.get("dataset_id") == "auction.jjyd.qiangchou":
        group_titles = spec.get("group_titles", {})
        sections: List[str] = []
        for group_key in ["qiangchou", "grab"]:
            subset = [row for row in rows if str(row.get("group", "")).strip() == group_key]
            if not subset:
                continue
            title = group_titles.get(group_key, group_key)
            lines = [f"**{item['dataset_label']}｜{title}｜共 {len(subset)} 行**"]
            lines.extend(build_card_table_lines(item, subset, columns)[1:])
            sections.append("\n".join(lines))
        return ["\n\n".join(sections)] if sections else [f"**{item['dataset_label']}**\n暂无数据"]

    lines = [f"**{item['dataset_label']}｜共 {len(rows)} 行**"]
    lines.extend(build_card_table_lines(item, rows, columns)[1:])
    return ["\n".join(lines)]


def build_feishu_card_message(title: str, body_markdown: str, template: str = "blue", note: str = "") -> Dict[str, Any]:
    elements: List[Dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": body_markdown,
        }
    ]
    if note:
        elements.extend(
            [
                {"tag": "hr"},
                {
                    "tag": "note",
                    "elements": [
                        {"tag": "plain_text", "content": note}
                    ],
                },
            ]
        )
    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title,
                },
                "template": template,
            },
            "elements": elements,
        },
    }


def chunk_rows(rows: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    if size <= 0:
        size = 10
    return [rows[i:i + size] for i in range(0, len(rows), size)] or [[]]


def resolve_card_columns(item: Dict[str, Any]) -> List[tuple[str, str]]:
    spec = TABLE_SPECS.get(item.get("dataset_id"), {})
    columns = spec.get("card_columns") or spec.get("columns") or [(header, header) for header in item.get("headers", [])]
    return columns


def make_feishu_table_row(columns: List[tuple[str, str]], row: Dict[str, Any], header: bool = False) -> Dict[str, Any]:
    feishu_columns: List[Dict[str, Any]] = []
    weight = max(1, int(24 / max(1, len(columns))))
    for title, key in columns:
        text = title if header else stringify_cell(row.get(key))
        content = f"**{escape_lark_md(text)}**" if header else escape_lark_md(text)
        feishu_columns.append(
            {
                "tag": "column",
                "width": "weighted",
                "weight": weight,
                "elements": [
                    {
                        "tag": "markdown",
                        "content": content,
                    }
                ],
            }
        )
    return {
        "tag": "column_set",
        "flex_mode": "none",
        "background_style": "grey" if header else "default",
        "columns": feishu_columns,
    }


def build_feishu_table_card(
    title: str,
    item: Dict[str, Any],
    rows: List[Dict[str, Any]],
    columns: List[tuple[str, str]],
    template: str = "blue",
    note: str = "",
    chunk_index: int = 1,
    chunk_count: int = 1,
) -> Dict[str, Any]:
    subtitle = f"**{item['dataset_label']}｜共 {len(rows)} 行**"
    if chunk_count > 1:
        subtitle = f"**{item['dataset_label']}｜第 {chunk_index}/{chunk_count} 段｜本段 {len(rows)} 行**"

    elements: List[Dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": subtitle,
        },
        make_feishu_table_row(columns, {}, header=True),
    ]

    if rows:
        for row in rows:
            elements.append(make_feishu_table_row(columns, row, header=False))
    else:
        elements.append({"tag": "markdown", "content": "暂无数据"})

    if note:
        elements.extend(
            [
                {"tag": "hr"},
                {
                    "tag": "note",
                    "elements": [
                        {"tag": "plain_text", "content": note}
                    ],
                },
            ]
        )

    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title,
                },
                "template": template,
            },
            "elements": elements,
        },
    }


def build_feishu_summary_card(report: Dict[str, Any]) -> Dict[str, Any]:
    template = CARD_TEMPLATES.get(report.get("group", ""), "blue")
    title = f"duanxianxia｜{report['group_label']}批量下载回执"
    hide_paths = report.get("group") in {"premarket", "intraday", "intraday_cashflow", "postmarket", "postmarket_cashflow"}
    note = "" if hide_paths else f"报告文件：{report['report_path']}"
    return build_feishu_card_message(title, render_summary_text(report).replace("**", ""), template=template, note=note)


def build_capped_feishu_table_card(
    title: str,
    item: Dict[str, Any],
    rows: List[Dict[str, Any]],
    columns: List[tuple[str, str]],
    template: str,
    total_row_count: int,
    note_prefix: str,
    chunk_index: int = 1,
    chunk_count: int = 1,
) -> Dict[str, Any]:
    spec = TABLE_SPECS.get(item.get("dataset_id", ""), {})
    max_webhook_rows = MAX_WEBHOOK_TABLE_ROWS
    configured_max = spec.get("webhook_max_rows")
    if isinstance(configured_max, int) and configured_max > 0:
        max_webhook_rows = configured_max


    display_rows = list(rows[:max_webhook_rows])
    while True:
        note = note_prefix
        if chunk_count > 1:
            note = f"{note}｜共 {total_row_count} 行｜第 {chunk_index}/{chunk_count} 段"
        elif total_row_count > len(display_rows):
            note = f"{note_prefix}｜仅展示前 {len(display_rows)}/{total_row_count} 行"
        card = build_feishu_table_card(
            title,
            item,
            display_rows,
            columns,
            template=template,
            note=note,
            chunk_index=chunk_index,
            chunk_count=chunk_count,
        )
        payload_bytes = len(json.dumps(card, ensure_ascii=False).encode("utf-8"))
        if payload_bytes <= MAX_FEISHU_CARD_PAYLOAD_BYTES or len(display_rows) <= 1:
            return card
        display_rows = display_rows[:-1]


def iter_webhook_items(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = list(report.get("items", []))
    priority_dataset_ids = {"home.qxlive.top_metrics"}

    def sort_key(item: Dict[str, Any]) -> tuple[int, int]:
        dataset_id = str(item.get("dataset_id", "") or "")
        priority = 0 if dataset_id in priority_dataset_ids else 1
        return (priority, int(item.get("seq", 9999) or 9999))

    return sorted(items, key=sort_key)


def build_feishu_detail_cards(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    template = CARD_TEMPLATES.get(report.get("group", ""), "blue")
    cards: List[Dict[str, Any]] = []
    for item in iter_webhook_items(report):
        if not item.get("success"):
            continue
        rows = load_capture_rows(item.get("capture_path", ""))
        total_row_count = len(rows)
        columns = resolve_card_columns(item)
        note_prefix = f"数据集：{item['dataset_label']}｜文件：{item.get('capture_path', '')}"

        spec = TABLE_SPECS.get(item.get("dataset_id", ""), {})
        row_chunk_size = int(spec.get("card_chunk_size", 0) or 0)
        if row_chunk_size <= 0:
            row_chunk_size = 0

        # Only split home.ztpool into multiple cards for webhook payload limits.
        if item.get("dataset_id") == "home.ztpool" and row_chunk_size > 0 and total_row_count > row_chunk_size:
            chunks = chunk_rows(rows, row_chunk_size)
            chunk_count = len(chunks)
            for chunk_index, chunk_rows_data in enumerate(chunks, start=1):
                cards.append(
                    build_capped_feishu_table_card(
                        f"duanxianxia｜{report['group_label']}明细表",
                        item,
                        chunk_rows_data,
                        columns,
                        template,
                        total_row_count,
                        note_prefix,
                        chunk_index=chunk_index,
                        chunk_count=chunk_count,
                    )
                )
            continue

        if item.get("dataset_id") == "auction.jjyd.qiangchou":
            group_titles = TABLE_SPECS.get(item.get("dataset_id"), {}).get("group_titles", {})
            all_group_rows = load_capture_rows(item.get("capture_path", ""))
            for group_key in ["qiangchou", "grab"]:
                group_rows = [row for row in all_group_rows if str(row.get("group", "")).strip() == group_key]
                if not group_rows:
                    continue
                label = group_titles.get(group_key, group_key)
                group_note_prefix = f"数据集：{item['dataset_label']}｜分组：{label}｜文件：{item.get('capture_path', '')}"
                cards.append(
                    build_capped_feishu_table_card(
                        f"duanxianxia｜{report['group_label']}明细表",
                        {**item, "dataset_label": f"{item['dataset_label']}｜{label}"},
                        group_rows,
                        columns,
                        template,
                        len(group_rows),
                        group_note_prefix,
                    )
                )
            continue

        cards.append(
            build_capped_feishu_table_card(
                f"duanxianxia｜{report['group_label']}明细表",
                item,
                rows,
                columns,
                template,
                total_row_count,
                note_prefix,
                chunk_index=1,
                chunk_count=1,
            )
        )
    return cards


def build_feishu_webhook_messages(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = [build_feishu_summary_card(report)]
    detail_cards = build_feishu_detail_cards(report)
    if detail_cards:
        messages.extend(detail_cards)
        return messages

    for text in render_external_detail_texts(report):
        messages.append(
            {
                "msg_type": "text",
                "content": {
                    "text": text.replace("**", "")
                },
            }
        )
    return messages


def render_ltgd_full(rows: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for row in rows:
        period = str(row.get("周期", ""))
        board = str(row.get("板块", ""))
        grouped.setdefault(period, {}).setdefault(board, []).append(row)

    period_order = ["5日", "10日", "20日", "50日"]
    board_order = ["主板", "创业科创板", "北交所"]

    for period in period_order:
        if period not in grouped:
            continue
        lines.append(f"- {period}")
        for board in board_order:
            values = grouped.get(period, {}).get(board, [])
            lines.append(f"  - {board}")
            if not values:
                lines.append("    - 暂无")
                continue
            for row in values:
                lines.append(
                    "    - "
                    f"{row.get('排名', '')}. "
                    f"{row.get('名称', '')}（{row.get('代码', '')}）"
                    f"｜区间涨幅 {row.get('区间涨幅', '')}"
                    f"｜概念 {row.get('概念', '')}"
                )
    return lines


def render_postmarket_details(report: Dict[str, Any]) -> List[str]:
    lines: List[str] = ["", "**盘后明细数据**"]
    for item in report.get("items", []):
        if not item.get("success"):
            continue
        rows = load_capture_rows(item.get("capture_path", ""))
        lines.append("")
        lines.append(f"**{item['dataset_label']}**")
        if item.get("dataset") == "review_daily":
            for row in rows:
                label = row.get("display_label") or row.get("metric_label") or row.get("metric_key", "")
                rate = row.get("display_rate") or row.get("value", "")
                parts = [f"- {label}：{rate}"]
                if row.get("ratio"):
                    parts.append(f"比值 {row.get('ratio')}")
                if row.get("jinji_count") not in (None, ""):
                    parts.append(f"晋级数 {row.get('jinji_count')}")
                if row.get("sample_count") not in (None, ""):
                    parts.append(f"样本数 {row.get('sample_count')}")
                if row.get("raw_value"):
                    parts.append(f"原值 {row.get('raw_value')}")
                lines.append("｜".join(parts))
        elif item.get("dataset") == "review_ltgd_range":
            lines.extend(render_ltgd_full(rows))
        else:
            headers = item.get("headers", [])
            for row in rows[:10]:
                parts = [f"{h}={row.get(h, '')}" for h in headers[:8]]
                lines.append(f"- {'｜'.join(parts)}")
            if len(rows) > 10:
                lines.append(f"- ……共 {len(rows)} 行")
    return lines


def infer_capture_date_from_path(capture_path: str) -> str:
    text = str(capture_path or "").strip()
    if not text:
        return ""
    match = re.search(r"/captures/(\d{4}-\d{2}-\d{2})/", text)
    if match:
        return match.group(1)
    return ""


def infer_analysis_date_from_rows(rows: List[Dict[str, Any]]) -> str:
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("date", "日期", "trade_date", "交易日期"):
            parsed = safe_date(row.get(key))
            if parsed is not None:
                return parsed.isoformat()
    return ""


def infer_report_date_context(report: Dict[str, Any], analysis: Dict[str, Any] | None = None) -> Tuple[str, str]:
    analysis = analysis if isinstance(analysis, dict) else {}

    analysis_trade_date = str(analysis.get("analysis_trade_date") or "").strip()
    capture_trade_date = str(analysis.get("capture_trade_date") or "").strip()

    if not capture_trade_date:
        for item in report.get("items", []):
            capture_path = str(item.get("capture_path") or "").strip()
            capture_trade_date = infer_capture_date_from_path(capture_path)
            if capture_trade_date:
                break
        if not capture_trade_date:
            generated_at = str(report.get("generated_at") or "").strip()
            parsed = safe_date(generated_at[:10]) if len(generated_at) >= 10 else None
            if parsed is not None:
                capture_trade_date = parsed.isoformat()

    if not analysis_trade_date:
        preferred_ids = [
            "review.daily.top_metrics",
            "review.daily.top_metrics.core11",
            "home.ztpool",
            "review.fupan.plate",
            "rank.hot_stock_day",
            "rank.rocket",
        ]
        items_by_id = {
            str(item.get("dataset_id") or "").strip(): item
            for item in report.get("items", [])
            if isinstance(item, dict)
        }
        for dataset_id in preferred_ids:
            item = items_by_id.get(dataset_id) or {}
            capture_path = str(item.get("capture_path") or "").strip()
            if not capture_path:
                continue
            rows = load_capture_rows(capture_path)
            analysis_trade_date = infer_analysis_date_from_rows(rows)
            if analysis_trade_date:
                break
        if not analysis_trade_date:
            for item in report.get("items", []):
                capture_path = str(item.get("capture_path") or "").strip()
                if not capture_path:
                    continue
                rows = load_capture_rows(capture_path)
                analysis_trade_date = infer_analysis_date_from_rows(rows)
                if analysis_trade_date:
                    break

    return analysis_trade_date, capture_trade_date


def select_signal_profile_display(profile: List[str], limit: int = 3) -> List[str]:
    if not profile:
        return []
    priority = {
        "低关注高结构": 100,
        "资金确认": 95,
        "热度确认": 90,
        "活跃池确认": 85,
        "低晋级率突围": 80,
        "低晋级率环境": 75,
        "日线结构强": 70,
        "20日突破": 65,
        "日线结构良好": 60,
        "连板成功": 50,
        "连板尝试": 45,
        "首板成功": 40,
        "首板尝试": 35,
    }
    ranked = sorted(
        enumerate(profile),
        key=lambda pair: (-priority.get(str(pair[1]), 0), pair[0]),
    )
    chosen = sorted(ranked[:limit], key=lambda pair: pair[0])
    return [str(item) for _, item in chosen]


def render_summary_text(report: Dict[str, Any]) -> str:
    status_text = "成功" if report["success"] else "失败"
    complete_text = "完整" if report["complete"] else "不完整"

    # 盘前/盘中/盘后任务只展示下载结果，不再展示 webhook 路由与文件路径。
    webhook = report.get("webhook", {})
    webhook_text = None
    if webhook.get("enabled") and webhook.get("success"):
        webhook_text = f"已推送（HTTP {webhook.get('http_status')}）"
    elif webhook.get("enabled") and not webhook.get("success"):
        webhook_text = f"推送失败（HTTP {webhook.get('http_status') or 'N/A'}）"
    else:
        webhook_text = "未配置或未识别到外部 webhook URL"

    announce_text = "由 OpenClaw announce 链路单独处理"
    target_groups = {"premarket", "intraday", "intraday_cashflow", "postmarket", "postmarket_cashflow"}
    group = report.get("group")

    lines = [
        f"**duanxianxia｜{report['group_label']}批量下载回执**",
        "",
        f"- 状态：**{status_text}**",
        f"- 完整性：**{complete_text}**",
        f"- 预期表数：{report['expected_count']}",
        f"- 成功表数：{report['success_count']}",
        f"- 失败表数：{report['failure_count']}",
        f"- 遗漏表数：{len(report['missing_items'])}",
        "",
        "**逐表结果**",
    ]

    if group not in target_groups:
        lines[-2:-2] = [
            f"- 外部 webhook：{webhook_text}",
            f"- OpenClaw announce：{announce_text}",
            f"- 报告文件：`{report['report_path']}`",
        ]

    for item in report["items"]:
        item_status = "成功" if item.get("success") else "失败"
        item_complete = "完整" if item.get("complete") else "不完整"
        item_lines = [
            f"- {item['seq']}. {item['dataset_label']}",
            f"  - 状态：{item_status}",
            f"  - 完整性：{item_complete}",
            f"  - 行数：{item.get('row_count', 0)}",
            f"  - 保存：{'是' if item.get('saved') else '否'}",
        ]
        if group not in target_groups:
            item_lines.append(f"  - 文件：`{item.get('capture_path', '')}`")
        lines.extend(item_lines)

        if item.get("error"):
            lines.append(f"  - 错误：{item['error']}")

    failed_items = report.get("failed_items", [])
    missing_items = report.get("missing_items", [])
    lines.extend(
        [
            "",
            f"- failed_items：{', '.join(failed_items) if failed_items else '无'}",
            f"- incomplete_items：{', '.join(report.get('incomplete_items', [])) if report.get('incomplete_items') else '无'}",
            f"- missing_items：{', '.join(missing_items) if missing_items else '无'}",
        ]
    )

    analysis = report.get("analysis", {})
    top_candidates = analysis.get("top_candidates", []) if isinstance(analysis, dict) else []
    if top_candidates:
        lines.extend([
            "",
            "**盘前分析候选**",
        ])
        for cand in top_candidates[:5]:
            reason_text = "；".join(cand.get("reasons", [])[:3]) or "无"
            risk_text = "；".join(cand.get("risks", [])[:2]) or "无"
            lines.append(
                f"- {cand.get('rank')}. {cand.get('name')}（{cand.get('code')}）｜评分 {cand.get('score')}｜命中 {cand.get('source_hit_count')} 表｜理由：{reason_text}｜风险：{risk_text}"
            )

    strong_up_candidates = analysis.get("strong_up_candidates", []) if isinstance(analysis, dict) else []
    weak_to_strong_candidates = analysis.get("weak_to_strong_candidates", []) if isinstance(analysis, dict) else []
    risk_watch = analysis.get("risk_watch", []) if isinstance(analysis, dict) else []
    quasi_strong_up_watch = analysis.get("quasi_strong_up_watch", []) if isinstance(analysis, dict) else []
    missed_continuation_watch = analysis.get("missed_continuation_watch", []) if isinstance(analysis, dict) else []
    if strong_up_candidates or weak_to_strong_candidates or risk_watch:
        market_snapshot = analysis.get("market_snapshot", {}) if isinstance(analysis, dict) else {}
        lines.extend([
            "",
            "**盘后分析框架**",
        ])
        if market_snapshot:
            snapshot_text = "｜".join(
                f"{k} {v}" for k, v in market_snapshot.items() if str(v or "").strip()
            )
            if snapshot_text:
                lines.append(f"- 环境：{snapshot_text}")
        analysis_trade_date, capture_trade_date = infer_report_date_context(report, analysis)
        if analysis_trade_date or capture_trade_date:
            lines.append(f"- 日期口径：分析交易日 {analysis_trade_date or 'N/A'}｜文件落盘日 {capture_trade_date or 'N/A'}")
            if analysis_trade_date and capture_trade_date and analysis_trade_date != capture_trade_date:
                lines.append(f"  - 说明：目录/文件日期表示抓取与保存时间，不代表当日已开盘，本批数据实际对应交易日 {analysis_trade_date}。")
        if strong_up_candidates:
            lines.append("- 明日强上强候选：")
            for cand in strong_up_candidates[:5]:
                reason_text = "；".join(cand.get("reasons", [])[:3]) or "无"
                risk_text = "；".join(cand.get("risks", [])[:2]) or "无"
                profile_text = " / ".join(select_signal_profile_display(cand.get("signal_profile", []), limit=3)) or "无"
                lines.append(
                    f"  - {cand.get('name')}（{cand.get('code')}）｜{cand.get('group_name')}｜状态 {cand.get('status')}｜涨幅 {cand.get('gain')}%｜评分 {cand.get('score')}｜画像：{profile_text}｜理由：{reason_text}｜风险：{risk_text}"
                )
        if weak_to_strong_candidates:
            lines.append("- 明日弱转强候选：")
            for cand in weak_to_strong_candidates[:5]:
                reason_text = "；".join(cand.get("reasons", [])[:3]) or "无"
                risk_text = "；".join(cand.get("risks", [])[:2]) or "无"
                profile_text = " / ".join(select_signal_profile_display(cand.get("signal_profile", []), limit=3)) or "无"
                lines.append(
                    f"  - {cand.get('name')}（{cand.get('code')}）｜{cand.get('group_name')}｜状态 {cand.get('status')}｜涨幅 {cand.get('gain')}%｜评分 {cand.get('score')}｜画像：{profile_text}｜理由：{reason_text}｜风险：{risk_text}"
                )
        if risk_watch:
            lines.append("- 风险观察：")
            for cand in risk_watch[:5]:
                risk_text = cand.get("watch_reason") or "；".join(cand.get("risks", [])[:2]) or "无"
                lines.append(
                    f"  - {cand.get('name')}（{cand.get('code')}）｜{cand.get('group_name')}｜状态 {cand.get('status')}｜涨幅 {cand.get('gain')}%｜关注点：{risk_text}"
                )
        if quasi_strong_up_watch:
            lines.append("- 准强上强观察：")
            for cand in quasi_strong_up_watch[:3]:
                profile_text = " / ".join(select_signal_profile_display(cand.get("signal_profile", []), limit=3)) or "无"
                missing_text = " / ".join(cand.get("missing_confirmations", [])[:3]) or "无"
                lines.append(
                    f"  - {cand.get('name')}（{cand.get('code')}）｜{cand.get('group_name')}｜评分 {cand.get('score')}｜距前八 {cand.get('gap_to_strong_up')} 分｜画像：{profile_text}｜还缺：{missing_text}"
                )
        if missed_continuation_watch:
            cutoff_score = analysis.get("strong_up_cutoff_score") if isinstance(analysis, dict) else None
            if cutoff_score not in (None, ""):
                lines.append(f"- 强上强前八门槛分：{cutoff_score}")
            quasi_codes = {str(cand.get("code") or "") for cand in quasi_strong_up_watch}
            missed_display = [
                cand for cand in missed_continuation_watch
                if str(cand.get("code") or "") not in quasi_codes
            ]
            if missed_display:
                lines.append("- 差一点上位的连板观察：")
                for cand in missed_display[:5]:
                    profile_text = " / ".join(select_signal_profile_display(cand.get("signal_profile", []), limit=3)) or "无"
                    missing_text = " / ".join(cand.get("missing_confirmations", [])[:3]) or "无"
                    gap_text = cand.get("gap_to_strong_up")
                    lines.append(
                        f"  - {cand.get('name')}（{cand.get('code')}）｜{cand.get('group_name')}｜评分 {cand.get('score')}｜距前八 {gap_text} 分｜画像：{profile_text}｜还缺：{missing_text}"
                    )

    return "\n".join(lines)


def render_text(report: Dict[str, Any]) -> str:
    return render_summary_text(report)


def main() -> int:
    args = build_parser().parse_args()
    group_cfg = GROUPS[args.group]
    now_cn = datetime.now(TZ_SHANGHAI)
    now_utc = datetime.now(timezone.utc)

    if args.group == "dailyline" and args.report_path:
        raise SystemExit("dailyline group does not support --report-path")
    if args.group == "dailyline" and args.capture_only:
        raise SystemExit("dailyline group does not support --capture-only")
    if args.report_path and args.capture_only:
        raise SystemExit("--report-path and --capture-only cannot be used together")

    if args.group == "dailyline":
        target_date = args.target_date or now_cn.strftime("%Y-%m-%d")
        report = run_dailyline_group(
            target_date=target_date,
            start_date=args.dailyline_start_date,
            review_meta_name=args.review_meta_name,
        )
        report_path = persist_report(report)
        report["report_path"] = str(report_path)
        report["webhook"] = {
            "enabled": False,
            "status": "disabled",
            "success": False,
            "http_status": None,
            "response_excerpt": "dailyline mode: no webhook sent",
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print(render_text(report))
        return 0 if report["success"] else 1

    if args.report_path:
        report = load_report(args.report_path)
        source_group = str(report.get("group", "") or "").strip()
        if source_group and source_group != args.group:
            raise SystemExit(f"report group mismatch: expected {args.group}, got {source_group}")

        source_generated_at = str(report.get("generated_at") or "").strip()
        source_generated_at_utc = str(report.get("generated_at_utc") or "").strip()
        source_timezone = str(report.get("timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai"

        report["group"] = args.group
        report["group_label"] = group_cfg["label"]
        report["trigger"] = "analysis_only"
        report["generated_at"] = source_generated_at or now_cn.isoformat(timespec="seconds")
        report["generated_at_utc"] = source_generated_at_utc or now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
        report["timezone"] = source_timezone
        report["source_report_path"] = args.report_path

        if args.group == "premarket":
            report["analysis"] = build_premarket_analysis(report)
        elif args.group in {"intraday", "intraday_cashflow"}:
            report["analysis"] = build_intraday_analysis(report)
        elif args.group in {"postmarket", "postmarket_cashflow"}:
            report["analysis"] = build_postmarket_analysis(report)
        report["bitable_sync"] = sync_analysis_to_bitable(report, meta_name=args.review_meta_name)

        if args.save_analysis_copy:
            report_path = persist_report(report)
            report["report_path"] = str(report_path)
        else:
            report["report_path"] = report.get("report_path") or args.report_path
            report_path = None

        if args.webhook_url:
            report["webhook"] = post_webhook(
                args.webhook_url,
                report,
                bearer=args.webhook_bearer,
                secret=args.webhook_secret,
            )
        else:
            report["webhook"] = {
                "enabled": False,
                "status": "disabled",
                "success": False,
                "http_status": None,
                "response_excerpt": "analysis_only mode: no webhook sent",
            }

        if report_path is not None:
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print(render_text(report))
        return 0

    fetcher = DuanxianxiaFetcher()

    items: List[Dict[str, Any]] = []
    failed_items: List[str] = []
    incomplete_items: List[str] = []

    target_date = now_cn.strftime("%Y-%m-%d")
    for dataset in group_cfg["datasets"]:
        ds_meta = DATASET_REGISTRY[dataset]
        try:
            item = run_dataset(fetcher, dataset)
        except Exception as exc:  # noqa: BLE001
            fallback_item = None
            if args.group in {"postmarket", "postmarket_cashflow"}:
                fallback_item = build_fallback_item_from_capture(dataset, target_date, now_cn)
            if fallback_item is not None:
                item = fallback_item
                item["original_error"] = f"{type(exc).__name__}: {exc}"
            else:
                failed_items.append(ds_meta["id"])
                item = {
                    "seq": SEQUENCE[dataset],
                    "dataset": dataset,
                    "dataset_id": ds_meta["id"],
                    "dataset_label": ds_meta["label"],
                    "source_path": ds_meta["path"],
                    "source_url": "",
                    "fetched_at": now_cn.isoformat(timespec="seconds"),
                    "row_count": 0,
                    "capture_path": "",
                    "saved": False,
                    "success": False,
                    "complete": False,
                    "failed_items": [ds_meta["id"]],
                    "missing_items": [ds_meta["id"]],
                    "headers": [],
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(limit=5),
                }
        items.append(item)
        if not item.get("complete", True):
            incomplete_items.append(item["dataset_id"])

    expected_ids = [DATASET_REGISTRY[d]["id"] for d in group_cfg["datasets"]]
    completed_ids = [item["dataset_id"] for item in items if item.get("success")]
    missing_items = [dataset_id for dataset_id in expected_ids if dataset_id not in completed_ids]

    report: Dict[str, Any] = {
        "project": "duanxianxia",
        "group": args.group,
        "group_label": group_cfg["label"],
        "trigger": "manual_or_cron",
        "generated_at": now_cn.isoformat(timespec="seconds"),
        "generated_at_utc": now_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "timezone": "Asia/Shanghai",
        "expected_count": len(expected_ids),
        "success_count": len(completed_ids),
        "failure_count": len(failed_items),
        "success": len(failed_items) == 0,
        "complete": len(missing_items) == 0 and len(failed_items) == 0 and len(incomplete_items) == 0,
        "saved": all(item.get("saved") for item in items if item.get("success")),
        "failed_items": failed_items,
        "incomplete_items": incomplete_items,
        "missing_items": missing_items,
        "items": items,
    }

    if args.capture_only:
        report["trigger"] = "capture_only"
        report["analysis"] = {}
        report["bitable_sync"] = {
            "enabled": False,
            "meta_name": args.review_meta_name,
            "created_count": 0,
            "skipped_existing_count": 0,
            "records": [],
            "reason": "capture_only mode: analysis skipped",
        }
        report_path = persist_report(report)
        report["report_path"] = str(report_path)
        report["webhook"] = {
            "enabled": False,
            "status": "disabled",
            "success": False,
            "http_status": None,
            "response_excerpt": "capture_only mode: no webhook sent",
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print(f"capture_only ok\nreport_path: {report_path}")
        return 0 if report["success"] else 1

    if args.group == "premarket":
        report["analysis"] = build_premarket_analysis(report)
    elif args.group in {"intraday", "intraday_cashflow"}:
        report["analysis"] = build_intraday_analysis(report)
    elif args.group in {"postmarket", "postmarket_cashflow"}:
        report["analysis"] = build_postmarket_analysis(report)
    report["bitable_sync"] = sync_analysis_to_bitable(report, meta_name=args.review_meta_name)

    report_path = persist_report(report)
    report["report_path"] = str(report_path)
    report["webhook"] = post_webhook(
        args.webhook_url,
        report,
        bearer=args.webhook_bearer,
        secret=args.webhook_secret,
    )
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(render_text(report))

    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
