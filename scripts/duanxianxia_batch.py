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
            "rocket",
            "hotlist_day",
            "auction_vratio",
            "auction_qiangchou",
            "auction_net_amount",
            "auction_fengdan",
            "auction_weimai",
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
    "auction_weimai": 10,
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
    "auction.jjyd.weimai": {
        "columns": [("排名", "rank"), ("名称", "name"), ("代码", "code"), ("委买/撮合", "auction_turnover_text"), ("涨幅", "latest_change_pct_text"), ("竞价主力", "main_net_inflow_text"), ("封单", "seal_amount_text"), ("流通Z", "market_cap_text"), ("竞涨", "auction_change_pct_text"), ("概念1", "concept_1"), ("概念2", "concept_2"), ("连板标签", "board_label")],
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
    """盘前分析已统一到 v7.3 选择性决策管线（单一事实来源）。

    旧的 premarket_5table_v5 五表打分器是死代码，且权重与 v7.3 相互矛盾
    （v5 把 grab/竞价最后 1 秒抢筹当作主信号，且完全不使用微买 weimai）。
    生产入口 duanxianxia_premarket_v7_runner.py 早已用 monkey-patch 把
    build_premarket_analysis 替换为 build_premarket_analysis_v7_3；同时
    直接运行 `python3 duanxianxia_batch.py premarket` 过去仍会走这段过时的
    v5 逻辑。现在统一委托到 v7.3：9:20-9:25 持续抢筹为主信号，竞价最后 1 秒
    抢筹降权确认，并已并入微买。

    采用函数内懒加载 import，避免 batch 与 v7 runner 之间的模块级循环 import。
    """
    from duanxianxia_premarket_v7_3_runner import build_premarket_analysis_v7_3

    return build_premarket_analysis_v7_3(report)


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
            "推荐