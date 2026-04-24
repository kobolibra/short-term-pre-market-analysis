#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import re
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from duanxianxia_batch import REPORT_ROOT, DAILYLINE_STOCK_ROOT, get_trade_day_pair, safe_date, zero_pad_stock_code
from feishu_bitable_cli import feishu_request, load_meta, update_record

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
SAME_DAY_SCENES = {"盘前推荐", "盘中联动推荐"}
PREVIOUS_DAY_SCENE = "盘后复盘选股"


def load_latest_report(group: str, target_date: str) -> Dict[str, Any]:
    root = REPORT_ROOT / target_date / group
    if not root.exists():
        return {}
    files = sorted(root.glob("*.json"))
    if not files:
        return {}
    return json.loads(files[-1].read_text(encoding="utf-8"))


def resolve_trade_dates(target_date: str) -> Tuple[str, str]:
    dailyline_report = load_latest_report("dailyline", target_date)
    analysis = dailyline_report.get("analysis", {}) if isinstance(dailyline_report, dict) else {}
    effective_trade_date = str(analysis.get("effective_trade_date") or "").strip()
    previous_trade_date = str(analysis.get("previous_trade_date") or "").strip()
    if effective_trade_date and previous_trade_date:
        return effective_trade_date, previous_trade_date

    capture = io.StringIO()
    with redirect_stdout(capture):
        return get_trade_day_pair(target_date)


def fetch_all_records(meta_name: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    meta = load_meta(meta_name)
    app_token = meta["app_token"]
    table_id = meta["table_id"]
    items: List[Dict[str, Any]] = []
    page_token = ""
    while True:
        query: Dict[str, Any] = {"page_size": 500}
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
    return meta, items


def extract_record_date(fields: Dict[str, Any]) -> str:
    explicit = safe_date(fields.get("日期"))
    if explicit is not None:
        return explicit.isoformat()
    text = str(fields.get("推荐时间") or "").strip()
    for token in text.split():
        parsed = safe_date(token)
        if parsed is not None:
            return parsed.isoformat()
    return ""


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().rstrip("%")
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def parse_int_like(value: Any, default: int = 0) -> int:
    text = str(value or "").strip()
    match = re.search(r"-?\d+", text)
    if not match:
        return default
    try:
        return int(match.group(0))
    except Exception:
        return default


def parse_chinese_amount(value: Any) -> Optional[float]:
    text = str(value or "").strip()
    if not text or text in {"-", "--"}:
        return None
    text = text.replace(",", "")
    try:
        if text.endswith("亿"):
            return float(text[:-1]) * 100000000
        if text.endswith("万"):
            return float(text[:-1]) * 10000
        return float(text)
    except Exception:
        return None


def average(values: List[float]) -> Optional[float]:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def get_dailyline_close_metrics(code: str, trade_date: str) -> Dict[str, Any]:
    path = DAILYLINE_STOCK_ROOT / f"{code}.csv"
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    target = next((row for row in rows if str(row.get("date") or "") == trade_date), None)
    if not target:
        return {}
    return {
        "收盘涨幅": round(float(target.get("pctChg") or 0.0), 2),
        "成交额": round(float(target.get("amount") or 0.0), 2),
        "换手率": round(float(target.get("turn") or 0.0), 2),
    }


def build_dailyline_trade_snapshot(code: str, trade_date: str) -> Dict[str, Any]:
    path = DAILYLINE_STOCK_ROOT / f"{code}.csv"
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    idx = next((index for index, row in enumerate(rows) if str(row.get("date") or "") == trade_date), -1)
    if idx < 0:
        return {}
    row = rows[idx]

    def float_at(i: int, key: str) -> Optional[float]:
        if i < 0 or i >= len(rows):
            return None
        return to_float(rows[i].get(key))

    close = to_float(row.get("close")) or 0.0
    open_price = to_float(row.get("open")) or 0.0
    high = to_float(row.get("high")) or 0.0
    low = to_float(row.get("low")) or 0.0
    preclose = to_float(row.get("preclose")) or 0.0
    volume = to_float(row.get("volume")) or 0.0
    amount = to_float(row.get("amount")) or 0.0
    turn = to_float(row.get("turn")) or 0.0
    pct_chg = to_float(row.get("pctChg")) or 0.0

    closes = [to_float(item.get("close")) or 0.0 for item in rows]
    highs = [to_float(item.get("high")) or 0.0 for item in rows]
    lows = [to_float(item.get("low")) or 0.0 for item in rows]
    volumes = [to_float(item.get("volume")) or 0.0 for item in rows]

    def rolling_mean(series: List[float], end_idx: int, window: int) -> Optional[float]:
        start = max(0, end_idx - window + 1)
        chunk = series[start : end_idx + 1]
        return average(chunk) if len(chunk) >= window else None

    ma5 = rolling_mean(closes, idx, 5)
    ma10 = rolling_mean(closes, idx, 10)
    ma20 = rolling_mean(closes, idx, 20)

    prev5_vol = average(volumes[max(0, idx - 5) : idx]) if idx > 0 else None
    recent20_high = max(highs[max(0, idx - 19) : idx + 1]) if highs[max(0, idx - 19) : idx + 1] else high
    recent20_low = min(lows[max(0, idx - 19) : idx + 1]) if lows[max(0, idx - 19) : idx + 1] else low
    close_near_high = high > 0 and (high - close) / high <= 0.015
    upper_shadow_pct = round(((high - max(open_price, close)) / preclose) * 100, 2) if preclose > 0 else 0.0
    body_pct = round(((close - open_price) / preclose) * 100, 2) if preclose > 0 else 0.0
    volume_ratio_5 = round(volume / prev5_vol, 2) if prev5_vol and prev5_vol > 0 else None

    return {
        "收盘涨幅": round(pct_chg, 2),
        "成交额": round(amount, 2),
        "换手率": round(turn, 2),
        "开盘价": open_price,
        "最高价": high,
        "最低价": low,
        "收盘价": close,
        "昨收价": preclose,
        "volume": volume,
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


def choose_first_number(*values: Any) -> Optional[float]:
    for value in values:
        parsed = to_float(value)
        if parsed is not None:
            return parsed
    return None


def build_premarket_snapshot(target_date: str) -> Dict[str, Dict[str, Any]]:
    report = load_latest_report("premarket", target_date)
    items = report.get("items", []) if isinstance(report, dict) else []
    captures: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        dataset_id = str(item.get("dataset_id") or "").strip()
        capture_path = Path(str(item.get("capture_path") or "").strip())
        if not dataset_id or not capture_path.exists():
            continue
        payload = json.loads(capture_path.read_text(encoding="utf-8"))
        rows = payload.get("rows", [])
        captures[dataset_id] = rows if isinstance(rows, list) else []

    analysis = report.get("analysis", {}) if isinstance(report, dict) else {}
    top_candidates = analysis.get("top_candidates", []) if isinstance(analysis, dict) else []
    reason_map = {
        zero_pad_stock_code(item.get("code")): "；".join((item.get("reasons") or [])[:4])
        for item in top_candidates
        if zero_pad_stock_code(item.get("code"))
    }

    snapshot: Dict[str, Dict[str, Any]] = {}
    for dataset_id in ["auction.jjyd.net_amount", "auction.jjyd.qiangchou", "auction.jjyd.vratio"]:
        for row in captures.get(dataset_id, []):
            code = zero_pad_stock_code(row.get("code") or row.get("代码"))
            if not code:
                continue
            item = snapshot.setdefault(code, {})
            item.setdefault("竞价涨幅", choose_first_number(row.get("auction_change_pct"), row.get("auction_change_pct_text"), row.get("竞价涨幅")))
            item.setdefault("推荐时涨幅", choose_first_number(row.get("latest_change_pct"), row.get("最新涨幅")))

    for row in captures.get("auction.jjlive.fengdan", []):
        if str(row.get("section_kind") or "") != "live":
            continue
        code = zero_pad_stock_code(row.get("code") or row.get("代码"))
        if not code:
            continue
        item = snapshot.setdefault(code, {})
        if item.get("推荐时涨幅") is None:
            item["推荐时涨幅"] = choose_first_number(row.get("latest_change_pct"), row.get("涨幅"))

    for code, reason_text in reason_map.items():
        snapshot.setdefault(code, {})["推荐理由"] = reason_text
    return snapshot


def build_postmarket_snapshot(target_date: str) -> Dict[str, Dict[str, Any]]:
    report = load_latest_report("postmarket_cashflow", target_date)
    items = report.get("items", []) if isinstance(report, dict) else []
    dataset_rows: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        dataset_id = str(item.get("dataset_id") or "").strip()
        capture_path = Path(str(item.get("capture_path") or "").strip())
        if not dataset_id or not capture_path.exists():
            continue
        payload = json.loads(capture_path.read_text(encoding="utf-8"))
        rows = payload.get("rows", [])
        dataset_rows[dataset_id] = rows if isinstance(rows, list) else []

    snapshot: Dict[str, Dict[str, Any]] = {}
    for row in dataset_rows.get("review.fupan.plate", []):
        code = zero_pad_stock_code(row.get("代码") or row.get("code"))
        if not code:
            continue
        snapshot.setdefault(code, {}).update(
            {
                "题材名称": str(row.get("题材名称") or "").strip(),
                "板数": str(row.get("板数") or "").strip(),
                "连板": parse_int_like(row.get("连板"), 0),
                "开板次数": parse_int_like(row.get("开板"), 0),
                "封单额": parse_chinese_amount(row.get("封单额")),
                "复盘成交额": parse_chinese_amount(row.get("成交额")),
                "复盘换手率": to_float(row.get("换手率")),
                "异动原因": str(row.get("异动原因") or "").strip(),
                "细标签": list(row.get("细标签列表") or []),
                "涨停类型": str(row.get("涨停类型") or "").strip(),
            }
        )

    for dataset_id in ["cashflow.stock.today", "cashflow.stock.3day", "cashflow.stock.5day", "cashflow.stock.10day"]:
        for row in dataset_rows.get(dataset_id, []):
            code = zero_pad_stock_code(row.get("代码") or row.get("code"))
            if not code:
                continue
            bucket = snapshot.setdefault(code, {})
            bucket[f"{dataset_id}.rank"] = parse_int_like(row.get("排名"), 0)
            bucket[f"{dataset_id}.main_net_inflow"] = parse_chinese_amount(row.get("主力净流入"))
            bucket[f"{dataset_id}.change_pct"] = to_float(row.get("涨跌幅"))
    return snapshot


def split_reason_text(reason_text: str) -> List[str]:
    return [part.strip() for part in str(reason_text or "").replace("；", ";").split(";") if part.strip()]


def summarize_signals(reason_text: str, scene: str) -> Dict[str, Any]:
    parts = split_reason_text(reason_text)
    tags: List[str] = []
    theme_ranks: List[int] = []
    if any("竞价爆量" in part for part in parts):
        tags.append("竞价爆量")
    if any(("末秒抢筹" in part) or ("区间抢筹" in part) for part in parts):
        tags.append("抢筹")
    if any("竞价净额" in part for part in parts):
        tags.append("竞价净额")
    if any("封单历史" in part for part in parts):
        tags.append("封单历史")
    if any("主题匹配" in part for part in parts):
        tags.append("主题匹配")
        for part in parts:
            match = __import__('re').search(r"主题前(\d+)", part)
            if match:
                theme_ranks.append(int(match.group(1)))
    if scene == "盘中联动推荐" and any(keyword in reason_text for keyword in ["火箭榜", "热股榜", "热门池", "飙升榜", "资金流向", "跨时段", "联动"]):
        tags.append("跨时段共振")
    return {
        "tags": tags,
        "theme_ranks": theme_ranks,
        "strong_theme": any(rank <= 10 for rank in theme_ranks),
        "weak_theme": any(rank >= 30 for rank in theme_ranks),
    }


def build_logic_reflection(
    scene: str,
    reason_text: str,
    close_pct: float,
    auction_pct: Optional[float],
    rec_pct: Optional[float],
    amount: float,
    turn: float,
) -> Tuple[str, str]:
    signal_info = summarize_signals(reason_text, scene)
    tags = signal_info["tags"]
    strong_theme = signal_info["strong_theme"]
    weak_theme = signal_info["weak_theme"]

    baseline_label = ""
    baseline_value: Optional[float] = None
    if scene == "盘前推荐" and auction_pct is not None:
        baseline_label = "竞价"
        baseline_value = auction_pct
    elif scene == "盘中联动推荐" and rec_pct is not None:
        baseline_label = "推荐时"
        baseline_value = rec_pct
    elif auction_pct is not None:
        baseline_label = "竞价"
        baseline_value = auction_pct

    excess_return = None if baseline_value is None else round(close_pct - baseline_value, 2)
    score_value = close_pct if scene == PREVIOUS_DAY_SCENE or excess_return is None else excess_return

    if score_value >= 3:
        label = "强兑现"
    elif score_value >= 1:
        label = "正向兑现"
    elif score_value > -1:
        label = "基本符合"
    elif score_value > -3:
        label = "偏弱"
    else:
        label = "明显失效"

    lead = f"次日收盘收益 {close_pct:.2f}%" if scene == PREVIOUS_DAY_SCENE else (
        f"相对{baseline_label}超额收益 {excess_return:+.2f}%（收盘涨幅 {close_pct:.2f}%）" if excess_return is not None else f"收盘涨幅 {close_pct:.2f}%"
    )

    strengths: List[str] = []
    weaknesses: List[str] = []
    rules: List[str] = []

    liquidity_ok = amount >= 1_000_000_000 or turn >= 12
    liquidity_weak = amount < 500_000_000 and turn < 6

    if score_value >= 1:
        if "跨时段共振" in tags:
            strengths.append("跨时段共振能提升确定性")
            rules.append("提高‘盘前候选+盘中多榜/资金继续强化’组合权重")
        if "抢筹" in tags and ("竞价净额" in tags or liquidity_ok):
            strengths.append("抢筹信号在资金承接配合下有效")
            rules.append("保留‘抢筹+净额/承接’的共振型信号")
        if "主题匹配" in tags and strong_theme:
            strengths.append("前排主线主题匹配是有效增益项")
            rules.append("继续偏向主题排名靠前且能与个股标签直接对上的票")
        if "封单历史" in tags and score_value >= 3 and ("抢筹" in tags or "竞价净额" in tags):
            strengths.append("封单历史在当日资金确认后才真正有用")
            rules.append("封单历史只作为加分项，不单独抬升排序")
    else:
        if scene == "盘前推荐" and auction_pct is not None and auction_pct >= 7:
            weaknesses.append("高竞价本身透支了日内赔率")
            rules.append("对高开过高的票单独加追高惩罚，不能只看强度")
        if scene == "盘中联动推荐" and rec_pct is not None and rec_pct >= 5:
            weaknesses.append("盘中确认点偏晚，超额空间已被市场吃掉")
            rules.append("盘中联动票要单列‘入场时涨幅上限’，避免确认太晚")
        if "主题匹配" in tags and weak_theme:
            weaknesses.append("后排弱主题匹配对收益贡献有限")
            rules.append("弱主题或后排题材只可辅助，不应主导推荐理由")
        if "封单历史" in tags and "抢筹" not in tags and "竞价净额" not in tags:
            weaknesses.append("历史封单缺少当日资金确认，稳定性不足")
            rules.append("封单历史必须搭配当日资金或抢筹确认才可上调")
        if liquidity_weak:
            weaknesses.append("成交额和换手不足，承接不够扎实")
            rules.append("后续下调低流动性票的推荐优先级")

    if not strengths and score_value >= 1:
        strengths.append("方向判断基本正确，但需要继续细化哪类信号贡献最大")
    if not weaknesses and score_value < 1:
        weaknesses.append("方向没有形成足够超额，说明当前理由还不够支撑交易优势")
    if not rules:
        if score_value >= 1:
            rules.append("保留当前主导信号组合，但继续用更多样本验证稳定性")
        else:
            rules.append("把这类信号组合降权，直到找到更稳定的确认条件")

    strength_text = "；".join(strengths[:2])
    weakness_text = "；".join(weaknesses[:2])
    rule_text = "；".join(rules[:2])

    if score_value >= 1:
        reflection = f"{lead}。说明 {strength_text}。规则上，{rule_text}。"
    else:
        reflection = f"{lead}。暴露出 {weakness_text}。规则上，{rule_text}。"
    return label, reflection


def build_next_day_advice(
    scene: str,
    reason_text: str,
    label: str,
    daily_snapshot: Dict[str, Any],
    postmarket_snapshot: Dict[str, Any],
    auction_pct: Optional[float],
    rec_pct: Optional[float],
) -> str:
    close_pct = daily_snapshot.get("收盘涨幅") or 0.0
    baseline = rec_pct if scene == "盘中联动推荐" and rec_pct is not None else auction_pct
    excess_return = round(close_pct - baseline, 2) if baseline is not None else None

    today_rank = postmarket_snapshot.get("cashflow.stock.today.rank") or 0
    rank3 = postmarket_snapshot.get("cashflow.stock.3day.rank") or 0
    rank5 = postmarket_snapshot.get("cashflow.stock.5day.rank") or 0
    rank10 = postmarket_snapshot.get("cashflow.stock.10day.rank") or 0
    board_count = postmarket_snapshot.get("连板") or 0
    open_board = postmarket_snapshot.get("开板次数") or 0
    seal_amount = postmarket_snapshot.get("封单额") or 0.0
    theme = str(postmarket_snapshot.get("题材名称") or "").strip()
    breakout_20 = bool(daily_snapshot.get("breakout_20"))
    above_ma5 = bool(daily_snapshot.get("above_ma5"))
    above_ma10 = bool(daily_snapshot.get("above_ma10"))
    above_ma20 = bool(daily_snapshot.get("above_ma20"))
    close_near_high = bool(daily_snapshot.get("close_near_high"))
    upper_shadow_pct = float(daily_snapshot.get("upper_shadow_pct") or 0.0)
    volume_ratio_5 = daily_snapshot.get("volume_ratio_5")
    strong_flow = any(rank and rank <= 20 for rank in [today_rank, rank3, rank5, rank10])
    medium_flow = any(rank and rank <= 60 for rank in [today_rank, rank3, rank5, rank10])
    trend_ok = above_ma5 and above_ma10 and (above_ma20 or breakout_20)

    if scene == PREVIOUS_DAY_SCENE:
        if close_pct >= 5 and strong_flow and trend_ok:
            return f"次日处理意见：可继续列入优先跟踪。{theme or '题材'}方向有资金延续，日线站上关键均线，若下一交易日高开不超过 3% 且回踩不破前收，可优先看承接后的低吸/分时转强。"
        if close_pct >= 0:
            return f"次日处理意见：保留观察，但不宜无脑追。重点看 {theme or '题材'} 是否还有新增催化，以及下一交易日能否守住前收和 MA5，若高开过大或开盘即跌回前收下方则放弃。"
        return "次日处理意见：转为观察或放弃。次日兑现不足说明盘后逻辑没有形成持续优势，除非下一交易日出现更强资金回流和分时反包，否则不再作为优先方案。"

    if label == "强兑现" and strong_flow and trend_ok and close_near_high and upper_shadow_pct <= 2.0:
        return f"次日处理意见：偏强看待，可列入优先处理。{theme or '相关题材'}叠加资金流向靠前，日线处于强趋势，若下一交易日高开不超过 3% 且分时回踩承接稳定，可继续关注换手后的二次进攻。"
    if label in {"强兑现", "正向兑现"} and (medium_flow or board_count >= 1) and trend_ok:
        return f"次日处理意见：以分歧低吸为主，不追一致性高开。当前日线仍在上升结构里，但已经兑现过一段，下一交易日重点看是否守住前收/MA5；若高开过大、上影过长或开板次数继续增加，应以兑现和回避为主。"
    if excess_return is not None and excess_return < 0:
        return "次日处理意见：谨慎处理，原则上不主动追。虽然收盘未必差，但相对入场基线没有给出超额收益，说明赔率已被透支，下一交易日除非明显低开后出现强承接反包，否则更适合降级观察。"
    if not trend_ok or upper_shadow_pct >= 3.0 or open_board >= 2:
        return "次日处理意见：偏兑现，不做激进接力。技术上存在上影/开板/趋势支撑不足的问题，下一交易日更适合先看是否守住前收与均线，再决定是否保留，若早盘承接弱则直接回避。"
    return "次日处理意见：暂列观察名单。结合盘后资金和日线结构看，还没有强到可以直接追击，下一交易日优先观察是否放量站稳前高、是否延续主力净流入，再决定是否提升优先级。"


def compute_excess_return(scene: str, close_pct: Optional[float], auction_pct: Optional[float], rec_pct: Optional[float]) -> Optional[float]:
    close_pct = close_pct if close_pct is not None else None
    if close_pct is None:
        return None
    if scene == "盘中联动推荐" and rec_pct is not None:
        return round(close_pct - rec_pct, 2)
    if auction_pct is not None:
        return round(close_pct - auction_pct, 2)
    return round(close_pct, 2)


def classify_handling_bucket(advice: str) -> str:
    if "优先处理" in advice or "优先跟踪" in advice:
        return "优先处理"
    if "分歧低吸" in advice:
        return "分歧低吸"
    if "偏兑现" in advice:
        return "偏兑现"
    if "谨慎处理" in advice:
        return "谨慎观察"
    if "观察名单" in advice or "保留观察" in advice:
        return "观察等待"
    return "其他"


def aggregate_strategy_summary(updated_samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    buckets: Dict[str, Dict[str, Any]] = {}
    handling_buckets: Dict[str, Dict[str, Any]] = {}
    for item in updated_samples:
        scene = str(item.get("scene") or "").strip()
        reason_text = str(item.get("推荐理由") or "").strip()
        signal_info = summarize_signals(reason_text, scene)
        tags = list(signal_info.get("tags") or [])
        if scene == "盘前推荐":
            tags.append("盘前推荐")
        elif scene == "盘中联动推荐":
            tags.append("盘中联动推荐")

        close_pct = to_float(item.get("收盘涨幅"))
        auction_pct = to_float(item.get("竞价涨幅"))
        rec_pct = to_float(item.get("推荐时涨幅"))
        excess_return = compute_excess_return(scene, close_pct, auction_pct, rec_pct)
        advice = str(item.get("反思结论") or "")
        handling = classify_handling_bucket(advice)
        handling_bucket = handling_buckets.setdefault(handling, {"count": 0, "codes": [], "avg_excess": []})
        handling_bucket["count"] += 1
        if item.get("code"):
            handling_bucket["codes"].append(str(item.get("code")))
        if excess_return is not None:
            handling_bucket["avg_excess"].append(excess_return)

        for tag in dict.fromkeys(tags):
            bucket = buckets.setdefault(tag, {"count": 0, "wins": 0, "excess_returns": [], "codes": []})
            bucket["count"] += 1
            if excess_return is not None and excess_return > 0:
                bucket["wins"] += 1
            if excess_return is not None:
                bucket["excess_returns"].append(excess_return)
            if item.get("code"):
                bucket["codes"].append(str(item.get("code")))

    signal_stats: List[Dict[str, Any]] = []
    for tag, bucket in buckets.items():
        avg_excess = average(bucket["excess_returns"])
        win_rate = round(bucket["wins"] / bucket["count"], 4) if bucket["count"] else 0.0
        signal_stats.append(
            {
                "signal": tag,
                "count": bucket["count"],
                "win_rate": win_rate,
                "avg_excess_return": round(avg_excess, 2) if avg_excess is not None else None,
                "codes": bucket["codes"][:8],
            }
        )
    base_signal_names = {"盘前推荐", "盘中联动推荐"}
    family_signal_stats = [item for item in signal_stats if item.get("signal") not in base_signal_names]
    family_signal_stats.sort(key=lambda item: ((item.get("avg_excess_return") is not None), item.get("avg_excess_return") or -999, item.get("count") or 0), reverse=True)
    signal_stats.sort(key=lambda item: ((item.get("avg_excess_return") is not None), item.get("avg_excess_return") or -999, item.get("count") or 0), reverse=True)

    handling_stats: List[Dict[str, Any]] = []
    for bucket_name, bucket in handling_buckets.items():
        avg_excess = average(bucket["avg_excess"])
        handling_stats.append(
            {
                "bucket": bucket_name,
                "count": bucket["count"],
                "avg_excess_return": round(avg_excess, 2) if avg_excess is not None else None,
                "codes": bucket["codes"][:8],
            }
        )
    handling_stats.sort(key=lambda item: item["count"], reverse=True)

    strengthen = [item for item in family_signal_stats if (item.get("count") or 0) >= 2 and (item.get("avg_excess_return") or -999) >= 1]
    weaken = [item for item in family_signal_stats if (item.get("count") or 0) >= 2 and (item.get("avg_excess_return") or 999) <= 0]

    scene_stats = [item for item in signal_stats if item.get("signal") in base_signal_names]

    strategy_lines: List[str] = []
    if strengthen:
        top = strengthen[:3]
        strategy_lines.append("建议加权：" + "；".join(f"{item['signal']}（样本{item['count']}，平均超额{item['avg_excess_return']}%）" for item in top))
    if weaken:
        top = weaken[:3]
        strategy_lines.append("建议降权：" + "；".join(f"{item['signal']}（样本{item['count']}，平均超额{item['avg_excess_return']}%）" for item in top))

    if not strategy_lines:
        strategy_lines.append("当前样本仍偏少，先继续累计，但已可开始跟踪不同信号族的平均超额收益。")

    return {
        "signal_stats": signal_stats,
        "family_signal_stats": family_signal_stats,
        "scene_stats": scene_stats,
        "handling_stats": handling_stats,
        "strategy_summary": strategy_lines,
    }


def target_date_for_scene(scene: str, effective_trade_date: str, previous_trade_date: str) -> str:
    if scene in SAME_DAY_SCENES:
        return effective_trade_date
    if scene == PREVIOUS_DAY_SCENE:
        parsed = safe_date(previous_trade_date)
        if parsed is None:
            return previous_trade_date
        return (parsed + timedelta(days=1)).isoformat()
    return ""


def build_update_fields(
    fields: Dict[str, Any],
    effective_trade_date: str,
    previous_trade_date: str,
    premarket_snapshot: Dict[str, Dict[str, Any]],
    postmarket_snapshot_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    scene = str(fields.get("推荐场景") or "").strip()
    record_date = extract_record_date(fields)
    code = zero_pad_stock_code(fields.get("股票代码"))
    if not code:
        return {}

    if scene in SAME_DAY_SCENES and record_date != effective_trade_date:
        return {}
    if scene == PREVIOUS_DAY_SCENE:
        grade = str(fields.get("推荐分级") or "").strip()
        if record_date != previous_trade_date or "不建议" in grade:
            return {}
    elif scene not in SAME_DAY_SCENES:
        return {}

    close_metrics = get_dailyline_close_metrics(code, effective_trade_date)
    daily_snapshot = build_dailyline_trade_snapshot(code, effective_trade_date)
    if not close_metrics or not daily_snapshot:
        return {}

    update_fields: Dict[str, Any] = {
        "日期": target_date_for_scene(scene, effective_trade_date, previous_trade_date),
        "收盘涨幅": close_metrics["收盘涨幅"],
        "成交额": close_metrics["成交额"],
        "换手率": close_metrics["换手率"],
    }

    snapshot = premarket_snapshot.get(code, {}) if scene == "盘前推荐" else {}

    auction_pct = choose_first_number(fields.get("竞价涨幅"), snapshot.get("竞价涨幅"))
    rec_pct = choose_first_number(fields.get("推荐时涨幅"), snapshot.get("推荐时涨幅"))
    if auction_pct is not None:
        update_fields["竞价涨幅"] = round(auction_pct, 2)
    if rec_pct is not None and (fields.get("推荐时涨幅") in (None, "") or scene == "盘前推荐"):
        update_fields["推荐时涨幅"] = round(rec_pct, 2)
    if scene == "盘前推荐" and snapshot.get("推荐理由") and not str(fields.get("推荐理由") or "").strip():
        update_fields["推荐理由"] = snapshot["推荐理由"]

    relative_to_auction: Optional[float] = None
    if auction_pct is not None:
        relative_to_auction = round(close_metrics["收盘涨幅"] - auction_pct, 2)
        update_fields["收盘相对竞价变化"] = relative_to_auction

    label, reflection = build_logic_reflection(
        scene=scene,
        reason_text=str(fields.get("推荐理由") or update_fields.get("推荐理由") or "").strip(),
        close_pct=close_metrics["收盘涨幅"],
        auction_pct=auction_pct,
        rec_pct=rec_pct,
        amount=close_metrics["成交额"],
        turn=close_metrics["换手率"],
    )
    update_fields["结果评价"] = label
    update_fields["反思结论"] = reflection
    postmarket_snapshot = postmarket_snapshot_map.get(code, {})
    next_day_advice = build_next_day_advice(
        scene=scene,
        reason_text=str(fields.get("推荐理由") or update_fields.get("推荐理由") or "").strip(),
        label=label,
        daily_snapshot=daily_snapshot,
        postmarket_snapshot=postmarket_snapshot,
        auction_pct=auction_pct,
        rec_pct=rec_pct,
    )
    update_fields["反思结论"] = f"{reflection} {next_day_advice}".strip()
    return update_fields


def render_text(summary: Dict[str, Any]) -> str:
    lines = [
        f"duanxianxia｜复盘字段自动回填",
        f"- 目标交易日：{summary['effective_trade_date']}",
        f"- 上一交易日：{summary['previous_trade_date']}",
        f"- 扫描记录数：{summary['scanned_count']}",
        f"- 成功更新：{summary['updated_count']}",
        f"- 跳过：{summary['skipped_count']}",
        f"- 失败：{summary['error_count']}",
        f"- dry_run：{'是' if summary['dry_run'] else '否'}",
    ]
    if summary.get("updated_samples"):
        lines.append("- 更新样例：")
        for item in summary["updated_samples"][:8]:
            lines.append(
                f"  - {item['record_id']}｜{item['scene']}｜{item['code']} {item['name']}｜收盘涨幅 {item.get('收盘涨幅')}｜结果 {item.get('结果评价')}"
            )
    if summary.get("errors"):
        lines.append("- 错误样例：")
        for item in summary["errors"][:5]:
            lines.append(f"  - {item}")
    strategy = summary.get("strategy_summary") or {}
    if strategy.get("strategy_summary"):
        lines.append("- 策略级结论：")
        for item in strategy.get("strategy_summary")[:5]:
            lines.append(f"  - {item}")
    if strategy.get("scene_stats"):
        lines.append("- 推荐场景统计：")
        for item in strategy.get("scene_stats")[:5]:
            lines.append(
                f"  - {item['signal']}｜样本 {item['count']}｜胜率 {round((item.get('win_rate') or 0)*100,1)}%｜平均超额 {item.get('avg_excess_return')}%"
            )
    if strategy.get("family_signal_stats"):
        lines.append("- 信号族统计（前5）：")
        for item in strategy.get("family_signal_stats")[:5]:
            lines.append(
                f"  - {item['signal']}｜样本 {item['count']}｜胜率 {round((item.get('win_rate') or 0)*100,1)}%｜平均超额 {item.get('avg_excess_return')}%"
            )
    return "\n".join(lines)


def persist_strategy_summary(summary: Dict[str, Any], target_date: str) -> Optional[str]:
    strategy = summary.get("strategy_summary") or {}
    if not strategy:
        return None
    root = REPORT_ROOT / target_date / "review_strategy"
    root.mkdir(parents=True, exist_ok=True)
    output = {
        "project": "duanxianxia",
        "group": "review_strategy",
        "generated_at": datetime.now(TZ_SHANGHAI).isoformat(),
        "target_date": target_date,
        "effective_trade_date": summary.get("effective_trade_date"),
        "previous_trade_date": summary.get("previous_trade_date"),
        "updated_count": summary.get("updated_count"),
        "strategy_summary": strategy,
    }
    path = root / f"{datetime.now(TZ_SHANGHAI).strftime('%H%M%S')}.json"
    path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill duanxianxia Feishu review records from downloaded daily lines")
    parser.add_argument("--target-date", default="", help="Trade date in Asia/Shanghai, default today")
    parser.add_argument("--meta-name", default="duanxianxia_review")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    now_cn = datetime.now(TZ_SHANGHAI)
    target_date = args.target_date or now_cn.strftime("%Y-%m-%d")
    effective_trade_date, previous_trade_date = resolve_trade_dates(target_date)
    premarket_snapshot = build_premarket_snapshot(effective_trade_date)
    postmarket_snapshot_map = build_postmarket_snapshot(effective_trade_date)
    meta, records = fetch_all_records(args.meta_name)

    updated_samples: List[Dict[str, Any]] = []
    errors: List[str] = []
    updated_count = 0
    skipped_count = 0

    for item in records:
        record_id = str(item.get("record_id") or "").strip()
        fields = item.get("fields") or {}
        if not record_id or not isinstance(fields, dict):
            skipped_count += 1
            continue
        update_fields = build_update_fields(fields, effective_trade_date, previous_trade_date, premarket_snapshot, postmarket_snapshot_map)
        if not update_fields:
            skipped_count += 1
            continue
        try:
            if not args.dry_run:
                update_record(meta["app_token"], meta["table_id"], record_id, update_fields)
            updated_count += 1
            if len(updated_samples) < 20:
                effective_reason = str(fields.get("推荐理由") or update_fields.get("推荐理由") or "").strip()
                updated_samples.append(
                    {
                        "record_id": record_id,
                        "scene": fields.get("推荐场景"),
                        "code": fields.get("股票代码"),
                        "name": fields.get("股票名称"),
                        "推荐理由": effective_reason,
                        **update_fields,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{record_id}: {type(exc).__name__}: {exc}")

    summary = {
        "effective_trade_date": effective_trade_date,
        "previous_trade_date": previous_trade_date,
        "scanned_count": len(records),
        "updated_count": updated_count,
        "skipped_count": skipped_count,
        "error_count": len(errors),
        "dry_run": args.dry_run,
        "updated_samples": updated_samples,
        "errors": errors,
    }
    summary["strategy_summary"] = aggregate_strategy_summary(updated_samples)
    summary["strategy_report_path"] = None if args.dry_run else persist_strategy_summary(summary, effective_trade_date)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(render_text(summary))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
