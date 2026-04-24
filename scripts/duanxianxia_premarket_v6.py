#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Premarket scoring v6.

相对 v5 (premarket_5table_v5) 的核心变更:
  1. 真正使用 fetcher 拓到的数值字段 (量比 / 今竞额比昨竞额 /
     换手 / 主力净买 / 抢筹幅度 / 9:15-9:25 封单 / 流通值),
     并以 percentile 打分而非表内行号线性衰减.
  2. 引入第 17 表 (home.qxlive.top_metrics) 作为大盘情绪 gate,
     冰点 / 高潮 / 常态不用同一套打分.
  3. 消费昨日盘后丠张表 (review.daily.top_metrics / review.ltgd.range /
     review.fupan.plate / home.ztpool) 实现 "昨日断板今回封 / 昨日题材龙头 /
     昨日进龙头高度" 这类经典感股的显式加分.
  4. 方向一致性校验: 4 竞价表至少 2 个同向才给完整数值分.
  5. untradable 由 "离涨停 0.2%" 换为 "封单厚度 + 封单递减 +
     流通值 + 主力净买" 的复合判定.
  6. 魔数全部外部化到 projects/duanxianxia/config/premarket_scoring.yaml,
     带 schema_version. 没装 pyyaml / 没配置文件也能跑 (fallback _DEFAULT_CONFIG).
  7. 主题 overlay 加入 "股票在子标签里的位次" 纬度, 不仅仅是字符串命中.
  8. fengdan 表的 board_label (连板标签) 为剩余打分奇点.

对外入口: build_premarket_analysis_v6(report, project_root=None, config=None) -> dict
返回值同形于 v5 中的 build_premarket_analysis, 保证 report.py / CLI 直接兼容.

Integration: see projects/duanxianxia/docs/premarket-v6-integration.md
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:  # pyyaml 可选
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore

VERSION = "premarket_5table_v6"
DEFAULT_CONFIG_PATH = Path("projects/duanxianxia/config/premarket_scoring.yaml")


# =============================================================================
# 1. 通用解析工具 (与 duanxianxia_batch.py 里的版本语义一致, 这里重写以保持模块独立)
# =============================================================================

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip().replace(",", "")
        if text.endswith("%"):
            text = text[:-1]
        return float(text)
    except Exception:
        return default


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"(\d{6})", text)
    return match.group(1) if match else text


def _parse_chinese_amount_wan(value: Any) -> float:
    """把 '1.2亿' '3000万' '1,200' 统一转为万元为单位的 float."""
    text = str(value or "").strip().replace(",", "")
    if not text:
        return 0.0
    mult_to_wan = 1.0
    if text.endswith("亿"):
        mult_to_wan = 1e4
        text = text[:-1]
    elif text.endswith("万"):
        mult_to_wan = 1.0
        text = text[:-1]
    try:
        return float(text) * mult_to_wan
    except Exception:
        return 0.0


SHORT_THEME_TOKENS = {
    "AI", "AR", "VR", "MR", "ST", "5G", "6G",
    "CPO", "MPO", "OCS", "PCB", "CPC", "GPU", "IP",
}


def _normalize_theme_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", "", text)
    for suffix in ("概念股", "概念", "板块", "题材"):
        if text.endswith(suffix) and len(text) > len(suffix):
            text = text[: -len(suffix)]
            break
    return text.strip("|-_/，,、；;")


def _is_noise_theme(token: str) -> bool:
    if not token or token in {"-", "暂无", "无", "首板", "反包", "连板"}:
        return True
    if re.fullmatch(r"\d+板", token) or re.fullmatch(r"\d+天\d+板", token):
        return True
    return False


def _split_theme_tokens(*values: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if value is None:
            continue
        parts = value if isinstance(value, list) else [value]
        for part in parts:
            for raw in re.split(r"[|、,/，；;]+", str(part or "")):
                token = _normalize_theme_token(raw)
                if _is_noise_theme(token):
                    continue
                if len(token) < 2 and token not in SHORT_THEME_TOKENS:
                    continue
                if token in seen:
                    continue
                seen.add(token)
                out.append(token)
    return out


def _theme_token_matches(left: str, right: str) -> bool:
    a = _normalize_theme_token(left)
    b = _normalize_theme_token(right)
    if not a or not b:
        return False
    if a == b:
        return True
    if (a in b or b in a):
        if min(len(a), len(b)) >= 3:
            return True
        if a in SHORT_THEME_TOKENS or b in SHORT_THEME_TOKENS:
            return True
    return False


def _infer_price_limit_pct(code: Any, name: Any = "") -> float:
    norm = _normalize_code(code)
    name_text = str(name or "").upper()
    if "ST" in name_text:
        return 5.0
    if norm.startswith(("300", "301", "688")):
        return 20.0
    if norm.startswith("8") or norm.startswith("92") or norm.startswith("43"):
        return 30.0
    return 10.0


# =============================================================================
# 2. 配置加载 (与 _DEFAULT_CONFIG 深合并)
# =============================================================================

_DEFAULT_CONFIG: Dict[str, Any] = {
    "version": VERSION,
    "schema_version": 1,
    "rank_scores": {
        "vratio":             {"weight": 0.6, "max_score": 18},
        "qiangchou_grab":     {"weight": 0.6, "max_score": 22},
        "qiangchou_interval": {"weight": 0.6, "max_score": 16},
        "net_amount":         {"weight": 0.6, "max_score": 20},
        "fengdan_live":       {"weight": 0.6, "max_score": 16},
    },
    "numeric_signals": {
        "volume_ratio_multiple":  {"weight": 4.0, "min_value": 1.2},
        "auction_turnover_ratio": {"weight": 4.5, "min_multiplier": 2.0},
        "turnover_rate_pct":      {"weight": 2.0, "min_value": 0.5},
        "main_net_inflow_wan":    {"weight": 5.0, "min_value": 200},
        "grab_strength":          {"weight": 4.0, "min_value": 0.3},
        "seal_amount_925":        {"weight": 3.0, "min_value": 500},
        "seal_amount_growth":     {"weight": 2.0},
        "market_cap_yi": {
            "weight": 2.0,
            "sweet_spot_min": 20,
            "sweet_spot_max": 150,
            "over_cap_threshold": 500,
            "over_cap_penalty": 1.0,
        },
    },
    "source_hit_bonuses": {"1": 0, "2": 4, "3": 10, "4": 18, "5": 25},
    "direction_consistency": {"min_positive_signals": 2, "penalty_if_below": 0.3},
    "untradable": {
        "hard_pct_gap": 0.2,
        "keep_if_seal_thick_wan": 3000,
        "keep_if_seal_growth": True,
        "keep_if_main_net_positive_wan": 5000,
        "keep_if_small_cap_yi": 80,
    },
    "theme_overlay": {
        "max_matches": 2,
        "top_match_weight": 1.0,
        "second_match_weight": 0.5,
        "cap": 12,
        "graylist": ["数字经济", "大科技", "新质生产力", "国企改革", "专精特新", "新能源", "大消费"],
        "gray_penalty_multiplier": 0.5,
        "sub_leader_bonus": {"rank_1": 2.5, "rank_2_3": 1.5, "rank_4_5": 0.8},
        "theme_rank_bonus": {"top3": 3.0, "top5": 2.0, "top10": 1.0},
        "theme_rank_penalty": {
            "over_120": -8.0, "over_80": -5.0, "over_50": -3.0, "over_20": -1.0,
        },
    },
    "risk_penalty": {
        "inflow_lt_minus_80000_wan": 0.15,
        "inflow_lt_minus_30000_wan": 0.10,
        "inflow_lt_0_wan": 0.05,
        "no_positive_direction_penalty": 0.10,
        "market_regime_cold_penalty": 0.15,
    },
    "market_regime": {
        "cold_thresholds": {"ZTBX_max": 15, "LBBX_max": 15, "HSLN_min_yi": 10000},
        "hot_thresholds":  {"ZTBX_min": 40, "LBGD_min": 6},
        "cold_score_multiplier": 0.65,
        "hot_score_multiplier":  1.15,
        "cold_max_candidates": 5,
        "hot_max_candidates":  10,
        "normal_max_candidates": 10,
    },
    "yesterday_postmarket": {
        "enabled": True,
        "lookback_calendar_days": 5,
        "prev_day_broken_board_bonus": 4.0,
        "prev_day_continuation_bonus": 3.0,
        "in_ltgd_range_bonus": 3.0,
        "theme_continuation_bonus": 2.0,
        "prev_theme_leader_bonus": 2.5,
        "prev_day_loser_penalty": -2.0,
    },
    "output": {
        "default_top_n": 10,
        "tiebreaker": "market_cap_yi_asc",
        "include_breakdown": True,
        "include_regime": True,
    },
}


def _deep_update(base: Dict[str, Any], override: Dict[str, Any]) -> None:
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v


def load_premarket_config(path: Optional[Path] = None,
                          project_root: Optional[Path] = None) -> Dict[str, Any]:
    cfg: Dict[str, Any] = json.loads(json.dumps(_DEFAULT_CONFIG))
    candidates: List[Path] = []
    if path is not None:
        candidates.append(Path(path))
    if project_root is not None:
        candidates.append(Path(project_root) / DEFAULT_CONFIG_PATH)
    candidates.append(DEFAULT_CONFIG_PATH)
    seen = set()
    for p in candidates:
        try:
            rp = p.resolve()
        except Exception:
            rp = p
        if rp in seen:
            continue
        seen.add(rp)
        if yaml is None or not p.exists():
            continue
        try:
            raw = yaml.safe_load(p.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                _deep_update(cfg, raw)
                break
        except Exception:
            continue
    return cfg


# =============================================================================
# 3. capture 加载 (用于昨日盘后四张表)
# =============================================================================

def _load_rows_from_file(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else None
    return [r for r in (rows or []) if isinstance(r, dict)]


def _latest_capture_in_dir(dataset_dir: Path) -> Path:
    if not dataset_dir.exists():
        return Path("")
    files = sorted(dataset_dir.glob("*.json"))
    return files[-1] if files else Path("")


def _resolve_prev_trading_day_captures(
    project_root: Path, today: date, dataset_ids: Iterable[str], lookback_days: int
) -> Dict[str, List[Dict[str, Any]]]:
    """在 captures/<YYYY-MM-DD>/<dataset_id>/ 下从 today-1 回找, 取第一个存在文件的日子."""
    result: Dict[str, List[Dict[str, Any]]] = {ds: [] for ds in dataset_ids}
    capture_root = project_root / "captures"
    for delta in range(1, max(1, lookback_days) + 1):
        day = today - timedelta(days=delta)
        day_dir = capture_root / day.isoformat()
        if not day_dir.exists():
            continue
        hit_any = False
        for ds in dataset_ids:
            if result.get(ds):
                continue
            path = _latest_capture_in_dir(day_dir / ds)
            if path and path.exists():
                result[ds] = _load_rows_from_file(path)
                if result[ds]:
                    hit_any = True
        if hit_any and all(result[ds] for ds in dataset_ids):
            break
    return result


# =============================================================================
# 4. 大盘情绪 gate (第 17 表)
# =============================================================================

# home.qxlive.top_metrics 里的 metric_key 在 duanxianxia_fetcher.py 中是大写简称.
# 这里支持多种 key 格式的匹配, 以免版本漂移.
_QXLIVE_KEYS = {
    "ZTBX": ["ZTBX", "ztbx", "涨停晋级率", "涨停捡宝"],
    "LBBX": ["LBBX", "lbbx", "连板晋级率", "连板捡宝"],
    "PBBX": ["PBBX", "pbbx"],
    "HSLN": ["HSLN", "hsln", "沪深量能"],
    "LBGD": ["LBGD", "lbgd", "连板高度"],
    "QX":   ["QX", "qx", "情绪线"],
    "ZT":   ["ZT", "zt"],
    "DT":   ["DT", "dt"],
}


def _qxlive_metric_value(rows: List[Dict[str, Any]], logical_key: str) -> Optional[float]:
    aliases = {a.lower() for a in _QXLIVE_KEYS.get(logical_key, [logical_key])}
    for row in rows:
        key = str(row.get("metric_key") or row.get("key") or row.get("name") or "").strip().lower()
        if key in aliases:
            for field in ("current_value", "button_value", "chart_tail_value", "value"):
                if field in row:
                    raw = row.get(field)
                    if raw in (None, "", "--"):
                        continue
                    num = _safe_float(raw, float("nan"))
                    if num == num:  # not NaN
                        return num
    return None


def _classify_regime(qxlive_rows: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Dict[str, Any]:
    regime_cfg = cfg["market_regime"]
    cold = regime_cfg["cold_thresholds"]
    hot = regime_cfg["hot_thresholds"]

    ztbx = _qxlive_metric_value(qxlive_rows, "ZTBX")
    lbbx = _qxlive_metric_value(qxlive_rows, "LBBX")
    hsln = _qxlive_metric_value(qxlive_rows, "HSLN")
    lbgd = _qxlive_metric_value(qxlive_rows, "LBGD")

    reasons: List[str] = []
    is_cold = False
    is_hot = False

    if ztbx is not None and ztbx <= cold["ZTBX_max"]:
        is_cold = True
        reasons.append(f"ZTBX={ztbx:.1f}<={cold['ZTBX_max']}")
    if lbbx is not None and lbbx <= cold["LBBX_max"]:
        is_cold = True
        reasons.append(f"LBBX={lbbx:.1f}<={cold['LBBX_max']}")
    if hsln is not None and hsln <= cold["HSLN_min_yi"]:
        is_cold = True
        reasons.append(f"HSLN={hsln:.0f}<= {cold['HSLN_min_yi']}")

    if ztbx is not None and ztbx >= hot["ZTBX_min"]:
        is_hot = True
        reasons.append(f"ZTBX={ztbx:.1f}>={hot['ZTBX_min']}")
    if lbgd is not None and lbgd >= hot["LBGD_min"]:
        is_hot = True
        reasons.append(f"LBGD={lbgd:.0f}>={hot['LBGD_min']}")

    # 冲突时冷优先 (风险控制)
    if is_cold and is_hot:
        is_hot = False
        reasons.append("conflict->cold")

    if is_cold:
        label = "cold"
        multiplier = regime_cfg["cold_score_multiplier"]
        cap = regime_cfg["cold_max_candidates"]
    elif is_hot:
        label = "hot"
        multiplier = regime_cfg["hot_score_multiplier"]
        cap = regime_cfg["hot_max_candidates"]
    else:
        label = "normal"
        multiplier = 1.0
        cap = regime_cfg["normal_max_candidates"]

    return {
        "label": label,
        "multiplier": multiplier,
        "max_candidates": cap,
        "reasons": reasons,
        "metrics": {"ZTBX": ztbx, "LBBX": lbbx, "HSLN": hsln, "LBGD": lbgd},
    }


# =============================================================================
# 5. 候选池汇聚 + 数值分布算
# =============================================================================

def _merge_candidates(rows_by_source: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    numeric_fields = [
        "volume_ratio_multiple", "auction_turnover_wan",
        "yesterday_auction_turnover_wan", "turnover_rate_pct",
        "main_net_inflow_wan", "grab_strength", "market_cap_yi",
        "seal_amount_wan", "auction_change_pct", "latest_change_pct",
    ]
    text_fields = ["amount_915", "amount_920", "amount_925", "board_label", "section_kind"]

    for source, rows in rows_by_source.items():
        for row in rows:
            code = _normalize_code(row.get("code"))
            if not code:
                continue
            item = merged.setdefault(code, {
                "code": code,
                "name": str(row.get("name") or "").strip(),
                "sources": set(),
                "rank_by_source": {},
                "concept_tokens": [],
                "concept_token_set": set(),
            })
            if not item["name"]:
                item["name"] = str(row.get("name") or "").strip()
            item["sources"].add(source)

            rank = _safe_int(row.get("rank"), 999)
            prev = item["rank_by_source"].get(source, 999)
            if rank < prev:
                item["rank_by_source"][source] = rank

            if source == "qiangchou":
                group = str(row.get("group") or "").strip()
                if group == "grab":
                    item["has_qiangchou_grab"] = True
                    item["rank_qiangchou_grab"] = min(
                        item.get("rank_qiangchou_grab", 999), rank
                    )
                else:
                    item["has_qiangchou_interval"] = True
                    item["rank_qiangchou_interval"] = min(
                        item.get("rank_qiangchou_interval", 999), rank
                    )

            for f in numeric_fields:
                new_v = _safe_float(row.get(f), 0.0)
                cur_v = _safe_float(item.get(f), 0.0)
                if abs(new_v) > abs(cur_v):
                    item[f] = new_v
                elif f not in item:
                    item[f] = row.get(f)

            for f in text_fields:
                if not item.get(f) and row.get(f):
                    item[f] = row.get(f)

            for token in _split_theme_tokens(
                row.get("concept"), row.get("concept_1"), row.get("concept_2"),
                row.get("tag_1"), row.get("tag_2"), row.get("tag_3"), row.get("tags"),
            ):
                if token in item["concept_token_set"]:
                    continue
                item["concept_token_set"].add(token)
                item["concept_tokens"].append(token)

    return merged


def _build_universes(candidates: Dict[str, Dict[str, Any]]) -> Dict[str, List[float]]:
    vals: Dict[str, List[float]] = defaultdict(list)
    for item in candidates.values():
        v = _safe_float(item.get("volume_ratio_multiple"), 0.0)
        if v > 0: vals["volume_ratio_multiple"].append(v)
        today = _safe_float(item.get("auction_turnover_wan"), 0.0)
        yday = _safe_float(item.get("yesterday_auction_turnover_wan"), 0.0)
        if today > 0 and yday > 0:
            vals["auction_turnover_ratio"].append(today / yday)
        t = _safe_float(item.get("turnover_rate_pct"), 0.0)
        if t > 0: vals["turnover_rate_pct"].append(t)
        n = _safe_float(item.get("main_net_inflow_wan"), 0.0)
        if n > 0: vals["main_net_inflow_wan"].append(n)
        g = _safe_float(item.get("grab_strength"), 0.0)
        if g > 0: vals["grab_strength"].append(g)
        s925 = _parse_chinese_amount_wan(item.get("amount_925"))
        if s925 > 0: vals["seal_amount_925"].append(s925)
    return dict(vals)


def _percentile(value: float, universe: List[float], min_value: float) -> float:
    if value < min_value or not universe:
        return 0.0
    n = len(universe)
    lower = sum(1 for v in universe if v < value)
    equal = sum(1 for v in universe if v == value)
    return max(0.0, min(1.0, (lower + equal * 0.5) / n))


# =============================================================================
# 6. 数值分 + rank 分 + 方向一致性
# =============================================================================

def _compute_numeric_score(
    cand: Dict[str, Any],
    universes: Dict[str, List[float]],
    cfg: Dict[str, Any],
) -> Tuple[float, Dict[str, float], int]:
    ns = cfg["numeric_signals"]
    breakdown: Dict[str, float] = {}
    total = 0.0
    positive = 0

    # 竞价量比
    v = _safe_float(cand.get("volume_ratio_multiple"), 0.0)
    p = _percentile(v, universes.get("volume_ratio_multiple", []),
                    ns["volume_ratio_multiple"].get("min_value", 0.0))
    s = p * ns["volume_ratio_multiple"]["weight"]
    if p > 0: positive += 1
    breakdown["volume_ratio_multiple"] = round(s, 2); total += s

    # 今竞额/昨竞额
    today = _safe_float(cand.get("auction_turnover_wan"), 0.0)
    yday = _safe_float(cand.get("yesterday_auction_turnover_wan"), 0.0)
    ratio = (today / yday) if yday > 0 and today > 0 else 0.0
    min_mult = ns["auction_turnover_ratio"].get("min_multiplier", 1.0)
    if ratio >= min_mult:
        p = _percentile(ratio, universes.get("auction_turnover_ratio", []), min_mult)
        s = p * ns["auction_turnover_ratio"]["weight"]
        if p > 0: positive += 1
    else:
        s = 0.0
    breakdown["auction_turnover_ratio"] = round(s, 2); total += s

    # 换手
    v = _safe_float(cand.get("turnover_rate_pct"), 0.0)
    p = _percentile(v, universes.get("turnover_rate_pct", []),
                    ns["turnover_rate_pct"].get("min_value", 0.0))
    s = p * ns["turnover_rate_pct"]["weight"]
    if p > 0: positive += 1
    breakdown["turnover_rate_pct"] = round(s, 2); total += s

    # 主力净买
    v = _safe_float(cand.get("main_net_inflow_wan"), 0.0)
    min_v = ns["main_net_inflow_wan"].get("min_value", 0.0)
    if v >= min_v:
        p = _percentile(v, universes.get("main_net_inflow_wan", []), min_v)
        s = p * ns["main_net_inflow_wan"]["weight"]
        if p > 0: positive += 1
    else:
        s = 0.0
    breakdown["main_net_inflow_wan"] = round(s, 2); total += s

    # 抢筹幅度
    v = _safe_float(cand.get("grab_strength"), 0.0)
    p = _percentile(v, universes.get("grab_strength", []),
                    ns["grab_strength"].get("min_value", 0.0))
    s = p * ns["grab_strength"]["weight"]
    if p > 0: positive += 1
    breakdown["grab_strength"] = round(s, 2); total += s

    # 9:25 封单绝对量
    s925 = _parse_chinese_amount_wan(cand.get("amount_925"))
    s920 = _parse_chinese_amount_wan(cand.get("amount_920"))
    s915 = _parse_chinese_amount_wan(cand.get("amount_915"))
    p = _percentile(s925, universes.get("seal_amount_925", []),
                    ns["seal_amount_925"].get("min_value", 0.0))
    s = p * ns["seal_amount_925"]["weight"]
    if p > 0: positive += 1
    breakdown["seal_amount_925"] = round(s, 2); total += s

    # 封单递增 bonus
    gw = ns["seal_amount_growth"]["weight"]
    if s925 > 0 and s925 >= s920 >= s915 > 0:
        if s925 > s915 * 1.05:
            s = gw; positive += 1
        else:
            s = gw * 0.5
    elif s925 > 0 and s915 > s925 * 1.2:
        s = -gw * 0.5  # 封单缩水
    else:
        s = 0.0
    breakdown["seal_amount_growth"] = round(s, 2); total += s

    # 流通值 sweet spot
    m = ns["market_cap_yi"]
    cap = _safe_float(cand.get("market_cap_yi"), 0.0)
    if m["sweet_spot_min"] <= cap <= m["sweet_spot_max"]:
        s = m["weight"]
    elif cap > m["over_cap_threshold"]:
        s = -m["over_cap_penalty"]
    elif cap > 0 and cap < m["sweet_spot_min"]:
        s = m["weight"] * 0.6  # 微盘略扣 (流动性差)
    elif cap > 0:
        denom = max(1.0, m["over_cap_threshold"] - m["sweet_spot_max"])
        s = max(0.0, m["weight"] * (1 - (cap - m["sweet_spot_max"]) / denom))
    else:
        s = 0.0
    breakdown["market_cap_yi"] = round(s, 2); total += s

    return round(total, 2), breakdown, positive


def _compute_rank_score(cand: Dict[str, Any], cfg: Dict[str, Any]) -> Tuple[float, Dict[str, float]]:
    rs = cfg["rank_scores"]
    ranks = cand["rank_by_source"]
    detail: Dict[str, float] = {}
    total = 0.0

    def _score(source_key: str, cfg_key: str) -> float:
        rank = ranks.get(source_key)
        if rank is None:
            return 0.0
        max_s = rs[cfg_key]["max_score"]
        w = rs[cfg_key]["weight"]
        return max(0.0, max_s - rank) * w

    detail["vratio"] = _score("vratio", "vratio")
    # qiangchou 分 grab / interval
    if cand.get("has_qiangchou_grab"):
        r = cand.get("rank_qiangchou_grab", 999)
        detail["qiangchou_grab"] = max(0.0, rs["qiangchou_grab"]["max_score"] - r) * rs["qiangchou_grab"]["weight"]
    else:
        detail["qiangchou_grab"] = 0.0
    if cand.get("has_qiangchou_interval"):
        r = cand.get("rank_qiangchou_interval", 999)
        detail["qiangchou_interval"] = max(0.0, rs["qiangchou_interval"]["max_score"] - r) * rs["qiangchou_interval"]["weight"]
    else:
        detail["qiangchou_interval"] = 0.0
    detail["net_amount"] = _score("net_amount", "net_amount")
    detail["fengdan_live"] = _score("fengdan", "fengdan_live")

    total = sum(detail.values())
    return round(total, 2), {k: round(v, 2) for k, v in detail.items()}


# =============================================================================
# 7. untradable 复合判定
# =============================================================================

def _is_untradable_v6(cand: Dict[str, Any], cfg: Dict[str, Any]) -> Tuple[bool, str]:
    untr = cfg["untradable"]
    limit_pct = _infer_price_limit_pct(cand.get("code"), cand.get("name"))
    auction_pct = _safe_float(cand.get("auction_change_pct"), 0.0)
    threshold = max(0.0, limit_pct - untr["hard_pct_gap"])
    if auction_pct < threshold:
        return False, ""  # 没接近涨停
    # 接近涨停时, 需要复合判定是否真的难买
    s925 = _parse_chinese_amount_wan(cand.get("amount_925"))
    s915 = _parse_chinese_amount_wan(cand.get("amount_915"))
    main_net = _safe_float(cand.get("main_net_inflow_wan"), 0.0)
    cap = _safe_float(cand.get("market_cap_yi"), 0.0)

    # 任一条命中即使昨难买也保留
    if untr["keep_if_seal_thick_wan"] and s925 >= untr["keep_if_seal_thick_wan"]:
        return False, f"seal925={s925:.0f}w>={untr['keep_if_seal_thick_wan']}"
    if untr.get("keep_if_seal_growth") and s925 > 0 and s915 > 0 and s925 >= s915:
        return False, "seal_growing"
    if untr["keep_if_main_net_positive_wan"] and main_net >= untr["keep_if_main_net_positive_wan"]:
        return False, f"main_net={main_net:.0f}w"
    if untr["keep_if_small_cap_yi"] and 0 < cap <= untr["keep_if_small_cap_yi"]:
        return False, f"small_cap={cap:.1f}亿"

    # 封单缩水 + 近涨停 + 没主力净买 = 真难买, 剩交易日烂板
    if s915 > 0 and s925 > 0 and s915 > s925 * 1.5:
        return True, f"seal_shrink 915={s915:.0f}>925={s925:.0f}"
    # 默认: 近涨停且无其他正向信号, 判 untradable
    return True, f"auction_pct={auction_pct:.2f}>={threshold:.2f}"


# =============================================================================
# 8. 主题 overlay (包含子标签位次)
# =============================================================================

def _build_theme_catalog(plate_summary_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """构造 home.kaipan.plate.summary 的主标签索引.
    每个主标签带 sub_list (子标签序列), 保持源数据序号以便判断子标签位次."""
    catalog: List[Dict[str, Any]] = []
    for idx, row in enumerate(plate_summary_rows):
        name = str(row.get("main_plate_name") or row.get("name") or "").strip()
        if not name:
            continue
        sub_raw = row.get("sub_plate_list") or row.get("sub_list") or ""
        if isinstance(sub_raw, list):
            sub_tokens = [_normalize_theme_token(x) for x in sub_raw]
        else:
            sub_tokens = [_normalize_theme_token(x) for x in re.split(r"[|、,/，；;]+", str(sub_raw))]
        sub_tokens = [t for t in sub_tokens if t and not _is_noise_theme(t)]

        catalog.append({
            "rank": _safe_int(row.get("rank"), idx + 1),
            "name": name,
            "strength": _safe_float(row.get("plate_strength") or row.get("strength"), 0.0),
            "main_inflow_wan": _safe_float(row.get("main_inflow_wan") or row.get("main_net_inflow_wan"), 0.0),
            "zt_count": _safe_int(row.get("zt_count") or row.get("limit_up_count"), 0),
            "sub_tokens": sub_tokens,
        })
    return catalog


def _evaluate_theme_for_candidate(
    cand: Dict[str, Any],
    theme_catalog: List[Dict[str, Any]],
    # stock_sub_rank_map: { (theme_name, code) -> sub_token_rank }
    sub_rank_map: Dict[Tuple[str, str], int],
    cfg: Dict[str, Any],
) -> Tuple[float, List[str]]:
    tcfg = cfg["theme_overlay"]
    graylist = set(tcfg["graylist"])
    tokens = cand.get("concept_tokens", [])
    if not tokens or not theme_catalog:
        return 0.0, []

    matches: List[Tuple[float, str]] = []
    for theme in theme_catalog:
        hit = False
        for t in tokens:
            if _theme_token_matches(t, theme["name"]):
                hit = True
                break
            for sub in theme["sub_tokens"]:
                if _theme_token_matches(t, sub):
                    hit = True
                    break
            if hit:
                break
        if not hit:
            continue

        raw = 2.0
        # 主标签排名奖惩
        rank = theme["rank"]
        rb = tcfg["theme_rank_bonus"]
        rp = tcfg["theme_rank_penalty"]
        if rank <= 3: raw += rb["top3"]
        elif rank <= 5: raw += rb["top5"]
        elif rank <= 10: raw += rb["top10"]
        if rank > 120: raw += rp["over_120"]
        elif rank > 80: raw += rp["over_80"]
        elif rank > 50: raw += rp["over_50"]
        elif rank > 20: raw += rp["over_20"]

        # 股票在该主题子标签里的位次 (v6 新)
        sub_rank = sub_rank_map.get((theme["name"], cand["code"]))
        if sub_rank is not None:
            slb = tcfg["sub_leader_bonus"]
            if sub_rank <= 1: raw += slb["rank_1"]
            elif sub_rank <= 3: raw += slb["rank_2_3"]
            elif sub_rank <= 5: raw += slb["rank_4_5"]

        # 灰名单压力
        if theme["name"] in graylist:
            raw *= tcfg["gray_penalty_multiplier"]

        matches.append((raw, theme["name"]))

    matches.sort(key=lambda x: -x[0])
    kept = matches[: tcfg["max_matches"]]
    if not kept:
        return 0.0, []
    score = kept[0][0] * tcfg["top_match_weight"]
    if len(kept) > 1:
        score += kept[1][0] * tcfg["second_match_weight"]
    score = min(tcfg["cap"], max(0.0, score))
    return round(score, 2), [name for _, name in kept]


# =============================================================================
# 9. 昨日盘后信号
# =============================================================================

def _evaluate_yesterday_signals(
    code: str,
    cand: Dict[str, Any],
    prev_captures: Dict[str, List[Dict[str, Any]]],
    today_theme_names: List[str],
    cfg: Dict[str, Any],
) -> Tuple[float, List[str]]:
    ycfg = cfg["yesterday_postmarket"]
    if not ycfg.get("enabled"):
        return 0.0, []
    bonus = 0.0
    reasons: List[str] = []

    # 昨日涨停池 home.ztpool
    ztpool_rows = prev_captures.get("home_ztpool") or []
    pool_hit = None
    for row in ztpool_rows:
        if _normalize_code(row.get("code")) == code:
            pool_hit = row
            break
    if pool_hit is not None:
        status = str(pool_hit.get("status") or pool_hit.get("board_label") or pool_hit.get("tag_1") or "").strip()
        is_broken = bool(pool_hit.get("broken") or pool_hit.get("broken_board")) or "断板" in status
        is_sealed = (not is_broken) and ("一字" in status or "T" in status or "连" in status
                                         or _safe_int(pool_hit.get("connect_count") or pool_hit.get("lian_count"), 0) >= 1)
        if is_broken:
            bonus += ycfg["prev_day_broken_board_bonus"]
            reasons.append("昨日断板今回封候选")
        elif is_sealed:
            bonus += ycfg["prev_day_continuation_bonus"]
            reasons.append(f"昨日涨停延续({status or '板'})")

    # 昨日龙头高度区间 review.ltgd.range
    ltgd_rows = prev_captures.get("review_ltgd_range") or []
    for row in ltgd_rows:
        if _normalize_code(row.get("code")) == code:
            bonus += ycfg["in_ltgd_range_bonus"]
            reasons.append("昨日进龙头高度")
            break

    # 昨日复盘题材龙头 review.fupan.plate
    fupan_rows = prev_captures.get("review_fupan_plate") or []
    fupan_theme_tokens: List[str] = []
    for row in fupan_rows:
        theme_name = _normalize_theme_token(row.get("plate_name") or row.get("name") or "")
        if not theme_name:
            continue
        fupan_theme_tokens.append(theme_name)
        leaders_raw = row.get("leader_codes") or row.get("leader_list") or row.get("leaders") or ""
        if isinstance(leaders_raw, list):
            leader_list = [_normalize_code(x) for x in leaders_raw]
        else:
            leader_list = [_normalize_code(x) for x in re.split(r"[|、,/，；;]+", str(leaders_raw))]
        leader_list = [x for x in leader_list if x]
        if code in leader_list[:2]:
            bonus += ycfg["prev_theme_leader_bonus"]
            reasons.append(f"昨日'{theme_name}'题材龙头")
            break

    # 题材延续: 昨日题材今日仍在 16 表前列
    for token in fupan_theme_tokens:
        for t_name in today_theme_names[:10]:
            if _theme_token_matches(token, t_name):
                bonus += ycfg["theme_continuation_bonus"]
                reasons.append(f"题材延续({token})")
                break
        else:
            continue
        break

    # 昨日破顶 / 加速板顶断
    top_rows = prev_captures.get("review_daily_top_metrics") or []
    for row in top_rows:
        key = str(row.get("metric_key") or row.get("key") or "").upper()
        if key == "PBBX":
            val = _safe_float(row.get("current_value") or row.get("value"), 0.0)
            if val >= 30 and cand.get("_yesterday_limit_up"):
                # 是昨饶 / 加速板顶断高危
                bonus += ycfg["prev_day_loser_penalty"]
                reasons.append("昨饶饶位顶断概率升高")
            break

    return round(bonus, 2), reasons


# =============================================================================
# 10. 主入口
# =============================================================================

def _rows_from_report(report: Dict[str, Any], dataset_id: str) -> List[Dict[str, Any]]:
    """从 report dict 里取指定 dataset 的 rows. 兼容两种常见结构:
      - report["datasets"][dataset_id] = {"rows": [...]}
      - report["rows_by_dataset"][dataset_id] = [...]
    """
    if not isinstance(report, dict):
        return []
    datasets = report.get("datasets")
    if isinstance(datasets, dict):
        node = datasets.get(dataset_id)
        if isinstance(node, dict):
            rows = node.get("rows")
            if isinstance(rows, list):
                return [r for r in rows if isinstance(r, dict)]
        elif isinstance(node, list):
            return [r for r in node if isinstance(r, dict)]
    rbd = report.get("rows_by_dataset")
    if isinstance(rbd, dict):
        rows = rbd.get(dataset_id)
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def _resolve_today(report: Dict[str, Any]) -> date:
    for k in ("trading_date", "date", "today"):
        v = report.get(k) if isinstance(report, dict) else None
        if v:
            try:
                return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()
            except Exception:
                continue
    return date.today()


def build_premarket_analysis_v6(
    report: Dict[str, Any],
    project_root: Optional[Path] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """端到端的 v6 盘前分析. 可直接替换 duanxianxia_batch.py 里的
    build_premarket_analysis / compute_premarket_analysis 调用点."""
    cfg = config or load_premarket_config(project_root=project_root)
    today = _resolve_today(report)

    # 1) 6 张盘前表
    rows_by_source: Dict[str, List[Dict[str, Any]]] = {
        "vratio":     _rows_from_report(report, "auction_vratio"),
        "qiangchou":  _rows_from_report(report, "auction_qiangchou"),
        "net_amount": _rows_from_report(report, "auction_net_amount"),
        "fengdan":    [r for r in _rows_from_report(report, "auction_fengdan")
                       if str(r.get("section_kind") or "").lower() == "live"],
    }
    plate_summary_rows = _rows_from_report(report, "home_qxlive_plate_summary")
    qxlive_top_rows = _rows_from_report(report, "home_qxlive_top_metrics")

    # 2) 大盘 gate
    regime = _classify_regime(qxlive_top_rows, cfg)

    # 3) 昨日盘后
    prev_captures: Dict[str, List[Dict[str, Any]]] = {}
    if project_root is not None and cfg["yesterday_postmarket"].get("enabled"):
        prev_captures = _resolve_prev_trading_day_captures(
            Path(project_root), today,
            ["review_daily_top_metrics", "review_ltgd_range",
             "review_fupan_plate", "home_ztpool"],
            cfg["yesterday_postmarket"].get("lookback_calendar_days", 5),
        )

    # 4) 候选池汇聚
    candidates = _merge_candidates(rows_by_source)
    universes = _build_universes(candidates)

    # 5) 主标签索引 + 子标签位次映射
    theme_catalog = _build_theme_catalog(plate_summary_rows)
    today_theme_names = [t["name"] for t in theme_catalog]
    # sub_rank_map 需要知道 "某个股票在某主标签的子标签里的位次".
    # 我们以候选股 concept_tokens 第一个同名子标签的顺序位为近似.
    sub_rank_map: Dict[Tuple[str, str], int] = {}
    for theme in theme_catalog:
        name = theme["name"]
        for idx, sub_token in enumerate(theme["sub_tokens"], start=1):
            for code, cand in candidates.items():
                for ct in cand["concept_tokens"]:
                    if _theme_token_matches(ct, sub_token):
                        key = (name, code)
                        if key not in sub_rank_map or idx < sub_rank_map[key]:
                            sub_rank_map[key] = idx
                        break

    # 6) 单股打分
    ranked: List[Dict[str, Any]] = []
    for code, cand in candidates.items():
        untradable, untradable_reason = _is_untradable_v6(cand, cfg)

        numeric_score, numeric_breakdown, positive_count = _compute_numeric_score(
            cand, universes, cfg
        )
        # 方向一致性扣减
        dc = cfg["direction_consistency"]
        direction_ok = positive_count >= dc["min_positive_signals"]
        if not direction_ok:
            numeric_score *= dc["penalty_if_below"]
            numeric_score = round(numeric_score, 2)

        rank_score, rank_breakdown = _compute_rank_score(cand, cfg)

        sources = cand["sources"]
        source_hit = len(sources)
        sh_cfg = cfg["source_hit_bonuses"]
        source_bonus = float(sh_cfg.get(str(min(source_hit, 5)), 0))

        theme_score, theme_reasons = _evaluate_theme_for_candidate(
            cand, theme_catalog, sub_rank_map, cfg
        )

        yesterday_bonus, yesterday_reasons = _evaluate_yesterday_signals(
            code, cand, prev_captures, today_theme_names, cfg
        )

        raw_score = (numeric_score + rank_score + source_bonus
                     + theme_score + yesterday_bonus)

        # 大盘 gate
        regime_multiplier = regime["multiplier"]
        score_after_regime = raw_score * regime_multiplier

        # 风险惩罚
        rp = cfg["risk_penalty"]
        penalty = 0.0
        # 主题主力流入最差值 (取候选命中的主标签的 inflow)
        worst_inflow: Optional[float] = None
        for theme in theme_catalog:
            if theme["name"] in theme_reasons:
                inflow = theme["main_inflow_wan"]
                if worst_inflow is None or inflow < worst_inflow:
                    worst_inflow = inflow
        if worst_inflow is not None:
            if worst_inflow < -80000: penalty = max(penalty, rp["inflow_lt_minus_80000_wan"])
            elif worst_inflow < -30000: penalty = max(penalty, rp["inflow_lt_minus_30000_wan"])
            elif worst_inflow < 0: penalty = max(penalty, rp["inflow_lt_0_wan"])
        if not direction_ok:
            penalty = max(penalty, rp["no_positive_direction_penalty"])
        if regime["label"] == "cold":
            penalty = max(penalty, rp["market_regime_cold_penalty"])

        ranking_score = round(score_after_regime * (1.0 - penalty), 2)

        # 理由汇总
        reasons: List[str] = []
        # 数值信号 (取 top 3)
        top_numeric = sorted(numeric_breakdown.items(), key=lambda x: -x[1])[:3]
        for name, val in top_numeric:
            if val > 0:
                reasons.append(f"{name}+{val:.1f}")
        reasons.extend(f"题材 {t}" for t in theme_reasons)
        reasons.extend(yesterday_reasons)
        if source_hit >= 3:
            reasons.append(f"{source_hit}表共振+{source_bonus:.0f}")
        if regime["label"] != "normal":
            reasons.append(f"大盘{regime['label']}x{regime_multiplier:.2f}")
        if not direction_ok:
            reasons.append(f"方向不一致x{dc['penalty_if_below']}")
        if penalty > 0:
            reasons.append(f"惩罚-{penalty*100:.0f}%")
        if untradable:
            reasons.append(f"untradable({untradable_reason})")
        if cand.get("board_label"):
            reasons.append(f"[{cand['board_label']}]")

        ranked.append({
            "code": code,
            "name": cand["name"],
            "score": round(raw_score, 2),
            "ranking_score": ranking_score,
            "untradable": untradable,
            "untradable_reason": untradable_reason,
            "source_hit_count": source_hit,
            "sources": sorted(sources),
            "numeric_score": numeric_score,
            "rank_score": rank_score,
            "source_bonus": source_bonus,
            "theme_score": theme_score,
            "theme_matches": theme_reasons,
            "yesterday_bonus": yesterday_bonus,
            "yesterday_reasons": yesterday_reasons,
            "positive_signal_count": positive_count,
            "direction_ok": direction_ok,
            "regime_multiplier": regime_multiplier,
            "risk_penalty": penalty,
            "market_cap_yi": _safe_float(cand.get("market_cap_yi"), 0.0),
            "reasons": reasons,
            **({"breakdown": {
                "numeric": numeric_breakdown,
                "rank": rank_breakdown,
            }} if cfg["output"].get("include_breakdown") else {}),
        })

    # 7) untradable 过滤 + 排序 + 截取
    tradable = [x for x in ranked if not x["untradable"]]
    # 排序: (-ranking_score, -raw_score, -source_hit, 流通值小优先)
    tb = cfg["output"].get("tiebreaker", "market_cap_yi_asc")
    if tb == "market_cap_yi_asc":
        tradable.sort(key=lambda x: (
            -x["ranking_score"], -x["score"], -x["source_hit_count"],
            x["market_cap_yi"] if x["market_cap_yi"] > 0 else 1e9,
            x["code"],
        ))
    else:
        tradable.sort(key=lambda x: (
            -x["ranking_score"], -x["score"], -x["source_hit_count"], x["code"],
        ))

    max_out = min(cfg["output"]["default_top_n"], regime["max_candidates"])
    top = tradable[: max(1, max_out)]

    result: Dict[str, Any] = {
        "version": cfg["version"],
        "schema_version": cfg.get("schema_version", 1),
        "trading_date": today.isoformat(),
        "candidate_count": len(ranked),
        "tradable_count": len(tradable),
        "untradable_count": len(ranked) - len(tradable),
        "top_candidates": top,
        "untradable_candidates": [x for x in ranked if x["untradable"]][:20],
    }
    if cfg["output"].get("include_regime"):
        result["market_regime"] = regime
    return result


# v5 合名兼容 alias
build_premarket_analysis = build_premarket_analysis_v6
compute_premarket_analysis = build_premarket_analysis_v6


__all__ = [
    "VERSION",
    "DEFAULT_CONFIG_PATH",
    "load_premarket_config",
    "build_premarket_analysis_v6",
    "build_premarket_analysis",
    "compute_premarket_analysis",
]
