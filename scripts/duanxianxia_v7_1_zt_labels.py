"""
duanxianxia_v7_1_zt_labels.py — v7.1 涨停三件套:zt_pattern + zt_quality + zt_seal_verified

输入: 候选股代码, fupan_t1 (review.fupan.plate), ztpool_t1 (home.ztpool), params

zt_pattern 取值:
  - 首板 / 二板 / 三板加(>=3) / 一字 / 烂板 / 反包板 / 无
  仅靠 fupan_t1 推导;反包板需 T-2 数据 → 本模块不处理,预留接口

zt_quality 取值: clean / average / dirty
  quality_score = 0.5 * seal_score + 0.3 * open_score + 0.2 * time_score
  - seal_score = min(seal_amount / amount / target_ratio, 1.0)
  - open_score = max(0, 1 - 开板 / open_punish_max)
  - time_score = max(0, 1 - 最后封板分钟 / 240)
  阈值 clean=0.7 / average=0.4

zt_seal_verified 取值: sealed / exploded / none
  查 ztpool 中状态样式(success/zha/fail) → 封 / 炸 / 未
  不存在 → none
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from duanxianxia_v7_parsing import (
    parse_money_to_wan,
    parse_int_safely,
    parse_time_to_minutes_after_open,
    parse_status_to_seal_verified,
    safe_div,
)
from duanxianxia_v7_1_stock_t1_label import _index_by_code


# ============================================================================
# zt_pattern
# ============================================================================

def _classify_zt_pattern(fupan_row: Dict[str, Any]) -> str:
    """从 fupan_row 推导 zt_pattern。"""
    if not fupan_row:
        return "无"
    zt_type = str(fupan_row.get("涨停类型", "") or "").strip()
    boards = parse_int_safely(fupan_row.get("板数"))
    open_count = parse_int_safely(fupan_row.get("开板"))

    if zt_type and "一字" in zt_type:
        return "一字"
    if open_count is not None and open_count >= 3:
        return "烂板"
    if boards is None or boards <= 0:
        return "无"
    if boards == 1:
        return "首板"
    if boards == 2:
        return "二板"
    return "三板加"


# ============================================================================
# zt_quality
# ============================================================================

def _compute_zt_quality(fupan_row: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    seal_target = float(params.get("zt_quality_seal_target_ratio", 0.3))
    open_punish_max = float(params.get("zt_quality_open_punish_max_count", 3))
    time_decay_min = float(params.get("zt_quality_time_decay_minutes", 240))
    clean_min = float(params.get("zt_quality_clean_min", 0.7))
    avg_min = float(params.get("zt_quality_average_min", 0.4))

    seal_wan = parse_money_to_wan(fupan_row.get("封单额"))
    amount_wan = parse_money_to_wan(fupan_row.get("成交额"))
    open_count = parse_int_safely(fupan_row.get("开板"))
    last_seal_time = fupan_row.get("最后封板")
    last_seal_minutes = parse_time_to_minutes_after_open(last_seal_time)

    # seal_score:封单额 / 成交额 针对阈值归一化
    if seal_wan is None or amount_wan is None or amount_wan <= 0:
        seal_score = 0.0
        seal_ratio = None
    else:
        seal_ratio = safe_div(seal_wan, amount_wan, default=0.0, den_floor=1e-6)
        seal_score = max(0.0, min(seal_ratio / max(seal_target, 1e-6), 1.0))

    # open_score: 开板越多越低
    if open_count is None:
        open_score = 0.5  # 未知 → 中性
    else:
        open_score = max(0.0, 1.0 - open_count / max(open_punish_max, 1.0))

    # time_score: 封板越早越佳,4h 线性衰减
    if last_seal_minutes is None:
        time_score = 0.5
    else:
        time_score = max(0.0, 1.0 - last_seal_minutes / max(time_decay_min, 1.0))
        time_score = min(time_score, 1.0)

    score = 0.5 * seal_score + 0.3 * open_score + 0.2 * time_score
    if score >= clean_min:
        label = "clean"
    elif score >= avg_min:
        label = "average"
    else:
        label = "dirty"

    return {
        "label": label,
        "score": score,
        "seal_score": seal_score,
        "open_score": open_score,
        "time_score": time_score,
        "seal_ratio": seal_ratio,
        "last_seal_minutes": last_seal_minutes,
        "open_count": open_count,
    }


# ============================================================================
# zt_seal_verified
# ============================================================================

def _compute_zt_seal_verified(ztpool_row: Optional[Dict[str, Any]]) -> str:
    if ztpool_row is None:
        return "none"
    status = ztpool_row.get("状态")
    status_class = ztpool_row.get("状态样式")
    return parse_status_to_seal_verified(status, status_class)


# ============================================================================
# 主函数
# ============================================================================

def compute_zt_labels(
    candidate_codes: List[str],
    fupan_t1: List[Dict[str, Any]],
    ztpool_t1: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """为每个候选股计算 zt_pattern / zt_quality / zt_seal_verified 三件套。

    返回:
        {code: {pattern, quality_label, quality_score, seal_verified, ...}}
    """
    fupan_idx = _index_by_code(fupan_t1)
    ztpool_idx = _index_by_code(ztpool_t1)

    out: Dict[str, Dict[str, Any]] = {}
    for raw in candidate_codes or []:
        code = str(raw or "").strip()
        if not code:
            continue
        if "." in code:
            code = code.split(".")[-1]
        if len(code) >= 6:
            code = code[-6:]
        if code in out:
            continue

        fupan_row = fupan_idx.get(code) or {}
        ztpool_row = ztpool_idx.get(code)

        pattern = _classify_zt_pattern(fupan_row)
        quality = _compute_zt_quality(fupan_row, params)
        seal_verified = _compute_zt_seal_verified(ztpool_row)

        out[code] = {
            "pattern": pattern,
            "quality_label": quality["label"],
            "quality_score": quality["score"],
            "quality_breakdown": {
                "seal_score": quality["seal_score"],
                "open_score": quality["open_score"],
                "time_score": quality["time_score"],
                "seal_ratio": quality["seal_ratio"],
                "last_seal_minutes": quality["last_seal_minutes"],
                "open_count": quality["open_count"],
            },
            "seal_verified": seal_verified,
            "in_fupan": bool(fupan_row),
            "in_ztpool": ztpool_row is not None,
        }
    return out


def _self_test() -> None:
    params = {
        "zt_quality_seal_target_ratio": 0.3,
        "zt_quality_open_punish_max_count": 3,
        "zt_quality_time_decay_minutes": 240,
        "zt_quality_clean_min": 0.7,
        "zt_quality_average_min": 0.4,
    }
    fupan = [
        # 首板 清纯:封单额 3.82亿 / 成交 1.4亿 → ratio=2.73 → cap 1.0; open=0; time=09:30 → 5分钟
        {"代码": "000001", "板数": "1", "涨停类型": "", "开板": "0", "封单额": "3.82亿", "成交额": "1.4亿", "最后封板": "09:30"},
        # 二板 average:封/成=0.15 → score=0.5;open=1; time=10:30 → 65分钟
        {"代码": "000002", "板数": "2", "涨停类型": "", "开板": "1", "封单额": "6000万", "成交额": "40000万", "最后封板": "10:30"},
        # 烂板 脘骨 dirty: 开板 5 次 → pattern=烂板
        {"代码": "000003", "板数": "1", "涨停类型": "", "开板": "5", "封单额": "500万", "成交额": "30000万", "最后封板": "14:30"},
        # 一字
        {"代码": "000004", "板数": "3", "涨停类型": "一字", "开板": "0", "封单额": "5亿", "成交额": "5000万", "最后封板": "09:25"},
    ]
    ztpool = [
        {"代码": "000001", "状态": "成", "状态样式": "success"},
        {"代码": "000002", "状态": "炸", "状态样式": "zha"},
    ]
    out = compute_zt_labels(["000001", "000002", "000003", "000004", "999999"], fupan, ztpool, params)

    assert out["000001"]["pattern"] == "首板", out["000001"]
    assert out["000001"]["quality_label"] == "clean", out["000001"]
    assert out["000001"]["seal_verified"] == "sealed"
    assert out["000002"]["pattern"] == "二板"
    # 000002:seal=0.5, open=2/3=0.667, time=(240-65)/240=0.729 → 0.5*0.5+0.3*0.667+0.2*0.729=0.595 → average
    assert out["000002"]["quality_label"] == "average", out["000002"]
    assert out["000002"]["seal_verified"] == "exploded"
    assert out["000003"]["pattern"] == "烂板"
    assert out["000004"]["pattern"] == "一字"
    assert out["000004"]["quality_label"] == "clean"
    assert out["999999"]["pattern"] == "无"
    assert out["999999"]["seal_verified"] == "none"
    print("zt_labels _self_test passed")


if __name__ == "__main__":
    _self_test()
