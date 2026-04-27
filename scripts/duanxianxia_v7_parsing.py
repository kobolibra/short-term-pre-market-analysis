"""
duanxianxia_v7_parsing.py — v7.1 共享解析工具

所有 v7.1 标签函数必须通过这些工具来 parse 原始 capture 字段,
绝不直接 int(...) / float(...) 字符串。

设计原则:
1. 接受 None / "" / "-" / "未" / 非预期类型,不抛异常,返回 None 或 default
2. 单元测试覆盖所有边界情况(见文件底部 _self_test)
3. 不依赖外部库(只用 re / typing)

入口测试:
    python scripts/duanxianxia_v7_parsing.py
"""

from __future__ import annotations

import re
from typing import Any, Optional


_MONEY_PATTERN = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*(亿|万|w|W|k|千)?\s*$")
_INT_PATTERN = re.compile(r"^([+-]?\d+)$")
_TIME_PATTERN = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")
_PCT_PATTERN = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*%\s*$")
_DAILYLINE_CODE_PATTERN = re.compile(r"^(?:sz|sh)\.(\d{6})$", re.IGNORECASE)
_PURE_CODE_PATTERN = re.compile(r"^\d{6}$")

_OPEN_TIME_MINUTES = 9 * 60 + 25      # 09:25 = 565
_MORNING_CLOSE_MINUTES = 11 * 60 + 30  # 11:30
_AFTERNOON_OPEN_MINUTES = 13 * 60      # 13:00
_LUNCH_BREAK_MINUTES = _AFTERNOON_OPEN_MINUTES - _MORNING_CLOSE_MINUTES  # 90

_NULLISH_TOKENS = {"", "-", "—", "未", "N/A", "n/a", "null", "None", "none", "NaN", "nan"}


def _is_nullish_str(txt: str) -> bool:
    return txt in _NULLISH_TOKENS


def parse_money_to_wan(s: Any) -> Optional[float]:
    """金额字符串 → 万元 float。

    支持:
      - 数字 (按万元理解)
      - "3.82亿" → 38200.0
      - "6070万" → 6070.0
      - "5282w" / "5282W" → 5282.0
      - "100" (无单位) → 100.0 (按万元)
      - "1,000万" → 1000.0 (容忍千位分隔符)
      - "-" / "未" / "" / None / 非数字字符串 → None
      - bool → None (防御)

    1 亿 = 10000 万。
    """
    if s is None:
        return None
    if isinstance(s, bool):
        return None
    if isinstance(s, (int, float)):
        return float(s)
    if not isinstance(s, str):
        return None
    txt = s.strip().replace(",", "").replace(",", "")
    if _is_nullish_str(txt):
        return None
    m = _MONEY_PATTERN.match(txt)
    if not m:
        return None
    value = float(m.group(1))
    unit = m.group(2)
    if unit == "亿":
        return value * 10000.0
    if unit in ("万", "w", "W"):
        return value
    if unit in ("k", "千"):
        return value / 10.0
    # 无单位 → 视为万元(与 cashflow 字段一致)
    return value


def parse_int_safely(s: Any) -> Optional[int]:
    """整数字符串 → int。

    支持:
      - int / float (float 截断)
      - "3" / "-5" / "0"
      - "3次" / "开板2次" → 抽出第一个整数
      - "-" / "未" / "" / None / 非数字 → None
      - bool → None (防御)
    """
    if s is None:
        return None
    if isinstance(s, bool):
        return None
    if isinstance(s, int):
        return s
    if isinstance(s, float):
        return int(s)
    if not isinstance(s, str):
        return None
    txt = s.strip()
    if _is_nullish_str(txt):
        return None
    m = _INT_PATTERN.match(txt)
    if m:
        return int(m.group(1))
    # 容错:抽出第一段连续整数
    digits_match = re.search(r"[+-]?\d+", txt)
    if digits_match:
        try:
            return int(digits_match.group(0))
        except (TypeError, ValueError):
            return None
    return None


def parse_time_to_minutes_after_open(s: Any) -> Optional[int]:
    """时间字符串 "HH:MM" 或 "HH:MM:SS" → 距 09:25 的有效交易分钟数(跳过午休)。

    映射:
      09:25 → 0
      09:30 → 5
      10:00 → 35
      11:30 → 125
      13:00 → 125 (午休跳过 90 分钟,从 13:00 起继续累计)
      13:01 → 126
      14:57 → 242
      15:00 → 245
      09:00 (开盘前) → 0
      11:45 (午休内,理论不出现) → 125 (收敛到 morning_close)
      None / "-" / "99:99" / 非法格式 → None
    """
    if s is None or not isinstance(s, str):
        return None
    txt = s.strip()
    if _is_nullish_str(txt):
        return None
    m = _TIME_PATTERN.match(txt)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= hh < 24 and 0 <= mm < 60):
        return None
    abs_minutes = hh * 60 + mm
    if abs_minutes <= _OPEN_TIME_MINUTES:
        return 0
    if abs_minutes <= _MORNING_CLOSE_MINUTES:
        return abs_minutes - _OPEN_TIME_MINUTES
    if abs_minutes < _AFTERNOON_OPEN_MINUTES:
        # 午休内的时间戳(理论不出现)→ 收敛到 morning_close
        return _MORNING_CLOSE_MINUTES - _OPEN_TIME_MINUTES
    return abs_minutes - _OPEN_TIME_MINUTES - _LUNCH_BREAK_MINUTES


def parse_pct_string(s: Any) -> Optional[float]:
    """百分比字符串 → 小数。

    支持:
      - "12%" → 0.12
      - "33.5%" → 0.335
      - "0%" → 0.0
      - "-12%" → -0.12
      - 数字(防御,按 "已是百分点" 处理)→ 除以 100
      - "-" / "" / None / 不带 % 的字符串 → None

    主要用于显示场景。数值计算请优先用 晋级数 / 样本数 整数除。
    """
    if s is None:
        return None
    if isinstance(s, bool):
        return None
    if isinstance(s, (int, float)):
        return float(s) / 100.0
    if not isinstance(s, str):
        return None
    txt = s.strip()
    if _is_nullish_str(txt):
        return None
    m = _PCT_PATTERN.match(txt)
    if m:
        return float(m.group(1)) / 100.0
    return None


def safe_div(
    num: Optional[float],
    den: Optional[float],
    *,
    default: float = 0.0,
    den_floor: float = 1e-9,
) -> float:
    """安全除法。num/den 任何一方为 None 或 |den| < den_floor → default。"""
    if num is None or den is None:
        return default
    try:
        if abs(den) < den_floor:
            return default
        return float(num) / float(den)
    except (TypeError, ValueError, ZeroDivisionError):
        return default


def normalize_dailyline_code(s: Any) -> Optional[str]:
    """从 'sz.000001' / 'sh.600036' / '000001' 提取 6 位代码。

    支持:
      - "sz.000001" → "000001"
      - "SH.600036" → "600036"
      - "000001" → "000001"
      - "" / None / "invalid" → None
    """
    if s is None or not isinstance(s, str):
        return None
    txt = s.strip()
    m = _DAILYLINE_CODE_PATTERN.match(txt)
    if m:
        return m.group(1)
    if _PURE_CODE_PATTERN.match(txt):
        return txt
    return None


def parse_status_to_seal_verified(status: Any, status_class: Any = None) -> str:
    """ztpool 状态 → 'sealed' / 'exploded' / 'none'。

    优先用 status_class (CSS class):
      - "success" → "sealed"
      - "zha" / "fail" → "exploded"
    回退用 status 文本:
      - "成" → "sealed"
      - "炸" → "exploded"
    其他 → "none"
    """
    if isinstance(status_class, str):
        cls = status_class.strip().lower()
        if cls == "success":
            return "sealed"
        if cls in ("zha", "fail"):
            return "exploded"
    if isinstance(status, str):
        st = status.strip()
        if st == "成":
            return "sealed"
        if st == "炸":
            return "exploded"
    return "none"


# =========================================================================
# 单元测试
# =========================================================================

def _self_test() -> None:
    # ---- parse_money_to_wan ----
    assert parse_money_to_wan("3.82亿") == 38200.0
    assert parse_money_to_wan("6070万") == 6070.0
    assert parse_money_to_wan("5282万") == 5282.0
    assert parse_money_to_wan("48.5亿") == 485000.0
    assert parse_money_to_wan("1.4亿") == 14000.0
    assert parse_money_to_wan("1.14亿") == 11400.0
    assert parse_money_to_wan("100") == 100.0          # 无单位默认万
    assert parse_money_to_wan("-100") == -100.0
    assert parse_money_to_wan("0") == 0.0
    assert parse_money_to_wan(None) is None
    assert parse_money_to_wan("") is None
    assert parse_money_to_wan("-") is None
    assert parse_money_to_wan("—") is None
    assert parse_money_to_wan("未") is None
    assert parse_money_to_wan("N/A") is None
    assert parse_money_to_wan(150.5) == 150.5
    assert parse_money_to_wan(150) == 150.0
    assert parse_money_to_wan(True) is None
    assert parse_money_to_wan(False) is None
    assert parse_money_to_wan("garbage") is None
    assert parse_money_to_wan("1,000万") == 1000.0
    assert parse_money_to_wan("5282w") == 5282.0
    assert parse_money_to_wan("5282W") == 5282.0
    assert parse_money_to_wan("  3.82亿  ") == 38200.0   # 前后空白
    assert parse_money_to_wan("5千") == 0.5             # 千 = 0.1 万

    # ---- parse_int_safely ----
    assert parse_int_safely("3") == 3
    assert parse_int_safely("0") == 0
    assert parse_int_safely("11") == 11
    assert parse_int_safely("-5") == -5
    assert parse_int_safely(None) is None
    assert parse_int_safely("") is None
    assert parse_int_safely("-") is None
    assert parse_int_safely("未") is None
    assert parse_int_safely(5) == 5
    assert parse_int_safely(5.7) == 5
    assert parse_int_safely(-3.2) == -3
    assert parse_int_safely("abc") is None
    assert parse_int_safely("3次") == 3                # 抽出数字
    assert parse_int_safely("开板2次") == 2
    assert parse_int_safely(True) is None
    assert parse_int_safely(False) is None

    # ---- parse_time_to_minutes_after_open ----
    assert parse_time_to_minutes_after_open("09:25") == 0
    assert parse_time_to_minutes_after_open("09:25:00") == 0
    assert parse_time_to_minutes_after_open("09:25:30") == 0
    assert parse_time_to_minutes_after_open("09:30") == 5
    assert parse_time_to_minutes_after_open("09:30:30") == 5
    assert parse_time_to_minutes_after_open("10:00") == 35
    assert parse_time_to_minutes_after_open("11:30") == 125
    assert parse_time_to_minutes_after_open("13:00") == 125  # 午休后立即
    assert parse_time_to_minutes_after_open("13:01") == 126
    assert parse_time_to_minutes_after_open("14:57") == 242
    assert parse_time_to_minutes_after_open("15:00") == 245
    assert parse_time_to_minutes_after_open("09:00") == 0    # 开盘前
    assert parse_time_to_minutes_after_open("08:00") == 0
    assert parse_time_to_minutes_after_open("12:00") == 125  # 午休内收敛
    assert parse_time_to_minutes_after_open("12:30") == 125
    assert parse_time_to_minutes_after_open(None) is None
    assert parse_time_to_minutes_after_open("") is None
    assert parse_time_to_minutes_after_open("-") is None
    assert parse_time_to_minutes_after_open("99:99") is None
    assert parse_time_to_minutes_after_open("abc") is None
    assert parse_time_to_minutes_after_open(123) is None     # 非字符串

    # ---- parse_pct_string ----
    assert abs(parse_pct_string("12%") - 0.12) < 1e-9
    assert abs(parse_pct_string("33.5%") - 0.335) < 1e-9
    assert parse_pct_string("0%") == 0.0
    assert abs(parse_pct_string("-12%") - (-0.12)) < 1e-9
    assert parse_pct_string(None) is None
    assert parse_pct_string("-") is None
    assert parse_pct_string("12") is None       # 没有 % 不识别
    assert parse_pct_string("") is None
    assert abs(parse_pct_string(12) - 0.12) < 1e-9  # 数字防御:按已是百分点
    assert parse_pct_string(True) is None

    # ---- safe_div ----
    assert safe_div(10, 2) == 5.0
    assert safe_div(10, 0) == 0.0
    assert safe_div(10, 0, default=-1.0) == -1.0
    assert safe_div(None, 2) == 0.0
    assert safe_div(10, None) == 0.0
    assert safe_div(10, 1e-12) == 0.0       # 低于 floor
    assert safe_div(10, 1e-12, den_floor=1e-15) == 1e13
    assert safe_div(-10, 2) == -5.0
    assert safe_div(0, 5) == 0.0

    # ---- normalize_dailyline_code ----
    assert normalize_dailyline_code("sz.000001") == "000001"
    assert normalize_dailyline_code("sh.600036") == "600036"
    assert normalize_dailyline_code("SH.300750") == "300750"
    assert normalize_dailyline_code("SZ.000858") == "000858"
    assert normalize_dailyline_code("000001") == "000001"
    assert normalize_dailyline_code("600036") == "600036"
    assert normalize_dailyline_code("") is None
    assert normalize_dailyline_code(None) is None
    assert normalize_dailyline_code("invalid") is None
    assert normalize_dailyline_code("sz.12345") is None  # 5 位非法
    assert normalize_dailyline_code("12345") is None     # 5 位非法
    assert normalize_dailyline_code("  sz.000001  ") == "000001"

    # ---- parse_status_to_seal_verified ----
    assert parse_status_to_seal_verified("成") == "sealed"
    assert parse_status_to_seal_verified("炸") == "exploded"
    assert parse_status_to_seal_verified("其他") == "none"
    assert parse_status_to_seal_verified(None) == "none"
    assert parse_status_to_seal_verified("") == "none"
    assert parse_status_to_seal_verified("成", "success") == "sealed"
    assert parse_status_to_seal_verified("炸", "zha") == "exploded"
    assert parse_status_to_seal_verified(None, "fail") == "exploded"
    assert parse_status_to_seal_verified(None, "success") == "sealed"
    assert parse_status_to_seal_verified("任何", "SUCCESS") == "sealed"  # case-insensitive
    assert parse_status_to_seal_verified("成", None) == "sealed"
    assert parse_status_to_seal_verified("成", "unknown_class") == "sealed"  # 回退到 status

    print("All v7 parsing utility self-tests passed (80+ assertions).")


if __name__ == "__main__":
    _self_test()
