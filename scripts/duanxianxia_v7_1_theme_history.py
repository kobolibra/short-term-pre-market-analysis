"""
duanxianxia_v7_1_theme_history.py — v7.1 theme_history 标签计算

输入:
  - theme_name: 候选题材名(未归一化)
  - kaipan_history: List[(date_str, rows, meta)],按日期降序 T-1, T-2, ..., T-N
  - theme_aliases: 同 v7.0 / industry_t1_label 一致
  - params: dict 含 theme_history_top_n 等

输出: dict
  {
    "theme_canonical": "算力",
    "streak_days": 3,
    "streak_dates": ["2026-04-25", "2026-04-24", "2026-04-23"],
    "label": "day3_high" | "day2_main" | "day1_fermenting" | "fading" | "fresh",
  }

诡计算逻辑:
  - streak = 从 T-1 起向后走,题材(归一化后)出现于 top_n 主标签的连续天数
  - streak == 0 → fresh(今天可能是首次,或者在 history 窗口里不出现)
  - streak == 1 → day1_fermenting
  - streak == 2 → day2_main
  - streak == 3 → day3_high
  - streak >= 4 → fading

注: spec 中 theme_history_day1_min/day2_min/day3_min/fading_min 是为未来引入 "在该天所需主标签排名"
阈值预留。当前实现仅用 streak,后续可扩展。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from duanxianxia_v7_1_industry_t1_label import build_canon_map, canonicalize


def _extract_top_main_plate_set(
    rows: List[Dict[str, Any]],
    canon_map: Dict[str, str],
    top_n: int,
) -> set:
    """从某天 kaipan rows 中取 top_n 主标签名称(归一化后)。

    rows 本身不保证有序,按 主标签序号 排序后取前 top_n;备选是按 板块强度原值 降序。
    为 robust 起见,优先按 主标签序号 升序(fetcher 落盘时已按强度排序过)。
    """
    def _seq(row: Dict[str, Any]) -> int:
        try:
            v = row.get("主标签序号")
            return int(v) if v not in (None, "") else 9999
        except Exception:
            return 9999

    sorted_rows = sorted(rows or [], key=_seq)
    out: set = set()
    for row in sorted_rows[:top_n]:
        name = str(row.get("主标签名称", "") or "").strip()
        if name:
            out.add(canonicalize(name, canon_map))
    return out


def compute_theme_history(
    theme_name: str,
    kaipan_history: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]],
    params: Dict[str, Any],
    theme_aliases: List[List[str]],
) -> Dict[str, Any]:
    """计算单个题材的 theme_history 标签。见文件头部详解。"""
    canon_map = build_canon_map(theme_aliases)
    canonical = canonicalize(theme_name, canon_map)
    top_n = int(params.get("theme_history_top_n", 20))

    streak = 0
    streak_dates: List[str] = []
    for date_str, rows, _meta in (kaipan_history or []):
        top_set = _extract_top_main_plate_set(rows, canon_map, top_n)
        if canonical in top_set:
            streak += 1
            streak_dates.append(date_str)
        else:
            break

    if streak == 0:
        label = "fresh"
    elif streak == 1:
        label = "day1_fermenting"
    elif streak == 2:
        label = "day2_main"
    elif streak == 3:
        label = "day3_high"
    else:
        label = "fading"

    return {
        "theme_canonical": canonical,
        "streak_days": streak,
        "streak_dates": streak_dates,
        "label": label,
    }


def compute_theme_history_batch(
    theme_names: List[str],
    kaipan_history: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]],
    params: Dict[str, Any],
    theme_aliases: List[List[str]],
) -> Dict[str, Dict[str, Any]]:
    """批量计算多个题材(去重 canonicalize 后)。返回 {canonical: result}。"""
    canon_map = build_canon_map(theme_aliases)
    seen: Dict[str, Dict[str, Any]] = {}
    for name in theme_names or []:
        canonical = canonicalize(name, canon_map)
        if canonical in seen:
            continue
        seen[canonical] = compute_theme_history(name, kaipan_history, params, theme_aliases)
    return seen


# ============================================================================
# 内置自测
# ============================================================================

def _self_test() -> None:
    aliases = [
        ["算力", "数据中心", "AI服务器"],
        ["半导体产业链", "芯片"],
    ]
    params = {"theme_history_top_n": 20}

    def _row(name: str, seq: int) -> Dict[str, Any]:
        return {"主标签名称": name, "主标签序号": seq}

    # 场景 1: 连续 3 天出现
    history_3 = [
        ("2026-04-25", [_row("算力", 1), _row("半导体产业链", 2)], {}),
        ("2026-04-24", [_row("数据中心", 1)], {}),
        ("2026-04-23", [_row("AI服务器", 5)], {}),
        ("2026-04-22", [_row("未知题材", 1)], {}),  # 中断
    ]
    r = compute_theme_history("算力", history_3, params, aliases)
    assert r["streak_days"] == 3, r
    assert r["label"] == "day3_high", r
    assert r["theme_canonical"] == "算力"
    assert r["streak_dates"] == ["2026-04-25", "2026-04-24", "2026-04-23"]

    # 场景 2: T-1 不出现
    history_0 = [
        ("2026-04-25", [_row("未知", 1)], {}),
        ("2026-04-24", [_row("算力", 1)], {}),  # 不连续,不计
    ]
    r = compute_theme_history("算力", history_0, params, aliases)
    assert r["streak_days"] == 0
    assert r["label"] == "fresh"

    # 场景 3: 连续 1 天
    history_1 = [
        ("2026-04-25", [_row("芯片", 1)], {}),
        ("2026-04-24", [_row("未知", 1)], {}),
    ]
    r = compute_theme_history("半导体产业链", history_1, params, aliases)
    assert r["streak_days"] == 1
    assert r["label"] == "day1_fermenting"

    # 场景 4: 4+ 连 → fading
    history_5 = [
        ("2026-04-25", [_row("算力", 1)], {}),
        ("2026-04-24", [_row("算力", 2)], {}),
        ("2026-04-23", [_row("算力", 3)], {}),
        ("2026-04-22", [_row("算力", 4)], {}),
        ("2026-04-21", [_row("算力", 5)], {}),
    ]
    r = compute_theme_history("算力", history_5, params, aliases)
    assert r["streak_days"] == 5
    assert r["label"] == "fading"

    # 场景 5: top_n 限制(排名 25 超出阈值 → 不计)
    history_outranked = [
        ("2026-04-25", [_row("算力", 25)] + [_row(f"填充{i}", i) for i in range(1, 25)], {}),
    ]
    r = compute_theme_history("算力", history_outranked, {"theme_history_top_n": 20}, aliases)
    assert r["streak_days"] == 0, r
    assert r["label"] == "fresh"

    # 场景 6: 同义词名称检查(今天 “AI服务器” 明天 “算力” → 同一个 canonical)
    history_alias = [
        ("2026-04-25", [_row("AI服务器", 1)], {}),
        ("2026-04-24", [_row("算力", 1)], {}),
    ]
    r = compute_theme_history("数据中心", history_alias, params, aliases)
    assert r["streak_days"] == 2, r
    assert r["label"] == "day2_main"
    assert r["theme_canonical"] == "算力"

    # batch 测试
    batch = compute_theme_history_batch(
        ["数据中心", "AI服务器", "芯片"], history_alias, params, aliases
    )
    # 数据中心 + AI服务器 去重 → 只 1 项;芯片 是另一个 canonical
    assert set(batch.keys()) == {"算力", "半导体产业链"}, batch

    print("theme_history _self_test passed")


if __name__ == "__main__":
    _self_test()
