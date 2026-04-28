"""
duanxianxia_v7_1_industry_t1_label.py — v7.1 industry_t1 标签计算

输入:
  - candidate_themes: 股票候选人的 matched_themes(从 auction concept 抽取并同义归一化后)
  - kaipan_t1_rows: T-1 home.kaipan.plate.summary rows(主标签 top 20)
  - kaipan_t1_meta: T-1 同上 meta(含 subplates / top_plates)
  - params: dict 含 industry_pct_strength_leader 等
  - theme_aliases: List[List[str]],首个为 canonical

输出: dict {theme_canonical_name: label}
  label 取值:
    - hit_strong:leader / hit_strong:rising / hit_strong:absorb_dip
    - hit_weak:fade
    - miss:new_entry

算法:
  1. 将 kaipan_t1_rows 按 板块强度原值 排序,计算 pct_strength(rank_desc/n)
  2. 同样计算 主力流入原值 的 pct_inflow
  3. 对每个 candidate_theme 查找匹配的 kaipan 主标签:
     - 用 canon_map 归一化后直接名称匹配
     - 如果未命中 → 查该题材是否是某个 top 主标签的 subplate → 以父主标签的 pct 减 1 个档位
     - 还不命中 → miss:new_entry
  4. 根据 pct_strength 阈值分档
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# canon_map 辅助(与 theme_history 共享)
# ============================================================================

def build_canon_map(theme_aliases: List[List[str]]) -> Dict[str, str]:
    """从 theme_aliases 构建 name -> canonical_name 映射。
    空表 → 空字典。重复名以首次出现为准。
    """
    canon: Dict[str, str] = {}
    for group in theme_aliases or []:
        if not group:
            continue
        canonical = group[0]
        for name in group:
            name = str(name or "").strip()
            if name and name not in canon:
                canon[name] = canonical
    return canon


def canonicalize(name: str, canon_map: Dict[str, str]) -> str:
    """应用 canon_map 归一化名称。未命中 → 原名。"""
    name = str(name or "").strip()
    return canon_map.get(name, name)


# ============================================================================
# 百分位计算
# ============================================================================

def _to_float_or_zero(v: Any) -> float:
    try:
        if v in (None, "", "-"):
            return 0.0
        return float(str(v).strip())
    except Exception:
        return 0.0


def _compute_pct_ranks(rows: List[Dict[str, Any]], field: str) -> Dict[str, float]:
    """计算每行在 field 上的百分位(0..1, 高为佳)。返回 {主标签名称: pct}。

    pct = (排名从低到高 - 1) / (n - 1),n=1 时返回 1.0
    """
    if not rows:
        return {}
    sorted_rows = sorted(rows, key=lambda r: _to_float_or_zero(r.get(field)))
    n = len(sorted_rows)
    out: Dict[str, float] = {}
    for idx, row in enumerate(sorted_rows):
        name = str(row.get("主标签名称", "") or "").strip()
        if not name:
            continue
        pct = idx / (n - 1) if n > 1 else 1.0
        out[name] = pct
    return out


# ============================================================================
# subplate 反向查找
# ============================================================================

def _build_subplate_to_top_map(
    kaipan_meta: Dict[str, Any],
    canon_map: Dict[str, str],
) -> Dict[str, str]:
    """从 kaipan_meta.subplates 构建 { canonical(子题材名称): canonical(父 top_plate 名称) }。

    依赖 fetcher 落盘所造字段: subplate 抹过平化,带 top_plate_name/top_plate_code。
    """
    out: Dict[str, str] = {}
    subplates = kaipan_meta.get("subplates", []) or []
    for sp in subplates:
        if not isinstance(sp, dict):
            continue
        sub_name = str(sp.get("子题材名称", "") or "").strip()
        top_name = str(sp.get("top_plate_name", "") or "").strip()
        if sub_name and top_name:
            out[canonicalize(sub_name, canon_map)] = canonicalize(top_name, canon_map)
    return out


# ============================================================================
# 主函数
# ============================================================================

def compute_industry_t1_labels(
    candidate_themes: List[str],
    kaipan_t1_rows: List[Dict[str, Any]],
    kaipan_t1_meta: Dict[str, Any],
    params: Dict[str, Any],
    theme_aliases: List[List[str]],
) -> Dict[str, Dict[str, Any]]:
    """为每个 candidate_theme 计算 industry_t1_label 及其依据。

    返回:
        {
          canonical_theme_name: {
            "label": "hit_strong:leader" | "hit_strong:rising" | ...,
            "pct_strength": 0.85,
            "pct_inflow": 0.72,
            "matched_via": "main" | "subplate_fallback" | "miss",
            "matched_main_plate": "算力",
          }
        }
    """
    canon_map = build_canon_map(theme_aliases)

    pct_strength = _compute_pct_ranks(kaipan_t1_rows, "板块强度原值")
    pct_inflow = _compute_pct_ranks(kaipan_t1_rows, "主力流入原值")

    # canonical(主标签名) → (pct_strength, pct_inflow)
    canon_pct: Dict[str, Tuple[float, float]] = {}
    for name in pct_strength:
        canon_name = canonicalize(name, canon_map)
        s_pct = pct_strength.get(name, 0.0)
        i_pct = pct_inflow.get(name, 0.0)
        # 多个原名映射到同一 canonical(同义词同一天同时出现)取最大值
        prev = canon_pct.get(canon_name, (-1.0, -1.0))
        canon_pct[canon_name] = (max(prev[0], s_pct), max(prev[1], i_pct))

    sub_to_top = _build_subplate_to_top_map(kaipan_t1_meta, canon_map)

    th_leader = float(params.get("industry_pct_strength_leader", 0.75))
    th_inflow_leader = float(params.get("industry_pct_inflow_leader", 0.70))
    th_rising = float(params.get("industry_pct_strength_rising", 0.50))
    th_absorb = float(params.get("industry_pct_strength_absorb_dip", 0.25))
    downgrade_steps = int(params.get("industry_subplate_downgrade_steps", 1))

    def _classify(s: float, i: float) -> str:
        if s >= th_leader and i >= th_inflow_leader:
            return "hit_strong:leader"
        if s >= th_rising:
            return "hit_strong:rising"
        if s >= th_absorb:
            return "hit_strong:absorb_dip"
        return "hit_weak:fade"

    # 降档顺序(从高到低),用于 subplate fallback
    label_order = ["hit_strong:leader", "hit_strong:rising", "hit_strong:absorb_dip", "hit_weak:fade"]

    def _downgrade(label: str, steps: int) -> str:
        try:
            idx = label_order.index(label)
        except ValueError:
            return label
        idx = min(len(label_order) - 1, idx + steps)
        return label_order[idx]

    out: Dict[str, Dict[str, Any]] = {}
    for theme in candidate_themes or []:
        if not theme:
            continue
        canonical = canonicalize(theme, canon_map)
        if canonical in out:
            continue

        if canonical in canon_pct:
            s, i = canon_pct[canonical]
            label = _classify(s, i)
            out[canonical] = {
                "label": label,
                "pct_strength": s,
                "pct_inflow": i,
                "matched_via": "main",
                "matched_main_plate": canonical,
            }
        elif canonical in sub_to_top:
            parent = sub_to_top[canonical]
            if parent in canon_pct:
                s, i = canon_pct[parent]
                base_label = _classify(s, i)
                label = _downgrade(base_label, downgrade_steps)
                out[canonical] = {
                    "label": label,
                    "pct_strength": s,
                    "pct_inflow": i,
                    "matched_via": "subplate_fallback",
                    "matched_main_plate": parent,
                }
            else:
                out[canonical] = {
                    "label": "miss:new_entry",
                    "pct_strength": 0.0,
                    "pct_inflow": 0.0,
                    "matched_via": "miss",
                    "matched_main_plate": "",
                }
        else:
            out[canonical] = {
                "label": "miss:new_entry",
                "pct_strength": 0.0,
                "pct_inflow": 0.0,
                "matched_via": "miss",
                "matched_main_plate": "",
            }
    return out


# ============================================================================
# 内置自测(运行本文件时触发)
# ============================================================================

def _self_test() -> None:
    rows = [
        {"主标签名称": "算力", "板块强度原值": "11276", "主力流入原值": "94412"},
        {"主标签名称": "半导体产业链", "板块强度原值": "8000", "主力流入原值": "40000"},
        {"主标签名称": "华为", "板块强度原值": "5000", "主力流入原值": "20000"},
        {"主标签名称": "大消费", "板块强度原值": "1000", "主力流入原值": "-5000"},
    ]
    meta = {
        "subplates": [
            {"子题材名称": "液冷服务器", "top_plate_name": "算力", "top_plate_code": "abc"},
            {"子题材名称": "机器人", "top_plate_name": "机器人", "top_plate_code": "xyz"},
        ]
    }
    aliases = [
        ["算力", "数据中心", "AI服务器"],
        ["半导体产业链", "芯片", "封测"],
    ]
    params = {
        "industry_pct_strength_leader": 0.75,
        "industry_pct_inflow_leader": 0.70,
        "industry_pct_strength_rising": 0.50,
        "industry_pct_strength_absorb_dip": 0.25,
    }

    out = compute_industry_t1_labels(
        candidate_themes=["数据中心", "芯片", "大消费", "未知题材", "液冷服务器"],
        kaipan_t1_rows=rows,
        kaipan_t1_meta=meta,
        params=params,
        theme_aliases=aliases,
    )

    # 数据中心 → canon=算力 → pct 顶 → leader
    assert out["算力"]["label"] == "hit_strong:leader", out["算力"]
    assert out["算力"]["matched_via"] == "main"
    # 芯片 → canon=半导体产业链 → pct=2/3 ≈ 0.67 → rising
    assert out["半导体产业链"]["label"] == "hit_strong:rising", out["半导体产业链"]
    # 大消费 → 末位 → fade
    assert out["大消费"]["label"] == "hit_weak:fade", out["大消费"]
    # 未知题材 → miss
    assert out["未知题材"]["label"] == "miss:new_entry"
    # 液冷服务器 → 子标签 → 父=算力(leader) → 降档为 rising
    assert out["液冷服务器"]["label"] == "hit_strong:rising", out["液冷服务器"]
    assert out["液冷服务器"]["matched_via"] == "subplate_fallback"

    # canon_map 隐式测试
    cm = build_canon_map(aliases)
    assert cm["数据中心"] == "算力"
    assert cm["芯片"] == "半导体产业链"
    assert canonicalize("不在表里的", cm) == "不在表里的"

    print("industry_t1_label _self_test passed")


if __name__ == "__main__":
    _self_test()
