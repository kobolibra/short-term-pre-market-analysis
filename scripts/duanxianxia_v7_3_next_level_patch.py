"""v7.3 excess-return first recall/ranking overlay.

This patch intentionally makes the premarket design less decorative and more
accountable to the 2026-05-19/2026-05-20 review result:

- optimize for post-auction excess return, not raw close_pct / near-board glamour;
- make hard-tech repair/diffusion the primary actionable repair family;
- split generic broad repair and soft-avoid repair into watch-only buckets;
- demote weak theme catch-up and high-cost board/power/retreat rows;
- keep all production rules premarket-visible; realized returns are diagnostics
  only.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

import duanxianxia_v7_3_output as v73

_APPLIED = False


HARDTECH_DEFAULT_KEYWORDS = [
    "存储", "芯片", "半导体", "半导体设备", "设备", "HBM", "CPO", "MPO", "光模块", "光芯片",
    "GPU", "先进封装", "封装", "元器件", "电阻", "电容", "PCB", "算力", "液冷", "数据中心",
    "端侧AI", "边缘AI", "AI硬件", "服务器", "交换机", "洁净室", "晶圆", "硅片", "光刻", "刻蚀",
    "固态电池", "锂电设备", "智能电网", "电力设备", "仪器", "传感器", "射频", "存算一体",
]
POWER_RETREAT_DEFAULT_KEYWORDS = [
    "火电", "煤电", "水电", "发电", "电厂", "电力运营", "绿色电力", "华能", "京能", "豫能",
]
ROBOT_RETREAT_DEFAULT_KEYWORDS = ["机器人", "减速器", "机器视觉", "工业母机"]
GENERIC_THEME_DEFAULT_KEYWORDS = ["并购重组", "股权转让", "实控人变更", "一季报增长", "业绩增长", "摘帽"]


def _as_float(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _as_int(v: Any, default: int = 0) -> int:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return int(float(str(v).replace(",", "").strip()))
    except Exception:
        return default


def _code(row: Dict[str, Any]) -> str:
    s = str(row.get("code") or row.get("代码") or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else digits


def _tags(row: Dict[str, Any]) -> List[str]:
    return list(dict.fromkeys(row.get("action_tags") or []))


def _add_tags(tags: List[str], *values: str) -> List[str]:
    for value in values:
        if value and value not in tags:
            tags.append(value)
    return tags


def _cfg_list(cfg: Dict[str, Any], key: str, default: Iterable[str]) -> List[str]:
    value = cfg.get(key)
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    return [str(x) for x in default]


def _flatten_text(value: Any, depth: int = 0) -> List[str]:
    if value in (None, "", "-") or depth > 3:
        return []
    if isinstance(value, (str, int, float)):
        return [str(value)]
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(_flatten_text(item, depth + 1))
        return out
    if isinstance(value, dict):
        out = []
        for key in (
            "matched_plate", "matched_tags", "best_theme", "matched_themes", "theme", "themes", "concept", "concepts",
            "概念", "题材", "板块", "industry", "industry_name", "name", "名称",
        ):
            if key in value:
                out.extend(_flatten_text(value.get(key), depth + 1))
        return out
    return []


def _text_blob(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in (
        "name", "名称", "matched_themes", "concept", "concepts", "概念", "题材", "industry", "industry_name",
        "theme_detail", "signal_summary", "auction_detail", "action_tags", "action_reason", "setup_reason",
    ):
        if key in row:
            parts.extend(_flatten_text(row.get(key)))
    return "|".join(parts)


def _has_keyword(row: Dict[str, Any], keywords: Iterable[str]) -> bool:
    blob = _text_blob(row).lower()
    return any(str(k).lower() in blob for k in keywords if str(k).strip())


def _is_20cm(row: Dict[str, Any]) -> bool:
    code = _code(row)
    return code.startswith(("300", "301", "688", "689", "8", "4"))


def _source_families(row: Dict[str, Any]) -> List[str]:
    detail = row.get("auction_detail") or {}
    fam = detail.get("source_families") or row.get("source_families") or []
    if isinstance(fam, list):
        return [str(x) for x in fam]
    return []


def _has_vratio(row: Dict[str, Any]) -> bool:
    detail = row.get("auction_detail") or {}
    if detail.get("vratio_rank") not in (None, ""):
        return True
    return any("vratio" in str(x).lower() or "爆量" in str(x) for x in _source_families(row))


def _market_cold(row: Dict[str, Any]) -> bool:
    regime = str(row.get("regime") or ((row.get("meta") or {}).get("regime")) or "").lower()
    return "cold" in regime or "冰" in regime


def _metrics(row: Dict[str, Any]) -> Dict[str, float]:
    return {
        "pct": float(v73._auction_pct(row) if v73._auction_pct(row) is not None else 0.0),
        "auction": float(v73._metric(row, "auction_strength", 0.0) or 0.0),
        "amount": float(v73._metric(row, "auction_amount_wan", 0.0) or 0.0),
        "liquidity": float(v73._metric(row, "liquidity_score", 50.0) or 50.0),
        "source": float(v73._metric(row, "source_evidence_score", 0.0) or 0.0),
        "family": float(v73._metric(row, "source_family_count", 0.0) or 0.0),
        "theme": float(v73._metric(row, "theme_strength_t0", 0.0) or 0.0),
        "net_pressure": float(v73._metric(row, "net_pressure", 0.0) or 0.0),
    }


def _score_repair(row: Dict[str, Any], cfg: Dict[str, Any], hardtech: bool = False, low_open: bool = False) -> float:
    m = _metrics(row)
    pct = m["pct"]
    if low_open:
        cost_fit = 20.0 if -5.0 <= pct < 0 else (12.0 if -9.0 <= pct < -5.0 else 4.0)
    else:
        cost_fit = 22.0 if -1.5 <= pct <= 2.5 else (16.0 if 2.5 < pct <= 4.5 else (10.0 if -4.0 <= pct < -1.5 else 0.0))
    hard_bonus = float(cfg.get("hardtech_bonus", 16)) if hardtech else 0.0
    cm20_bonus = float(cfg.get("elastic_20cm_bonus", 8)) if _is_20cm(row) else 0.0
    return v73._clamp(
        cost_fit
        + hard_bonus
        + cm20_bonus
        + min(22.0, m["auction"] * 0.35)
        + min(18.0, m["amount"] / 5000.0 * 18.0)
        + min(12.0, m["source"] * 0.30)
        + min(8.0, m["family"] * 3.0)
        + min(6.0, max(0.0, m["theme"] - 20.0) * 0.05)
    )


def _score_elastic(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    m = _metrics(row)
    pct = m["pct"]
    cost_fit = 24.0 if -1.5 <= pct <= 2.0 else (16.0 if -3.5 <= pct <= 4.0 else 0.0)
    return v73._clamp(
        cost_fit
        + (12.0 if _is_20cm(row) else 0.0)
        + (8.0 if _has_vratio(row) else 0.0)
        + min(18.0, m["auction"] * 0.28)
        + min(14.0, m["liquidity"] * 0.14)
        + min(14.0, m["amount"] / 3000.0 * 14.0)
        + min(10.0, m["source"] * 0.25)
    )


def _is_hardtech(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _has_keyword(row, _cfg_list(cfg, "hardtech_keywords", HARDTECH_DEFAULT_KEYWORDS))


def _is_power_or_robot_retreat(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _has_keyword(row, _cfg_list(cfg, "power_retreat_keywords", POWER_RETREAT_DEFAULT_KEYWORDS)) or _has_keyword(row, _cfg_list(cfg, "robot_retreat_keywords", ROBOT_RETREAT_DEFAULT_KEYWORDS))


def _is_generic_theme(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _has_keyword(row, _cfg_list(cfg, "generic_theme_keywords", GENERIC_THEME_DEFAULT_KEYWORDS))


def _is_hardtech_repair_candidate(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    if not _is_hardtech(row, cfg):
        return False
    if str(row.get("action_type")) in {"BOARD_WATCH", "HIGH_COST_REPAIR_WATCH"}:
        return False
    m = _metrics(row)
    if m["pct"] >= float(cfg.get("hardtech_repair_pct_max", 5.2)) or m["pct"] < float(cfg.get("hardtech_repair_pct_min", -6.0)):
        return False
    if m["amount"] < float(cfg.get("hardtech_repair_min_amount_wan", 600)):
        return False
    if m["auction"] < float(cfg.get("hardtech_repair_min_auction_strength", 10)):
        return False
    return _score_repair(row, cfg, hardtech=True) >= float(cfg.get("hardtech_repair_score_min", 34))


def _is_low_cost_elastic(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    m = _metrics(row)
    if not (float(cfg.get("low_cost_elastic_pct_min", -3.5)) <= m["pct"] <= float(cfg.get("low_cost_elastic_pct_max", 4.0))):
        return None
    if m["amount"] < float(cfg.get("low_cost_elastic_min_amount_wan", 500)):
        return None
    if m["auction"] < float(cfg.get("low_cost_elastic_min_auction_strength", 10)):
        return None
    if _is_power_or_robot_retreat(row, cfg) and not _is_hardtech(row, cfg):
        return None
    score = _score_elastic(row, cfg)
    if score < float(cfg.get("low_cost_elastic_score_min", 34)):
        return None
    if _is_20cm(row):
        return "LOW_COST_20CM_ELASTIC"
    if _has_vratio(row) and m["amount"] <= float(cfg.get("low_amount_vratio_max_amount_wan", 2500)):
        return "LOW_AMOUNT_VRATIO_ELASTIC"
    if _is_hardtech(row, cfg):
        return "LOW_COST_ELASTIC_CATCHUP"
    return None


def _is_generic_broad_repair(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    m = _metrics(row)
    if str(row.get("action_type")) not in {"DEBUG_ONLY", "CONFIRMATION_WATCH", "BROAD_REPAIR_MOMENTUM"}:
        return False
    if _is_hardtech(row, cfg) or _is_power_or_robot_retreat(row, cfg):
        return False
    if not (float(cfg.get("generic_repair_pct_min", -2.0)) <= m["pct"] <= float(cfg.get("generic_repair_pct_max", 4.0))):
        return False
    if m["amount"] < float(cfg.get("generic_repair_min_amount_wan", 800)) or m["auction"] < float(cfg.get("generic_repair_min_auction_strength", 12)):
        return False
    return _score_repair(row, cfg, hardtech=False) >= float(cfg.get("generic_repair_score_min", 38))


def _expected_score(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality"))
    m = _metrics(row)
    pct = m["pct"]
    pool_bonus = {
        "HARDTECH_LOW_OPEN_REPAIR": float(cfg.get("expected_bonus_hardtech_low_open", 42)),
        "HARDTECH_REPAIR_MOMENTUM": float(cfg.get("expected_bonus_hardtech_repair", 40)),
        "LOW_COST_20CM_ELASTIC": float(cfg.get("expected_bonus_low_cost_20cm", 38)),
        "LOW_COST_ELASTIC_CATCHUP": float(cfg.get("expected_bonus_low_cost_elastic", 34)),
        "LOW_AMOUNT_VRATIO_ELASTIC": float(cfg.get("expected_bonus_low_amount_vratio", 32)),
        "MOMENTUM_CATCHUP": float(cfg.get("expected_bonus_momentum", 24)),
        "AUCTION_FOLLOW": float(cfg.get("expected_bonus_auction_follow", 16)),
        "LOW_OPEN_REVERSAL": float(cfg.get("expected_bonus_low_open_reversal", 10)),
        "THEME_CATCHUP": float(cfg.get("expected_bonus_theme", 6)),
        "THEME_CATCHUP_CONFIRMATION": -2.0,
        "GENERIC_LOW_OPEN_REPAIR": 0.0,
        "DEEP_LOW_OPEN_REPAIR": -4.0,
        "GENERIC_BROAD_REPAIR": -6.0,
        "SECONDARY_REPAIR_CANDIDATE_HARDTECH": 2.0,
        "SOFT_AVOID_RETREAT": -18.0,
        "SOFT_AVOID_YESTERDAY_BOARD_UNWIND": -20.0,
        "POWER_LOW_OPEN_REPAIR": -22.0,
        "BOARD_WATCH": -24.0,
        "HIGH_COST_REPAIR_WATCH": -24.0,
        "FAKE_STRENGTH_WATCH": -12.0,
        "SOFT_AVOID_REPAIR_CANDIDATE": -15.0,
        "CONFIRMATION_WATCH": -8.0,
        "AVOID": -45.0,
        "DEBUG_ONLY": -80.0,
    }.get(action, -40.0)
    quality_bonus = {
        "hardtech_repair": 10.0,
        "hardtech_low_open_repair": 10.0,
        "low_cost_elastic": 8.0,
        "low_cost_20cm_elastic": 9.0,
        "low_amount_vratio_elastic": 7.0,
        "momentum": 5.0,
        "main_attack": 2.0,
        "repair": 1.0,
        "strong": 2.0,
        "medium": -1.0,
        "weak": -5.0,
        "watch_only": -8.0,
        "soft_avoid": -10.0,
        "hard_avoid": -18.0,
    }.get(quality, 0.0)
    cost_penalty = max(0.0, pct - float(cfg.get("expected_cost_penalty_start_pct", 4.8))) * float(cfg.get("expected_cost_penalty_per_pct", 3.0))
    deep_penalty = 5.0 if pct < -6.0 and action not in {"HARDTECH_LOW_OPEN_REPAIR"} else 0.0
    generic_penalty = 6.0 if _is_generic_theme(row, cfg) and not _is_hardtech(row, cfg) else 0.0
    cold_board_penalty = 8.0 if _market_cold(row) and action in {"BOARD_WATCH", "HIGH_COST_REPAIR_WATCH"} else 0.0
    return (
        pool_bonus
        + quality_bonus
        + min(16.0, m["auction"] * 0.16)
        + min(12.0, m["amount"] / 5000.0 * 12.0)
        + min(8.0, m["source"] * 0.20)
        + min(6.0, m["family"] * 2.0)
        - cost_penalty
        - deep_penalty
        - generic_penalty
        - cold_board_penalty
    )


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({
        "HARDTECH_LOW_OPEN_REPAIR": 8,
        "HARDTECH_REPAIR_MOMENTUM": 10,
        "LOW_COST_20CM_ELASTIC": 12,
        "LOW_COST_ELASTIC_CATCHUP": 14,
        "LOW_AMOUNT_VRATIO_ELASTIC": 16,
        "AUCTION_FOLLOW": 25,
        "MOMENTUM_CATCHUP": 28,
        "LOW_OPEN_REVERSAL": 55,
        "THEME_CATCHUP": 70,
        "GENERIC_LOW_OPEN_REPAIR": 76,
        "THEME_CATCHUP_CONFIRMATION": 82,
        "GENERIC_BROAD_REPAIR": 84,
        "SECONDARY_REPAIR_CANDIDATE_HARDTECH": 86,
        "DEEP_LOW_OPEN_REPAIR": 88,
        "POWER_LOW_OPEN_REPAIR": 90,
        "BOARD_WATCH": 92,
        "HIGH_COST_REPAIR_WATCH": 94,
        "SOFT_AVOID_RETREAT": 96,
        "SOFT_AVOID_YESTERDAY_BOARD_UNWIND": 97,
    })
    for action in [
        "HARDTECH_LOW_OPEN_REPAIR", "HARDTECH_REPAIR_MOMENTUM", "LOW_COST_20CM_ELASTIC",
        "LOW_COST_ELASTIC_CATCHUP", "LOW_AMOUNT_VRATIO_ELASTIC", "AUCTION_FOLLOW", "MOMENTUM_CATCHUP",
    ]:
        v73.ACTIONABLE.add(action)
    # These were too often mistaken as buy-list entries. Keep them visible, but
    # exclude from the executable Top30 unless another stronger rule upgrades them.
    for action in ["BOARD_WATCH", "THEME_CATCHUP", "LOW_OPEN_REVERSAL"]:
        v73.ACTIONABLE.discard(action)
    for action in [
        "THEME_CATCHUP_CONFIRMATION", "GENERIC_LOW_OPEN_REPAIR", "GENERIC_BROAD_REPAIR",
        "SECONDARY_REPAIR_CANDIDATE_HARDTECH", "DEEP_LOW_OPEN_REPAIR", "POWER_LOW_OPEN_REPAIR",
        "HIGH_COST_REPAIR_WATCH", "SOFT_AVOID_RETREAT", "SOFT_AVOID_YESTERDAY_BOARD_UNWIND",
    ]:
        v73.NON_ACTIONABLE_WATCH.add(action)

    def upgrade_row_next(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        out = base_upgrade(row, cfg)
        tags = _tags(out)
        m = _metrics(out)
        action = str(out.get("action_type"))
        quality = str(out.get("signal_quality") or out.get("action_quality") or "")
        hardtech = _is_hardtech(out, cfg)
        retreat = _is_power_or_robot_retreat(out, cfg)

        # Weak theme catch-up was a major source of decorative complexity. It is
        # now confirmation-only unless it is also hard-tech/elastic and upgraded
        # by a stronger rule below.
        if action == "THEME_CATCHUP":
            fam = _as_int(v73._metric(out, "source_family_count", 0), 0)
            amount = float(v73._metric(out, "auction_amount_wan", 0.0) or 0.0)
            if quality != "strong" or fam < int(cfg.get("theme_action_min_source_family_count", 1)) or amount < float(cfg.get("theme_action_min_amount_wan", 1200)) or _is_generic_theme(out, cfg):
                _add_tags(tags, "theme_demoted_confirmation", "not_excess_return_primary")
                score = max(0.0, float(out.get("action_score") or 0.0) - float(cfg.get("theme_confirmation_score_penalty", 12)))
                out.update(action_type="THEME_CATCHUP_CONFIRMATION", action_quality="watch_only", signal_quality="watch_only", action_reason="theme_catchup_demoted_needs_intraday_confirmation", action_score=round(score, 2), action_tags=tags)
                action = "THEME_CATCHUP_CONFIRMATION"

        # Split low-open repair. Only hard-tech low-open repair remains a primary
        # premarket action; generic/deep/power repairs are watch-only.
        if action == "LOW_OPEN_REVERSAL":
            if retreat:
                _add_tags(tags, "power_or_robot_retreat", "low_open_watch_only")
                out.update(action_type="POWER_LOW_OPEN_REPAIR", action_quality="watch_only", signal_quality="watch_only", action_reason="low_open_repair_in_retreat_theme_watch_only", action_tags=tags)
            elif hardtech:
                score = _score_repair(out, cfg, hardtech=True, low_open=True)
                _add_tags(tags, "hardtech", "low_open_repair", "excess_return_primary")
                out.update(action_type="HARDTECH_LOW_OPEN_REPAIR", action_quality="hardtech_low_open_repair", signal_quality="hardtech_low_open_repair", action_reason="hardtech_low_open_repair_excess_return_primary", action_score=round(score, 2), action_confidence=v73._confidence(score, 58, 40), action_tags=tags)
            elif m["pct"] < float(cfg.get("deep_low_open_watch_pct", -5.0)):
                _add_tags(tags, "deep_low_open", "watch_only")
                out.update(action_type="DEEP_LOW_OPEN_REPAIR", action_quality="watch_only", signal_quality="watch_only", action_reason="deep_low_open_repair_requires_intraday_reclaim", action_tags=tags)
            else:
                score = _score_repair(out, cfg, hardtech=False, low_open=True)
                _add_tags(tags, "generic_low_open", "watch_only")
                out.update(action_type="GENERIC_LOW_OPEN_REPAIR", action_quality="watch_only", signal_quality="watch_only", action_reason="generic_low_open_repair_watch_only_after_review", action_score=round(score, 2), action_tags=tags)
            action = str(out.get("action_type"))

        # Hard-tech repair/diffusion was the real 2026-05-20 effective line.
        if _is_hardtech_repair_candidate(out, cfg):
            score = _score_repair(out, cfg, hardtech=True)
            _add_tags(tags, "hardtech", "repair_momentum", "excess_return_primary")
            out.update(action_type="HARDTECH_REPAIR_MOMENTUM", action_quality="hardtech_repair", signal_quality="hardtech_repair", action_reason="hardtech_repair_diffusion_excess_return_primary", action_score=round(score, 2), action_confidence=v73._confidence(score, 60, 42), action_tags=tags)
            action = "HARDTECH_REPAIR_MOMENTUM"

        # Low-cost elastic buckets catch the 20cm / low auction-cost winners that
        # were previously hidden behind DEBUG_ONLY, soft avoid, or generic pools.
        elastic_type = _is_low_cost_elastic(out, cfg)
        if elastic_type and action not in {"HARDTECH_LOW_OPEN_REPAIR", "HARDTECH_REPAIR_MOMENTUM"}:
            score = _score_elastic(out, cfg)
            q = {
                "LOW_COST_20CM_ELASTIC": "low_cost_20cm_elastic",
                "LOW_AMOUNT_VRATIO_ELASTIC": "low_amount_vratio_elastic",
                "LOW_COST_ELASTIC_CATCHUP": "low_cost_elastic",
            }[elastic_type]
            _add_tags(tags, q, "excess_return_primary")
            out.update(action_type=elastic_type, action_quality=q, signal_quality=q, action_reason=f"{q}_excess_return_primary", action_score=round(score, 2), action_confidence=v73._confidence(score, 58, 40), action_tags=tags)
            action = elastic_type

        # Generic broad repair remains visible but is no longer allowed to crowd
        # out the more precise hard-tech/elastic buckets.
        if _is_generic_broad_repair(out, cfg) and action not in v73.ACTIONABLE:
            score = _score_repair(out, cfg, hardtech=False)
            _add_tags(tags, "generic_broad_repair", "watch_only")
            out.update(action_type="GENERIC_BROAD_REPAIR", action_quality="watch_only", signal_quality="watch_only", action_reason="generic_broad_repair_watch_only_not_primary", action_score=round(score, 2), action_confidence=v73._confidence(score, 60, 40), action_tags=tags)
            action = "GENERIC_BROAD_REPAIR"

        # Soft avoid split: hard-tech candidates are monitored separately; power /
        # robot / high-cost unwind stays avoid-like after the 5/20 failure set.
        if action in {"SOFT_AVOID_REPAIR_CANDIDATE", "FAKE_STRENGTH_WATCH", "AVOID"}:
            pct = m["pct"]
            high_cost = pct >= float(cfg.get("soft_unwind_high_cost_pct", 5.0))
            if hardtech and not high_cost and -3.5 <= pct <= 4.0:
                _add_tags(tags, "hardtech_secondary_repair", "requires_intraday_confirmation")
                score = _score_repair(out, cfg, hardtech=True) - 8.0
                out.update(action_type="SECONDARY_REPAIR_CANDIDATE_HARDTECH", action_quality="watch_only", signal_quality="watch_only", action_reason="hardtech_secondary_repair_candidate_watch_only", action_score=round(v73._clamp(score), 2), action_tags=tags)
            elif retreat or high_cost:
                _add_tags(tags, "retreat_or_high_cost_unwind", "avoid_like")
                out.update(action_type="SOFT_AVOID_YESTERDAY_BOARD_UNWIND", action_quality="hard_avoid", signal_quality="hard_avoid", action_reason="retreat_or_high_cost_unwind_avoid_like", action_tags=tags)
            else:
                _add_tags(tags, "soft_avoid_retreat", "watch_only")
                out.update(action_type="SOFT_AVOID_RETREAT", action_quality="soft_avoid", signal_quality="soft_avoid", action_reason="soft_avoid_retreat_watch_only", action_tags=tags)

        out["expected_return_score"] = round(_expected_score(out, cfg), 2)
        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)
        return out

    def expected_sort_next(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (float(r.get("expected_return_score") or -999), float(r.get("action_score") or 0), float(r.get("final_score") or 0)), reverse=True)

    def pools_next(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = v73._sort_action(rows)
        expected = expected_sort_next(rows)
        specs = {
            "hardtech_low_open_repair_pool": lambda r: r.get("action_type") == "HARDTECH_LOW_OPEN_REPAIR",
            "hardtech_repair_momentum_pool": lambda r: r.get("action_type") == "HARDTECH_REPAIR_MOMENTUM",
            "low_cost_20cm_elastic_pool": lambda r: r.get("action_type") == "LOW_COST_20CM_ELASTIC",
            "low_cost_elastic_catchup_pool": lambda r: r.get("action_type") == "LOW_COST_ELASTIC_CATCHUP",
            "low_amount_vratio_elastic_pool": lambda r: r.get("action_type") == "LOW_AMOUNT_VRATIO_ELASTIC",
            "generic_low_open_repair_watch_pool": lambda r: r.get("action_type") == "GENERIC_LOW_OPEN_REPAIR",
            "generic_broad_repair_watch_pool": lambda r: r.get("action_type") == "GENERIC_BROAD_REPAIR",
            "secondary_hardtech_repair_watch_pool": lambda r: r.get("action_type") == "SECONDARY_REPAIR_CANDIDATE_HARDTECH",
            "power_low_open_repair_watch_pool": lambda r: r.get("action_type") == "POWER_LOW_OPEN_REPAIR",
            "soft_avoid_unwind_pool": lambda r: r.get("action_type") in {"SOFT_AVOID_RETREAT", "SOFT_AVOID_YESTERDAY_BOARD_UNWIND"},
        }
        for name, pred in specs.items():
            source = expected if "watch" not in name and "soft_avoid" not in name else ranked
            out[name] = [v73._compact(r) for r in source if pred(r)][:pool_max]
        return out

    def diagnostics_next(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        buckets: Dict[str, List[Dict[str, Any]]] = {
            "hardtech_repair_winners": [],
            "elastic_winners": [],
            "generic_repair_false_positives": [],
            "board_or_unwind_failures": [],
            "missed_hardtech_or_elastic_winners": [],
        }
        for r in rows:
            ex = _as_float(v73._perf(r).get("excess_return"), None)
            if ex is None:
                continue
            c = v73._compact(r)
            action = str(r.get("action_type"))
            if action in {"HARDTECH_LOW_OPEN_REPAIR", "HARDTECH_REPAIR_MOMENTUM"} and ex >= 5:
                c["diagnostic"] = "hardtech_repair_winner"; buckets["hardtech_repair_winners"].append(c)
            if action in {"LOW_COST_20CM_ELASTIC", "LOW_COST_ELASTIC_CATCHUP", "LOW_AMOUNT_VRATIO_ELASTIC"} and ex >= 5:
                c["diagnostic"] = "elastic_winner"; buckets["elastic_winners"].append(c)
            if action in {"GENERIC_LOW_OPEN_REPAIR", "GENERIC_BROAD_REPAIR", "THEME_CATCHUP_CONFIRMATION"} and ex <= -3:
                c["diagnostic"] = "generic_repair_false_positive"; buckets["generic_repair_false_positives"].append(c)
            if action in {"BOARD_WATCH", "HIGH_COST_REPAIR_WATCH", "POWER_LOW_OPEN_REPAIR", "SOFT_AVOID_YESTERDAY_BOARD_UNWIND"} and ex <= -3:
                c["diagnostic"] = "board_or_unwind_failure"; buckets["board_or_unwind_failures"].append(c)
            if action not in v73.ACTIONABLE and (_is_hardtech(r, {}) or _is_20cm(r) or _has_vratio(r)) and ex >= 8:
                c["diagnostic"] = "missed_hardtech_or_elastic_winner"; buckets["missed_hardtech_or_elastic_winners"].append(c)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        for name, rows_ in buckets.items():
            rows_.sort(key=key, reverse=("false" not in name and "failure" not in name))
            out[name] = rows_[:30]
        return out

    v73._upgrade_row = upgrade_row_next
    v73._sort_expected_return_proxy = expected_sort_next
    v73._pools = pools_next
    v73._diagnostics = diagnostics_next


apply()
