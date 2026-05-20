"""v7.3 practical excess-return overlay.

This overlay is deliberately NOT a sector whitelist or a two-day fit.
It uses the recent reviews only to fix the decision framework:
- rank for post-auction excess return, not raw close_pct;
- prefer low auction-cost + real current-day evidence + liquidity;
- demote high-cost/board/retreat/watch-only rows from the primary buy list;
- let today's data decide the leading theme dynamically.

Production rules use only premarket-visible fields. Realized returns remain
review-only diagnostics.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import duanxianxia_v7_3_output as v73

_APPLIED = False

DEFAULT_BROAD_EVENT_THEMES = ["并购重组", "股权转让", "实控人变更", "一季报增长", "业绩增长", "业绩预增", "摘帽"]
DEFAULT_RETREAT_KEYWORDS = ["昨日连板", "连板", "高标", "断板", "退潮"]


def _f(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "-", "None", "null", "NULL"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _code(row: Dict[str, Any]) -> str:
    digits = "".join(ch for ch in str(row.get("code") or row.get("代码") or "") if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else digits


def _tags(row: Dict[str, Any]) -> List[str]:
    return list(dict.fromkeys(row.get("action_tags") or []))


def _add(tags: List[str], *items: str) -> List[str]:
    for item in items:
        if item and item not in tags:
            tags.append(item)
    return tags


def _cfg_list(cfg: Dict[str, Any], key: str, default: Iterable[str]) -> List[str]:
    value = cfg.get(key)
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    return [str(x) for x in default]


def _flatten(v: Any, depth: int = 0) -> List[str]:
    if v in (None, "", "-") or depth > 3:
        return []
    if isinstance(v, (str, int, float)):
        return [str(v)]
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            out.extend(_flatten(x, depth + 1))
        return out
    if isinstance(v, dict):
        out: List[str] = []
        for key in ("matched_plate", "matched_tags", "best_theme", "matched_themes", "theme", "themes", "concept", "concepts", "概念", "题材", "板块", "industry", "industry_name", "name", "名称", "action_reason", "setup_reason"):
            if key in v:
                out.extend(_flatten(v.get(key), depth + 1))
        return out
    return []


def _blob(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in ("name", "名称", "matched_themes", "concept", "concepts", "概念", "题材", "industry", "industry_name", "theme_detail", "signal_summary", "auction_detail", "action_tags", "action_reason", "setup_reason"):
        if key in row:
            parts.extend(_flatten(row.get(key)))
    return "|".join(parts)


def _has(row: Dict[str, Any], keywords: Iterable[str]) -> bool:
    text = _blob(row).lower()
    return any(str(k).lower() in text for k in keywords if str(k).strip())


def _is_20cm(row: Dict[str, Any]) -> bool:
    return _code(row).startswith(("300", "301", "688", "689", "8", "4"))


def _families(row: Dict[str, Any]) -> List[str]:
    detail = row.get("auction_detail") or {}
    fam = detail.get("source_families") or row.get("source_families") or []
    return [str(x) for x in fam] if isinstance(fam, list) else []


def _has_vratio(row: Dict[str, Any]) -> bool:
    detail = row.get("auction_detail") or {}
    return detail.get("vratio_rank") not in (None, "") or any("vratio" in str(x).lower() or "爆量" in str(x) for x in _families(row))


def _m(row: Dict[str, Any]) -> Dict[str, float]:
    pct = v73._auction_pct(row)
    return {
        "pct": float(pct if pct is not None else 0.0),
        "auction": float(v73._metric(row, "auction_strength", 0.0) or 0.0),
        "amount": float(v73._metric(row, "auction_amount_wan", 0.0) or 0.0),
        "liquidity": float(v73._metric(row, "liquidity_score", 50.0) or 50.0),
        "source": float(v73._metric(row, "source_evidence_score", 0.0) or 0.0),
        "family": float(v73._metric(row, "source_family_count", 0.0) or 0.0),
        "theme": float(v73._metric(row, "theme_strength_t0", 0.0) or 0.0),
    }


def _plate(row: Dict[str, Any]) -> str:
    t = row.get("theme_detail") or {}
    s = row.get("signal_summary") or {}
    return str(t.get("matched_plate") or s.get("matched_plate") or "")


def _broad_event(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    broad = set(_cfg_list(cfg, "broad_event_themes", DEFAULT_BROAD_EVENT_THEMES))
    plate = _plate(row)
    return bool(plate and plate in broad) or _has(row, broad)


def _retreat(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    return _has(row, _cfg_list(cfg, "retreat_keywords", DEFAULT_RETREAT_KEYWORDS))


def _theme_leader(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    """Dynamic current-day leadership. No sector is hard-coded."""
    m = _m(row)
    if m["theme"] < float(cfg.get("dynamic_theme_min_strength", 70)):
        return False
    if m["family"] < float(cfg.get("dynamic_theme_min_family_count", 1)) and m["source"] < float(cfg.get("dynamic_theme_min_source", 12)):
        return False
    if _broad_event(row, cfg) and m["source"] < float(cfg.get("broad_event_min_source", 20)):
        return False
    return True


def _low_cost(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    pct = _m(row)["pct"]
    return float(cfg.get("low_cost_pct_min", -3.5)) <= pct <= float(cfg.get("low_cost_pct_max", 4.5))


def _liq_ok(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    m = _m(row)
    return m["amount"] >= float(cfg.get("primary_min_amount_wan", 600)) and m["auction"] >= float(cfg.get("primary_min_auction_strength", 10))


def _repair_score(row: Dict[str, Any], cfg: Dict[str, Any], leader: bool, low_open: bool = False) -> float:
    m = _m(row); pct = m["pct"]
    if low_open:
        cost_fit = 20.0 if -5.0 <= pct < 0 else (10.0 if -8.0 <= pct < -5.0 else 0.0)
    else:
        cost_fit = 22.0 if -1.5 <= pct <= 2.5 else (16.0 if 2.5 < pct <= 4.5 else (8.0 if -4.0 <= pct < -1.5 else 0.0))
    return v73._clamp(
        cost_fit
        + (14.0 if leader else 0.0)
        + (6.0 if _is_20cm(row) else 0.0)
        + min(20.0, m["auction"] * 0.32)
        + min(16.0, m["amount"] / 5000.0 * 16.0)
        + min(12.0, m["source"] * 0.30)
        + min(8.0, m["family"] * 3.0)
    )


def _elastic_score(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    m = _m(row); pct = m["pct"]
    cost_fit = 24.0 if -1.5 <= pct <= 2.0 else (14.0 if -3.5 <= pct <= 4.0 else 0.0)
    return v73._clamp(
        cost_fit
        + (8.0 if _theme_leader(row, cfg) else 0.0)
        + (10.0 if _is_20cm(row) else 0.0)
        + (7.0 if _has_vratio(row) else 0.0)
        + min(18.0, m["auction"] * 0.28)
        + min(12.0, m["liquidity"] * 0.12)
        + min(14.0, m["amount"] / 3000.0 * 14.0)
        + min(10.0, m["source"] * 0.25)
    )


def _elastic_type(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    if not (_low_cost(row, cfg) and _liq_ok(row, cfg)):
        return None
    if _retreat(row, cfg) and not _theme_leader(row, cfg):
        return None
    if _elastic_score(row, cfg) < float(cfg.get("low_cost_elastic_score_min", 34)):
        return None
    if _is_20cm(row):
        return "LOW_COST_20CM_ELASTIC"
    if _has_vratio(row) and _m(row)["amount"] <= float(cfg.get("low_amount_vratio_max_amount_wan", 2500)):
        return "LOW_AMOUNT_VRATIO_ELASTIC"
    return "LOW_COST_ELASTIC_CATCHUP"


def _expected(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality"))
    m = _m(row); pct = m["pct"]
    pool = {
        "DYNAMIC_THEME_LOW_OPEN_REPAIR": 38,
        "DYNAMIC_THEME_REPAIR": 36,
        "LOW_COST_20CM_ELASTIC": 34,
        "LOW_COST_ELASTIC_CATCHUP": 30,
        "LOW_AMOUNT_VRATIO_ELASTIC": 28,
        "MOMENTUM_CATCHUP": 22,
        "AUCTION_FOLLOW": 16,
        "THEME_LEADERSHIP_CATCHUP": 14,
        "THEME_CATCHUP_CONFIRMATION": -2,
        "GENERIC_REPAIR_WATCH": -5,
        "DEEP_LOW_OPEN_WATCH": -8,
        "RETREAT_OR_HIGH_COST_WATCH": -20,
        "BOARD_WATCH": -24,
        "HIGH_COST_REPAIR_WATCH": -24,
        "FAKE_STRENGTH_WATCH": -12,
        "SOFT_AVOID_REPAIR_CANDIDATE": -16,
        "CONFIRMATION_WATCH": -8,
        "AVOID": -45,
        "DEBUG_ONLY": -80,
    }.get(action, -40)
    q = {
        "dynamic_theme_repair": 8,
        "dynamic_theme_low_open_repair": 8,
        "low_cost_elastic": 7,
        "low_cost_20cm_elastic": 8,
        "low_amount_vratio_elastic": 6,
        "momentum": 5,
        "main_attack": 2,
        "strong": 2,
        "medium": -1,
        "weak": -5,
        "watch_only": -8,
        "hard_avoid": -18,
    }.get(quality, 0)
    cost_penalty = max(0.0, pct - float(cfg.get("expected_cost_penalty_start_pct", 4.8))) * float(cfg.get("expected_cost_penalty_per_pct", 3.0))
    broad_penalty = 5.0 if _broad_event(row, cfg) and action not in {"DYNAMIC_THEME_REPAIR", "DYNAMIC_THEME_LOW_OPEN_REPAIR"} else 0.0
    retreat_penalty = 8.0 if _retreat(row, cfg) else 0.0
    return pool + q + min(16, m["auction"] * 0.16) + min(12, m["amount"] / 5000 * 12) + min(8, m["source"] * 0.20) + min(6, m["family"] * 2) - cost_penalty - broad_penalty - retreat_penalty


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({
        "DYNAMIC_THEME_LOW_OPEN_REPAIR": 8,
        "DYNAMIC_THEME_REPAIR": 10,
        "LOW_COST_20CM_ELASTIC": 12,
        "LOW_COST_ELASTIC_CATCHUP": 14,
        "LOW_AMOUNT_VRATIO_ELASTIC": 16,
        "AUCTION_FOLLOW": 24,
        "MOMENTUM_CATCHUP": 28,
        "THEME_LEADERSHIP_CATCHUP": 32,
        "THEME_CATCHUP_CONFIRMATION": 82,
        "GENERIC_REPAIR_WATCH": 84,
        "DEEP_LOW_OPEN_WATCH": 88,
        "RETREAT_OR_HIGH_COST_WATCH": 94,
        "BOARD_WATCH": 95,
        "HIGH_COST_REPAIR_WATCH": 96,
    })
    for a in ["DYNAMIC_THEME_LOW_OPEN_REPAIR", "DYNAMIC_THEME_REPAIR", "LOW_COST_20CM_ELASTIC", "LOW_COST_ELASTIC_CATCHUP", "LOW_AMOUNT_VRATIO_ELASTIC", "AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "THEME_LEADERSHIP_CATCHUP"]:
        v73.ACTIONABLE.add(a)
    for a in ["BOARD_WATCH", "THEME_CATCHUP", "LOW_OPEN_REVERSAL"]:
        v73.ACTIONABLE.discard(a)
    for a in ["THEME_CATCHUP_CONFIRMATION", "GENERIC_REPAIR_WATCH", "DEEP_LOW_OPEN_WATCH", "RETREAT_OR_HIGH_COST_WATCH", "HIGH_COST_REPAIR_WATCH"]:
        v73.NON_ACTIONABLE_WATCH.add(a)

    def upgrade(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        out = base_upgrade(row, cfg)
        tags = _tags(out)
        m = _m(out)
        action = str(out.get("action_type"))
        quality = str(out.get("signal_quality") or out.get("action_quality") or "")
        leader = _theme_leader(out, cfg)
        high_cost_or_retreat = _retreat(out, cfg) or m["pct"] >= float(cfg.get("retreat_high_cost_pct", 6.5))

        if action in {"BOARD_WATCH", "HIGH_COST_REPAIR_WATCH"} or (action in {"SOFT_AVOID_REPAIR_CANDIDATE", "FAKE_STRENGTH_WATCH", "AVOID"} and high_cost_or_retreat):
            _add(tags, "retreat_or_high_cost", "watch_only")
            out.update(action_type="RETREAT_OR_HIGH_COST_WATCH", action_quality="watch_only", signal_quality="watch_only", action_reason="retreat_or_high_cost_watch_only_for_excess_return", action_tags=tags)
            action = "RETREAT_OR_HIGH_COST_WATCH"

        if action == "THEME_CATCHUP":
            if leader and quality == "strong" and _low_cost(out, cfg) and m["amount"] >= float(cfg.get("theme_action_min_amount_wan", 1200)):
                score = _repair_score(out, cfg, leader=True)
                _add(tags, "dynamic_theme_leadership", "excess_return_primary")
                out.update(action_type="THEME_LEADERSHIP_CATCHUP", action_quality="dynamic_theme_repair", signal_quality="dynamic_theme_repair", action_reason="current_day_theme_leadership_low_cost_catchup", action_score=round(score, 2), action_confidence=v73._confidence(score, 58, 40), action_tags=tags)
            else:
                _add(tags, "theme_demoted_confirmation", "not_primary_without_dynamic_confirmation")
                score = max(0.0, float(out.get("action_score") or 0.0) - float(cfg.get("theme_confirmation_score_penalty", 12)))
                out.update(action_type="THEME_CATCHUP_CONFIRMATION", action_quality="watch_only", signal_quality="watch_only", action_reason="theme_catchup_demoted_needs_dynamic_confirmation", action_score=round(score, 2), action_tags=tags)
            action = str(out.get("action_type"))

        if action == "LOW_OPEN_REVERSAL":
            if leader and _liq_ok(out, cfg):
                score = _repair_score(out, cfg, leader=True, low_open=True)
                _add(tags, "dynamic_theme_leadership", "low_open_repair", "excess_return_primary")
                out.update(action_type="DYNAMIC_THEME_LOW_OPEN_REPAIR", action_quality="dynamic_theme_low_open_repair", signal_quality="dynamic_theme_low_open_repair", action_reason="current_day_leading_theme_low_open_repair", action_score=round(score, 2), action_confidence=v73._confidence(score, 58, 40), action_tags=tags)
            elif m["pct"] < float(cfg.get("deep_low_open_watch_pct", -5.0)):
                _add(tags, "deep_low_open", "watch_only")
                out.update(action_type="DEEP_LOW_OPEN_WATCH", action_quality="watch_only", signal_quality="watch_only", action_reason="deep_low_open_requires_intraday_reclaim", action_tags=tags)
            else:
                score = _repair_score(out, cfg, leader=False, low_open=True)
                _add(tags, "generic_repair", "watch_only")
                out.update(action_type="GENERIC_REPAIR_WATCH", action_quality="watch_only", signal_quality="watch_only", action_reason="generic_low_open_repair_watch_only", action_score=round(score, 2), action_tags=tags)
            action = str(out.get("action_type"))

        if leader and _low_cost(out, cfg) and _liq_ok(out, cfg) and action not in {"DYNAMIC_THEME_LOW_OPEN_REPAIR", "THEME_LEADERSHIP_CATCHUP"}:
            score = _repair_score(out, cfg, leader=True)
            if score >= float(cfg.get("dynamic_repair_score_min", 36)):
                _add(tags, "dynamic_theme_leadership", "repair_momentum", "excess_return_primary")
                out.update(action_type="DYNAMIC_THEME_REPAIR", action_quality="dynamic_theme_repair", signal_quality="dynamic_theme_repair", action_reason="current_day_leading_theme_repair_momentum", action_score=round(score, 2), action_confidence=v73._confidence(score, 60, 42), action_tags=tags)
                action = "DYNAMIC_THEME_REPAIR"

        etype = _elastic_type(out, cfg)
        if etype and action not in {"DYNAMIC_THEME_LOW_OPEN_REPAIR", "DYNAMIC_THEME_REPAIR", "THEME_LEADERSHIP_CATCHUP"}:
            score = _elastic_score(out, cfg)
            q = {"LOW_COST_20CM_ELASTIC": "low_cost_20cm_elastic", "LOW_AMOUNT_VRATIO_ELASTIC": "low_amount_vratio_elastic", "LOW_COST_ELASTIC_CATCHUP": "low_cost_elastic"}[etype]
            _add(tags, q, "excess_return_primary")
            out.update(action_type=etype, action_quality=q, signal_quality=q, action_reason=f"{q}_excess_return_primary", action_score=round(score, 2), action_confidence=v73._confidence(score, 58, 40), action_tags=tags)
            action = etype

        if action in {"BROAD_REPAIR_MOMENTUM", "DEBUG_ONLY", "CONFIRMATION_WATCH"} and _low_cost(out, cfg) and _liq_ok(out, cfg) and not leader:
            score = _repair_score(out, cfg, leader=False)
            if score >= float(cfg.get("generic_repair_score_min", 38)):
                _add(tags, "generic_repair", "watch_only")
                out.update(action_type="GENERIC_REPAIR_WATCH", action_quality="watch_only", signal_quality="watch_only", action_reason="generic_repair_watch_only_not_primary", action_score=round(score, 2), action_tags=tags)

        out["expected_return_score"] = round(_expected(out, cfg), 2)
        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)
        return out

    def expected_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (float(r.get("expected_return_score") or -999), float(r.get("action_score") or 0), float(r.get("final_score") or 0)), reverse=True)

    def pools(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = v73._sort_action(rows)
        expected = expected_sort(rows)
        specs = {
            "dynamic_theme_low_open_repair_pool": lambda r: r.get("action_type") == "DYNAMIC_THEME_LOW_OPEN_REPAIR",
            "dynamic_theme_repair_pool": lambda r: r.get("action_type") == "DYNAMIC_THEME_REPAIR",
            "theme_leadership_catchup_pool": lambda r: r.get("action_type") == "THEME_LEADERSHIP_CATCHUP",
            "low_cost_20cm_elastic_pool": lambda r: r.get("action_type") == "LOW_COST_20CM_ELASTIC",
            "low_cost_elastic_catchup_pool": lambda r: r.get("action_type") == "LOW_COST_ELASTIC_CATCHUP",
            "low_amount_vratio_elastic_pool": lambda r: r.get("action_type") == "LOW_AMOUNT_VRATIO_ELASTIC",
            "generic_repair_watch_pool": lambda r: r.get("action_type") == "GENERIC_REPAIR_WATCH",
            "deep_low_open_watch_pool": lambda r: r.get("action_type") == "DEEP_LOW_OPEN_WATCH",
            "retreat_or_high_cost_watch_pool": lambda r: r.get("action_type") == "RETREAT_OR_HIGH_COST_WATCH",
        }
        for name, pred in specs.items():
            source = expected if "watch" not in name else ranked
            out[name] = [v73._compact(r) for r in source if pred(r)][:pool_max]
        return out

    def diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        buckets: Dict[str, List[Dict[str, Any]]] = {"dynamic_theme_winners": [], "elastic_winners": [], "watch_false_positives": [], "missed_dynamic_winners": []}
        for r in rows:
            ex = _f(v73._perf(r).get("excess_return"), None)
            if ex is None:
                continue
            c = v73._compact(r); action = str(r.get("action_type"))
            if action in {"DYNAMIC_THEME_LOW_OPEN_REPAIR", "DYNAMIC_THEME_REPAIR", "THEME_LEADERSHIP_CATCHUP"} and ex >= 5:
                c["diagnostic"] = "dynamic_theme_winner"; buckets["dynamic_theme_winners"].append(c)
            if action in {"LOW_COST_20CM_ELASTIC", "LOW_COST_ELASTIC_CATCHUP", "LOW_AMOUNT_VRATIO_ELASTIC"} and ex >= 5:
                c["diagnostic"] = "elastic_winner"; buckets["elastic_winners"].append(c)
            if action in {"GENERIC_REPAIR_WATCH", "DEEP_LOW_OPEN_WATCH", "RETREAT_OR_HIGH_COST_WATCH", "THEME_CATCHUP_CONFIRMATION"} and ex <= -3:
                c["diagnostic"] = "watch_false_positive"; buckets["watch_false_positives"].append(c)
            if action not in v73.ACTIONABLE and (_theme_leader(r, {}) or _is_20cm(r) or _has_vratio(r)) and ex >= 8:
                c["diagnostic"] = "missed_dynamic_or_elastic_winner"; buckets["missed_dynamic_winners"].append(c)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        for name, items in buckets.items():
            items.sort(key=key, reverse=("false" not in name))
            out[name] = items[:30]
        return out

    v73._upgrade_row = upgrade
    v73._sort_expected_return_proxy = expected_sort
    v73._pools = pools
    v73._diagnostics = diagnostics


apply()
