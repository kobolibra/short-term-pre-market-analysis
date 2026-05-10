"""Professional next-level v7.3 recall/ranking overlay.

This overlay is intentionally narrow and auditable:
- production rules use only premarket-visible fields;
- realized returns stay review-only;
- BROAD_REPAIR_MOMENTUM is a recall expansion for no-theme/no-source repair;
- HIGH_COST_REPAIR_WATCH is non-actionable, used to avoid hiding high-cost
  repair candidates inside hard avoid;
- expected-return ranking is display-only and now ranks broad repair explicitly.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import duanxianxia_v7_3_output as v73

_APPLIED = False


def _as_float(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL"):
            return default
        return float(v)
    except Exception:
        return default


def _tags(row: Dict[str, Any]) -> List[str]:
    return list(dict.fromkeys(row.get("action_tags") or []))


def _broad_repair_score(row: Dict[str, Any], cfg: Dict[str, Any], pct: Optional[float], auction: float, amt: float, src: float, theme: float) -> float:
    cost_fit = 0.0
    if pct is not None:
        if -2.0 <= pct <= 2.0:
            cost_fit = 18.0
        elif 2.0 < pct <= 5.0:
            cost_fit = 12.0
        elif -18.0 <= pct < -5.0:
            cost_fit = 10.0
    return v73._clamp(
        min(30.0, auction * 0.75)
        + min(22.0, amt / 5000.0 * 22.0)
        + cost_fit
        + min(8.0, max(src, theme * 0.05))
    )


def _is_broad_repair_candidate(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    if not bool(cfg.get("broad_repair_enabled", True)):
        return False
    if str(row.get("action_type")) != "DEBUG_ONLY":
        return False
    if str(row.get("setup_v72") or "none") != "none" or str(row.get("confidence") or "none") != "none":
        return False
    auction_type = str(row.get("auction_setup_type") or (row.get("auction_detail") or {}).get("auction_setup_type") or "")
    if auction_type in {"BOARD_LOCK_WATCH", "FAKE_STRENGTH"}:
        return False
    pct = v73._auction_pct(row)
    if pct is None or pct >= float(cfg.get("confirmation_high_cost_pct", 7.0)):
        return False
    theme = float(v73._metric(row, "theme_strength_t0", 0.0) or 0.0)
    src = float(v73._metric(row, "source_evidence_score", 0.0) or 0.0)
    fam = int(v73._metric(row, "source_family_count", 0) or 0)
    if theme > float(cfg.get("broad_repair_theme_max", 20)) or src > float(cfg.get("broad_repair_source_max", 1.0)) or fam > int(cfg.get("broad_repair_family_max", 1)):
        return False
    auction = float(v73._metric(row, "auction_strength", 0.0) or 0.0)
    amt = float(v73._metric(row, "auction_amount_wan", 0.0) or 0.0)
    normal = (
        float(cfg.get("broad_repair_pct_min", -2.0)) <= pct <= float(cfg.get("broad_repair_pct_max", 5.0))
        and auction >= float(cfg.get("broad_repair_min_auction_strength", 12))
        and amt >= float(cfg.get("broad_repair_min_amount_wan", 800))
    )
    deep = (
        float(cfg.get("broad_repair_deep_pct_min", -18.0)) <= pct <= float(cfg.get("broad_repair_deep_pct_max", -5.0))
        and auction >= float(cfg.get("broad_repair_deep_min_auction_strength", 8))
        and amt >= float(cfg.get("broad_repair_deep_min_amount_wan", 800))
    )
    if not (normal or deep):
        return False
    return _broad_repair_score(row, cfg, pct, auction, amt, src, theme) >= float(cfg.get("broad_repair_score_min", 24))


def _is_high_cost_repair_watch(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    if str(row.get("action_type")) != "AVOID":
        return False
    pct = v73._auction_pct(row)
    if pct is None:
        return False
    amt = float(v73._metric(row, "auction_amount_wan", 0.0) or 0.0)
    theme = float(v73._metric(row, "theme_strength_t0", 0.0) or 0.0)
    return (
        float(cfg.get("high_cost_repair_watch_pct_min", 7.0)) <= pct <= float(cfg.get("high_cost_repair_watch_pct_max", 10.5))
        and amt >= float(cfg.get("high_cost_repair_watch_min_amount_wan", 5000))
        and theme >= float(cfg.get("high_cost_repair_watch_min_theme", 80))
    )


def _expected_score(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality"))
    pct = v73._auction_pct(row)
    auction = float(v73._metric(row, "auction_strength", 0.0) or 0.0)
    amt = float(v73._metric(row, "auction_amount_wan", 0.0) or 0.0)
    src = float(v73._metric(row, "source_evidence_score", 0.0) or 0.0)
    theme = float(v73._metric(row, "theme_strength_t0", 0.0) or 0.0)

    pool_bonus = {
        "MOMENTUM_CATCHUP": float(cfg.get("expected_bonus_momentum", 30)),
        "LOW_OPEN_REVERSAL": float(cfg.get("expected_bonus_low_open_reversal", 27)),
        "BROAD_REPAIR_MOMENTUM": float(cfg.get("expected_bonus_broad_repair", 24)),
        "THEME_CATCHUP": float(cfg.get("expected_bonus_theme", 16)),
        "AUCTION_FOLLOW": float(cfg.get("expected_bonus_auction_follow", 13)),
        "CONFIRMATION_WATCH": 4.0,
        "FAKE_STRENGTH_WATCH": float(cfg.get("expected_bonus_fake_watch", 4)),
        "HIGH_COST_REPAIR_WATCH": float(cfg.get("expected_bonus_high_cost_repair_watch", -8)),
        "SOFT_AVOID_REPAIR_CANDIDATE": -10.0,
        "BOARD_WATCH": -8.0,
        "AVOID": -30.0,
        "DEBUG_ONLY": -70.0,
    }.get(action, -50.0)
    quality_bonus = {
        "momentum": 8.0,
        "repair": 7.0,
        "broad_repair": 7.0,
        "strong": 6.0,
        "medium": 3.0,
        "main_attack": 3.0,
        "weak": -1.0,
        "high_cost_watch": -10.0,
        "high_cost_repair_watch": -12.0,
        "soft_avoid": -12.0,
        "hard_avoid": -18.0,
    }.get(quality, 0.0)
    cost_penalty = max(0.0, float(pct or 0) - 5.5) * 2.0 if pct is not None else 0.0
    deep_repair_bonus = 4.0 if action == "BROAD_REPAIR_MOMENTUM" and pct is not None and pct < -5 else 0.0
    return (
        pool_bonus
        + quality_bonus
        + min(18.0, auction * 0.18)
        + min(14.0, amt / 5000.0 * 14.0)
        + min(8.0, src * 0.25)
        + min(5.0, theme * 0.03)
        + deep_repair_bonus
        - cost_penalty
    )


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({"BROAD_REPAIR_MOMENTUM": 32, "HIGH_COST_REPAIR_WATCH": 92})
    v73.ACTIONABLE.add("BROAD_REPAIR_MOMENTUM")
    v73.NON_ACTIONABLE_WATCH.add("HIGH_COST_REPAIR_WATCH")

    def upgrade_row_next(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        out = base_upgrade(row, cfg)
        pct = v73._auction_pct(out)
        auction = float(v73._metric(out, "auction_strength", 0.0) or 0.0)
        amt = float(v73._metric(out, "auction_amount_wan", 0.0) or 0.0)
        src = float(v73._metric(out, "source_evidence_score", 0.0) or 0.0)
        theme = float(v73._metric(out, "theme_strength_t0", 0.0) or 0.0)
        tags = _tags(out)

        if _is_broad_repair_candidate(out, cfg):
            for t in ["no_theme_no_source", "broad_repair_momentum"]:
                if t not in tags:
                    tags.append(t)
            if pct is not None and pct < -5 and "deep_repair" not in tags:
                tags.append("deep_repair")
            score = _broad_repair_score(out, cfg, pct, auction, amt, src, theme)
            out.update(action_type="BROAD_REPAIR_MOMENTUM", action_quality="broad_repair", signal_quality="broad_repair", action_reason="no_theme_no_source_broad_repair_momentum", action_score=round(score, 2), action_confidence=v73._confidence(score, 55, 35), action_tags=tags)
        elif _is_high_cost_repair_watch(out, cfg):
            for t in ["high_cost_fake_strength", "repair_watch_not_actionable"]:
                if t not in tags:
                    tags.append(t)
            score = v73._clamp(0.25 * auction + 0.25 * min(100.0, amt / 5000.0 * 100.0) + 0.15 * theme)
            out.update(action_type="HIGH_COST_REPAIR_WATCH", action_quality="high_cost_repair_watch", signal_quality="high_cost_repair_watch", action_reason="high_cost_fake_strength_repair_watch_only", action_score=round(score, 2), action_confidence=v73._confidence(score, 60, 40), action_tags=tags)
        out["expected_return_score"] = round(_expected_score(out, cfg), 2)
        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)
        return out

    def expected_sort_next(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (float(r.get("expected_return_score") or -999), float(r.get("action_score") or 0), float(r.get("final_score") or 0)), reverse=True)

    def pools_next(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = v73._sort_action(rows)
        out["broad_repair_momentum_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == "BROAD_REPAIR_MOMENTUM"][:pool_max]
        out["high_cost_repair_watch_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == "HIGH_COST_REPAIR_WATCH"][:pool_max]
        return out

    def diagnostics_next(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        broad_winners: List[Dict[str, Any]] = []
        broad_false: List[Dict[str, Any]] = []
        high_cost_watch_winners: List[Dict[str, Any]] = []
        for r in rows:
            ex = _as_float(v73._perf(r).get("excess_return"), None)
            if ex is None:
                continue
            c = v73._compact(r)
            if r.get("action_type") == "BROAD_REPAIR_MOMENTUM" and ex >= 8:
                c["diagnostic"] = "broad_repair_winner"; broad_winners.append(c)
            if r.get("action_type") == "BROAD_REPAIR_MOMENTUM" and ex <= -3:
                c["diagnostic"] = "broad_repair_false_positive"; broad_false.append(c)
            if r.get("action_type") == "HIGH_COST_REPAIR_WATCH" and ex >= 8:
                c["diagnostic"] = "high_cost_repair_watch_winner"; high_cost_watch_winners.append(c)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        broad_winners.sort(key=key, reverse=True); broad_false.sort(key=key); high_cost_watch_winners.sort(key=key, reverse=True)
        out["broad_repair_winners"] = broad_winners[:30]
        out["broad_repair_false_positives"] = broad_false[:30]
        out["high_cost_repair_watch_winners"] = high_cost_watch_winners[:30]
        return out

    v73._upgrade_row = upgrade_row_next
    v73._sort_expected_return_proxy = expected_sort_next
    v73._pools = pools_next
    v73._diagnostics = diagnostics_next


apply()
