"""Next-level v7.3 recall/risk overlay.

This module monkey-patches duanxianxia_v7_3_output at import time so the
existing runner/backfill/bundle tools can keep their public API while gaining
new production rules.

Production rules still use only premarket-visible fields.  Realized
close/excess returns remain review-only.
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
    setup = str(row.get("setup_v72") or "none")
    conf = str(row.get("confidence") or "none")
    if setup != "none" or conf != "none":
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
    if theme > float(cfg.get("broad_repair_theme_max", 20)):
        return False
    if src > float(cfg.get("broad_repair_source_max", 1.0)):
        return False
    if fam > int(cfg.get("broad_repair_family_max", 1)):
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
    score = _broad_repair_score(row, cfg, pct, auction, amt, src, theme)
    return score >= float(cfg.get("broad_repair_score_min", 24))


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


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({
        "BROAD_REPAIR_MOMENTUM": 32,
        "HIGH_COST_REPAIR_WATCH": 92,
    })
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
            if "no_theme_no_source" not in tags:
                tags.append("no_theme_no_source")
            if "broad_repair_momentum" not in tags:
                tags.append("broad_repair_momentum")
            if pct is not None and pct < -5 and "deep_repair" not in tags:
                tags.append("deep_repair")
            score = _broad_repair_score(out, cfg, pct, auction, amt, src, theme)
            out.update(
                action_type="BROAD_REPAIR_MOMENTUM",
                action_quality="broad_repair",
                signal_quality="broad_repair",
                action_reason="no_theme_no_source_broad_repair_momentum",
                action_score=round(score, 2),
                action_confidence=v73._confidence(score, 55, 35),
                action_tags=tags,
            )

        elif _is_high_cost_repair_watch(out, cfg):
            if "high_cost_fake_strength" not in tags:
                tags.append("high_cost_fake_strength")
            if "repair_watch_not_actionable" not in tags:
                tags.append("repair_watch_not_actionable")
            score = v73._clamp(0.25 * auction + 0.25 * min(100.0, amt / 5000.0 * 100.0) + 0.15 * theme)
            out.update(
                action_type="HIGH_COST_REPAIR_WATCH",
                action_quality="high_cost_repair_watch",
                signal_quality="high_cost_repair_watch",
                action_reason="high_cost_fake_strength_repair_watch_only",
                action_score=round(score, 2),
                action_confidence=v73._confidence(score, 60, 40),
                action_tags=tags,
            )

        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)
        return out

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
        broad_winners.sort(key=key, reverse=True)
        broad_false.sort(key=key)
        high_cost_watch_winners.sort(key=key, reverse=True)
        out["broad_repair_winners"] = broad_winners[:30]
        out["broad_repair_false_positives"] = broad_false[:30]
        out["high_cost_repair_watch_winners"] = high_cost_watch_winners[:30]
        return out

    v73._upgrade_row = upgrade_row_next
    v73._pools = pools_next
    v73._diagnostics = diagnostics_next


apply()
