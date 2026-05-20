"""v7.3 evidence-balanced excess-return overlay.

This is not a rollback-only patch and not a two-day sector fit.

Lessons from 2026-05-19 and 2026-05-20:
- Action priority order is not a good buy-list order.  On both reviewed days,
  the expected-return proxy order was better than raw action order.
- Low-cost / 20cm alone is not alpha; the failed 183925 run over-promoted this.
- Weak/medium theme catch-up is noisy and should not crowd the primary list.
- Broad repair / low-open repair / momentum can work, but only as evidence-
  balanced pools with cost and liquidity guardrails.
- High-cost, board-lock, fake-strength and retreat-like rows must stay visible
  for watch/review, but should not dominate the executable top list.

Production uses only premarket-visible fields.  Realized returns are diagnostics
only.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

import duanxianxia_v7_3_output as v73

_APPLIED = False

BROAD_EVENT_THEMES = {"并购重组", "股权转让", "实控人变更", "一季报增长", "业绩增长", "业绩预增", "年报增长", "半年报增长", "摘帽"}
RETREAT_WORDS = {"昨日连板", "连板", "高标", "断板", "退潮"}


def _as_float(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _tags(row: Dict[str, Any]) -> List[str]:
    return list(dict.fromkeys(row.get("action_tags") or []))


def _add_tags(tags: List[str], *items: str) -> List[str]:
    for item in items:
        if item and item not in tags:
            tags.append(item)
    return tags


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
        for k in ("matched_plate", "matched_tags", "best_theme", "matched_themes", "theme", "themes", "concept", "concepts", "概念", "题材", "板块", "industry", "industry_name", "name", "名称", "action_reason", "setup_reason"):
            if k in v:
                out.extend(_flatten(v.get(k), depth + 1))
        return out
    return []


def _blob(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key in ("name", "名称", "matched_themes", "concept", "concepts", "概念", "题材", "industry", "industry_name", "theme_detail", "signal_summary", "auction_detail", "action_tags", "action_reason", "setup_reason"):
        if key in row:
            parts.extend(_flatten(row.get(key)))
    return "|".join(parts)


def _has_any(row: Dict[str, Any], words: Iterable[str]) -> bool:
    text = _blob(row).lower()
    return any(str(w).lower() in text for w in words if str(w).strip())


def _metrics(row: Dict[str, Any]) -> Dict[str, float]:
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


def _is_broad_event(row: Dict[str, Any]) -> bool:
    return _has_any(row, BROAD_EVENT_THEMES)


def _is_retreat_like(row: Dict[str, Any]) -> bool:
    return _has_any(row, RETREAT_WORDS)


def _broad_repair_score(row: Dict[str, Any], cfg: Dict[str, Any], pct: Optional[float], auction: float, amt: float, src: float, theme: float) -> float:
    if pct is None:
        cost_fit = 0.0
    elif -2.0 <= pct <= 2.5:
        cost_fit = 20.0
    elif 2.5 < pct <= 5.0:
        cost_fit = 11.0
    elif -18.0 <= pct < -5.0:
        cost_fit = 8.0
    else:
        cost_fit = 0.0
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


def _quality_penalty(action: str, quality: str) -> float:
    if action == "THEME_CATCHUP" and quality == "weak":
        return 12.0
    if action == "THEME_CATCHUP" and quality == "medium":
        return 5.0
    if action == "LOW_OPEN_REVERSAL" and quality != "repair":
        return 5.0
    return 0.0


def _expected_score(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality"))
    m = _metrics(row)
    pct = m["pct"]
    pool_bonus = {
        "MOMENTUM_CATCHUP": float(cfg.get("expected_bonus_momentum", 30)),
        "LOW_OPEN_REVERSAL": float(cfg.get("expected_bonus_low_open_reversal", 27)),
        "BROAD_REPAIR_MOMENTUM": float(cfg.get("expected_bonus_broad_repair", 24)),
        "THEME_CATCHUP": float(cfg.get("expected_bonus_theme", 16)),
        "AUCTION_FOLLOW": float(cfg.get("expected_bonus_auction_follow", 13)),
        "CONFIRMATION_WATCH": 4.0,
        "FAKE_STRENGTH_WATCH": float(cfg.get("expected_bonus_fake_watch", 4)),
        "HIGH_COST_REPAIR_WATCH": float(cfg.get("expected_bonus_high_cost_repair_watch", -8)),
        "SOFT_AVOID_REPAIR_CANDIDATE": float(cfg.get("expected_bonus_soft_avoid", -6)),
        "BOARD_WATCH": -10.0,
        "AVOID": -30.0,
        "DEBUG_ONLY": -70.0,
    }.get(action, -50.0)
    quality_bonus = {
        "momentum": 8.0,
        "repair": 7.0,
        "broad_repair": 7.0,
        "strong": 6.0,
        "medium": 1.0,
        "main_attack": 3.0,
        "weak": -5.0,
        "high_cost_watch": -10.0,
        "high_cost_repair_watch": -12.0,
        "soft_avoid": -6.0,
        "hard_avoid": -18.0,
    }.get(quality, 0.0)
    cost_penalty = max(0.0, pct - float(cfg.get("expected_cost_penalty_start_pct", 5.5))) * float(cfg.get("expected_cost_penalty_per_pct", 2.0))
    deep_repair_bonus = 4.0 if action == "BROAD_REPAIR_MOMENTUM" and pct < -5 else 0.0
    low_cost_bonus = 3.0 if -2.0 <= pct <= 2.5 and action in {"BROAD_REPAIR_MOMENTUM", "LOW_OPEN_REVERSAL", "THEME_CATCHUP"} else 0.0
    broad_event_penalty = 4.0 if _is_broad_event(row) and action == "THEME_CATCHUP" and m["source"] < 18 else 0.0
    retreat_penalty = 6.0 if _is_retreat_like(row) else 0.0
    return (
        pool_bonus
        + quality_bonus
        + min(18.0, m["auction"] * 0.18)
        + min(14.0, m["amount"] / 5000.0 * 14.0)
        + min(8.0, m["source"] * 0.25)
        + min(5.0, m["theme"] * 0.03)
        + deep_repair_bonus
        + low_cost_bonus
        - cost_penalty
        - _quality_penalty(action, quality)
        - broad_event_penalty
        - retreat_penalty
    )


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    # Keep actionable families broad, but rank them by expected-return evidence.
    v73.ACTION_PRIORITY.update({"BROAD_REPAIR_MOMENTUM": 32, "HIGH_COST_REPAIR_WATCH": 92})
    v73.ACTIONABLE.update({"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "BROAD_REPAIR_MOMENTUM", "THEME_CATCHUP"})
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
            _add_tags(tags, "no_theme_no_source", "broad_repair_momentum")
            if pct is not None and pct < -5:
                _add_tags(tags, "deep_repair")
            score = _broad_repair_score(out, cfg, pct, auction, amt, src, theme)
            out.update(action_type="BROAD_REPAIR_MOMENTUM", action_quality="broad_repair", signal_quality="broad_repair", action_reason="no_theme_no_source_broad_repair_momentum", action_score=round(score, 2), action_confidence=v73._confidence(score, 55, 35), action_tags=tags)
        elif _is_high_cost_repair_watch(out, cfg):
            _add_tags(tags, "high_cost_fake_strength", "repair_watch_not_actionable")
            score = v73._clamp(0.25 * auction + 0.25 * min(100.0, amt / 5000.0 * 100.0) + 0.15 * theme)
            out.update(action_type="HIGH_COST_REPAIR_WATCH", action_quality="high_cost_repair_watch", signal_quality="high_cost_repair_watch", action_reason="high_cost_fake_strength_repair_watch_only", action_score=round(score, 2), action_confidence=v73._confidence(score, 60, 40), action_tags=tags)

        # Do not remove weak theme rows from diagnostics, but make their ranking
        # honestly reflect their two-day weakness.
        if out.get("action_type") == "THEME_CATCHUP" and str(out.get("signal_quality") or out.get("action_quality")) == "weak":
            _add_tags(tags, "weak_theme_needs_intraday_confirmation")
            out["action_tags"] = tags

        out["expected_return_score"] = round(_expected_score(out, cfg), 2)
        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)
        return out

    def expected_sort_next(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (float(r.get("expected_return_score") or -999), float(r.get("action_score") or 0), float(r.get("final_score") or 0)), reverse=True)

    def action_sort_next(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Main practical change: executable Top30 follows evidence-balanced
        # expected-return ranking, while watch/non-actionable rows stay behind.
        return sorted(
            rows,
            key=lambda r: (
                0 if r.get("action_type") in v73.ACTIONABLE else 1,
                -float(r.get("expected_return_score") or -999),
                int(r.get("action_priority") or 999),
                -float(r.get("action_score") or 0),
                -float(r.get("final_score") or 0),
            ),
        )

    def pools_next(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = action_sort_next(rows)
        expected = expected_sort_next(rows)
        out["broad_repair_momentum_pool"] = [v73._compact(r) for r in expected if r.get("action_type") == "BROAD_REPAIR_MOMENTUM"][:pool_max]
        out["high_cost_repair_watch_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == "HIGH_COST_REPAIR_WATCH"][:pool_max]
        out["expected_primary_pool"] = [v73._compact(r) for r in expected if r.get("action_type") in v73.ACTIONABLE][:pool_max]
        return out

    def diagnostics_next(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        broad_winners: List[Dict[str, Any]] = []
        broad_false: List[Dict[str, Any]] = []
        weak_theme_false: List[Dict[str, Any]] = []
        soft_avoid_winners: List[Dict[str, Any]] = []
        for r in rows:
            ex = _as_float(v73._perf(r).get("excess_return"), None)
            if ex is None:
                continue
            c = v73._compact(r)
            action = r.get("action_type")
            quality = r.get("signal_quality") or r.get("action_quality")
            if action == "BROAD_REPAIR_MOMENTUM" and ex >= 8:
                c["diagnostic"] = "broad_repair_winner"; broad_winners.append(c)
            if action == "BROAD_REPAIR_MOMENTUM" and ex <= -3:
                c["diagnostic"] = "broad_repair_false_positive"; broad_false.append(c)
            if action == "THEME_CATCHUP" and quality == "weak" and ex <= -3:
                c["diagnostic"] = "weak_theme_false_positive"; weak_theme_false.append(c)
            if action == "SOFT_AVOID_REPAIR_CANDIDATE" and ex >= 8:
                c["diagnostic"] = "soft_avoid_winner_review_only"; soft_avoid_winners.append(c)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        broad_winners.sort(key=key, reverse=True); broad_false.sort(key=key); weak_theme_false.sort(key=key); soft_avoid_winners.sort(key=key, reverse=True)
        out["broad_repair_winners"] = broad_winners[:30]
        out["broad_repair_false_positives"] = broad_false[:30]
        out["weak_theme_false_positives"] = weak_theme_false[:30]
        out["soft_avoid_winners_review_only"] = soft_avoid_winners[:30]
        return out

    v73._upgrade_row = upgrade_row_next
    v73._sort_expected_return_proxy = expected_sort_next
    v73._sort_action = action_sort_next
    v73._pools = pools_next
    v73._diagnostics = diagnostics_next


apply()
