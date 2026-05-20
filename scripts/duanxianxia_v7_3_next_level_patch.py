"""v7.3 selective high-conviction premarket overlay.

Goal: fewer, cleaner, higher-quality candidates.

This replaces the previous decorative/over-expanded overlays.  The report should
not pretend that a few auction tables can justify a long, complicated buy list.
Production selection is now deliberately conservative:

- no sector hard-code;
- no low-cost/20cm standalone alpha;
- no weak theme catch-up in the buy list;
- no board/high-cost/fake-strength rows in the buy list;
- actionable candidates must pass explicit cost, liquidity, source/amount and
  action-quality gates;
- if the evidence is not strong enough, the row stays in watch/review pools.

Realized returns are only used by diagnostics, never by production rules.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import duanxianxia_v7_3_output as v73

_APPLIED = False


WATCH_ACTION = "QUALITY_WATCH"


def _num(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _tags(row: Dict[str, Any]) -> List[str]:
    return list(dict.fromkeys(row.get("action_tags") or []))


def _add(tags: List[str], *items: str) -> List[str]:
    for item in items:
        if item and item not in tags:
            tags.append(item)
    return tags


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


def _entry_tag(row: Dict[str, Any]) -> str:
    return str(row.get("entry_tag") or (row.get("auction_detail") or {}).get("entry_tag") or "normal")


def _auction_type(row: Dict[str, Any]) -> str:
    return str(row.get("auction_setup_type") or (row.get("auction_detail") or {}).get("auction_setup_type") or "")


def _rank_present(row: Dict[str, Any], key: str) -> bool:
    detail = row.get("auction_detail") or {}
    return detail.get(key) not in (None, "", 0, "0")


def _has_any_source(row: Dict[str, Any]) -> bool:
    return (
        _rank_present(row, "qiangchou_920_925_rank")
        or _rank_present(row, "qiangchou_last_second_rank")
        or _rank_present(row, "vratio_rank")
        or _rank_present(row, "net_amount_rank")
        or _rank_present(row, "fengdan_rank")
        or _m(row)["source"] >= 8
        or _m(row)["family"] >= 1
    )


def _broad_repair_score(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    m = _m(row)
    pct = m["pct"]
    if -2.0 <= pct <= 2.5:
        cost_fit = 20.0
    elif 2.5 < pct <= 5.0:
        cost_fit = 10.0
    elif -10.0 <= pct < -5.0:
        cost_fit = 6.0
    else:
        cost_fit = 0.0
    return v73._clamp(
        cost_fit
        + min(30.0, m["auction"] * 0.70)
        + min(22.0, m["amount"] / 5000.0 * 22.0)
        + min(8.0, max(m["source"], m["theme"] * 0.05))
    )


def _is_broad_repair_candidate(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    if not bool(cfg.get("broad_repair_enabled", True)):
        return False
    if str(row.get("action_type")) != "DEBUG_ONLY":
        return False
    if str(row.get("setup_v72") or "none") != "none" or str(row.get("confidence") or "none") != "none":
        return False
    if _auction_type(row) in {"BOARD_LOCK_WATCH", "FAKE_STRENGTH"}:
        return False
    m = _m(row)
    if not (float(cfg.get("broad_repair_pct_min", -2.0)) <= m["pct"] <= float(cfg.get("broad_repair_pct_max", 5.0))):
        return False
    if m["pct"] >= float(cfg.get("confirmation_high_cost_pct", 7.0)):
        return False
    if m["theme"] > float(cfg.get("broad_repair_theme_max", 20)):
        return False
    if m["source"] > float(cfg.get("broad_repair_source_max", 1.0)) or m["family"] > float(cfg.get("broad_repair_family_max", 1)):
        return False
    if m["auction"] < float(cfg.get("broad_repair_min_auction_strength", 12)):
        return False
    if m["amount"] < float(cfg.get("broad_repair_min_amount_wan", 800)):
        return False
    return _broad_repair_score(row, cfg) >= float(cfg.get("broad_repair_score_min", 24))


def _is_high_cost_repair_watch(row: Dict[str, Any], cfg: Dict[str, Any]) -> bool:
    if str(row.get("action_type")) != "AVOID":
        return False
    m = _m(row)
    return (
        float(cfg.get("high_cost_repair_watch_pct_min", 7.0)) <= m["pct"] <= float(cfg.get("high_cost_repair_watch_pct_max", 10.5))
        and m["amount"] >= float(cfg.get("high_cost_repair_watch_min_amount_wan", 5000))
        and m["theme"] >= float(cfg.get("high_cost_repair_watch_min_theme", 80))
    )


def _expected_score(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    m = _m(row)
    pct = m["pct"]
    pool_bonus = {
        "MOMENTUM_CATCHUP": 30.0,
        "LOW_OPEN_REVERSAL": 28.0,
        "BROAD_REPAIR_MOMENTUM": 26.0,
        "AUCTION_FOLLOW": 18.0,
        "THEME_CATCHUP": 14.0,
        "SOFT_AVOID_REPAIR_CANDIDATE": -8.0,
        "FAKE_STRENGTH_WATCH": -12.0,
        "BOARD_WATCH": -25.0,
        "HIGH_COST_REPAIR_WATCH": -25.0,
        "AVOID": -40.0,
        WATCH_ACTION: -35.0,
        "DEBUG_ONLY": -80.0,
    }.get(action, -50.0)
    quality_bonus = {
        "momentum": 8.0,
        "repair": 8.0,
        "broad_repair": 7.0,
        "main_attack": 4.0,
        "strong": 5.0,
        "medium": -2.0,
        "weak": -10.0,
        "soft_avoid": -6.0,
        "watch_only": -12.0,
        "hard_avoid": -18.0,
    }.get(quality, 0.0)
    cost_penalty = max(0.0, pct - float(cfg.get("expected_cost_penalty_start_pct", 5.0))) * float(cfg.get("expected_cost_penalty_per_pct", 3.0))
    amount_bonus = min(14.0, m["amount"] / 5000.0 * 14.0)
    evidence_bonus = min(10.0, m["source"] * 0.25 + m["family"] * 2.0)
    auction_bonus = min(18.0, m["auction"] * 0.18)
    low_cost_bonus = 4.0 if -2.0 <= pct <= 2.5 and action in {"BROAD_REPAIR_MOMENTUM", "LOW_OPEN_REVERSAL", "THEME_CATCHUP"} else 0.0
    return pool_bonus + quality_bonus + auction_bonus + amount_bonus + evidence_bonus + low_cost_bonus - cost_penalty


def _gate_reason(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    """Return None if row is high-conviction actionable; otherwise reason."""
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    m = _m(row)
    expected = float(row.get("expected_return_score") or _expected_score(row, cfg))

    if action not in {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "BROAD_REPAIR_MOMENTUM", "THEME_CATCHUP"}:
        return "not_primary_action"
    if _entry_tag(row) == "avoid" or _auction_type(row) in {"FAKE_STRENGTH", "BOARD_LOCK_WATCH"}:
        return "avoid_fake_or_board_lock"
    if m["pct"] >= float(cfg.get("buy_max_auction_pct", 6.5)):
        return "auction_cost_too_high_for_excess_return"
    if m["amount"] < float(cfg.get("buy_min_amount_wan", 900)):
        return "auction_amount_too_small"
    if m["liquidity"] < float(cfg.get("buy_min_liquidity_score", 35)):
        return "liquidity_too_weak"
    if expected < float(cfg.get("buy_min_expected_score", 45)):
        return "expected_score_below_buy_bar"

    if action == "AUCTION_FOLLOW":
        if not (2.0 <= m["pct"] <= 6.5):
            return "auction_follow_cost_window_fail"
        if m["auction"] < 50 or not _has_any_source(row):
            return "auction_follow_evidence_fail"
        return None

    if action == "MOMENTUM_CATCHUP":
        if not (1.5 <= m["pct"] <= 5.8):
            return "momentum_cost_window_fail"
        if m["auction"] < 50 or m["amount"] < 1000 or m["liquidity"] < 55:
            return "momentum_evidence_fail"
        return None

    if action == "LOW_OPEN_REVERSAL":
        if m["pct"] >= 0:
            return "not_low_open"
        if m["pct"] < -10.0:
            return "deep_low_open_too_risky"
        if m["amount"] < 3000 or m["auction"] < 25:
            return "low_open_support_not_enough"
        if m["pct"] < -5.0 and float(row.get("action_score") or 0) < 55:
            return "deep_low_open_needs_stronger_score"
        return None

    if action == "BROAD_REPAIR_MOMENTUM":
        if not (-2.0 <= m["pct"] <= 4.8):
            return "broad_repair_cost_window_fail"
        if float(row.get("action_score") or 0) < 35:
            return "broad_repair_score_too_low"
        if m["auction"] < 15 or m["amount"] < 1000:
            return "broad_repair_support_not_enough"
        return None

    if action == "THEME_CATCHUP":
        if quality != "strong":
            return "theme_not_strong_quality"
        if not (-0.5 <= m["pct"] <= 2.2):
            return "theme_cost_window_fail"
        if m["amount"] < 1500:
            return "theme_amount_too_small"
        return None

    return "unhandled"


def _downgrade_to_watch(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    out = dict(row)
    tags = _tags(out)
    _add(tags, "quality_gate_failed", reason)
    out.update(
        action_type=WATCH_ACTION,
        action_quality="watch_only",
        signal_quality="watch_only",
        action_reason=f"quality_gate_failed:{reason}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(WATCH_ACTION, 900),
    )
    return out


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({"BROAD_REPAIR_MOMENTUM": 32, WATCH_ACTION: 900, "HIGH_COST_REPAIR_WATCH": 920})
    v73.ACTIONABLE.clear()
    v73.ACTIONABLE.update({"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "BROAD_REPAIR_MOMENTUM", "THEME_CATCHUP"})
    v73.NON_ACTIONABLE_WATCH.update({WATCH_ACTION, "HIGH_COST_REPAIR_WATCH", "BOARD_WATCH", "FAKE_STRENGTH_WATCH", "SOFT_AVOID_REPAIR_CANDIDATE"})

    def upgrade_row(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        out = base_upgrade(row, cfg)
        tags = _tags(out)

        if _is_broad_repair_candidate(out, cfg):
            _add(tags, "no_theme_no_source", "broad_repair_momentum")
            score = _broad_repair_score(out, cfg)
            out.update(action_type="BROAD_REPAIR_MOMENTUM", action_quality="broad_repair", signal_quality="broad_repair", action_reason="no_theme_no_source_broad_repair_momentum", action_score=round(score, 2), action_confidence=v73._confidence(score, 55, 35), action_tags=tags)
        elif _is_high_cost_repair_watch(out, cfg):
            _add(tags, "high_cost_fake_strength", "repair_watch_not_actionable")
            m = _m(out)
            score = v73._clamp(0.25 * m["auction"] + 0.25 * min(100.0, m["amount"] / 5000.0 * 100.0) + 0.15 * m["theme"])
            out.update(action_type="HIGH_COST_REPAIR_WATCH", action_quality="watch_only", signal_quality="watch_only", action_reason="high_cost_repair_watch_only", action_score=round(score, 2), action_confidence=v73._confidence(score, 60, 40), action_tags=tags)

        out["expected_return_score"] = round(_expected_score(out, cfg), 2)
        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)

        reason = _gate_reason(out, cfg)
        if reason is not None and str(out.get("action_type")) in v73.ACTIONABLE:
            out = _downgrade_to_watch(out, reason)
            out["expected_return_score"] = round(_expected_score(out, cfg), 2)
        return out

    def expected_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (float(r.get("expected_return_score") or -999), float(r.get("action_score") or 0), float(r.get("final_score") or 0)), reverse=True)

    def action_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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

    def pools(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = action_sort(rows)
        expected = expected_sort(rows)
        out["selective_buy_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") in v73.ACTIONABLE][:pool_max]
        out["broad_repair_momentum_pool"] = [v73._compact(r) for r in expected if r.get("action_type") == "BROAD_REPAIR_MOMENTUM"][:pool_max]
        out["quality_watch_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == WATCH_ACTION][:pool_max]
        out["high_cost_repair_watch_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == "HIGH_COST_REPAIR_WATCH"][:pool_max]
        return out

    def diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        gate_reject_winners: List[Dict[str, Any]] = []
        buy_false: List[Dict[str, Any]] = []
        for r in rows:
            ex = _num(v73._perf(r).get("excess_return"), None)
            if ex is None:
                continue
            c = v73._compact(r)
            if r.get("action_type") == WATCH_ACTION and ex >= 5:
                c["diagnostic"] = "quality_gate_rejected_winner"; gate_reject_winners.append(c)
            if r.get("action_type") in v73.ACTIONABLE and ex <= -3:
                c["diagnostic"] = "selective_buy_false_positive"; buy_false.append(c)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        gate_reject_winners.sort(key=key, reverse=True)
        buy_false.sort(key=key)
        out["quality_gate_rejected_winners"] = gate_reject_winners[:30]
        out["selective_buy_false_positives"] = buy_false[:30]
        return out

    v73._upgrade_row = upgrade_row
    v73._sort_expected_return_proxy = expected_sort
    v73._sort_action = action_sort
    v73._pools = pools
    v73._diagnostics = diagnostics


apply()
