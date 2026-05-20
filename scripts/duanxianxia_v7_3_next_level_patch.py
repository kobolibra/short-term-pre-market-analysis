"""v7.3 next-level selective decision overlay.

Purpose
-------
The old design tried to explain too much and buy too much.  This overlay turns
v7.3 into a practical premarket decision system:

1. Preserve broad discovery, but only a small set can become BUY candidates.
2. A BUY must pass explicit evidence gates: cost, amount, liquidity, source,
   action-specific structure, and market regime.
3. Everything else is WATCH or AVOID with a rejection reason.  No forced Top30.
4. No hard-coded sector.  No low-cost/20cm standalone alpha.  No decorative
   complexity in the executable list.
5. Realized returns are review diagnostics only; production uses only premarket
   fields.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import duanxianxia_v7_3_output as v73

_APPLIED = False

BUY_ACTION = "HIGH_CONVICTION_BUY"
WATCH_ACTION = "QUALITY_WATCH"
AVOID_ACTION = "STRUCTURAL_AVOID"

SOURCE_RANK_KEYS = (
    "qiangchou_920_925_rank",
    "qiangchou_last_second_rank",
    "vratio_rank",
    "net_amount_rank",
    "fengdan_rank",
)


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


def _detail(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("auction_detail") or {}


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
        "net_pressure": float(v73._metric(row, "net_pressure", 0.0) or 0.0),
    }


def _regime(row: Dict[str, Any]) -> str:
    return str(row.get("regime") or "normal")


def _entry_tag(row: Dict[str, Any]) -> str:
    return str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")


def _auction_type(row: Dict[str, Any]) -> str:
    return str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "")


def _rank(row: Dict[str, Any], key: str) -> Optional[int]:
    v = _detail(row).get(key)
    try:
        if v in (None, "", 0, "0"):
            return None
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


def _source_count(row: Dict[str, Any]) -> int:
    detail = _detail(row)
    fam = detail.get("source_families") or []
    if isinstance(fam, list) and fam:
        return len([x for x in fam if str(x).strip()])
    return sum(1 for k in SOURCE_RANK_KEYS if _rank(row, k) is not None)


def _has_source(row: Dict[str, Any], max_rank: int = 30) -> bool:
    if _m(row)["source"] >= 8 or _m(row)["family"] >= 1:
        return True
    return any((_rank(row, k) is not None and (_rank(row, k) or 999) <= max_rank) for k in SOURCE_RANK_KEYS)


def _top_source(row: Dict[str, Any], max_rank: int = 10) -> bool:
    return any((_rank(row, k) is not None and (_rank(row, k) or 999) <= max_rank) for k in SOURCE_RANK_KEYS)


def _regime_bar(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    r = _regime(row)
    if "cold" in r:
        return float(cfg.get("buy_min_conviction_cold", 68))
    if "hot_to" in r or "downgrad" in r:
        return float(cfg.get("buy_min_conviction_downgrading", 70))
    return float(cfg.get("buy_min_conviction", 64))


def _fatal_risk(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    m = _m(row)
    if row.get("risk_penalty") == 0:
        return "hard_risk_kill"
    if _entry_tag(row) == "avoid" or _auction_type(row) == "FAKE_STRENGTH":
        return "fake_strength_or_avoid_entry"
    if _auction_type(row) == "BOARD_LOCK_WATCH" or _entry_tag(row) == "board_watch":
        return "board_lock_not_excess_return_trade"
    if m["pct"] >= float(cfg.get("buy_hard_max_pct", 7.0)):
        return "auction_cost_too_high"
    if m["amount"] <= 0:
        return "missing_auction_amount"
    return None


def _broad_repair_score(row: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    m = _m(row)
    pct = m["pct"]
    if -1.8 <= pct <= 2.3:
        cost_fit = 22.0
    elif 2.3 < pct <= 4.8:
        cost_fit = 11.0
    elif -6.5 <= pct < -1.8:
        cost_fit = 8.0
    else:
        cost_fit = 0.0
    return v73._clamp(
        cost_fit
        + min(30.0, m["auction"] * 0.72)
        + min(22.0, m["amount"] / 5000.0 * 22.0)
        + min(8.0, max(m["source"], m["theme"] * 0.05))
        + min(5.0, m["liquidity"] * 0.05)
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
    if m["theme"] > float(cfg.get("broad_repair_theme_max", 20)):
        return False
    if m["source"] > float(cfg.get("broad_repair_source_max", 1.0)) or m["family"] > float(cfg.get("broad_repair_family_max", 1)):
        return False
    if m["auction"] < float(cfg.get("broad_repair_min_auction_strength", 12)):
        return False
    if m["amount"] < float(cfg.get("broad_repair_min_amount_wan", 800)):
        return False
    return _broad_repair_score(row, cfg) >= float(cfg.get("broad_repair_score_min", 24))


def _conviction(row: Dict[str, Any], cfg: Dict[str, Any]) -> Tuple[float, List[str]]:
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    m = _m(row)
    pct = m["pct"]
    reasons: List[str] = []

    base = {
        "MOMENTUM_CATCHUP": 35.0,
        "LOW_OPEN_REVERSAL": 33.0,
        "BROAD_REPAIR_MOMENTUM": 32.0,
        "AUCTION_FOLLOW": 28.0,
        "THEME_CATCHUP": 22.0,
    }.get(action, 0.0)
    if base <= 0:
        return -100.0, ["not_buy_action"]

    quality_adj = {
        "momentum": 9.0,
        "repair": 8.0,
        "broad_repair": 8.0,
        "main_attack": 5.0,
        "strong": 4.0,
        "medium": -8.0,
        "weak": -18.0,
    }.get(quality, 0.0)

    cost = 0.0
    if action == "LOW_OPEN_REVERSAL":
        if -5.5 <= pct < -0.3:
            cost = 12.0; reasons.append("good_low_open_cost")
        elif -8.5 <= pct < -5.5:
            cost = 5.0; reasons.append("deep_repair_cost")
        else:
            cost = -12.0; reasons.append("bad_reversal_cost")
    elif action in {"BROAD_REPAIR_MOMENTUM", "THEME_CATCHUP"}:
        if -1.8 <= pct <= 2.3:
            cost = 12.0; reasons.append("low_cost")
        elif 2.3 < pct <= 4.8:
            cost = 4.0; reasons.append("mid_cost")
        else:
            cost = -10.0; reasons.append("cost_window_weak")
    else:
        if 1.5 <= pct <= 5.8:
            cost = 8.0; reasons.append("healthy_attack_cost")
        else:
            cost = -10.0; reasons.append("attack_cost_window_weak")

    amount = min(14.0, m["amount"] / float(cfg.get("amount_full_wan", 5000)) * 14.0)
    auction = min(14.0, m["auction"] * 0.18)
    source = min(12.0, m["source"] * 0.24 + _source_count(row) * 2.5)
    liquidity = min(6.0, m["liquidity"] * 0.06)

    penalty = 0.0
    if pct > float(cfg.get("buy_soft_max_pct", 6.2)):
        penalty += (pct - float(cfg.get("buy_soft_max_pct", 6.2))) * 4.0
        reasons.append("high_cost_penalty")
    if m["liquidity"] < float(cfg.get("buy_min_liquidity_score", 35)):
        penalty += 10.0; reasons.append("liquidity_penalty")
    if m["amount"] < float(cfg.get("buy_min_amount_wan", 900)):
        penalty += 12.0; reasons.append("amount_penalty")
    if quality == "weak":
        reasons.append("weak_quality_penalty")
    if _regime(row).startswith("cold") and action == "THEME_CATCHUP":
        penalty += 8.0; reasons.append("cold_theme_penalty")

    score = base + quality_adj + cost + amount + auction + source + liquidity - penalty
    return round(score, 2), reasons


def _gate(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    fatal = _fatal_risk(row, cfg)
    if fatal:
        return fatal
    action = str(row.get("action_type"))
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    m = _m(row)
    conv = float(row.get("conviction_score") or 0.0)

    if action not in {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "BROAD_REPAIR_MOMENTUM", "THEME_CATCHUP"}:
        return "not_primary_action"
    if conv < _regime_bar(row, cfg):
        return "conviction_below_regime_bar"
    if m["amount"] < float(cfg.get("buy_min_amount_wan", 900)):
        return "auction_amount_too_small"
    if m["liquidity"] < float(cfg.get("buy_min_liquidity_score", 35)):
        return "liquidity_too_weak"

    if action == "AUCTION_FOLLOW":
        if not (2.0 <= m["pct"] <= 6.2):
            return "auction_follow_cost_window_fail"
        if m["auction"] < 50 or not _has_source(row, 30):
            return "auction_follow_evidence_fail"
        return None

    if action == "MOMENTUM_CATCHUP":
        if not (1.5 <= m["pct"] <= 5.8):
            return "momentum_cost_window_fail"
        if m["auction"] < 50 or m["amount"] < 1000 or m["liquidity"] < 55:
            return "momentum_evidence_fail"
        return None

    if action == "LOW_OPEN_REVERSAL":
        if not (-8.5 <= m["pct"] < -0.3):
            return "low_open_cost_window_fail"
        if m["amount"] < (4500 if m["pct"] < -5.5 else 2500):
            return "low_open_amount_not_enough"
        if not (_rank(row, "net_amount_rank") is not None or _rank(row, "qiangchou_920_925_rank") is not None):
            return "low_open_missing_net_or_sustained_support"
        return None

    if action == "BROAD_REPAIR_MOMENTUM":
        if not (-1.8 <= m["pct"] <= 4.8):
            return "broad_repair_cost_window_fail"
        if float(row.get("action_score") or 0) < 38:
            return "broad_repair_score_too_low"
        if m["auction"] < 15 or m["amount"] < 1000:
            return "broad_repair_support_not_enough"
        return None

    if action == "THEME_CATCHUP":
        if quality != "strong":
            return "theme_not_strong_quality"
        if not (-0.5 <= m["pct"] <= 2.2):
            return "theme_cost_window_fail"
        if m["amount"] < 2000:
            return "theme_amount_too_small"
        if not (_has_source(row, 30) or m["theme"] >= 95):
            return "theme_lacks_independent_auction_evidence"
        return None

    return "unhandled"


def _mark_watch(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    out = dict(row)
    tags = _tags(out)
    _add(tags, "quality_gate_failed", reason)
    out.update(
        pre_gate_action_type=row.get("action_type"),
        action_type=WATCH_ACTION,
        action_quality="watch_only",
        signal_quality="watch_only",
        action_reason=f"quality_gate_failed:{reason}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(WATCH_ACTION, 900),
    )
    return out


def _mark_buy(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    tags = _tags(out)
    _add(tags, "high_conviction_buy")
    out.update(
        pre_gate_action_type=row.get("action_type"),
        action_type=BUY_ACTION,
        action_quality=str(row.get("signal_quality") or row.get("action_quality") or "buy"),
        signal_quality=str(row.get("signal_quality") or row.get("action_quality") or "buy"),
        action_reason=f"BUY:{row.get('action_type')}:{row.get('action_reason') or ''}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(BUY_ACTION, 1),
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

    v73.ACTION_PRIORITY.update({BUY_ACTION: 1, "BROAD_REPAIR_MOMENTUM": 32, WATCH_ACTION: 900, AVOID_ACTION: 950, "HIGH_COST_REPAIR_WATCH": 920})
    v73.ACTIONABLE.clear()
    v73.ACTIONABLE.add(BUY_ACTION)
    v73.NON_ACTIONABLE_WATCH.update({WATCH_ACTION, AVOID_ACTION, "HIGH_COST_REPAIR_WATCH", "BOARD_WATCH", "FAKE_STRENGTH_WATCH", "SOFT_AVOID_REPAIR_CANDIDATE"})

    def upgrade_row(row: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        out = base_upgrade(row, cfg)
        tags = _tags(out)

        if _is_broad_repair_candidate(out, cfg):
            _add(tags, "no_theme_no_source", "broad_repair_momentum")
            score = _broad_repair_score(out, cfg)
            out.update(action_type="BROAD_REPAIR_MOMENTUM", action_quality="broad_repair", signal_quality="broad_repair", action_reason="no_theme_no_source_broad_repair_momentum", action_score=round(score, 2), action_confidence=v73._confidence(score, 55, 35), action_tags=tags)

        conv, reasons = _conviction(out, cfg)
        out["conviction_score"] = conv
        out["conviction_reasons"] = reasons
        out["expected_return_score"] = conv
        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)

        fatal = _fatal_risk(out, cfg)
        if fatal and str(out.get("action_type")) in {"BOARD_WATCH", "AVOID", "FAKE_STRENGTH_WATCH", "HIGH_COST_REPAIR_WATCH"}:
            tags = _tags(out)
            _add(tags, "structural_avoid", fatal)
            out.update(action_type=AVOID_ACTION, action_quality="avoid", signal_quality="avoid", action_reason=f"structural_avoid:{fatal}", action_tags=tags, action_priority=v73.ACTION_PRIORITY.get(AVOID_ACTION, 950))
            out["expected_return_score"] = conv
            return out

        reason = _gate(out, cfg)
        if reason is None:
            out = _mark_buy(out)
        elif str(out.get("action_type")) in {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "BROAD_REPAIR_MOMENTUM", "THEME_CATCHUP"}:
            out = _mark_watch(out, reason)
        out["expected_return_score"] = conv
        out["action_priority"] = v73.ACTION_PRIORITY.get(str(out.get("action_type")), 999)
        return out

    def conviction_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (float(r.get("expected_return_score") or -999), float(r.get("action_score") or 0), float(r.get("final_score") or 0)), reverse=True)

    def action_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (0 if r.get("action_type") == BUY_ACTION else 1, -float(r.get("expected_return_score") or -999), int(r.get("action_priority") or 999), -float(r.get("action_score") or 0)))

    def pools(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = action_sort(rows)
        conv = conviction_sort(rows)
        out["selective_buy_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == BUY_ACTION][:pool_max]
        out["quality_watch_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == WATCH_ACTION][:pool_max]
        out["structural_avoid_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == AVOID_ACTION][:pool_max]
        out["conviction_leaderboard"] = [v73._compact(r) for r in conv if r.get("action_type") in {BUY_ACTION, WATCH_ACTION}][:pool_max]
        return out

    def diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        rejected_winners: List[Dict[str, Any]] = []
        buy_false: List[Dict[str, Any]] = []
        for r in rows:
            ex = _num(v73._perf(r).get("excess_return"), None)
            if ex is None:
                continue
            c = v73._compact(r)
            if r.get("action_type") == WATCH_ACTION and ex >= 5:
                c["diagnostic"] = "quality_gate_rejected_winner"; rejected_winners.append(c)
            if r.get("action_type") == BUY_ACTION and ex <= -3:
                c["diagnostic"] = "selective_buy_false_positive"; buy_false.append(c)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        rejected_winners.sort(key=key, reverse=True)
        buy_false.sort(key=key)
        out["quality_gate_rejected_winners"] = rejected_winners[:30]
        out["selective_buy_false_positives"] = buy_false[:30]
        return out

    v73._upgrade_row = upgrade_row
    v73._sort_expected_return_proxy = conviction_sort
    v73._sort_action = action_sort
    v73._pools = pools
    v73._diagnostics = diagnostics


apply()
