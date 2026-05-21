"""v7.3 portfolio edge engine.

This is a real rebuild, not another threshold patch.

The earlier overlays failed because they tried to turn many descriptive pools into
trade decisions.  This module separates the workflow into three layers:

1. Discovery: keep the broad v7.2/v7.3 universe and the original expected-return
   proxy as a useful rank prior.
2. Edge evaluation: score only executable premarket structures with transparent
   evidence requirements.  DEBUG_ONLY / no-setup broad repair rows are never a
   default BUY source.
3. Portfolio construction: select a tiny diversified book, with market-regime
   exposure caps and explicit no-trade behavior.

Production uses only premarket-visible fields.  Close/excess-return fields are
review diagnostics only, and the output flags suspicious performance backfills so
bad review data cannot drive future tuning.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duanxianxia_v7_3_output as v73

_APPLIED = False

BUY_ACTION = "HIGH_CONVICTION_BUY"
WATCH_ACTION = "WATCH_TOP"
AVOID_ACTION = "STRUCTURAL_AVOID"
REJECT_ACTION = "REJECT_LOW_EDGE"

PRIMARY_ACTIONS = {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "THEME_CATCHUP"}
SOURCE_RANK_KEYS = (
    "qiangchou_920_925_rank",
    "qiangchou_last_second_rank",
    "vratio_rank",
    "net_amount_rank",
    "fengdan_rank",
)


def _num(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if value in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(value).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _detail(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("auction_detail") or {}


def _tags(row: Dict[str, Any]) -> List[str]:
    return list(dict.fromkeys(row.get("action_tags") or []))


def _add(tags: List[str], *items: str) -> List[str]:
    for item in items:
        if item and item not in tags:
            tags.append(item)
    return tags


def _rank(row: Dict[str, Any], key: str) -> Optional[int]:
    raw = _detail(row).get(key)
    try:
        if raw in (None, "", 0, "0"):
            return None
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return None


def _metric(row: Dict[str, Any]) -> Dict[str, float]:
    pct = v73._auction_pct(row)
    return {
        "pct": float(pct if pct is not None else 0.0),
        "auction": float(v73._metric(row, "auction_strength", 0.0) or 0.0),
        "amount": float(v73._metric(row, "auction_amount_wan", 0.0) or 0.0),
        "liquidity": float(v73._metric(row, "liquidity_score", 50.0) or 50.0),
        "source": float(v73._metric(row, "source_evidence_score", 0.0) or 0.0),
        "family": float(v73._metric(row, "source_family_count", 0.0) or 0.0),
        "theme": float(v73._metric(row, "theme_strength_t0", 0.0) or 0.0),
        "hotness": float(v73._metric(row, "hotness_score", 0.0) or 0.0),
        "net_pressure": float(v73._metric(row, "net_pressure", 0.0) or 0.0),
    }


def _source_count(row: Dict[str, Any]) -> int:
    detail = _detail(row)
    families = detail.get("source_families") or []
    if isinstance(families, list) and families:
        return len([x for x in families if str(x).strip()])
    fam = int(_metric(row)["family"] or 0)
    if fam:
        return fam
    return sum(1 for k in SOURCE_RANK_KEYS if _rank(row, k) is not None)


def _rank_hit(row: Dict[str, Any], max_rank: int) -> bool:
    for key in SOURCE_RANK_KEYS:
        rank = _rank(row, key)
        if rank is not None and rank <= max_rank:
            return True
    return False


def _support_hit(row: Dict[str, Any], max_rank: int = 80) -> bool:
    m = _metric(row)
    return m["source"] >= 6 or _source_count(row) >= 1 or _rank_hit(row, max_rank)


def _regime(shaped: Dict[str, Any]) -> str:
    meta = shaped.get("meta") or {}
    reg = meta.get("regime") if isinstance(meta.get("regime"), dict) else {}
    return str((reg or {}).get("label") or meta.get("regime_label") or "normal")


def _entry_tag(row: Dict[str, Any]) -> str:
    return str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")


def _auction_type(row: Dict[str, Any]) -> str:
    return str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "")


def _structural_reject(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    m = _metric(row)
    auction_type = _auction_type(row)
    entry = _entry_tag(row)
    action = str(row.get("action_type") or "")
    if row.get("risk_penalty") == 0:
        return "hard_risk_kill"
    if entry == "board_watch" or auction_type == "BOARD_LOCK_WATCH" or action == "BOARD_WATCH":
        return "board_lock_not_intraday_alpha"
    if m["pct"] >= float(cfg.get("absolute_cost_kill_pct", 7.2)):
        return "auction_cost_too_high"
    if entry == "avoid" or auction_type == "FAKE_STRENGTH" or action == "AVOID":
        # Fake strength is no longer blindly buried forever, but it cannot enter
        # the premarket BUY book without intraday repair confirmation.
        return "fake_strength_needs_intraday_repair"
    return None


def _rank_prior(rank: int, cfg: Dict[str, Any]) -> float:
    full = float(cfg.get("rank_prior_full", 38.0))
    decay = float(cfg.get("rank_prior_decay", 0.45))
    return max(0.0, full - max(0, rank - 1) * decay)


def _cost_score(family: str, pct: float) -> float:
    if family == "reversal":
        if -4.8 <= pct <= -0.3:
            return 18.0
        if -7.2 <= pct < -4.8:
            return 9.0
        if -9.3 <= pct < -7.2:
            return 0.0
        return -22.0
    if family == "theme":
        if -0.3 <= pct <= 2.2:
            return 14.0
        if -1.2 <= pct < -0.3 or 2.2 < pct <= 3.2:
            return 4.0
        return -16.0
    if family in {"attack", "momentum"}:
        if 1.5 <= pct <= 5.2:
            return 14.0
        if 0.8 <= pct < 1.5 or 5.2 < pct <= 6.5:
            return 4.0
        return -16.0
    return -20.0


def _family(action: str) -> str:
    if action == "AUCTION_FOLLOW":
        return "attack"
    if action == "MOMENTUM_CATCHUP":
        return "momentum"
    if action == "LOW_OPEN_REVERSAL":
        return "reversal"
    if action == "THEME_CATCHUP":
        return "theme"
    return "other"


def _edge_score(row: Dict[str, Any], rank: int, cfg: Dict[str, Any]) -> Tuple[float, List[str]]:
    action = str(row.get("action_type") or "")
    family = _family(action)
    m = _metric(row)
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    reasons: List[str] = []

    action_base = {"attack": 10.0, "momentum": 12.0, "reversal": 12.0, "theme": 7.0}.get(family, -30.0)
    quality_adj = {
        "main_attack": 5.0,
        "momentum": 8.0,
        "repair": 7.0,
        "strong": 5.0,
        "medium": -3.0,
        "weak": -15.0,
        "watch_only": -20.0,
        "debug": -35.0,
        "avoid": -50.0,
    }.get(quality, 0.0)
    rank_score = _rank_prior(rank, cfg)
    amount_score = min(15.0, m["amount"] / float(cfg.get("amount_full_wan", 5000)) * 15.0)
    auction_score = min(14.0, m["auction"] * 0.18)
    source_score = min(12.0, m["source"] * 0.20 + _source_count(row) * 2.0)
    liquidity_score = min(6.0, m["liquidity"] * 0.06)
    theme_score = min(6.0, m["theme"] * 0.04) if family == "theme" else min(3.0, m["theme"] * 0.015)
    hot_score = min(4.0, m["hotness"] * 0.04)

    penalty = 0.0
    if m["pct"] > float(cfg.get("soft_cost_penalty_start_pct", 5.8)):
        penalty += (m["pct"] - float(cfg.get("soft_cost_penalty_start_pct", 5.8))) * 7.0
        reasons.append("high_cost_penalty")
    if m["amount"] < float(cfg.get("min_any_buy_amount_wan", 1000)):
        penalty += 12.0
        reasons.append("thin_amount")
    if m["liquidity"] < float(cfg.get("min_any_buy_liquidity", 35)):
        penalty += 10.0
        reasons.append("weak_liquidity")
    if str(row.get("setup_v72") or "none") == "none":
        penalty += 18.0
        reasons.append("no_setup_penalty")

    score = rank_score + action_base + quality_adj + _cost_score(family, m["pct"]) + amount_score + auction_score + source_score + liquidity_score + theme_score + hot_score - penalty
    reasons.extend([family, f"rank_prior={round(rank_score, 1)}", f"amount={round(amount_score, 1)}", f"auction={round(auction_score, 1)}"])
    return round(v73._clamp(score, -100.0, 100.0), 2), reasons


def _gate(row: Dict[str, Any], shaped: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    fatal = _structural_reject(row, cfg)
    if fatal:
        return fatal
    action = str(row.get("action_type") or "")
    family = _family(action)
    m = _metric(row)
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    regime = _regime(shaped)
    cold = "cold" in regime

    if action not in PRIMARY_ACTIONS:
        return "not_executable_primary_action"

    if family == "attack":
        if not (1.8 <= m["pct"] <= 6.5):
            return "attack_cost_window_fail"
        if m["auction"] < 50 or m["amount"] < 1500 or not _support_hit(row, 60):
            return "attack_evidence_fail"
        return None

    if family == "momentum":
        if not (1.2 <= m["pct"] <= 5.8):
            return "momentum_cost_window_fail"
        if m["auction"] < 48 or m["amount"] < 1600 or m["liquidity"] < 42:
            return "momentum_evidence_fail"
        if cold and not _support_hit(row, 80) and m["amount"] < 3000 and m["hotness"] < 55:
            return "cold_momentum_needs_confirm"
        return None

    if family == "reversal":
        if not (-9.3 <= m["pct"] <= -0.2):
            return "reversal_cost_window_fail"
        if m["pct"] < -7.2:
            if m["amount"] < float(cfg.get("deep_reversal_min_amount_wan", 9000)) or m["auction"] < float(cfg.get("deep_reversal_min_auction", 35)):
                return "deep_reversal_needs_exceptional_support"
        elif m["amount"] < 2400 or m["auction"] < 20:
            return "reversal_support_not_enough"
        if not (_rank(row, "net_amount_rank") is not None or _rank(row, "qiangchou_920_925_rank") is not None or m["source"] >= 8):
            return "reversal_missing_net_or_sustained_support"
        return None

    if family == "theme":
        if not (-1.2 <= m["pct"] <= 3.0):
            return "theme_cost_window_fail"
        if m["theme"] < 80 or m["amount"] < 1800:
            return "theme_strength_or_amount_fail"
        # This fixes the previous strong-only mistake: medium/weak theme can be
        # elevated only when auction confirmation is real.
        if quality != "strong" and not (m["auction"] >= 45 and (m["source"] >= 8 or _source_count(row) >= 1 or m["hotness"] >= 55)):
            return "theme_lacks_independent_auction_confirm"
        return None

    return "unhandled_family"


def _mark_buy(row: Dict[str, Any]) -> Dict[str, Any]:
    original = str(row.get("action_type") or "")
    out = dict(row)
    tags = _tags(out)
    _add(tags, "portfolio_buy", f"source_action:{original}")
    out.update(
        pre_gate_action_type=original,
        action_type=BUY_ACTION,
        action_quality=str(row.get("signal_quality") or row.get("action_quality") or "buy"),
        signal_quality=str(row.get("signal_quality") or row.get("action_quality") or "buy"),
        action_reason=f"BUY:{original}:{row.get('action_reason') or ''}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(BUY_ACTION, 1),
    )
    return out


def _mark_watch(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    original = str(row.get("action_type") or "")
    out = dict(row)
    tags = _tags(out)
    _add(tags, "watch_top", reason)
    out.update(
        pre_gate_action_type=original,
        action_type=WATCH_ACTION,
        action_quality="watch_top",
        signal_quality="watch_top",
        action_reason=f"WATCH_TOP:{original}:{reason}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(WATCH_ACTION, 850),
    )
    return out


def _mark_avoid(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    out = dict(row)
    tags = _tags(out)
    _add(tags, "structural_avoid", reason)
    out.update(
        action_type=AVOID_ACTION,
        action_quality="avoid",
        signal_quality="avoid",
        action_reason=f"STRUCTURAL_AVOID:{reason}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(AVOID_ACTION, 950),
    )
    return out


def _mark_reject(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    out = dict(row)
    tags = _tags(out)
    _add(tags, "reject_low_edge", reason)
    out.update(
        pre_gate_action_type=row.get("action_type"),
        action_type=REJECT_ACTION,
        action_quality="reject",
        signal_quality="reject",
        action_reason=f"REJECT:{row.get('action_type')}:{reason}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(REJECT_ACTION, 990),
    )
    return out


def _portfolio_limits(shaped: Dict[str, Any], cfg: Dict[str, Any], max_candidates: int) -> Dict[str, Any]:
    regime = _regime(shaped)
    if regime == "cold":
        max_buy = min(max_candidates, int(cfg.get("max_buy_cold", 2)))
        threshold = float(cfg.get("edge_buy_threshold_cold", 72))
    elif "cold_to" in regime or "warming" in regime:
        max_buy = min(max_candidates, int(cfg.get("max_buy_warming", 4)))
        threshold = float(cfg.get("edge_buy_threshold_warming", 66))
    else:
        max_buy = min(max_candidates, int(cfg.get("max_buy_normal", 5)))
        threshold = float(cfg.get("edge_buy_threshold_normal", 64))
    return {"max_buy": max_buy, "threshold": threshold, "regime": regime}


def _performance_quality(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    vals: List[float] = []
    for row in rows:
        ex = _num(v73._perf(row).get("excess_return"), None)
        if ex is not None:
            vals.append(float(ex))
    if not vals:
        return {"with_performance": 0, "suspect": False, "reason": "no_backfilled_performance"}
    near_zero = sum(1 for x in vals if abs(x) < 0.02)
    ratio = near_zero / len(vals)
    suspect = len(vals) >= 20 and ratio >= 0.70
    return {"with_performance": len(vals), "near_zero_excess_count": near_zero, "near_zero_excess_ratio": round(ratio, 4), "suspect": suspect, "reason": "excess_return_nearly_all_zero" if suspect else "ok"}


def _select_portfolio(rows: List[Dict[str, Any]], shaped: Dict[str, Any], cfg: Dict[str, Any], max_candidates: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    expected_order = v73._sort_expected_return_proxy(rows)
    rank_map = {str(r.get("code") or ""): i for i, r in enumerate(expected_order, start=1)}
    evaluated: List[Dict[str, Any]] = []

    for row in rows:
        rr = dict(row)
        code = str(rr.get("code") or "")
        rank = rank_map.get(code, 999)
        edge, reasons = _edge_score(rr, rank, cfg)
        gate = _gate(rr, shaped, cfg)
        rr["edge_score"] = edge
        rr["conviction_score"] = edge
        rr["expected_return_score"] = edge
        rr["edge_reasons"] = reasons
        rr["expected_rank_prior"] = rank
        rr["quality_gate_reason"] = gate
        rr["portfolio_family"] = _family(str(rr.get("action_type") or ""))
        evaluated.append(rr)

    limits = _portfolio_limits(shaped, cfg, max_candidates)
    threshold = float(limits["threshold"])
    max_buy = int(limits["max_buy"])
    evaluated.sort(key=lambda r: (float(r.get("edge_score") or -999), -int(r.get("expected_rank_prior") or 999)), reverse=True)

    selected: List[Dict[str, Any]] = []
    family_counts: Counter[str] = Counter()
    deep_reversal = 0
    family_caps = {
        "attack": int(cfg.get("cap_attack", 2)),
        "momentum": int(cfg.get("cap_momentum", 2)),
        "reversal": int(cfg.get("cap_reversal", 2)),
        "theme": int(cfg.get("cap_theme", 1)),
    }
    for row in evaluated:
        family = str(row.get("portfolio_family") or "other")
        if row.get("quality_gate_reason") is not None:
            continue
        if float(row.get("edge_score") or -999) < threshold:
            continue
        if family_counts[family] >= family_caps.get(family, 0):
            continue
        if family == "reversal" and _metric(row)["pct"] < -7.2:
            if deep_reversal >= int(cfg.get("cap_deep_reversal", 1)):
                continue
            deep_reversal += 1
        selected.append(_mark_buy(row))
        family_counts[family] += 1
        if len(selected) >= max_buy:
            break

    selected_codes = {str(r.get("code") or "") for r in selected}
    watch_cap = int(cfg.get("watch_top_max", 12))
    watch_floor = threshold - float(cfg.get("watch_edge_gap", 14))
    watch_codes: set[str] = set()
    for row in evaluated:
        code = str(row.get("code") or "")
        if code in selected_codes:
            continue
        if row.get("quality_gate_reason") in {"fake_strength_needs_intraday_repair", "board_lock_not_intraday_alpha", "auction_cost_too_high", "hard_risk_kill"}:
            continue
        if str(row.get("action_type") or "") not in PRIMARY_ACTIONS:
            continue
        if float(row.get("edge_score") or -999) >= watch_floor:
            watch_codes.add(code)
        if len(watch_codes) >= watch_cap:
            break

    selected_by_code = {str(r.get("code") or ""): r for r in selected}
    eval_by_code = {str(r.get("code") or ""): r for r in evaluated}
    rebuilt: List[Dict[str, Any]] = []
    reject_counts: Counter[str] = Counter()
    for original in rows:
        code = str(original.get("code") or "")
        if code in selected_by_code:
            rebuilt.append(selected_by_code[code])
            continue
        ev = eval_by_code.get(code, original)
        reason = str((ev or {}).get("quality_gate_reason") or "edge_below_buy_bar")
        reject_counts[reason] += 1
        if code in watch_codes:
            rebuilt.append(_mark_watch(ev, reason))
        else:
            fatal = _structural_reject(ev, cfg)
            if fatal and str(ev.get("action_type")) in {"AVOID", "BOARD_WATCH", "FAKE_STRENGTH_WATCH", "SOFT_AVOID_REPAIR_CANDIDATE"}:
                rebuilt.append(_mark_avoid(ev, fatal))
            elif str(ev.get("action_type")) in PRIMARY_ACTIONS:
                rebuilt.append(_mark_reject(ev, reason))
            else:
                keep = dict(ev)
                keep["action_priority"] = v73.ACTION_PRIORITY.get(str(keep.get("action_type")), 999)
                rebuilt.append(keep)
    return selected, rebuilt, dict(reject_counts)


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({BUY_ACTION: 1, WATCH_ACTION: 850, AVOID_ACTION: 950, REJECT_ACTION: 990})
    v73.ACTIONABLE.clear()
    v73.ACTIONABLE.add(BUY_ACTION)
    v73.NON_ACTIONABLE_WATCH.update({WATCH_ACTION, AVOID_ACTION, REJECT_ACTION})

    def action_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (0 if r.get("action_type") == BUY_ACTION else 1, 0 if r.get("action_type") == WATCH_ACTION else 1, -float(r.get("edge_score") or r.get("conviction_score") or r.get("expected_return_score") or -999), int(r.get("action_priority") or 999)))

    def pools(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = action_sort(rows)
        out["portfolio_buy_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == BUY_ACTION][:pool_max]
        out["watch_top_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == WATCH_ACTION][:pool_max]
        out["structural_avoid_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == AVOID_ACTION][:pool_max]
        out["reject_low_edge_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == REJECT_ACTION][:pool_max]
        return out

    def diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        watch_winners: List[Dict[str, Any]] = []
        reject_winners: List[Dict[str, Any]] = []
        buy_false: List[Dict[str, Any]] = []
        for row in rows:
            ex = _num(v73._perf(row).get("excess_return"), None)
            if ex is None:
                continue
            compact = v73._compact(row)
            if row.get("action_type") == WATCH_ACTION and ex >= 5:
                compact["diagnostic"] = "watch_top_winner"; watch_winners.append(compact)
            if row.get("action_type") == REJECT_ACTION and ex >= 5:
                compact["diagnostic"] = "reject_low_edge_winner"; reject_winners.append(compact)
            if row.get("action_type") == BUY_ACTION and ex <= -3:
                compact["diagnostic"] = "portfolio_buy_false_positive"; buy_false.append(compact)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        watch_winners.sort(key=key, reverse=True)
        reject_winners.sort(key=key, reverse=True)
        buy_false.sort(key=key)
        out["watch_top_winners"] = watch_winners[:30]
        out["reject_low_edge_winners"] = reject_winners[:30]
        out["portfolio_buy_false_positives"] = buy_false[:30]
        return out

    def rebuild(shaped: Dict[str, Any], rows: List[Dict[str, Any]], cfg: Dict[str, Any], max_candidates: int, watch_tier_max: int, pool_max: int) -> Dict[str, Any]:
        selected, rebuilt_rows, reject_counts = _select_portfolio(rows, shaped, cfg, max_candidates)
        ranked = action_sort(rebuilt_rows)
        legacy = v73._sort_score(rebuilt_rows)
        meta = dict(shaped.get("meta") or {})
        notes = list(meta.get("interpretation_notes") or [])
        required = [
            "Portfolio edge engine: BUY is a small diversified portfolio, not a descriptive pool rank.",
            "DEBUG_ONLY/no-setup broad repair rows are never default BUY sources.",
            "WATCH_TOP is capped and only means near-miss; all other primary rows become REJECT_LOW_EDGE or structural avoid.",
            "Close/excess-return fields are review-only; performance_quality flags suspicious backfills.",
        ]
        for note in required:
            if note not in notes:
                notes.append(note)
        meta["interpretation_notes"] = notes
        meta["portfolio_mode"] = "edge_engine_v1"
        meta["portfolio_regime"] = _regime(shaped)
        meta["portfolio_buy_count"] = len(selected)
        meta["reject_reason_counts"] = reject_counts
        meta["performance_quality"] = _performance_quality(rebuilt_rows)
        return {
            "version": v73.VERSION,
            "meta": meta,
            "setup_stats": shaped.get("setup_stats") or v73.v72.setup_stats_v72(rebuilt_rows),
            "action_stats": v73._stats(rebuilt_rows),
            "action_quality_stats": v73._quality_stats(rebuilt_rows),
            "pool_performance": v73._performance_stats(rebuilt_rows),
            "review_diagnostics": diagnostics(rebuilt_rows),
            "candidate_pools": pools(rebuilt_rows, pool_max),
            "top_candidates": selected[:max_candidates],
            "actionable_candidates": selected[:max_candidates],
            "expected_return_candidates": selected[:max_candidates],
            "watch_tier": ranked[:watch_tier_max],
            "expected_return_watch_tier": ranked[:watch_tier_max],
            "legacy_top_candidates": [r for r in legacy if r.get("setup_v72") != "none"][:max_candidates],
            "all_candidates_action_ranked": ranked,
            "all_candidates_expected_return_ranked": ranked,
            "all_candidates_debug": legacy,
            "intraday_anchors": v73.v72.build_intraday_anchors_v72(selected[:20]),
        }

    def upgrade_shaped_v72_to_v73(shaped: Dict[str, Any], action_config: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, pool_max: int = 15) -> Dict[str, Any]:
        cfg = action_config or {}
        source = shaped.get("all_candidates_action_ranked") or shaped.get("all_candidates_debug") or []
        rows = [base_upgrade(r, cfg) for r in source]
        return rebuild(shaped, rows, cfg, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)

    def shape_v7_3_output(decisions: List[Dict[str, Any]], meta: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, action_config: Optional[Dict[str, Any]] = None, pool_max: int = 15) -> Dict[str, Any]:
        base = v73.v72.shape_v7_2_output(decisions, meta=meta, max_candidates=max_candidates, watch_tier_max=watch_tier_max, action_config=action_config)
        return upgrade_shaped_v72_to_v73(base, action_config=action_config, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)

    v73._sort_action = action_sort
    v73._pools = pools
    v73._diagnostics = diagnostics
    v73.upgrade_shaped_v72_to_v73 = upgrade_shaped_v72_to_v73
    v73.shape_v7_3_output = shape_v7_3_output


apply()
