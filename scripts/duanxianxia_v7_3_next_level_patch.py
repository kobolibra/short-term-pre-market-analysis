"""v7.3 simple premarket signal selector.

This intentionally removes the previous concept stack.

Goal
----
Use only the few fields available before the open to answer one practical
question: which names have enough *real premarket edge* to deserve attention?

No decorative pools.  No debug/no-setup buying.  No sector hard-code.  No broad
repair auto-buy.  No post-close fields in production.

The selector is deliberately small and auditable:

1. Build four real signal scores from premarket data:
   - sustained auction buying
   - net/order support on low open
   - clean amount/liquidity
   - theme confirmation only when confirmed by auction money
2. Apply hard risk/cost/liquidity filters.
3. Select a tiny portfolio with explicit caps.
4. Put only near misses into WATCH; everything else is REJECT/AVOID.
5. Flag suspicious performance backfills in review output.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

import duanxianxia_v7_3_output as v73

_APPLIED = False

BUY = "BUY"
WATCH = "WATCH"
REJECT = "REJECT"
AVOID = "AVOID"

BUY_SOURCE_ACTIONS = {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "THEME_CATCHUP"}
RANK_KEYS = (
    "qiangchou_920_925_rank",
    "qiangchou_last_second_rank",
    "vratio_rank",
    "net_amount_rank",
    "fengdan_rank",
)


def _to_float(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _detail(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("auction_detail") or {}


def _rank(row: Dict[str, Any], key: str) -> Optional[int]:
    raw = _detail(row).get(key)
    try:
        if raw in (None, "", 0, "0"):
            return None
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return None


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
        "hotness": float(v73._metric(row, "hotness_score", 0.0) or 0.0),
        "net_pressure": float(v73._metric(row, "net_pressure", 0.0) or 0.0),
    }


def _source_count(row: Dict[str, Any]) -> int:
    detail = _detail(row)
    families = detail.get("source_families") or []
    if isinstance(families, list) and families:
        return len([x for x in families if str(x).strip()])
    fam = int(_m(row)["family"] or 0)
    if fam:
        return fam
    return sum(1 for key in RANK_KEYS if _rank(row, key) is not None)


def _best_rank(row: Dict[str, Any]) -> int:
    ranks = [_rank(row, key) for key in RANK_KEYS]
    ranks = [r for r in ranks if r is not None]
    return min(ranks) if ranks else 999


def _has_order_support(row: Dict[str, Any], max_rank: int = 80) -> bool:
    m = _m(row)
    return m["source"] >= 6 or _source_count(row) > 0 or _best_rank(row) <= max_rank


def _regime(shaped: Dict[str, Any]) -> str:
    meta = shaped.get("meta") or {}
    reg = meta.get("regime") if isinstance(meta.get("regime"), dict) else {}
    return str((reg or {}).get("label") or meta.get("regime_label") or "normal")


def _entry(row: Dict[str, Any]) -> str:
    return str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")


def _auction_type(row: Dict[str, Any]) -> str:
    return str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "")


def _rank_prior(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    # The old expected-return proxy was empirically better than pure action order.
    # Use it only as a prior, never as an automatic buy list.
    ordered = v73._sort_expected_return_proxy(rows)
    return {str(row.get("code") or ""): i for i, row in enumerate(ordered, start=1)}


def _score_cost(row: Dict[str, Any]) -> Tuple[float, str]:
    m = _m(row)
    action = str(row.get("action_type") or "")
    pct = m["pct"]
    if action == "LOW_OPEN_REVERSAL":
        if -4.8 <= pct <= -0.3:
            return 18, "good_reversal_cost"
        if -7.2 <= pct < -4.8:
            return 8, "deep_reversal_cost"
        if -9.2 <= pct < -7.2:
            return -5, "extreme_reversal_cost"
        return -25, "bad_reversal_cost"
    if action == "THEME_CATCHUP":
        if -0.5 <= pct <= 2.2:
            return 16, "good_theme_cost"
        if -1.2 <= pct < -0.5 or 2.2 < pct <= 3.2:
            return 4, "borderline_theme_cost"
        return -20, "bad_theme_cost"
    # attack / momentum
    if 1.5 <= pct <= 5.5:
        return 16, "good_attack_cost"
    if 0.8 <= pct < 1.5 or 5.5 < pct <= 6.8:
        return 3, "borderline_attack_cost"
    return -22, "bad_attack_cost"


def _real_signal_score(row: Dict[str, Any], expected_rank: int, cfg: Dict[str, Any]) -> Tuple[float, List[str]]:
    m = _m(row)
    action = str(row.get("action_type") or "")
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    reasons: List[str] = []

    # Rank prior is useful, but capped.  This prevents hand-built rules from
    # discarding names that the existing proxy consistently put near the top.
    rank_score = max(0.0, float(cfg.get("rank_prior_points", 32)) - max(0, expected_rank - 1) * float(cfg.get("rank_prior_decay", 0.35)))
    cost_score, cost_reason = _score_cost(row)
    reasons.append(cost_reason)

    action_score = {
        "AUCTION_FOLLOW": 10,
        "MOMENTUM_CATCHUP": 12,
        "LOW_OPEN_REVERSAL": 12,
        "THEME_CATCHUP": 8,
    }.get(action, -40)
    quality_score = {
        "main_attack": 5,
        "momentum": 8,
        "repair": 8,
        "strong": 6,
        "medium": -2,
        "weak": -12,
    }.get(quality, 0)

    amount_score = min(16.0, m["amount"] / float(cfg.get("amount_full_wan", 5000)) * 16.0)
    auction_score = min(16.0, m["auction"] * 0.20)
    source_score = min(14.0, m["source"] * 0.20 + _source_count(row) * 2.5 + max(0, 40 - min(_best_rank(row), 40)) * 0.08)
    liquidity_score = min(6.0, m["liquidity"] * 0.06)
    hot_theme_score = 0.0
    if action == "THEME_CATCHUP":
        hot_theme_score = min(10.0, m["theme"] * 0.06 + m["hotness"] * 0.04)
    else:
        hot_theme_score = min(4.0, m["hotness"] * 0.03)

    penalty = 0.0
    if m["pct"] > float(cfg.get("soft_cost_start_pct", 5.8)):
        penalty += (m["pct"] - float(cfg.get("soft_cost_start_pct", 5.8))) * 7.0
        reasons.append("high_cost_penalty")
    if m["amount"] < float(cfg.get("min_amount_wan", 1200)):
        penalty += 12
        reasons.append("amount_too_small")
    if m["liquidity"] < float(cfg.get("min_liquidity", 35)):
        penalty += 10
        reasons.append("liquidity_too_weak")
    if str(row.get("setup_v72") or "none") == "none":
        penalty += 20
        reasons.append("no_setup")

    score = rank_score + action_score + quality_score + cost_score + amount_score + auction_score + source_score + liquidity_score + hot_theme_score - penalty
    reasons.extend([f"rank={expected_rank}", f"amount={round(amount_score, 1)}", f"auction={round(auction_score, 1)}", f"source={round(source_score, 1)}"])
    return round(v73._clamp(score, -100, 100), 2), reasons


def _hard_filter(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    m = _m(row)
    action = str(row.get("action_type") or "")
    if action not in BUY_SOURCE_ACTIONS:
        return "not_buy_source_action"
    if row.get("risk_penalty") == 0:
        return "hard_risk"
    if _entry(row) == "board_watch" or _auction_type(row) == "BOARD_LOCK_WATCH" or action == "BOARD_WATCH":
        return "board_lock"
    if _entry(row) == "avoid" or _auction_type(row) == "FAKE_STRENGTH" or action == "AVOID":
        return "fake_strength_or_avoid"
    if m["pct"] >= float(cfg.get("absolute_max_cost_pct", 7.0)):
        return "cost_too_high"
    return None


def _signal_filter(row: Dict[str, Any], shaped: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    hard = _hard_filter(row, cfg)
    if hard:
        return hard
    m = _m(row)
    action = str(row.get("action_type") or "")
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    cold = "cold" in _regime(shaped)

    if action == "AUCTION_FOLLOW":
        if not (1.8 <= m["pct"] <= 6.5):
            return "auction_follow_bad_cost"
        if m["auction"] < 50 or m["amount"] < 1500 or not _has_order_support(row, 60):
            return "auction_follow_weak_support"
        return None

    if action == "MOMENTUM_CATCHUP":
        if not (1.2 <= m["pct"] <= 5.8):
            return "momentum_bad_cost"
        if m["auction"] < 48 or m["amount"] < 1500 or m["liquidity"] < 42:
            return "momentum_weak_support"
        if cold and not _has_order_support(row, 80) and m["amount"] < 3000 and m["hotness"] < 55:
            return "cold_momentum_not_confirmed"
        return None

    if action == "LOW_OPEN_REVERSAL":
        if not (-9.2 <= m["pct"] <= -0.2):
            return "reversal_bad_cost"
        if m["pct"] < -7.2:
            if m["amount"] < float(cfg.get("deep_reversal_min_amount_wan", 9000)) or m["auction"] < float(cfg.get("deep_reversal_min_auction", 35)):
                return "deep_reversal_weak_support"
        elif m["amount"] < 2200 or m["auction"] < 20:
            return "reversal_weak_support"
        if not (_rank(row, "net_amount_rank") is not None or _rank(row, "qiangchou_920_925_rank") is not None or m["source"] >= 8):
            return "reversal_no_real_order_support"
        return None

    if action == "THEME_CATCHUP":
        if not (-1.2 <= m["pct"] <= 3.0):
            return "theme_bad_cost"
        if m["theme"] < 80 or m["amount"] < 1800:
            return "theme_weak_strength_or_amount"
        # Do not require strong label blindly; require independent auction money.
        if quality != "strong" and not (m["auction"] >= 45 and (_has_order_support(row, 80) or m["hotness"] >= 55)):
            return "theme_not_confirmed_by_auction"
        return None

    return "unhandled"


def _limits(shaped: Dict[str, Any], cfg: Dict[str, Any], max_candidates: int) -> Tuple[int, float]:
    regime = _regime(shaped)
    if regime == "cold":
        return min(max_candidates, int(cfg.get("max_buy_cold", 2))), float(cfg.get("buy_score_cold", 72))
    if "warming" in regime or "cold_to" in regime:
        return min(max_candidates, int(cfg.get("max_buy_warming", 4))), float(cfg.get("buy_score_warming", 66))
    return min(max_candidates, int(cfg.get("max_buy_normal", 5))), float(cfg.get("buy_score_normal", 64))


def _make(row: Dict[str, Any], action: str, reason: str) -> Dict[str, Any]:
    out = dict(row)
    original = str(row.get("action_type") or "")
    tags = _tags(out)
    _add(tags, action.lower(), reason)
    out.update(
        pre_gate_action_type=original,
        action_type=action,
        action_quality=action.lower(),
        signal_quality=action.lower(),
        action_reason=f"{action}:{original}:{reason}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(action, 999),
        action_score=out.get("edge_score", out.get("action_score")),
    )
    return out


def _select(rows: List[Dict[str, Any]], shaped: Dict[str, Any], cfg: Dict[str, Any], max_candidates: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    rank_map = _rank_prior(rows)
    evaluated: List[Dict[str, Any]] = []
    for row in rows:
        rr = dict(row)
        expected_rank = rank_map.get(str(rr.get("code") or ""), 999)
        score, reasons = _real_signal_score(rr, expected_rank, cfg)
        gate = _signal_filter(rr, shaped, cfg)
        rr["edge_score"] = score
        rr["conviction_score"] = score
        rr["expected_return_score"] = score
        rr["expected_rank_prior"] = expected_rank
        rr["edge_reasons"] = reasons
        rr["gate_reason"] = gate
        rr["signal_family"] = str(rr.get("action_type") or "")
        evaluated.append(rr)

    max_buy, threshold = _limits(shaped, cfg, max_candidates)
    evaluated.sort(key=lambda r: (float(r.get("edge_score") or -999), -int(r.get("expected_rank_prior") or 999)), reverse=True)

    caps = {
        "AUCTION_FOLLOW": int(cfg.get("cap_auction_follow", 2)),
        "MOMENTUM_CATCHUP": int(cfg.get("cap_momentum", 2)),
        "LOW_OPEN_REVERSAL": int(cfg.get("cap_reversal", 2)),
        "THEME_CATCHUP": int(cfg.get("cap_theme", 1)),
    }
    counts: Counter[str] = Counter()
    deep_reversal = 0
    buys: List[Dict[str, Any]] = []
    for row in evaluated:
        action = str(row.get("action_type") or "")
        if row.get("gate_reason") is not None:
            continue
        if float(row.get("edge_score") or -999) < threshold:
            continue
        if counts[action] >= caps.get(action, 0):
            continue
        if action == "LOW_OPEN_REVERSAL" and _m(row)["pct"] < -7.2:
            if deep_reversal >= int(cfg.get("cap_deep_reversal", 1)):
                continue
            deep_reversal += 1
        buys.append(_make(row, BUY, "passed"))
        counts[action] += 1
        if len(buys) >= max_buy:
            break

    buy_codes = {str(row.get("code") or "") for row in buys}
    watch_gap = float(cfg.get("watch_score_gap", 12))
    watch_max = int(cfg.get("watch_max", 10))
    watch_codes: set[str] = set()
    for row in evaluated:
        code = str(row.get("code") or "")
        if code in buy_codes:
            continue
        if str(row.get("action_type") or "") not in BUY_SOURCE_ACTIONS:
            continue
        if row.get("gate_reason") in {"hard_risk", "board_lock", "fake_strength_or_avoid", "cost_too_high"}:
            continue
        if float(row.get("edge_score") or -999) >= threshold - watch_gap:
            watch_codes.add(code)
        if len(watch_codes) >= watch_max:
            break

    eval_by_code = {str(row.get("code") or ""): row for row in evaluated}
    buy_by_code = {str(row.get("code") or ""): row for row in buys}
    rebuilt: List[Dict[str, Any]] = []
    reject_counts: Counter[str] = Counter()
    for raw in rows:
        code = str(raw.get("code") or "")
        if code in buy_by_code:
            rebuilt.append(buy_by_code[code])
            continue
        ev = eval_by_code.get(code, raw)
        reason = str((ev or {}).get("gate_reason") or "score_too_low")
        reject_counts[reason] += 1
        if code in watch_codes:
            rebuilt.append(_make(ev, WATCH, reason))
        elif reason in {"hard_risk", "board_lock", "fake_strength_or_avoid", "cost_too_high"}:
            rebuilt.append(_make(ev, AVOID, reason))
        elif str(ev.get("action_type") or "") in BUY_SOURCE_ACTIONS:
            rebuilt.append(_make(ev, REJECT, reason))
        else:
            keep = dict(ev)
            keep["action_priority"] = v73.ACTION_PRIORITY.get(str(keep.get("action_type")), 999)
            rebuilt.append(keep)
    return buys, rebuilt, dict(reject_counts)


def _performance_quality(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    vals: List[float] = []
    for row in rows:
        ex = _to_float(v73._perf(row).get("excess_return"), None)
        if ex is not None:
            vals.append(float(ex))
    if not vals:
        return {"with_performance": 0, "suspect": False, "reason": "no_performance"}
    near_zero = sum(1 for x in vals if abs(x) < 0.02)
    ratio = near_zero / len(vals)
    suspect = len(vals) >= 20 and ratio >= 0.70
    return {"with_performance": len(vals), "near_zero_excess_count": near_zero, "near_zero_excess_ratio": round(ratio, 4), "suspect": suspect, "reason": "excess_return_nearly_all_zero" if suspect else "ok"}


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({BUY: 1, WATCH: 20, AVOID: 900, REJECT: 950})
    v73.ACTIONABLE.clear()
    v73.ACTIONABLE.add(BUY)
    v73.NON_ACTIONABLE_WATCH.update({WATCH, AVOID, REJECT})

    def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (int(v73.ACTION_PRIORITY.get(str(r.get("action_type")), 999)), -float(r.get("edge_score") or r.get("conviction_score") or -999), int(r.get("expected_rank_prior") or 999)))

    def pools(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        ranked = sort_rows(rows)
        return {
            "buy": [v73._compact(r) for r in ranked if r.get("action_type") == BUY][:pool_max],
            "watch": [v73._compact(r) for r in ranked if r.get("action_type") == WATCH][:pool_max],
            "reject": [v73._compact(r) for r in ranked if r.get("action_type") == REJECT][:pool_max],
            "avoid": [v73._compact(r) for r in ranked if r.get("action_type") == AVOID][:pool_max],
        }

    def diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        out = base_diagnostics(rows)
        watch_winners: List[Dict[str, Any]] = []
        reject_winners: List[Dict[str, Any]] = []
        buy_losers: List[Dict[str, Any]] = []
        for row in rows:
            ex = _to_float(v73._perf(row).get("excess_return"), None)
            if ex is None:
                continue
            compact = v73._compact(row)
            if row.get("action_type") == WATCH and ex >= 5:
                compact["diagnostic"] = "watch_winner"; watch_winners.append(compact)
            if row.get("action_type") == REJECT and ex >= 5:
                compact["diagnostic"] = "reject_winner"; reject_winners.append(compact)
            if row.get("action_type") == BUY and ex <= -3:
                compact["diagnostic"] = "buy_loser"; buy_losers.append(compact)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        watch_winners.sort(key=key, reverse=True)
        reject_winners.sort(key=key, reverse=True)
        buy_losers.sort(key=key)
        out["watch_winners"] = watch_winners[:30]
        out["reject_winners"] = reject_winners[:30]
        out["buy_losers"] = buy_losers[:30]
        return out

    def rebuild(shaped: Dict[str, Any], rows: List[Dict[str, Any]], cfg: Dict[str, Any], max_candidates: int, watch_tier_max: int, pool_max: int) -> Dict[str, Any]:
        buys, rebuilt, reject_counts = _select(rows, shaped, cfg, max_candidates)
        ranked = sort_rows(rebuilt)
        meta = dict(shaped.get("meta") or {})
        meta["selector"] = "simple_premarket_signal_selector_v1"
        meta["regime_label"] = _regime(shaped)
        meta["buy_count"] = len(buys)
        meta["reject_reason_counts"] = reject_counts
        meta["performance_quality"] = _performance_quality(rebuilt)
        meta["rules"] = [
            "Only AUCTION_FOLLOW/MOMENTUM_CATCHUP/LOW_OPEN_REVERSAL/THEME_CATCHUP can become BUY.",
            "DEBUG_ONLY, broad repair, board lock, fake strength and high-cost rows cannot become premarket BUY.",
            "BUY requires hard filters, real premarket order support, score threshold, and family caps.",
            "WATCH is capped near-miss only; REJECT/AVOID are not trading lists.",
        ]
        return {
            "version": v73.VERSION,
            "meta": meta,
            "setup_stats": shaped.get("setup_stats") or v73.v72.setup_stats_v72(rebuilt),
            "action_stats": v73._stats(rebuilt),
            "action_quality_stats": v73._quality_stats(rebuilt),
            "pool_performance": v73._performance_stats(rebuilt),
            "review_diagnostics": diagnostics(rebuilt),
            "candidate_pools": pools(rebuilt, pool_max),
            "top_candidates": buys[:max_candidates],
            "actionable_candidates": buys[:max_candidates],
            "expected_return_candidates": buys[:max_candidates],
            "watch_tier": ranked[:watch_tier_max],
            "expected_return_watch_tier": ranked[:watch_tier_max],
            "legacy_top_candidates": [r for r in v73._sort_score(rebuilt) if r.get("setup_v72") != "none"][:max_candidates],
            "all_candidates_action_ranked": ranked,
            "all_candidates_expected_return_ranked": ranked,
            "all_candidates_debug": v73._sort_score(rebuilt),
            "intraday_anchors": v73.v72.build_intraday_anchors_v72(buys[:20]),
        }

    def upgrade_shaped_v72_to_v73(shaped: Dict[str, Any], action_config: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, pool_max: int = 15) -> Dict[str, Any]:
        cfg = action_config or {}
        source = shaped.get("all_candidates_action_ranked") or shaped.get("all_candidates_debug") or []
        rows = [base_upgrade(r, cfg) for r in source]
        return rebuild(shaped, rows, cfg, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)

    def shape_v7_3_output(decisions: List[Dict[str, Any]], meta: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, action_config: Optional[Dict[str, Any]] = None, pool_max: int = 15) -> Dict[str, Any]:
        base = v73.v72.shape_v7_2_output(decisions, meta=meta, max_candidates=max_candidates, watch_tier_max=watch_tier_max, action_config=action_config)
        return upgrade_shaped_v72_to_v73(base, action_config=action_config, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)

    v73._sort_action = sort_rows
    v73._pools = pools
    v73._diagnostics = diagnostics
    v73.upgrade_shaped_v72_to_v73 = upgrade_shaped_v72_to_v73
    v73.shape_v7_3_output = shape_v7_3_output


apply()
