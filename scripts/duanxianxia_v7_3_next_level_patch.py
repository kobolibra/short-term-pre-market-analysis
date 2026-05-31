"""v7.4 next-level premarket selector overlay.

The previous v7.3 selector improved discipline, but the review files still show
three structural problems:

1. Price-cost leakage: `latest_change_pct` can become a real-time/close change;
   BUY decisions must use the 09:25 auction change whenever it is present.
2. Theme overreach: THEME_CATCHUP produced both strong winners and very large
   losers.  It is therefore a watch source, not a premarket BUY source.
3. Cold/warming momentum traps: STAR/ChiNext high-volatility momentum in cold or
   cold-to-warming regimes needs much stronger independent order confirmation.

This module monkey-patches `duanxianxia_v7_3_output` so existing runner/backfill
entry points keep their public API while using the stricter selector below.
Production rules use only premarket-visible fields.  Realized close/excess
returns are review-only and only appear in diagnostics.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import duanxianxia_v7_3_output as v73

_APPLIED = False

BUY = "BUY"
WATCH = "WATCH"
REJECT = "REJECT"
AVOID = "AVOID"

SOURCE_ACTIONS = {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "THEME_CATCHUP"}
BUY_PATTERNS = {"LOW_OPEN_NET_REVERSAL", "CONFIRMED_MOMENTUM", "AUCTION_FOLLOW_THROUGH"}
RANK_KEYS = ("qiangchou_920_925_rank", "qiangchou_last_second_rank", "vratio_rank", "net_amount_rank", "fengdan_rank")
CROWDED_THEME_TOKENS = ("半导体", "芯片", "集成电路", "先进封装", "算力", "光模块", "存储")


def _f(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _detail(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("auction_detail") or {}


def _theme_detail(row: Dict[str, Any]) -> Dict[str, Any]:
    return row.get("theme_detail") or {}


def _rank(row: Dict[str, Any], key: str) -> Optional[int]:
    raw = _detail(row).get(key)
    try:
        if raw in (None, "", 0, "0"):
            return None
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return None


def _best_rank(row: Dict[str, Any]) -> int:
    ranks = [_rank(row, k) for k in RANK_KEYS]
    ranks = [r for r in ranks if r is not None]
    return min(ranks) if ranks else 999


def _source_count(row: Dict[str, Any]) -> int:
    detail = _detail(row)
    families = detail.get("source_families") or []
    if isinstance(families, list) and families:
        return len([x for x in families if str(x).strip()])
    fam = int(v73._metric(row, "source_family_count", 0) or 0)
    if fam:
        return fam
    return sum(1 for k in RANK_KEYS if _rank(row, k) is not None)


def _pct(row: Dict[str, Any]) -> Optional[float]:
    return v73._auction_pct(row)


def _m(row: Dict[str, Any]) -> Dict[str, float]:
    pct = _pct(row)
    return {
        "pct": float(pct if pct is not None else 0.0),
        "auction": float(v73._metric(row, "auction_strength", 0.0) or 0.0),
        "amount": float(v73._metric(row, "auction_amount_wan", 0.0) or 0.0),
        "liquidity": float(v73._metric(row, "liquidity_score", 50.0) or 50.0),
        "source": float(v73._metric(row, "source_evidence_score", 0.0) or 0.0),
        "theme": float(v73._metric(row, "theme_strength_t0", 0.0) or 0.0),
        "hotness": float(v73._metric(row, "hotness_score", 0.0) or 0.0),
        "net_pressure": float(v73._metric(row, "net_pressure", 0.0) or 0.0),
        "orderbook": float(v73._metric(row, "orderbook_quality_score", 45.0) or 45.0),
    }


def _tags(row: Dict[str, Any]) -> List[str]:
    return list(dict.fromkeys(row.get("action_tags") or []))


def _add(tags: List[str], *items: str) -> List[str]:
    for item in items:
        if item and item not in tags:
            tags.append(item)
    return tags


def _regime(shaped: Dict[str, Any]) -> str:
    meta = shaped.get("meta") or {}
    reg = meta.get("regime") if isinstance(meta.get("regime"), dict) else {}
    return str((reg or {}).get("label") or (reg or {}).get("regime") or meta.get("regime_label") or "normal")


def _is_coldish(shaped: Dict[str, Any]) -> bool:
    reg = _regime(shaped)
    return "cold" in reg or "warming" in reg


def _entry(row: Dict[str, Any]) -> str:
    return str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")


def _atype(row: Dict[str, Any]) -> str:
    return str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "")


def _raw_action(row: Dict[str, Any]) -> str:
    return str(row.get("pre_gate_action_type") or row.get("action_type") or "")


def _is_volatile_board(row: Dict[str, Any]) -> bool:
    code = str(row.get("code") or "")
    return code.startswith(("300", "301", "688", "689", "8", "9"))


def _is_crowded_theme(row: Dict[str, Any]) -> bool:
    t = _theme_detail(row)
    text = " ".join([str(t.get("matched_plate") or "")] + [str(x) for x in (t.get("matched_tags") or [])])
    return any(tok in text for tok in CROWDED_THEME_TOKENS)


def _has_order_support(row: Dict[str, Any], max_rank: int = 60) -> bool:
    m = _m(row)
    return m["source"] >= 10 or _source_count(row) >= 2 or _best_rank(row) <= max_rank


def _has_primary_order_support(row: Dict[str, Any], max_rank: int = 50) -> bool:
    return (
        (_rank(row, "qiangchou_920_925_rank") is not None and _rank(row, "qiangchou_920_925_rank") <= max_rank)
        or (_rank(row, "net_amount_rank") is not None and _rank(row, "net_amount_rank") <= max_rank)
        or (_rank(row, "vratio_rank") is not None and _rank(row, "vratio_rank") <= max_rank)
    )


def _hard_reject(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    action = _raw_action(row)
    m = _m(row)
    if action not in SOURCE_ACTIONS:
        return "not_source_action"
    if row.get("risk_penalty") == 0:
        return "hard_risk"
    if _entry(row) == "board_watch" or _atype(row) == "BOARD_LOCK_WATCH" or action == "BOARD_WATCH":
        return "board_lock"
    if _entry(row) == "avoid" or _atype(row) == "FAKE_STRENGTH" or action == "AVOID":
        return "fake_strength_or_avoid"
    if m["pct"] >= float(cfg.get("absolute_max_cost_pct", 6.8)):
        return "cost_too_high"
    if m["amount"] < float(cfg.get("hard_min_amount_wan", 500)):
        return "amount_too_small"
    if m["liquidity"] < float(cfg.get("hard_min_liquidity", 25)):
        return "liquidity_too_weak"
    return None


def _rank_prior(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    ordered = v73._sort_expected_return_proxy(rows)
    return {str(r.get("code") or ""): i for i, r in enumerate(ordered, start=1)}


def _pattern(row: Dict[str, Any], shaped: Dict[str, Any], cfg: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    hard = _hard_reject(row, cfg)
    if hard:
        return "AVOID", hard

    action = _raw_action(row)
    m = _m(row)
    coldish = _is_coldish(shaped)
    pct = m["pct"]

    if action == "LOW_OPEN_REVERSAL":
        if not (float(cfg.get("reversal_pct_min", -7.5)) <= pct <= float(cfg.get("reversal_pct_max", -1.0))):
            return "LOW_OPEN_WATCH", "reversal_cost_not_discounted"
        if m["amount"] < float(cfg.get("reversal_min_amount_wan", 3500)):
            return "LOW_OPEN_WATCH", "reversal_amount_too_small"
        if m["auction"] < float(cfg.get("reversal_min_auction", 24)):
            return "LOW_OPEN_WATCH", "reversal_auction_too_weak"
        if not _has_primary_order_support(row, int(cfg.get("reversal_primary_rank_max", 60))):
            return "LOW_OPEN_WATCH", "reversal_no_primary_order_support"
        if pct < float(cfg.get("deep_reversal_pct", -6.0)) and (
            m["amount"] < float(cfg.get("deep_reversal_min_amount_wan", 9000))
            or m["auction"] < float(cfg.get("deep_reversal_min_auction", 38))
            or _best_rank(row) > int(cfg.get("deep_reversal_best_rank_max", 20))
        ):
            return "LOW_OPEN_WATCH", "deep_reversal_support_not_exceptional"
        return "LOW_OPEN_NET_REVERSAL", None

    if action == "MOMENTUM_CATCHUP":
        if not (float(cfg.get("momentum_min_pct", 1.2)) <= pct <= float(cfg.get("momentum_max_pct", 4.8))):
            return "MOMENTUM_WATCH", "momentum_cost_bad"
        if m["amount"] < float(cfg.get("momentum_min_amount_wan", 3000)):
            return "MOMENTUM_WATCH", "momentum_amount_too_small"
        if m["auction"] < float(cfg.get("momentum_min_auction", 55)) or m["liquidity"] < float(cfg.get("momentum_min_liquidity", 50)):
            return "MOMENTUM_WATCH", "momentum_strength_or_liquidity_weak"
        if not _has_order_support(row, int(cfg.get("momentum_rank_max", 45))):
            return "MOMENTUM_WATCH", "momentum_no_independent_order_support"
        if not _has_primary_order_support(row, int(cfg.get("momentum_primary_rank_max", 45))):
            return "MOMENTUM_WATCH", "momentum_no_primary_order_support"
        if coldish and _is_volatile_board(row):
            if not (
                _source_count(row) >= int(cfg.get("volatile_momentum_min_source_count", 2))
                and _best_rank(row) <= int(cfg.get("volatile_momentum_best_rank_max", 10))
                and m["amount"] >= float(cfg.get("volatile_momentum_min_amount_wan", 15000))
            ):
                return "MOMENTUM_WATCH", "cold_volatile_board_momentum_trap"
        if coldish and _is_crowded_theme(row) and pct > float(cfg.get("crowded_theme_cost_max_cold", 3.2)) and _source_count(row) < int(cfg.get("crowded_theme_min_source_count", 3)):
            return "MOMENTUM_WATCH", "cold_crowded_theme_cost_too_high"
        return "CONFIRMED_MOMENTUM", None

    if action == "AUCTION_FOLLOW":
        if not (float(cfg.get("follow_min_pct", 2.0)) <= pct <= float(cfg.get("follow_max_pct", 5.2))):
            return "FOLLOW_WATCH", "follow_cost_bad"
        if m["amount"] < float(cfg.get("follow_min_amount_wan", 5000)) or m["auction"] < float(cfg.get("follow_min_auction", 60)):
            return "FOLLOW_WATCH", "follow_amount_or_strength_weak"
        if _source_count(row) < int(cfg.get("follow_min_source_count", 2)) and _best_rank(row) > int(cfg.get("follow_best_rank_max", 20)):
            return "FOLLOW_WATCH", "follow_lacks_multi_source_confirmation"
        if coldish and _is_volatile_board(row):
            return "FOLLOW_WATCH", "cold_volatile_follow_watch_only"
        return "AUCTION_FOLLOW_THROUGH", None

    if action == "THEME_CATCHUP":
        # Review evidence says theme-only buys can be spectacular winners, but
        # also -10% traps.  Keep as watch until intraday confirms breadth/hold.
        if m["theme"] >= float(cfg.get("theme_watch_min_theme", 80)) and -1.0 <= pct <= float(cfg.get("theme_watch_max_pct", 2.2)):
            return "THEME_WATCH", "theme_requires_intraday_confirmation"
        return "THEME_REJECT", "theme_not_buyable_premarket"

    return "REJECT", "not_durable_pattern"


def _score(row: Dict[str, Any], pattern: str, rank: int, cfg: Dict[str, Any]) -> Tuple[float, List[str]]:
    m = _m(row)
    pct = m["pct"]
    base = {
        "LOW_OPEN_NET_REVERSAL": 50,
        "CONFIRMED_MOMENTUM": 46,
        "AUCTION_FOLLOW_THROUGH": 43,
        "LOW_OPEN_WATCH": 30,
        "MOMENTUM_WATCH": 28,
        "FOLLOW_WATCH": 25,
        "THEME_WATCH": 20,
    }.get(pattern, 0)
    rank_score = max(0.0, float(cfg.get("rank_prior_points", 18)) - max(0, rank - 1) * float(cfg.get("rank_prior_decay", 0.30)))
    amount_score = min(15.0, m["amount"] / float(cfg.get("amount_full_wan", 7000)) * 15.0)
    auction_score = min(14.0, m["auction"] * 0.17)
    source_score = min(12.0, m["source"] * 0.18 + _source_count(row) * 1.8 + max(0, 30 - min(_best_rank(row), 30)) * 0.08)
    liquidity_score = min(5.0, m["liquidity"] * 0.05)
    cost_adj = 0.0
    if pattern == "LOW_OPEN_NET_REVERSAL":
        cost_adj = 9.0 if -5.8 <= pct <= -1.2 else 3.0
    elif pattern == "CONFIRMED_MOMENTUM":
        cost_adj = 6.0 if 1.5 <= pct <= 4.2 else 0.0
    elif pattern == "AUCTION_FOLLOW_THROUGH":
        cost_adj = 5.0 if 2.0 <= pct <= 4.8 else 0.0
    penalty = 0.0
    if _is_volatile_board(row) and pattern in {"CONFIRMED_MOMENTUM", "AUCTION_FOLLOW_THROUGH"}:
        penalty += float(cfg.get("volatile_board_penalty", 8))
    if _is_crowded_theme(row) and pattern == "CONFIRMED_MOMENTUM" and pct > 3.2:
        penalty += float(cfg.get("crowded_theme_penalty", 8))
    if pct > 5.0:
        penalty += (pct - 5.0) * 6.0
    score = base + rank_score + amount_score + auction_score + source_score + liquidity_score + cost_adj - penalty
    reasons = [pattern, f"rank={rank}", f"amount={round(amount_score,1)}", f"auction={round(auction_score,1)}", f"source={round(source_score,1)}"]
    return round(v73._clamp(score, -100, 100), 2), reasons


def _limits(shaped: Dict[str, Any], cfg: Dict[str, Any], max_candidates: int) -> Tuple[int, float]:
    reg = _regime(shaped)
    if reg == "cold":
        return min(max_candidates, int(cfg.get("max_buy_cold", 1))), float(cfg.get("buy_score_cold", 78))
    if "cold" in reg or "warming" in reg:
        return min(max_candidates, int(cfg.get("max_buy_warming", 2))), float(cfg.get("buy_score_warming", 74))
    return min(max_candidates, int(cfg.get("max_buy_normal", 3))), float(cfg.get("buy_score_normal", 72))


def _make(row: Dict[str, Any], action: str, reason: str) -> Dict[str, Any]:
    out = dict(row)
    original = _raw_action(row)
    tags = _tags(out)
    _add(tags, action.lower(), reason, str(out.get("durable_pattern") or ""))
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
        rank = rank_map.get(str(rr.get("code") or ""), 999)
        pattern, fail = _pattern(rr, shaped, cfg)
        edge, reasons = _score(rr, pattern, rank, cfg)
        rr.update(expected_rank_prior=rank, durable_pattern=pattern, gate_reason=fail, edge_score=edge, conviction_score=edge, expected_return_score=edge, edge_reasons=reasons)
        evaluated.append(rr)

    max_buy, threshold = _limits(shaped, cfg, max_candidates)
    evaluated.sort(key=lambda r: (float(r.get("edge_score") or -999), -int(r.get("expected_rank_prior") or 999)), reverse=True)

    caps = {
        "LOW_OPEN_NET_REVERSAL": int(cfg.get("cap_reversal", 2)),
        "CONFIRMED_MOMENTUM": int(cfg.get("cap_momentum", 1)),
        "AUCTION_FOLLOW_THROUGH": int(cfg.get("cap_follow", 1)),
    }
    counts: Counter[str] = Counter()
    buys: List[Dict[str, Any]] = []
    for row in evaluated:
        pattern = str(row.get("durable_pattern") or "")
        if pattern not in BUY_PATTERNS:
            continue
        if row.get("gate_reason") is not None:
            continue
        if float(row.get("edge_score") or -999) < threshold:
            continue
        if counts[pattern] >= caps.get(pattern, 0):
            continue
        buys.append(_make(row, BUY, pattern))
        counts[pattern] += 1
        if len(buys) >= max_buy:
            break

    buy_codes = {str(r.get("code") or "") for r in buys}
    watch_gap = float(cfg.get("watch_score_gap", 16))
    watch_max = int(cfg.get("watch_max", 10))
    watch_codes: set[str] = set()
    hard_watch_exclusions = {"fake_strength_or_avoid", "board_lock", "cost_too_high", "hard_risk", "amount_too_small", "liquidity_too_weak"}
    for row in evaluated:
        code = str(row.get("code") or "")
        if code in buy_codes:
            continue
        pattern = str(row.get("durable_pattern") or "")
        reason = str(row.get("gate_reason") or pattern or "score_too_low")
        near_buy = pattern in BUY_PATTERNS and float(row.get("edge_score") or -999) >= threshold - watch_gap
        watch_pattern = pattern in {"LOW_OPEN_WATCH", "MOMENTUM_WATCH", "FOLLOW_WATCH", "THEME_WATCH"}
        if (near_buy or watch_pattern) and reason not in hard_watch_exclusions and float(row.get("edge_score") or -999) >= threshold - watch_gap:
            watch_codes.add(code)
        if len(watch_codes) >= watch_max:
            break

    buy_by_code = {str(r.get("code") or ""): r for r in buys}
    eval_by_code = {str(r.get("code") or ""): r for r in evaluated}
    rebuilt: List[Dict[str, Any]] = []
    reject_counts: Counter[str] = Counter()
    for raw in rows:
        code = str(raw.get("code") or "")
        if code in buy_by_code:
            rebuilt.append(buy_by_code[code])
            continue
        ev = eval_by_code.get(code, raw)
        reason = str(ev.get("gate_reason") or ev.get("durable_pattern") or "score_too_low")
        reject_counts[reason] += 1
        if code in watch_codes:
            rebuilt.append(_make(ev, WATCH, reason))
        elif reason in hard_watch_exclusions:
            rebuilt.append(_make(ev, AVOID, reason))
        elif _raw_action(ev) in SOURCE_ACTIONS:
            rebuilt.append(_make(ev, REJECT, reason))
        else:
            keep = dict(ev)
            keep["action_priority"] = v73.ACTION_PRIORITY.get(str(keep.get("action_type")), 999)
            rebuilt.append(keep)
    return buys, rebuilt, dict(reject_counts)


def _performance_quality(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    vals: List[float] = []
    for row in rows:
        ex = _f(v73._perf(row).get("excess_return"), None)
        if ex is not None:
            vals.append(float(ex))
    if not vals:
        return {"with_performance": 0, "suspect": False, "reason": "no_performance"}
    near_zero = sum(1 for x in vals if abs(x) < 0.02)
    return {"with_performance": len(vals), "near_zero_excess_count": near_zero, "near_zero_excess_ratio": round(near_zero / len(vals), 4), "suspect": len(vals) >= 20 and near_zero / len(vals) >= 0.70}


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({BUY: 1, WATCH: 20, AVOID: 900, REJECT: 950})
    v73.ACTIONABLE.clear(); v73.ACTIONABLE.add(BUY)
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
            ex = _f(v73._perf(row).get("excess_return"), None)
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
        watch_winners.sort(key=key, reverse=True); reject_winners.sort(key=key, reverse=True); buy_losers.sort(key=key)
        out["watch_winners"] = watch_winners[:30]
        out["reject_winners"] = reject_winners[:30]
        out["buy_losers"] = buy_losers[:30]
        return out

    def rebuild(shaped: Dict[str, Any], rows: List[Dict[str, Any]], cfg: Dict[str, Any], max_candidates: int, watch_tier_max: int, pool_max: int) -> Dict[str, Any]:
        buys, rebuilt, reject_counts = _select(rows, shaped, cfg, max_candidates)
        ranked = sort_rows(rebuilt)
        meta = dict(shaped.get("meta") or {})
        meta["selector"] = "v7_4_next_level_high_conviction_selector"
        meta["regime_label"] = _regime(shaped)
        meta["buy_count"] = len(buys)
        meta["reject_reason_counts"] = reject_counts
        meta["performance_quality"] = _performance_quality(rebuilt)
        meta["rules"] = [
            "BUY only comes from durable premarket patterns: discounted net reversal, confirmed momentum, or auction follow-through.",
            "THEME_CATCHUP is watch-only at premarket because review data showed large losers despite strong themes.",
            "Cold/cold-to-warming volatile-board momentum requires top-rank, large amount, and multi-source confirmation.",
            "Price cost uses auction_change_pct first; latest/close change is only a fallback when auction_change_pct is missing.",
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
