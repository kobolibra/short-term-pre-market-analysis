"""Unified v8 premarket engine for duanxianxia.

This is the production selection layer.  It replaces the previous mixed
v7.2 -> v7.3 -> monkey-patch overlay chain with one explicit engine:

    v7.2 data/signal extraction -> v8 unified decision engine -> v8 output

Hard production rule:
- Premarket price/cost uses auction_change_pct only.
- latest_change_pct / 最新涨幅 / 涨幅 are not selection inputs.

The engine is intentionally mechanism-based rather than sample-fitted:
- cost/payoff asymmetry
- auction confirmation breadth
- auction amount/liquidity
- orderbook/risk/tradability
- market regime budget
- pattern caps
"""
from __future__ import annotations

from collections import Counter
from statistics import median
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

VERSION = "premarket_v8_unified_edge_engine"
BUY = "BUY"
WATCH = "WATCH"
REJECT = "REJECT"
AVOID = "AVOID"

SOURCE_ACTIONS = {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "THEME_CATCHUP"}
BUY_PATTERNS = {"LOW_OPEN_NET_REVERSAL", "CONFIRMED_MOMENTUM", "AUCTION_FOLLOW_THROUGH", "THEME_AUCTION_CONFIRMED"}
RANK_KEYS = ("qiangchou_920_925_rank", "qiangchou_last_second_rank", "vratio_rank", "net_amount_rank", "fengdan_rank")
PRIMARY_RANK_KEYS = ("qiangchou_920_925_rank", "net_amount_rank", "vratio_rank")
ACTION_PRIORITY = {BUY: 1, WATCH: 20, REJECT: 900, AVOID: 950, "DEBUG": 999}


def _f(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return default


def _detail(row: Mapping[str, Any]) -> Mapping[str, Any]:
    val = row.get("auction_detail")
    return val if isinstance(val, Mapping) else {}


def _theme_detail(row: Mapping[str, Any]) -> Mapping[str, Any]:
    val = row.get("theme_detail")
    return val if isinstance(val, Mapping) else {}


def _metric(row: Mapping[str, Any], key: str, default: Optional[float] = 0.0) -> Optional[float]:
    if key in row:
        return _f(row.get(key), default)
    detail = _detail(row)
    if key in detail:
        return _f(detail.get(key), default)
    summary = row.get("signal_summary") if isinstance(row.get("signal_summary"), Mapping) else {}
    if key in summary:
        return _f(summary.get(key), default)
    return default


def auction_pct(row: Mapping[str, Any]) -> Optional[float]:
    """Premarket cost/price.  Only auction_change_pct / auction_pct are valid."""
    for value in (row.get("auction_pct"), row.get("auction_change_pct"), _detail(row).get("auction_change_pct")):
        pct = _f(value, None)
        if pct is not None:
            return pct
    summary = row.get("signal_summary") if isinstance(row.get("signal_summary"), Mapping) else {}
    pct = _f(summary.get("auction_change_pct"), None)
    return pct


def _rank(row: Mapping[str, Any], key: str) -> Optional[int]:
    try:
        raw = _detail(row).get(key)
        if raw in (None, "", 0, "0"):
            return None
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return None


def _best_rank(row: Mapping[str, Any], keys: Iterable[str] = RANK_KEYS) -> int:
    ranks = [_rank(row, k) for k in keys]
    ranks = [r for r in ranks if r is not None]
    return min(ranks) if ranks else 999


def _source_count(row: Mapping[str, Any]) -> int:
    families = _detail(row).get("source_families") or []
    if isinstance(families, list) and families:
        return len([x for x in families if str(x).strip()])
    fam = int(_metric(row, "source_family_count", 0) or 0)
    if fam:
        return fam
    return sum(1 for k in RANK_KEYS if _rank(row, k) is not None)


def _raw_action(row: Mapping[str, Any]) -> str:
    return str(row.get("action_type") or "")


def _entry(row: Mapping[str, Any]) -> str:
    return str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")


def _auction_type(row: Mapping[str, Any]) -> str:
    return str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "GENERAL_WATCH")


def _regime_label(shaped_v72: Mapping[str, Any]) -> str:
    meta = shaped_v72.get("meta") if isinstance(shaped_v72.get("meta"), Mapping) else {}
    reg = meta.get("regime") if isinstance(meta.get("regime"), Mapping) else {}
    return str(reg.get("label") or reg.get("regime") or meta.get("regime_label") or "normal")


def _market_budget(shaped_v72: Mapping[str, Any], cfg: Mapping[str, Any], max_candidates: int) -> Tuple[int, float]:
    regime = _regime_label(shaped_v72)
    if regime == "cold":
        return min(max_candidates, int(cfg.get("max_buy_cold", 1))), float(cfg.get("buy_score_cold", 78))
    if "cold" in regime or "warming" in regime:
        return min(max_candidates, int(cfg.get("max_buy_warming", 3))), float(cfg.get("buy_score_warming", 73))
    if regime == "hot":
        return min(max_candidates, int(cfg.get("max_buy_hot", cfg.get("max_buy_normal", 4)))), float(cfg.get("buy_score_hot", cfg.get("buy_score_normal", 70)))
    return min(max_candidates, int(cfg.get("max_buy_normal", 4))), float(cfg.get("buy_score_normal", 70))


def _metrics(row: Mapping[str, Any]) -> Dict[str, float]:
    return {
        "pct": float(auction_pct(row) if auction_pct(row) is not None else 0.0),
        "auction": float(_metric(row, "auction_strength", 0.0) or 0.0),
        "amount": float(_metric(row, "auction_amount_wan", 0.0) or 0.0),
        "liquidity": float(_metric(row, "liquidity_score", 50.0) or 50.0),
        "source": float(_metric(row, "source_evidence_score", 0.0) or 0.0),
        "theme": float(_metric(row, "theme_strength_t0", 0.0) or 0.0),
        "hotness": float(_metric(row, "hotness_score", 0.0) or 0.0),
        "net_pressure": float(_metric(row, "net_pressure", 0.0) or 0.0),
        "risk_mult": float(_metric(row, "risk_multiplier", 1.0) or 1.0),
        "trad_mult": float(_metric(row, "tradability_multiplier", 1.0) or 1.0),
    }


def _cost_quality(pct: float, pattern: str) -> float:
    if pattern == "LOW_OPEN_NET_REVERSAL":
        if -5.8 <= pct <= -1.0:
            return 94.0
        if -8.2 <= pct < -5.8:
            return 64.0
        if -1.0 < pct <= -0.3:
            return 56.0
        return 18.0
    if pattern == "CONFIRMED_MOMENTUM":
        if 1.2 <= pct <= 3.8:
            return 90.0
        if 3.8 < pct <= 5.2:
            return 63.0
        if 0.2 <= pct < 1.2:
            return 55.0
        return 18.0
    if pattern == "AUCTION_FOLLOW_THROUGH":
        if 1.8 <= pct <= 4.6:
            return 88.0
        if 4.6 < pct <= 5.8:
            return 58.0
        return 22.0
    if pattern == "THEME_AUCTION_CONFIRMED":
        if -0.8 <= pct <= 2.2:
            return 90.0
        if 2.2 < pct <= 3.5:
            return 58.0
        return 22.0
    return 35.0


def _confirmation_quality(row: Mapping[str, Any]) -> float:
    m = _metrics(row)
    family = _source_count(row)
    best = _best_rank(row)
    primary = _best_rank(row, PRIMARY_RANK_KEYS)
    rank_score = 0.0
    if best <= 10:
        rank_score = 34.0
    elif best <= 20:
        rank_score = 25.0
    elif best <= 40:
        rank_score = 15.0
    elif best <= 80:
        rank_score = 8.0
    primary_bonus = 14.0 if primary <= 30 else (7.0 if primary <= 60 else 0.0)
    return min(100.0, m["source"] * 0.55 + family * 9.0 + rank_score + primary_bonus)


def _amount_quality(amount_wan: float, liquidity: float, cfg: Mapping[str, Any]) -> float:
    full = float(cfg.get("amount_quality_full_wan", 9000))
    amount_score = min(100.0, max(0.0, amount_wan) / max(full, 1.0) * 100.0)
    return min(100.0, amount_score * 0.62 + liquidity * 0.38)


def _risk_quality(row: Mapping[str, Any]) -> float:
    m = _metrics(row)
    base = 100.0 * max(0.0, min(1.2, m["risk_mult"])) * max(0.0, min(1.2, m["trad_mult"]))
    if _entry(row) == "low_liquidity_confirm":
        base -= 16
    if _auction_type(row) == "HEALTHY_DIVERGENCE":
        base += 4
    if _auction_type(row) == "FAKE_STRENGTH":
        base -= 45
    return max(0.0, min(100.0, base))


def _has_primary_support(row: Mapping[str, Any], max_rank: int) -> bool:
    return _best_rank(row, PRIMARY_RANK_KEYS) <= max_rank


def _has_any_order_support(row: Mapping[str, Any], max_rank: int) -> bool:
    m = _metrics(row)
    return _source_count(row) >= 2 or m["source"] >= 10 or _best_rank(row) <= max_rank


def _base_pattern(row: Mapping[str, Any]) -> str:
    action = _raw_action(row)
    atype = _auction_type(row)
    if action == "LOW_OPEN_REVERSAL" or atype == "LOW_OPEN_REVERSAL":
        return "LOW_OPEN_REVERSAL"
    if action == "MOMENTUM_CATCHUP":
        return "MOMENTUM_CATCHUP"
    if action == "AUCTION_FOLLOW":
        return "AUCTION_FOLLOW"
    if action == "THEME_CATCHUP":
        return "THEME_CATCHUP"
    return action or "NONE"


def _hard_reject(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Optional[str]:
    m = _metrics(row)
    if row.get("risk_penalty") == 0:
        return "hard_risk"
    if _entry(row) == "avoid" or _auction_type(row) == "FAKE_STRENGTH" or _raw_action(row) == "AVOID":
        return "fake_strength_or_avoid"
    if _entry(row) == "board_watch" or _auction_type(row) == "BOARD_LOCK_WATCH" or _raw_action(row) == "BOARD_WATCH":
        return "board_lock"
    if m["pct"] >= float(cfg.get("absolute_max_cost_pct", 7.0)):
        return "cost_too_high"
    if m["amount"] < float(cfg.get("hard_min_amount_wan", 300)):
        return "amount_too_small"
    if m["liquidity"] < float(cfg.get("hard_min_liquidity", 20)):
        return "liquidity_too_weak"
    return None


def _durable_pattern(row: Mapping[str, Any], shaped_v72: Mapping[str, Any], cfg: Mapping[str, Any]) -> Tuple[str, Optional[str]]:
    hard = _hard_reject(row, cfg)
    if hard:
        return "AVOID", hard
    m = _metrics(row)
    pct = m["pct"]
    base = _base_pattern(row)
    coldish = "cold" in _regime_label(shaped_v72) or "warming" in _regime_label(shaped_v72)

    if base == "LOW_OPEN_REVERSAL":
        if not (float(cfg.get("reversal_pct_min", -8.2)) <= pct <= float(cfg.get("reversal_pct_max", -0.5))):
            return "LOW_OPEN_WATCH", "reversal_cost_not_discounted"
        if m["amount"] < float(cfg.get("reversal_min_amount_wan", 3200)):
            return "LOW_OPEN_WATCH", "reversal_amount_too_small"
        if m["auction"] < float(cfg.get("reversal_min_auction", 22)):
            return "LOW_OPEN_WATCH", "reversal_auction_too_weak"
        if not (_has_primary_support(row, int(cfg.get("reversal_primary_rank_max", 60))) or m["net_pressure"] > 0 or m["source"] >= float(cfg.get("reversal_min_source_evidence", 8))):
            return "LOW_OPEN_WATCH", "reversal_no_primary_order_support"
        if pct < float(cfg.get("deep_reversal_pct", -6.5)) and (_confirmation_quality(row) < float(cfg.get("deep_reversal_min_confirmation", 48)) or m["amount"] < float(cfg.get("deep_reversal_min_amount_wan", 8000))):
            return "LOW_OPEN_WATCH", "deep_reversal_confirmation_not_enough"
        return "LOW_OPEN_NET_REVERSAL", None

    if base == "MOMENTUM_CATCHUP":
        if not (float(cfg.get("momentum_min_pct", 1.2)) <= pct <= float(cfg.get("momentum_max_pct", 5.2))):
            return "MOMENTUM_WATCH", "momentum_cost_bad"
        if m["amount"] < float(cfg.get("momentum_min_amount_wan", 2500)):
            return "MOMENTUM_WATCH", "momentum_amount_too_small"
        if m["auction"] < float(cfg.get("momentum_min_auction", 50)) or m["liquidity"] < float(cfg.get("momentum_min_liquidity", 45)):
            return "MOMENTUM_WATCH", "momentum_strength_or_liquidity_weak"
        if not _has_any_order_support(row, int(cfg.get("momentum_rank_max", 60))):
            return "MOMENTUM_WATCH", "momentum_no_independent_order_support"
        if coldish and _confirmation_quality(row) < float(cfg.get("cold_momentum_min_confirmation", 52)):
            return "MOMENTUM_WATCH", "coldish_momentum_confirmation_not_enough"
        return "CONFIRMED_MOMENTUM", None

    if base == "AUCTION_FOLLOW":
        if not (float(cfg.get("follow_min_pct", 1.8)) <= pct <= float(cfg.get("follow_max_pct", 5.8))):
            return "FOLLOW_WATCH", "follow_cost_bad"
        if m["amount"] < float(cfg.get("follow_min_amount_wan", 3000)) or m["auction"] < float(cfg.get("follow_min_auction", 55)):
            return "FOLLOW_WATCH", "follow_amount_or_strength_weak"
        if not (_source_count(row) >= int(cfg.get("follow_min_source_count", 2)) or _best_rank(row) <= int(cfg.get("follow_best_rank_max", 20))):
            return "FOLLOW_WATCH", "follow_lacks_multi_source_confirmation"
        return "AUCTION_FOLLOW_THROUGH", None

    if base == "THEME_CATCHUP":
        if m["theme"] < float(cfg.get("theme_buy_min_theme", 82)):
            return "THEME_WATCH", "theme_strength_not_enough"
        if not (float(cfg.get("theme_buy_min_pct", -0.8)) <= pct <= float(cfg.get("theme_buy_max_pct", 3.5))):
            return "THEME_WATCH", "theme_cost_bad"
        if m["amount"] < float(cfg.get("theme_buy_min_amount_wan", 2800)):
            return "THEME_WATCH", "theme_amount_too_small"
        if _confirmation_quality(row) < float(cfg.get("theme_buy_min_confirmation", 50)):
            return "THEME_WATCH", "theme_lacks_auction_confirmation"
        return "THEME_AUCTION_CONFIRMED", None

    return "REJECT", "not_durable_pattern"


def _rank_prior(rows: List[Mapping[str, Any]]) -> Dict[str, int]:
    def score(row: Mapping[str, Any]) -> float:
        m = _metrics(row)
        cost = max(0.0, 7.0 - max(0.0, m["pct"] - 1.0)) * 2.0
        return m["auction"] * 0.35 + m["theme"] * 0.18 + m["source"] * 0.22 + min(100.0, m["amount"] / 5000 * 100.0) * 0.15 + cost
    ordered = sorted(rows, key=score, reverse=True)
    return {str(r.get("code") or ""): i for i, r in enumerate(ordered, start=1)}


def _edge(row: Mapping[str, Any], pattern: str, prior_rank: int, cfg: Mapping[str, Any]) -> Tuple[float, Dict[str, float], List[str]]:
    m = _metrics(row)
    cost = _cost_quality(m["pct"], pattern)
    confirm = _confirmation_quality(row)
    amount = _amount_quality(m["amount"], m["liquidity"], cfg)
    risk = _risk_quality(row)
    auction = min(100.0, m["auction"])
    prior = max(0.0, 100.0 - max(0, prior_rank - 1) * float(cfg.get("rank_prior_decay_points", 2.5)))
    bonus = {"LOW_OPEN_NET_REVERSAL": 7.0, "CONFIRMED_MOMENTUM": 5.0, "AUCTION_FOLLOW_THROUGH": 3.0, "THEME_AUCTION_CONFIRMED": 2.0, "LOW_OPEN_WATCH": -8.0, "MOMENTUM_WATCH": -8.0, "FOLLOW_WATCH": -10.0, "THEME_WATCH": -10.0}.get(pattern, -25.0)
    score = cost * 0.24 + confirm * 0.28 + amount * 0.18 + risk * 0.16 + auction * 0.08 + prior * 0.06 + bonus
    if pattern == "THEME_AUCTION_CONFIRMED" and confirm < 58:
        score -= 5
    if pattern == "CONFIRMED_MOMENTUM" and m["pct"] > 4.6:
        score -= (m["pct"] - 4.6) * 5
    if pattern == "LOW_OPEN_NET_REVERSAL" and m["pct"] < -6.5:
        score -= 4
    components = {"cost_quality": round(cost, 2), "confirmation_quality": round(confirm, 2), "amount_quality": round(amount, 2), "risk_quality": round(risk, 2), "auction_strength": round(auction, 2), "rank_prior": round(prior, 2)}
    reasons = [pattern, f"cost={round(cost,1)}", f"confirm={round(confirm,1)}", f"amount={round(amount,1)}", f"risk={round(risk,1)}"]
    return round(max(-100.0, min(100.0, score)), 2), components, reasons


def _clone_with_decision(row: Mapping[str, Any], action: str, reason: str) -> Dict[str, Any]:
    out = dict(row)
    tags = list(dict.fromkeys(out.get("action_tags") or []))
    for tag in (action.lower(), reason, str(out.get("durable_pattern") or "")):
        if tag and tag not in tags:
            tags.append(tag)
    pct = auction_pct(out)
    if pct is not None:
        out["auction_pct"] = pct
    out.update(action_type=action, action_quality=action.lower(), signal_quality=action.lower(), action_reason=f"{action}:{reason}", action_tags=tags, action_priority=ACTION_PRIORITY.get(action, 999), action_score=out.get("edge_score", out.get("action_score")))
    return out


def _compact(row: Mapping[str, Any]) -> Dict[str, Any]:
    theme = _theme_detail(row)
    out = {
        "code": row.get("code"), "name": row.get("name"),
        "action_type": row.get("action_type"), "action_score": row.get("action_score"), "action_reason": row.get("action_reason"),
        "durable_pattern": row.get("durable_pattern"), "gate_reason": row.get("gate_reason"),
        "edge_score": row.get("edge_score"), "conviction_score": row.get("conviction_score"), "edge_components": row.get("edge_components"),
        "auction_pct": auction_pct(row), "auction_strength": _metric(row, "auction_strength", 0.0), "auction_amount_wan": _metric(row, "auction_amount_wan", 0.0),
        "theme_strength_t0": _metric(row, "theme_strength_t0", 0.0), "hotness_score": _metric(row, "hotness_score", None),
        "source_evidence_score": _metric(row, "source_evidence_score", 0.0), "source_family_count": _source_count(row),
        "qiangchou_920_925_rank": _rank(row, "qiangchou_920_925_rank"), "qiangchou_last_second_rank": _rank(row, "qiangchou_last_second_rank"),
        "matched_plate": theme.get("matched_plate"), "matched_tags": theme.get("matched_tags") or [],
        "performance": _perf(row),
    }
    return {k: v for k, v in out.items() if v is not None}


def _perf(row: Mapping[str, Any]) -> Dict[str, Any]:
    src = row.get("derived_performance") if isinstance(row.get("derived_performance"), Mapping) else row.get("performance") if isinstance(row.get("performance"), Mapping) else {}
    out: Dict[str, Any] = {}
    for key in ("auction_pct", "open_pct", "close_pct", "excess_return", "dailyline_found", "prev_close", "day_open", "day_high", "day_low", "day_close"):
        if src.get(key) is not None:
            out[key] = src.get(key)
    if "auction_pct" not in out and auction_pct(row) is not None:
        out["auction_pct"] = auction_pct(row)
    return out


def _stats(rows: Iterable[Mapping[str, Any]], key: str) -> Dict[str, int]:
    c: Counter[str] = Counter()
    for row in rows:
        c[str(row.get(key) or "none")] += 1
    return dict(c)


def _performance_stats(rows: List[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for action in sorted({str(r.get("action_type") or "none") for r in rows}, key=lambda a: ACTION_PRIORITY.get(a, 999)):
        vals = [_f(_perf(r).get("excess_return"), None) for r in rows if r.get("action_type") == action]
        vals = [float(x) for x in vals if x is not None]
        out[action] = {"count": sum(1 for r in rows if r.get("action_type") == action), "with_performance": len(vals), "avg_excess_return": round(sum(vals) / len(vals), 2) if vals else None, "med_excess_return": round(median(vals), 2) if vals else None, "positive_excess_count": sum(1 for x in vals if x > 0), "negative_excess_count": sum(1 for x in vals if x < 0)}
    return out


def _diagnostics(rows: List[Mapping[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    watch_winners: List[Dict[str, Any]] = []
    reject_winners: List[Dict[str, Any]] = []
    buy_losers: List[Dict[str, Any]] = []
    for row in rows:
        ex = _f(_perf(row).get("excess_return"), None)
        if ex is None:
            continue
        item = _compact(row)
        if row.get("action_type") == WATCH and ex >= 5:
            item["diagnostic"] = "watch_winner"; watch_winners.append(item)
        if row.get("action_type") == REJECT and ex >= 5:
            item["diagnostic"] = "reject_winner"; reject_winners.append(item)
        if row.get("action_type") == BUY and ex <= -3:
            item["diagnostic"] = "buy_loser"; buy_losers.append(item)
    key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
    watch_winners.sort(key=key, reverse=True)
    reject_winners.sort(key=key, reverse=True)
    buy_losers.sort(key=key)
    return {"watch_winners": watch_winners[:30], "reject_winners": reject_winners[:30], "buy_losers": buy_losers[:30]}


def build_v8_output(shaped_v72: Mapping[str, Any], cfg: Mapping[str, Any], max_candidates: int = 4, watch_tier_max: int = 12, pool_max: int = 8) -> Dict[str, Any]:
    source_rows = list(shaped_v72.get("all_candidates_action_ranked") or shaped_v72.get("all_candidates_debug") or [])
    prior = _rank_prior(source_rows)
    evaluated: List[Dict[str, Any]] = []
    for raw in source_rows:
        row = dict(raw)
        row["pre_v8_action_type"] = row.get("action_type")
        pattern, gate = _durable_pattern(row, shaped_v72, cfg)
        edge, components, reasons = _edge(row, pattern, prior.get(str(row.get("code") or ""), 999), cfg)
        row.update(durable_pattern=pattern, gate_reason=gate, edge_score=edge, conviction_score=edge, expected_return_score=edge, edge_components=components, edge_reasons=reasons, expected_rank_prior=prior.get(str(row.get("code") or ""), 999))
        evaluated.append(row)

    evaluated.sort(key=lambda r: (float(r.get("edge_score") or -999), -int(r.get("expected_rank_prior") or 999)), reverse=True)
    max_buy, threshold = _market_budget(shaped_v72, cfg, max_candidates)
    caps = {"LOW_OPEN_NET_REVERSAL": int(cfg.get("cap_reversal", 2)), "CONFIRMED_MOMENTUM": int(cfg.get("cap_momentum", 1)), "AUCTION_FOLLOW_THROUGH": int(cfg.get("cap_follow", 1)), "THEME_AUCTION_CONFIRMED": int(cfg.get("cap_theme_confirmed", 1))}
    counts: Counter[str] = Counter()
    buy_codes: set[str] = set()
    rows: List[Dict[str, Any]] = []
    for row in evaluated:
        pattern = str(row.get("durable_pattern") or "")
        if pattern in BUY_PATTERNS and row.get("gate_reason") is None and float(row.get("edge_score") or -999) >= threshold and counts[pattern] < caps.get(pattern, 0) and len(buy_codes) < max_buy:
            buy_codes.add(str(row.get("code") or "")); counts[pattern] += 1; rows.append(_clone_with_decision(row, BUY, pattern))
        else:
            rows.append(row)

    hard_reasons = {"fake_strength_or_avoid", "board_lock", "cost_too_high", "hard_risk", "amount_too_small", "liquidity_too_weak"}
    watch_gap = float(cfg.get("watch_score_gap", 14))
    watch_max = int(cfg.get("watch_max", 8))
    watch_count = 0
    final_rows: List[Dict[str, Any]] = []
    for row in rows:
        if row.get("action_type") == BUY:
            final_rows.append(row); continue
        reason = str(row.get("gate_reason") or row.get("durable_pattern") or "score_too_low")
        pattern = str(row.get("durable_pattern") or "")
        near_buy = pattern in BUY_PATTERNS and float(row.get("edge_score") or -999) >= threshold - watch_gap
        watch_pattern = pattern in {"LOW_OPEN_WATCH", "MOMENTUM_WATCH", "FOLLOW_WATCH", "THEME_WATCH"}
        if reason in hard_reasons:
            final_rows.append(_clone_with_decision(row, AVOID, reason))
        elif watch_count < watch_max and (near_buy or watch_pattern) and float(row.get("edge_score") or -999) >= threshold - watch_gap:
            watch_count += 1; final_rows.append(_clone_with_decision(row, WATCH, reason))
        elif _base_pattern(row) in SOURCE_ACTIONS or pattern in {"REJECT", "AVOID"}:
            final_rows.append(_clone_with_decision(row, REJECT, reason))
        else:
            keep = dict(row); keep.update(action_type="DEBUG", action_priority=ACTION_PRIORITY["DEBUG"]); final_rows.append(keep)

    ranked = sorted(final_rows, key=lambda r: (ACTION_PRIORITY.get(str(r.get("action_type")), 999), -float(r.get("edge_score") or -999), int(r.get("expected_rank_prior") or 999)))
    buy_rows = [r for r in ranked if r.get("action_type") == BUY]
    pools = {name: [_compact(r) for r in ranked if r.get("action_type") == name][:pool_max] for name in (BUY, WATCH, REJECT, AVOID)}
    meta = dict(shaped_v72.get("meta") or {})
    meta.update(selector=VERSION, price_cost_field="auction_change_pct", regime_label=_regime_label(shaped_v72), buy_count=len(buy_rows), buy_threshold=threshold, buy_budget=max_buy, pattern_caps=dict(caps), rules=["Unified v8 output; no v7.3 monkey patch overlay.", "Premarket price/cost uses auction_change_pct only.", "BUY requires mechanism + edge score + market-regime budget + pattern cap.", "No board/sector/exchange hard ban from short samples."])
    return {"version": VERSION, "meta": meta, "setup_stats": dict(shaped_v72.get("setup_stats") or {}), "action_stats": _stats(ranked, "action_type"), "pattern_stats": _stats(ranked, "durable_pattern"), "pool_performance": _performance_stats(ranked), "review_diagnostics": _diagnostics(ranked), "candidate_pools": pools, "top_candidates": buy_rows[:max_candidates], "actionable_candidates": buy_rows[:max_candidates], "watch_tier": ranked[:watch_tier_max], "all_candidates_action_ranked": ranked, "all_candidates_debug": evaluated}
