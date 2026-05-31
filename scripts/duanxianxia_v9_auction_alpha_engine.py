"""v9 pure premarket auction alpha engine.

This module goes back to the actual problem:
use 09:25 auction data itself to discover high win-rate / high payoff candidates.

It does NOT depend on old v7 action_type / setup buckets.  v7.2 is used only as
an upstream data/signal extractor because it already loads captures and builds a
candidate universe.  The final selection logic here is rebuilt from auction
microstructure primitives:

1. auction cost / payoff asymmetry             -> auction_change_pct only
2. order-flow confirmation                     -> qiangchou / net amount / vratio
3. funding intensity                           -> amount, pressure, liquidity
4. orderbook truthfulness / tradability         -> fengdan + risk multipliers
5. theme/hotness as context, not primary alpha  -> only after auction confirms
6. market-regime budget                         -> buy count and thresholds

No latest_change_pct/current quote fields are production inputs.
"""
from __future__ import annotations

from collections import Counter
from statistics import median
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

VERSION = "premarket_v9_pure_auction_alpha"
BUY = "BUY"
WATCH = "WATCH"
REJECT = "REJECT"
AVOID = "AVOID"
ACTION_PRIORITY = {BUY: 1, WATCH: 20, REJECT: 900, AVOID: 950, "DEBUG": 999}
RANK_KEYS = ("qiangchou_920_925_rank", "qiangchou_last_second_rank", "net_amount_rank", "vratio_rank", "fengdan_rank")
PRIMARY_KEYS = ("qiangchou_920_925_rank", "net_amount_rank", "vratio_rank")
ALPHA_TYPES = {
    "REVERSAL_ABSORPTION",
    "SUSTAINED_ORDERFLOW_MOMENTUM",
    "MULTI_SOURCE_AUCTION_FOLLOW",
    "THEME_CONFIRMED_LOW_COST",
    "BROAD_REPAIR_MOMENTUM",
}


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
    for value in (row.get("auction_pct"), row.get("auction_change_pct"), _detail(row).get("auction_change_pct")):
        pct = _f(value, None)
        if pct is not None:
            return pct
    summary = row.get("signal_summary") if isinstance(row.get("signal_summary"), Mapping) else {}
    return _f(summary.get("auction_change_pct"), None)


def _rank(row: Mapping[str, Any], key: str) -> Optional[int]:
    try:
        raw = _detail(row).get(key)
        if raw in (None, "", 0, "0"):
            return None
        return int(float(str(raw).replace(",", "")))
    except Exception:
        return None


def _best_rank(row: Mapping[str, Any], keys: Iterable[str] = RANK_KEYS) -> int:
    vals = [_rank(row, k) for k in keys]
    vals = [v for v in vals if v is not None]
    return min(vals) if vals else 999


def _source_count(row: Mapping[str, Any]) -> int:
    families = _detail(row).get("source_families") or []
    if isinstance(families, list) and families:
        return len([x for x in families if str(x).strip()])
    fam = int(_metric(row, "source_family_count", 0) or 0)
    if fam:
        return fam
    return sum(1 for k in RANK_KEYS if _rank(row, k) is not None)


def _regime_label(shaped: Mapping[str, Any]) -> str:
    meta = shaped.get("meta") if isinstance(shaped.get("meta"), Mapping) else {}
    reg = meta.get("regime") if isinstance(meta.get("regime"), Mapping) else {}
    return str(reg.get("label") or reg.get("regime") or meta.get("regime_label") or "normal")


def _m(row: Mapping[str, Any]) -> Dict[str, float]:
    return {
        "pct": float(auction_pct(row) if auction_pct(row) is not None else 0.0),
        "auction": float(_metric(row, "auction_strength", 0.0) or 0.0),
        "amount": float(_metric(row, "auction_amount_wan", 0.0) or 0.0),
        "liquidity": float(_metric(row, "liquidity_score", 50.0) or 50.0),
        "source": float(_metric(row, "source_evidence_score", 0.0) or 0.0),
        "money": float(_metric(row, "money_intent_score", 0.0) or 0.0),
        "orderbook": float(_metric(row, "orderbook_quality_score", 45.0) or 45.0),
        "resonance": float(_metric(row, "resonance_score", 0.0) or 0.0),
        "theme": float(_metric(row, "theme_strength_t0", 0.0) or 0.0),
        "hotness": float(_metric(row, "hotness_score", 0.0) or 0.0),
        "net_pressure": float(_metric(row, "net_pressure", 0.0) or 0.0),
        "risk_mult": float(_metric(row, "risk_multiplier", 1.0) or 1.0),
        "trad_mult": float(_metric(row, "tradability_multiplier", 1.0) or 1.0),
    }


def _clip(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _rank_score(rank: int, best: float, mid: float, tail: float) -> float:
    if rank <= 10:
        return best
    if rank <= 30:
        return mid
    if rank <= 80:
        return tail
    return 0.0


def _orderflow(row: Mapping[str, Any]) -> float:
    m = _m(row)
    sustained = _rank(row, "qiangchou_920_925_rank") or 999
    last = _rank(row, "qiangchou_last_second_rank") or 999
    net = _rank(row, "net_amount_rank") or 999
    vratio = _rank(row, "vratio_rank") or 999
    fam = _source_count(row)
    score = (
        _rank_score(sustained, 34, 25, 10)
        + _rank_score(net, 27, 20, 9)
        + _rank_score(vratio, 20, 14, 7)
        + _rank_score(last, 12, 8, 3)
        + min(18, fam * 6)
        + min(18, m["source"] * 0.25)
    )
    return _clip(score)


def _funding(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> float:
    m = _m(row)
    amount_full = float(cfg.get("amount_quality_full_wan", 9000))
    amount = min(100.0, max(0.0, m["amount"]) / max(amount_full, 1.0) * 100.0)
    pressure = _clip(max(0.0, m["net_pressure"]) / max(float(cfg.get("net_pressure_full_ratio", 0.002)), 1e-9) * 100.0)
    return _clip(amount * 0.42 + m["money"] * 0.24 + m["liquidity"] * 0.22 + pressure * 0.12)


def _cost_curve(pct: float, alpha_type: str) -> float:
    if alpha_type == "REVERSAL_ABSORPTION":
        if -5.8 <= pct <= -1.0:
            return 96
        if -8.2 <= pct < -5.8:
            return 66
        if -1.0 < pct <= -0.3:
            return 58
        return 15
    if alpha_type == "BROAD_REPAIR_MOMENTUM":
        if -1.2 <= pct <= 3.8:
            return 90
        if 3.8 < pct <= 5.2:
            return 62
        return 22
    if alpha_type == "THEME_CONFIRMED_LOW_COST":
        if -0.8 <= pct <= 2.2:
            return 92
        if 2.2 < pct <= 3.5:
            return 60
        return 20
    if 1.0 <= pct <= 3.8:
        return 90
    if 3.8 < pct <= 5.3:
        return 62
    if 0.0 <= pct < 1.0:
        return 58
    return 20


def _truth(row: Mapping[str, Any]) -> float:
    m = _m(row)
    entry = str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")
    atype = str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "GENERAL_WATCH")
    score = 100.0 * max(0.0, min(1.2, m["risk_mult"])) * max(0.0, min(1.2, m["trad_mult"]))
    if entry == "avoid" or atype == "FAKE_STRENGTH":
        score -= 60
    if entry == "board_watch" or atype == "BOARD_LOCK_WATCH":
        score -= 30
    if entry == "low_liquidity_confirm":
        score -= 18
    return _clip(score)


def _context(row: Mapping[str, Any]) -> float:
    m = _m(row)
    # Theme/hotness are context, not the core alpha. They can lift a confirmed
    # auction signal, but cannot replace order-flow/funding/truth.
    return _clip(m["theme"] * 0.55 + m["hotness"] * 0.25 + m["resonance"] * 0.20)


def _hard_block(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Optional[str]:
    m = _m(row)
    entry = str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")
    atype = str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "GENERAL_WATCH")
    if row.get("risk_penalty") == 0:
        return "hard_risk"
    if entry == "avoid" or atype == "FAKE_STRENGTH":
        return "fake_strength_or_avoid"
    if entry == "board_watch" or atype == "BOARD_LOCK_WATCH":
        return "board_lock_not_alpha"
    if m["pct"] >= float(cfg.get("absolute_max_cost_pct", 7.0)):
        return "cost_too_high"
    if m["amount"] < float(cfg.get("hard_min_amount_wan", 300)):
        return "amount_too_small"
    if m["liquidity"] < float(cfg.get("hard_min_liquidity", 20)):
        return "liquidity_too_weak"
    return None


def _detect_alpha(row: Mapping[str, Any], shaped: Mapping[str, Any], cfg: Mapping[str, Any]) -> Tuple[str, Optional[str]]:
    hard = _hard_block(row, cfg)
    if hard:
        return "AVOID", hard
    m = _m(row)
    pct = m["pct"]
    order = _orderflow(row)
    fund = _funding(row, cfg)
    truth = _truth(row)
    ctx = _context(row)
    primary_rank = _best_rank(row, PRIMARY_KEYS)
    atype = str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "")
    coldish = "cold" in _regime_label(shaped) or "warming" in _regime_label(shaped)

    if pct < 0:
        if not (float(cfg.get("reversal_pct_min", -8.5)) <= pct <= float(cfg.get("reversal_pct_max", -0.4))):
            return "REVERSAL_WATCH", "discount_not_in_payoff_band"
        if fund < float(cfg.get("reversal_min_funding", 42)) or order < float(cfg.get("reversal_min_orderflow", 38)):
            return "REVERSAL_WATCH", "discount_without_absorption"
        if pct < float(cfg.get("deep_reversal_pct", -6.5)) and (fund < float(cfg.get("deep_reversal_min_funding", 60)) or order < float(cfg.get("deep_reversal_min_orderflow", 55))):
            return "REVERSAL_WATCH", "deep_discount_absorption_not_enough"
        return "REVERSAL_ABSORPTION", None

    if m["theme"] >= float(cfg.get("broad_repair_min_theme", 75)) and ctx >= float(cfg.get("broad_repair_min_context", 58)) and order >= float(cfg.get("broad_repair_min_orderflow", 45)) and fund >= float(cfg.get("broad_repair_min_funding", 45)) and float(cfg.get("broad_repair_min_pct", -0.5)) <= pct <= float(cfg.get("broad_repair_max_pct", 5.2)):
        return "BROAD_REPAIR_MOMENTUM", None

    if order >= float(cfg.get("sustained_min_orderflow", 58)) and fund >= float(cfg.get("sustained_min_funding", 46)) and truth >= float(cfg.get("sustained_min_truth", 68)) and float(cfg.get("sustained_min_pct", 1.0)) <= pct <= float(cfg.get("sustained_max_pct", 5.2)):
        if coldish and order < float(cfg.get("cold_sustained_min_orderflow", 66)):
            return "MOMENTUM_WATCH", "coldish_orderflow_not_enough"
        return "SUSTAINED_ORDERFLOW_MOMENTUM", None

    if _source_count(row) >= int(cfg.get("follow_min_source_count", 2)) and primary_rank <= int(cfg.get("follow_primary_rank_max", 40)) and fund >= float(cfg.get("follow_min_funding", 44)) and float(cfg.get("follow_min_pct", 1.5)) <= pct <= float(cfg.get("follow_max_pct", 5.8)):
        return "MULTI_SOURCE_AUCTION_FOLLOW", None

    if m["theme"] >= float(cfg.get("theme_min_strength", 82)) and ctx >= float(cfg.get("theme_min_context", 58)) and order >= float(cfg.get("theme_min_orderflow", 42)) and fund >= float(cfg.get("theme_min_funding", 42)) and float(cfg.get("theme_min_pct", -0.8)) <= pct <= float(cfg.get("theme_max_pct", 3.5)):
        return "THEME_CONFIRMED_LOW_COST", None

    if order >= float(cfg.get("watch_min_orderflow", 35)) or fund >= float(cfg.get("watch_min_funding", 42)) or atype in {"HEALTHY_DIVERGENCE", "SUSTAINED_PLUS_LAST_SECOND"}:
        return "AUCTION_WATCH", "auction_evidence_incomplete"
    return "REJECT", "no_auction_alpha"


def _edge(row: Mapping[str, Any], alpha: str, cfg: Mapping[str, Any], prior_rank: int) -> Tuple[float, Dict[str, float], List[str]]:
    m = _m(row)
    order = _orderflow(row)
    fund = _funding(row, cfg)
    cost = _cost_curve(m["pct"], alpha)
    truth = _truth(row)
    ctx = _context(row)
    prior = max(0.0, 100.0 - max(0, prior_rank - 1) * float(cfg.get("rank_prior_decay_points", 2.0)))
    bonus = {"REVERSAL_ABSORPTION": 7, "SUSTAINED_ORDERFLOW_MOMENTUM": 6, "MULTI_SOURCE_AUCTION_FOLLOW": 4, "THEME_CONFIRMED_LOW_COST": 3, "BROAD_REPAIR_MOMENTUM": 5, "AUCTION_WATCH": -8}.get(alpha, -25)
    score = cost * 0.23 + order * 0.27 + fund * 0.20 + truth * 0.16 + ctx * 0.08 + prior * 0.06 + bonus
    if alpha == "THEME_CONFIRMED_LOW_COST" and order < 50:
        score -= 5
    if alpha == "SUSTAINED_ORDERFLOW_MOMENTUM" and m["pct"] > 4.6:
        score -= (m["pct"] - 4.6) * 5
    components = {"cost_payoff": round(cost, 2), "orderflow": round(order, 2), "funding": round(fund, 2), "truth_tradability": round(truth, 2), "context": round(ctx, 2), "rank_prior": round(prior, 2)}
    reasons = [alpha, f"pct={round(m['pct'],2)}", f"order={round(order,1)}", f"fund={round(fund,1)}", f"truth={round(truth,1)}"]
    return round(_clip(score, -100, 100), 2), components, reasons


def _prior(rows: List[Mapping[str, Any]], cfg: Mapping[str, Any]) -> Dict[str, int]:
    def score(row: Mapping[str, Any]) -> float:
        m = _m(row)
        return _orderflow(row) * 0.34 + _funding(row, cfg) * 0.28 + _truth(row) * 0.18 + _context(row) * 0.10 + _cost_curve(m["pct"], "SUSTAINED_ORDERFLOW_MOMENTUM") * 0.10
    ordered = sorted(rows, key=score, reverse=True)
    return {str(r.get("code") or ""): i for i, r in enumerate(ordered, start=1)}


def _budget(shaped: Mapping[str, Any], cfg: Mapping[str, Any], max_candidates: int) -> Tuple[int, float]:
    label = _regime_label(shaped)
    if label == "cold":
        return min(max_candidates, int(cfg.get("max_buy_cold", 1))), float(cfg.get("buy_score_cold", 80))
    if "cold" in label or "warming" in label:
        return min(max_candidates, int(cfg.get("max_buy_warming", 3))), float(cfg.get("buy_score_warming", 74))
    return min(max_candidates, int(cfg.get("max_buy_normal", 4))), float(cfg.get("buy_score_normal", 71))


def _clone(row: Mapping[str, Any], action: str, reason: str) -> Dict[str, Any]:
    out = dict(row)
    tags = list(dict.fromkeys(out.get("action_tags") or []))
    for tag in (action.lower(), reason, str(out.get("alpha_type") or "")):
        if tag and tag not in tags:
            tags.append(tag)
    pct = auction_pct(out)
    if pct is not None:
        out["auction_pct"] = pct
    out.update(action_type=action, action_quality=action.lower(), signal_quality=action.lower(), action_reason=f"{action}:{reason}", action_tags=tags, action_priority=ACTION_PRIORITY.get(action, 999), action_score=out.get("edge_score"))
    return out


def _perf(row: Mapping[str, Any]) -> Dict[str, Any]:
    src = row.get("derived_performance") if isinstance(row.get("derived_performance"), Mapping) else row.get("performance") if isinstance(row.get("performance"), Mapping) else {}
    out: Dict[str, Any] = {}
    for key in ("auction_pct", "open_pct", "close_pct", "excess_return", "dailyline_found", "prev_close", "day_open", "day_high", "day_low", "day_close"):
        if src.get(key) is not None:
            out[key] = src.get(key)
    if "auction_pct" not in out and auction_pct(row) is not None:
        out["auction_pct"] = auction_pct(row)
    return out


def _compact(row: Mapping[str, Any]) -> Dict[str, Any]:
    theme = _theme_detail(row)
    out = {
        "code": row.get("code"), "name": row.get("name"), "action_type": row.get("action_type"), "action_score": row.get("action_score"), "action_reason": row.get("action_reason"),
        "alpha_type": row.get("alpha_type"), "gate_reason": row.get("gate_reason"), "edge_score": row.get("edge_score"), "edge_components": row.get("edge_components"),
        "auction_pct": auction_pct(row), "auction_strength": _metric(row, "auction_strength", 0.0), "auction_amount_wan": _metric(row, "auction_amount_wan", 0.0),
        "theme_strength_t0": _metric(row, "theme_strength_t0", 0.0), "hotness_score": _metric(row, "hotness_score", None), "source_evidence_score": _metric(row, "source_evidence_score", 0.0),
        "source_family_count": _source_count(row), "orderflow_score": row.get("edge_components", {}).get("orderflow") if isinstance(row.get("edge_components"), Mapping) else None,
        "funding_score": row.get("edge_components", {}).get("funding") if isinstance(row.get("edge_components"), Mapping) else None,
        "qiangchou_920_925_rank": _rank(row, "qiangchou_920_925_rank"), "qiangchou_last_second_rank": _rank(row, "qiangchou_last_second_rank"), "net_amount_rank": _rank(row, "net_amount_rank"),
        "matched_plate": theme.get("matched_plate"), "matched_tags": theme.get("matched_tags") or [], "performance": _perf(row),
    }
    return {k: v for k, v in out.items() if v is not None}


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
    watch_winners.sort(key=key, reverse=True); reject_winners.sort(key=key, reverse=True); buy_losers.sort(key=key)
    return {"watch_winners": watch_winners[:30], "reject_winners": reject_winners[:30], "buy_losers": buy_losers[:30]}


def build_v9_output(shaped_v72: Mapping[str, Any], cfg: Mapping[str, Any], max_candidates: int = 4, watch_tier_max: int = 12, pool_max: int = 8) -> Dict[str, Any]:
    source_rows = list(shaped_v72.get("all_candidates_debug") or shaped_v72.get("all_candidates_action_ranked") or [])
    prior = _prior(source_rows, cfg)
    evaluated: List[Dict[str, Any]] = []
    for raw in source_rows:
        row = dict(raw)
        row["pre_v9_action_type"] = row.get("action_type")
        alpha, gate = _detect_alpha(row, shaped_v72, cfg)
        edge, components, reasons = _edge(row, alpha, cfg, prior.get(str(row.get("code") or ""), 999))
        row.update(alpha_type=alpha, gate_reason=gate, edge_score=edge, conviction_score=edge, expected_return_score=edge, edge_components=components, edge_reasons=reasons, expected_rank_prior=prior.get(str(row.get("code") or ""), 999))
        evaluated.append(row)

    evaluated.sort(key=lambda r: (float(r.get("edge_score") or -999), -int(r.get("expected_rank_prior") or 999)), reverse=True)
    max_buy, threshold = _budget(shaped_v72, cfg, max_candidates)
    caps = {"REVERSAL_ABSORPTION": int(cfg.get("cap_reversal", 2)), "SUSTAINED_ORDERFLOW_MOMENTUM": int(cfg.get("cap_sustained", 1)), "MULTI_SOURCE_AUCTION_FOLLOW": int(cfg.get("cap_follow", 1)), "THEME_CONFIRMED_LOW_COST": int(cfg.get("cap_theme", 1)), "BROAD_REPAIR_MOMENTUM": int(cfg.get("cap_broad_repair", 1))}
    counts: Counter[str] = Counter()
    buy_codes: set[str] = set()
    first_pass: List[Dict[str, Any]] = []
    for row in evaluated:
        alpha = str(row.get("alpha_type") or "")
        if alpha in ALPHA_TYPES and row.get("gate_reason") is None and float(row.get("edge_score") or -999) >= threshold and counts[alpha] < caps.get(alpha, 0) and len(buy_codes) < max_buy:
            buy_codes.add(str(row.get("code") or "")); counts[alpha] += 1; first_pass.append(_clone(row, BUY, alpha))
        else:
            first_pass.append(row)

    hard = {"fake_strength_or_avoid", "board_lock_not_alpha", "cost_too_high", "hard_risk", "amount_too_small", "liquidity_too_weak"}
    watch_gap = float(cfg.get("watch_score_gap", 14))
    watch_max = int(cfg.get("watch_max", 8))
    watch_count = 0
    final: List[Dict[str, Any]] = []
    for row in first_pass:
        if row.get("action_type") == BUY:
            final.append(row); continue
        alpha = str(row.get("alpha_type") or "")
        reason = str(row.get("gate_reason") or alpha or "score_too_low")
        if reason in hard or alpha == "AVOID":
            final.append(_clone(row, AVOID, reason))
        elif watch_count < watch_max and (alpha in ALPHA_TYPES or alpha.endswith("WATCH") or alpha == "AUCTION_WATCH") and float(row.get("edge_score") or -999) >= threshold - watch_gap:
            watch_count += 1; final.append(_clone(row, WATCH, reason))
        elif alpha in {"REJECT", "AVOID"} or alpha.endswith("WATCH") or alpha == "AUCTION_WATCH" or alpha in ALPHA_TYPES:
            final.append(_clone(row, REJECT, reason))
        else:
            keep = dict(row); keep.update(action_type="DEBUG", action_priority=ACTION_PRIORITY["DEBUG"]); final.append(keep)

    ranked = sorted(final, key=lambda r: (ACTION_PRIORITY.get(str(r.get("action_type")), 999), -float(r.get("edge_score") or -999), int(r.get("expected_rank_prior") or 999)))
    buys = [r for r in ranked if r.get("action_type") == BUY]
    pools = {name: [_compact(r) for r in ranked if r.get("action_type") == name][:pool_max] for name in (BUY, WATCH, REJECT, AVOID)}
    meta = dict(shaped_v72.get("meta") or {})
    meta.update(selector=VERSION, price_cost_field="auction_change_pct", regime_label=_regime_label(shaped_v72), buy_count=len(buys), buy_threshold=threshold, buy_budget=max_buy, alpha_caps=dict(caps), rules=["Pure auction alpha engine; final decisions do not depend on old v7 action buckets.", "Premarket price/cost uses auction_change_pct only.", "Theme/hotness are context only; order-flow + funding + tradability are primary.", "BUY requires alpha type + edge score + market budget + alpha cap.", "No board/sector/exchange hard ban from short samples."])
    return {"version": VERSION, "meta": meta, "setup_stats": dict(shaped_v72.get("setup_stats") or {}), "action_stats": _stats(ranked, "action_type"), "alpha_stats": _stats(ranked, "alpha_type"), "pool_performance": _performance_stats(ranked), "review_diagnostics": _diagnostics(ranked), "candidate_pools": pools, "top_candidates": buys[:max_candidates], "actionable_candidates": buys[:max_candidates], "watch_tier": ranked[:watch_tier_max], "all_candidates_action_ranked": ranked, "all_candidates_debug": evaluated}
