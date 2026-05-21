"""v7.3 practical portfolio overlay.

This patch replaces the previous strict-gate overlay after the 2026-05-19/20/21
re-runs showed a key failure mode: the BUY list collapsed into a few synthetic
broad-repair/debug names while ignoring the stronger ranked auction evidence.

Principles
----------
1. Do not buy DEBUG_ONLY/no-setup names by default.
2. Use the original v7.3 expected-return proxy as a rank prior; it was more
   stable than the hand-built conviction gate in the available reviews.
3. Convert only a small, diversified set of evidence-backed rows into BUY.
4. Keep broad repair for recall, but cap it and require exceptional evidence.
5. Preserve full review/debug output, but make the executable list explicit.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import duanxianxia_v7_3_output as v73

_APPLIED = False

BUY_ACTION = "HIGH_CONVICTION_BUY"
WATCH_ACTION = "QUALITY_WATCH"
AVOID_ACTION = "STRUCTURAL_AVOID"
BROAD_ACTION = "BROAD_REPAIR_MOMENTUM"

SOURCE_RANK_KEYS = (
    "qiangchou_920_925_rank",
    "qiangchou_last_second_rank",
    "vratio_rank",
    "net_amount_rank",
    "fengdan_rank",
)
PRIMARY_ACTIONS = {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "LOW_OPEN_REVERSAL", "THEME_CATCHUP"}


def _num(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if v in (None, "", "None", "null", "NULL", "-"):
            return default
        return float(str(v).replace("%", "").replace(",", "").strip())
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
    v = _detail(row).get(key)
    try:
        if v in (None, "", 0, "0"):
            return None
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


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
    }


def _regime_label(shaped: Dict[str, Any], row: Optional[Dict[str, Any]] = None) -> str:
    meta = shaped.get("meta") or {}
    reg = meta.get("regime") if isinstance(meta.get("regime"), dict) else {}
    label = str((reg or {}).get("label") or (row or {}).get("regime") or "normal")
    return label


def _source_count(row: Dict[str, Any]) -> int:
    detail = _detail(row)
    fam = detail.get("source_families") or []
    if isinstance(fam, list) and fam:
        return len([x for x in fam if str(x).strip()])
    return int(_m(row)["family"] or 0) or sum(1 for k in SOURCE_RANK_KEYS if _rank(row, k) is not None)


def _has_rank(row: Dict[str, Any], max_rank: int = 60) -> bool:
    return any((_rank(row, k) is not None and (_rank(row, k) or 999) <= max_rank) for k in SOURCE_RANK_KEYS)


def _has_source(row: Dict[str, Any], max_rank: int = 60) -> bool:
    m = _m(row)
    return m["source"] >= 6 or _source_count(row) >= 1 or _has_rank(row, max_rank)


def _fatal(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[str]:
    m = _m(row)
    auction_type = str(row.get("auction_setup_type") or _detail(row).get("auction_setup_type") or "")
    entry_tag = str(row.get("entry_tag") or _detail(row).get("entry_tag") or "normal")
    if row.get("risk_penalty") == 0:
        return "hard_risk_kill"
    if entry_tag == "avoid" or auction_type == "FAKE_STRENGTH":
        return "fake_strength_or_avoid_entry"
    if entry_tag == "board_watch" or auction_type == "BOARD_LOCK_WATCH":
        return "board_lock_not_excess_return_trade"
    if m["pct"] >= float(cfg.get("buy_hard_max_pct", 7.2)):
        return "auction_cost_too_high"
    return None


def _cost_fit(action: str, pct: float) -> float:
    if action == "LOW_OPEN_REVERSAL":
        if -5.8 <= pct <= -0.3:
            return 16.0
        if -8.8 <= pct < -5.8:
            return 7.0
        return -18.0
    if action == "THEME_CATCHUP":
        if -0.5 <= pct <= 2.4:
            return 13.0
        if 2.4 < pct <= 3.2:
            return 4.0
        return -14.0
    if action == BROAD_ACTION:
        if -1.2 <= pct <= 1.8:
            return 18.0
        if 1.8 < pct <= 2.5:
            return 6.0
        return -20.0
    if 1.4 <= pct <= 5.8:
        return 11.0
    if 0.5 <= pct < 1.4 or 5.8 < pct <= 6.8:
        return 2.0
    return -12.0


def _base_score(row: Dict[str, Any], rank_map: Dict[str, int], cfg: Dict[str, Any]) -> float:
    action = str(row.get("candidate_action_type") or row.get("action_type") or "")
    m = _m(row)
    code = str(row.get("code") or "")
    rank = rank_map.get(code, 999)
    rank_bonus = max(0.0, 45.0 - min(rank, 80) * 0.55)
    action_bonus = {
        "MOMENTUM_CATCHUP": 14.0,
        "LOW_OPEN_REVERSAL": 13.0,
        "AUCTION_FOLLOW": 11.0,
        "THEME_CATCHUP": 7.0,
        BROAD_ACTION: 2.0,
    }.get(action, 0.0)
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    quality_bonus = {"momentum": 8, "repair": 7, "main_attack": 5, "strong": 4, "medium": -3, "weak": -14, "broad_repair": -6}.get(quality, 0)
    amount_score = min(14.0, m["amount"] / float(cfg.get("amount_full_wan", 5000)) * 14.0)
    auction_score = min(13.0, m["auction"] * 0.18)
    source_score = min(10.0, m["source"] * 0.18 + _source_count(row) * 2.0)
    liq_score = min(6.0, m["liquidity"] * 0.06)
    score = rank_bonus + action_bonus + quality_bonus + _cost_fit(action, m["pct"]) + amount_score + auction_score + source_score + liq_score
    if m["pct"] > float(cfg.get("buy_soft_max_pct", 6.4)):
        score -= (m["pct"] - float(cfg.get("buy_soft_max_pct", 6.4))) * 6.0
    if str(row.get("setup_v72") or "none") == "none" and action != BROAD_ACTION:
        score -= 10.0
    return round(v73._clamp(score, -100, 100), 2)


def _eligible_reason(row: Dict[str, Any], cfg: Dict[str, Any], shaped: Dict[str, Any]) -> Optional[str]:
    fatal = _fatal(row, cfg)
    if fatal:
        return fatal
    action = str(row.get("candidate_action_type") or row.get("action_type") or "")
    m = _m(row)
    quality = str(row.get("signal_quality") or row.get("action_quality") or "")
    cold = "cold" in _regime_label(shaped, row)

    if action == "AUCTION_FOLLOW":
        if not (1.8 <= m["pct"] <= 6.8):
            return "auction_follow_cost_window_fail"
        if m["auction"] < 45 or m["amount"] < 1000 or not _has_source(row, 60):
            return "auction_follow_evidence_fail"
        return None

    if action == "MOMENTUM_CATCHUP":
        if not (1.4 <= m["pct"] <= 6.2):
            return "momentum_cost_window_fail"
        if m["auction"] < 48 or m["amount"] < 1200 or m["liquidity"] < 45:
            return "momentum_evidence_fail"
        if cold and not _has_source(row, 80) and m["amount"] < 2500:
            return "cold_momentum_needs_source_or_amount"
        return None

    if action == "LOW_OPEN_REVERSAL":
        if not (-8.8 <= m["pct"] <= -0.2):
            return "low_open_cost_window_fail"
        if m["auction"] < 20 or m["amount"] < (4200 if m["pct"] < -5.8 else 2200):
            return "low_open_support_not_enough"
        if not (_rank(row, "net_amount_rank") is not None or _rank(row, "qiangchou_920_925_rank") is not None or m["source"] >= 10):
            return "low_open_missing_real_support"
        return None

    if action == "THEME_CATCHUP":
        if quality != "strong":
            return "theme_not_strong_quality"
        if not (-0.5 <= m["pct"] <= 2.8):
            return "theme_cost_window_fail"
        if m["amount"] < 1800 or m["theme"] < 80:
            return "theme_amount_or_strength_fail"
        if not (_has_source(row, 80) or m["auction"] >= 42):
            return "theme_lacks_auction_confirmation"
        return None

    if action == BROAD_ACTION:
        # Broad repair is useful for recall, but the last overlay bought too many
        # synthetic no-setup names.  It can only be a tiny optional slot.
        if not bool(cfg.get("broad_repair_buy_enabled", False)):
            return "broad_repair_buy_disabled"
        if not (-1.2 <= m["pct"] <= 2.0):
            return "broad_repair_cost_window_fail"
        min_amt = 9000 if cold else 6000
        min_auc = 42 if cold else 34
        if m["amount"] < min_amt or m["auction"] < min_auc or m["liquidity"] < 55:
            return "broad_repair_needs_exceptional_amount_auction"
        return None

    return "not_primary_action"


def _make_broad_candidate(row: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(row.get("action_type")) != "DEBUG_ONLY":
        return None
    if str(row.get("setup_v72") or "none") != "none" or str(row.get("confidence") or "none") != "none":
        return None
    m = _m(row)
    if not (-1.5 <= m["pct"] <= 2.5):
        return None
    if m["theme"] > float(cfg.get("broad_repair_theme_max", 20)):
        return None
    if m["source"] > float(cfg.get("broad_repair_source_max", 1.0)) or m["family"] > float(cfg.get("broad_repair_family_max", 1)):
        return None
    if m["auction"] < float(cfg.get("broad_repair_min_auction_strength", 18)) or m["amount"] < float(cfg.get("broad_repair_min_amount_wan", 1500)):
        return None
    out = dict(row)
    tags = _tags(out)
    _add(tags, "broad_repair_recall", "watch_first")
    out.update(candidate_action_type=BROAD_ACTION, action_tags=tags, broad_repair_candidate=True)
    return out


def _mark_buy(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    original = str(out.get("candidate_action_type") or out.get("action_type") or "")
    tags = _tags(out)
    _add(tags, "high_conviction_buy", f"source_action:{original}")
    out.update(
        pre_gate_action_type=original,
        action_type=BUY_ACTION,
        action_quality=str(out.get("signal_quality") or out.get("action_quality") or "buy"),
        signal_quality=str(out.get("signal_quality") or out.get("action_quality") or "buy"),
        action_reason=f"BUY:{original}:{out.get('action_reason') or ''}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(BUY_ACTION, 1),
    )
    return out


def _mark_watch(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    out = dict(row)
    original = str(out.get("candidate_action_type") or out.get("action_type") or "")
    tags = _tags(out)
    _add(tags, "quality_watch", reason)
    out.update(
        pre_gate_action_type=original,
        action_type=WATCH_ACTION,
        action_quality="watch_only",
        signal_quality="watch_only",
        action_reason=f"quality_watch:{original}:{reason}",
        action_tags=tags,
        action_priority=v73.ACTION_PRIORITY.get(WATCH_ACTION, 900),
    )
    return out


def _mark_avoid(row: Dict[str, Any], reason: str) -> Dict[str, Any]:
    out = dict(row)
    tags = _tags(out)
    _add(tags, "structural_avoid", reason)
    out.update(action_type=AVOID_ACTION, action_quality="avoid", signal_quality="avoid", action_reason=f"structural_avoid:{reason}", action_tags=tags, action_priority=v73.ACTION_PRIORITY.get(AVOID_ACTION, 950))
    return out


def _select(rows: List[Dict[str, Any]], shaped: Dict[str, Any], cfg: Dict[str, Any], max_candidates: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    expected_order = v73._sort_expected_return_proxy(rows)
    rank_map = {str(r.get("code") or ""): i for i, r in enumerate(expected_order, start=1)}

    pool: List[Dict[str, Any]] = []
    for r in rows:
        base = dict(r)
        if str(base.get("action_type")) in PRIMARY_ACTIONS:
            base["candidate_action_type"] = base.get("action_type")
            pool.append(base)
        broad = _make_broad_candidate(r, cfg)
        if broad is not None:
            pool.append(broad)

    evaluated: List[Dict[str, Any]] = []
    for r in pool:
        score = _base_score(r, rank_map, cfg)
        reason = _eligible_reason(r, cfg, shaped)
        rr = dict(r)
        rr["conviction_score"] = score
        rr["expected_return_score"] = score
        rr["quality_gate_reason"] = reason
        evaluated.append(rr)

    cold = "cold" in _regime_label(shaped)
    buy_bar = float(cfg.get("portfolio_buy_min_score_cold" if cold else "portfolio_buy_min_score", 62 if cold else 58))
    evaluated.sort(key=lambda x: (float(x.get("conviction_score") or -999), -rank_map.get(str(x.get("code") or ""), 999)), reverse=True)

    selected: List[Dict[str, Any]] = []
    type_counts: Dict[str, int] = {}
    max_by_type = {
        "AUCTION_FOLLOW": int(cfg.get("max_auction_follow", 2)),
        "MOMENTUM_CATCHUP": int(cfg.get("max_momentum", 2)),
        "LOW_OPEN_REVERSAL": int(cfg.get("max_low_open", 2)),
        "THEME_CATCHUP": int(cfg.get("max_theme", 1)),
        BROAD_ACTION: int(cfg.get("max_broad_repair_buy", 0)),
    }
    for r in evaluated:
        typ = str(r.get("candidate_action_type") or r.get("action_type"))
        if r.get("quality_gate_reason") is not None:
            continue
        if float(r.get("conviction_score") or -999) < buy_bar:
            continue
        if type_counts.get(typ, 0) >= max_by_type.get(typ, 1):
            continue
        selected.append(_mark_buy(r))
        type_counts[typ] = type_counts.get(typ, 0) + 1
        if len(selected) >= max_candidates:
            break

    selected_codes = {str(r.get("code") or "") for r in selected}
    rebuilt: List[Dict[str, Any]] = []
    eval_by_code_action = {(str(r.get("code") or ""), str(r.get("candidate_action_type") or r.get("action_type") or "")): r for r in evaluated}
    for r in rows:
        code = str(r.get("code") or "")
        if code in selected_codes:
            rebuilt.append(next(x for x in selected if str(x.get("code") or "") == code))
            continue
        fatal = _fatal(r, cfg)
        if fatal and str(r.get("action_type")) in {"AVOID", "FAKE_STRENGTH_WATCH", "BOARD_WATCH"}:
            rebuilt.append(_mark_avoid(r, fatal))
            continue
        if str(r.get("action_type")) in PRIMARY_ACTIONS:
            ev = eval_by_code_action.get((code, str(r.get("action_type"))))
            reason = str((ev or {}).get("quality_gate_reason") or "score_below_buy_bar")
            wr = dict(ev or r)
            rebuilt.append(_mark_watch(wr, reason))
        else:
            # Do not silently promote debug/no-setup rows.  They remain debug/review rows.
            r2 = dict(r)
            r2["action_priority"] = v73.ACTION_PRIORITY.get(str(r2.get("action_type")), 999)
            rebuilt.append(r2)
    return selected, rebuilt


def apply() -> None:
    global _APPLIED
    if _APPLIED:
        return
    _APPLIED = True

    base_upgrade = v73._upgrade_row
    base_pools = v73._pools
    base_diagnostics = v73._diagnostics

    v73.ACTION_PRIORITY.update({BUY_ACTION: 1, WATCH_ACTION: 900, AVOID_ACTION: 950, BROAD_ACTION: 880})
    v73.ACTIONABLE.clear()
    v73.ACTIONABLE.add(BUY_ACTION)
    v73.NON_ACTIONABLE_WATCH.update({WATCH_ACTION, AVOID_ACTION, BROAD_ACTION})

    def action_sort(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: (0 if r.get("action_type") == BUY_ACTION else 1, -float(r.get("conviction_score") or r.get("expected_return_score") or -999), int(r.get("action_priority") or 999), -float(r.get("action_score") or 0)))

    def pools(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
        out = base_pools(rows, pool_max)
        ranked = action_sort(rows)
        out["selective_buy_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == BUY_ACTION][:pool_max]
        out["quality_watch_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == WATCH_ACTION][:pool_max]
        out["structural_avoid_pool"] = [v73._compact(r) for r in ranked if r.get("action_type") == AVOID_ACTION][:pool_max]
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
                c["diagnostic"] = "quality_watch_rejected_winner"; rejected_winners.append(c)
            if r.get("action_type") == BUY_ACTION and ex <= -3:
                c["diagnostic"] = "selective_buy_false_positive"; buy_false.append(c)
        key = lambda x: float((x.get("performance") or {}).get("excess_return") or 0)
        rejected_winners.sort(key=key, reverse=True)
        buy_false.sort(key=key)
        out["quality_gate_rejected_winners"] = rejected_winners[:30]
        out["selective_buy_false_positives"] = buy_false[:30]
        return out

    def rebuild(shaped: Dict[str, Any], rows: List[Dict[str, Any]], cfg: Dict[str, Any], max_candidates: int, watch_tier_max: int, pool_max: int) -> Dict[str, Any]:
        selected, rebuilt_rows = _select(rows, shaped, cfg, max_candidates)
        ranked = action_sort(rebuilt_rows)
        expected = action_sort(rebuilt_rows)
        legacy = v73._sort_score(rebuilt_rows)
        meta = dict(shaped.get("meta") or {})
        notes = list(meta.get("interpretation_notes") or [])
        for note in [
            "v7.3 portfolio overlay: executable BUY is selected from evidence-backed original action rows, not from debug/no-setup rows.",
            "Broad repair remains a recall/watch concept; it is capped and disabled as a default BUY source after latest false-positive runs.",
            "conviction_score uses premarket fields plus expected-return rank prior; realized close/excess return is review-only.",
        ]:
            if note not in notes:
                notes.append(note)
        meta["interpretation_notes"] = notes
        meta["portfolio_buy_count"] = len(selected)
        meta["portfolio_mode"] = "rank_prior_evidence_portfolio"
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
            "expected_return_watch_tier": expected[:watch_tier_max],
            "legacy_top_candidates": [r for r in legacy if r.get("setup_v72") != "none"][:max_candidates],
            "all_candidates_action_ranked": ranked,
            "all_candidates_expected_return_ranked": expected,
            "all_candidates_debug": legacy,
            "intraday_anchors": v73.v72.build_intraday_anchors_v72(selected[:20]),
        }

    def upgrade_shaped_v72_to_v73(shaped: Dict[str, Any], action_config: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, pool_max: int = 15) -> Dict[str, Any]:
        cfg = action_config or {}
        source = shaped.get("all_candidates_action_ranked") or shaped.get("all_candidates_debug") or []
        base_rows = [base_upgrade(r, cfg) for r in source]
        return rebuild(shaped, base_rows, cfg, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)

    def shape_v7_3_output(decisions: List[Dict[str, Any]], meta: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, action_config: Optional[Dict[str, Any]] = None, pool_max: int = 15) -> Dict[str, Any]:
        base = v73.v72.shape_v7_2_output(decisions, meta=meta, max_candidates=max_candidates, watch_tier_max=watch_tier_max, action_config=action_config)
        return upgrade_shaped_v72_to_v73(base, action_config=action_config, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)

    v73._sort_action = action_sort
    v73._pools = pools
    v73._diagnostics = diagnostics
    v73.upgrade_shaped_v72_to_v73 = upgrade_shaped_v72_to_v73
    v73.shape_v7_3_output = shape_v7_3_output


apply()
