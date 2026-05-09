"""v7.3 action-pool output upgrade.

Formalizes the action-pool report so the trading view is no longer one mixed
Top30.  Production classification still uses only premarket-visible fields; any
close_pct/excess_return fields are used only for review diagnostics.

Important review-mode rule:
- The premarket runner cannot know close_pct/excess_return at 09:25.
- A later review bundle may backfill those fields into flat CSV/JSONL.
- When that happens, v7.3 source JSON must be recomputed so pool_performance and
  review_diagnostics are not left empty.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional

import duanxianxia_v7_2_output as v72

VERSION = "premarket_v7_3"
ACTION_PRIORITY = {
    "AUCTION_FOLLOW": 10,
    "MOMENTUM_CATCHUP": 15,
    "THEME_CATCHUP": 20,
    "LOW_OPEN_REVERSAL": 30,
    "BOARD_WATCH": 40,
    "CONFIRMATION_WATCH": 80,
    "AVOID": 99,
    "DEBUG_ONLY": 999,
}
ACTIONABLE = {"AUCTION_FOLLOW", "MOMENTUM_CATCHUP", "THEME_CATCHUP", "LOW_OPEN_REVERSAL", "BOARD_WATCH"}
PERFORMANCE_KEYS = ("auction_pct", "open_pct", "close_pct", "excess_return", "dailyline_found", "prev_close", "day_open", "day_high", "day_low", "day_close")


def _f(v: Any, default: Optional[float] = 0.0) -> Optional[float]:
    return v72._f(v, default)


def _detail(d: Dict[str, Any]) -> Dict[str, Any]:
    return d.get("auction_detail") or {}


def _metric(d: Dict[str, Any], key: str, default: Optional[float] = 0.0) -> Optional[float]:
    if key in d:
        return _f(d.get(key), default)
    a = _detail(d)
    if key in a:
        return _f(a.get(key), default)
    s = d.get("signal_summary") or {}
    if key in s:
        return _f(s.get(key), default)
    return default


def _auction_pct(d: Dict[str, Any]) -> Optional[float]:
    if d.get("auction_pct") is not None:
        return _f(d.get("auction_pct"), None)
    perf = d.get("derived_performance") or d.get("performance") or {}
    if isinstance(perf, dict) and perf.get("auction_pct") is not None:
        return _f(perf.get("auction_pct"), None)
    return _metric(d, "latest_change_pct", None)


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _confidence(score: float, high: float = 65.0, mid: float = 45.0) -> str:
    if score >= high:
        return "high"
    if score >= mid:
        return "medium"
    return "low"


def _theme_aux_score(d: Dict[str, Any], cfg: Dict[str, Any]) -> float:
    auction = float(_metric(d, "auction_strength", 0.0) or 0.0)
    hot = float(_metric(d, "hotness_score", 0.0) or 0.0)
    liq = float(_metric(d, "liquidity_score", 50.0) or 50.0)
    src = float(_metric(d, "source_evidence_score", 0.0) or 0.0)
    fam = int(_metric(d, "source_family_count", 0) or 0)
    amt = float(_metric(d, "auction_amount_wan", 0.0) or 0.0)
    entry = str(d.get("entry_tag") or _detail(d).get("entry_tag") or "normal")
    aux = min(25, auction * 0.35) + min(20, hot * 0.20) + min(15, src * 0.60) + min(15, fam * 5.0) + min(15, amt / 5000 * 15) + min(10, liq * 0.10)
    if entry == "low_liquidity_confirm":
        aux -= 18
    return _clamp(aux)


def _theme_quality(aux: float, cfg: Dict[str, Any]) -> str:
    if aux >= float(cfg.get("theme_catchup_strong_min_aux_score", 35)):
        return "strong"
    if aux <= float(cfg.get("theme_catchup_weak_max_aux_score", 12)):
        return "weak"
    return "medium"


def _perf(d: Dict[str, Any]) -> Dict[str, Any]:
    src = d.get("derived_performance") or d.get("performance") or d
    out: Dict[str, Any] = {}
    if isinstance(src, dict):
        for k in PERFORMANCE_KEYS:
            if src.get(k) is not None:
                out[k] = src.get(k)
    if "auction_pct" not in out:
        pct = _auction_pct(d)
        if pct is not None:
            out["auction_pct"] = pct
    return out


def _compact(d: Dict[str, Any]) -> Dict[str, Any]:
    out = v72._compact_decision(d)
    out["action_quality"] = d.get("action_quality")
    out["action_priority"] = d.get("action_priority")
    p = _perf(d)
    if p:
        out["performance"] = p
    return out


def _upgrade_row(d: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(d)
    action = str(out.get("action_type") or "CONFIRMATION_WATCH")
    reason = str(out.get("action_reason") or "")
    setup = str(out.get("setup_v72") or "none")
    conf = str(out.get("confidence") or "none")
    pct = _auction_pct(out)
    auction = float(_metric(out, "auction_strength", 0.0) or 0.0)
    liq = float(_metric(out, "liquidity_score", 50.0) or 50.0)
    amt = float(_metric(out, "auction_amount_wan", 0.0) or 0.0)
    src = float(_metric(out, "source_evidence_score", 0.0) or 0.0)
    theme = float(_metric(out, "theme_strength_t0", 0.0) or 0.0)
    tags = list(out.get("action_tags") or [])

    if action == "CONFIRMATION_WATCH" and setup == "none" and conf == "none" and (out.get("final_score") or 0) == 0 and "not_selected" in reason:
        out.update(action_type="DEBUG_ONLY", action_confidence="none", action_score=0.0, action_reason="not_selected_debug_universe_only", action_quality="debug")
    elif action == "CONFIRMATION_WATCH":
        lo = float(cfg.get("momentum_pct_min", 2.0)); hi = float(cfg.get("momentum_pct_max", 5.8))
        min_auc = float(cfg.get("momentum_min_auction_strength", 50)); min_liq = float(cfg.get("momentum_min_liquidity_score", 55)); min_amt = float(cfg.get("momentum_min_amount_wan", 1000))
        if pct is not None and lo <= pct <= hi and auction >= min_auc and liq >= min_liq and amt >= min_amt:
            score = _clamp(0.46 * auction + 0.18 * liq + 0.14 * min(100, amt / 5000 * 100) + 0.10 * max(src, theme * 0.25))
            if src < float(cfg.get("follow_min_source_evidence", 18)) and "incomplete_source_evidence" not in tags:
                tags.append("incomplete_source_evidence")
            out.update(action_type="MOMENTUM_CATCHUP", action_confidence=_confidence(score, 58, 42), action_score=round(score, 2), action_reason="strong_auction_momentum_incomplete_theme_or_source", action_quality="momentum", action_tags=tags)
        else:
            out.setdefault("action_quality", "watch")
    elif action == "THEME_CATCHUP":
        aux = _theme_aux_score(out, cfg)
        quality = _theme_quality(aux, cfg)
        if quality == "strong" and "theme_aux_strong" not in tags:
            tags.append("theme_aux_strong")
        elif quality == "weak" and "theme_aux_weak" not in tags:
            tags.append("theme_aux_weak")
            out["action_score"] = round(_clamp(float(out.get("action_score") or 0) - 12), 2)
        out.update(action_quality=quality, action_tags=tags)
    elif action == "BOARD_WATCH":
        out.setdefault("action_quality", "watch_only")
    elif action == "LOW_OPEN_REVERSAL":
        out.setdefault("action_quality", "repair")
    elif action == "AUCTION_FOLLOW":
        out.setdefault("action_quality", "main_attack")
    elif action == "AVOID":
        out.setdefault("action_quality", "avoid")
    elif action == "DEBUG_ONLY":
        out.setdefault("action_quality", "debug")

    out["action_priority"] = ACTION_PRIORITY.get(str(out.get("action_type")), 999)
    return out


def _sort_action(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda x: (int(x.get("action_priority") or 999), -float(x.get("action_score") or 0), -float(x.get("final_score") or 0)))


def _sort_score(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda x: x.get("final_score") or 0, reverse=True)


def _stats(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        k = str(r.get("action_type") or "DEBUG_ONLY")
        out[k] = out.get(k, 0) + 1
    return out


def _quality_stats(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        k = f"{r.get('action_type') or 'DEBUG_ONLY'}:{r.get('action_quality') or 'standard'}"
        out[k] = out.get(k, 0) + 1
    return out


def _nums(rows: Iterable[Dict[str, Any]], key: str) -> List[float]:
    vals = []
    for r in rows:
        v = _f(_perf(r).get(key), None)
        if v is not None:
            vals.append(float(v))
    return vals


def _stat_block(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    c = _nums(rows, "close_pct"); e = _nums(rows, "excess_return")
    if not c and not e:
        return {"count": len(rows), "with_performance": 0}
    return {"count": len(rows), "with_performance": max(len(c), len(e)), "avg_close_pct": round(sum(c)/len(c), 2) if c else None, "med_close_pct": round(median(c), 2) if c else None, "avg_excess_return": round(sum(e)/len(e), 2) if e else None, "med_excess_return": round(median(e), 2) if e else None, "positive_excess_count": sum(1 for x in e if x > 0), "negative_excess_count": sum(1 for x in e if x < 0)}


def _performance_stats(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {a: _stat_block([r for r in rows if r.get("action_type") == a]) for a in sorted({str(r.get("action_type") or "DEBUG_ONLY") for r in rows}, key=lambda a: ACTION_PRIORITY.get(a, 999))}


def _diagnostics(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    missed, false_pos = [], []
    for r in rows:
        ex = _f(_perf(r).get("excess_return"), None)
        if ex is None:
            continue
        c = _compact(r)
        if r.get("action_type") in {"CONFIRMATION_WATCH", "DEBUG_ONLY"} and ex >= 8:
            c["diagnostic"] = "missed_winner_candidate"; missed.append(c)
        if r.get("action_type") in ACTIONABLE and ex <= -3:
            c["diagnostic"] = "actionable_false_positive"; false_pos.append(c)
    missed.sort(key=lambda x: float((x.get("performance") or {}).get("excess_return") or 0), reverse=True)
    false_pos.sort(key=lambda x: float((x.get("performance") or {}).get("excess_return") or 0))
    return {"missed_winners": missed[:30], "false_positives": false_pos[:30]}


def _pools(rows: List[Dict[str, Any]], pool_max: int) -> Dict[str, List[Dict[str, Any]]]:
    ranked = _sort_action(rows); legacy = _sort_score(rows)
    spec = {
        "main_attack_pool": lambda r: r.get("action_type") == "AUCTION_FOLLOW",
        "momentum_catchup_pool": lambda r: r.get("action_type") == "MOMENTUM_CATCHUP",
        "theme_rotation_pool": lambda r: r.get("setup_v72") == "T0-ROTATE",
        "theme_catchup_pool": lambda r: r.get("action_type") == "THEME_CATCHUP",
        "low_open_reversal_pool": lambda r: r.get("action_type") == "LOW_OPEN_REVERSAL",
        "board_watch_pool": lambda r: r.get("action_type") == "BOARD_WATCH",
        "confirmation_watch_pool": lambda r: r.get("action_type") == "CONFIRMATION_WATCH",
        "avoid_or_risk_pool": lambda r: r.get("action_type") == "AVOID",
        "debug_only_pool": lambda r: r.get("action_type") == "DEBUG_ONLY",
    }
    out: Dict[str, List[Dict[str, Any]]] = {}
    for name, pred in spec.items():
        source = legacy if name == "theme_rotation_pool" else ranked
        out[name] = [_compact(r) for r in source if pred(r)][:pool_max]
    return out


def _rebuild_v73(shaped: Dict[str, Any], rows: List[Dict[str, Any]], max_candidates: int, watch_tier_max: int, pool_max: int) -> Dict[str, Any]:
    ranked = _sort_action(rows)
    legacy = _sort_score(rows)
    actionable = [r for r in ranked if r.get("action_type") in ACTIONABLE]
    meta = dict(shaped.get("meta") or {})
    notes = list(meta.get("interpretation_notes") or [])
    required_notes = [
        "v7.3 is action-pool first: use candidate_pools/actionable_candidates as the trading view, not the legacy mixed rank.",
        "DEBUG_ONLY rows are retained only for debug/review and are excluded from trading pools.",
        "MOMENTUM_CATCHUP is separated from AUCTION_FOLLOW because it has price momentum but incomplete theme/source evidence.",
    ]
    for note in required_notes:
        if note not in notes:
            notes.append(note)
    meta["interpretation_notes"] = notes
    return {"version": VERSION, "meta": meta, "setup_stats": shaped.get("setup_stats") or v72.setup_stats_v72(rows), "action_stats": _stats(rows), "action_quality_stats": _quality_stats(rows), "pool_performance": _performance_stats(rows), "review_diagnostics": _diagnostics(rows), "candidate_pools": _pools(rows, pool_max), "top_candidates": actionable[:max_candidates], "actionable_candidates": actionable[:max_candidates], "legacy_top_candidates": [r for r in legacy if r.get("setup_v72") != "none"][:max_candidates], "watch_tier": ranked[:watch_tier_max], "all_candidates_action_ranked": ranked, "all_candidates_debug": legacy, "intraday_anchors": v72.build_intraday_anchors_v72(actionable[:20])}


def upgrade_shaped_v72_to_v73(shaped: Dict[str, Any], action_config: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, pool_max: int = 15) -> Dict[str, Any]:
    cfg = action_config or {}
    source = shaped.get("all_candidates_action_ranked") or shaped.get("all_candidates_debug") or []
    rows = [_upgrade_row(r, cfg) for r in source]
    return _rebuild_v73(shaped, rows, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)


def _code_key(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def _coerce_perf_value(key: str, value: Any) -> Any:
    if value in (None, "", "None", "null", "NULL"):
        return None
    if key == "dailyline_found":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}
    parsed = _f(value, None)
    return parsed if parsed is not None else value


def load_performance_map_from_flat(path: str | Path) -> Dict[str, Dict[str, Any]]:
    """Load code -> performance fields from a flat CSV or JSONL review export."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"missing performance flat file: {p}")
    rows: List[Dict[str, Any]] = []
    if p.suffix.lower() == ".jsonl":
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.strip():
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
    else:
        with p.open("r", encoding="utf-8", newline="") as fp:
            rows = list(csv.DictReader(fp))
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = _code_key(row.get("code") or row.get("股票代码") or row.get("代码"))
        if not code:
            continue
        perf: Dict[str, Any] = {}
        for key in PERFORMANCE_KEYS:
            if key in row:
                val = _coerce_perf_value(key, row.get(key))
                if val is not None:
                    perf[key] = val
        if perf:
            out[code] = perf
    return out


def attach_performance_to_rows(rows: List[Dict[str, Any]], performance_by_code: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return rows with derived_performance injected by stock code.

    Existing premarket classification fields are preserved.  This function only
    adds realized review fields, then downstream stats/diagnostics are rebuilt.
    """
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        copied = dict(row)
        code = _code_key(copied.get("code") or copied.get("股票代码") or copied.get("代码"))
        perf = dict(copied.get("derived_performance") or copied.get("performance") or {})
        if code in performance_by_code:
            perf.update(performance_by_code[code])
        if perf:
            copied["derived_performance"] = perf
            # Also expose these at row top-level for flat-export compatibility.
            for key in PERFORMANCE_KEYS:
                if perf.get(key) is not None:
                    copied[key] = perf.get(key)
        out.append(copied)
    return out


def recompute_v73_review_metrics(shaped: Dict[str, Any], performance_by_code: Optional[Dict[str, Dict[str, Any]]] = None, action_config: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, pool_max: int = 15) -> Dict[str, Any]:
    """Recompute v7.3 pool_performance/review_diagnostics after review backfill.

    Use this after close_pct/excess_return are available.  It fixes the previous
    failure mode where markdown/flat files had realized returns, but the source
    analysis JSON still showed with_performance=0.
    """
    cfg = action_config or {}
    source = shaped.get("all_candidates_action_ranked") or shaped.get("all_candidates_debug") or []
    rows = attach_performance_to_rows(source, performance_by_code or {})
    rows = [_upgrade_row(r, cfg) for r in rows]
    rebuilt = _rebuild_v73(shaped, rows, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)
    rebuilt.setdefault("meta", {})
    rebuilt["meta"]["review_metrics_recomputed"] = True
    rebuilt["meta"]["review_performance_source"] = "flat_backfill" if performance_by_code else "embedded_rows"
    return rebuilt


def shape_v7_3_output(decisions: List[Dict[str, Any]], meta: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, action_config: Optional[Dict[str, Any]] = None, pool_max: int = 15) -> Dict[str, Any]:
    base = v72.shape_v7_2_output(decisions, meta=meta, max_candidates=max_candidates, watch_tier_max=watch_tier_max, action_config=action_config)
    return upgrade_shaped_v72_to_v73(base, action_config=action_config, max_candidates=max_candidates, watch_tier_max=watch_tier_max, pool_max=pool_max)


def write_v7_3_outputs(output_dir: str, decisions: List[Dict[str, Any]], meta: Optional[Dict[str, Any]] = None, max_candidates: int = 30, watch_tier_max: int = 60, analysis_filename: str = "analysis_v7_3.json", anchors_filename: str = "intraday_anchors.json", action_config: Optional[Dict[str, Any]] = None, pool_max: int = 15) -> Dict[str, str]:
    out_dir = Path(output_dir); out_dir.mkdir(parents=True, exist_ok=True)
    shaped = shape_v7_3_output(decisions, meta=meta, max_candidates=max_candidates, watch_tier_max=watch_tier_max, action_config=action_config, pool_max=pool_max)
    analysis_path = out_dir / analysis_filename; anchors_path = out_dir / anchors_filename
    analysis_path.write_text(json.dumps(shaped, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    anchors_path.write_text(json.dumps(shaped["intraday_anchors"], ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"analysis_path": str(analysis_path), "anchors_path": str(anchors_path)}


def _self_test() -> None:
    rows = [
        {"code":"000001","name":"follow","setup_v72":"T0-ROTATE","confidence":"high","final_score":60,"auction_strength":76,"theme_strength_t0":95,"auction_detail":{"latest_change_pct":5,"source_evidence_score":30,"source_family_count":3,"auction_amount_wan":5000,"liquidity_score":90}},
        {"code":"000002","name":"momentum","setup_v72":"T0-GENERAL","confidence":"low","final_score":40,"auction_strength":55,"theme_strength_t0":20,"auction_detail":{"latest_change_pct":3,"source_evidence_score":0,"auction_amount_wan":3000,"liquidity_score":80}},
        {"code":"000003","name":"debug","setup_v72":"none","confidence":"none","final_score":0,"auction_strength":0,"theme_strength_t0":0},
    ]
    out = shape_v7_3_output(rows, action_config={"momentum_min_amount_wan":1000})
    assert out["version"] == VERSION
    assert out["action_stats"].get("MOMENTUM_CATCHUP") == 1, out["action_stats"]
    assert out["action_stats"].get("DEBUG_ONLY") == 1, out["action_stats"]
    assert "momentum_catchup_pool" in out["candidate_pools"]
    perf = {"000001": {"close_pct": 10, "auction_pct": 5, "excess_return": 5}, "000002": {"close_pct": 20, "auction_pct": 3, "excess_return": 17}}
    recomputed = recompute_v73_review_metrics(out, perf, action_config={"momentum_min_amount_wan":1000})
    assert recomputed["pool_performance"]["AUCTION_FOLLOW"]["with_performance"] == 1, recomputed["pool_performance"]
    assert recomputed["pool_performance"]["MOMENTUM_CATCHUP"]["with_performance"] == 1, recomputed["pool_performance"]
    print("output v7.3 _self_test passed")


if __name__ == "__main__":
    _self_test()
