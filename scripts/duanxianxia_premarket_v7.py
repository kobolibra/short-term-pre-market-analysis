"""
Duanxianxia premarket scoring — v7.0 (setup classifier).

v7 abandons v6's linear-additive score and replaces it with:
  Layer 0  Candidate pool = union of auction.* 4 sources (no scoring filter).
  Layer 1  T-1 single-point labels per candidate:
             industry_t1_label  ∈ {hit_strong:acceleration, hit_strong:absorb_dip,
                                   hit_weak:fade, hit_weak:continuation_weak,
                                   miss:new_entry}
             stock_t1_label     ∈ {hit_top, hit_mid, miss}
             zt_pattern         ∈ {首板, 二板, 三板+, 一字, 烂板, 反包板, 无}
             zt_quality         ∈ {clean, dirty, none}
             lhb_status         ∈ {listed, none}
  Layer 2  T-N cumulative labels:
             cashflow_continuity ∈ {accumulating, distributing, neutral, miss}
             longtou_status      ∈ {confirmed_longtou, mid_position, follower, none}
             theme_history       ∈ {fresh, day1_fermenting, day2_main, day3_high, fading}
                                  (v7.0 stub; backfilled in v7.1 once multi-day window built)
  Layer 3  market_regime         ∈ {cold, normal, hot}  (reuses v6 _classify_regime)

Then 5 mutually-exclusive setups (E > B > A > C > D > none) are evaluated.

Design principle (\"missing == signal\"): when a candidate is absent from
T-1 fupan / cashflow.stock.today / kaipan top 20 we emit explicit miss-style
labels rather than NULL. miss is itself a positive classification for setups
A (cold-market new entry) and D (fade fanbao).

Public API (drop-in compatible with v6 module + batch.py inline analysis):
    VERSION = \"premarket_5table_v7.0\"
    build_premarket_analysis_v7(report, project_root=None, config=None) -> dict
    build_premarket_analysis(report, project_root=None, config=None) -> dict   # alias
    compute_premarket_analysis(report, project_root=None, config=None) -> dict # alias

Output shape (top_candidates entries are stable):
    {\"enabled\": bool, \"version\": str, \"regime\": str,
     \"top_candidates\": [{code, name, setup, setup_priority, labels, anchors,
                            source_hits, source_hit_count, reasons, risks,
                            auction_change_pct, latest_change_pct, score}]}

Side effect: writes <project_root>/reports/<date>/intraday_anchors.json so the
10:01 validator can pick it up without any chat-side state.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore

from duanxianxia_premarket_v6 import (  # type: ignore
    _Candidate,
    _build_theme_canon_map,
    _canonicalize,
    _classify_regime,
    _dataset_meta,
    _dataset_rows,
    _infer_prev_trading_day,
    _is_untradable_v6,
    _merge_candidates,
    _parse_chinese_amount_to_wan,
    _parse_chinese_amount_to_yi,
    _parse_float,
    _parse_int,
    _resolve_prev_trading_day_captures,
    _split_concepts,
    _theme_names_match,
    load_premarket_config,
)

VERSION = "premarket_5table_v7.0"

DEFAULT_V7_CONFIG_PATH = Path("projects/duanxianxia/config/premarket_v7_setups.yaml")

# Setup ordering: priority used for sorting; classifier still iterates explicit list
# below so the precedence is also encoded for human-readability.
SETUP_ORDER: Tuple[str, ...] = ("E", "B", "A", "C", "D")
_DEFAULT_SETUP_PRIORITY: Dict[str, int] = {"E": 5, "B": 4, "A": 3, "C": 2, "D": 1, "none": 0}

_DEFAULT_LABEL_THRESHOLDS: Dict[str, Any] = {
    "stock_t1_label": {"hit_top_max_rank": 50, "hit_mid_max_rank": 150},
    "cashflow_continuity": {"accumulating_rank_max": 75, "distributing_rank_min": 105},
    "longtou_status": {"longtou_min_board": 2, "mid_position_max_seq": 3},
    "industry_t1_label": {"fade_min_count": 5, "acceleration_min_count": 2},
    "zt_quality": {"dirty_min_open_count": 2},
}

_DEFAULT_SETUP_RULES: Dict[str, Dict[str, Any]] = {
    "E": {"conditions": {"zt_pattern_in": ["一字"]}},
    "B": {"conditions": {
        "regime_not_in": ["cold"],
        "industry_t1_label_in": ["hit_strong:acceleration", "hit_strong:absorb_dip"],
        "zt_pattern_in": ["二板", "三板+"],
        "longtou_status_in": ["confirmed_longtou"],
        "cashflow_continuity_in": ["accumulating"],
    }},
    "A": {"conditions": {
        "regime_in": ["cold"],
        "industry_t1_label_in": ["miss:new_entry", "hit_strong:absorb_dip"],
        "zt_pattern_in": ["首板", "二板", "无"],
        "longtou_status_in": ["confirmed_longtou", "mid_position"],
        "source_hit_count_min": 2,
    }},
    "C": {"conditions": {
        "regime_not_in": ["cold"],
        "industry_t1_label_in": ["hit_strong:acceleration", "hit_strong:absorb_dip"],
        "zt_pattern_in": ["无", "首板"],
        "stock_t1_label_in": ["hit_top", "hit_mid"],
        "longtou_status_in": ["follower", "none"],
        "source_hit_count_min": 3,
    }},
    "D": {"conditions": {
        "industry_t1_label_in": ["hit_weak:fade"],
        "zt_pattern_in": ["反包板", "首板"],
        "longtou_status_in": ["follower"],
        "cashflow_continuity_in": ["distributing", "neutral"],
    }},
}

_DEFAULT_ANCHORS: Dict[str, Dict[str, Any]] = {
    "A": {"check_at": "10:00", "conditions": [
        {"type": "price_above_auction", "tolerance_pct": -1.0},
        {"type": "amount_min_yi", "by_time": "10:00", "min_yi": 5.0},
        {"type": "industry_rank_top", "max_rank": 5},
    ]},
    "B": {"check_at": "09:35", "conditions": [
        {"type": "price_above_auction", "tolerance_pct": -0.5},
        {"type": "industry_rank_top", "max_rank": 3},
    ]},
    "C": {"check_at": "09:50", "conditions": [
        {"type": "intraday_uptrend"},
        {"type": "fengdan_min_wan", "min_wan": 3000},
    ]},
    "D": {"check_at": "10:00", "conditions": [
        {"type": "fanbao_complete"},
        {"type": "price_above_auction", "tolerance_pct": -0.5},
    ]},
    "E": {"check_at": "09:30", "conditions": [
        {"type": "miaoban"},
        {"type": "fengdan_min_yi", "min_yi": 1.0},
    ]},
}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def load_v7_config(path: Optional[Path | str] = None,
                   project_root: Optional[Path | str] = None) -> Dict[str, Any]:
    """Load premarket_v7_setups.yaml. Falls back to bundled defaults on missing file."""
    root = Path(project_root) if project_root else Path.cwd()
    cfg_path = Path(path) if path else (root / DEFAULT_V7_CONFIG_PATH)
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()
    if yaml is None or not cfg_path.exists():
        return {
            "version": VERSION,
            "setups": _DEFAULT_SETUP_RULES,
            "setup_priority": _DEFAULT_SETUP_PRIORITY,
            "anchors": _DEFAULT_ANCHORS,
            **_DEFAULT_LABEL_THRESHOLDS,
            "output": {"max_candidates": 50, "emit_setup_none": False,
                        "intraday_anchors_dirname": "reports",
                        "validation_filename": "intraday_validation.json"},
        }
    with cfg_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _cfg_section(cfg: Mapping[str, Any], key: str, default: Any) -> Any:
    val = cfg.get(key) if cfg else None
    return val if val else default


def _norm_code(code: Any) -> str:
    s = str(code or "").strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s
    if len(digits) > 6:
        digits = digits[-6:]
    return digits.zfill(6)


# ---------------------------------------------------------------------------
# Layer 1: T-1 single-point labels
# ---------------------------------------------------------------------------
def compute_stock_t1_label(code: str,
                            today_cashflow_rows: Sequence[Mapping[str, Any]],
                            *, hit_top_max_rank: int = 50,
                            hit_mid_max_rank: int = 150) -> str:
    """rank<=hit_top -> hit_top; rank<=hit_mid -> hit_mid; missing or beyond -> miss."""
    norm = _norm_code(code)
    if not norm or not today_cashflow_rows:
        return "miss"
    for row in today_cashflow_rows:
        rcode = _norm_code(row.get("代码") or row.get("code"))
        if rcode != norm:
            continue
        rank = _parse_int(row.get("排名") or row.get("rank"))
        if rank is None:
            return "miss"
        if rank <= hit_top_max_rank:
            return "hit_top"
        if rank <= hit_mid_max_rank:
            return "hit_mid"
        return "miss"
    return "miss"


def _lookup_fupan_row(code: str,
                       fupan_rows: Sequence[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
    norm = _norm_code(code)
    if not norm or not fupan_rows:
        return None
    for r in fupan_rows:
        if _norm_code(r.get("代码") or r.get("code")) == norm:
            return r
    return None


def compute_zt_pattern(fupan_row: Optional[Mapping[str, Any]]) -> str:
    if not fupan_row:
        return "无"
    zt_type = str(fupan_row.get("涨停类型") or "").strip()
    if "一字" in zt_type:
        return "一字"
    if "反包" in zt_type:
        return "反包板"
    boards = _parse_int(fupan_row.get("连板")) or 0
    if boards >= 3:
        return "三板+"
    if boards == 2:
        return "二板"
    if boards == 1:
        return "首板"
    if "烂" in zt_type:
        return "烂板"
    if str(fupan_row.get("板数") or "").strip():
        return "首板"
    return "无"


def compute_zt_quality(fupan_row: Optional[Mapping[str, Any]],
                        *, dirty_min_open_count: int = 2) -> str:
    if not fupan_row:
        return "none"
    open_count = _parse_int(fupan_row.get("开板")) or 0
    if open_count >= dirty_min_open_count:
        return "dirty"
    return "clean"


def compute_lhb_status(fupan_row: Optional[Mapping[str, Any]]) -> str:
    if not fupan_row:
        return "none"
    raw = str(fupan_row.get("龙虎榜") or "").strip()
    if raw and raw not in {"-", "--", "无"}:
        return "listed"
    return "none"


# ---------------------------------------------------------------------------
# Layer 1/2: industry & longtou
# ---------------------------------------------------------------------------
def compute_industry_t1_label(cand_industries: Sequence[str],
                                fupan_rows: Sequence[Mapping[str, Any]],
                                canon_map: Mapping[str, str],
                                *, fade_min_count: int = 5,
                                acceleration_min_count: int = 2) -> str:
    """Resolve industry_t1_label.

    Note: T-1 industry-level cashflow is not reliably available (only top-20
    snapshots from kaipan). We approximate using:
      - cand has NO matching T-0 main industry → miss:new_entry (cold setup eligible)
      - cand industry matched in T-1 fupan with high zt count → hit_weak:fade (拥挤)
      - cand industry matched with moderate count → hit_strong:acceleration
      - cand industry matched with single occurrence → hit_strong:absorb_dip
    """
    if not cand_industries:
        return "miss:new_entry"
    counts: Dict[str, int] = {}
    for r in fupan_rows or []:
        nm = str(r.get("题材名称") or "").strip()
        if not nm:
            continue
        counts[nm] = counts.get(nm, 0) + 1
    matched_max = 0
    for ind in cand_industries:
        for theme_name, count in counts.items():
            if _theme_names_match(theme_name, ind, canon_map):
                matched_max = max(matched_max, count)
                break
    if matched_max == 0:
        # T-0 industry exists but T-1 fupan没体现 → 可能是新主线
        return "hit_strong:absorb_dip"
    if matched_max >= fade_min_count:
        return "hit_weak:fade"
    if matched_max >= acceleration_min_count:
        return "hit_strong:acceleration"
    return "hit_strong:absorb_dip"


def compute_longtou_status(fupan_row: Optional[Mapping[str, Any]],
                            *, longtou_min_board: int = 2,
                            mid_position_max_seq: int = 3) -> str:
    if not fupan_row:
        return "none"
    seq = _parse_int(fupan_row.get("题材内序号"))
    if seq is None:
        return "none"
    boards = _parse_int(fupan_row.get("连板")) or 0
    if seq == 1 and boards >= longtou_min_board:
        return "confirmed_longtou"
    if seq <= mid_position_max_seq:
        return "mid_position"
    return "follower"


# ---------------------------------------------------------------------------
# Layer 2: cashflow_continuity
# ---------------------------------------------------------------------------
def compute_cashflow_continuity(code: str,
                                  prev_caps: Mapping[str, Any],
                                  *, accumulating_rank_max: int = 75,
                                  distributing_rank_min: int = 105) -> str:
    norm = _norm_code(code)
    if not norm:
        return "miss"
    ranks: Dict[str, int] = {}
    for period in ("today", "3day", "5day", "10day"):
        ds = prev_caps.get(f"cashflow.stock.{period}")
        rows: Sequence[Mapping[str, Any]]
        if isinstance(ds, Mapping):
            rows = ds.get("rows") or []
        else:
            rows = []
        for r in rows:
            if _norm_code(r.get("代码") or r.get("code")) == norm:
                rk = _parse_int(r.get("排名") or r.get("rank"))
                if rk is not None:
                    ranks[period] = rk
                break
    present = len(ranks)
    if present == 0:
        return "miss"
    if present == 1:
        return "neutral"
    seq = [ranks[p] for p in ("10day", "5day", "3day", "today") if p in ranks]
    if len(seq) < 2:
        return "neutral"
    if all(r <= accumulating_rank_max for r in seq):
        return "accumulating"
    if seq[-1] < seq[0] and seq[-1] <= accumulating_rank_max:
        return "accumulating"
    if seq[-1] > seq[0] and seq[-1] >= distributing_rank_min:
        return "distributing"
    return "neutral"


# ---------------------------------------------------------------------------
# Setup classifier (mutex, first match wins)
# ---------------------------------------------------------------------------
def _conditions_match(cond: Mapping[str, Any], facts: Mapping[str, Any]) -> bool:
    for key, expected in cond.items():
        if key.endswith("_in"):
            field = key[:-3]
            if facts.get(field) not in expected:
                return False
        elif key.endswith("_not_in"):
            field = key[:-7]
            if facts.get(field) in expected:
                return False
        elif key.endswith("_min"):
            field = key[:-4]
            v = facts.get(field)
            if v is None or v < expected:
                return False
        elif key.endswith("_max"):
            field = key[:-4]
            v = facts.get(field)
            if v is None or v > expected:
                return False
    return True


def classify_setup(labels: Mapping[str, str], regime: str, source_hit_count: int,
                    *, setup_rules: Optional[Mapping[str, Any]] = None,
                    setup_order: Sequence[str] = SETUP_ORDER) -> str:
    facts = {**labels, "regime": regime, "source_hit_count": source_hit_count}
    rules = setup_rules or _DEFAULT_SETUP_RULES
    for setup in setup_order:
        rule = rules.get(setup)
        if not rule:
            continue
        cond = rule.get("conditions") or {}
        if _conditions_match(cond, facts):
            return setup
    return "none"


# ---------------------------------------------------------------------------
# Reasons & risks
# ---------------------------------------------------------------------------
_SETUP_NAMES = {
    "A": "Setup A 冰点反弹首日龙头",
    "B": "Setup B 主升期龙头接力",
    "C": "Setup C 题材首板突破",
    "D": "Setup D 退潮反包",
    "E": "Setup E 一字板埋伏",
    "none": "未匹配 setup",
}


def _build_reasons(setup: str, labels: Mapping[str, str], industries: Sequence[str],
                    cand: _Candidate) -> List[str]:
    reasons: List[str] = [_SETUP_NAMES.get(setup, setup)]
    reasons.append(f"行业:{labels.get('industry_t1_label', '?')}")
    reasons.append(f"个股:{labels.get('stock_t1_label', '?')}")
    reasons.append(
        f"涨停:{labels.get('zt_pattern', '?')}/{labels.get('zt_quality', '?')}"
    )
    reasons.append(f"龙头:{labels.get('longtou_status', '?')}")
    reasons.append(f"资金延续:{labels.get('cashflow_continuity', '?')}")
    if industries:
        reasons.append(f"主线:{','.join(list(industries)[:3])}")
    sources = list((cand.sources or {}).keys())
    if sources:
        reasons.append(f"竞价多源:{'/'.join(sources)}")
    return reasons


def _build_risks(setup: str, labels: Mapping[str, str], regime: str) -> List[str]:
    risks: List[str] = []
    if labels.get("zt_quality") == "dirty":
        risks.append("T-1 涨停为烂板，承接弱")
    if labels.get("lhb_status") == "listed":
        risks.append("T-1 已登龙虎榜，可能游资接力或派发")
    if labels.get("cashflow_continuity") == "distributing":
        risks.append("T-N 资金有流出迹象")
    if regime == "cold" and setup in {"B", "C"}:
        risks.append("冷市做强势 setup，赔率打折")
    if regime == "hot" and setup == "A":
        risks.append("热市做冷启动 setup，注意拥挤")
    if labels.get("industry_t1_label") == "miss:new_entry" and setup not in {"A", "E"}:
        risks.append("个股行业未在 T-0 kaipan 主线，主线匹配较弱")
    return risks


# ---------------------------------------------------------------------------
# Anchors
# ---------------------------------------------------------------------------
def _instantiate_anchors(setup: str, cand_facts: Mapping[str, Any],
                          anchor_specs: Mapping[str, Any]) -> Dict[str, Any]:
    spec = anchor_specs.get(setup) if anchor_specs else None
    if not spec:
        return {}
    out_conditions = []
    for raw in spec.get("conditions") or []:
        cond = dict(raw)
        if cond.get("type") == "price_above_auction":
            cond["ref_pct"] = cand_facts.get("auction_change_pct")
        out_conditions.append(cond)
    return {
        "check_at": spec.get("check_at"),
        "conditions": out_conditions,
    }


# ---------------------------------------------------------------------------
# Helpers: locate capture dir + load extended T-1 captures
# ---------------------------------------------------------------------------
def _resolve_capture_dir(report: Mapping[str, Any]) -> Optional[Path]:
    for key in ("captures_dir", "capture_dir", "base_dir"):
        val = report.get(key)
        if val:
            return Path(str(val))
    for item in report.get("items") or []:
        cp = item.get("capture_path")
        if cp:
            p = Path(str(cp))
            if p.parent.parent.exists():
                return p.parent.parent
    return None


def _load_prev_cashflow_into(prev_caps: Dict[str, Any],
                               project_root: Path,
                               prev_date: Optional[str]) -> None:
    if not prev_date:
        return
    base = project_root / "captures" / prev_date
    for period in ("today", "3day", "5day", "10day"):
        key = f"cashflow.stock.{period}"
        if key in prev_caps:
            continue
        sub = base / key
        if not sub.is_dir():
            continue
        files = sorted(sub.glob("*.json"))
        if not files:
            continue
        try:
            prev_caps[key] = json.loads(files[-1].read_text(encoding="utf-8")) or {}
        except (OSError, json.JSONDecodeError):
            continue


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------
def build_premarket_analysis_v7(report: Mapping[str, Any],
                                  project_root: Optional[Path | str] = None,
                                  config: Optional[Mapping[str, Any]] = None,
                                  v7_config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Run v7 setup-classifier analysis on a premarket capture report."""
    if config is None:
        try:
            config = load_premarket_config(project_root=project_root)
        except Exception:
            config = {}
    if v7_config is None:
        try:
            v7_config = load_v7_config(project_root=project_root)
        except Exception:
            v7_config = {}

    canon_map = _build_theme_canon_map(config)
    label_th = {**_DEFAULT_LABEL_THRESHOLDS}
    if v7_config:
        for k in label_th:
            if isinstance(v7_config.get(k), Mapping):
                label_th[k] = {**label_th[k], **dict(v7_config[k])}
    setup_rules = (v7_config.get("setups") if v7_config else None) or _DEFAULT_SETUP_RULES
    anchor_specs = (v7_config.get("anchors") if v7_config else None) or _DEFAULT_ANCHORS
    setup_priority = (v7_config.get("setup_priority") if v7_config else None) or _DEFAULT_SETUP_PRIORITY
    output_cfg = (v7_config.get("output") if v7_config else None) or {}
    max_candidates = int(output_cfg.get("max_candidates", 50) or 50)
    emit_setup_none = bool(output_cfg.get("emit_setup_none", False))
    anchors_dirname = str(output_cfg.get("intraday_anchors_dirname", "reports"))

    # Layer 0: candidate pool
    pool = _merge_candidates(report)

    # Layer 3: regime
    regime_rows = _dataset_rows(report, "home.qxlive.top_metrics")
    if not regime_rows:
        regime_rows = _dataset_rows(report, "review.daily.top_metrics")
    regime, regime_detail = _classify_regime(regime_rows or [], phase="premarket", config=config)

    # T-0 kaipan industries (top 20)
    kaipan_rows = _dataset_rows(report, "home.kaipan.plate.summary")
    industry_t0_names: List[str] = []
    for r in kaipan_rows[:20]:
        nm = str(r.get("主标签名称") or "").strip()
        if nm and nm not in industry_t0_names:
            industry_t0_names.append(nm)

    # Resolve project root + previous trading day captures
    capture_dir = _resolve_capture_dir(report)
    if project_root is not None:
        pr_root = Path(project_root)
    elif capture_dir is not None:
        pr_root = capture_dir.parent.parent  # captures/<date>/.. -> project root
    else:
        pr_root = Path.cwd()
    prev_date = _infer_prev_trading_day(capture_dir) if capture_dir else None
    prev_caps: Dict[str, Any] = dict(_resolve_prev_trading_day_captures(pr_root, prev_date) or {})
    _load_prev_cashflow_into(prev_caps, pr_root, prev_date)

    fupan_dataset = prev_caps.get("review.fupan.plate") or {}
    fupan_rows: List[Mapping[str, Any]] = list(fupan_dataset.get("rows") or []) if isinstance(fupan_dataset, Mapping) else []
    today_cf_dataset = prev_caps.get("cashflow.stock.today") or {}
    today_cf_rows: List[Mapping[str, Any]] = list(today_cf_dataset.get("rows") or []) if isinstance(today_cf_dataset, Mapping) else []

    # Untradable filter
    valid: List[Tuple[str, _Candidate]] = []
    for code, cand in pool.items():
        cd = {"code": code, "latest_change_pct": cand.latest_change_pct}
        if _is_untradable_v6(cd, config=config):
            continue
        valid.append((code, cand))

    out: List[Dict[str, Any]] = []
    label_stats: Dict[str, Dict[str, int]] = {}
    setup_stats: Dict[str, int] = {}
    for code, cand in valid:
        cand_industries: List[str] = []
        for con in cand.concepts or []:
            for ind in industry_t0_names:
                if _theme_names_match(con, ind, canon_map):
                    if ind not in cand_industries:
                        cand_industries.append(ind)
                    break

        fupan_row = _lookup_fupan_row(code, fupan_rows)
        labels = {
            "industry_t1_label": compute_industry_t1_label(
                cand_industries, fupan_rows, canon_map,
                fade_min_count=int(label_th["industry_t1_label"]["fade_min_count"]),
                acceleration_min_count=int(label_th["industry_t1_label"]["acceleration_min_count"]),
            ),
            "stock_t1_label": compute_stock_t1_label(
                code, today_cf_rows,
                hit_top_max_rank=int(label_th["stock_t1_label"]["hit_top_max_rank"]),
                hit_mid_max_rank=int(label_th["stock_t1_label"]["hit_mid_max_rank"]),
            ),
            "zt_pattern": compute_zt_pattern(fupan_row),
            "zt_quality": compute_zt_quality(
                fupan_row,
                dirty_min_open_count=int(label_th["zt_quality"]["dirty_min_open_count"]),
            ),
            "lhb_status": compute_lhb_status(fupan_row),
            "longtou_status": compute_longtou_status(
                fupan_row,
                longtou_min_board=int(label_th["longtou_status"]["longtou_min_board"]),
                mid_position_max_seq=int(label_th["longtou_status"]["mid_position_max_seq"]),
            ),
            "cashflow_continuity": compute_cashflow_continuity(
                code, prev_caps,
                accumulating_rank_max=int(label_th["cashflow_continuity"]["accumulating_rank_max"]),
                distributing_rank_min=int(label_th["cashflow_continuity"]["distributing_rank_min"]),
            ),
            # v7.0 stub. Backfill in v7.1 once theme T-N tracking is implemented.
            "theme_history": "fresh",
        }
        for k, v in labels.items():
            bucket = label_stats.setdefault(k, {})
            bucket[v] = bucket.get(v, 0) + 1

        source_hit_count = len(cand.sources or {})
        setup = classify_setup(labels, regime, source_hit_count, setup_rules=setup_rules)
        setup_stats[setup] = setup_stats.get(setup, 0) + 1

        cand_facts = {
            "code": code,
            "name": cand.name,
            "auction_change_pct": cand.auction_change_pct,
        }
        anchors = _instantiate_anchors(setup, cand_facts, anchor_specs) if setup != "none" else {}

        out.append({
            "code": code,
            "name": cand.name or "",
            "setup": setup,
            "setup_priority": int(setup_priority.get(setup, 0)),
            "labels": labels,
            "matched_industries": cand_industries,
            "source_hits": list((cand.sources or {}).keys()),
            "source_hit_count": source_hit_count,
            "auction_change_pct": cand.auction_change_pct,
            "latest_change_pct": cand.latest_change_pct,
            "market_cap_yi": cand.market_cap_yi,
            "concepts": (cand.concepts or [])[:8],
            "anchors": anchors,
            "reasons": _build_reasons(setup, labels, cand_industries, cand),
            "risks": _build_risks(setup, labels, regime),
            # v7 doesn't use a single linear score. Expose setup_priority via score so
            # downstream tools that sort by 'score' (e.g. legacy Feishu cards) still work.
            "score": float(setup_priority.get(setup, 0)),
            "ranking_score": float(setup_priority.get(setup, 0)),
            "theme_score": 0.0,
            "ranking_penalty": 0.0,
            "theme_matches": [],
            "concept_tokens": (cand.concepts or [])[:8],
        })

    out.sort(key=lambda x: (-x["setup_priority"], -x["source_hit_count"], x["code"]))
    if not emit_setup_none:
        ranked = [c for c in out if c["setup"] != "none"][:max_candidates]
        # Append a few representative "none" rows for debugging visibility
        rest = [c for c in out if c["setup"] == "none"][:5]
        top_candidates = ranked + rest
    else:
        top_candidates = out[:max_candidates]

    # Anchors JSON side-effect
    anchors_payload = {
        "version": VERSION,
        "date": capture_dir.name if capture_dir else None,
        "prev_date": prev_date,
        "regime": regime,
        "candidates": [
            {
                "code": c["code"],
                "name": c["name"],
                "setup": c["setup"],
                "auction_change_pct": c["auction_change_pct"],
                "matched_industries": c["matched_industries"],
                "anchors": c["anchors"],
            }
            for c in top_candidates if c["setup"] != "none"
        ],
    }
    anchors_path: Optional[str] = None
    try:
        if capture_dir is not None and capture_dir.name:
            target_dir = pr_root / anchors_dirname / capture_dir.name
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / "intraday_anchors.json"
            target.write_text(
                json.dumps(anchors_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            anchors_path = str(target)
    except Exception:
        anchors_path = None

    return {
        "enabled": True,
        "version": VERSION,
        "regime": regime,
        "regime_detail": regime_detail,
        "prev_date": prev_date,
        "industry_kaipan_top": industry_t0_names,
        "label_stats": label_stats,
        "setup_stats": setup_stats,
        "top_candidates": top_candidates,
        "intraday_anchors_path": anchors_path,
    }


# Backward-compat aliases — batch.py and review_backfill.py call these names.
build_premarket_analysis = build_premarket_analysis_v7
compute_premarket_analysis = build_premarket_analysis_v7


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Run v7 premarket analysis on a saved report.json")
    ap.add_argument("report_path", help="path to a premarket report.json (output of duanxianxia_batch.py)")
    ap.add_argument("--project-root", default=".", help="project root containing config/ and captures/")
    ap.add_argument("--out", help="write analysis JSON to this path (default: stdout)")
    args = ap.parse_args(argv)
    with open(args.report_path, "r", encoding="utf-8") as fh:
        report = json.load(fh)
    result = build_premarket_analysis_v7(report, project_root=args.project_root)
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(payload, encoding="utf-8")
    else:
        sys.stdout.write(payload + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
