"""
Duanxianxia premarket scoring — v6.1

Aligned to the real capture schema observed in samples/2026-04-23/.
See projects/duanxianxia/docs/premarket-v6-field-mapping.md for the
field-level code ↔ data ↔ page map.

Public API (stable across v6 → v6.1):
    VERSION = "premarket_5table_v6.1"
    load_premarket_config(path=None, project_root=None) -> dict
    build_premarket_analysis_v6(report, project_root=None, config=None) -> dict
    build_premarket_analysis  — alias
    compute_premarket_analysis — alias (same signature)

The entry point accepts an already-parsed ``report`` object (the output of
scripts/duanxianxia_batch.py's CaptureReport.to_dict()) and returns a scoring
dict. It can also be called from __main__ on a capture directory for smoke
testing against sample data.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover — yaml is expected in prod
    yaml = None  # type: ignore

VERSION = "premarket_5table_v6.1"
DEFAULT_CONFIG_PATH = Path("projects/duanxianxia/config/premarket_scoring.yaml")

# Metric keys in home.qxlive.top_metrics / review.daily.top_metrics.
_QXLIVE_KEYS: frozenset = frozenset({
    "QX", "ZT", "DT", "KQXY", "HSLN", "LBGD", "SZ", "XD",
    "PB", "ZTBX", "LBBX", "PBBX",
})


# ---------------------------------------------------------------------------
# Small parsers
# ---------------------------------------------------------------------------
_NUMERIC_CLEAN_RE = re.compile(r"[%,\s]")


def _parse_float(value: Any) -> Optional[float]:
    """Tolerant float parser — strings, ints, None, empty-string all handled."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    try:
        return float(_NUMERIC_CLEAN_RE.sub("", str(value)))
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    f = _parse_float(value)
    if f is None:
        return None
    try:
        return int(f)
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_chinese_amount_to_yi(text: Any) -> Optional[float]:
    """Parse '32.3亿' / '4860万' / '+2.3亿' / '-1.5亿' / '-' → float in 亿.

    Returns None for '-', empty, or unparseable.
    """
    if text is None:
        return None
    s = str(text).strip()
    if not s or s == "-":
        return None
    sign = 1.0
    if s.startswith("+"):
        s = s[1:]
    if s.startswith("-"):
        sign = -1.0
        s = s[1:]
    try:
        if "亿" in s:
            return sign * float(s.replace("亿", ""))
        if "万" in s:
            return sign * float(s.replace("万", "")) / 10000.0
        return sign * float(_NUMERIC_CLEAN_RE.sub("", s))
    except ValueError:
        return None


def _parse_chinese_amount_to_wan(text: Any) -> Optional[float]:
    """Parse Chinese amount to 万 (keeps sign)."""
    yi = _parse_chinese_amount_to_yi(text)
    if yi is None:
        return None
    return yi * 10000.0


def _split_concepts(raw: Any) -> List[str]:
    """Normalise the `concept` field — pipe, 、, or single-string."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    s = str(raw).strip()
    if not s:
        return []
    parts: List[str] = []
    for chunk in re.split(r"[|｜、,，\s]+", s):
        chunk = chunk.strip()
        if chunk:
            parts.append(chunk)
    return parts


def _rank_score(rank: Optional[int], top_n: int, max_score: float) -> float:
    """Linear-decay score: rank 1 → max_score, rank top_n → 0; beyond top_n → 0."""
    if rank is None or top_n <= 0 or max_score <= 0:
        return 0.0
    if rank < 1 or rank > top_n:
        return 0.0
    return max_score * (1.0 - (rank - 1) / top_n)


def _numeric_bonus(value: Optional[float], cfg: Optional[Mapping[str, Any]]) -> float:
    if value is None or not cfg:
        return 0.0
    strong_min = cfg.get("strong_min")
    if strong_min is not None and value >= float(strong_min):
        return float(cfg.get("strong_score", 0) or 0)
    weak_min = cfg.get("weak_min")
    if weak_min is not None and value >= float(weak_min):
        return float(cfg.get("weak_score", 0) or 0)
    return 0.0


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------
def load_premarket_config(path: Optional[Path | str] = None,
                          project_root: Optional[Path | str] = None) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to load premarket_scoring.yaml")
    root = Path(project_root) if project_root else Path.cwd()
    cfg_path = Path(path) if path else (root / DEFAULT_CONFIG_PATH)
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()
    with cfg_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


# ---------------------------------------------------------------------------
# Report helpers — tolerate both raw capture dicts and CaptureReport.to_dict()
# ---------------------------------------------------------------------------
def _dataset_rows(report: Mapping[str, Any], key: str) -> List[Dict[str, Any]]:
    """Return rows for a dataset key in either `report['datasets'][key]['rows']`
    or `report[key]['rows']` or `report[key]` (if a list)."""
    if not report:
        return []
    ds = None
    if "datasets" in report and isinstance(report["datasets"], Mapping):
        ds = report["datasets"].get(key)
    if ds is None:
        ds = report.get(key)
    if ds is None:
        return []
    if isinstance(ds, list):
        return list(ds)
    if isinstance(ds, Mapping):
        rows = ds.get("rows")
        if isinstance(rows, list):
            return list(rows)
    return []


def _dataset_meta(report: Mapping[str, Any], key: str) -> Dict[str, Any]:
    if not report:
        return {}
    if "datasets" in report and isinstance(report["datasets"], Mapping):
        ds = report["datasets"].get(key)
        if isinstance(ds, Mapping):
            m = ds.get("meta")
            if isinstance(m, Mapping):
                return dict(m)
    ds = report.get(key)
    if isinstance(ds, Mapping):
        m = ds.get("meta")
        if isinstance(m, Mapping):
            return dict(m)
    return {}


# ---------------------------------------------------------------------------
# Market regime classification
# ---------------------------------------------------------------------------
def _classify_regime(top_metrics_rows: Sequence[Mapping[str, Any]],
                     *, phase: str = "premarket",
                     config: Optional[Mapping[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
    """Vote across QX / HSLN / KQXY / ZTBX / LBBX / PBBX / PB using thresholds.

    Reads each row's ``raw_chart_tail_value`` (numeric) if present, else
    parses ``value`` (string — may include %, 亿/万, signs).
    """
    reg_cfg = ((config or {}).get("market_regime") or {})
    votes = {"cold": 0, "hot": 0}
    per_metric: Dict[str, Dict[str, Any]] = {}

    def _raw_value(row: Mapping[str, Any]) -> Any:
        rv = row.get("raw_chart_tail_value")
        if rv is not None:
            return rv
        return row.get("value")

    for row in top_metrics_rows or []:
        key = row.get("metric_key")
        if key not in _QXLIVE_KEYS:
            continue
        raw = _raw_value(row)
        vote: Optional[str] = None

        if key == "QX":
            q = _parse_float(raw)
            if q is not None:
                cfg = reg_cfg.get("QX", {})
                if q <= cfg.get("cold_max", 30):
                    vote = "cold"
                elif q >= cfg.get("hot_min", 70):
                    vote = "hot"
                per_metric[key] = {"value": q, "vote": vote}
        elif key == "HSLN":
            # Premarket: "+2.3亿" / "-1.5亿". Postmarket review: raw_chart_tail_value is a bare number.
            yi: Optional[float]
            if phase == "premarket":
                yi = _parse_chinese_amount_to_yi(raw)
            else:
                yi = _parse_float(raw)
            if yi is not None:
                cfg = reg_cfg.get("HSLN_premarket" if phase == "premarket" else "HSLN_postmarket", {}) \
                    or reg_cfg.get("HSLN_premarket", {})
                if yi <= cfg.get("cold_max", -1.5):
                    vote = "cold"
                elif yi >= cfg.get("hot_min", 2.0):
                    vote = "hot"
                per_metric[key] = {"value": yi, "vote": vote}
        elif key == "KQXY":
            k = _parse_float(raw)
            if k is not None:
                cfg = reg_cfg.get("KQXY", {})
                # higher KQXY = more cold
                if k >= cfg.get("cold_min", 60):
                    vote = "cold"
                elif k <= cfg.get("hot_max", 35):
                    vote = "hot"
                per_metric[key] = {"value": k, "vote": vote}
        elif key == "ZTBX":
            z = _parse_float(raw)
            if z is not None:
                cfg = reg_cfg.get("ZTBX", {})
                if z <= cfg.get("cold_max", 0):
                    vote = "cold"
                elif z >= cfg.get("hot_min", 3):
                    vote = "hot"
                per_metric[key] = {"value": z, "vote": vote}
        elif key == "LBBX":
            l = _parse_float(raw)
            if l is not None:
                cfg = reg_cfg.get("LBBX", {})
                if l <= cfg.get("cold_max", 0):
                    vote = "cold"
                elif l >= cfg.get("hot_min", 5):
                    vote = "hot"
                per_metric[key] = {"value": l, "vote": vote}
        elif key == "PBBX":
            p = _parse_float(raw)
            if p is not None:
                cfg = reg_cfg.get("PBBX", {})
                if p <= cfg.get("cold_max", 30):
                    vote = "cold"
                elif p >= cfg.get("hot_min", 60):
                    vote = "hot"
                per_metric[key] = {"value": p, "vote": vote}
        elif key == "PB":
            pb = _parse_float(raw)
            if pb is not None:
                cfg = reg_cfg.get("PB", {})
                if pb <= cfg.get("cold_max", 40):
                    vote = "cold"
                elif pb >= cfg.get("hot_min", 70):
                    vote = "hot"
                per_metric[key] = {"value": pb, "vote": vote}

        if vote:
            votes[vote] += 1

    if votes["cold"] > votes["hot"]:
        regime = "cold"
    elif votes["hot"] > votes["cold"]:
        regime = "hot"
    else:
        regime = "normal"
    return regime, {"votes": votes, "metrics": per_metric}


# ---------------------------------------------------------------------------
# Theme catalog
# ---------------------------------------------------------------------------
def _build_theme_catalog(plate_summary_rows: Sequence[Mapping[str, Any]],
                         *, plate_meta: Optional[Mapping[str, Any]] = None,
                         config: Optional[Mapping[str, Any]] = None) -> List[Dict[str, Any]]:
    """Parse home.kaipan.plate.summary rows (all Chinese keys)."""
    cfg = ((config or {}).get("theme_overlay") or {})
    strength_floor = float(cfg.get("strength_floor", 1.0) or 0.0)
    min_inflow_wan = float(cfg.get("main_inflow_min_wan", 2000) or 0.0)
    top_n = int(cfg.get("top_n_themes", 15) or 15)
    use_subplates = bool(cfg.get("use_subplates", True))

    # Build subplate map from meta.top_plates[].subplates[]
    subplate_index: Dict[str, List[Dict[str, Any]]] = {}
    if use_subplates and plate_meta:
        for tp in (plate_meta.get("top_plates") or []):
            name = tp.get("主标签名称") or tp.get("top_plate_name") or tp.get("name")
            subplates = tp.get("subplates") or []
            if name:
                subplate_index[str(name)] = [
                    {
                        "name": sp.get("子题材名称") or sp.get("subplate_name") or sp.get("name"),
                        "code": sp.get("子题材代码") or sp.get("subplate_code") or sp.get("code"),
                    }
                    for sp in subplates
                ]

    items: List[Dict[str, Any]] = []
    for r in plate_summary_rows or []:
        name = r.get("主标签名称") or r.get("plate_name") or r.get("name")
        if not name:
            continue
        strength = _parse_float(r.get("板块强度原值") or r.get("板块强度") or r.get("strength"))
        inflow_wan = _parse_float(r.get("主力流入原值") or r.get("main_inflow_wan"))
        inflow_yuan = _parse_float(r.get("主力流入真实金额"))
        zt_count = _parse_int(r.get("涨停数量") or r.get("zt_count")) or 0

        # Fallback subplate list from 子标签列表 (pipe or 、 separated) or 子标签名称 array
        sub_list_raw = r.get("子标签列表") or r.get("子标签名称")
        fallback_sub: List[Dict[str, Any]] = []
        if isinstance(sub_list_raw, list):
            fallback_sub = [{"name": str(x)} for x in sub_list_raw if x]
        elif isinstance(sub_list_raw, str) and sub_list_raw:
            for chunk in re.split(r"[|｜、,，\s]+", sub_list_raw):
                chunk = chunk.strip()
                if chunk:
                    fallback_sub.append({"name": chunk})

        subplates = subplate_index.get(str(name)) or fallback_sub

        if strength is not None and strength < strength_floor:
            continue
        if inflow_wan is not None and inflow_wan < min_inflow_wan and zt_count < 1:
            continue

        items.append({
            "name": str(name),
            "code": r.get("主标签代码") or r.get("plate_code") or r.get("code"),
            "strength": strength,
            "inflow_wan": inflow_wan,
            "inflow_yuan": inflow_yuan,
            "zt_count": zt_count,
            "subplates": subplates,
        })

    items.sort(key=lambda x: -(x["strength"] or 0.0))
    return items[:top_n]


# ---------------------------------------------------------------------------
# Yesterday (T-1) postmarket signals
# ---------------------------------------------------------------------------
def _resolve_prev_trading_day_captures(project_root: Path,
                                       prev_date: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """Load T-1 capture datasets. Directory names use DOTS, not underscores."""
    if not prev_date:
        return {}
    captures_dir = Path(project_root) / "captures" / str(prev_date)
    if not captures_dir.exists():
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    wanted = ("review.ltgd.range", "review.fupan.plate",
              "review.daily.top_metrics", "home.ztpool")
    for name in wanted:
        subdir = captures_dir / name
        if not subdir.is_dir():
            continue
        json_files = sorted(subdir.glob("*.json"))
        if not json_files:
            continue
        try:
            with json_files[-1].open("r", encoding="utf-8") as fh:
                result[name] = json.load(fh) or {}
        except (OSError, json.JSONDecodeError):
            continue
    return result


def _evaluate_yesterday_signals(prev_captures: Mapping[str, Any],
                                *, config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    cfg = ((config or {}).get("yesterday_postmarket") or {})
    min_board = int(cfg.get("min_board_count", 2) or 2)
    min_theme_stock = int(cfg.get("min_theme_stock_count", 3) or 3)
    ltgd_cycles = cfg.get("ltgd_cycles_to_use", ["5日", "10日"]) or []
    ltgd_min_pct = float(cfg.get("ltgd_min_range_pct", 20) or 0.0)

    fupan_rows: List[Dict[str, Any]] = []
    fupan_dataset = prev_captures.get("review.fupan.plate") or {}
    if isinstance(fupan_dataset, Mapping):
        raw = fupan_dataset.get("rows")
        if isinstance(raw, list):
            fupan_rows = list(raw)

    ltgd_rows: List[Dict[str, Any]] = []
    ltgd_dataset = prev_captures.get("review.ltgd.range") or {}
    if isinstance(ltgd_dataset, Mapping):
        raw = ltgd_dataset.get("rows")
        if isinstance(raw, list):
            ltgd_rows = list(raw)

    # Group fupan by 题材名称
    theme_map: Dict[str, List[Dict[str, Any]]] = {}
    for r in fupan_rows:
        theme = r.get("题材名称")
        if not theme:
            continue
        theme_map.setdefault(str(theme), []).append(r)

    hot_themes: List[Dict[str, Any]] = []
    leader_map: Dict[str, Dict[str, Any]] = {}
    for theme, rows in theme_map.items():
        if len(rows) < min_theme_stock:
            continue
        entry = {"name": theme, "stock_count": len(rows), "leader_codes": []}
        for r in rows:
            board = _parse_int(r.get("连板")) or 0
            code = r.get("代码")
            if not code:
                continue
            if board >= min_board:
                entry["leader_codes"].append(code)
                leader_map[str(code)] = {
                    "code": code,
                    "name": r.get("名称"),
                    "board": board,
                    "theme": theme,
                    "reason": r.get("异动原因", ""),
                    "fine_tags": r.get("细标签列表") or [],
                }
        hot_themes.append(entry)

    ltgd_codes: Dict[str, Dict[str, Any]] = {}
    for r in ltgd_rows:
        cycle = r.get("周期")
        if cycle not in ltgd_cycles:
            continue
        code = r.get("代码")
        if not code:
            continue
        pct = _parse_float(str(r.get("区间涨幅", "")).replace("%", ""))
        if pct is None or pct < ltgd_min_pct:
            continue
        rec = ltgd_codes.setdefault(str(code), {
            "code": code,
            "name": r.get("名称"),
            "cycles": {},
            "max_range_pct": 0.0,
            "concepts": _split_concepts(r.get("概念")),
        })
        rec["cycles"][cycle] = pct
        if pct > rec["max_range_pct"]:
            rec["max_range_pct"] = pct

    return {
        "hot_themes": hot_themes,
        "leader_map": leader_map,
        "ltgd_codes": ltgd_codes,
    }


# ---------------------------------------------------------------------------
# Untradable filter
# ---------------------------------------------------------------------------
def _is_untradable_v6(candidate: Mapping[str, Any],
                     *, config: Optional[Mapping[str, Any]] = None) -> bool:
    cfg = ((config or {}).get("untradable") or {})
    upper = float(cfg.get("upper_limit_pct", 9.7) or 9.7)
    latest = _parse_float(candidate.get("latest_change_pct"))
    if latest is not None and latest >= upper:
        return True
    code = str(candidate.get("code") or "")
    for prefix in cfg.get("exclude_prefixes") or []:
        if code.startswith(str(prefix)):
            return True
    return False


# ---------------------------------------------------------------------------
# Merge candidates across 4 auction sources
# ---------------------------------------------------------------------------
@dataclass
class _Candidate:
    code: str
    name: Optional[str] = None
    sources: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    concepts: List[str] = field(default_factory=list)
    board_label: Optional[str] = None
    latest_change_pct: Optional[float] = None
    auction_change_pct: Optional[float] = None
    market_cap_yi: Optional[float] = None

    def add_concepts(self, concepts: Iterable[str]) -> None:
        for c in concepts:
            if c and c not in self.concepts:
                self.concepts.append(c)


def _merge_candidates(report: Mapping[str, Any]) -> Dict[str, _Candidate]:
    pool: Dict[str, _Candidate] = {}

    def _ensure(code: str) -> _Candidate:
        if code not in pool:
            pool[code] = _Candidate(code=code)
        return pool[code]

    # --- vratio ---
    for row in _dataset_rows(report, "auction.jjyd.vratio"):
        code = row.get("code")
        if not code:
            continue
        cand = _ensure(str(code))
        cand.name = cand.name or row.get("name")
        cand.sources["vratio"] = {
            "rank": _parse_int(row.get("rank")),
            "volume_ratio_multiple": _parse_float(row.get("volume_ratio_multiple")),
            "auction_turnover_wan": _parse_float(row.get("auction_turnover_wan")),
            "turnover_rate_pct": _parse_float(row.get("turnover_rate_pct")),
            "yesterday_auction_turnover_wan": _parse_float(row.get("yesterday_auction_turnover_wan")),
        }
        if cand.auction_change_pct is None:
            cand.auction_change_pct = _parse_float(row.get("auction_change_pct"))
        if cand.latest_change_pct is None:
            cand.latest_change_pct = _parse_float(row.get("latest_change_pct"))
        cand.add_concepts(_split_concepts(row.get("concept")))

    # --- qiangchou (group == "grab") ---
    for row in _dataset_rows(report, "auction.jjyd.qiangchou"):
        if str(row.get("group") or "").lower() not in ("grab", "qiangchou"):
            # captures use "grab"; accept either for safety
            continue
        code = row.get("code")
        if not code:
            continue
        cand = _ensure(str(code))
        cand.name = cand.name or row.get("name")
        cand.sources["qiangchou"] = {
            "rank": _parse_int(row.get("rank")),
            "grab_strength": _parse_float(row.get("grab_strength")),
            "auction_turnover_wan": _parse_float(row.get("auction_turnover_wan")),
            "turnover_rate_pct": _parse_float(row.get("turnover_rate_pct")),
        }
        if cand.auction_change_pct is None:
            cand.auction_change_pct = _parse_float(row.get("auction_change_pct"))
        if cand.latest_change_pct is None:
            cand.latest_change_pct = _parse_float(row.get("latest_change_pct"))
        cand.add_concepts(_split_concepts(row.get("concept")))

    # --- net_amount ---
    for row in _dataset_rows(report, "auction.jjyd.net_amount"):
        code = row.get("code")
        if not code:
            continue
        cand = _ensure(str(code))
        cand.name = cand.name or row.get("name")
        cand.sources["net_amount"] = {
            "rank": _parse_int(row.get("rank")),
            "main_net_inflow_wan": _parse_float(row.get("main_net_inflow_wan")),
            "auction_turnover_wan": _parse_float(row.get("auction_turnover_wan")),
        }
        if cand.market_cap_yi is None:
            cand.market_cap_yi = _parse_float(row.get("market_cap_yi"))
        if cand.auction_change_pct is None:
            cand.auction_change_pct = _parse_float(row.get("auction_change_pct"))
        if cand.latest_change_pct is None:
            cand.latest_change_pct = _parse_float(row.get("latest_change_pct"))
        cand.add_concepts(_split_concepts(row.get("concept")))
        cand.add_concepts([c for c in (row.get("concept_1"), row.get("concept_2")) if c])

    # --- fengdan (live) ---
    for row in _dataset_rows(report, "auction.jjlive.fengdan"):
        if str(row.get("section_kind") or "").lower() != "live":
            # allow empty section_kind for robustness
            if row.get("section_kind") not in (None, ""):
                continue
        code = row.get("code")
        if not code:
            continue
        cand = _ensure(str(code))
        cand.name = cand.name or row.get("name")
        a915 = _parse_chinese_amount_to_yi(row.get("amount_915"))
        a920 = _parse_chinese_amount_to_yi(row.get("amount_920"))
        a925 = _parse_chinese_amount_to_yi(row.get("amount_925"))
        cand.sources["fengdan"] = {
            "rank": _parse_int(row.get("rank")),
            "board_label": row.get("board_label"),
            "amount_915_yi": a915,
            "amount_920_yi": a920,
            "amount_925_yi": a925,
            "fengdan_925_yi": a925,  # alias for config key
        }
        if cand.board_label is None:
            cand.board_label = row.get("board_label")
        if cand.latest_change_pct is None:
            cand.latest_change_pct = _parse_float(row.get("latest_change_pct"))
        tags = row.get("tags") or []
        if isinstance(tags, list):
            cand.add_concepts([str(t) for t in tags if t])

    return pool


# ---------------------------------------------------------------------------
# Theme evaluation per candidate
# ---------------------------------------------------------------------------
def _evaluate_theme_for_candidate(candidate: _Candidate,
                                  themes: Sequence[Mapping[str, Any]],
                                  *, config: Optional[Mapping[str, Any]] = None
                                  ) -> Tuple[float, List[str], List[str]]:
    cfg = ((config or {}).get("theme_overlay") or {})
    match_bonus = float(cfg.get("match_bonus", 4) or 0.0)
    sub_bonus = float(cfg.get("subplate_match_bonus", 2) or 0.0)

    cand_concept_set = {c.strip() for c in candidate.concepts if c and c.strip()}
    if not cand_concept_set:
        return 0.0, [], []

    matched_themes: List[str] = []
    matched_subs: List[str] = []
    score = 0.0
    for th in themes:
        th_name = th.get("name")
        if not th_name:
            continue
        if th_name in cand_concept_set:
            matched_themes.append(th_name)
            score += match_bonus
        for sp in th.get("subplates") or []:
            sp_name = sp.get("name") if isinstance(sp, Mapping) else None
            if sp_name and sp_name in cand_concept_set:
                matched_subs.append(sp_name)
                score += sub_bonus
    return score, matched_themes, matched_subs


# ---------------------------------------------------------------------------
# Scoring core
# ---------------------------------------------------------------------------
def _compute_numeric_score(cand: _Candidate, *, config: Mapping[str, Any]) -> Tuple[float, Dict[str, float]]:
    cfg = config.get("numeric_signals") or {}
    breakdown: Dict[str, float] = {}
    total = 0.0

    vr = cand.sources.get("vratio", {})
    b = _numeric_bonus(vr.get("volume_ratio_multiple"), cfg.get("volume_ratio_multiple"))
    if b: breakdown["volume_ratio_multiple"] = b; total += b
    b = _numeric_bonus(vr.get("auction_turnover_wan"), cfg.get("auction_turnover_wan"))
    if b: breakdown["auction_turnover_wan"] = b; total += b

    qc = cand.sources.get("qiangchou", {})
    b = _numeric_bonus(qc.get("grab_strength"), cfg.get("grab_strength"))
    if b: breakdown["grab_strength"] = b; total += b

    na = cand.sources.get("net_amount", {})
    b = _numeric_bonus(na.get("main_net_inflow_wan"), cfg.get("main_net_inflow_wan"))
    if b: breakdown["main_net_inflow_wan"] = b; total += b

    fd = cand.sources.get("fengdan", {})
    b = _numeric_bonus(fd.get("amount_925_yi"), cfg.get("fengdan_925_yi"))
    if b: breakdown["fengdan_925_yi"] = b; total += b

    return total, breakdown


def _compute_rank_score(cand: _Candidate, *, config: Mapping[str, Any]) -> Tuple[float, Dict[str, float]]:
    cfg = config.get("rank_scores") or {}
    breakdown: Dict[str, float] = {}
    total = 0.0
    for src_name in ("vratio", "qiangchou", "net_amount", "fengdan"):
        src = cand.sources.get(src_name)
        if not src:
            continue
        src_cfg = cfg.get(src_name) or {}
        s = _rank_score(src.get("rank"), int(src_cfg.get("top_n", 0) or 0),
                        float(src_cfg.get("max_score", 0) or 0))
        if s:
            breakdown[src_name] = s
            total += s
    return total, breakdown


def _compute_risk_penalty(cand: _Candidate, themes: Sequence[Mapping[str, Any]],
                          matched_themes: Sequence[str], *,
                          config: Mapping[str, Any]) -> Tuple[float, Dict[str, float]]:
    cfg = config.get("risk_penalty") or {}
    breakdown: Dict[str, float] = {}
    total = 0.0
    if not matched_themes:
        return 0.0, breakdown
    crowded_threshold = int(cfg.get("crowded_zt_count", 8) or 8)
    crowded_penalty = float(cfg.get("crowded_penalty", -3) or 0.0)
    outflow_yuan = float(cfg.get("large_outflow_yuan", -500000000) or 0.0)
    outflow_penalty = float(cfg.get("large_outflow_penalty", -2) or 0.0)

    by_name = {th["name"]: th for th in themes if th.get("name")}
    for name in matched_themes:
        th = by_name.get(name)
        if not th:
            continue
        if (th.get("zt_count") or 0) >= crowded_threshold:
            breakdown.setdefault("crowded", 0.0)
            breakdown["crowded"] += crowded_penalty
            total += crowded_penalty
        iy = th.get("inflow_yuan")
        if iy is not None and iy <= outflow_yuan:
            breakdown.setdefault("large_outflow", 0.0)
            breakdown["large_outflow"] += outflow_penalty
            total += outflow_penalty
    return total, breakdown


def _yesterday_bonus(cand: _Candidate, yesterday: Mapping[str, Any],
                    matched_themes: Sequence[str], *,
                    config: Mapping[str, Any]) -> Tuple[float, Dict[str, Any]]:
    cfg = config.get("yesterday_postmarket") or {}
    theme_w = float(cfg.get("theme_weight", 3) or 0.0)
    leader_w = float(cfg.get("leader_weight", 5) or 0.0)
    ltgd_w = float(cfg.get("ltgd_weight", 2) or 0.0)

    hot_themes = {t["name"] for t in (yesterday.get("hot_themes") or []) if t.get("name")}
    leader_map = yesterday.get("leader_map") or {}
    ltgd_codes = yesterday.get("ltgd_codes") or {}

    info: Dict[str, Any] = {}
    score = 0.0
    matched_hot = [t for t in matched_themes if t in hot_themes]
    if matched_hot:
        b = theme_w * len(matched_hot)
        score += b
        info["hot_themes"] = matched_hot
        info["hot_theme_bonus"] = b
    if str(cand.code) in leader_map:
        score += leader_w
        info["was_leader"] = leader_map[str(cand.code)]
        info["leader_bonus"] = leader_w
    if str(cand.code) in ltgd_codes:
        score += ltgd_w
        info["ltgd"] = ltgd_codes[str(cand.code)]
        info["ltgd_bonus"] = ltgd_w
    return score, info


def _compute_source_hit_bonus(cand: _Candidate, *, config: Mapping[str, Any]) -> float:
    cfg = config.get("source_hit_bonuses") or {}
    n = len(cand.sources)
    if n >= 4:
        return float(cfg.get("four_sources", 10) or 0.0)
    if n == 3:
        return float(cfg.get("three_sources", 6) or 0.0)
    if n == 2:
        return float(cfg.get("two_sources", 3) or 0.0)
    return 0.0


def _compute_direction_consistency(cand: _Candidate, *, config: Mapping[str, Any]) -> float:
    cfg = config.get("direction_consistency") or {}
    if cand.auction_change_pct is not None and cand.latest_change_pct is not None:
        if cand.auction_change_pct > 0 and cand.latest_change_pct > 0:
            return float(cfg.get("score", 2) or 0.0)
    return 0.0


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def build_premarket_analysis_v6(report: Mapping[str, Any],
                                *,
                                project_root: Optional[Path | str] = None,
                                config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Main entry point — consumes a capture report, returns a scoring dict."""
    cfg = dict(config) if config is not None else load_premarket_config(project_root=project_root)
    root = Path(project_root) if project_root else Path.cwd()

    # 1. Regime
    qx_rows = _dataset_rows(report, "home.qxlive.top_metrics")
    regime, regime_detail = _classify_regime(qx_rows, phase="premarket", config=cfg)
    regime_mult = float(((cfg.get("market_regime") or {}).get("regime_multipliers") or {})
                        .get(regime, 1.0) or 1.0)

    # 2. Themes
    plate_rows = _dataset_rows(report, "home.kaipan.plate.summary")
    plate_meta = _dataset_meta(report, "home.kaipan.plate.summary")
    themes = _build_theme_catalog(plate_rows, plate_meta=plate_meta, config=cfg)

    # 3. Yesterday (T-1)
    prev_date = (report.get("prev_trading_day")
                 or report.get("prev_date")
                 or _dataset_meta(report, "home.qxlive.top_metrics").get("prev_trading_day"))
    prev_captures = _resolve_prev_trading_day_captures(root, prev_date)
    yesterday = _evaluate_yesterday_signals(prev_captures, config=cfg)

    # 4. Merge candidates from 4 auction sources
    pool = _merge_candidates(report)

    # 5. Score each candidate
    scored: List[Dict[str, Any]] = []
    round_to = int((cfg.get("output") or {}).get("round_scores_to", 2) or 2)

    for code, cand in pool.items():
        if _is_untradable_v6({
            "code": cand.code,
            "latest_change_pct": cand.latest_change_pct,
        }, config=cfg):
            continue

        rank_s, rank_bd = _compute_rank_score(cand, config=cfg)
        num_s, num_bd = _compute_numeric_score(cand, config=cfg)
        hit_s = _compute_source_hit_bonus(cand, config=cfg)
        dir_s = _compute_direction_consistency(cand, config=cfg)
        theme_s, matched_themes, matched_subs = _evaluate_theme_for_candidate(
            cand, themes, config=cfg)
        risk_s, risk_bd = _compute_risk_penalty(cand, themes, matched_themes, config=cfg)
        yest_s, yest_info = _yesterday_bonus(cand, yesterday, matched_themes, config=cfg)

        raw_total = (rank_s + num_s + hit_s + dir_s + theme_s + risk_s + yest_s)
        total = raw_total * regime_mult

        scored.append({
            "code": cand.code,
            "name": cand.name,
            "score": round(total, round_to),
            "raw_score": round(raw_total, round_to),
            "regime": regime,
            "regime_multiplier": regime_mult,
            "source_count": len(cand.sources),
            "sources": list(cand.sources.keys()),
            "board_label": cand.board_label,
            "auction_change_pct": cand.auction_change_pct,
            "latest_change_pct": cand.latest_change_pct,
            "matched_themes": matched_themes,
            "matched_subplates": matched_subs,
            "yesterday": yest_info,
            "breakdown": {
                "rank": rank_bd,
                "numeric": num_bd,
                "source_hit": hit_s,
                "direction": dir_s,
                "theme": theme_s,
                "risk": risk_bd,
                "yesterday": yest_s,
            },
            "source_details": cand.sources,
            "concepts": cand.concepts,
        })

    scored.sort(key=lambda x: (-(x["score"] or 0), x["code"]))
    max_out = int((cfg.get("output") or {}).get("max_candidates", 50) or 50)
    top = scored[:max_out]

    result: Dict[str, Any] = {
        "version": VERSION,
        "regime": regime,
        "regime_detail": regime_detail,
        "regime_multiplier": regime_mult,
        "themes": themes,
        "yesterday": {
            "prev_date": prev_date,
            "hot_themes": yesterday.get("hot_themes", []),
            "leader_count": len(yesterday.get("leader_map") or {}),
            "ltgd_count": len(yesterday.get("ltgd_codes") or {}),
        },
        "candidates": top,
        "candidate_total": len(scored),
    }
    if (cfg.get("output") or {}).get("include_debug"):
        result["debug"] = {
            "pool_size": len(pool),
            "source_coverage": {
                src: sum(1 for c in pool.values() if src in c.sources)
                for src in ("vratio", "qiangchou", "net_amount", "fengdan")
            },
        }
    return result


# Backwards-compatible aliases
build_premarket_analysis = build_premarket_analysis_v6
compute_premarket_analysis = build_premarket_analysis_v6


# ---------------------------------------------------------------------------
# CLI / smoke test
# ---------------------------------------------------------------------------
def _load_capture_dir_as_report(capture_dir: Path) -> Dict[str, Any]:
    """Load a flat captures/<date>/<dataset.name>/<HHMMSS>.json tree into a report dict."""
    capture_dir = Path(capture_dir)
    datasets: Dict[str, Dict[str, Any]] = {}
    for sub in sorted(capture_dir.iterdir()):
        if not sub.is_dir():
            continue
        json_files = sorted(sub.glob("*.json"))
        if not json_files:
            continue
        try:
            with json_files[-1].open("r", encoding="utf-8") as fh:
                datasets[sub.name] = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
    return {"capture_date": capture_dir.name, "datasets": datasets}


def _main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Duanxianxia premarket v6.1 smoke test")
    parser.add_argument("capture_dir", help="Path to captures/<date>/ or samples/<date>/")
    parser.add_argument("--config", default=None, help="Path to premarket_scoring.yaml")
    parser.add_argument("--project-root", default=".", help="Project root (for resolving T-1 captures)")
    parser.add_argument("--prev-date", default=None,
                        help="Override previous trading day (YYYY-MM-DD)")
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args(argv)

    root = Path(args.project_root).resolve()
    report = _load_capture_dir_as_report(Path(args.capture_dir))
    if args.prev_date:
        report["prev_trading_day"] = args.prev_date

    cfg = load_premarket_config(path=args.config, project_root=root)
    result = build_premarket_analysis_v6(report, project_root=root, config=cfg)

    print(f"version={result['version']}  regime={result['regime']} (x{result['regime_multiplier']:.2f})")
    print(f"themes={len(result['themes'])}  candidates={result['candidate_total']}")
    print(f"T-1 hot_themes={len(result['yesterday']['hot_themes'])} "
          f"leaders={result['yesterday']['leader_count']} "
          f"ltgd={result['yesterday']['ltgd_count']}")
    print("-" * 80)
    for i, c in enumerate(result["candidates"][: args.top], start=1):
        print(f"{i:2d}. {c['code']} {c['name'] or '':<10} "
              f"score={c['score']:.2f}  sources={','.join(c['sources'])}  "
              f"themes={','.join(c['matched_themes'][:3])}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(_main())
