#!/usr/bin/env python3
"""v7.1 盘前分析 runner: existing captures -> labels -> setup_engine -> output."""
from __future__ import annotations

import argparse, csv, json, re, sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from duanxianxia_v7_1_data_loader import load_premarket_bundle
from duanxianxia_v7_1_industry_t1_label import compute_industry_t1_labels, build_canon_map, canonicalize
from duanxianxia_v7_1_theme_history import compute_theme_history_batch
from duanxianxia_v7_1_stock_t1_label import compute_stock_t1_labels
from duanxianxia_v7_1_cashflow_continuity import compute_cashflow_continuity
from duanxianxia_v7_1_zt_labels import compute_zt_labels
from duanxianxia_v7_1_longtou_status import compute_longtou_status
from duanxianxia_v7_1_tech_profile import compute_tech_profile
from duanxianxia_v7_1_regime import compute_regime
from duanxianxia_v7_1_setup_engine import classify_candidates
from duanxianxia_v7_1_output import write_v7_1_outputs, shape_v7_1_output

TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_PROJECT_ROOT = Path("/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia")
CONFIG_REL = Path("config/premarket_v7_1_setups.yaml")
DEFAULT_ALIASES = [["业绩增长","一季报增长","年报增长","业绩预增","预增","半年报增长"],["半导体产业链","芯片","集成电路","元器件","芯片封测","封测","先进封装","CPU"],["光通信","通信","光模块","CPO"],["华为","华为海思","华为产业链","鸿蒙"],["算力","算力概念","AI算力","AI服务器","IDC","数据中心","AI智能体"],["大消费","零售","服装家纺","猪肉","农业","食品饮料"]]
DEFAULT_PARAMS = {
    "premarket_auction_cutoff":"092900", "industry_pct_strength_leader":0.75, "industry_pct_inflow_leader":0.70,
    "industry_pct_strength_rising":0.50, "industry_pct_strength_absorb_dip":0.25, "industry_subplate_downgrade_steps":1,
    "super_ratio_top":0.4, "super_ratio_mid":0.3, "super_ratio_main_inflow_floor_wan":1000, "cashflow_effective_min_wan":300,
    "theme_history_top_n":20, "tech_profile_lookback_volume_days":20, "tech_profile_volume_ratio_min":0.5,
    "tech_profile_churn_volume_ratio_min":2.0, "tech_profile_churn_pct_chg_max":2.0,
    "zt_quality_clean_min":0.7, "zt_quality_average_min":0.4, "zt_quality_open_punish_max_count":3, "zt_quality_seal_target_ratio":0.3, "zt_quality_time_decay_minutes":240,
    "regime_warming_qx_today_min":35, "regime_warming_qx_yesterday_max":30, "regime_warming_lbbx_today_min":2, "regime_warming_lbbx_yesterday_max":0, "regime_downgrade_promo_rate_max":0.20,
    "ltgd_top_n_for_longtou":5, "ltgd_confirmed_board_min":4, "ltgd_confirmed_board3_rank_max":3,
}

def _norm_code(value: Any) -> str:
    m = re.search(r"(\d{6})", str(value or "")); return m.group(1) if m else ""

def _split_themes(*values: Any) -> List[str]:
    out, seen = [], set()
    for value in values:
        if value is None: continue
        parts = value if isinstance(value, list) else re.split(r"[|、,/，；;]+", str(value))
        for part in parts:
            token = re.sub(r"\s+", "", str(part or "").strip())
            for suffix in ("概念股", "概念", "板块", "题材"):
                if token.endswith(suffix) and len(token) > len(suffix): token = token[:-len(suffix)]; break
            if not token or token in {"-","暂无","无","首板","连板","反包"}: continue
            if len(token) < 2 and token not in {"AI","AR","VR","MR","5G","6G","CPO"}: continue
            if token not in seen: seen.add(token); out.append(token)
    return out

def _to_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""): return None
        return float(str(value).strip().rstrip("%"))
    except Exception:
        return None

def load_v7_1_config(project_root: Path) -> Dict[str, Any]:
    path = project_root / CONFIG_REL
    params = dict(DEFAULT_PARAMS)
    if path.exists() and yaml is not None:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            params.update(data.get("params") or {})
            return {"version": data.get("version") or "premarket_v7_1", "params": params, "theme_aliases": data.get("theme_aliases") or DEFAULT_ALIASES, "output": data.get("output") or {"max_candidates":30}}
        except Exception as exc:
            sys.stderr.write(f"[v7.1 runner WARN] config load failed {path}: {exc}; using defaults\n")
    return {"version":"premarket_v7_1", "params":params, "theme_aliases":DEFAULT_ALIASES, "output":{"max_candidates":30}}

def build_candidates_from_auction(bundle: Any, theme_aliases: Optional[List[List[str]]] = None) -> List[Dict[str, Any]]:
    candidates: Dict[str, Dict[str, Any]] = {}; canon_map = build_canon_map(theme_aliases or [])
    def ensure(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        code = _norm_code(row.get("code") or row.get("代码"))
        if not code: return None
        return candidates.setdefault(code, {"code":code, "name":str(row.get("name") or row.get("名称") or "").strip(), "source_hits":[], "source_hit_count":0, "matched_themes":[], "auction_change_pct":None, "latest_change_pct":None, "raw_rows":{}})
    def add(rows: List[Dict[str, Any]], source: str) -> None:
        for row in rows or []:
            if source == "fengdan" and str(row.get("section_kind") or "").strip() not in {"", "live"}: continue
            item = ensure(row)
            if item is None: continue
            if source not in item["source_hits"]: item["source_hits"].append(source)
            item["source_hit_count"] = len(item["source_hits"])
            if not item.get("name"): item["name"] = str(row.get("name") or row.get("名称") or "").strip()
            pct = _to_float(row.get("auction_change_pct") or row.get("竞价涨幅")); latest = _to_float(row.get("latest_change_pct") or row.get("涨幅") or row.get("最新涨幅"))
            if item["auction_change_pct"] is None and pct is not None: item["auction_change_pct"] = pct
            if item["latest_change_pct"] is None and latest is not None: item["latest_change_pct"] = latest
            for t in _split_themes(row.get("concept"), row.get("concept_1"), row.get("concept_2"), row.get("tag_1"), row.get("tag_2"), row.get("题材1"), row.get("题材2"), row.get("概念")):
                for x in [canonicalize(t, canon_map), t]:
                    if x and x not in item["matched_themes"]: item["matched_themes"].append(x)
            item["raw_rows"][source] = row
    add(bundle.auction_vratio, "vratio"); add(bundle.auction_qiangchou, "qiangchou"); add(bundle.auction_netamount, "net_amount"); add(bundle.auction_fengdan, "fengdan")
    return list(candidates.values())

def load_dailyline_dict(project_root: Path, codes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    out = {}; root = project_root / "dailyline" / "stocks"
    for code in codes:
        path = root / f"{code}.csv"
        if path.exists():
            try:
                with path.open("r", encoding="utf-8", newline="") as fp: out[code] = list(csv.DictReader(fp))
            except Exception as exc: sys.stderr.write(f"[v7.1 runner WARN] dailyline load failed {path}: {exc}\n")
    return out

def _alias_label_maps(raw_themes: List[str], label_map: Dict[str, Any], aliases: List[List[str]]) -> Dict[str, Any]:
    cm = build_canon_map(aliases); out = dict(label_map or {})
    for t in raw_themes:
        c = canonicalize(t, cm)
        if c in out and t not in out: out[t] = out[c]
    return out

def compute_all_labels(bundle: Any, candidates: List[Dict[str, Any]], config: Dict[str, Any], project_root: Path) -> Dict[str, Any]:
    params, aliases = config["params"], config["theme_aliases"]
    codes = [c["code"] for c in candidates]
    themes: List[str] = []
    for c in candidates:
        for t in c.get("matched_themes") or []:
            if t not in themes: themes.append(t)
    dailyline = load_dailyline_dict(project_root, codes)
    industry = compute_industry_t1_labels(themes, bundle.kaipan_t1_rows, bundle.kaipan_t1_meta, params, aliases)
    history = compute_theme_history_batch(themes, bundle.kaipan_history, params, aliases)
    return {
        "industry_t1": _alias_label_maps(themes, industry, aliases),
        "theme_history": _alias_label_maps(themes, history, aliases),
        "stock_t1": compute_stock_t1_labels(codes, bundle.cashflow_today_t1, bundle.cashflow_3day_t1, params),
        "cashflow_continuity": compute_cashflow_continuity(codes, bundle.cashflow_today_t1, bundle.cashflow_3day_t1, bundle.cashflow_5day_t1, bundle.cashflow_10day_t1, params),
        "zt": compute_zt_labels(codes, bundle.fupan_t1, bundle.ztpool_t1, params),
        "longtou": compute_longtou_status(codes, bundle.fupan_t1, bundle.ltgd_5day_t1, params),
        "tech_profile": compute_tech_profile(codes, dailyline, params),
        "regime": compute_regime({"rows":bundle.qxlive_top_t1_rows,"meta":bundle.qxlive_top_t1_meta}, {"rows":bundle.qxlive_top_t2_rows,"meta":bundle.qxlive_top_t2_meta}, params),
    }

def run_v7_1(date_str: str, project_root: Path, output_dir: Optional[Path] = None, no_write: bool = False) -> Dict[str, Any]:
    config = load_v7_1_config(project_root); params = config["params"]
    bundle = load_premarket_bundle(date_str, project_root, premarket_auction_cutoff=str(params.get("premarket_auction_cutoff", "092900")))
    candidates = build_candidates_from_auction(bundle, config.get("theme_aliases") or [])
    labels = compute_all_labels(bundle, candidates, config, project_root)
    max_candidates = int((config.get("output") or {}).get("max_candidates", 30))
    decisions = classify_candidates(candidates, labels, max_candidates=None)
    meta = {"date_t0":bundle.date_t0, "date_t1":bundle.date_t1, "date_t2":bundle.date_t2, "generated_at":datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"), "candidate_count":len(candidates), "bundle_summary":bundle.to_summary_dict(), "regime":labels.get("regime"), "warnings":bundle.warnings}
    shaped = shape_v7_1_output(decisions, meta=meta, max_candidates=max_candidates)
    if not no_write:
        if output_dir is None:
            output_dir = project_root / "reports" / date_str / "premarket"; analysis_name = f"{datetime.now(TZ_SHANGHAI).strftime('%H%M%S')}_analysis_v7_1.json"
        else:
            analysis_name = "analysis_v7_1.json"
        shaped["paths"] = write_v7_1_outputs(str(output_dir), decisions, meta=meta, max_candidates=max_candidates, analysis_filename=analysis_name)
    return shaped

def main() -> int:
    p = argparse.ArgumentParser(); p.add_argument("--date", required=True); p.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT)); p.add_argument("--output-dir", default=""); p.add_argument("--no-write", action="store_true"); p.add_argument("--json", action="store_true")
    a = p.parse_args(); r = run_v7_1(a.date, Path(a.project_root), Path(a.output_dir) if a.output_dir else None, a.no_write)
    print(json.dumps(r, ensure_ascii=False, indent=2, default=str) if a.json else f"v7.1 done date={a.date} candidates={r['meta']['candidate_count']} setup_stats={r['setup_stats']}")
    return 0
if __name__ == "__main__": raise SystemExit(main())
