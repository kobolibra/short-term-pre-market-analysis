#!/usr/bin/env python3
"""
duanxianxia_premarket_v7_1_runner.py — v7.1 盘前分析旁路 runner

不抓新数据,只读取 captures/<date>/... 已落盘快照,生成 v7.1 analysis + intraday_anchors。
D7 前不替换 v7.0 cron。
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

try:
    import yaml  # type: ignore
except Exception:  # noqa: BLE001
    yaml = None

from duanxianxia_v7_1_data_loader import load_premarket_bundle
from duanxianxia_v7_1_industry_t1_label import compute_industry_t1_labels
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

DEFAULT_PARAMS: Dict[str, Any] = {
    "industry_pct_strength_leader": 0.75,
    "industry_pct_inflow_leader": 0.70,
    "industry_pct_strength_rising": 0.50,
    "industry_pct_strength_absorb_dip": 0.25,
    "industry_subplate_downgrade_steps": 1,
    "super_ratio_top": 0.4,
    "super_ratio_mid": 0.3,
    "super_ratio_main_inflow_floor_wan": 1000,
    "theme_history_top_n": 20,
    "tech_profile_lookback_volume_days": 20,
    "tech_profile_volume_ratio_min": 0.5,
    "zt_quality_clean_min": 0.7,
    "zt_quality_average_min": 0.4,
    "zt_quality_open_punish_max_count": 3,
    "zt_quality_seal_target_ratio": 0.3,
    "zt_quality_time_decay_minutes": 240,
    "regime_warming_qx_today_min": 35,
    "regime_warming_qx_yesterday_max": 30,
    "regime_warming_lbbx_today_min": 2,
    "regime_warming_lbbx_yesterday_max": 0,
    "regime_downgrade_promo_rate_max": 0.20,
    "ltgd_top_n_for_longtou": 5,
}

DEFAULT_ALIASES: List[List[str]] = [
    ["业绩增长", "一季报增长", "年报增长", "业绩预增", "预增", "半年报增长"],
    ["半导体产业链", "芯片", "集成电路", "元器件", "芯片封测", "封测", "先进封装", "CPU"],
    ["光通信", "通信", "光模块", "CPO"],
    ["华为", "华为海思", "华为产业链", "鸿蒙"],
    ["算力", "算力概念", "AI算力", "AI服务器", "IDC", "数据中心", "AI智能体"],
    ["大消费", "零售", "服装家纺", "猪肉", "农业", "食品饮料"],
]


def _norm_code(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"(\d{6})", text)
    return match.group(1) if match else ""


def _split_themes(*values: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        if value is None:
            continue
        parts = value if isinstance(value, list) else re.split(r"[|、,/，；;]+", str(value))
        for part in parts:
            token = str(part or "").strip()
            token = re.sub(r"\s+", "", token)
            for suffix in ("概念股", "概念", "板块", "题材"):
                if token.endswith(suffix) and len(token) > len(suffix):
                    token = token[: -len(suffix)]
                    break
            if not token or token in {"-", "暂无", "无", "首板", "连板", "反包"}:
                continue
            if len(token) < 2 and token not in {"AI", "AR", "VR", "MR", "5G", "6G", "CPO"}:
                continue
            if token not in seen:
                seen.add(token)
                out.append(token)
    return out


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).strip().rstrip("%"))
    except Exception:
        return None


def load_v7_1_config(project_root: Path) -> Dict[str, Any]:
    path = project_root / CONFIG_REL
    if path.exists() and yaml is not None:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            params = dict(DEFAULT_PARAMS)
            params.update(data.get("params") or {})
            return {
                "version": data.get("version") or "premarket_v7_1",
                "params": params,
                "theme_aliases": data.get("theme_aliases") or DEFAULT_ALIASES,
                "output": data.get("output") or {"max_candidates": 30},
            }
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[v7.1 runner WARN] failed loading {path}: {exc}; using defaults\n")
    return {"version": "premarket_v7_1", "params": dict(DEFAULT_PARAMS), "theme_aliases": DEFAULT_ALIASES, "output": {"max_candidates": 30}}


def build_candidates_from_auction(bundle: Any) -> List[Dict[str, Any]]:
    candidates: Dict[str, Dict[str, Any]] = {}

    def ensure(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        code = _norm_code(row.get("code") or row.get("代码"))
        if not code:
            return None
        item = candidates.setdefault(code, {
            "code": code,
            "name": str(row.get("name") or row.get("名称") or "").strip(),
            "source_hits": [],
            "source_hit_count": 0,
            "matched_themes": [],
            "auction_change_pct": None,
            "latest_change_pct": None,
            "raw_rows": {},
        })
        if not item.get("name"):
            item["name"] = str(row.get("name") or row.get("名称") or "").strip()
        return item

    def add_source(rows: List[Dict[str, Any]], source: str) -> None:
        for row in rows or []:
            # fengdan 只用 live 区段;其他表无 section_kind
            if source == "fengdan" and str(row.get("section_kind") or "").strip() not in {"", "live"}:
                continue
            item = ensure(row)
            if item is None:
                continue
            if source not in item["source_hits"]:
                item["source_hits"].append(source)
            item["source_hit_count"] = len(item["source_hits"])
            pct = _to_float(row.get("auction_change_pct") or row.get("竞价涨幅"))
            latest = _to_float(row.get("latest_change_pct") or row.get("涨幅") or row.get("最新涨幅"))
            if item.get("auction_change_pct") is None and pct is not None:
                item["auction_change_pct"] = pct
            if item.get("latest_change_pct") is None and latest is not None:
                item["latest_change_pct"] = latest
            themes = _split_themes(row.get("concept"), row.get("concept_1"), row.get("concept_2"), row.get("tag_1"), row.get("tag_2"), row.get("题材1"), row.get("题材2"), row.get("概念"))
            for t in themes:
                if t not in item["matched_themes"]:
                    item["matched_themes"].append(t)
            item["raw_rows"][source] = row

    add_source(bundle.auction_vratio, "vratio")
    add_source(bundle.auction_qiangchou, "qiangchou")
    add_source(bundle.auction_netamount, "net_amount")
    add_source(bundle.auction_fengdan, "fengdan")
    return list(candidates.values())


def load_dailyline_dict(project_root: Path, codes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    root = project_root / "dailyline" / "stocks"
    for code in codes:
        path = root / f"{code}.csv"
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8", newline="") as fp:
                out[code] = list(csv.DictReader(fp))
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[v7.1 runner WARN] dailyline load failed {path}: {exc}\n")
    return out


def compute_all_labels(bundle: Any, candidates: List[Dict[str, Any]], config: Dict[str, Any], project_root: Path) -> Dict[str, Any]:
    params = config["params"]
    aliases = config["theme_aliases"]
    codes = [c["code"] for c in candidates]
    themes: List[str] = []
    for c in candidates:
        for t in c.get("matched_themes") or []:
            if t not in themes:
                themes.append(t)

    dailyline = load_dailyline_dict(project_root, codes)
    qx_t1 = {"rows": bundle.qxlive_top_t1_rows, "meta": bundle.qxlive_top_t1_meta}
    qx_t2 = {"rows": bundle.qxlive_top_t2_rows, "meta": bundle.qxlive_top_t2_meta}

    return {
        "industry_t1": compute_industry_t1_labels(themes, bundle.kaipan_t1_rows, bundle.kaipan_t1_meta, params, aliases),
        "theme_history": compute_theme_history_batch(themes, bundle.kaipan_history, params, aliases),
        "stock_t1": compute_stock_t1_labels(codes, bundle.cashflow_today_t1, bundle.cashflow_3day_t1, params),
        "cashflow_continuity": compute_cashflow_continuity(codes, bundle.cashflow_today_t1, bundle.cashflow_3day_t1, bundle.cashflow_5day_t1, bundle.cashflow_10day_t1, params),
        "zt": compute_zt_labels(codes, bundle.fupan_t1, bundle.ztpool_t1, params),
        "longtou": compute_longtou_status(codes, bundle.fupan_t1, bundle.ltgd_5day_t1, params),
        "tech_profile": compute_tech_profile(codes, dailyline, params),
        "regime": compute_regime(qx_t1, qx_t2, params),
    }


def run_v7_1(date_str: str, project_root: Path, output_dir: Optional[Path] = None, no_write: bool = False) -> Dict[str, Any]:
    config = load_v7_1_config(project_root)
    bundle = load_premarket_bundle(date_str, project_root)
    candidates = build_candidates_from_auction(bundle)
    labels = compute_all_labels(bundle, candidates, config, project_root)
    max_candidates = int((config.get("output") or {}).get("max_candidates", 30))
    decisions = classify_candidates(candidates, labels, max_candidates=None)
    meta = {
        "date_t0": bundle.date_t0,
        "date_t1": bundle.date_t1,
        "date_t2": bundle.date_t2,
        "generated_at": datetime.now(TZ_SHANGHAI).isoformat(timespec="seconds"),
        "candidate_count": len(candidates),
        "bundle_summary": bundle.to_summary_dict(),
        "regime": labels.get("regime"),
        "warnings": bundle.warnings,
    }
    shaped = shape_v7_1_output(decisions, meta=meta, max_candidates=max_candidates)

    if not no_write:
        if output_dir is None:
            stamp = datetime.now(TZ_SHANGHAI).strftime("%H%M%S")
            output_dir = project_root / "reports" / date_str / "premarket"
            analysis_name = f"{stamp}_analysis_v7_1.json"
        else:
            analysis_name = "analysis_v7_1.json"
        paths = write_v7_1_outputs(str(output_dir), decisions, meta=meta, max_candidates=max_candidates, analysis_filename=analysis_name)
        shaped["paths"] = paths
    return shaped


def main() -> int:
    parser = argparse.ArgumentParser(description="Run v7.1 premarket analysis from existing captures")
    parser.add_argument("--date", required=True, help="T-0 date YYYY-MM-DD")
    parser.add_argument("--project-root", default=str(DEFAULT_PROJECT_ROOT))
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = run_v7_1(
        date_str=args.date,
        project_root=Path(args.project_root),
        output_dir=Path(args.output_dir) if args.output_dir else None,
        no_write=args.no_write,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    else:
        print(f"v7.1 done date={args.date} candidates={result['meta']['candidate_count']} setup_stats={result['setup_stats']}")
        if result.get("paths"):
            print(json.dumps(result["paths"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
