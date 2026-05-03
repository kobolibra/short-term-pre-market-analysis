"""v7.2 data loader: wrap v7.1 bundle and add T0 hotness/qxlive datasets.

New optional T0 captures:
- rank.rocket                 (飙升榜)
- rank.hot_stock_day          (热度榜)
- home.kaipan.plate.summary   (板块汇总，用于 T0 theme_strength)
- home.qxlive.top_metrics     (主页 qxlive 顶部指标，用于 T0 regime)

This loader is intentionally tolerant: if some datasets are missing, v7.2
continues to run and corresponding T0 signals are treated as missing instead of
being silently forced to zero.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from duanxianxia_v7_1_data_loader import load_premarket_bundle

DS_RANK_ROCKET = "rank.rocket"
DS_RANK_HOT_STOCK_DAY = "rank.hot_stock_day"
DS_KAIPAN_PLATE_SUMMARY = "home.kaipan.plate.summary"
DS_HOME_QXLIVE_TOP = "home.qxlive.top_metrics"


@dataclass
class PremarketV72Bundle:
    v71: Any
    rocket_rows: List[Dict[str, Any]]
    hot_stock_day_rows: List[Dict[str, Any]]
    kaipan_plate_t0_rows: List[Dict[str, Any]]
    qxlive_top_t0_rows: List[Dict[str, Any]]
    warnings: List[str]

    @property
    def date_t0(self) -> str:
        return self.v71.date_t0

    @property
    def date_t1(self) -> str:
        return self.v71.date_t1

    @property
    def date_t2(self) -> str:
        return self.v71.date_t2

    def to_summary_dict(self) -> Dict[str, Any]:
        base = self.v71.to_summary_dict() if hasattr(self.v71, "to_summary_dict") else {}
        base.update({
            "rocket_rows": len(self.rocket_rows),
            "hot_stock_day_rows": len(self.hot_stock_day_rows),
            "kaipan_plate_t0_rows": len(self.kaipan_plate_t0_rows),
            "qxlive_top_t0_rows": len(self.qxlive_top_t0_rows),
            "v72_warnings": self.warnings,
        })
        return base


def _read_json_rows(path: Path) -> List[Dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("rows", "data", "items", "list", "result"):
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        return [data]
    return []


def _latest_capture(
    project_root: Path,
    date_str: str,
    dataset_id: str,
    cutoff_hhmmss: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    root = project_root / "captures" / date_str / dataset_id
    if not root.exists():
        return [], None
    files = sorted(root.glob("*.json"))
    if cutoff_hhmmss:
        files = [p for p in files if p.stem <= cutoff_hhmmss]
    if not files:
        return [], None
    path = files[-1]
    return _read_json_rows(path), str(path)


def load_premarket_v72_bundle(
    date_str: str,
    project_root: Path,
    premarket_auction_cutoff: str = "092900",
) -> PremarketV72Bundle:
    v71 = load_premarket_bundle(
        date_str,
        project_root,
        premarket_auction_cutoff=premarket_auction_cutoff,
    )
    warnings: List[str] = []

    rocket, _rocket_path = _latest_capture(project_root, date_str, DS_RANK_ROCKET, premarket_auction_cutoff)
    hotday, _hotday_path = _latest_capture(project_root, date_str, DS_RANK_HOT_STOCK_DAY, premarket_auction_cutoff)
    kaipan_t0, _kaipan_path = _latest_capture(project_root, date_str, DS_KAIPAN_PLATE_SUMMARY, premarket_auction_cutoff)
    qxlive_t0, _qxlive_path = _latest_capture(project_root, date_str, DS_HOME_QXLIVE_TOP, premarket_auction_cutoff)

    if not rocket:
        warnings.append(f"missing_or_empty:{DS_RANK_ROCKET}")
    if not hotday:
        warnings.append(f"missing_or_empty:{DS_RANK_HOT_STOCK_DAY}")
    if not kaipan_t0:
        warnings.append(f"missing_or_empty:{DS_KAIPAN_PLATE_SUMMARY}:t0")
    if not qxlive_t0:
        warnings.append(f"missing_or_empty:{DS_HOME_QXLIVE_TOP}:t0")

    if hasattr(v71, "warnings") and v71.warnings:
        warnings.extend(v71.warnings)

    return PremarketV72Bundle(
        v71=v71,
        rocket_rows=rocket,
        hot_stock_day_rows=hotday,
        kaipan_plate_t0_rows=kaipan_t0,
        qxlive_top_t0_rows=qxlive_t0,
        warnings=warnings,
    )
