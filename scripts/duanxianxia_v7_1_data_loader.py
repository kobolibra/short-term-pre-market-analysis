"""
duanxianxia_v7_1_data_loader.py — v7.1 capture 数据加载器

严格时点隔离:
- T0 竞价只允许读取 premarket_auction_cutoff 之前的 capture,默认 09:29:00。
- 若 cutoff 之前没有 capture,直接视为缺失,禁止回退到盘中/盘后 capture。
- T-1/T-2 qxlive top_metrics 只取 ≤09:33 的早盘首批快照（覆盖历史首包时间漂移,避免误报缺失）。
- T0 板块汇总/qxlive 顶部指标同样只取 cutoff 之前的早盘快照,缺失即视为缺失(不前视)。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CAPTURE_DIRNAME = "captures"
DS_AUCTION_VRATIO = "auction.jjyd.vratio"
DS_AUCTION_QIANGCHOU = "auction.jjyd.qiangchou"
DS_AUCTION_NETAMOUNT = "auction.jjyd.net_amount"
DS_AUCTION_FENGDAN = "auction.jjlive.fengdan"
DS_AUCTION_WEIMAI = "auction.jjyd.weimai"   # 竞价异动/涨停委买
DS_HOME_KAIPAN = "home.kaipan.plate.summary"
DS_HOME_ZTPOOL = "home.ztpool"
DS_HOME_QXLIVE_TOP = "home.qxlive.top_metrics"
DS_REVIEW_FUPAN = "review.fupan.plate"
DS_REVIEW_LTGD = "review.ltgd.range"
DS_CASHFLOW_TODAY = "cashflow.stock.today"
DS_CASHFLOW_3DAY = "cashflow.stock.3day"
DS_CASHFLOW_5DAY = "cashflow.stock.5day"
DS_CASHFLOW_10DAY = "cashflow.stock.10day"
QXLIVE_PREMARKET_BOUNDARY_HHMMSS = "093300"
PREMARKET_AUCTION_CUTOFF_HHMMSS = "092900"
DEFAULT_KAIPAN_HISTORY_DAYS = 10
_HHMMSS_FILE_PATTERN = re.compile(r"^(\d{6})\.json$")
_MAX_LOOKBACK_DAYS = 30

class DataLoaderError(Exception):
    pass
class CaptureNotFoundError(DataLoaderError):
    pass
class CaptureFormatError(DataLoaderError):
    pass

def _capture_dir(project_root: Path, date_str: str, dataset_id: str) -> Path:
    return project_root / CAPTURE_DIRNAME / date_str / dataset_id

def _list_capture_files(dir_path: Path) -> List[Tuple[str, Path]]:
    if not dir_path.exists() or not dir_path.is_dir():
        return []
    out: List[Tuple[str, Path]] = []
    for p in sorted(dir_path.iterdir()):
        m = _HHMMSS_FILE_PATTERN.match(p.name)
        if m:
            out.append((m.group(1), p))
    out.sort(key=lambda x: x[0])
    return out

def _load_capture_json(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise DataLoaderError(f"Failed to load {path}: {e}") from e
    if not isinstance(data, dict):
        raise CaptureFormatError(f"Capture {path} top-level is not dict")
    return data

def load_capture_at_time(project_root: Path, date_str: str, dataset_id: str, *, max_hhmmss: Optional[str] = None, pick: str = "earliest_before", raise_if_missing: bool = True) -> Optional[Dict[str, Any]]:
    files = _list_capture_files(_capture_dir(project_root, date_str, dataset_id))
    if not files:
        if raise_if_missing:
            raise CaptureNotFoundError(f"No capture for {dataset_id} at {date_str}")
        return None
    if pick == "latest" and max_hhmmss is None:
        return _load_capture_json(files[-1][1])
    eligible = [(t, p) for (t, p) in files if max_hhmmss is None or t <= max_hhmmss]
    if not eligible:
        msg = f"No {dataset_id} capture at {date_str} <= {max_hhmmss}; refusing after-cutoff fallback to avoid lookahead"
        if raise_if_missing:
            raise CaptureNotFoundError(msg)
        sys.stderr.write(f"[data_loader WARN] {msg}\n")
        return None
    if pick == "earliest_before":
        return _load_capture_json(eligible[0][1])
    if pick in {"latest_before", "latest"}:
        return _load_capture_json(eligible[-1][1])
    raise ValueError(f"Unknown pick mode: {pick}")

def _extract_rows(capture: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if capture is None:
        return []
    rows = capture.get("rows")
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise CaptureFormatError(f"Capture rows is not list")
    return rows

def _extract_meta(capture: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if capture is None:
        return {}
    meta = capture.get("meta")
    if meta is None:
        return {}
    if not isinstance(meta, dict):
        raise CaptureFormatError(f"Capture meta is not dict")
    return meta

def _date_has_kaipan_capture(project_root: Path, d: date) -> bool:
    return bool(_list_capture_files(_capture_dir(project_root, d.isoformat(), DS_HOME_KAIPAN)))

def previous_trading_day(project_root: Path, d: date, *, n: int = 1, max_lookback: int = _MAX_LOOKBACK_DAYS) -> date:
    cur = d
    found = 0
    for _ in range(max_lookback):
        cur -= timedelta(days=1)
        if _date_has_kaipan_capture(project_root, cur):
            found += 1
            if found >= n:
                return cur
    sys.stderr.write(f"[data_loader WARN] previous_trading_day fallback to weekday before {d}\n")
    cur = d
    found = 0
    while found < n:
        cur -= timedelta(days=1)
        if cur.weekday() < 5:
            found += 1
    return cur

@dataclass
class PremarketDataBundle:
    date_t0: str
    date_t1: str
    date_t2: Optional[str]
    project_root: str
    auction_vratio: List[Dict[str, Any]]
    auction_qiangchou: List[Dict[str, Any]]
    auction_netamount: List[Dict[str, Any]]
    auction_fengdan: List[Dict[str, Any]]
    auction_weimai: List[Dict[str, Any]]
    kaipan_t1_rows: List[Dict[str, Any]]
    kaipan_t1_meta: Dict[str, Any]
    cashflow_today_t1: List[Dict[str, Any]]
    cashflow_3day_t1: List[Dict[str, Any]]
    cashflow_5day_t1: List[Dict[str, Any]]
    cashflow_10day_t1: List[Dict[str, Any]]
    fupan_t1: List[Dict[str, Any]]
    ltgd_5day_t1: List[Dict[str, Any]]
    ztpool_t1: List[Dict[str, Any]]
    qxlive_top_t1_rows: List[Dict[str, Any]]
    qxlive_top_t1_meta: Dict[str, Any]
    qxlive_top_t2_rows: List[Dict[str, Any]]
    qxlive_top_t2_meta: Dict[str, Any]
    kaipan_history: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    # T0 早盘快照(≤cutoff/≤09:33),供 v9 全量层使用;缺失即为空,不前视。
    kaipan_t0_rows: List[Dict[str, Any]] = field(default_factory=list)
    kaipan_t0_meta: Dict[str, Any] = field(default_factory=dict)
    qxlive_top_t0_rows: List[Dict[str, Any]] = field(default_factory=list)
    qxlive_top_t0_meta: Dict[str, Any] = field(default_factory=dict)
    # v4 新增: T-1 盘后 qxlive 指标 (收盘, 不限时间, 取最新, 用于水位计算)
    qxlive_close_t1_rows: List[Dict[str, Any]] = field(default_factory=list)
    qxlive_close_t1_meta: Dict[str, Any] = field(default_factory=dict)

    def to_summary_dict(self) -> Dict[str, Any]:
        return {
            "date_t0": self.date_t0, "date_t1": self.date_t1, "date_t2": self.date_t2, "project_root": self.project_root,
            "counts": {"auction_vratio": len(self.auction_vratio), "auction_qiangchou": len(self.auction_qiangchou), "auction_netamount": len(self.auction_netamount), "auction_fengdan": len(self.auction_fengdan), "auction_weimai": len(self.auction_weimai), "kaipan_t1": len(self.kaipan_t1_rows), "kaipan_t0": len(self.kaipan_t0_rows), "cashflow_today_t1": len(self.cashflow_today_t1), "cashflow_3day_t1": len(self.cashflow_3day_t1), "cashflow_5day_t1": len(self.cashflow_5day_t1), "cashflow_10day_t1": len(self.cashflow_10day_t1), "fupan_t1": len(self.fupan_t1), "ltgd_5day_t1": len(self.ltgd_5day_t1), "ztpool_t1": len(self.ztpool_t1), "qxlive_top_t1": len(self.qxlive_top_t1_rows), "qxlive_top_t0": len(self.qxlive_top_t0_rows), "qxlive_top_t2": len(self.qxlive_top_t2_rows), "kaipan_history": len(self.kaipan_history)},
            "kaipan_t1_meta_keys": sorted(self.kaipan_t1_meta.keys()), "warnings": self.warnings,
        }

def load_premarket_bundle(date_t0: str, project_root: Path | str, *, history_days: int = DEFAULT_KAIPAN_HISTORY_DAYS, qxlive_premarket_boundary: str = QXLIVE_PREMARKET_BOUNDARY_HHMMSS, premarket_auction_cutoff: str = PREMARKET_AUCTION_CUTOFF_HHMMSS, require_t2: bool = False) -> PremarketDataBundle:
    project_root = Path(project_root)
    if not project_root.exists():
        raise DataLoaderError(f"project_root does not exist: {project_root}")
    warnings: List[str] = []
    d_t0 = date.fromisoformat(date_t0)
    d_t1 = previous_trading_day(project_root, d_t0, n=1)
    try:
        d_t2 = previous_trading_day(project_root, d_t0, n=2)
        date_t2_str: Optional[str] = d_t2.isoformat()
    except Exception:
        if require_t2:
            raise
        date_t2_str = None
        warnings.append("T-2 trading day not found")
    date_t1_str = d_t1.isoformat()

    def _t0(ds: str) -> List[Dict[str, Any]]:
        return _extract_rows(load_capture_at_time(project_root, date_t0, ds, max_hhmmss=premarket_auction_cutoff, pick="earliest_before", raise_if_missing=True))

    auction_vratio = _t0(DS_AUCTION_VRATIO)
    auction_qiangchou = _t0(DS_AUCTION_QIANGCHOU)
    auction_netamount = _t0(DS_AUCTION_NETAMOUNT)
    auction_fengdan = _t0(DS_AUCTION_FENGDAN)
    auction_weimai = _t0(DS_AUCTION_WEIMAI)

    kaipan_t1 = load_capture_at_time(project_root, date_t1_str, DS_HOME_KAIPAN, pick="latest")
    kaipan_t1_rows = _extract_rows(kaipan_t1)
    kaipan_t1_meta = _extract_meta(kaipan_t1)

    # T0 板块汇总:今日早盘 ≤cutoff 的首批快照(题材强度/资金/涨停数的当日口径)。
    kaipan_t0 = load_capture_at_time(project_root, date_t0, DS_HOME_KAIPAN, max_hhmmss=premarket_auction_cutoff, pick="earliest_before", raise_if_missing=False)
    kaipan_t0_rows = _extract_rows(kaipan_t0)
    kaipan_t0_meta = _extract_meta(kaipan_t0)
    if not kaipan_t0_rows:
        warnings.append(f"missing_or_empty: {DS_HOME_KAIPAN} t0")

    def _try(ds: str, d: str = date_t1_str, *, pick: str = "latest", max_hhmmss: Optional[str] = None) -> List[Dict[str, Any]]:
        cap = load_capture_at_time(project_root, d, ds, pick=pick, max_hhmmss=max_hhmmss, raise_if_missing=False)
        if cap is None:
            warnings.append(f"missing capture: {ds} {d}")
            return []
        return _extract_rows(cap)

    cashflow_today_t1 = _try(DS_CASHFLOW_TODAY)
    cashflow_3day_t1 = _try(DS_CASHFLOW_3DAY)
    cashflow_5day_t1 = _try(DS_CASHFLOW_5DAY)
    cashflow_10day_t1 = _try(DS_CASHFLOW_10DAY)
    fupan_t1 = _try(DS_REVIEW_FUPAN)
    ltgd_all = _try(DS_REVIEW_LTGD)
    ltgd_5day_t1 = [r for r in ltgd_all if str(r.get("周期", "") or "").strip() == "5日"]
    ztpool_t1 = _try(DS_HOME_ZTPOOL)
    # home.ztpool 的 rows 是单只股票行，每行含 ladder_group + promo_rate
    # 晋级率在每行都有，需要按 ladder_group 聚合
    # 如果 _try 返回空/不完整，直接重新加载 full capture

    # T0 qxlive 顶部指标:今日早盘 ≤09:33 首批快照(当日市场环境/regime 口径)。
    q0 = load_capture_at_time(project_root, date_t0, DS_HOME_QXLIVE_TOP, max_hhmmss=qxlive_premarket_boundary, pick="earliest_before", raise_if_missing=False)
    qxlive_top_t0_rows = _extract_rows(q0); qxlive_top_t0_meta = _extract_meta(q0)
    if not qxlive_top_t0_rows:
        warnings.append(f"missing_or_empty: {DS_HOME_QXLIVE_TOP} t0")

    q1 = load_capture_at_time(project_root, date_t1_str, DS_HOME_QXLIVE_TOP, max_hhmmss=qxlive_premarket_boundary, pick="earliest_before", raise_if_missing=False)
    qxlive_top_t1_rows = _extract_rows(q1); qxlive_top_t1_meta = _extract_meta(q1)
    if date_t2_str:
        q2 = load_capture_at_time(project_root, date_t2_str, DS_HOME_QXLIVE_TOP, max_hhmmss=qxlive_premarket_boundary, pick="earliest_before", raise_if_missing=False)
        qxlive_top_t2_rows = _extract_rows(q2); qxlive_top_t2_meta = _extract_meta(q2)
    else:
        qxlive_top_t2_rows = []; qxlive_top_t2_meta = {}

    # v4 新增: T-1 盘后 qxlive 指标 (收盘, 不限时间, 取最新, 用于水位计算)
    qc1 = load_capture_at_time(project_root, date_t1_str, DS_HOME_QXLIVE_TOP,
                                pick="latest", raise_if_missing=False)
    qxlive_close_t1_rows = _extract_rows(qc1)
    qxlive_close_t1_meta = _extract_meta(qc1) if qc1 else {}

    kaipan_history: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]] = []
    cur = d_t1
    for _ in range(history_days):
        ds = cur.isoformat()
        cap = load_capture_at_time(project_root, ds, DS_HOME_KAIPAN, pick="latest", raise_if_missing=False)
        if cap is not None:
            kaipan_history.append((ds, _extract_rows(cap), _extract_meta(cap)))
        cur = previous_trading_day(project_root, cur, n=1)

    return PremarketDataBundle(date_t0, date_t1_str, date_t2_str, str(project_root), auction_vratio, auction_qiangchou, auction_netamount, auction_fengdan, auction_weimai, kaipan_t1_rows, kaipan_t1_meta, cashflow_today_t1, cashflow_3day_t1, cashflow_5day_t1, cashflow_10day_t1, fupan_t1, ltgd_5day_t1, ztpool_t1, qxlive_top_t1_rows, qxlive_top_t1_meta, qxlive_top_t2_rows, qxlive_top_t2_meta, kaipan_history, warnings, kaipan_t0_rows=kaipan_t0_rows, kaipan_t0_meta=kaipan_t0_meta, qxlive_top_t0_rows=qxlive_top_t0_rows, qxlive_top_t0_meta=qxlive_top_t0_meta, qxlive_close_t1_rows=qxlive_close_t1_rows, qxlive_close_t1_meta=qxlive_close_t1_meta)

def _main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True); p.add_argument("--project-root", required=True); p.add_argument("--history-days", type=int, default=DEFAULT_KAIPAN_HISTORY_DAYS); p.add_argument("--premarket-auction-cutoff", default=PREMARKET_AUCTION_CUTOFF_HHMMSS); p.add_argument("--full", action="store_true")
    a = p.parse_args()
    b = load_premarket_bundle(a.date, a.project_root, history_days=a.history_days, premarket_auction_cutoff=a.premarket_auction_cutoff)
    print(json.dumps(asdict(b) if a.full else b.to_summary_dict(), ensure_ascii=False, indent=2, default=str))
    return 0
if __name__ == "__main__":
    sys.exit(_main())
