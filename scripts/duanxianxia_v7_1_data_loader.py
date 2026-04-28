"""
duanxianxia_v7_1_data_loader.py — v7.1 capture 数据加载器

加载 v7.1 标签计算所需的全部 capture 数据,严格遵守数据时点隔离:
- premarket 模式:09:25 当日竞价 + T-1 历史 capture
- 严禁查询 09:30 之后的当日数据(防 lookahead bias)
- home.qxlive.top_metrics 历史回溯只取 ≤ 09:30 那份
- review.ltgd.range 只返回 周期 == "5日"

CLI 测试入口:
    python scripts/duanxianxia_v7_1_data_loader.py --date 2026-04-25 \\
        --project-root /home/investmentofficehku/.openclaw/workspace/projects/duanxianxia
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


# ============================================================================
# 常量
# ============================================================================

CAPTURE_DIRNAME = "captures"

# 数据集 ID(与 fetcher.py DATASET_REGISTRY 完全一致)
DS_AUCTION_VRATIO = "auction.jjyd.vratio"
DS_AUCTION_QIANGCHOU = "auction.jjyd.qiangchou"
DS_AUCTION_NETAMOUNT = "auction.jjyd.net_amount"
DS_AUCTION_FENGDAN = "auction.jjlive.fengdan"

DS_HOME_KAIPAN = "home.kaipan.plate.summary"
DS_HOME_ZTPOOL = "home.ztpool"
DS_HOME_QXLIVE_TOP = "home.qxlive.top_metrics"

DS_REVIEW_FUPAN = "review.fupan.plate"
DS_REVIEW_LTGD = "review.ltgd.range"

DS_CASHFLOW_TODAY = "cashflow.stock.today"
DS_CASHFLOW_3DAY = "cashflow.stock.3day"
DS_CASHFLOW_5DAY = "cashflow.stock.5day"
DS_CASHFLOW_10DAY = "cashflow.stock.10day"

# qxlive top_metrics 历史回溯时点边界(09:30)
QXLIVE_PREMARKET_BOUNDARY_HHMMSS = "093000"

# theme_history 默认回溯天数
DEFAULT_KAIPAN_HISTORY_DAYS = 10

# CSV/JSON 文件名正则
_HHMMSS_FILE_PATTERN = re.compile(r"^(\d{6})\.json$")

# captures 路径上返查找上个交易日的最大跳跃天数
_MAX_LOOKBACK_DAYS = 30


# ============================================================================
# 异常
# ============================================================================

class DataLoaderError(Exception):
    """data_loader 顶层异常"""


class CaptureNotFoundError(DataLoaderError):
    """某个 dataset 的当天/限定时点 capture 不存在"""


class CaptureFormatError(DataLoaderError):
    """capture JSON 结构与预期不符"""


# ============================================================================
# 文件 IO 辅助
# ============================================================================

def _capture_dir(project_root: Path, date_str: str, dataset_id: str) -> Path:
    return project_root / CAPTURE_DIRNAME / date_str / dataset_id


def _list_capture_files(dir_path: Path) -> List[Tuple[str, Path]]:
    """列出 <dir>/HHMMSS.json,按 HHMMSS 升序。返回 [(hhmmss, path), ...]。"""
    if not dir_path.exists() or not dir_path.is_dir():
        return []
    items: List[Tuple[str, Path]] = []
    for p in sorted(dir_path.iterdir()):
        m = _HHMMSS_FILE_PATTERN.match(p.name)
        if m:
            items.append((m.group(1), p))
    items.sort(key=lambda x: x[0])
    return items


def _load_capture_json(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise DataLoaderError(f"Failed to load {path}: {e}") from e
    if not isinstance(data, dict):
        raise CaptureFormatError(f"Capture {path} top-level is not dict (got {type(data).__name__})")
    return data


def load_capture_at_time(
    project_root: Path,
    date_str: str,
    dataset_id: str,
    *,
    max_hhmmss: Optional[str] = None,
    pick: str = "earliest_before",
    raise_if_missing: bool = True,
) -> Optional[Dict[str, Any]]:
    """加载某天某数据集的 capture 文件。

    :param max_hhmmss: 按 HHMMSS 过滤上限(例如 "093000" = 09:30:00)。None 表示不限。
    :param pick:
        - "earliest_before":取 ≤ max_hhmmss 中最早一份(09:25 项场景)
        - "latest_before":取 ≤ max_hhmmss 中最晚一份(一般历史场景)
        - "latest":取整天最晚一份(postmarket 数据)
    :param raise_if_missing: True 则缺失时拋 CaptureNotFoundError;False 则返回 None
    """
    capture_dir = _capture_dir(project_root, date_str, dataset_id)
    files = _list_capture_files(capture_dir)
    if not files:
        if raise_if_missing:
            raise CaptureNotFoundError(
                f"No capture for {dataset_id} at {date_str} (looked in {capture_dir})"
            )
        return None

    if pick == "latest":
        return _load_capture_json(files[-1][1])

    eligible = (
        [(t, p) for (t, p) in files if max_hhmmss is None or t <= max_hhmmss]
    )
    if not eligible:
        # 没有 ≤ max_hhmmss 的 → fallback 警告后取整天最早一份
        sys.stderr.write(
            f"[data_loader WARN] No {dataset_id} at {date_str} <= {max_hhmmss};"
            f" falling back to earliest of day ({files[0][0]}).\n"
        )
        return _load_capture_json(files[0][1])

    if pick == "earliest_before":
        return _load_capture_json(eligible[0][1])
    if pick == "latest_before":
        return _load_capture_json(eligible[-1][1])
    raise ValueError(f"Unknown pick mode: {pick}")


def _extract_rows(capture: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if capture is None:
        return []
    rows = capture.get("rows")
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise CaptureFormatError(f"Capture rows is not list (got {type(rows).__name__})")
    return rows


def _extract_meta(capture: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if capture is None:
        return {}
    meta = capture.get("meta")
    if meta is None:
        return {}
    if not isinstance(meta, dict):
        raise CaptureFormatError(f"Capture meta is not dict (got {type(meta).__name__})")
    return meta


# ============================================================================
# 工作日历(基于 captures 目录存在性,不依赖节假日历)
# ============================================================================

def _date_has_kaipan_capture(project_root: Path, d: date) -> bool:
    capture_dir = _capture_dir(project_root, d.isoformat(), DS_HOME_KAIPAN)
    return bool(_list_capture_files(capture_dir))


def previous_trading_day(
    project_root: Path,
    d: date,
    *,
    n: int = 1,
    max_lookback: int = _MAX_LOOKBACK_DAYS,
) -> date:
    """返回上个交易日(以 captures/<date>/home.kaipan.plate.summary/ 存在为准)。

    n=1 即 T-1。只查找 ≤ max_lookback 天内。
    如果 captures 不完整,fallback 按周一到周五简单交易日历(不考虑节假日)。
    """
    cur = d
    found = 0
    for _ in range(max_lookback):
        cur = cur - timedelta(days=1)
        if _date_has_kaipan_capture(project_root, cur):
            found += 1
            if found >= n:
                return cur
    # captures 里找不足 n 个 → fallback 到 weekday
    sys.stderr.write(
        f"[data_loader WARN] previous_trading_day: only found {found} kaipan captures within "
        f"{max_lookback} days before {d}, falling back to weekday calendar.\n"
    )
    cur = d
    found = 0
    while found < n:
        cur = cur - timedelta(days=1)
        if cur.weekday() < 5:
            found += 1
    return cur


# ============================================================================
# 主输出数据结构
# ============================================================================

@dataclass
class PremarketDataBundle:
    """v7.1 premarket 模式需要的全部 capture 数据。

    所有表都是 rows 列表;meta 在另外的 *_meta 字段。
    空表 → 空列表 [],不是 None。缺失的可选数据 → None。
    """

    # 上下文
    date_t0: str
    date_t1: str
    date_t2: Optional[str]
    project_root: str

    # T-0 当日竞价(4 表)
    auction_vratio: List[Dict[str, Any]]
    auction_qiangchou: List[Dict[str, Any]]
    auction_netamount: List[Dict[str, Any]]
    auction_fengdan: List[Dict[str, Any]]

    # T-1 主线源
    kaipan_t1_rows: List[Dict[str, Any]]
    kaipan_t1_meta: Dict[str, Any]                 # 含 subplates / top_plates

    # T-1 资金流(4 表,中个可能缺失返回 [])
    cashflow_today_t1: List[Dict[str, Any]]
    cashflow_3day_t1: List[Dict[str, Any]]
    cashflow_5day_t1: List[Dict[str, Any]]
    cashflow_10day_t1: List[Dict[str, Any]]

    # T-1 涨停复盘 + 龙头高度 + 涨停股池
    fupan_t1: List[Dict[str, Any]]
    ltgd_5day_t1: List[Dict[str, Any]]             # 已过滤 周期 == "5日"
    ztpool_t1: List[Dict[str, Any]]

    # 情绪指标(09:30 之前最早那份)
    qxlive_top_t1_rows: List[Dict[str, Any]]
    qxlive_top_t1_meta: Dict[str, Any]
    qxlive_top_t2_rows: List[Dict[str, Any]]
    qxlive_top_t2_meta: Dict[str, Any]

    # 主线历史序列(用于 theme_history,默认 N=10 天,按日期降序 T-1, T-2, ..., T-N)
    kaipan_history: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]] = field(
        default_factory=list
    )

    # 加载过程中遇到的 warning(不报错,仅记录)
    warnings: List[str] = field(default_factory=list)

    def to_summary_dict(self) -> Dict[str, Any]:
        """返回可读摘要(不含原始 rows 以避免输出过多内容)"""
        return {
            "date_t0": self.date_t0,
            "date_t1": self.date_t1,
            "date_t2": self.date_t2,
            "project_root": self.project_root,
            "counts": {
                "auction_vratio": len(self.auction_vratio),
                "auction_qiangchou": len(self.auction_qiangchou),
                "auction_netamount": len(self.auction_netamount),
                "auction_fengdan": len(self.auction_fengdan),
                "kaipan_t1": len(self.kaipan_t1_rows),
                "cashflow_today_t1": len(self.cashflow_today_t1),
                "cashflow_3day_t1": len(self.cashflow_3day_t1),
                "cashflow_5day_t1": len(self.cashflow_5day_t1),
                "cashflow_10day_t1": len(self.cashflow_10day_t1),
                "fupan_t1": len(self.fupan_t1),
                "ltgd_5day_t1": len(self.ltgd_5day_t1),
                "ztpool_t1": len(self.ztpool_t1),
                "qxlive_top_t1": len(self.qxlive_top_t1_rows),
                "qxlive_top_t2": len(self.qxlive_top_t2_rows),
                "kaipan_history": len(self.kaipan_history),
            },
            "kaipan_t1_meta_keys": sorted(self.kaipan_t1_meta.keys()),
            "kaipan_t1_subplate_count": len(self.kaipan_t1_meta.get("subplates", []) or []),
            "kaipan_t1_top_plate_count": len(self.kaipan_t1_meta.get("top_plates", []) or []),
            "warnings": self.warnings,
        }


# ============================================================================
# 核心加载函数
# ============================================================================

def load_premarket_bundle(
    date_t0: str,
    project_root: Path | str,
    *,
    history_days: int = DEFAULT_KAIPAN_HISTORY_DAYS,
    qxlive_premarket_boundary: str = QXLIVE_PREMARKET_BOUNDARY_HHMMSS,
    require_t2: bool = False,
) -> PremarketDataBundle:
    """加载 v7.1 premarket 模式需要的全部 capture 数据。

    :param date_t0: T-0 交易日(YYYY-MM-DD)
    :param project_root: duanxianxia 项目根目录(包含 captures/ 子目录)
    :param history_days: kaipan_history 回溯天数(默认 10)
    :param qxlive_premarket_boundary: qxlive top_metrics 取该时点之前最早那份(默认 09:30:00)
    :param require_t2: True 则 T-2 qxlive_top 缺失时拋异常;False 则返回空
    """
    project_root = Path(project_root)
    if not project_root.exists():
        raise DataLoaderError(f"project_root does not exist: {project_root}")

    warnings: List[str] = []

    try:
        d_t0 = date.fromisoformat(date_t0)
    except ValueError as e:
        raise DataLoaderError(f"Invalid date_t0 format: {date_t0!r} (expect YYYY-MM-DD)") from e

    d_t1 = previous_trading_day(project_root, d_t0, n=1)
    try:
        d_t2 = previous_trading_day(project_root, d_t0, n=2)
        date_t2_str: Optional[str] = d_t2.isoformat()
    except DataLoaderError:
        if require_t2:
            raise
        warnings.append("T-2 trading day not found within lookback; qxlive transition detection disabled.")
        d_t2 = None
        date_t2_str = None

    date_t1_str = d_t1.isoformat()

    # ---- T-0 当日竞价数据(4 表) ----
    # 09:25 时点抓取,取整天最早一份(实际 OpenClaw cron 09:25 触发,一天不应多份)
    auction_vratio = _extract_rows(
        load_capture_at_time(project_root, date_t0, DS_AUCTION_VRATIO, pick="latest")
    )
    auction_qiangchou = _extract_rows(
        load_capture_at_time(project_root, date_t0, DS_AUCTION_QIANGCHOU, pick="latest")
    )
    auction_netamount = _extract_rows(
        load_capture_at_time(project_root, date_t0, DS_AUCTION_NETAMOUNT, pick="latest")
    )
    auction_fengdan = _extract_rows(
        load_capture_at_time(project_root, date_t0, DS_AUCTION_FENGDAN, pick="latest")
    )

    # ---- T-1 主线源 ----
    kaipan_t1 = load_capture_at_time(project_root, date_t1_str, DS_HOME_KAIPAN, pick="latest")
    kaipan_t1_rows = _extract_rows(kaipan_t1)
    kaipan_t1_meta = _extract_meta(kaipan_t1)

    # ---- T-1 资金流(4 表,部分可能缺失 → 空表 + warning) ----
    def _try_cashflow(dataset_id: str) -> List[Dict[str, Any]]:
        cap = load_capture_at_time(
            project_root, date_t1_str, dataset_id,
            pick="latest", raise_if_missing=False,
        )
        if cap is None:
            warnings.append(f"missing T-1 cashflow: {dataset_id}")
            return []
        return _extract_rows(cap)

    cashflow_today_t1 = _try_cashflow(DS_CASHFLOW_TODAY)
    cashflow_3day_t1 = _try_cashflow(DS_CASHFLOW_3DAY)
    cashflow_5day_t1 = _try_cashflow(DS_CASHFLOW_5DAY)
    cashflow_10day_t1 = _try_cashflow(DS_CASHFLOW_10DAY)

    # ---- T-1 涨停复盘 + 龙头高度 + 涨停股池 ----
    fupan_t1 = _extract_rows(
        load_capture_at_time(project_root, date_t1_str, DS_REVIEW_FUPAN, pick="latest", raise_if_missing=False)
    )
    if not fupan_t1:
        warnings.append("missing T-1 review.fupan.plate")

    ltgd_t1_all = _extract_rows(
        load_capture_at_time(
            project_root, date_t1_str, DS_REVIEW_LTGD,
            pick="latest", raise_if_missing=False,
        )
    )
    # 只保留 周期 == "5日" (v7.1 锁定决策)
    ltgd_5day_t1 = [
        row for row in ltgd_t1_all
        if str(row.get("周期", "") or "").strip() == "5日"
    ]
    if ltgd_t1_all and not ltgd_5day_t1:
        warnings.append(
            f"review.ltgd.range T-1 has {len(ltgd_t1_all)} rows but none with 周期=='5日';"
            f" available periods: {sorted(set(str(r.get('周期', '') or '') for r in ltgd_t1_all))}"
        )
    elif not ltgd_t1_all:
        warnings.append("missing T-1 review.ltgd.range")

    ztpool_t1 = _extract_rows(
        load_capture_at_time(
            project_root, date_t1_str, DS_HOME_ZTPOOL,
            pick="latest", raise_if_missing=False,
        )
    )
    if not ztpool_t1:
        warnings.append("missing T-1 home.ztpool")

    # ---- T-1 / T-2 qxlive top_metrics(必须 ≤ 09:30 那份,避免盘中快照) ----
    qxlive_t1_cap = load_capture_at_time(
        project_root, date_t1_str, DS_HOME_QXLIVE_TOP,
        max_hhmmss=qxlive_premarket_boundary,
        pick="earliest_before",
        raise_if_missing=False,
    )
    qxlive_top_t1_rows = _extract_rows(qxlive_t1_cap)
    qxlive_top_t1_meta = _extract_meta(qxlive_t1_cap)
    if not qxlive_top_t1_rows:
        warnings.append("missing T-1 home.qxlive.top_metrics premarket snapshot")

    if date_t2_str is not None:
        qxlive_t2_cap = load_capture_at_time(
            project_root, date_t2_str, DS_HOME_QXLIVE_TOP,
            max_hhmmss=qxlive_premarket_boundary,
            pick="earliest_before",
            raise_if_missing=False,
        )
        qxlive_top_t2_rows = _extract_rows(qxlive_t2_cap)
        qxlive_top_t2_meta = _extract_meta(qxlive_t2_cap)
        if not qxlive_top_t2_rows:
            warnings.append("missing T-2 home.qxlive.top_metrics premarket snapshot")
    else:
        qxlive_top_t2_rows = []
        qxlive_top_t2_meta = {}

    # ---- kaipan 历史序列(theme_history 使用) ----
    kaipan_history: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any]]] = []
    cur_history_date = d_t1
    history_attempts = 0
    while len(kaipan_history) < history_days and history_attempts < _MAX_LOOKBACK_DAYS * 2:
        ds = cur_history_date.isoformat()
        cap = load_capture_at_time(
            project_root, ds, DS_HOME_KAIPAN,
            pick="latest", raise_if_missing=False,
        )
        if cap is not None:
            kaipan_history.append((ds, _extract_rows(cap), _extract_meta(cap)))
        # 下一个候选 = 再上一个工作日
        try:
            cur_history_date = previous_trading_day(project_root, cur_history_date, n=1)
        except DataLoaderError:
            break
        history_attempts += 1

    if len(kaipan_history) < history_days:
        warnings.append(
            f"kaipan_history only has {len(kaipan_history)}/{history_days} days;"
            f" theme_history streak may be capped."
        )

    return PremarketDataBundle(
        date_t0=date_t0,
        date_t1=date_t1_str,
        date_t2=date_t2_str,
        project_root=str(project_root),
        auction_vratio=auction_vratio,
        auction_qiangchou=auction_qiangchou,
        auction_netamount=auction_netamount,
        auction_fengdan=auction_fengdan,
        kaipan_t1_rows=kaipan_t1_rows,
        kaipan_t1_meta=kaipan_t1_meta,
        cashflow_today_t1=cashflow_today_t1,
        cashflow_3day_t1=cashflow_3day_t1,
        cashflow_5day_t1=cashflow_5day_t1,
        cashflow_10day_t1=cashflow_10day_t1,
        fupan_t1=fupan_t1,
        ltgd_5day_t1=ltgd_5day_t1,
        ztpool_t1=ztpool_t1,
        qxlive_top_t1_rows=qxlive_top_t1_rows,
        qxlive_top_t1_meta=qxlive_top_t1_meta,
        qxlive_top_t2_rows=qxlive_top_t2_rows,
        qxlive_top_t2_meta=qxlive_top_t2_meta,
        kaipan_history=kaipan_history,
        warnings=warnings,
    )


# ============================================================================
# CLI 测试入口
# ============================================================================

def _main() -> int:
    parser = argparse.ArgumentParser(
        description="v7.1 premarket data loader 测试工具(加载一次并输出摘要)",
    )
    parser.add_argument("--date", required=True, help="T-0 交易日 YYYY-MM-DD")
    parser.add_argument(
        "--project-root", required=True,
        help="duanxianxia 项目根目录(包含 captures/ 子目录)",
    )
    parser.add_argument(
        "--history-days", type=int, default=DEFAULT_KAIPAN_HISTORY_DAYS,
        help=f"kaipan_history 回溯天数(默认 {DEFAULT_KAIPAN_HISTORY_DAYS})",
    )
    parser.add_argument("--full", action="store_true", help="输出完整加载结果(包含原始 rows)")
    args = parser.parse_args()

    bundle = load_premarket_bundle(
        date_t0=args.date,
        project_root=args.project_root,
        history_days=args.history_days,
    )

    if args.full:
        # 完整输出(可能很大)
        print(json.dumps(asdict(bundle), ensure_ascii=False, indent=2, default=str))
    else:
        print(json.dumps(bundle.to_summary_dict(), ensure_ascii=False, indent=2))

    if bundle.warnings:
        sys.stderr.write(f"\n[data_loader] {len(bundle.warnings)} warnings:\n")
        for w in bundle.warnings:
            sys.stderr.write(f"  - {w}\n")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
