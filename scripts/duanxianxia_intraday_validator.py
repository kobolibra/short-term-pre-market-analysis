#!/usr/bin/env python3
"""
Duanxianxia v7 intraday validator.

Runs at 10:01 cron after `duanxianxia_batch.py intraday_cashflow` capture.
Reads:
  - reports/<date>/intraday_anchors.json   (produced by premarket v7)
  - captures/<date>/<intraday datasets>/*.json
Writes:
  - reports/<date>/intraday_validation.json

For each premarket candidate, evaluates the anchor conditions defined for
its setup and emits one of {hit, partial, miss} along with per-condition
results. Designed to be safe to re-run; output is overwritten.

Intraday data sources used:
  - rank.rocket          (主流冲涨值)
  - rank.hot_stock_day   (热度榜)
  - pool.hot             (热门池)
  - pool.surge           (飙升池)
  - cashflow.stock.today (盘中口径，日内累计)

Anchor condition types (extensible — see _eval_condition):
  price_above_auction   (proxies: pool.hot.涨幅 >= ref_pct + tolerance)
  amount_min_yi         (pool.hot.成交; check available proxy)
  industry_rank_top     (matched_industries appears in any intraday rank)
  intraday_uptrend      (pool.hot.涨幅 > auction_change_pct)
  fengdan_min_wan       (pool.hot.主力 >= min_wan, proxy)
  fanbao_complete       (pool.hot.涨幅 > 0)
  miaoban               (rank.rocket top 5 + 涨幅>=9 quick approx)
  fengdan_min_yi        (proxy via pool.hot.主力)

Where a condition has no data source available it is recorded as 'unknown'
rather than 'miss', preserving the missing-as-signal philosophy.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

VERSION = "intraday_validator_v7.0"
TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _norm_code(code: Any) -> str:
    s = str(code or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s
    if len(digits) > 6:
        digits = digits[-6:]
    return digits.zfill(6)


def _parse_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        s = str(value).replace("%", "").replace(",", "").strip()
        return float(s) if s else None
    except Exception:
        return None


def _parse_yi(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s in {"-", "--"}:
        return None
    sign = 1.0
    if s.startswith("-"):
        sign = -1.0
        s = s[1:]
    try:
        if "亿" in s:
            return sign * float(s.replace("亿", ""))
        if "万" in s:
            return sign * float(s.replace("万", "")) / 10000.0
        return sign * float(s)
    except Exception:
        return None


def _load_latest_capture(captures_dir: Path, dataset_id: str) -> List[Mapping[str, Any]]:
    sub = captures_dir / dataset_id
    if not sub.is_dir():
        return []
    files = sorted(sub.glob("*.json"))
    if not files:
        return []
    try:
        payload = json.loads(files[-1].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = payload.get("rows") if isinstance(payload, Mapping) else None
    return list(rows) if isinstance(rows, list) else []


def _index_by_code(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    out: Dict[str, Mapping[str, Any]] = {}
    for r in rows:
        c = _norm_code(r.get("代码") or r.get("code"))
        if c and c not in out:
            out[c] = r
    return out


def _eval_condition(cond: Mapping[str, Any],
                     code: str,
                     anchor_meta: Mapping[str, Any],
                     intraday: Mapping[str, Any]) -> Dict[str, Any]:
    """Return {status: hit|miss|partial|unknown, detail: str, value: ...}."""
    ctype = str(cond.get("type") or "")
    pool_hot = intraday.get("pool.hot_idx", {})
    rocket_idx = intraday.get("rank.rocket_idx", {})
    hot_day_idx = intraday.get("rank.hot_stock_day_idx", {})
    surge_idx = intraday.get("pool.surge_idx", {})
    today_cf_idx = intraday.get("cashflow.stock.today_idx", {})
    matched_industries: Sequence[str] = anchor_meta.get("matched_industries") or []

    row = pool_hot.get(code) or {}
    intraday_pct = _parse_float(row.get("涨幅") or row.get("change_pct"))
    if intraday_pct is None:
        # surge fallback
        srow = surge_idx.get(code) or {}
        intraday_pct = _parse_float(srow.get("change_pct") or srow.get("涨幅"))
    main_inflow = _parse_yi(row.get("主力"))  # 亿
    amount_yi = _parse_yi(row.get("成交"))  # 亿

    if ctype == "price_above_auction":
        ref = _parse_float(cond.get("ref_pct") or anchor_meta.get("auction_change_pct"))
        tol = float(cond.get("tolerance_pct") or 0.0)
        if intraday_pct is None or ref is None:
            return {"type": ctype, "status": "unknown", "detail": "missing intraday or auction pct"}
        ok = intraday_pct >= (ref + tol)
        return {"type": ctype, "status": "hit" if ok else "miss",
                "intraday_pct": intraday_pct, "ref_pct": ref, "tolerance": tol}
    if ctype == "amount_min_yi":
        min_yi = float(cond.get("min_yi") or 0)
        if amount_yi is None:
            return {"type": ctype, "status": "unknown", "detail": "pool.hot 成交 missing"}
        return {"type": ctype, "status": "hit" if amount_yi >= min_yi else "miss",
                "amount_yi": amount_yi, "min_yi": min_yi}
    if ctype == "industry_rank_top":
        max_rank = int(cond.get("max_rank") or 5)
        # We don't have intraday industry rank — fall back to hot/rocket leader presence
        leader_present = code in rocket_idx or code in hot_day_idx
        if not matched_industries:
            return {"type": ctype, "status": "unknown", "detail": "no matched industry from premarket"}
        return {"type": ctype,
                "status": "hit" if leader_present else "partial",
                "detail": "intraday industry rank not directly available; leader presence used as proxy",
                "max_rank": max_rank}
    if ctype == "intraday_uptrend":
        ref = _parse_float(anchor_meta.get("auction_change_pct")) or 0.0
        if intraday_pct is None:
            return {"type": ctype, "status": "unknown"}
        return {"type": ctype, "status": "hit" if intraday_pct > ref else "miss",
                "intraday_pct": intraday_pct, "ref_pct": ref}
    if ctype == "fengdan_min_wan":
        min_wan = float(cond.get("min_wan") or 0)
        if main_inflow is None:
            return {"type": ctype, "status": "unknown"}
        # main_inflow in 亿 -> 万
        wan = main_inflow * 10000.0
        return {"type": ctype, "status": "hit" if wan >= min_wan else "partial",
                "detail": "using pool.hot 主力 as fengdan proxy",
                "main_inflow_wan": wan, "min_wan": min_wan}
    if ctype == "fengdan_min_yi":
        min_yi = float(cond.get("min_yi") or 0)
        if main_inflow is None:
            return {"type": ctype, "status": "unknown"}
        return {"type": ctype, "status": "hit" if main_inflow >= min_yi else "partial",
                "detail": "using pool.hot 主力 as fengdan proxy",
                "main_inflow_yi": main_inflow, "min_yi": min_yi}
    if ctype == "fanbao_complete":
        if intraday_pct is None:
            return {"type": ctype, "status": "unknown"}
        return {"type": ctype, "status": "hit" if intraday_pct > 0 else "miss",
                "intraday_pct": intraday_pct}
    if ctype == "miaoban":
        # Rocket top 5 with rank<=5 OR 涨幅>=9.5 in pool.hot
        rrow = rocket_idx.get(code) or {}
        rrank = int(_parse_float(rrow.get("rank")) or 9999)
        if rrank <= 5:
            return {"type": ctype, "status": "hit", "rocket_rank": rrank}
        if intraday_pct is not None and intraday_pct >= 9.5:
            return {"type": ctype, "status": "hit", "intraday_pct": intraday_pct}
        if intraday_pct is None and not rrow:
            return {"type": ctype, "status": "unknown"}
        return {"type": ctype, "status": "miss",
                "rocket_rank": rrank if rrow else None,
                "intraday_pct": intraday_pct}
    return {"type": ctype, "status": "unknown", "detail": "unknown condition type"}


def _verdict(condition_results: Sequence[Mapping[str, Any]]) -> str:
    if not condition_results:
        return "unknown"
    statuses = [r.get("status") for r in condition_results]
    if all(s == "hit" for s in statuses):
        return "hit"
    if any(s == "miss" for s in statuses):
        return "miss"
    if any(s == "hit" for s in statuses):
        return "partial"
    return "unknown"


def validate(target_date: Optional[str] = None,
              project_root: Optional[Path | str] = None) -> Dict[str, Any]:
    pr = Path(project_root) if project_root else Path.cwd()
    date_str = target_date or datetime.now(TZ_SHANGHAI).strftime("%Y-%m-%d")
    anchors_path = pr / "reports" / date_str / "intraday_anchors.json"
    if not anchors_path.exists():
        return {"enabled": False, "version": VERSION, "date": date_str,
                "reason": f"intraday_anchors.json not found at {anchors_path}"}
    payload = json.loads(anchors_path.read_text(encoding="utf-8"))
    candidates = payload.get("candidates") or []
    if not candidates:
        return {"enabled": True, "version": VERSION, "date": date_str,
                "reason": "no premarket candidates classified into setup"}

    captures_dir = pr / "captures" / date_str
    intraday = {
        "pool.hot_idx": _index_by_code(_load_latest_capture(captures_dir, "pool.hot")),
        "pool.surge_idx": _index_by_code(_load_latest_capture(captures_dir, "pool.surge")),
        "rank.rocket_idx": _index_by_code(_load_latest_capture(captures_dir, "rank.rocket")),
        "rank.hot_stock_day_idx": _index_by_code(_load_latest_capture(captures_dir, "rank.hot_stock_day")),
        "cashflow.stock.today_idx": _index_by_code(_load_latest_capture(captures_dir, "cashflow.stock.today")),
    }

    results: List[Dict[str, Any]] = []
    setup_summary: Dict[str, Dict[str, int]] = {}
    for cand in candidates:
        code = _norm_code(cand.get("code"))
        setup = cand.get("setup") or "none"
        anchors = cand.get("anchors") or {}
        conditions = anchors.get("conditions") or []
        cond_results = [
            _eval_condition(c, code, cand, intraday) for c in conditions
        ]
        verdict = _verdict(cond_results)
        bucket = setup_summary.setdefault(setup, {"hit": 0, "partial": 0, "miss": 0, "unknown": 0})
        bucket[verdict] = bucket.get(verdict, 0) + 1
        results.append({
            "code": code,
            "name": cand.get("name"),
            "setup": setup,
            "verdict": verdict,
            "check_at": anchors.get("check_at"),
            "matched_industries": cand.get("matched_industries") or [],
            "conditions": cond_results,
        })

    out = {
        "enabled": True,
        "version": VERSION,
        "date": date_str,
        "prev_date": payload.get("prev_date"),
        "regime": payload.get("regime"),
        "validated_at": datetime.now(TZ_SHANGHAI).isoformat(),
        "setup_summary": setup_summary,
        "results": results,
    }
    target = pr / "reports" / date_str / "intraday_validation.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    out["validation_path"] = str(target)
    return out


def _main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Validate premarket v7 anchors against intraday capture")
    ap.add_argument("--target-date", default=None,
                     help="YYYY-MM-DD (default: today in Asia/Shanghai)")
    ap.add_argument("--project-root", default="/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia",
                     help="project root containing reports/ and captures/")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)
    result = validate(args.target_date, args.project_root)
    if not args.quiet:
        sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    if not result.get("enabled"):
        return 0
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
