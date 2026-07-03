#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_auction_vr_qc_validation_0122.py -- Task 0122 (read-only).

补齐前例未覆盖的两张竞价表:
 (V) 竞价爆量  auction.jjyd.vratio -> volume_ratio (量比)
 (Q) 竞价抢筹  auction.jjyd.qiangchou -> grab_strength
并附:
 (X) 跨表同一 code 的 free_float_mktcap 一致性 (vratio/qiangchou/net_amount/weimai 统一到元后是否对齐)
 (T) 002979 / 002396 两只前例股的量比 & 抢筹
只读。
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from duanxianxia_master_indicators import _rows_of, _row_code, _norm_code  # noqa: E402
from duanxianxia_canonical_routing import canonicalize_row  # noqa: E402

WS = Path(os.environ.get("DXX_WS", "/home/investmentofficehku/.openclaw/workspace"))
ROOT = WS / "projects" / "duanxianxia"
CAP = ROOT / "captures"


def _dates():
    if not CAP.is_dir():
        return []
    return sorted(p.name for p in CAP.iterdir()
                  if p.is_dir() and len(p.name) == 10 and p.name[4] == "-")


def _snaps(dd):
    return sorted(p.name for p in dd.glob("*.json")) if dd.is_dir() else []


def _load(dd, which="last"):
    s = _snaps(dd)
    if not s:
        return None, None
    fn = s[-1] if which == "last" else s[0]
    try:
        return json.loads((dd / fn).read_text(encoding="utf-8")), fn
    except Exception as e:  # noqa: BLE001
        return {"_err": str(e)}, fn


def _canon(ds, row):
    try:
        return canonicalize_row(ds, row) or {}
    except Exception as e:  # noqa: BLE001
        return {"_canon_err": str(e)}


def _index(payload, ds):
    out = {}
    if not isinstance(payload, (dict, list)):
        return out
    try:
        for row in _rows_of(payload):
            rc = _row_code(row)
            if not rc:
                continue
            out[_norm_code(rc)] = _canon(ds, row)
    except Exception:  # noqa: BLE001
        pass
    return out


def _num(x):
    try:
        return float(x)
    except Exception:  # noqa: BLE001
        return None


def _batch(date, fn):
    return "%s %s" % (date, (fn or "").replace(".json", ""))


def main():
    D = _dates()
    if not D:
        print(json.dumps({"error": "no date folders"}))
        return 0
    today, prev = D[-1], (D[-2] if len(D) >= 2 else D[-1])
    out = {"job": "0122_auction_vr_qc_validation", "today": today}

    vr_p, vr_fn = _load(CAP / today / "auction.jjyd.vratio")
    qc_p, qc_fn = _load(CAP / today / "auction.jjyd.qiangchou")
    na_p, na_fn = _load(CAP / today / "auction.jjyd.net_amount")
    wm_p, wm_fn = _load(CAP / today / "auction.jjyd.weimai")
    vr = _index(vr_p, "auction.jjyd.vratio")
    qc = _index(qc_p, "auction.jjyd.qiangchou")
    na = _index(na_p, "auction.jjyd.net_amount")
    wm = _index(wm_p, "auction.jjyd.weimai")

    def apct(c):
        return (na.get(c, {}) or {}).get("auction_change_pct") or (vr.get(c, {}) or {}).get("auction_change_pct")

    def board(c):
        return (wm.get(c, {}) or {}).get("board_label")

    # coverage
    def cov(idx, key):
        n = sum(1 for v in idx.values() if _num(v.get(key)) is not None)
        return {"rows": len(idx), "present": n,
                "pct": round(100.0 * n / len(idx), 1) if idx else 0.0}
    out["coverage"] = {"volume_ratio(vratio)": cov(vr, "volume_ratio"),
                       "grab_strength(qiangchou)": cov(qc, "grab_strength")}
    out["provenance_files"] = {"vratio": _batch(today, vr_fn), "qiangchou": _batch(today, qc_fn),
                              "net_amount": _batch(today, na_fn), "weimai": _batch(today, wm_fn)}

    # (V) 竞价爆量 top by volume_ratio
    vlist = [(c, _num(v.get("volume_ratio")), v) for c, v in vr.items() if _num(v.get("volume_ratio")) is not None]
    vlist.sort(key=lambda t: t[1], reverse=True)
    out["V_top_volume_ratio"] = [{
        "code": c, "name": v.get("name"), "volume_ratio": vv,
        "auction_change_pct": apct(c), "board_label": board(c),
        "free_float_mktcap": _num(v.get("free_float_mktcap")),
        "auction_turnover": _num(v.get("auction_turnover")),
        "src": "auction.jjyd.vratio", "batch": _batch(today, vr_fn)}
        for c, vv, v in vlist[:8]]

    # (Q) 竞价抢筹 top by grab_strength
    qlist = [(c, _num(v.get("grab_strength")), v) for c, v in qc.items() if _num(v.get("grab_strength")) is not None]
    qlist.sort(key=lambda t: t[1], reverse=True)
    out["Q_top_grab_strength"] = [{
        "code": c, "name": v.get("name"), "grab_strength": qv,
        "auction_change_pct": apct(c), "board_label": board(c),
        "volume_ratio": _num(vr.get(c, {}).get("volume_ratio")),
        "src": "auction.jjyd.qiangchou", "batch": _batch(today, qc_fn)}
        for c, qv, v in qlist[:8]]

    # (T) target stocks
    tgt = {}
    for c in ("002979", "002396"):
        cc = _norm_code(c)
        tgt[c] = {"name": (vr.get(cc, {}) or wm.get(cc, {})).get("name"),
                  "volume_ratio": _num(vr.get(cc, {}).get("volume_ratio")),
                  "grab_strength": _num(qc.get(cc, {}).get("grab_strength")),
                  "auction_change_pct": apct(cc), "board_label": board(cc)}
    out["T_targets"] = tgt

    # (X) cross-table FF consistency
    codes = set(vr) | set(qc) | set(na) | set(wm)
    worst = []
    consistent = 0
    checked = 0
    for c in codes:
        ffs = {"vratio": _num(vr.get(c, {}).get("free_float_mktcap")),
               "qiangchou": _num(qc.get(c, {}).get("free_float_mktcap")),
               "net_amount": _num(na.get(c, {}).get("free_float_mktcap")),
               "weimai": _num(wm.get(c, {}).get("free_float_mktcap"))}
        pres = {k: v for k, v in ffs.items() if v is not None and v > 0}
        if len(pres) < 2:
            continue
        checked += 1
        vals = list(pres.values())
        spread = (max(vals) - min(vals)) / max(vals)
        if spread <= 0.01:
            consistent += 1
        else:
            worst.append({"code": c, "name": (vr.get(c, {}) or na.get(c, {})).get("name"),
                          "spread_pct": round(spread * 100, 2), "ff_by_table": pres})
    worst.sort(key=lambda x: x["spread_pct"], reverse=True)
    out["X_ff_cross_table_consistency"] = {
        "checked_codes(>=2 tables)": checked, "consistent(<=1pct)": consistent,
        "inconsistent": len(worst), "worst_examples": worst[:5]}

    out["_summary"] = {
        "volume_ratio_present": out["coverage"]["volume_ratio(vratio)"]["present"],
        "grab_strength_present": out["coverage"]["grab_strength(qiangchou)"]["present"],
        "ff_checked": checked, "ff_consistent": consistent, "ff_inconsistent": len(worst)}
    print("=== AUCTION VR/QC VALIDATION (Task 0122) ===")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
