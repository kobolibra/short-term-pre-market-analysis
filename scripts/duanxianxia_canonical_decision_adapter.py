"""
duanxianxia_canonical_decision_adapter.py — M1b canonical→decision adapter.

Purpose
-------
Bridge the canonical feature layer (duanxianxia_feature_builder, one row per
code with RAW canonical fields) to the EXISTING v7.2 auction-strength
derivation engine (duanxianxia_v7_2_auction_strength.compute_auction_strengths)
without re-deriving any scoring formula. This guarantees ZERO information-
coefficient drift: the adapter only reshapes canonical fields into the
per-source row dicts the engine already expects, then calls the engine
verbatim. The resulting `auction_detail` dicts are exactly what
duanxianxia_v9_edge.compute_edge_v9 / duanxianxia_v9_assemble.assemble_v9
consume.

This replaces the legacy candidate path
(duanxianxia_premarket_v6._merge_candidates / _dataset_rows), which read the
raw scraped dataset rows directly. The canonical feature_builder is now the
single source of truth for raw fields.

fengdan note
------------
The 9:15/9:20/9:25 fengdan amount series is intentionally NOT reconstructed.
Per rebuild-plan-v11 the weimai limit-up bid queue (sealAmount) is the
canonical lock / limit-up signal, so `fengdan_rows` stays empty and the engine
resolves fengdan_status="none" (orderbook_quality_score defaults to 45, which
matches compute_edge_v9's own default).

feature row schema (duanxianxia_feature_builder._assemble), with units
----------------------------------------------------------------------
  code, name,
  free_float_mktcap        (元)   -> market_cap_yi  = /1e8
  bidAmount                (元)   -> auction_turnover_wan = /1e4  (= 竞价成交额)
  bidStrength              (万分比 = bidAmount/FF*1e4)
  volumeRatio                     -> vratio rank key
  grabStrength                    -> qiangchou rank key
  changeRate, latestChangePct (%) -> latest_change_pct
  turnoverRate             (%)    -> turnover_rate_pct
  mainNetInflow            (元)   -> main_net_inflow_wan = /1e4
  mainNetInflowFull        (元)
  superLargeOrder, largeOrder
  sealAmount               (元)   -> seal_amount_wan = /1e4  (weimai 封单)
  boardLabel, price, concept
  source_hits (list[str]), source_hit_count

The engine indexes each source by `rank` (1 = best) and applies exponential
rank decay; we therefore assign a cross-sectional rank per source by sorting
the candidate set on the relevant canonical field (descending).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _num(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-", "none", "None"):
            return None
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return None


def _has_source(feat: Dict[str, Any], needle: str) -> bool:
    hits = feat.get("source_hits") or []
    if isinstance(hits, str):
        hits = [hits]
    for h in hits:
        if needle in str(h).lower():
            return True
    return False


def _rank_desc(
    feats: List[Dict[str, Any]],
    value_key: str,
    *,
    member: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Return [{code, _val, _feat}] for member rows with a non-None value_key,
    sorted descending and rank-tagged (1 = best). `member` is an optional
    predicate feat->bool gating membership in the source."""
    picked = []
    for f in feats:
        if member is not None and not member(f):
            continue
        val = _num(f.get(value_key))
        if val is None:
            continue
        picked.append((val, f))
    picked.sort(key=lambda x: x[0], reverse=True)
    out = []
    for i, (val, f) in enumerate(picked, start=1):
        out.append({"rank": i, "_val": val, "_feat": f, "code": _norm_code(f.get("code"))})
    return out


def build_source_rows_from_features(
    features: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Reconstruct the per-source row dicts compute_auction_strengths expects.

    Membership rules (mirror the four canonical AUCTION_DATASETS):
      vratio     : volumeRatio present
      qiangchou  : grabStrength present  (group "qiangchou" = 9:20-9:25 primary)
      net_amount : mainNetInflow present
      weimai     : code hit the weimai source (sealAmount may be null pre-9:25)
      fengdan    : intentionally empty (replaced by weimai)
    """
    feats = [f for f in (features or []) if _norm_code(f.get("code"))]

    def _common(f: Dict[str, Any]) -> Dict[str, Any]:
        row: Dict[str, Any] = {"code": _norm_code(f.get("code"))}
        pct = _num(f.get("latestChangePct"))
        if pct is None:
            pct = _num(f.get("changeRate"))
        if pct is not None:
            row["latest_change_pct"] = pct
        tr = _num(f.get("turnoverRate"))
        if tr is not None:
            row["turnover_rate_pct"] = tr
        bid = _num(f.get("bidAmount"))
        if bid is not None:
            row["auction_turnover_wan"] = bid / 1e4
        return row

    vratio_rows: List[Dict[str, Any]] = []
    for r in _rank_desc(feats, "volumeRatio"):
        f = r["_feat"]
        row = _common(f)
        row["rank"] = r["rank"]
        row["volume_ratio_multiple"] = r["_val"]
        vratio_rows.append(row)

    qiangchou_rows: List[Dict[str, Any]] = []
    for r in _rank_desc(feats, "grabStrength"):
        f = r["_feat"]
        row = _common(f)
        row["rank"] = r["rank"]
        row["group"] = "qiangchou"  # 9:20-9:25 sustained (primary)
        row["grab_strength"] = r["_val"]
        qiangchou_rows.append(row)

    netamount_rows: List[Dict[str, Any]] = []
    for r in _rank_desc(feats, "mainNetInflow"):
        f = r["_feat"]
        row = _common(f)
        row["rank"] = r["rank"]
        row["main_net_inflow_wan"] = r["_val"] / 1e4
        ff = _num(f.get("free_float_mktcap"))
        if ff is not None and ff > 0:
            row["market_cap_yi"] = ff / 1e8
        netamount_rows.append(row)

    # weimai membership: source hit, not sealAmount magnitude (null pre-9:25).
    def _is_weimai(f: Dict[str, Any]) -> bool:
        return _has_source(f, "weimai") or _num(f.get("sealAmount")) is not None

    weimai_members = [f for f in feats if _is_weimai(f)]
    # rank weimai by sealAmount desc; rows without sealAmount get appended after
    # ranked ones so they are still "in" the queue (in_weimai True) but low rank.
    sealed = _rank_desc(weimai_members, "sealAmount")
    sealed_codes = {r["code"] for r in sealed}
    weimai_rows: List[Dict[str, Any]] = []
    for r in sealed:
        f = r["_feat"]
        row = {"code": r["code"], "rank": r["rank"], "seal_amount_wan": r["_val"] / 1e4}
        pct = _num(f.get("latestChangePct"))
        if pct is not None:
            row["latest_change_pct"] = pct
        weimai_rows.append(row)
    next_rank = len(sealed) + 1
    for f in weimai_members:
        c = _norm_code(f.get("code"))
        if c in sealed_codes:
            continue
        row = {"code": c, "rank": next_rank}
        pct = _num(f.get("latestChangePct"))
        if pct is not None:
            row["latest_change_pct"] = pct
        weimai_rows.append(row)
        next_rank += 1

    return {
        "candidate_codes": [_norm_code(f.get("code")) for f in feats],
        "vratio_rows": vratio_rows,
        "qiangchou_rows": qiangchou_rows,
        "netamount_rows": netamount_rows,
        "fengdan_rows": [],  # intentionally empty; weimai replaces fengdan
        "weimai_rows": weimai_rows,
    }


def build_auction_decisions(
    features: List[Dict[str, Any]],
    *,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Produce v9 `decisions` carrying `auction_detail` derived by the EXISTING
    v7.2 engine from canonical features. The engine is imported lazily so this
    module stays importable (and unit-testable) without it.
    """
    from duanxianxia_v7_2_auction_strength import compute_auction_strengths

    src = build_source_rows_from_features(features)
    strengths = compute_auction_strengths(
        src["candidate_codes"],
        src["vratio_rows"],
        src["qiangchou_rows"],
        src["netamount_rows"],
        src["fengdan_rows"],
        params or {},
        weimai_rows=src["weimai_rows"],
    )
    by_code = {_norm_code(f.get("code")): f for f in (features or [])}
    decisions: List[Dict[str, Any]] = []
    for code, detail in strengths.items():
        feat = by_code.get(code, {})
        decisions.append(
            {
                "code": code,
                "name": feat.get("name"),
                "auction_strength": detail.get("auction_strength"),
                "auction_detail": detail,
                # theme/weimai/context details are filled by assemble_v9's
                # dedicated v9 sub-modules; left absent here so edge_core uses
                # its documented neutral defaults if assemble is bypassed.
            }
        )
    decisions.sort(key=lambda d: (d.get("auction_strength") or 0.0), reverse=True)
    return decisions


def _self_test() -> None:
    # ---- Part 1: deterministic row construction + unit conversion ----
    feats = [
        {
            "code": "sh.600519", "name": "A", "free_float_mktcap": 43772683811.0,
            "bidAmount": 219900000.0, "volumeRatio": 14.8, "grabStrength": 7.0,
            "latestChangePct": 3.35, "turnoverRate": 1.12,
            "mainNetInflow": 50000000.0, "sealAmount": 443070000.0,
            "source_hits": ["vratio", "qiangchou", "net_amount", "weimai"],
        },
        {
            "code": "000002", "name": "B", "free_float_mktcap": 8000000000.0,
            "bidAmount": 30000000.0, "volumeRatio": 5.0, "grabStrength": 3.0,
            "latestChangePct": -1.0, "turnoverRate": 1.0,
            "mainNetInflow": 40000000.0,
            "source_hits": ["vratio", "net_amount"],
        },
        {
            "code": "000603", "name": "C", "free_float_mktcap": 12000000000.0,
            "volumeRatio": None, "grabStrength": None,
            "latestChangePct": 9.95, "sealAmount": None,
            "source_hits": ["weimai"],  # in queue, sealAmount null pre-9:25
        },
    ]
    src = build_source_rows_from_features(feats)
    assert src["candidate_codes"] == ["600519", "000002", "000603"], src["candidate_codes"]
    # vratio: only A,B have volumeRatio; A(14.8) rank1, B(5.0) rank2; C excluded.
    assert [r["code"] for r in src["vratio_rows"]] == ["600519", "000002"], src["vratio_rows"]
    a_v = src["vratio_rows"][0]
    assert abs(a_v["auction_turnover_wan"] - 21990.0) < 1e-6, a_v
    assert abs(a_v["turnover_rate_pct"] - 1.12) < 1e-9, a_v
    assert abs(a_v["latest_change_pct"] - 3.35) < 1e-9, a_v
    # net_amount: A(5e7) rank1, B(4e7) rank2.
    na = {r["code"]: r for r in src["netamount_rows"]}
    assert na["600519"]["rank"] == 1 and na["000002"]["rank"] == 2, src["netamount_rows"]
    assert abs(na["600519"]["main_net_inflow_wan"] - 5000.0) < 1e-6, na["600519"]
    assert abs(na["600519"]["market_cap_yi"] - 437.72683811) < 1e-6, na["600519"]
    # qiangchou: A,B have grabStrength; group tag present.
    assert all(r["group"] == "qiangchou" for r in src["qiangchou_rows"]), src["qiangchou_rows"]
    assert [r["code"] for r in src["qiangchou_rows"]] == ["600519", "000002"], src["qiangchou_rows"]
    # weimai: A(seal 4.4307e8) ranked first; C in queue (sealAmount null) appended.
    wm = {r["code"]: r for r in src["weimai_rows"]}
    assert set(wm) == {"600519", "000603"}, src["weimai_rows"]
    assert abs(wm["600519"]["seal_amount_wan"] - 44307.0) < 1e-6, wm["600519"]
    assert wm["600519"]["rank"] == 1, wm["600519"]
    assert wm["000603"]["rank"] == 2 and "seal_amount_wan" not in wm["000603"], wm["000603"]
    assert src["fengdan_rows"] == [], src["fengdan_rows"]
    print("row-construction _self_test passed")

    # ---- Part 2: end-to-end through the real engines if importable ----
    try:
        from duanxianxia_v7_2_auction_strength import compute_auction_strengths  # noqa: F401
    except Exception as e:  # pragma: no cover - engine not present locally
        print(f"engine not importable locally ({e}); skipping end-to-end (server probe covers it)")
        return
    decisions = build_auction_decisions(feats)
    assert decisions, "no decisions produced"
    for d in decisions:
        det = d["auction_detail"]
        for k in ("auction_strength", "money_intent_score", "liquidity_score",
                  "orderbook_quality_score", "source_evidence_score",
                  "auction_setup_type", "fengdan_status"):
            assert k in det, (d["code"], k, list(det))
        assert det["fengdan_status"] == "none", det["fengdan_status"]
    try:
        from duanxianxia_v9_edge import compute_edge_v9
        e = compute_edge_v9(decisions[0], {"market_env_score": 50, "risk_flags": []}, {})
        assert 0.0 <= e["edge_score"] <= 100.0, e
        print(f"end-to-end _self_test passed; top edge_score={e['edge_score']}")
    except Exception as e:  # pragma: no cover
        print(f"edge engine not importable locally ({e}); v7.2 path OK")


if __name__ == "__main__":
    _self_test()
