#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_canonical.py  --  Task 0090 (v10 rebuild, transform-1 canonical layer)

Single source of truth that turns a dataset's raw[] positional array (or, for
pool.hot, the legacy named-string row) into a canonical dict with:
  * correct field names (fixes the historical mislabels)
  * an explicit caliber tag on every market-cap field (FF / FLOAT / TOTAL)
  * all monetary values normalised to base 元 (yuan)

Unit decisions are LOCKED by job 0089 (unit probe, 2026-06-29), cross-validated
across tables on the shared sample 多氟多/002407:
  - vratio/qiangchou/net_amount raw[2]/[6] free_float_mktcap : 亿  -> x1e8 -> 元
  - weimai raw[12]/[13]/[14]/[15]/[17] & raw[4]/[6]/[8]/[9]  : already 元 (NO x1e4)
  - net_amount raw[4]/[5] (main_net_inflow / auction_turnover): 万  -> x1e4 -> 元
  - vratio raw[3]/[6]/[10] (seal / auction_turnover)          : 万  -> x1e4 -> 元
  - pool.surge item[8]/[9] (turnover / float_mktcap, FLOAT)   : already 元
  - pool.hot   item[9] free_float_mktcap '182亿'              : parse -> x1e8 -> 元

Importing this module runs _self_test(); a wrong unit / swapped index / a
market-cap field without a caliber tag raises AssertionError and blocks import.
"""

from __future__ import annotations

# --------------------------------------------------------------------------- #
# Unit handling
# --------------------------------------------------------------------------- #
UNIT_FACTOR = {"yuan": 1.0, "wan": 1e4, "yi": 1e8}   # money -> 元
MONEY_UNITS = set(UNIT_FACTOR)
PASSTHROUGH_UNITS = {"pct", "ratio", "price", "count"}
MCAP_CANON = {"free_float_mktcap": "FF", "float_mktcap": "FLOAT", "total_mktcap": "TOTAL"}
_NULLS = {"", "none", "null", "-", "—", "nan"}


def _to_num(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s.lower() in _NULLS:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def parse_cn_amount(s):
    """'182亿'->1.82e10, '+9046万'->9.046e7, '+7.0亿'->7e8, '20.5亿'->2.05e9."""
    if s is None:
        return None
    t = str(s).strip().replace("+", "").replace(",", "")
    if t.lower() in _NULLS:
        return None
    mult = 1.0
    if t.endswith("亿"):        # 亿
        mult, t = 1e8, t[:-1]
    elif t.endswith("万"):      # 万
        mult, t = 1e4, t[:-1]
    elif t.endswith("元"):      # 元
        t = t[:-1]
    try:
        return round(float(t) * mult)
    except ValueError:
        return None


def parse_cn_pct(s):
    if s is None:
        return None
    t = str(s).strip().rstrip("%")
    return _to_num(t)


def _convert(val, unit):
    if unit == "text":
        if val is None:
            return None
        s = str(val).strip()
        return None if s.lower() in _NULLS else s
    if unit in MONEY_UNITS:
        n = _to_num(val)
        return None if n is None else round(n * UNIT_FACTOR[unit])
    if unit in PASSTHROUGH_UNITS:
        return _to_num(val)
    # named-string parsers (pool.hot)
    if unit == "cn_amount":
        return parse_cn_amount(val)
    if unit == "cn_pct":
        return parse_cn_pct(val)
    return val


# --------------------------------------------------------------------------- #
# Registry  dataset_id -> {raw_kind, parse_spec, fields:[{canonical,caliber,unit,raw_ref}]}
#   raw_ref = positional index (positional rows) or chinese source key (named_strings)
# --------------------------------------------------------------------------- #
def _f(canonical, unit, raw_ref, caliber=None):
    d = {"canonical": canonical, "unit": unit, "raw_ref": raw_ref}
    if caliber:
        d["caliber"] = caliber
    return d


REGISTRY = {
    # --- auction.jjyd.vratio (竞价爆量) raw[13] ----------------------------- #
    "auction.jjyd.vratio": {
        "raw_kind": "positional",
        "parse_spec": "list[13]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("free_float_mktcap", "yi", 2, caliber="FF"),   # was MISLABEL auction_volume_ratio
            _f("seal_amount", "wan", 3),
            _f("auction_change_pct", "pct", 4),
            _f("latest_change_pct", "pct", 5),
            _f("auction_turnover", "wan", 6),                 # = bidAmount
            _f("concept", "text", 7),
            _f("yesterday_auction_turnover", "wan", 10),
            _f("volume_ratio", "ratio", 11),                  # the REAL 量比
            _f("turnover_rate", "pct", 12),
        ],
    },
    # --- auction.jjyd.qiangchou (竞价抢筹) raw[13] -------------------------- #
    # NOTE: upstream response is {list:{grab:[...],qiangchou:[...]}}; keep BOTH
    # groups separately. Row layout is identical to vratio except raw[11].
    "auction.jjyd.qiangchou": {
        "raw_kind": "positional",
        "parse_spec": "list[13]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("free_float_mktcap", "yi", 2, caliber="FF"),   # same MISLABEL fix as vratio
            _f("seal_amount", "wan", 3),
            _f("auction_change_pct", "pct", 4),
            _f("latest_change_pct", "pct", 5),
            _f("auction_turnover", "wan", 6),
            _f("concept", "text", 7),
            _f("yesterday_auction_turnover", "wan", 10),
            _f("grab_strength", "ratio", 11),                 # NOT volume_ratio here
            _f("turnover_rate", "pct", 12),
        ],
    },
    # --- auction.jjyd.net_amount (竞价净额, AES) raw[9] --------------------- #
    "auction.jjyd.net_amount": {
        "raw_kind": "positional",
        "parse_spec": "list[9]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("auction_change_pct", "pct", 2),
            _f("latest_change_pct", "pct", 3),
            _f("main_net_inflow", "wan", 4),
            _f("auction_turnover", "wan", 5),
            _f("free_float_mktcap", "yi", 6, caliber="FF"),   # was market_cap_yi
            _f("concept", "text", 7),
            _f("turnover_rate", "pct", 8),
        ],
    },
    # --- auction.jjyd.weimai (涨停委买, AES) raw[18] ------------------------ #
    # 0089 LOCK: every monetary field is ALREADY in 元 (no x1e4 / x1e8).
    "auction.jjyd.weimai": {
        "raw_kind": "positional",
        "parse_spec": "list[18]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("price", "price", 2),
            _f("latest_change_pct", "pct", 3),
            _f("auction_turnover", "yuan", 4),                # no _wan suffix; already 元
            _f("auction_change", "pct", 5),
            _f("main_net_inflow", "yuan", 6),
            _f("turnover_rate", "pct", 7),
            _f("seal_volume", "yuan", 8),
            _f("auction_amount", "yuan", 9),
            _f("seal_volume_again", "yuan", 10),
            _f("concept", "text", 11),
            _f("free_float_mktcap", "yuan", 12, caliber="FF"),  # was market_cap (job 0078 FF)
            _f("main_net_inflow_full", "yuan", 13),           # 0089: 元
            _f("super_large_order", "yuan", 14),              # 0089: 元 (spearman -0.919 w/ [13])
            _f("large_order", "yuan", 15),                    # 0089: 元
            _f("board_label", "text", 16),
            _f("seal_amount", "yuan", 17),                    # 0089: 元 (old name _wan is a misnomer)
        ],
    },
    # --- pool.surge (冲涨池) item[11]; raw stored ------------------------- #
    "pool.surge": {
        "raw_kind": "positional",
        "parse_spec": "list[11]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("change_pct", "pct", 2),
            _f("concept", "text", 6),
            _f("board_state", "text", 7),                     # currently dropped by fetcher -> keep
            _f("turnover_amount", "yuan", 8),                 # already 元
            _f("float_mktcap", "yuan", 9, caliber="FLOAT"),   # ONLY FLOAT table; raw already 元
            _f("turnover_rate", "pct", 10),                   # take SITE item[10], do NOT recompute
        ],
    },
    # --- pool.hot (热门池) NO raw stored: parse legacy named strings ------- #
    "pool.hot": {
        "raw_kind": "named_strings",
        "parse_spec": "dict(cn_keys)",
        "fields": [
            _f("change_pct", "cn_pct", "涨幅"),                  # 涨幅
            _f("main_net", "cn_amount", "主力"),                # 主力
            _f("real_turnover_rate", "cn_pct", "实际换手"),  # 实际换手
            _f("turnover_amount", "cn_amount", "成交"),         # 成交
            _f("free_float_mktcap", "cn_amount", "流通", caliber="FF"),  # 流通 (MISLABEL: is FF)
            _f("concept", "text", "概念"),                      # 概念
        ],
    },
}


# --------------------------------------------------------------------------- #
# Core transform
# --------------------------------------------------------------------------- #
def raw_to_canonical(dataset_id: str, raw_row):
    """raw[] (or named-string dict for pool.hot) -> canonical dict.
    ALL unit conversion lives here."""
    if dataset_id not in REGISTRY:
        raise KeyError(f"unknown dataset_id: {dataset_id!r}")
    spec = REGISTRY[dataset_id]
    named = spec["raw_kind"] == "named_strings"
    out = {}
    for fld in spec["fields"]:
        ref = fld["raw_ref"]
        if named:
            val = raw_row.get(ref) if isinstance(raw_row, dict) else None
        else:
            val = raw_row[ref] if isinstance(raw_row, (list, tuple)) and isinstance(ref, int) and ref < len(raw_row) else None
        out[fld["canonical"]] = _convert(val, fld["unit"])
    return out


def field_caliber(dataset_id: str, canonical: str):
    for fld in REGISTRY[dataset_id]["fields"]:
        if fld["canonical"] == canonical:
            return fld.get("caliber")
    raise KeyError(f"{dataset_id} has no field {canonical!r}")


# --------------------------------------------------------------------------- #
# Caliber validator -- a market-cap field with no caliber tag fails the build
# --------------------------------------------------------------------------- #
def validate_caliber(registry=REGISTRY):
    errors = []
    for dsid, spec in registry.items():
        for fld in spec["fields"]:
            name = fld["canonical"]
            if "mktcap" in name or name in MCAP_CANON:
                cal = fld.get("caliber")
                if not cal:
                    errors.append(f"{dsid}.{name}: market-cap field missing caliber tag")
                elif cal not in ("FF", "FLOAT", "TOTAL"):
                    errors.append(f"{dsid}.{name}: invalid caliber {cal!r}")
                elif name in MCAP_CANON and cal != MCAP_CANON[name]:
                    errors.append(f"{dsid}.{name}: caliber {cal} != expected {MCAP_CANON[name]}")
    if errors:
        raise AssertionError("Caliber validation FAILED: " + "; ".join(errors))
    return True


# --------------------------------------------------------------------------- #
# Self-test -- hardcoded REAL samples from job 0089 unit probe (2026-06-29)
# --------------------------------------------------------------------------- #
def _self_test():
    # 1) caliber validator must pass on the shipped registry
    validate_caliber()

    # 2) vratio: raw[2]=FF mktcap (亿), raw[11]=volume_ratio (倍, no conversion)
    v = ["002407", "多氟多", 462, 32740, "none", "10.0",
         "1779", "氢氟酸", "10.0", "1779", "15", 6.1, 0.52]
    cv = raw_to_canonical("auction.jjyd.vratio", v)
    assert cv["free_float_mktcap"] == 46_200_000_000, cv["free_float_mktcap"]
    assert cv["volume_ratio"] == 6.1, cv["volume_ratio"]
    assert cv["auction_turnover"] == 17_790_000, cv["auction_turnover"]   # 1779万
    # mislabel guards: raw[2] must NOT be the volume ratio, raw[11] must NOT be the mktcap
    assert cv["volume_ratio"] != v[2], "raw[2] wrongly used as volume_ratio"
    assert cv["free_float_mktcap"] != round(float(v[11]) * 1e8), "raw[11] wrongly used as mktcap"
    assert field_caliber("auction.jjyd.vratio", "free_float_mktcap") == "FF"

    # 3) qiangchou: raw[11]=grab_strength (not volume_ratio); raw[2]=FF
    q = ["300279", "和晶科技", 22, None, "none", "1.01",
         "189", "机器人", "1.01", "189", None, "11.93", 0.09]
    cq = raw_to_canonical("auction.jjyd.qiangchou", q)
    assert cq["free_float_mktcap"] == 2_200_000_000, cq["free_float_mktcap"]
    assert cq["grab_strength"] == 11.93, cq["grab_strength"]
    assert cq["auction_turnover"] == 1_890_000, cq["auction_turnover"]
    assert "volume_ratio" not in cq, "qiangchou must expose grab_strength, not volume_ratio"

    # 4) weimai: every monetary field already in 元 (NO x1e4 / x1e8)
    w = ["002407", "多氟多", 45.66, 10, 2339609266, "none",
         144416464, 0.56, 258717139, 1016893860, 258717139,
         "氢氟酸、电解液",
         46177984662, 144416464, 203217386, -58800922, "首板", 208089]
    cw = raw_to_canonical("auction.jjyd.weimai", w)
    assert cw["free_float_mktcap"] == 46177984662, cw["free_float_mktcap"]
    assert cw["main_net_inflow_full"] == 144416464, cw["main_net_inflow_full"]
    assert cw["super_large_order"] == 203217386, cw["super_large_order"]
    assert cw["large_order"] == -58800922, cw["large_order"]
    assert cw["auction_turnover"] == 2339609266, cw["auction_turnover"]
    assert cw["seal_amount"] == 208089, cw["seal_amount"]
    # guard: seal_amount must NOT be treated as 万 (would be 2.08e9)
    assert cw["seal_amount"] != 208089 * 10000, "weimai seal_amount wrongly converted from 万"
    assert field_caliber("auction.jjyd.weimai", "free_float_mktcap") == "FF"

    # 5) net_amount: main_net_inflow/auction_turnover are 万; FF is 亿
    n = ["002407", "多氟多", 10, 10, 14442, 25872, 461.8,
         "氢氟酸|电解液", 0.56]
    cn = raw_to_canonical("auction.jjyd.net_amount", n)
    assert cn["free_float_mktcap"] == 46_180_000_000, cn["free_float_mktcap"]
    assert cn["main_net_inflow"] == 144_420_000, cn["main_net_inflow"]
    assert cn["auction_turnover"] == 258_720_000, cn["auction_turnover"]
    # cross-table consistency on 多氟多 (002407): net_amount vs weimai must agree (<0.5%)
    assert abs(cn["free_float_mktcap"] - cw["free_float_mktcap"]) / cw["free_float_mktcap"] < 5e-3
    assert abs(cn["main_net_inflow"] - cw["main_net_inflow"]) / cw["main_net_inflow"] < 5e-3

    # 6) pool.surge: item[8]/[9] already 元; item[9]=FLOAT; turnover_rate=site item[10]
    s = ["688233", "神工股份", 17.88, "", "", "",
         "芯片+存储", "", 1622951900, 32511365000, 4.99]
    cs = raw_to_canonical("pool.surge", s)
    assert cs["float_mktcap"] == 32511365000, cs["float_mktcap"]
    assert cs["turnover_amount"] == 1622951900, cs["turnover_amount"]
    assert cs["turnover_rate"] == 4.99, cs["turnover_rate"]
    assert field_caliber("pool.surge", "float_mktcap") == "FLOAT"

    # 7) pool.hot: no raw -> parse legacy strings; '182亿' -> 1.82e10 元
    h = {"涨幅": "10.51%", "主力": "+9046万",
         "实际换手": "11.4%", "成交": "20.5亿",
         "流通": "182亿", "概念": "洁净室+光刻胶"}
    ch = raw_to_canonical("pool.hot", h)
    assert ch["free_float_mktcap"] == 18_200_000_000, ch["free_float_mktcap"]
    assert ch["turnover_amount"] == 2_050_000_000, ch["turnover_amount"]
    assert ch["main_net"] == 90_460_000, ch["main_net"]
    assert ch["change_pct"] == 10.51, ch["change_pct"]
    assert field_caliber("pool.hot", "free_float_mktcap") == "FF"
    assert parse_cn_amount("+7.0亿") == 700_000_000

    # 8) caliber validator must REJECT a market-cap field that drops its tag
    broken = {"x": {"raw_kind": "positional", "parse_spec": "list",
                    "fields": [_f("free_float_mktcap", "yi", 0)]}}  # no caliber
    try:
        validate_caliber(broken)
    except AssertionError:
        pass
    else:
        raise AssertionError("validator failed to reject uncalibrated market-cap field")

    return True


# Block import on any unit / mislabel / caliber regression.
_self_test()


if __name__ == "__main__":
    print("duanxianxia_canonical self-test: PASS")
    print("datasets:", ", ".join(REGISTRY))
