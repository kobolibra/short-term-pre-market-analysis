#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_canonical.py  --  Task 0090 (v10 rebuild, transform-1 canonical layer)

Single source of truth that turns a dataset's raw[] positional array (or, for
pool.hot, the legacy named-string row) into a canonical dict with correct field
names (fixes the historical mislabels), a caliber tag on every market-cap
field, and all monetary values normalised to base yuan.

0156 FIX (weimai raw[4]/raw[8]): raw[4] is the GROSS limit-direction committed
buy BEFORE removing auction-executed turnover ("weimai, un-netted"), renamed
seal_amount_wan_raw (user naming). raw[8] is the REAL matched auction turnover,
renamed auction_turnover to match the other tables. Sealed-board identity:
seal_amount_wan_raw - auction_turnover(raw8) = seal_amount(raw17).
"""

from __future__ import annotations

UNIT_FACTOR = {"yuan": 1.0, "wan": 1e4, "yi": 1e8}   # money -> yuan
MONEY_UNITS = set(UNIT_FACTOR)
PASSTHROUGH_UNITS = {"pct", "ratio", "price", "count"}
MCAP_CANON = {"free_float_mktcap": "FF", "float_mktcap": "FLOAT", "total_mktcap": "TOTAL"}
_NULLS = {"", "none", "null", "-", "\u2014", "nan"}

NAMED_KINDS = {"named_strings", "named_dict"}


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
    if s is None:
        return None
    t = str(s).strip().replace("+", "").replace(",", "")
    if t.lower() in _NULLS:
        return None
    mult = 1.0
    if t.endswith("\u4ebf"):
        mult, t = 1e8, t[:-1]
    elif t.endswith("\u4e07"):
        mult, t = 1e4, t[:-1]
    elif t.endswith("\u5143"):
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
    if unit == "cn_amount":
        return parse_cn_amount(val)
    if unit == "cn_pct":
        return parse_cn_pct(val)
    return val


def _f(canonical, unit, raw_ref, caliber=None, fallback_ref=None):
    d = {"canonical": canonical, "unit": unit, "raw_ref": raw_ref}
    if caliber:
        d["caliber"] = caliber
    if fallback_ref is not None:
        d["fallback_ref"] = fallback_ref
    return d


REGISTRY = {
    "auction.jjyd.vratio": {
        "raw_kind": "positional",
        "parse_spec": "list[13]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("free_float_mktcap", "yi", 2, caliber="FF"),   # was MISLABEL auction_volume_ratio
            _f("seal_amount", "wan", 3),
            _f("auction_change_pct", "pct", 4, fallback_ref=8),
            _f("latest_change_pct", "pct", 5),
            _f("auction_turnover", "wan", 6),                 # = bidAmount
            _f("concept", "text", 7),
            _f("yesterday_auction_turnover", "wan", 10),
            _f("volume_ratio", "ratio", 11),                  # the REAL volume ratio
            _f("turnover_rate", "pct", 12),
        ],
    },
    "auction.jjyd.qiangchou": {
        "raw_kind": "positional",
        "parse_spec": "list[13]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("free_float_mktcap", "yi", 2, caliber="FF"),
            _f("seal_amount", "wan", 3),
            _f("auction_change_pct", "pct", 4, fallback_ref=8),
            _f("latest_change_pct", "pct", 5),
            _f("auction_turnover", "wan", 6),
            _f("concept", "text", 7),
            _f("yesterday_auction_turnover", "wan", 10),
            _f("grab_strength", "ratio", 11),
            _f("turnover_rate", "pct", 12),
        ],
    },
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
    # --- auction.jjyd.weimai raw[18] --- 0089 unit lock: monetary fields already
    # in yuan EXCEPT raw[17] seal_amount (wan). 0156 FIX applied to raw[4]/[8]/[10].
    "auction.jjyd.weimai": {
        "raw_kind": "positional",
        "parse_spec": "list[18]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("price", "price", 2),
            _f("latest_change_pct", "pct", 3),
            # 0156 FIX: raw[4] is the GROSS committed buy BEFORE removing the
            # auction-executed turnover (un-netted weimai), NOT the matched
            # turnover. Per user naming it parallels seal_amount -> seal_amount_wan_raw.
            # Sealed-board identity: seal_amount_wan_raw - auction_turnover(raw8) = seal_amount(raw17).
            _f("seal_amount_wan_raw", "yuan", 4),             # gross committed buy; already yuan
            _f("auction_change", "pct", 5),
            _f("main_net_inflow", "yuan", 6),
            _f("turnover_rate", "pct", 7),
            _f("auction_turnover", "yuan", 8),                # 0156 FIX: real matched turnover; == vratio raw[6]; unified name
            _f("auction_amount", "yuan", 9),
            _f("auction_turnover_dup", "yuan", 10),           # 0156 FIX: duplicate of raw[8]
            _f("concept", "text", 11),
            _f("free_float_mktcap", "yuan", 12, caliber="FF"),  # was market_cap (job 0078 FF)
            _f("main_net_inflow_full", "yuan", 13),
            _f("super_large_order", "yuan", 14),
            _f("large_order", "yuan", 15),
            _f("board_label", "text", 16),
            _f("seal_amount", "wan", 17),                    # 0091: wan->x1e4->yuan (seal amount; sealed boards only)
        ],
    },
    "pool.surge": {
        "raw_kind": "positional",
        "parse_spec": "list[11]",
        "fields": [
            _f("code", "text", 0),
            _f("name", "text", 1),
            _f("change_pct", "pct", 2),
            _f("concept", "text", 6),
            _f("board_state", "text", 7),
            _f("turnover_amount", "yuan", 8),
            _f("float_mktcap", "yuan", 9, caliber="FLOAT"),
            _f("turnover_rate", "pct", 10),
        ],
    },
    "pool.hot": {
        "raw_kind": "named_strings",
        "parse_spec": "dict(cn_keys)",
        "fields": [
            _f("change_pct", "cn_pct", "\u6da8\u5e45"),
            _f("main_net", "cn_amount", "\u4e3b\u529b"),
            _f("real_turnover_rate", "cn_pct", "\u5b9e\u9645\u6362\u624b"),
            _f("turnover_amount", "cn_amount", "\u6210\u4ea4"),
            _f("free_float_mktcap", "cn_amount", "\u6d41\u901a", caliber="FF"),
            _f("concept", "text", "\u6982\u5ff5"),
        ],
    },
}


_CASHFLOW_FIELDS = [
    _f("code", "text", "\u4ee3\u7801"),
    _f("name", "text", "\u540d\u79f0"),
    _f("price", "price", "\u6700\u65b0\u4ef7"),
    _f("latest_change_pct", "cn_pct", "\u6da8\u8dcc\u5e45"),
    _f("main_net", "cn_amount", "\u4e3b\u529b\u51c0\u6d41\u5165"),
    _f("xl_net", "cn_amount", "\u7279\u5927\u5355\u51c0\u6d41\u5165"),
    _f("big_net", "cn_amount", "\u5927\u5355\u51c0\u6d41\u5165"),
    _f("mid_net", "cn_amount", "\u4e2d\u5355\u51c0\u6d41\u5165"),
    _f("small_net", "cn_amount", "\u5c0f\u5355\u51c0\u6d41\u5165"),
]
for _cf_dsid in ("cashflow.stock.today", "cashflow.stock.3day",
                 "cashflow.stock.5day", "cashflow.stock.10day"):
    REGISTRY[_cf_dsid] = {
        "raw_kind": "named_dict",
        "parse_spec": "dict(cn_keys)",
        "fields": [dict(_fld) for _fld in _CASHFLOW_FIELDS],
    }

REGISTRY.update({
    "rank.rocket": {
        "raw_kind": "named_dict",
        "parse_spec": "dict(en_keys)",
        "fields": [
            _f("code", "text", "code"),
            _f("name", "text", "name"),
            _f("hot_rank", "count", "rank"),
            _f("hot_value", "count", "raw_rate"),
            _f("hot_delta_disp", "text", "value"),
        ],
    },
    "rank.hot_stock_day": {
        "raw_kind": "named_dict",
        "parse_spec": "dict(en_keys)",
        "fields": [
            _f("code", "text", "code"),
            _f("name", "text", "name"),
            _f("hot_rank", "count", "rank"),
            _f("hot_value", "count", "raw_rate"),
            _f("hot_delta_disp", "text", "value"),
        ],
    },
    "auction.jjlive.fengdan": {
        "raw_kind": "named_dict",
        "parse_spec": "dict(en_keys)",
        "fields": [
            _f("code", "text", "code"),
            _f("name", "text", "name"),
            _f("rank", "count", "rank"),
            _f("board_label", "text", "board_label"),
            _f("seal_bid_915", "cn_amount", "amount_915", caliber="commit_bid"),
            _f("seal_bid_920", "cn_amount", "amount_920", caliber="commit_bid"),
            _f("seal_bid_925", "cn_amount", "amount_925", caliber="commit_bid"),
            _f("latest_change_pct", "cn_pct", "latest_change_pct"),
            _f("concept", "text", "tag_1"),
            ],
    },
    "home.ztpool": {
        "raw_kind": "named_dict",
        "parse_spec": "dict(cn_keys)",
        "fields": [
            _f("code", "text", "\u4ee3\u7801"),
            _f("name", "text", "\u540d\u79f0"),
            _f("zt_status", "text", "\u72b6\u6001"),
            _f("zt_status_style", "text", "\u72b6\u6001\u6837\u5f0f"),
            _f("latest_change_pct", "cn_pct", "\u6da8\u5e45"),
            _f("concept", "text", "\u9898\u6750"),
            _f("ladder_group", "text", "\u5206\u7ec4\u540d\u79f0"),
            _f("promo_rate", "cn_pct", "\u664b\u7ea7\u7387"),
            _f("promo_num", "count", "\u664b\u7ea7\u6570"),
            _f("sample_num", "count", "\u6837\u672c\u6570"),
            _f("market", "text", "\u5e02\u573a"),
            _f("date", "text", "\u65e5\u671f"),
        ],
    },
    "review.ltgd.range": {
        "raw_kind": "named_dict",
        "parse_spec": "dict(cn_keys)",
        "fields": [
            _f("code", "text", "\u4ee3\u7801"),
            _f("name", "text", "\u540d\u79f0"),
            _f("range_period", "text", "\u5468\u671f"),
            _f("range_return", "cn_pct", "\u533a\u95f4\u6da8\u5e45"),
            _f("range_rank", "count", "\u6392\u540d"),
            _f("board", "text", "\u677f\u5757"),
            _f("concept", "text", "\u6982\u5ff5"),
            _f("date_range", "text", "\u65e5\u671f\u533a\u95f4"),
        ],
    },
    "review.fupan.plate": {
        "raw_kind": "named_dict",
        "parse_spec": "dict(cn_keys)",
        "fields": [
            _f("code", "text", "\u4ee3\u7801"),
            _f("name", "text", "\u540d\u79f0"),
            _f("price", "price", "\u80a1\u4ef7"),
            _f("latest_change_pct", "cn_pct", "\u6da8\u5e45"),
            _f("zt_type", "text", "\u6da8\u505c\u7c7b\u578b"),
            _f("board_count_text", "text", "\u677f\u6570"),
            _f("streak", "count", "\u8fde\u677f"),
            _f("first_seal_time", "text", "\u9996\u6b21\u5c01\u677f"),
            _f("last_seal_time", "text", "\u6700\u540e\u5c01\u677f"),
            _f("open_num", "count", "\u5f00\u677f"),
            _f("seal_amount", "cn_amount", "\u5c01\u5355\u989d", caliber="commit_bid"),
            _f("turnover_amount", "cn_amount", "\u6210\u4ea4\u989d"),
            _f("turnover_rate", "cn_pct", "\u6362\u624b\u7387"),
            _f("free_float_mktcap", "cn_amount", "\u5b9e\u9645\u6d41\u901a", caliber="FF"),
            _f("float_mktcap", "cn_amount", "\u6d41\u901a\u5e02\u503c", caliber="FLOAT"),
            _f("total_mktcap", "cn_amount", "\u603b\u5e02\u503c", caliber="TOTAL"),
            _f("concept", "text", "\u9898\u6750\u540d\u79f0"),
            _f("change_reason", "text", "\u5f02\u52a8\u539f\u56e0"),
        ],
    },
})


def raw_to_canonical(dataset_id: str, raw_row):
    if dataset_id not in REGISTRY:
        raise KeyError(f"unknown dataset_id: {dataset_id!r}")
    spec = REGISTRY[dataset_id]
    named = spec["raw_kind"] in NAMED_KINDS

    def _get(ref):
        if named:
            return raw_row.get(ref) if isinstance(raw_row, dict) else None
        return raw_row[ref] if isinstance(raw_row, (list, tuple)) and isinstance(ref, int) and ref < len(raw_row) else None

    out = {}
    for fld in spec["fields"]:
        conv = _convert(_get(fld["raw_ref"]), fld["unit"])
        if conv is None and fld.get("fallback_ref") is not None:
            fbs = fld["fallback_ref"]
            if not isinstance(fbs, (list, tuple)):
                fbs = [fbs]
            for fb in fbs:
                conv = _convert(_get(fb), fld["unit"])
                if conv is not None:
                    break
        out[fld["canonical"]] = conv
    return out


def field_caliber(dataset_id: str, canonical: str):
    for fld in REGISTRY[dataset_id]["fields"]:
        if fld["canonical"] == canonical:
            return fld.get("caliber")
    raise KeyError(f"{dataset_id} has no field {canonical!r}")


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


def _self_test():
    validate_caliber()

    v = ["002407", "\u591a\u6c1f\u591a", 462, 32740, "none", "10.0",
         "1779", "\u6c22\u6c1f\u9178", "10.0", "1779", "15", 6.1, 0.52]
    cv = raw_to_canonical("auction.jjyd.vratio", v)
    assert cv["free_float_mktcap"] == 46_200_000_000, cv["free_float_mktcap"]
    assert cv["volume_ratio"] == 6.1, cv["volume_ratio"]
    assert cv["auction_turnover"] == 17_790_000, cv["auction_turnover"]
    assert cv["auction_change_pct"] == 10.0, cv["auction_change_pct"]
    _vp = list(v); _vp[4] = 8.88
    assert raw_to_canonical("auction.jjyd.vratio", _vp)["auction_change_pct"] == 8.88, "primary must win over fallback"
    assert cv["volume_ratio"] != v[2], "raw[2] wrongly used as volume_ratio"
    assert cv["free_float_mktcap"] != round(float(v[11]) * 1e8), "raw[11] wrongly used as mktcap"
    assert field_caliber("auction.jjyd.vratio", "free_float_mktcap") == "FF"

    q = ["300279", "\u548c\u6676\u79d1\u6280", 22, None, "none", "1.01",
         "189", "\u673a\u5668\u4eba", "1.01", "189", None, "11.93", 0.09]
    cq = raw_to_canonical("auction.jjyd.qiangchou", q)
    assert cq["free_float_mktcap"] == 2_200_000_000, cq["free_float_mktcap"]
    assert cq["grab_strength"] == 11.93, cq["grab_strength"]
    assert cq["auction_turnover"] == 1_890_000, cq["auction_turnover"]
    assert cq["auction_change_pct"] == 1.01, cq["auction_change_pct"]
    assert "volume_ratio" not in cq, "qiangchou must expose grab_strength, not volume_ratio"

    # weimai: 0156 FIX -- raw[4]=gross committed buy(seal_amount_wan_raw),
    # raw[8]=real matched turnover(auction_turnover), raw[10]=raw[8] dup.
    w = ["002407", "\u591a\u6c1f\u591a", 45.66, 10, 2339609266, "none",
         144416464, 0.56, 258717139, 1016893860, 258717139,
         "\u6c22\u6c1f\u9178\u3001\u7535\u89e3\u6db2",
         46177984662, 144416464, 203217386, -58800922, "\u9996\u677f", 208089]
    cw = raw_to_canonical("auction.jjyd.weimai", w)
    assert cw["free_float_mktcap"] == 46177984662, cw["free_float_mktcap"]
    assert cw["main_net_inflow_full"] == 144416464, cw["main_net_inflow_full"]
    assert cw["super_large_order"] == 203217386, cw["super_large_order"]
    assert cw["large_order"] == -58800922, cw["large_order"]
    assert cw["seal_amount_wan_raw"] == 2339609266, cw["seal_amount_wan_raw"]
    assert cw["auction_turnover"] == 258717139, cw["auction_turnover"]
    assert cw["auction_turnover_dup"] == 258717139, cw["auction_turnover_dup"]
    assert cw["auction_turnover"] != 2339609266, "raw[4] wrongly used as auction_turnover"
    assert cw["seal_amount"] == 208089 * 10000, cw["seal_amount"]
    # identity: gross committed buy(raw4) - matched turnover(raw8) = seal_amount(raw17)
    assert abs((cw["seal_amount_wan_raw"] - cw["auction_turnover"]) - cw["seal_amount"]) <= 10000, \
        (cw["seal_amount_wan_raw"], cw["auction_turnover"], cw["seal_amount"])
    assert cw["seal_amount"] != 208089, "weimai seal_amount wrongly left as yuan"
    assert field_caliber("auction.jjyd.weimai", "free_float_mktcap") == "FF"

    n = ["002407", "\u591a\u6c1f\u591a", 10, 10, 14442, 25872, 461.8,
         "\u6c22\u6c1f\u9178|\u7535\u89e3\u6db2", 0.56]
    cn = raw_to_canonical("auction.jjyd.net_amount", n)
    assert cn["free_float_mktcap"] == 46_180_000_000, cn["free_float_mktcap"]
    assert cn["main_net_inflow"] == 144_420_000, cn["main_net_inflow"]
    assert cn["auction_turnover"] == 258_720_000, cn["auction_turnover"]
    assert abs(cn["free_float_mktcap"] - cw["free_float_mktcap"]) / cw["free_float_mktcap"] < 5e-3
    assert abs(cn["main_net_inflow"] - cw["main_net_inflow"]) / cw["main_net_inflow"] < 5e-3

    s = ["688233", "\u795e\u5de5\u80a1\u4efd", 17.88, "", "", "",
         "\u82af\u7247+\u5b58\u50a8", "", 1622951900, 32511365000, 4.99]
    cs = raw_to_canonical("pool.surge", s)
    assert cs["float_mktcap"] == 32511365000, cs["float_mktcap"]
    assert cs["turnover_amount"] == 1622951900, cs["turnover_amount"]
    assert cs["turnover_rate"] == 4.99, cs["turnover_rate"]
    assert field_caliber("pool.surge", "float_mktcap") == "FLOAT"

    h = {"\u6da8\u5e45": "10.51%", "\u4e3b\u529b": "+9046\u4e07",
         "\u5b9e\u9645\u6362\u624b": "11.4%", "\u6210\u4ea4": "20.5\u4ebf",
         "\u6d41\u901a": "182\u4ebf", "\u6982\u5ff5": "\u6d01\u51c0\u5ba4+\u5149\u523b\u80f6"}
    ch = raw_to_canonical("pool.hot", h)
    assert ch["free_float_mktcap"] == 18_200_000_000, ch["free_float_mktcap"]
    assert ch["turnover_amount"] == 2_050_000_000, ch["turnover_amount"]
    assert ch["main_net"] == 90_460_000, ch["main_net"]
    assert ch["change_pct"] == 10.51, ch["change_pct"]
    assert field_caliber("pool.hot", "free_float_mktcap") == "FF"
    assert parse_cn_amount("+7.0\u4ebf") == 700_000_000

    broken = {"x": {"raw_kind": "positional", "parse_spec": "list",
                    "fields": [_f("free_float_mktcap", "yi", 0)]}}
    try:
        validate_caliber(broken)
    except AssertionError:
        pass
    else:
        raise AssertionError("validator failed to reject uncalibrated market-cap field")

    rk = raw_to_canonical("rank.rocket",
        {"rank": "1", "code": "002674", "name": "\u5174\u4e1a\u79d1\u6280",
         "value": "+71w", "raw_rate": "707944"})
    assert rk["code"] == "002674", rk
    assert rk["hot_value"] == 707944.0, rk["hot_value"]
    assert rk["hot_rank"] == 1.0, rk["hot_rank"]
    assert rk["hot_delta_disp"] == "+71w", rk
    assert REGISTRY["rank.hot_stock_day"]["raw_kind"] == "named_dict"

    cf = raw_to_canonical("cashflow.stock.today",
        {"\u6392\u540d": 1, "\u540d\u79f0": "\u5146\u6613\u521b\u65b0",
         "\u4ee3\u7801": "603986", "\u80a1\u5708": "\u8fdb\u5165",
         "\u6700\u65b0\u4ef7": "487.90", "\u6da8\u8dcc\u5e45": "4.47%",
         "\u4e3b\u529b\u51c0\u6d41\u5165": "7.76\u4ebf",
         "\u7279\u5927\u5355\u51c0\u6d41\u5165": "8.33\u4ebf",
         "\u5927\u5355\u51c0\u6d41\u5165": "-5720\u4e07",
         "\u4e2d\u5355\u51c0\u6d41\u5165": "-7.76\u4ebf",
         "\u5c0f\u5355\u51c0\u6d41\u5165": "-43.3\u4e07"})
    assert cf["code"] == "603986", cf
    assert cf["price"] == 487.90, cf["price"]
    assert cf["latest_change_pct"] == 4.47, cf["latest_change_pct"]
    assert cf["main_net"] == 776_000_000, cf["main_net"]
    assert cf["big_net"] == -57_200_000, cf["big_net"]
    assert cf["small_net"] == -433_000, cf["small_net"]
    for _d in ("cashflow.stock.today", "cashflow.stock.3day",
               "cashflow.stock.5day", "cashflow.stock.10day"):
        assert _d in REGISTRY, _d
        assert [f["canonical"] for f in REGISTRY[_d]["fields"]] == \
               [f["canonical"] for f in REGISTRY["cashflow.stock.today"]["fields"]]

    fd = raw_to_canonical("auction.jjlive.fengdan",
        {"rank": 1, "code": "603890", "name": "\u6625\u79cb\u7535\u5b50",
         "tag_1": "AI PC", "board_label": "2\u677f",
         "amount_915": "63.9\u4ebf", "amount_920": "19.7\u4ebf",
         "amount_925": "20.6\u4ebf", "latest_change_pct": "10.00%"})
    assert fd["code"] == "603890", fd
    assert fd["seal_bid_915"] == 6_390_000_000, fd["seal_bid_915"]
    assert fd["latest_change_pct"] == 10.0, fd["latest_change_pct"]
    assert fd["concept"] == "AI PC", fd["concept"]
    assert field_caliber("auction.jjlive.fengdan", "seal_bid_915") == "commit_bid"

    zt = raw_to_canonical("home.ztpool",
        {"\u65e5\u671f": "2026-07-01", "\u5206\u7ec4\u5e8f\u53f7": "2",
         "\u5206\u7ec4\u540d\u79f0": "2\u8fdb3", "\u7ec4\u5185\u5e8f\u53f7": "1",
         "\u664b\u7ea7\u7387\u6587\u672c": "3/18=17%", "\u664b\u7ea7\u6570": "3",
         "\u6837\u672c\u6570": "18", "\u664b\u7ea7\u7387": "17%",
         "\u5e02\u573a": "\u6caa", "\u4ee3\u7801": "600113",
         "\u540d\u79f0": "\u6d59\u6c5f\u4e1c\u65e5", "\u72b6\u6001": "\u6210",
         "\u72b6\u6001\u6837\u5f0f": "success", "\u6da8\u5e45": "10.01%",
         "\u9898\u6750": "\u96f6\u552e"})
    assert zt["code"] == "600113", zt
    assert zt["zt_status"] == "\u6210", zt["zt_status"]
    assert zt["latest_change_pct"] == 10.01, zt["latest_change_pct"]
    assert zt["ladder_group"] == "2\u8fdb3", zt["ladder_group"]
    assert zt["promo_rate"] == 17.0, zt["promo_rate"]
    assert zt["promo_num"] == 3.0 and zt["sample_num"] == 18.0, zt

    lt = raw_to_canonical("review.ltgd.range",
        {"\u5468\u671f": "5\u65e5", "\u677f\u5757": "\u4e3b\u677f",
         "\u677f\u5757\u987a\u5e8f": "0", "\u6392\u540d": "8",
         "\u4ee3\u7801": "000004", "\u540d\u79f0": "\u56fd\u534e\u9000",
         "\u533a\u95f4\u6da8\u5e45": "46%", "\u6982\u5ff5": "\u7834\u51c0\u80a1\u6982\u5ff5",
         "\u6982\u5ff5\u952e": "\u7834\u51c0\u80a1\u6982\u5ff5",
         "\u65e5\u671f\u533a\u95f4": "2026-06-25 - 2026-07-02"})
    assert lt["code"] == "000004", lt
    assert lt["range_period"] == "5\u65e5", lt["range_period"]
    assert lt["range_return"] == 46.0, lt["range_return"]
    assert lt["range_rank"] == 8.0, lt["range_rank"]

    fp = raw_to_canonical("review.fupan.plate",
        {"\u65e5\u671f": "2026-07-02", "\u9898\u6750\u540d\u79f0": "\u673a\u5668\u4eba",
         "\u540d\u79f0": "\u798f\u83b1\u65b0\u6750", "\u4ee3\u7801": "605488",
         "\u80a1\u4ef7": "34.32", "\u6da8\u5e45": "10.00%",
         "\u6da8\u505c\u7c7b\u578b": "\u56de\u5c01\u677f", "\u677f\u6570": "2\u59292\u677f",
         "\u8fde\u677f": "2", "\u9996\u6b21\u5c01\u677f": "10:09:26",
         "\u6700\u540e\u5c01\u677f": "13:26:39", "\u5f00\u677f": "3",
         "\u5c01\u5355\u989d": "5697\u4e07", "\u6210\u4ea4\u989d": "7\u4ebf",
         "\u6362\u624b\u7387": "7.6%", "\u5b9e\u9645\u6d41\u901a": "42.4\u4ebf",
         "\u6d41\u901a\u5e02\u503c": "95\u4ebf", "\u603b\u5e02\u503c": "104\u4ebf",
         "\u5f02\u52a8\u539f\u56e0": "x"})
    assert fp["code"] == "605488", fp
    assert fp["price"] == 34.32, fp["price"]
    assert fp["streak"] == 2.0, fp["streak"]
    assert fp["open_num"] == 3.0, fp["open_num"]
    assert fp["seal_amount"] == 56_970_000, fp["seal_amount"]
    assert fp["turnover_amount"] == 700_000_000, fp["turnover_amount"]
    assert fp["turnover_rate"] == 7.6, fp["turnover_rate"]
    assert fp["free_float_mktcap"] == 4_240_000_000, fp["free_float_mktcap"]
    assert fp["float_mktcap"] == 9_500_000_000, fp["float_mktcap"]
    assert fp["total_mktcap"] == 10_400_000_000, fp["total_mktcap"]
    assert field_caliber("review.fupan.plate", "free_float_mktcap") == "FF"
    assert field_caliber("review.fupan.plate", "float_mktcap") == "FLOAT"
    assert field_caliber("review.fupan.plate", "total_mktcap") == "TOTAL"
    assert fp["free_float_mktcap"] <= fp["float_mktcap"] <= fp["total_mktcap"], fp

    return True


_self_test()


if __name__ == "__main__":
    print("duanxianxia_canonical self-test: PASS")
    print("datasets:", ", ".join(REGISTRY))
