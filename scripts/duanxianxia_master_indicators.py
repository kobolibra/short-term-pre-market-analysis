#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_master_indicators.py -- Task 0113 (additive, read-only); Task 0117 extended.

任何选股框架都是“每只股票 x 多维度指标”的综合判断。但我们下载的十几张表：
角度不同、含股不同、字段不同。本模块把“指标 <-> 表”的关系固化为单一真源，
并提供以个股为中心的宽表装配器，一次性解决四件事：
  (1) 指标->表路由: sources_for(indicator) / tables_for(indicator) -> 明确该关联哪些表。
  (2) 跨表同一性: 以规范化 6 位代码为唯一主键，取全部股级表代码并集。
  (3) 别名去重: 同一语义字段在不同表名字不同 -> 候选 key 列表归一; 危险重名隔离
      (量比=vratio.raw[11]; 抢筹=qiangchou.raw[11]; FF/FLOAT/TOTAL 三口径不合并)。
  (4) 缺失语义: 某票在某表当天缺席 -> 显式 None + 记录命中来源; 绝不用 0 冒充。

真源 = canonical-field-dictionary.md / field-rename-map.md / fixed-table-contract.md
     + duanxianxia_canonical.REGISTRY (Task 0116 将 10 张股级表全部登记)。
只读; 不写 git。build_master_panel() 只能从已登记 canonical REGISTRY 的表拉数;
未登记的表在索引里标 canonical=False (已知缺口, 待登记)。

Task 0117: 0116 已把 fengdan/ztpool/fupan/ltgd/cashflow.{today,3,5,10day}/rank.{rocket,
hot_stock_day} 全部登记 canonical REGISTRY, 故此处 canonical 均翻为 True, 并将
旧 index-only 指标的候选 key 对齐到 0116 的 canonical 字段名。
用法: python3 scripts/duanxianxia_master_indicators.py [captures_dir] [--cutoff HH:MM]
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WS = HERE.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

# ---------------------------------------------------------------------------
# DATASET SCOPE TABLE -- 每张表的联接角色 (股级 vs 市场上下文) 与主键
# ---------------------------------------------------------------------------
STOCK = "stock"       # 以个股为行, 可按 code 并入宽表
CONTEXT = "context"   # 非个股 (板块/大盘/梯队), 作为行级上下文
DATASETS = {
    "auction.jjyd.vratio":       {"scope": STOCK, "canonical": True,  "note": "竞价爆量; 竞价异动股(~155/天)"},
    "auction.jjyd.qiangchou":    {"scope": STOCK, "canonical": True,  "note": "竞价抢筹; raw[11]=抢筹幅度(非量比)"},
    "auction.jjyd.net_amount":   {"scope": STOCK, "canonical": True,  "note": "竞价净额; raw[6]=FF"},
    "auction.jjyd.weimai":       {"scope": STOCK, "canonical": True,  "note": "委买/打板; raw[12]=FF, raw[17]=封单额万"},
    "pool.hot":                  {"scope": STOCK, "canonical": True,  "note": "盘中热门池; item9=FF(旧标流通)"},
    "pool.surge":                {"scope": STOCK, "canonical": True,  "note": "盘中冲涨池; item9=FLOAT(唯一真流通)"},
    # ---- Task 0116 新登记 canonical (named_dict) ----
    "auction.jjlive.fengdan":    {"scope": STOCK, "canonical": True,  "note": "竞价封单阶梯; 915/920/925=委买/封单额(commit_bid, 非成交)"},
    "home.ztpool":               {"scope": STOCK, "canonical": True,  "note": "涨停晋级梯队; 状态 成/炸/败 -> zt_status; intraday-eod"},
    "review.fupan.plate":        {"scope": STOCK, "canonical": True,  "note": "涨停复盘; FF/FLOAT/TOTAL 三口径黄金锥点表; 开板=open_num; postmarket-eod"},
    "review.ltgd.range":         {"scope": STOCK, "canonical": True,  "note": "龙头高度区间涨幅; 5/10/20/50日 按 range_period 透视"},
    "cashflow.stock.today":      {"scope": STOCK, "canonical": True,  "note": "资金流向当日 n=50; 亿/万->元; postmarket-eod"},
    "cashflow.stock.3day":       {"scope": STOCK, "canonical": True,  "note": "资金流向 3日; postmarket-eod_window"},
    "cashflow.stock.5day":       {"scope": STOCK, "canonical": True,  "note": "资金流向 5日; postmarket-eod_window"},
    "cashflow.stock.10day":      {"scope": STOCK, "canonical": True,  "note": "资金流向 10日; postmarket-eod_window"},
    "rank.rocket":               {"scope": STOCK, "canonical": True,  "note": "人气飙升榜; raw_rate=热度原值(非资金); premarket-live"},
    "rank.hot_stock_day":        {"scope": STOCK, "canonical": True,  "note": "个股当日人气榜; raw_rate=热度原值; premarket-live"},
    # ---- 市场环境表 (非个股粒度; 不入个股 canonical, 留作 market_env_score) ----
    "home.kaipan.plate.summary": {"scope": CONTEXT, "canonical": False, "note": "板块强度(非个股)"},
    "home.qxlive.top_metrics":   {"scope": CONTEXT, "canonical": False, "note": "大盘 12 指标(按 metric_key, 非个股)"},
    "review.daily.top_metrics":  {"scope": CONTEXT, "canonical": False, "note": "每日复盘顶部 17 情绪指标(非个股)"},
}

# ---------------------------------------------------------------------------
# MASTER INDICATOR INDEX -- 每个分析指标 -> 来源表(按优先级) + 口径 + 别名 key
#   sources: [ (dataset_id, provenance) , ... ]  列表顺序 = 取数优先级
#   keys:    canonical dict 上依次尝试的候选键 (解决“不同表字段名不同”)
#   Task 0117: 所有 key 均与 duanxianxia_canonical 的 canonical 字段名对齐。
# ---------------------------------------------------------------------------
INDICATORS = {
    "free_float_mktcap": {
        "desc": "自由流通市值", "caliber": "FF", "unit": "元",
        "keys": ["free_float_mktcap", "free_float_market_cap"],
        "sources": [("auction.jjyd.net_amount", "raw[6] 亿->元"),
                    ("auction.jjyd.vratio", "raw[2] 亿->元 (旧误标 auction_volume_ratio)"),
                    ("auction.jjyd.qiangchou", "raw[2] 亿->元 (旧误标)"),
                    ("auction.jjyd.weimai", "raw[12] 元"),
                    ("pool.hot", "item9 自由流通(旧标流通)"),
                    ("review.fupan.plate", "实际流通 (EOD 锥点)")],
        "dedup_note": "与 FLOAT/TOTAL 严格分开; vratio/qiangchou raw[2] 历史被误标为量比",
    },
    "float_mktcap": {
        "desc": "流通市值", "caliber": "FLOAT", "unit": "元",
        "keys": ["float_mktcap", "float_market_cap"],
        "sources": [("pool.surge", "item9 亿->元 (本簇唯一真流通)"),
                    ("review.fupan.plate", "流通市值")],
    },
    "total_mktcap": {
        "desc": "总市值", "caliber": "TOTAL", "unit": "元",
        "keys": ["total_mktcap", "total_market_cap"],
        "sources": [("review.fupan.plate", "总市值 (唯一 TOTAL 真源)")],
    },
    "auction_turnover": {
        "desc": "竞价成交额 (bidAmount)", "caliber": None, "unit": "元",
        "keys": ["auction_turnover", "auction_turnover_wan", "bidAmount"],
        "sources": [("auction.jjyd.vratio", "raw[6] 万->元"),
                    ("auction.jjyd.net_amount", "raw[5] 万->元"),
                    ("auction.jjyd.weimai", "raw[4] 元"),
                    ("auction.jjyd.qiangchou", "raw[6] 万->元")],
    },
    "volume_ratio": {
        "desc": "量比", "caliber": None, "unit": "x",
        "keys": ["volume_ratio", "volumeRatio", "volume_ratio_multiple"],
        "sources": [("auction.jjyd.vratio", "raw[11] 真量比")],
        "dedup_note": "仅 vratio.raw[11]; 绝不取 qiangchou.raw[11](=抢筹)",
    },
    "grab_strength": {
        "desc": "抢筹幅度", "caliber": None, "unit": "x",
        "keys": ["grab_strength", "grabStrength"],
        "sources": [("auction.jjyd.qiangchou", "raw[11] 抢筹幅度")],
        "dedup_note": "仅 qiangchou.raw[11]; 与 volume_ratio 互斥",
    },
    "turnover_rate": {
        "desc": "换手率", "caliber": None, "unit": "%",
        "keys": ["turnover_rate", "turnover_rate_pct", "turnoverRate", "real_turnover_rate"],
        "sources": [("auction.jjyd.vratio", "raw[12]"),
                    ("auction.jjyd.net_amount", "raw[8]"),
                    ("auction.jjyd.weimai", "raw[7]"),
                    ("pool.hot", "item11 实际换手")],
    },
    "auction_change_pct": {
        "desc": "竞价涨幅", "caliber": None, "unit": "%",
        "keys": ["auction_change_pct"],
        "sources": [("auction.jjyd.vratio", "raw[4]"),
                    ("auction.jjyd.net_amount", "raw[2]")],
    },
    "latest_change_pct": {
        "desc": "最新涨幅", "caliber": None, "unit": "%",
        "keys": ["latest_change_pct", "change_pct", "latestChangePct"],
        "sources": [("auction.jjyd.vratio", "raw[5]"),
                    ("auction.jjyd.net_amount", "raw[3]"),
                    ("auction.jjyd.weimai", "raw[3]"),
                    ("pool.hot", "item2"), ("pool.surge", "item2")],
    },
    "main_net_inflow": {
        "desc": "主力净额(竞价/盘中)", "caliber": None, "unit": "元",
        "keys": ["main_net_inflow", "main_net", "mainNetInflow"],
        "sources": [("auction.jjyd.net_amount", "raw[4] 万->元"),
                    ("auction.jjyd.weimai", "raw[6]"),
                    ("pool.hot", "item10 主力")],
        "dedup_note": "竞价/盘中同时点; cashflow 主力净流入是盘后 EOD, 单独成指标避免跨时污染",
    },
    "seal_amount": {
        "desc": "封单额(竞价打板)", "caliber": None, "unit": "元",
        "keys": ["seal_amount", "seal_amount_wan", "sealAmount"],
        "sources": [("auction.jjyd.vratio", "raw[3] 万->元"),
                    ("auction.jjyd.weimai", "raw[17] 万->元")],
    },
    "concept": {
        "desc": "题材", "caliber": None, "unit": "text",
        "keys": ["concept"],
        "sources": [("auction.jjyd.vratio", "raw[7]"), ("auction.jjyd.qiangchou", "raw[7]"),
                    ("auction.jjyd.net_amount", "raw[7]"), ("auction.jjyd.weimai", "raw[11]"),
                    ("pool.hot", "item6"), ("pool.surge", "item6"),
                    ("auction.jjlive.fengdan", "tag_1"),
                    ("review.fupan.plate", "题材名称"),
                    ("home.ztpool", "题材"), ("review.ltgd.range", "概念")],
    },
    "board_label": {
        "desc": "连板/板态标签", "caliber": None, "unit": "text",
        "keys": ["board_label", "board_state", "boardLabel"],
        "sources": [("auction.jjyd.weimai", "raw[16]"),
                    ("pool.hot", "item7 板态(待回填)"), ("pool.surge", "item7 板态(待回填)"),
                    ("auction.jjlive.fengdan", "board_label")],
    },
    "price": {
        "desc": "股价", "caliber": None, "unit": "元",
        "keys": ["price"],
        "sources": [("auction.jjyd.weimai", "raw[2]"),
                    ("review.fupan.plate", "股价")],
    },
    # ==== Task 0117: 新登记表的指标 (key 已对齐 0116 canonical 字段名) ====
    # -- 人气热度 (量纲为人气, 非资金; premarket-live) --
    "hot_value": {
        "desc": "人气热度原值", "caliber": None, "unit": "热度",
        "keys": ["hot_value"],
        "sources": [("rank.rocket", "raw_rate 飙升榜热度"),
                    ("rank.hot_stock_day", "raw_rate 个股人气热度")],
        "dedup_note": "人气量纲; 绝不与任何资金/成交字段合并",
    },
    "hot_rank": {
        "desc": "人气榜排名", "caliber": None, "unit": "名",
        "keys": ["hot_rank"],
        "sources": [("rank.rocket", "rank"), ("rank.hot_stock_day", "rank")],
    },
    # -- 竞价封单阶梯 (委买/封单额, commit_bid; premarket-live) --
    "seal_bid_915": {
        "desc": "9:15 竞价委买/封单额", "caliber": "commit_bid", "unit": "元",
        "keys": ["seal_bid_915"],
        "sources": [("auction.jjlive.fengdan", "amount_915 委买/封单(非成交)")],
        "dedup_note": "commit_bid 口径; 绝不与成交额(turnover) 合并",
    },
    "seal_bid_920": {
        "desc": "9:20 竞价委买/封单额", "caliber": "commit_bid", "unit": "元",
        "keys": ["seal_bid_920"],
        "sources": [("auction.jjlive.fengdan", "amount_920 委买/封单(非成交)")],
        "dedup_note": "commit_bid 口径; 绝不与成交额合并",
    },
    "seal_bid_925": {
        "desc": "9:25 竞价委买/封单额", "caliber": "commit_bid", "unit": "元",
        "keys": ["seal_bid_925"],
        "sources": [("auction.jjlive.fengdan", "amount_925 委买/封单(非成交)")],
        "dedup_note": "commit_bid 口径; 绝不与成交额合并",
    },
    # -- 涨停晋级梯队 (intraday-eod) --
    "seal_status": {
        "desc": "封板状态 成/炸/败", "caliber": None, "unit": "text",
        "keys": ["zt_status"],
        "sources": [("home.ztpool", "状态 -> zt_status")],
    },
    "ladder_group": {
        "desc": "晋级梯队分组 3进4/2进3/1进2/首板", "caliber": None, "unit": "text",
        "keys": ["ladder_group"],
        "sources": [("home.ztpool", "分组名称 -> ladder_group")],
    },
    "promo_rate": {
        "desc": "梯队晋级率(组级, 每行携带)", "caliber": None, "unit": "%",
        "keys": ["promo_rate"],
        "sources": [("home.ztpool", "晋级率 -> promo_rate")],
    },
    # -- 涨停复盘 (postmarket-eod; 黄金锥点) --
    "streak": {
        "desc": "连板数", "caliber": None, "unit": "板",
        "keys": ["streak"],
        "sources": [("review.fupan.plate", "连板 -> streak")],
    },
    "open_num": {
        "desc": "开板次数(每股)", "caliber": None, "unit": "次",
        "keys": ["open_num"],
        "sources": [("review.fupan.plate", "开板 -> open_num (填补之前缺失的每股开板数)")],
    },
    "fupan_seal_amount": {
        "desc": "涨停复盘封单额(EOD)", "caliber": "commit_bid", "unit": "元",
        "keys": ["seal_amount"],
        "sources": [("review.fupan.plate", "封单额 -> seal_amount")],
        "dedup_note": "EOD 口径, 与竞价 seal_amount 分开(时段不同)",
    },
    "fupan_turnover_amount": {
        "desc": "涨停复盘成交额(EOD)", "caliber": None, "unit": "元",
        "keys": ["turnover_amount"],
        "sources": [("review.fupan.plate", "成交额 -> turnover_amount")],
    },
    # -- 资金流向 (postmarket-eod / eod_window) --
    "cashflow_main_net": {
        "desc": "资金流向主力净流入(当日)", "caliber": None, "unit": "元",
        "keys": ["main_net"],
        "sources": [("cashflow.stock.today", "主力净流入 -> main_net")],
    },
    "cashflow_main_net_3day": {
        "desc": "资金流向主力净流入(3日)", "caliber": None, "unit": "元",
        "keys": ["main_net"],
        "sources": [("cashflow.stock.3day", "主力净流入 3日")],
    },
    "cashflow_main_net_5day": {
        "desc": "资金流向主力净流入(5日)", "caliber": None, "unit": "元",
        "keys": ["main_net"],
        "sources": [("cashflow.stock.5day", "主力净流入 5日")],
    },
    "cashflow_main_net_10day": {
        "desc": "资金流向主力净流入(10日)", "caliber": None, "unit": "元",
        "keys": ["main_net"],
        "sources": [("cashflow.stock.10day", "主力净流入 10日")],
    },
    # -- 龙头高度区间涨幅 (postmarket-eod_window; 按 range_period 透视) --
    "interval_change": {
        "desc": "区间涨幅(龙头高度)", "caliber": None, "unit": "%",
        "keys": ["range_return"],
        "sources": [("review.ltgd.range", "区间涨幅 -> range_return")],
    },
    "interval_period": {
        "desc": "区间窗口 5/10/20/50日(透视键)", "caliber": None, "unit": "text",
        "keys": ["range_period"],
        "sources": [("review.ltgd.range", "周期 -> range_period")],
    },
}


def sources_for(indicator):
    return list(INDICATORS[indicator]["sources"])


def tables_for(indicator):
    return [ds for ds, _ in INDICATORS[indicator]["sources"]]


def field_to_tables_index():
    return {ind: tables_for(ind) for ind in INDICATORS}


def _self_test():
    # 危险重名隔离
    assert tables_for("volume_ratio") == ["auction.jjyd.vratio"], tables_for("volume_ratio")
    assert tables_for("grab_strength") == ["auction.jjyd.qiangchou"], tables_for("grab_strength")
    assert not (set(tables_for("volume_ratio")) & set(tables_for("grab_strength")))
    # 三口径市值不得混用
    for a, b in (("free_float_mktcap", "float_mktcap"), ("free_float_mktcap", "total_mktcap")):
        assert INDICATORS[a]["caliber"] != INDICATORS[b]["caliber"]
    # 每个 source 的 dataset 必须在 DATASETS 索引里 (防止孤儿表名)
    for ind, spec in INDICATORS.items():
        for ds, _prov in spec["sources"]:
            assert ds in DATASETS, f"{ind}: unknown dataset {ds}"
    # Task 0117: 10 张新表已翻 canonical=True + scope=STOCK
    _newly = ["auction.jjlive.fengdan", "home.ztpool", "review.fupan.plate",
              "review.ltgd.range", "cashflow.stock.today", "cashflow.stock.3day",
              "cashflow.stock.5day", "cashflow.stock.10day",
              "rank.rocket", "rank.hot_stock_day"]
    for ds in _newly:
        assert DATASETS[ds]["canonical"] is True, f"{ds} must be canonical"
        assert DATASETS[ds]["scope"] == STOCK, f"{ds} must be STOCK scope"
    # 3 张环境表保持非个股 (不入个股面板)
    for ds in ("home.kaipan.plate.summary", "home.qxlive.top_metrics", "review.daily.top_metrics"):
        assert DATASETS[ds]["scope"] == CONTEXT and DATASETS[ds]["canonical"] is False
    # index-only 指标的 key 已对齐 canonical 字段名 (不再是中文/旧名)
    assert INDICATORS["seal_status"]["keys"] == ["zt_status"]
    assert INDICATORS["cashflow_main_net"]["keys"] == ["main_net"]
    assert INDICATORS["interval_change"]["keys"] == ["range_return"]
    assert INDICATORS["seal_bid_920"]["keys"] == ["seal_bid_920"]
    assert INDICATORS["open_num"]["keys"] == ["open_num"]
    # total_mktcap 现有真源 (fupan)
    assert tables_for("total_mktcap") == ["review.fupan.plate"]
    # hot_value 仅来自人气榜, 不污染资金指标
    assert set(tables_for("hot_value")) == {"rank.rocket", "rank.hot_stock_day"}
    return True


_self_test()


# ===========================================================================
# RUNTIME: stock-centric panel assembler
# ===========================================================================
def _load_layer():
    imp = {}
    try:
        from duanxianxia_canonical import REGISTRY
        imp["REGISTRY"] = sorted(REGISTRY.keys())
    except Exception as e:  # noqa: BLE001
        REGISTRY = {}
        imp["REGISTRY_err"] = str(e)
    try:
        from duanxianxia_canonical_routing import canonicalize_row
    except Exception as e:  # noqa: BLE001
        canonicalize_row = None
        imp["routing_err"] = str(e)
    try:
        from duanxianxia_feature_builder import _pick_capture_file, _cutoff_seconds
    except Exception:  # noqa: BLE001
        _pick_capture_file = None
        _cutoff_seconds = None
    return REGISTRY, canonicalize_row, _pick_capture_file, _cutoff_seconds, imp


def _norm_code(c):
    s = "".join(ch for ch in str(c or "") if ch.isdigit())
    return s.zfill(6)[-6:] if s else ""


def _rows_of(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("rows", "items", "data", "list"):
            if isinstance(payload.get(k), list):
                return payload[k]
    return []


def _row_code(row):
    if isinstance(row, dict):
        return _norm_code(row.get("code") or row.get("\u4ee3\u7801") or (row.get("raw") or [None])[0])
    if isinstance(row, (list, tuple)) and row:
        return _norm_code(row[0])
    return ""


def _first_key(d, keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return None


def build_master_panel(date_dir, cutoff="09:29"):
    """从一个 capture 日目录装配个股宽表。返回 codes 并集 + panel + coverage + provenance + conflicts。"""
    date_dir = Path(date_dir)
    REGISTRY, canonicalize_row, _pick, _cutsec, imp = _load_layer()
    cutoff_secs = _cutsec(cutoff) if _cutsec else (9 * 3600 + 29 * 60)

    # 1) 加载每个可 canonical 的股级表 -> {code: canonical_dict}
    per_table = {}          # ds -> {code: canonical}
    load_report = {}
    for ds, meta in DATASETS.items():
        if meta["scope"] != STOCK:
            continue
        dd = date_dir / ds
        if not (meta["canonical"] and ds in REGISTRY and canonicalize_row and dd.is_dir()):
            load_report[ds] = {"loaded": False,
                               "reason": "unmapped" if not meta["canonical"] else
                                         ("not_in_registry" if ds not in REGISTRY else
                                          ("no_dir" if not dd.is_dir() else "no_router"))}
            continue
        files = sorted(dd.glob("*.json"))
        if not files:
            load_report[ds] = {"loaded": False, "reason": "empty"}
            continue
        payload = None
        if _pick:
            chosen, _m = _pick(dd, cutoff_secs)
            payload = chosen[1] if chosen else None
        if payload is None:
            payload = json.loads(files[-1].read_text(encoding="utf-8"))
        by_code, errs = {}, 0
        for row in _rows_of(payload):
            c = canonicalize_row(ds, row)
            if not isinstance(c, dict) or c.get("_canonical_error"):
                errs += 1
                continue
            code = _row_code(row) or _norm_code(c.get("code"))
            if code:
                by_code[code] = c
        per_table[ds] = by_code
        load_report[ds] = {"loaded": True, "codes": len(by_code), "canonical_err": errs}

    # 2) 代码并集 (跨表同一性)
    all_codes = set()
    for by_code in per_table.values():
        all_codes.update(by_code.keys())
    all_codes = sorted(all_codes)

    # 3) 逐股逐指标 resolve (优先级 + 别名 + 显式缺失 + 冲突检测)
    runnable = [ind for ind in INDICATORS
                if any(ds in per_table for ds in tables_for(ind))]
    panel = {}
    cov = {ind: 0 for ind in runnable}
    prov = {ind: {} for ind in runnable}
    conflicts = {ind: 0 for ind in runnable}
    for code in all_codes:
        row = {"code": code}
        for ind in runnable:
            spec = INDICATORS[ind]
            keys = spec["keys"]
            chosen_val, chosen_src = None, None
            seen_numeric = []
            for ds, _prov in spec["sources"]:
                cdict = per_table.get(ds, {}).get(code)
                if cdict is None:
                    continue
                v = _first_key(cdict, keys)
                if v is None:
                    continue
                if chosen_val is None:
                    chosen_val, chosen_src = v, ds
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    seen_numeric.append(float(v))
            row[ind] = chosen_val
            row[ind + "__src"] = chosen_src
            if chosen_val is not None:
                cov[ind] += 1
                prov[ind][chosen_src] = prov[ind].get(chosen_src, 0) + 1
            if len(seen_numeric) >= 2:
                lo, hi = min(seen_numeric), max(seen_numeric)
                if hi != 0 and (hi - lo) / abs(hi) > 0.01:
                    conflicts[ind] += 1
        name = None
        for ds in per_table:
            cd = per_table[ds].get(code)
            if cd and cd.get("name"):
                name = cd.get("name")
                break
        row["name"] = name
        panel[code] = row

    n = len(all_codes)
    summary = {
        "date_dir": str(date_dir),
        "n_codes": n,
        "imports": imp,
        "load_report": load_report,
        "runnable_indicators": runnable,
        "index_only_indicators": [i for i in INDICATORS if i not in runnable],
        "coverage_pct": {i: (round(100 * cov[i] / n, 1) if n else 0.0) for i in runnable},
        "provenance": prov,
        "conflicts": conflicts,
    }
    return {"summary": summary, "panel": panel}


if __name__ == "__main__":
    args = sys.argv[1:]
    cutoff = "09:29"
    cap = None
    i = 0
    while i < len(args):
        if args[i] == "--cutoff" and i + 1 < len(args):
            cutoff = args[i + 1]
            i += 2
            continue
        cap = args[i]
        i += 1
    cap_dir = Path(cap) if cap else (WS / "projects" / "duanxianxia" / "captures")
    if not cap_dir.is_absolute():
        cap_dir = WS / cap_dir

    DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    dates = sorted(p.name for p in cap_dir.iterdir()
                   if p.is_dir() and DATE_RE.match(p.name)) if cap_dir.is_dir() else []

    out = {
        "task": "0113_master_indicators",
        "captures_dir": str(cap_dir),
        "cutoff": cutoff,
        "n_datasets_indexed": len(DATASETS),
        "n_indicators": len(INDICATORS),
        "field_to_tables_index": field_to_tables_index(),
        "per_date": {},
    }
    for d in dates:
        try:
            res = build_master_panel(cap_dir / d, cutoff=cutoff)
            s = res["summary"]
            sample = list(res["panel"].values())[:2]
            s["sample_rows"] = sample
            out["per_date"][d] = s
        except Exception as e:  # noqa: BLE001
            out["per_date"][d] = {"error": f"{type(e).__name__}: {e}"}

    print("=== DUANXIANXIA MASTER INDICATOR INDEX + PANEL (Task 0113/0117) ===")
    print(f"datasets_indexed={len(DATASETS)} indicators={len(INDICATORS)} dates={len(dates)}")
    print(json.dumps(out, ensure_ascii=False, indent=1, default=str))
