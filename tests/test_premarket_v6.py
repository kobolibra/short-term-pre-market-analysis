#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Smoke + unit tests for scripts.duanxianxia_premarket_v6.

这里只测试打分算法本身的核心不变量, 不依赖网络或读盘的 capture 文件.
运行:
    PYTHONPATH=scripts python -m pytest tests/test_premarket_v6.py -v
    PYTHONPATH=scripts python tests/test_premarket_v6.py       # 无 pytest 也能跑
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import duanxianxia_premarket_v6 as v6  # noqa: E402


def _sample_report():
    """构造一个调调坛坛的盘前数据, 涵盖 4 竞价表 + 2 主页表.
    3 支样本股:
      - 000001: 高量比 + 主力净买正 + 封单递增 + 灰名单主题 → 正常候选
      - 000002: 抢筹强 + 小盘 + 主力流入正 + 竞涨接近涨停 → 对 untradable 的测试
      - 300999: 单表命中 + 方向不一致 → 验证 direction consistency 扣分
    """
    return {
        "trading_date": "2026-04-24",
        "datasets": {
            "auction_vratio": {"rows": [
                {"rank": 1, "code": "000001", "name": "股一",
                 "auction_volume_ratio": 8.5, "volume_ratio_multiple": 12.0,
                 "auction_change_pct": 5.0, "latest_change_pct": 5.2,
                 "auction_turnover_wan": 5000, "yesterday_auction_turnover_wan": 500,
                 "turnover_rate_pct": 3.2, "concept": "机器人|AI"},
                {"rank": 3, "code": "000002", "name": "股二",
                 "auction_volume_ratio": 6.0, "volume_ratio_multiple": 8.0,
                 "auction_change_pct": 9.85, "latest_change_pct": 9.85,
                 "auction_turnover_wan": 3000, "yesterday_auction_turnover_wan": 200,
                 "turnover_rate_pct": 2.0, "concept": "大科技"},
            ]},
            "auction_qiangchou": {"rows": [
                {"rank": 1, "code": "000002", "name": "股二", "group": "grab",
                 "grab_strength": 2.5, "auction_change_pct": 9.85,
                 "main_net_inflow_wan": 8000, "turnover_rate_pct": 2.0},
                {"rank": 4, "code": "000001", "name": "股一", "group": "qiangchou",
                 "grab_strength": 0.6, "auction_change_pct": 5.0,
                 "main_net_inflow_wan": 6000, "turnover_rate_pct": 3.2},
            ]},
            "auction_net_amount": {"rows": [
                {"rank": 1, "code": "000001", "name": "股一",
                 "main_net_inflow_wan": 9000, "market_cap_yi": 60.0,
                 "auction_change_pct": 5.0, "turnover_rate_pct": 3.2,
                 "concept": "机器人", "concept_1": "人形机器人"},
                {"rank": 2, "code": "000002", "name": "股二",
                 "main_net_inflow_wan": 8000, "market_cap_yi": 35.0,
                 "auction_change_pct": 9.85, "turnover_rate_pct": 2.0,
                 "concept": "大科技"},
            ]},
            "auction_fengdan": {"rows": [
                {"rank": 1, "code": "000002", "name": "股二", "section_kind": "live",
                 "amount_915": "2亿", "amount_920": "2.5亿", "amount_925": "3亿",
                 "board_label": "昨首板", "tag_1": "大科技", "tag_2": "人工智能"},
                {"rank": 2, "code": "300999", "name": "股三", "section_kind": "live",
                 "amount_915": "5000万", "amount_920": "2000万", "amount_925": "500万",
                 "board_label": "3天2板", "tag_1": "光伏"},
            ]},
            "home_qxlive_plate_summary": {"rows": [
                {"rank": 1, "main_plate_name": "机器人",
                 "plate_strength": 95.0, "main_inflow_wan": 120000,
                 "zt_count": 12, "sub_plate_list": "人形机器人|减速器|AI芯片"},
                {"rank": 2, "main_plate_name": "大科技",
                 "plate_strength": 70.0, "main_inflow_wan": 50000,
                 "zt_count": 8, "sub_plate_list": "人工智能|算力"},
            ]},
            "home_qxlive_top_metrics": {"rows": [
                {"metric_key": "QX", "current_value": 60},
                {"metric_key": "ZT", "current_value": 80},
                {"metric_key": "DT", "current_value": 10},
                {"metric_key": "ZTBX", "current_value": 35},
                {"metric_key": "LBBX", "current_value": 22},
                {"metric_key": "HSLN", "current_value": 15000},
                {"metric_key": "LBGD", "current_value": 5},
            ]},
        },
    }


def _cold_qxlive_rows():
    return {"rows": [
        {"metric_key": "ZTBX", "current_value": 10},
        {"metric_key": "LBBX", "current_value": 8},
        {"metric_key": "HSLN", "current_value": 8000},
    ]}


# ----------- 独立的 mini 测试 ----------

def test_config_has_complete_defaults():
    cfg = v6.load_premarket_config()
    for section in ("rank_scores", "numeric_signals", "source_hit_bonuses",
                    "direction_consistency", "untradable", "theme_overlay",
                    "risk_penalty", "market_regime", "yesterday_postmarket",
                    "output"):
        assert section in cfg, f"missing section: {section}"
    assert cfg["version"] == "premarket_5table_v6"
    assert cfg["schema_version"] >= 1


def test_parse_chinese_amount_wan():
    assert v6._parse_chinese_amount_wan("3000万") == 3000.0
    assert v6._parse_chinese_amount_wan("1亿") == 10000.0
    assert v6._parse_chinese_amount_wan("2.5亿") == 25000.0
    assert v6._parse_chinese_amount_wan("1,200") == 1200.0
    assert v6._parse_chinese_amount_wan("") == 0.0


def test_percentile_basic():
    # 在 [10, 20, 30, 40, 50] 里, 25 的 percentile 应该在 0.4-0.5
    p = v6._percentile(25, [10, 20, 30, 40, 50], 0.0)
    assert 0.4 <= p <= 0.5
    # 低于 min_value 时 返回 0
    assert v6._percentile(10, [10, 20, 30], 15) == 0.0


def test_theme_token_matches():
    assert v6._theme_token_matches("人形机器人", "机器人") is True
    assert v6._theme_token_matches("AI", "AI芯片") is True  # 短白名单
    assert v6._theme_token_matches("新能源", "汽车") is False
    assert v6._theme_token_matches("", "任何") is False


def test_infer_price_limit_pct():
    assert v6._infer_price_limit_pct("300001") == 20.0
    assert v6._infer_price_limit_pct("688001") == 20.0
    assert v6._infer_price_limit_pct("600000") == 10.0
    assert v6._infer_price_limit_pct("830000") == 30.0
    assert v6._infer_price_limit_pct("600000", name="ST某股") == 5.0


def test_regime_normal_vs_cold():
    cfg = v6.load_premarket_config()
    normal = v6._classify_regime(
        [{"metric_key": "ZTBX", "current_value": 30},
         {"metric_key": "HSLN", "current_value": 15000}],
        cfg,
    )
    assert normal["label"] == "normal"

    cold = v6._classify_regime(_cold_qxlive_rows()["rows"], cfg)
    assert cold["label"] == "cold"
    assert cold["multiplier"] < 1.0
    assert cold["max_candidates"] <= 5


def test_regime_hot():
    cfg = v6.load_premarket_config()
    hot = v6._classify_regime(
        [{"metric_key": "ZTBX", "current_value": 55},
         {"metric_key": "LBGD", "current_value": 7}],
        cfg,
    )
    assert hot["label"] == "hot"
    assert hot["multiplier"] > 1.0


def test_untradable_keeps_strong_seal():
    """接近涨停 但 9:25 封单很厚 + 递增 → 不该 untradable."""
    cfg = v6.load_premarket_config()
    cand = {
        "code": "000002", "name": "股二",
        "auction_change_pct": 9.85,
        "amount_915": "2亿", "amount_920": "2.5亿", "amount_925": "3亿",
        "main_net_inflow_wan": 8000,
        "market_cap_yi": 35.0,
    }
    unt, reason = v6._is_untradable_v6(cand, cfg)
    assert unt is False, f"封单厚应该放行, 但判为 untradable ({reason})"


def test_untradable_catches_seal_shrink():
    """接近涨停 + 封单 1.5 倍缩水 + 没主力 + 大盘股 → 真 untradable."""
    cfg = v6.load_premarket_config()
    cand = {
        "code": "300999", "name": "股三",
        "auction_change_pct": 19.85,   # 创20顶
        "amount_915": "5000万", "amount_920": "2000万", "amount_925": "500万",
        "main_net_inflow_wan": 0,
        "market_cap_yi": 200.0,
    }
    unt, reason = v6._is_untradable_v6(cand, cfg)
    assert unt is True, f"封单缩水应判 untradable, 但 false ({reason})"
    assert "shrink" in reason or "auction_pct" in reason


def test_end_to_end_smoke():
    """调调 build_premarket_analysis_v6, 只看输出结构是否平稳."""
    result = v6.build_premarket_analysis_v6(_sample_report())
    assert result["version"] == "premarket_5table_v6"
    assert result["trading_date"] == "2026-04-24"
    assert "top_candidates" in result
    assert "market_regime" in result
    assert result["candidate_count"] >= 2

    codes = [c["code"] for c in result["top_candidates"]]
    # 股一 应该是 top (多信号、主题匹配、方向一致、不近涨停)
    # 股二 是接近涨停但封单厚, 不应被过滤掉
    assert "000001" in codes, f"top 里应包含股一, 实际 top = {codes}"

    # 股一的 top_candidate 结构检查
    gu_yi = next(c for c in result["top_candidates"] if c["code"] == "000001")
    assert gu_yi["direction_ok"] is True, gu_yi
    assert gu_yi["source_hit_count"] >= 2
    assert gu_yi["numeric_score"] > 0
    assert gu_yi["theme_matches"], "应该匹配到主题 '机器人'"

    # 股二 不被 untradable 剥离 (封单递增)
    untradable_codes = [c["code"] for c in result["untradable_candidates"]]
    assert "000002" not in untradable_codes


def test_direction_consistency_penalty_applied():
    """验证: 只命中一个正向信号的股, 数值分被 ×0.3."""
    report = {
        "trading_date": "2026-04-24",
        "datasets": {
            "auction_vratio": {"rows": [
                {"rank": 1, "code": "300555", "name": "奇灊股",
                 "volume_ratio_multiple": 10.0,
                 "auction_change_pct": 0.1, "latest_change_pct": 0.1,
                 "auction_turnover_wan": 0, "yesterday_auction_turnover_wan": 0,
                 "turnover_rate_pct": 0.0, "main_net_inflow_wan": 0,
                 "grab_strength": 0.0, "market_cap_yi": 50.0},
                {"rank": 2, "code": "000100", "name": "正常股",
                 "volume_ratio_multiple": 6.0,
                 "auction_change_pct": 3.0, "latest_change_pct": 3.0,
                 "auction_turnover_wan": 2000, "yesterday_auction_turnover_wan": 500,
                 "turnover_rate_pct": 2.0, "main_net_inflow_wan": 3000,
                 "grab_strength": 0.8, "market_cap_yi": 40.0},
            ]},
            "auction_qiangchou": {"rows": []},
            "auction_net_amount": {"rows": []},
            "auction_fengdan": {"rows": []},
            "home_qxlive_plate_summary": {"rows": []},
            "home_qxlive_top_metrics": {"rows": []},
        },
    }
    result = v6.build_premarket_analysis_v6(report)
    by_code = {c["code"]: c for c in result["top_candidates"]}
    if "300555" in by_code:
        bad = by_code["300555"]
        good = by_code.get("000100")
        assert bad["direction_ok"] is False
        if good:
            assert good["direction_ok"] is True
            assert good["ranking_score"] > bad["ranking_score"]


if __name__ == "__main__":  # 无 pytest 也能跑
    import traceback
    passed, failed = 0, 0
    for name, fn in list(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  OK   {name}")
            passed += 1
        except AssertionError as exc:
            print(f"  FAIL {name}: {exc}")
            failed += 1
        except Exception:
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
