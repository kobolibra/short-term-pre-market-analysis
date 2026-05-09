# 201743_analysis_v7_2.json 字段字典与复盘说明

- 原始报告: `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/reports/2026-04-29/premarket/201743_analysis_v7_2.json`
- 候选总数: `308`
- 导出 CSV: `201743_all_candidates_flat.csv`
- 导出 JSONL: `201743_all_candidates_flat.jsonl`

## 顶层结构

- `all_candidates_debug`: list[308]
- `candidate_pools`: dict[5]
- `intraday_anchors`: list[20]
- `meta`: dict[9]
- `setup_stats`: dict[4]
- `top_candidates`: list[30]
- `version`: str
- `watch_tier`: list[50]

## 绩效补充口径

- `auction_pct`: 采用 `auction_detail.latest_change_pct` 作为竞价涨幅口径
- `open_pct`: `(open - prev_close) / prev_close * 100`
- `close_pct`: `(close - prev_close) / prev_close * 100`
- `excess_return`: 对齐 `scripts/duanxianxia_review_backfill.py` 的盘前口径，按 `close_pct - auction_pct` 计算
- 若缺失当日 dailyline 行或缺失竞价涨幅，则对应绩效字段为 `null`

## all_candidates_debug 主要字段说明

- `code`: 股票代码
- `name`: 股票名称
- `setup_v72`: v7.2 setup 分类
- `setup_v71_compat`: 兼容旧 v7.1 的 setup 映射
- `confidence`: 当前候选置信度
- `setup_reason`: setup 入选原因标签
- `auction_setup_type`: 竞价形态分类
- `regime`: 市场环境标签，本次为 cold
- `entry_tag`: 入场标签
- `entry_reason`: 入场标签原因
- `final_score`: 最终总分
- `today_signal_raw`: 当日原始信号分
- `auction_strength`: 竞价综合强度
- `theme_strength_t0`: T0 题材强度分位
- `hotness_score`: 热度分
- `t1_multiplier`: T-1 因子乘数
- `regime_multiplier`: 环境乘数
- `risk_penalty`: 风险惩罚乘数
- `risk_flag`: 是否打风险标
- `trade_date`: 绩效对应交易日
- `dailyline_found`: 是否找到当日日线行
- `prev_close/day_open/day_high/day_low/day_close`: 日线原始价格
- `auction_pct/open_pct/close_pct/excess_return`: 绩效补充列
- `score_weight_*`: 总分里各模块权重
- `theme_*`: 题材匹配细节：匹配题材、标签、板块强度、忽略字段等
- `auction_*`: 竞价细节：量比/抢筹/净额/封单/流动性/风险/命中族数等
- `signal_*`: 汇总后的信号摘要
- `label_*`: 派生标签快照：龙头、资金连续性、技术形态
- `stock_t1_*`: T-1 个股资金标签与主力净流入等
- `cashflow_raw_*`: 原始资金流窗口值
- `tech_raw_*`: 技术面中间量：MA20、量比、离高点距离、换手特征等
- `risk_*`: 风控字段：出流、流值口径、是否使用 T-1 review context
- `t1_adjustments_json`: T-1 修正项明细 JSON
- `*_json`: 保留完整嵌套原始 JSON，便于回溯

## 本次 meta / regime

```json
{
  "date_t0": "2026-04-29",
  "date_t1": "2026-04-28",
  "date_t2": "2026-04-27",
  "generated_at": "2026-05-08T20:17:43+08:00",
  "candidate_count": 308,
  "bundle_summary": {
    "date_t0": "2026-04-29",
    "date_t1": "2026-04-28",
    "date_t2": "2026-04-27",
    "project_root": "/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia",
    "counts": {
      "auction_vratio": 198,
      "auction_qiangchou": 41,
      "auction_netamount": 49,
      "auction_fengdan": 96,
      "kaipan_t1": 20,
      "cashflow_today_t1": 150,
      "cashflow_3day_t1": 150,
      "cashflow_5day_t1": 150,
      "cashflow_10day_t1": 150,
      "fupan_t1": 59,
      "ltgd_5day_t1": 20,
      "ztpool_t1": 123,
      "qxlive_top_t1": 12,
      "qxlive_top_t2": 0,
      "kaipan_history": 10
    },
    "kaipan_t1_meta_keys": [
      "complete",
      "count",
      "failed_items",
      "field",
      "missing_items",
      "money_value_semantics",
      "selected_top_plate",
      "source",
      "subplate_count",
      "subplates",
      "table_headers",
      "top_plate_count",
      "top_plate_summaries",
      "top_plates"
    ],
    "warnings": [],
    "rocket_rows": 100,
    "hot_stock_day_rows": 100,
    "kaipan_plate_t0_rows": 20,
    "qxlive_top_t0_rows": 12,
    "v72_warnings": []
  },
  "regime": {
    "label": "cold",
    "reason": "qx=20.0, dt=11.0, kqxy=0.0, breadth=0.27373612823674476",
    "qx_t0": 20.0,
    "qx_t1": 20.0,
    "dt_t0": 11.0,
    "kqxy_t0": 0.0,
    "sz_t0": 1332.0,
    "xd_t0": 3534.0,
    "breadth_t0": 0.27373612823674476,
    "lbbx_t0": 0.2,
    "lbbx_t1": 0.88,
    "ztbx_t0": 0.73,
    "promo_t0": null,
    "ignored_metrics": [
      "HSLN",
      "PB",
      "PBBX"
    ]
  },
  "warnings": [],
  "notes": [
    "v7.2 conservative mode: T0 auction + exact plate-tag strength + hotness dominate.",
    "T0 qxlive HSLN/PB/PBBX are ignored for premarket regime.",
    "T0 plate 主力流入 and 涨停数量 are ignored; only 板块强度 is used.",
    "T-1 review tables are not used as premarket scoring factors when use_t1_review_context=false."
  ]
}
```

## candidate_pools 尺寸
- `main_attack_pool`: 0
- `theme_rotation_pool`: 1
- `board_watch_pool`: 1
- `confirmation_watch_pool`: 15
- `avoid_or_risk_pool`: 15

## watch_tier / intraday_anchors

- `watch_tier`: 50 条
- `intraday_anchors`: 20 条
