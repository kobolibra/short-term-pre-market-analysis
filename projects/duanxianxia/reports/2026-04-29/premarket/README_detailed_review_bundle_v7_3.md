# 2026-04-29 premarket detailed review bundle (v7.3)

本目录基于最新代码生成的 v7.3 报告：`120615_analysis_v7_3.json`。

## 关键点

- 使用本地已有 `2026-04-29` 盘前 captures 重跑，未重复下载盘前数据。
- 本次源报告已是 `premarket_v7_3`。
- 附带补充了当日绩效字段：`auction_pct` / `open_pct` / `close_pct` / `excess_return`。
- 附带补充了动作分层与质量字段：`action_type` / `action_quality` / `action_reason` / `action_score` / `action_priority` / `action_confidence` / `action_tags`。
- 单独输出了 `candidate_pools` 分池明细文件。

## v7.3 关键结构

- `action_stats`
- `action_quality_stats`
- `pool_performance`
- `review_diagnostics`
- `candidate_pools.momentum_catchup_pool`
- `candidate_pools.debug_only_pool`

## 文件说明

- `120615_analysis_v7_3.json`：原始 v7.3 报告
- `120615_all_candidates_flat.csv`：全量扁平化 CSV，已补充绩效字段与动作字段
- `120615_all_candidates_flat.jsonl`：全量扁平化 JSONL，已补充绩效字段与动作字段
- `120615_analysis_summary.md`：摘要，包括 action stats、quality stats、pool performance、diagnostics 与 Top30 表
- `120615_analysis_field_catalog.md`：字段说明
- `120615_all_candidates_ranked_list.md`：全量排序清单
- `120615_candidate_pools_detail.md`：分池明细

## 绩效字段口径

- `auction_pct`: 竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return`: `close_pct - auction_pct`
