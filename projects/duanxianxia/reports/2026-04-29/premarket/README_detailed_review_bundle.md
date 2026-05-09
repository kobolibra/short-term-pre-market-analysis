# 2026-04-29 premarket detailed review bundle (new v7.2 action-pool output)

本目录基于最新代码生成的新版 v7.2 报告：`111108_analysis_v7_2.json`。

## 关键点

- 使用本地已有 `2026-04-29` 盘前 captures 重跑，未重复下载盘前数据。
- 本次源报告已是新版 v7.2 action-pool 结构。
- 附带补充了当日绩效字段：`auction_pct` / `open_pct` / `close_pct` / `excess_return`。
- 附带补充了新版动作分层字段：`action_type` / `action_reason` / `action_score` / `action_priority` / `action_confidence` / `action_tags`。

## 新版 v7.2 新增结构

- `action_stats`
- `actionable_candidates`
- `legacy_top_candidates`
- `candidate_pools.theme_catchup_pool`
- `candidate_pools.low_open_reversal_pool`
- `candidate_pools.board_watch_pool`

## 文件说明

- `111108_analysis_v7_2.json`：原始新版 v7.2 action-pool 报告
- `111108_all_candidates_flat.csv`：全量扁平化 CSV，已补充绩效字段与动作字段
- `111108_all_candidates_flat.jsonl`：全量扁平化 JSONL，已补充绩效字段与动作字段
- `111108_analysis_summary.md`：摘要，包括 action_stats、池子统计与 Top30 表
- `111108_analysis_field_catalog.md`：字段说明
- `111108_all_candidates_ranked_list.md`：全量排序清单

## 绩效字段口径

- `auction_pct`: 竞价涨幅，采用 `auction_detail.latest_change_pct`
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return`: `close_pct - auction_pct`
