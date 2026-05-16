# 2026-05-15 premarket detailed review bundle (v7.3, 165149)

本目录基于最新代码生成的 v7.3 报告：`165149_analysis_v7_3.json`。

## 关键点

- 使用本地已有 `2026-05-15` 盘前 captures 重跑，未重复下载盘前数据。
- 本次源报告已是 `premarket_v7_3`。
- bundle 生成时加载 `projects/duanxianxia/config/premarket_v7_3_setups.yaml`，避免 runner 与 backfill/review 分类逻辑漂移。
- 使用同日个股日线回填生成的 `165149_review_performance_flat.csv` 作为绩效来源，给 `165149` 报告重算了 `pool_performance` / `review_diagnostics` / `review_profiles`。
- 报告区分 `Action Order Top30` 与 `Expected Return Proxy Top30`。前者是交易动作顺序，后者是盘前可见字段的收益预期展示。
- 额外补充了直观人工查看文件：`165149_analysis_v7_3_top30_detailed.csv` / `165149_analysis_v7_3_top30_detailed.md`。

## v7.3 关键结构

- `action_stats`
- `action_quality_stats`
- `pool_performance`
- `review_diagnostics`
- `review_profiles`
- `expected_return_candidates`
- `candidate_pools.momentum_catchup_pool`
- `candidate_pools.debug_only_pool`
- `candidate_pools.fake_strength_watch_pool`
- `candidate_pools.soft_avoid_repair_pool`

## 文件说明

- `165149_analysis_v7_3.json`：原始 v7.3 报告（已重算 review metrics）
- `165149_review_performance_flat.csv`：本次 bundle 的绩效输入表（由同日个股日线回填生成）
- `165149_all_candidates_flat.csv`：全量扁平化 CSV，已补充绩效字段与动作字段
- `165149_all_candidates_flat.jsonl`：全量扁平化 JSONL，已补充绩效字段与动作字段
- `165149_analysis_summary.md`：摘要，包括 action stats、quality stats、pool performance、diagnostics、profiles、Action Top30、Expected Top30
- `165149_analysis_field_catalog.md`：字段说明
- `165149_all_candidates_ranked_list.md`：全量 action-order 排序清单
- `165149_all_candidates_expected_return_ranked_list.md`：全量 expected-return proxy 排序清单
- `165149_candidate_pools_detail.md`：分池明细
- `165149_analysis_v7_3_top30_detailed.csv`：面向人工快速浏览的 Top30 详细表
- `165149_analysis_v7_3_top30_detailed.md`：Top30 中文说明辅助表

## 绩效字段口径

- `auction_pct`：竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`
- `open_pct`：当日开盘相对昨收涨幅
- `close_pct`：当日收盘相对昨收涨幅
- `excess_return`：`close_pct - auction_pct`
