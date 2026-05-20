# 092956_analysis_v7_3.json 全量候选摘要

- source_report: `092956_analysis_v7_3.json`
- version: `premarket_v7_3`
- date_t0: `2026-05-20`
- generated_at: `2026-05-20T09:29:56+08:00`
- candidate_count: `213`
- regime: `{'label': 'cold', 'reason': 'qx=27.0, dt=4.0, kqxy=0.0, breadth=0.21292460646230324', 'qx_t0': 27.0, 'qx_t1': 32.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 1028.0, 'xd_t0': 3800.0, 'breadth_t0': 0.21292460646230324, 'lbbx_t0': 3.41, 'lbbx_t1': 4.08, 'ztbx_t0': 1.65, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']}`
- regime reason: `qx=27.0, dt=4.0, kqxy=0.0, breadth=0.21292460646230324`

## action_stats

- `AUCTION_FOLLOW`: `1`
- `MOMENTUM_CATCHUP`: `3`
- `THEME_CATCHUP`: `5`
- `LOW_OPEN_REVERSAL`: `6`
- `BROAD_REPAIR_MOMENTUM`: `28`
- `BOARD_WATCH`: `9`
- `CONFIRMATION_WATCH`: `18`
- `FAKE_STRENGTH_WATCH`: `5`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `27`
- `AVOID`: `8`
- `DEBUG_ONLY`: `103`

## action_quality_stats

- `AUCTION_FOLLOW:main_attack`: `1`
- `MOMENTUM_CATCHUP:momentum`: `3`
- `THEME_CATCHUP:strong`: `2`
- `THEME_CATCHUP:weak`: `1`
- `THEME_CATCHUP:medium`: `2`
- `LOW_OPEN_REVERSAL:repair`: `6`
- `BROAD_REPAIR_MOMENTUM:broad_repair`: `28`
- `BOARD_WATCH:watch_only`: `9`
- `CONFIRMATION_WATCH:watch`: `16`
- `CONFIRMATION_WATCH:high_cost_watch`: `2`
- `FAKE_STRENGTH_WATCH:repair_watch`: `5`
- `SOFT_AVOID_REPAIR_CANDIDATE:soft_avoid`: `27`
- `AVOID:hard_avoid`: `8`
- `DEBUG_ONLY:debug`: `103`

## pool_performance

- `AUCTION_FOLLOW`: `{"count": 1, "with_performance": 0}`
- `MOMENTUM_CATCHUP`: `{"count": 3, "with_performance": 0}`
- `THEME_CATCHUP`: `{"count": 5, "with_performance": 0}`
- `LOW_OPEN_REVERSAL`: `{"count": 6, "with_performance": 0}`
- `BROAD_REPAIR_MOMENTUM`: `{"count": 28, "with_performance": 0}`
- `BOARD_WATCH`: `{"count": 9, "with_performance": 0}`
- `CONFIRMATION_WATCH`: `{"count": 18, "with_performance": 0}`
- `FAKE_STRENGTH_WATCH`: `{"count": 5, "with_performance": 0}`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `{"count": 27, "with_performance": 0}`
- `AVOID`: `{"count": 8, "with_performance": 0}`
- `DEBUG_ONLY`: `{"count": 103, "with_performance": 0}`

## review_diagnostics

- `missed_winners`: `0`
- `debug_missed_winners`: `0`
- `avoid_missed_winners`: `0`
- `soft_avoid_missed_winners`: `0`
- `fake_strength_watch_winners`: `0`
- `false_positives`: `0`
- `high_cost_confirmation_failures`: `0`
- `broad_repair_winners`: `0`
- `broad_repair_false_positives`: `0`
- `high_cost_repair_watch_winners`: `0`

## review_profiles

### missed_winners

- empty

### debug_missed_winners

- empty

### avoid_missed_winners

- empty

### soft_avoid_missed_winners

- empty

### fake_strength_watch_winners

- empty

### broad_repair_winners

- empty

### broad_repair_false_positives

- empty

### high_cost_repair_watch_winners

- empty

### false_positives

- empty

### high_cost_confirmation_failures

- empty

## candidate_pools counts

- `main_attack_pool`: 1
- `momentum_catchup_pool`: 3
- `theme_rotation_pool`: 1
- `theme_catchup_pool`: 5
- `low_open_reversal_pool`: 6
- `broad_repair_momentum_pool`: 15
- `board_watch_pool`: 9
- `confirmation_watch_pool`: 15
- `fake_strength_watch_pool`: 5
- `high_cost_repair_watch_pool`: 0
- `soft_avoid_repair_pool`: 15
- `avoid_or_risk_pool`: 8
- `debug_only_pool`: 15

## 绩效补充口径

- `auction_pct`: 竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return = close_pct - auction_pct`

## 收盘涨幅 / 超额收益（全量）

- `dailyline_matched`: `0 / 213`
- `avg_close_pct`: `None`
- `med_close_pct`: `None`
- `avg_excess_return`: `None`
- `med_excess_return`: `None`
- `pos_close_count`: `None/0`
- `pos_excess_count`: `None/0`

## 收盘涨幅 / 超额收益（Action Order Top30）

- `dailyline_matched`: `0 / 30`
- `avg_close_pct`: `None`
- `med_close_pct`: `None`
- `avg_excess_return`: `None`
- `med_excess_return`: `None`
- `pos_close_count`: `None/0`
- `pos_excess_count`: `None/0`

## 收盘涨幅 / 超额收益（Expected Return Proxy Top30）

- `dailyline_matched`: `0 / 30`
- `avg_close_pct`: `None`
- `med_close_pct`: `None`
- `avg_excess_return`: `None`
- `med_excess_return`: `None`
- `pos_close_count`: `None/0`
- `pos_excess_count`: `None/0`

## setup_v72 分布

- `T0-ROTATE`: `1`
- `T0-GENERAL`: `33`
- `T0-REVERSAL`: `6`
- `none`: `173`

## action_type 分布

- `AUCTION_FOLLOW`: `1`
- `MOMENTUM_CATCHUP`: `3`
- `THEME_CATCHUP`: `5`
- `LOW_OPEN_REVERSAL`: `6`
- `BROAD_REPAIR_MOMENTUM`: `28`
- `BOARD_WATCH`: `9`
- `CONFIRMATION_WATCH`: `18`
- `FAKE_STRENGTH_WATCH`: `5`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `27`
- `AVOID`: `8`
- `DEBUG_ONLY`: `103`

## action_quality 分布

- `main_attack`: `1`
- `momentum`: `3`
- `strong`: `2`
- `medium`: `2`
- `weak`: `1`
- `repair`: `6`
- `broad_repair`: `28`
- `watch_only`: `9`
- `watch`: `16`
- `high_cost_watch`: `2`
- `repair_watch`: `5`
- `soft_avoid`: `27`
- `hard_avoid`: `8`
- `debug`: `103`

## confidence 分布

- `high`: `1`
- `low`: `39`
- `none`: `173`

## auction_setup_type 分布

- `SUSTAINED_PLUS_LAST_SECOND`: `3`
- `GENERAL_WATCH`: `117`
- `LOW_OPEN_REVERSAL`: `6`
- `LOW_OPEN_WEAK`: `41`
- `BOARD_LOCK_WATCH`: `6`
- `FAKE_STRENGTH`: `40`

## Action Order Top30（交易动作顺序，不等于纯收益预测）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 000880 | 潍柴重机 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-ROTATE | high | 63.06 | 7.14 |  |  | main_attack_pool|theme_rotation_pool |
| 2 | 603108 | 润达医疗 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 48.48 | 4.64 |  |  | momentum_catchup_pool |
| 3 | 688449 | 联芸科技 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 37.4 | 5.65 |  |  | momentum_catchup_pool |
| 4 | 001896 | 豫能控股 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 39.77 | 2.29 |  |  | momentum_catchup_pool |
| 5 | 603986 | 兆易创新 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 43.75 | 0.85 |  |  | theme_catchup_pool |
| 6 | 002785 | 万里石 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 32.71 | 1.05 |  |  | theme_catchup_pool |
| 7 | 002030 | 达安基因 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.84 | 1.81 |  |  | theme_catchup_pool |
| 8 | 600785 | 新华百货 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 25.86 | 1.65 |  |  | theme_catchup_pool |
| 9 | 000026 | 飞亚达 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.9 | 1.13 |  |  | theme_catchup_pool |
| 10 | 600578 | 京能电力 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 41.44 | -3.02 |  |  | low_open_reversal_pool |
| 11 | 002208 | 合肥城建 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 36.94 | -4.34 |  |  | low_open_reversal_pool |
| 12 | 300302 | 同有科技 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 33.21 | -3.2 |  |  | low_open_reversal_pool |
| 13 | 600208 | 衢州发展 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 32.34 | -10.1 |  |  | low_open_reversal_pool |
| 14 | 605299 | 舒华体育 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 21.83 | -9.78 |  |  | low_open_reversal_pool |
| 15 | 002384 | 东山精密 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.73 | -0.89 |  |  | low_open_reversal_pool |
| 16 | 300166 | 东方国信 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.35 |  |  | broad_repair_momentum_pool |
| 17 | 688545 | 兴福电子 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.23 |  |  | broad_repair_momentum_pool |
| 18 | 301531 | 春光集团 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.5 |  |  | broad_repair_momentum_pool |
| 19 | 600563 | 法拉电子 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.0 |  |  | broad_repair_momentum_pool |
| 20 | 601678 | 滨化股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.32 |  |  | broad_repair_momentum_pool |
| 21 | 688361 | 中科飞测 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 4.31 |  |  | broad_repair_momentum_pool |
| 22 | 002273 | 水晶光电 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.38 |  |  | broad_repair_momentum_pool |
| 23 | 688072 | 拓荆科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.72 |  |  | broad_repair_momentum_pool |
| 24 | 688808 | 联讯仪器 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.07 |  |  | broad_repair_momentum_pool |
| 25 | 688521 | 芯原股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.64 |  |  | broad_repair_momentum_pool |
| 26 | 688256 | 寒武纪 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.46 |  |  | broad_repair_momentum_pool |
| 27 | 000938 | 紫光股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.62 |  |  | broad_repair_momentum_pool |
| 28 | 300394 | 天孚通信 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.75 |  |  | broad_repair_momentum_pool |
| 29 | 002466 | 天齐锂业 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.23 |  |  | broad_repair_momentum_pool |
| 30 | 601991 | 大唐发电 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -9.79 |  |  | broad_repair_momentum_pool |

## Expected Return Proxy Top30（盘前可见字段的收益预期展示）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 603108 | 润达医疗 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 48.48 | 4.64 |  |  | momentum_catchup_pool |
| 2 | 688449 | 联芸科技 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 37.4 | 5.65 |  |  | momentum_catchup_pool |
| 3 | 001896 | 豫能控股 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 39.77 | 2.29 |  |  | momentum_catchup_pool |
| 4 | 600578 | 京能电力 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 41.44 | -3.02 |  |  | low_open_reversal_pool |
| 5 | 600208 | 衢州发展 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 32.34 | -10.1 |  |  | low_open_reversal_pool |
| 6 | 300302 | 同有科技 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 33.21 | -3.2 |  |  | low_open_reversal_pool |
| 7 | 002208 | 合肥城建 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 36.94 | -4.34 |  |  | low_open_reversal_pool |
| 8 | 605299 | 舒华体育 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 21.83 | -9.78 |  |  | low_open_reversal_pool |
| 9 | 002384 | 东山精密 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.73 | -0.89 |  |  | low_open_reversal_pool |
| 10 | 601991 | 大唐发电 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -9.79 |  |  | broad_repair_momentum_pool |
| 11 | 300166 | 东方国信 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.35 |  |  | broad_repair_momentum_pool |
| 12 | 603986 | 兆易创新 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 43.75 | 0.85 |  |  | theme_catchup_pool |
| 13 | 688361 | 中科飞测 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 4.31 |  |  | broad_repair_momentum_pool |
| 14 | 688072 | 拓荆科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.72 |  |  | broad_repair_momentum_pool |
| 15 | 688545 | 兴福电子 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.23 |  |  | broad_repair_momentum_pool |
| 16 | 600563 | 法拉电子 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.0 |  |  | broad_repair_momentum_pool |
| 17 | 301531 | 春光集团 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.5 |  |  | broad_repair_momentum_pool |
| 18 | 000880 | 潍柴重机 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-ROTATE | high | 63.06 | 7.14 |  |  | main_attack_pool|theme_rotation_pool |
| 19 | 688808 | 联讯仪器 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.07 |  |  | broad_repair_momentum_pool |
| 20 | 688256 | 寒武纪 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.46 |  |  | broad_repair_momentum_pool |
| 21 | 688521 | 芯原股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.64 |  |  | broad_repair_momentum_pool |
| 22 | 000938 | 紫光股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.62 |  |  | broad_repair_momentum_pool |
| 23 | 300394 | 天孚通信 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.75 |  |  | broad_repair_momentum_pool |
| 24 | 002273 | 水晶光电 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.38 |  |  | broad_repair_momentum_pool |
| 25 | 002466 | 天齐锂业 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.23 |  |  | broad_repair_momentum_pool |
| 26 | 601678 | 滨化股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.32 |  |  | broad_repair_momentum_pool |
| 27 | 002709 | 天赐材料 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.81 |  |  |  |
| 28 | 002918 | 蒙娜丽莎 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 3.64 |  |  |  |
| 29 | 600111 | 北方稀土 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.5 |  |  |  |
| 30 | 688233 | 神工股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.25 |  |  |  |

