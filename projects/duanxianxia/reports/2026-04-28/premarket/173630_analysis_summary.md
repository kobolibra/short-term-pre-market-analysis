# 173630_analysis_v7_3.json 全量候选摘要

- source_report: `173630_analysis_v7_3.json`
- version: `premarket_v7_3`
- date_t0: `2026-04-28`
- generated_at: `2026-05-10T17:36:30+08:00`
- candidate_count: `105`
- regime: `{'label': 'cold', 'reason': 'qx=20.0, dt=13.0, kqxy=0.0, breadth=0.2677213889459626', 'qx_t0': 20.0, 'qx_t1': None, 'dt_t0': 13.0, 'kqxy_t0': 0.0, 'sz_t0': 1303.0, 'xd_t0': 3564.0, 'breadth_t0': 0.2677213889459626, 'lbbx_t0': 0.88, 'lbbx_t1': None, 'ztbx_t0': 1.57, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']}`
- regime reason: `qx=20.0, dt=13.0, kqxy=0.0, breadth=0.2677213889459626`

## action_stats

- `MOMENTUM_CATCHUP`: `1`
- `THEME_CATCHUP`: `5`
- `LOW_OPEN_REVERSAL`: `5`
- `BOARD_WATCH`: `3`
- `CONFIRMATION_WATCH`: `3`
- `FAKE_STRENGTH_WATCH`: `1`
- `HIGH_COST_REPAIR_WATCH`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `34`
- `AVOID`: `12`
- `DEBUG_ONLY`: `40`

## action_quality_stats

- `MOMENTUM_CATCHUP:momentum`: `1`
- `THEME_CATCHUP:strong`: `2`
- `THEME_CATCHUP:weak`: `3`
- `LOW_OPEN_REVERSAL:repair`: `5`
- `BOARD_WATCH:watch_only`: `3`
- `CONFIRMATION_WATCH:watch`: `1`
- `CONFIRMATION_WATCH:high_cost_watch`: `2`
- `FAKE_STRENGTH_WATCH:repair_watch`: `1`
- `HIGH_COST_REPAIR_WATCH:high_cost_repair_watch`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE:soft_avoid`: `34`
- `AVOID:hard_avoid`: `12`
- `DEBUG_ONLY:debug`: `40`

## pool_performance

- `MOMENTUM_CATCHUP`: `{"count": 1, "with_performance": 1, "avg_close_pct": 9.98, "med_close_pct": 9.98, "avg_excess_return": 6.23, "med_excess_return": 6.23, "positive_excess_count": 1, "negative_excess_count": 0}`
- `THEME_CATCHUP`: `{"count": 5, "with_performance": 5, "avg_close_pct": 2.13, "med_close_pct": 2.34, "avg_excess_return": 1.47, "med_excess_return": 1.3, "positive_excess_count": 5, "negative_excess_count": 0}`
- `LOW_OPEN_REVERSAL`: `{"count": 5, "with_performance": 5, "avg_close_pct": 3.07, "med_close_pct": 6.32, "avg_excess_return": 6.46, "med_excess_return": 7.15, "positive_excess_count": 4, "negative_excess_count": 1}`
- `BOARD_WATCH`: `{"count": 3, "with_performance": 3, "avg_close_pct": 13.34, "med_close_pct": 10.02, "avg_excess_return": 0.58, "med_excess_return": 0.0, "positive_excess_count": 1, "negative_excess_count": 0}`
- `CONFIRMATION_WATCH`: `{"count": 3, "with_performance": 3, "avg_close_pct": 7.81, "med_close_pct": 9.85, "avg_excess_return": 2.18, "med_excess_return": 2.55, "positive_excess_count": 2, "negative_excess_count": 1}`
- `FAKE_STRENGTH_WATCH`: `{"count": 1, "with_performance": 1, "avg_close_pct": -0.71, "med_close_pct": -0.71, "avg_excess_return": -2.93, "med_excess_return": -2.93, "positive_excess_count": 0, "negative_excess_count": 1}`
- `HIGH_COST_REPAIR_WATCH`: `{"count": 1, "with_performance": 1, "avg_close_pct": 10.0, "med_close_pct": 10.0, "avg_excess_return": 0.07, "med_excess_return": 0.07, "positive_excess_count": 1, "negative_excess_count": 0}`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `{"count": 34, "with_performance": 28, "avg_close_pct": 1.49, "med_close_pct": 0.59, "avg_excess_return": 0.3, "med_excess_return": 0.66, "positive_excess_count": 15, "negative_excess_count": 13}`
- `AVOID`: `{"count": 12, "with_performance": 11, "avg_close_pct": 7.93, "med_close_pct": 10.0, "avg_excess_return": -1.06, "med_excess_return": 0.0, "positive_excess_count": 4, "negative_excess_count": 5}`
- `DEBUG_ONLY`: `{"count": 40, "with_performance": 25, "avg_close_pct": 0.05, "med_close_pct": -1.57, "avg_excess_return": -0.21, "med_excess_return": -1.22, "positive_excess_count": 8, "negative_excess_count": 16}`

## review_diagnostics

- `missed_winners`: `6`
- `debug_missed_winners`: `3`
- `avoid_missed_winners`: `0`
- `soft_avoid_missed_winners`: `2`
- `fake_strength_watch_winners`: `0`
- `false_positives`: `0`
- `high_cost_confirmation_failures`: `0`
- `broad_repair_winners`: `0`
- `broad_repair_false_positives`: `0`
- `high_cost_repair_watch_winners`: `0`

## review_profiles

### missed_winners

- `count`: `6`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 3], ["FAKE_STRENGTH", 2], ["LOW_OPEN_WEAK", 1]]`
- `action_type_top`: `[["DEBUG_ONLY", 3], ["SOFT_AVOID_REPAIR_CANDIDATE", 2], ["CONFIRMATION_WATCH", 1]]`
- `action_quality_top`: `[["debug", 3], ["soft_avoid", 2], ["high_cost_watch", 1]]`
- `setup_v72_top`: `[["none", 5], ["T0-GENERAL", 1]]`
- `confidence_top`: `[["none", 5], ["low", 1]]`
- `entry_tag_top`: `[["normal", 4], ["avoid", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 3], ["[-2,0)", 1], ["[7,9)", 1], ["[2,5)", 1]]`
- `auction_amount_bucket`: `[["missing", 6]]`
- `numeric_stats`: `{"auction_pct": {"count": 6, "min": -0.25, "p25": 1.49, "median": 1.64, "p75": 2.03, "max": 8.23, "avg": 2.46}, "auction_strength": {"count": 6, "min": 3.58, "p25": 6.83, "median": 7.88, "p75": 8.88, "max": 10.88, "avg": 7.66}, "auction_amount_wan": {"count": 0}, "liquidity_score": {"count": 6, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "theme_strength_t0": {"count": 6, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 95.0, "avg": 32.5}, "source_evidence_score": {"count": 6, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 6, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 6, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 27.95, "avg": 4.66}, "expected_return_score": {"count": 6, "min": -68.16, "p25": -67.8, "median": -44.28, "p75": -20.17, "max": -6.65, "avg": -41.89}, "action_score": {"count": 6, "min": 0.0, "p25": 0.0, "median": 2.92, "p75": 6.32, "max": 6.97, "avg": 3.19}}`
- `top_names`: `["002989 中天精装", "688530 欧莱新材", "601177 杭齿前进", "603931 格林达", "603336 宏辉果蔬", "600152 维科技术"]`

### debug_missed_winners

- `count`: `3`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 2], ["LOW_OPEN_WEAK", 1]]`
- `action_type_top`: `[["DEBUG_ONLY", 3]]`
- `action_quality_top`: `[["debug", 3]]`
- `setup_v72_top`: `[["none", 3]]`
- `confidence_top`: `[["none", 3]]`
- `entry_tag_top`: `[["normal", 3]]`
- `auction_pct_bucket`: `[["[0,2)", 2], ["[-2,0)", 1]]`
- `auction_amount_bucket`: `[["missing", 3]]`
- `numeric_stats`: `{"auction_pct": {"count": 3, "min": -0.25, "p25": -0.25, "median": 1.49, "p75": 1.74, "max": 1.74, "avg": 0.99}, "auction_strength": {"count": 3, "min": 6.88, "p25": 6.88, "median": 8.88, "p75": 8.88, "max": 8.88, "avg": 8.21}, "auction_amount_wan": {"count": 0}, "liquidity_score": {"count": 3, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "theme_strength_t0": {"count": 3, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 3, "min": -68.16, "p25": -68.16, "median": -67.8, "p75": -67.8, "max": -67.8, "avg": -67.92}, "action_score": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}}`
- `top_names`: `["002989 中天精装", "601177 杭齿前进", "603336 宏辉果蔬"]`

### avoid_missed_winners

- empty

### soft_avoid_missed_winners

- `count`: `2`
- `auction_setup_type_top`: `[["FAKE_STRENGTH", 2]]`
- `action_type_top`: `[["SOFT_AVOID_REPAIR_CANDIDATE", 2]]`
- `action_quality_top`: `[["soft_avoid", 2]]`
- `setup_v72_top`: `[["none", 2]]`
- `confidence_top`: `[["none", 2]]`
- `entry_tag_top`: `[["avoid", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 1], ["[2,5)", 1]]`
- `auction_amount_bucket`: `[["missing", 2]]`
- `numeric_stats`: `{"auction_pct": {"count": 2, "min": 1.54, "p25": 1.54, "median": 1.78, "p75": 2.03, "max": 2.03, "avg": 1.78}, "auction_strength": {"count": 2, "min": 3.58, "p25": 3.58, "median": 5.21, "p75": 6.83, "max": 6.83, "avg": 5.21}, "auction_amount_wan": {"count": 0}, "liquidity_score": {"count": 2, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "theme_strength_t0": {"count": 2, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 2, "min": -20.76, "p25": -20.76, "median": -20.47, "p75": -20.17, "max": -20.17, "avg": -20.47}, "action_score": {"count": 2, "min": 6.32, "p25": 6.32, "median": 6.64, "p75": 6.97, "max": 6.97, "avg": 6.64}}`
- `top_names`: `["603931 格林达", "600152 维科技术"]`

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

- `main_attack_pool`: 0
- `momentum_catchup_pool`: 1
- `theme_rotation_pool`: 0
- `theme_catchup_pool`: 5
- `low_open_reversal_pool`: 5
- `broad_repair_momentum_pool`: 0
- `board_watch_pool`: 3
- `confirmation_watch_pool`: 3
- `fake_strength_watch_pool`: 1
- `high_cost_repair_watch_pool`: 1
- `soft_avoid_repair_pool`: 15
- `avoid_or_risk_pool`: 12
- `debug_only_pool`: 15

## 绩效补充口径

- `auction_pct`: 竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return = close_pct - auction_pct`

## 收盘涨幅 / 超额收益（全量）

- `dailyline_matched`: `83 / 105`
- `avg_close_pct`: `2.88`
- `med_close_pct`: `1.99`
- `avg_excess_return`: `0.51`
- `med_excess_return`: `0.0`
- `pos_close_count`: `50/83`
- `pos_excess_count`: `41/83`

## 收盘涨幅 / 超额收益（Action Order Top30）

- `dailyline_matched`: `14 / 14`
- `avg_close_pct`: `5.43`
- `med_close_pct`: `5.54`
- `avg_excess_return`: `3.4`
- `med_excess_return`: `1.52`
- `pos_close_count`: `11/14`
- `pos_excess_count`: `11/14`

## 收盘涨幅 / 超额收益（Expected Return Proxy Top30）

- `dailyline_matched`: `14 / 14`
- `avg_close_pct`: `5.43`
- `med_close_pct`: `5.54`
- `avg_excess_return`: `3.4`
- `med_excess_return`: `1.52`
- `pos_close_count`: `11/14`
- `pos_excess_count`: `11/14`

## setup_v72 分布

- `T0-GENERAL`: `9`
- `T0-REVERSAL`: `5`
- `none`: `91`

## action_type 分布

- `MOMENTUM_CATCHUP`: `1`
- `THEME_CATCHUP`: `5`
- `LOW_OPEN_REVERSAL`: `5`
- `BOARD_WATCH`: `3`
- `CONFIRMATION_WATCH`: `3`
- `FAKE_STRENGTH_WATCH`: `1`
- `HIGH_COST_REPAIR_WATCH`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `34`
- `AVOID`: `12`
- `DEBUG_ONLY`: `40`

## action_quality 分布

- `momentum`: `1`
- `strong`: `2`
- `weak`: `3`
- `repair`: `5`
- `watch_only`: `3`
- `watch`: `1`
- `high_cost_watch`: `2`
- `repair_watch`: `1`
- `high_cost_repair_watch`: `1`
- `soft_avoid`: `34`
- `hard_avoid`: `12`
- `debug`: `40`

## confidence 分布

- `low`: `14`
- `none`: `91`

## auction_setup_type 分布

- `GENERAL_WATCH`: `31`
- `LOW_OPEN_WEAK`: `19`
- `LOW_OPEN_REVERSAL`: `5`
- `BOARD_LOCK_WATCH`: `2`
- `FAKE_STRENGTH`: `48`

## Action Order Top30（交易动作顺序，不等于纯收益预测）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 603318 | 水发燃气 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 39.21 | 3.75 | 9.98 | 6.23 | momentum_catchup_pool |
| 2 | 600338 | 西藏珠峰 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 50.34 | 0.36 | 1.09 | 0.73 | theme_catchup_pool |
| 3 | 600206 | 有研新材 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 39.75 | -0.67 | -0.26 | 0.41 | theme_catchup_pool |
| 4 | 001332 | 锡装股份 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.88 | 0.18 | 2.34 | 2.16 | theme_catchup_pool |
| 5 | 600433 | 冠豪高新 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.21 | 0.0 | 2.73 | 2.73 | theme_catchup_pool |
| 6 | 603125 | 常青科技 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 30.64 | 3.45 | 4.75 | 1.3 | theme_catchup_pool |
| 7 | 600410 | 华胜天成 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 34.94 | -9.92 | -10.0 | -0.08 | low_open_reversal_pool |
| 8 | 600726 | 华电能源 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 38.19 | -0.78 | 10.03 | 10.81 | low_open_reversal_pool |
| 9 | 600770 | 综艺股份 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 34.28 | -3.59 | 10.06 | 13.65 | low_open_reversal_pool |
| 10 | 000066 | 中国长城 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.45 | -0.83 | 6.32 | 7.15 | low_open_reversal_pool |
| 11 | 601869 | 长飞光纤 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.8 | -1.85 | -1.06 | 0.79 | low_open_reversal_pool |
| 12 | 002218 | 拓日新能 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 10.02 | 10.02 | 0.0 | board_watch_pool |
| 13 | 001266 | 宏英智能 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 10.01 | 10.01 | 0.0 | board_watch_pool |
| 14 | 300905 | 宝丽迪 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 18.25 | 20.0 | 1.75 | board_watch_pool |

## Expected Return Proxy Top30（盘前可见字段的收益预期展示）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 603318 | 水发燃气 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 39.21 | 3.75 | 9.98 | 6.23 | momentum_catchup_pool |
| 2 | 600770 | 综艺股份 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 34.28 | -3.59 | 10.06 | 13.65 | low_open_reversal_pool |
| 3 | 600410 | 华胜天成 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 34.94 | -9.92 | -10.0 | -0.08 | low_open_reversal_pool |
| 4 | 600726 | 华电能源 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 38.19 | -0.78 | 10.03 | 10.81 | low_open_reversal_pool |
| 5 | 601869 | 长飞光纤 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.8 | -1.85 | -1.06 | 0.79 | low_open_reversal_pool |
| 6 | 000066 | 中国长城 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.45 | -0.83 | 6.32 | 7.15 | low_open_reversal_pool |
| 7 | 600338 | 西藏珠峰 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 50.34 | 0.36 | 1.09 | 0.73 | theme_catchup_pool |
| 8 | 600206 | 有研新材 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 39.75 | -0.67 | -0.26 | 0.41 | theme_catchup_pool |
| 9 | 603125 | 常青科技 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 30.64 | 3.45 | 4.75 | 1.3 | theme_catchup_pool |
| 10 | 001332 | 锡装股份 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.88 | 0.18 | 2.34 | 2.16 | theme_catchup_pool |
| 11 | 600433 | 冠豪高新 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.21 | 0.0 | 2.73 | 2.73 | theme_catchup_pool |
| 12 | 002218 | 拓日新能 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 10.02 | 10.02 | 0.0 | board_watch_pool |
| 13 | 001266 | 宏英智能 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 10.01 | 10.01 | 0.0 | board_watch_pool |
| 14 | 300905 | 宝丽迪 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 18.25 | 20.0 | 1.75 | board_watch_pool |

