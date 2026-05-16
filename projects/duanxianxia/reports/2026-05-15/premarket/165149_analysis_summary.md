# 165149_analysis_v7_3.json 全量候选摘要

- source_report: `165149_analysis_v7_3.json`
- version: `premarket_v7_3`
- date_t0: `2026-05-15`
- generated_at: `2026-05-16T16:51:49+08:00`
- candidate_count: `216`
- regime: `{'label': 'normal', 'reason': 'qx=None, lbbx=None, ztbx=None, breadth=None', 'qx_t0': None, 'qx_t1': 28.0, 'dt_t0': None, 'kqxy_t0': None, 'sz_t0': None, 'xd_t0': None, 'breadth_t0': None, 'lbbx_t0': None, 'lbbx_t1': 2.08, 'ztbx_t0': None, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']}`
- regime reason: `qx=None, lbbx=None, ztbx=None, breadth=None`

## action_stats

- `AUCTION_FOLLOW`: `4`
- `MOMENTUM_CATCHUP`: `5`
- `LOW_OPEN_REVERSAL`: `5`
- `BROAD_REPAIR_MOMENTUM`: `51`
- `BOARD_WATCH`: `7`
- `CONFIRMATION_WATCH`: `4`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `15`
- `AVOID`: `10`
- `DEBUG_ONLY`: `115`

## action_quality_stats

- `AUCTION_FOLLOW:main_attack`: `4`
- `MOMENTUM_CATCHUP:momentum`: `5`
- `LOW_OPEN_REVERSAL:repair`: `5`
- `BROAD_REPAIR_MOMENTUM:broad_repair`: `51`
- `BOARD_WATCH:watch_only`: `7`
- `CONFIRMATION_WATCH:watch`: `2`
- `CONFIRMATION_WATCH:high_cost_watch`: `2`
- `SOFT_AVOID_REPAIR_CANDIDATE:soft_avoid`: `15`
- `AVOID:hard_avoid`: `10`
- `DEBUG_ONLY:debug`: `115`

## pool_performance

- `AUCTION_FOLLOW`: `{"count": 4, "with_performance": 4, "avg_close_pct": 4.48, "med_close_pct": 6.42, "avg_excess_return": 0.27, "med_excess_return": 1.94, "positive_excess_count": 2, "negative_excess_count": 2}`
- `MOMENTUM_CATCHUP`: `{"count": 5, "with_performance": 5, "avg_close_pct": 9.09, "med_close_pct": 8.52, "avg_excess_return": 5.06, "med_excess_return": 4.43, "positive_excess_count": 3, "negative_excess_count": 2}`
- `LOW_OPEN_REVERSAL`: `{"count": 5, "with_performance": 5, "avg_close_pct": -1.9, "med_close_pct": -7.07, "avg_excess_return": 1.29, "med_excess_return": -4.45, "positive_excess_count": 2, "negative_excess_count": 3}`
- `BOARD_WATCH`: `{"count": 7, "with_performance": 7, "avg_close_pct": 53.89, "med_close_pct": 10.04, "avg_excess_return": 2.28, "med_excess_return": 0.0, "positive_excess_count": 3, "negative_excess_count": 0}`
- `CONFIRMATION_WATCH`: `{"count": 4, "with_performance": 4, "avg_close_pct": 8.97, "med_close_pct": 8.86, "avg_excess_return": 3.96, "med_excess_return": 4.21, "positive_excess_count": 3, "negative_excess_count": 1}`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `{"count": 15, "with_performance": 15, "avg_close_pct": -2.57, "med_close_pct": -2.97, "avg_excess_return": -3.22, "med_excess_return": -2.25, "positive_excess_count": 5, "negative_excess_count": 10}`
- `AVOID`: `{"count": 10, "with_performance": 10, "avg_close_pct": 1.05, "med_close_pct": -0.07, "avg_excess_return": -3.57, "med_excess_return": -2.69, "positive_excess_count": 3, "negative_excess_count": 5}`
- `DEBUG_ONLY`: `{"count": 115, "with_performance": 108, "avg_close_pct": 0.71, "med_close_pct": -0.33, "avg_excess_return": -0.88, "med_excess_return": -1.99, "positive_excess_count": 31, "negative_excess_count": 74}`
- `BROAD_REPAIR_MOMENTUM`: `{"count": 51, "with_performance": 51, "avg_close_pct": -0.78, "med_close_pct": -1.58, "avg_excess_return": -1.88, "med_excess_return": -2.04, "positive_excess_count": 12, "negative_excess_count": 39}`

## review_diagnostics

- `missed_winners`: `8`
- `debug_missed_winners`: `6`
- `avoid_missed_winners`: `0`
- `soft_avoid_missed_winners`: `1`
- `fake_strength_watch_winners`: `0`
- `false_positives`: `5`
- `high_cost_confirmation_failures`: `0`

## review_profiles

### missed_winners

- `count`: `8`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 7], ["FAKE_STRENGTH", 1]]`
- `action_type_top`: `[["DEBUG_ONLY", 6], ["SOFT_AVOID_REPAIR_CANDIDATE", 1], ["CONFIRMATION_WATCH", 1]]`
- `action_quality_top`: `[["debug", 6], ["soft_avoid", 1], ["high_cost_watch", 1]]`
- `setup_v72_top`: `[["none", 7], ["T0-GENERAL", 1]]`
- `confidence_top`: `[["none", 7], ["low", 1]]`
- `entry_tag_top`: `[["normal", 4], ["low_liquidity_confirm", 2], ["avoid", 1], ["high_open_confirm", 1]]`
- `auction_pct_bucket`: `[["[2,5)", 3], ["[0,2)", 3], ["[-2,0)", 1], [">=9", 1]]`
- `auction_amount_bucket`: `[["missing", 3], ["<500w", 2], ["500-1000w", 2], ["3000-8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 8, "min": -1.79, "p25": 1.46, "median": 1.93, "p75": 3.66, "max": 9.17, "avg": 2.57}, "auction_strength": {"count": 8, "min": 2.27, "p25": 6.45, "median": 8.84, "p75": 13.62, "max": 46.42, "avg": 12.89}, "auction_amount_wan": {"count": 5, "min": 144.0, "p25": 190.0, "median": 626.0, "p75": 634.0, "max": 3811.0, "avg": 1081.0}, "liquidity_score": {"count": 0}, "theme_strength_t0": {"count": 8, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 8, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 3.91, "max": 18.29, "avg": 3.09}, "source_family_count": {"count": 8, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 2.0, "avg": 0.5}, "final_score": {"count": 8, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 39.08, "avg": 4.88}}`
- `top_names`: `["301081 严牌股份", "002971 和远气体", "300263 隆华科技", "688662 富信科技", "601996 丰林集团", "603082 北自科技", "603078 江化微", "600578 京能电力"]`

### debug_missed_winners

- `count`: `6`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 6]]`
- `action_type_top`: `[["DEBUG_ONLY", 6]]`
- `action_quality_top`: `[["debug", 6]]`
- `setup_v72_top`: `[["none", 6]]`
- `confidence_top`: `[["none", 6]]`
- `entry_tag_top`: `[["normal", 4], ["low_liquidity_confirm", 2]]`
- `auction_pct_bucket`: `[["[2,5)", 3], ["[0,2)", 3]]`
- `auction_amount_bucket`: `[["<500w", 2], ["500-1000w", 2], ["missing", 2]]`
- `numeric_stats`: `{"auction_pct": {"count": 6, "min": 1.08, "p25": 1.46, "median": 1.93, "p75": 3.13, "max": 3.66, "avg": 2.2}, "auction_strength": {"count": 6, "min": 3.33, "p25": 6.45, "median": 8.84, "p75": 13.38, "max": 13.62, "avg": 9.08}, "auction_amount_wan": {"count": 4, "min": 144.0, "p25": 190.0, "median": 408.0, "p75": 634.0, "max": 634.0, "avg": 398.5}, "liquidity_score": {"count": 0}, "theme_strength_t0": {"count": 6, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 6, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 3.91, "max": 18.29, "avg": 3.7}, "source_family_count": {"count": 6, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 1.0, "avg": 0.33}, "final_score": {"count": 6, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}}`
- `top_names`: `["301081 严牌股份", "688662 富信科技", "601996 丰林集团", "603082 北自科技", "603078 江化微", "600578 京能电力"]`

### avoid_missed_winners

- empty

### soft_avoid_missed_winners

- `count`: `1`
- `auction_setup_type_top`: `[["FAKE_STRENGTH", 1]]`
- `action_type_top`: `[["SOFT_AVOID_REPAIR_CANDIDATE", 1]]`
- `action_quality_top`: `[["soft_avoid", 1]]`
- `setup_v72_top`: `[["none", 1]]`
- `confidence_top`: `[["none", 1]]`
- `entry_tag_top`: `[["avoid", 1]]`
- `auction_pct_bucket`: `[["[-2,0)", 1]]`
- `auction_amount_bucket`: `[["missing", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 1, "min": -1.79, "p25": -1.79, "median": -1.79, "p75": -1.79, "max": -1.79, "avg": -1.79}, "auction_strength": {"count": 1, "min": 2.27, "p25": 2.27, "median": 2.27, "p75": 2.27, "max": 2.27, "avg": 2.27}, "auction_amount_wan": {"count": 0}, "liquidity_score": {"count": 0}, "theme_strength_t0": {"count": 1, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}}`
- `top_names`: `["002971 和远气体"]`

### fake_strength_watch_winners

- empty

### false_positives

- `count`: `5`
- `auction_setup_type_top`: `[["LOW_OPEN_REVERSAL", 3], ["GENERAL_WATCH", 2]]`
- `action_type_top`: `[["LOW_OPEN_REVERSAL", 3], ["AUCTION_FOLLOW", 1], ["MOMENTUM_CATCHUP", 1]]`
- `action_quality_top`: `[["repair", 3], ["main_attack", 1], ["momentum", 1]]`
- `setup_v72_top`: `[["T0-REVERSAL", 3], ["T0-NEW", 1], ["T0-GENERAL", 1]]`
- `confidence_top`: `[["low", 5]]`
- `entry_tag_top`: `[["normal", 5]]`
- `auction_pct_bucket`: `[["[2,5)", 2], ["[-2,0)", 2], ["<-5", 1]]`
- `auction_amount_bucket`: `[[">=8000w", 4], ["3000-8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 5, "min": -5.12, "p25": -1.66, "median": -1.21, "p75": 2.07, "max": 3.78, "avg": -0.43}, "auction_strength": {"count": 5, "min": 39.88, "p25": 45.22, "median": 49.75, "p75": 61.72, "max": 74.54, "avg": 54.22}, "auction_amount_wan": {"count": 5, "min": 6229.0, "p25": 9889.0, "median": 11742.0, "p75": 16068.0, "max": 19062.0, "avg": 12598.0}, "liquidity_score": {"count": 0}, "theme_strength_t0": {"count": 5, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 5, "min": 11.45, "p25": 11.92, "median": 16.97, "p75": 19.32, "max": 38.78, "avg": 19.69}, "source_family_count": {"count": 5, "min": 1.0, "p25": 1.0, "median": 1.0, "p75": 1.0, "max": 2.0, "avg": 1.2}, "final_score": {"count": 5, "min": 38.49, "p25": 40.52, "median": 42.18, "p75": 42.41, "max": 58.01, "avg": 44.32}}`
- `top_names`: `["002498 汉缆股份", "002015 协鑫能科", "002428 云南锗业", "000066 中国长城", "605020 永和股份"]`

### high_cost_confirmation_failures

- empty

## candidate_pools counts

- `main_attack_pool`: 4
- `momentum_catchup_pool`: 5
- `theme_rotation_pool`: 0
- `theme_catchup_pool`: 0
- `low_open_reversal_pool`: 5
- `board_watch_pool`: 7
- `confirmation_watch_pool`: 4
- `fake_strength_watch_pool`: 0
- `soft_avoid_repair_pool`: 15
- `avoid_or_risk_pool`: 10
- `debug_only_pool`: 15

## 绩效补充口径

- `auction_pct`: 竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return = close_pct - auction_pct`

## 收盘涨幅 / 超额收益（全量）

- `dailyline_matched`: `209 / 216`
- `avg_close_pct`: `2.28`
- `med_close_pct`: `-0.05`
- `avg_excess_return`: `-1.01`
- `med_excess_return`: `-1.7`
- `pos_close_count`: `104/209`
- `pos_excess_count`: `64/206`

## 收盘涨幅 / 超额收益（Action Order Top30）

- `dailyline_matched`: `21 / 21`
- `avg_close_pct`: `20.53`
- `med_close_pct`: `10.0`
- `avg_excess_return`: `2.32`
- `med_excess_return`: `0.0`
- `pos_close_count`: `16/21`
- `pos_excess_count`: `10/21`

## 收盘涨幅 / 超额收益（Expected Return Proxy Top30）

- `dailyline_matched`: `21 / 21`
- `avg_close_pct`: `20.53`
- `med_close_pct`: `10.0`
- `avg_excess_return`: `2.32`
- `med_excess_return`: `0.0`
- `pos_close_count`: `16/21`
- `pos_excess_count`: `10/21`

## setup_v72 分布

- `T0-NEW`: `2`
- `T0-GENERAL`: `15`
- `T0-REVERSAL`: `5`
- `none`: `194`

## action_type 分布

- `AUCTION_FOLLOW`: `4`
- `MOMENTUM_CATCHUP`: `5`
- `LOW_OPEN_REVERSAL`: `5`
- `BOARD_WATCH`: `7`
- `CONFIRMATION_WATCH`: `4`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `15`
- `AVOID`: `10`
- `BROAD_REPAIR_MOMENTUM`: `51`
- `DEBUG_ONLY`: `115`

## action_quality 分布

- `main_attack`: `4`
- `momentum`: `5`
- `repair`: `5`
- `watch_only`: `7`
- `watch`: `2`
- `high_cost_watch`: `2`
- `soft_avoid`: `15`
- `hard_avoid`: `10`
- `broad_repair`: `51`
- `debug`: `115`

## confidence 分布

- `low`: `22`
- `none`: `194`

## auction_setup_type 分布

- `GENERAL_WATCH`: `161`
- `LOW_OPEN_REVERSAL`: `5`
- `BOARD_LOCK_WATCH`: `3`
- `FAKE_STRENGTH`: `25`
- `LOW_OPEN_WEAK`: `22`

## Action Order Top30（交易动作顺序，不等于纯收益预测）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 002498 | 汉缆股份 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-NEW | low | 58.01 | 2.07 | -5.71 | -7.78 | main_attack_pool |
| 2 | 300302 | 同有科技 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-NEW | low | 53.88 | 3.02 | 2.81 | -0.21 | main_attack_pool |
| 3 | 002407 | 多氟多 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-GENERAL | low | 50.63 | 5.94 | 10.02 | 4.08 | main_attack_pool |
| 4 | 300959 | 线上线下 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-GENERAL | low | 45.9 | 5.83 | 10.81 | 4.98 | main_attack_pool |
| 5 | 605020 | 永和股份 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 42.41 | 3.78 | -0.47 | -4.25 | momentum_catchup_pool |
| 6 | 688143 | 长盈通 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 44.53 | 2.62 | 18.12 | 15.5 | momentum_catchup_pool |
| 7 | 002297 | 博云新材 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 41.04 | 4.01 | 3.77 | -0.24 | momentum_catchup_pool |
| 8 | 301486 | 致尚科技 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 40.16 | 4.09 | 8.52 | 4.43 | momentum_catchup_pool |
| 9 | 688535 | 华海诚科 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 44.38 | 5.62 | 15.49 | 9.87 | momentum_catchup_pool |
| 10 | 601991 | 大唐发电 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 45.88 | -2.92 | 10.0 | 12.92 | low_open_reversal_pool |
| 11 | 002015 | 协鑫能科 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 42.18 | -1.66 | -7.62 | -5.96 | low_open_reversal_pool |
| 12 | 600396 | 华电辽能 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 40.1 | -5.05 | 4.75 | 9.8 | low_open_reversal_pool |
| 13 | 000066 | 中国长城 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 40.52 | -5.12 | -9.57 | -4.45 | low_open_reversal_pool |
| 14 | 002428 | 云南锗业 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 38.49 | -1.21 | -7.07 | -5.86 | low_open_reversal_pool |
| 15 | 601678 | 滨化股份 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 41.98 | 10.04 | 10.04 | 0.0 | board_watch_pool |
| 16 | 002915 | 中欣氟材 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 39.86 | 10.0 | 10.0 | 0.0 | board_watch_pool |
| 17 | 300721 | 怡达股份 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 43.3 | 9.6 | 19.98 | 10.38 | board_watch_pool |
| 18 | 688549 | 中巨芯-U | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 38.19 | 16.65 | 20.01 | 3.36 | board_watch_pool |
| 19 | 001393 | N维通利 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 295.0 | 297.2 | 2.2 | board_watch_pool |
| 20 | 001259 | 利仁科技 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 10.0 | 10.0 | 0.0 | board_watch_pool |
| 21 | 002918 | 蒙娜丽莎 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 9.98 | 9.98 | 0.0 | board_watch_pool |

## Expected Return Proxy Top30（盘前可见字段的收益预期展示）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 605020 | 永和股份 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 42.41 | 3.78 | -0.47 | -4.25 | momentum_catchup_pool |
| 2 | 688143 | 长盈通 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 44.53 | 2.62 | 18.12 | 15.5 | momentum_catchup_pool |
| 3 | 002297 | 博云新材 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 41.04 | 4.01 | 3.77 | -0.24 | momentum_catchup_pool |
| 4 | 301486 | 致尚科技 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 40.16 | 4.09 | 8.52 | 4.43 | momentum_catchup_pool |
| 5 | 688535 | 华海诚科 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 44.38 | 5.62 | 15.49 | 9.87 | momentum_catchup_pool |
| 6 | 002015 | 协鑫能科 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 42.18 | -1.66 | -7.62 | -5.96 | low_open_reversal_pool |
| 7 | 002428 | 云南锗业 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 38.49 | -1.21 | -7.07 | -5.86 | low_open_reversal_pool |
| 8 | 601991 | 大唐发电 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 45.88 | -2.92 | 10.0 | 12.92 | low_open_reversal_pool |
| 9 | 600396 | 华电辽能 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 40.1 | -5.05 | 4.75 | 9.8 | low_open_reversal_pool |
| 10 | 000066 | 中国长城 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 40.52 | -5.12 | -9.57 | -4.45 | low_open_reversal_pool |
| 11 | 002498 | 汉缆股份 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-NEW | low | 58.01 | 2.07 | -5.71 | -7.78 | main_attack_pool |
| 12 | 300302 | 同有科技 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-NEW | low | 53.88 | 3.02 | 2.81 | -0.21 | main_attack_pool |
| 13 | 002407 | 多氟多 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-GENERAL | low | 50.63 | 5.94 | 10.02 | 4.08 | main_attack_pool |
| 14 | 300959 | 线上线下 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-GENERAL | low | 45.9 | 5.83 | 10.81 | 4.98 | main_attack_pool |
| 15 | 601678 | 滨化股份 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 41.98 | 10.04 | 10.04 | 0.0 | board_watch_pool |
| 16 | 300721 | 怡达股份 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 43.3 | 9.6 | 19.98 | 10.38 | board_watch_pool |
| 17 | 002915 | 中欣氟材 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 39.86 | 10.0 | 10.0 | 0.0 | board_watch_pool |
| 18 | 002918 | 蒙娜丽莎 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 9.98 | 9.98 | 0.0 | board_watch_pool |
| 19 | 688549 | 中巨芯-U | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | T0-GENERAL | low | 38.19 | 16.65 | 20.01 | 3.36 | board_watch_pool |
| 20 | 001259 | 利仁科技 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 10.0 | 10.0 | 0.0 | board_watch_pool |
| 21 | 001393 | N维通利 | BOARD_WATCH | watch_only | near_limit_or_locked_board_watch | none | none | 0.0 | 295.0 | 297.2 | 2.2 | board_watch_pool |

