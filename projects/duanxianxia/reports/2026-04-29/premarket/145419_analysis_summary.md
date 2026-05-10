# 145419_analysis_v7_3.json 全量候选摘要

- source_report: `145419_analysis_v7_3.json`
- version: `premarket_v7_3`
- date_t0: `2026-04-29`
- generated_at: `2026-05-10T14:54:19+08:00`
- candidate_count: `308`
- regime: `{'label': 'cold', 'reason': 'qx=20.0, dt=11.0, kqxy=0.0, breadth=0.27373612823674476', 'qx_t0': 20.0, 'qx_t1': 20.0, 'dt_t0': 11.0, 'kqxy_t0': 0.0, 'sz_t0': 1332.0, 'xd_t0': 3534.0, 'breadth_t0': 0.27373612823674476, 'lbbx_t0': 0.2, 'lbbx_t1': 0.88, 'ztbx_t0': 0.73, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']}`
- regime reason: `qx=20.0, dt=11.0, kqxy=0.0, breadth=0.27373612823674476`

## action_stats

- `AUCTION_FOLLOW`: `1`
- `MOMENTUM_CATCHUP`: `1`
- `THEME_CATCHUP`: `20`
- `LOW_OPEN_REVERSAL`: `9`
- `BROAD_REPAIR_MOMENTUM`: `42`
- `BOARD_WATCH`: `7`
- `CONFIRMATION_WATCH`: `7`
- `FAKE_STRENGTH_WATCH`: `6`
- `HIGH_COST_REPAIR_WATCH`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `27`
- `AVOID`: `10`
- `DEBUG_ONLY`: `177`

## action_quality_stats

- `AUCTION_FOLLOW:main_attack`: `1`
- `MOMENTUM_CATCHUP:momentum`: `1`
- `THEME_CATCHUP:medium`: `6`
- `THEME_CATCHUP:strong`: `2`
- `THEME_CATCHUP:weak`: `12`
- `LOW_OPEN_REVERSAL:repair`: `9`
- `BROAD_REPAIR_MOMENTUM:broad_repair`: `42`
- `BOARD_WATCH:watch_only`: `7`
- `CONFIRMATION_WATCH:watch`: `6`
- `CONFIRMATION_WATCH:high_cost_watch`: `1`
- `FAKE_STRENGTH_WATCH:repair_watch`: `6`
- `HIGH_COST_REPAIR_WATCH:high_cost_repair_watch`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE:soft_avoid`: `27`
- `AVOID:hard_avoid`: `10`
- `DEBUG_ONLY:debug`: `177`

## pool_performance

- `AUCTION_FOLLOW`: `{"count": 1, "with_performance": 1, "avg_close_pct": 7.52, "med_close_pct": 7.52, "avg_excess_return": 1.77, "med_excess_return": 1.77, "positive_excess_count": 1, "negative_excess_count": 0}`
- `MOMENTUM_CATCHUP`: `{"count": 1, "with_performance": 1, "avg_close_pct": 20.0, "med_close_pct": 20.0, "avg_excess_return": 17.14, "med_excess_return": 17.14, "positive_excess_count": 1, "negative_excess_count": 0}`
- `THEME_CATCHUP`: `{"count": 20, "with_performance": 20, "avg_close_pct": 4.08, "med_close_pct": 4.47, "avg_excess_return": 3.07, "med_excess_return": 2.48, "positive_excess_count": 15, "negative_excess_count": 5}`
- `LOW_OPEN_REVERSAL`: `{"count": 9, "with_performance": 9, "avg_close_pct": 2.65, "med_close_pct": 3.86, "avg_excess_return": 5.7, "med_excess_return": 5.47, "positive_excess_count": 8, "negative_excess_count": 1}`
- `BROAD_REPAIR_MOMENTUM`: `{"count": 42, "with_performance": 42, "avg_close_pct": 6.06, "med_close_pct": 6.43, "avg_excess_return": 4.63, "med_excess_return": 4.95, "positive_excess_count": 38, "negative_excess_count": 3}`
- `BOARD_WATCH`: `{"count": 7, "with_performance": 5, "avg_close_pct": 11.53, "med_close_pct": 10.0, "avg_excess_return": -0.24, "med_excess_return": 0.0, "positive_excess_count": 2, "negative_excess_count": 1}`
- `CONFIRMATION_WATCH`: `{"count": 7, "with_performance": 7, "avg_close_pct": 3.74, "med_close_pct": 3.0, "avg_excess_return": 2.44, "med_excess_return": 1.97, "positive_excess_count": 5, "negative_excess_count": 2}`
- `FAKE_STRENGTH_WATCH`: `{"count": 6, "with_performance": 6, "avg_close_pct": 6.14, "med_close_pct": 7.93, "avg_excess_return": 2.7, "med_excess_return": 2.99, "positive_excess_count": 4, "negative_excess_count": 2}`
- `HIGH_COST_REPAIR_WATCH`: `{"count": 1, "with_performance": 1, "avg_close_pct": 17.08, "med_close_pct": 17.08, "avg_excess_return": 8.3, "med_excess_return": 8.3, "positive_excess_count": 1, "negative_excess_count": 0}`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `{"count": 27, "with_performance": 27, "avg_close_pct": 1.27, "med_close_pct": 1.33, "avg_excess_return": 1.17, "med_excess_return": 0.22, "positive_excess_count": 16, "negative_excess_count": 11}`
- `AVOID`: `{"count": 10, "with_performance": 10, "avg_close_pct": 7.05, "med_close_pct": 8.41, "avg_excess_return": 0.71, "med_excess_return": 0.57, "positive_excess_count": 6, "negative_excess_count": 2}`
- `DEBUG_ONLY`: `{"count": 177, "with_performance": 170, "avg_close_pct": 4.5, "med_close_pct": 4.64, "avg_excess_return": 2.33, "med_excess_return": 2.25, "positive_excess_count": 140, "negative_excess_count": 30}`

## review_diagnostics

- `missed_winners`: `12`
- `debug_missed_winners`: `10`
- `avoid_missed_winners`: `0`
- `soft_avoid_missed_winners`: `2`
- `fake_strength_watch_winners`: `0`
- `false_positives`: `3`
- `high_cost_confirmation_failures`: `1`
- `broad_repair_winners`: `7`
- `broad_repair_false_positives`: `1`
- `high_cost_repair_watch_winners`: `1`

## review_profiles

### missed_winners

- `count`: `12`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 7], ["LOW_OPEN_WEAK", 3], ["FAKE_STRENGTH", 2]]`
- `action_type_top`: `[["DEBUG_ONLY", 10], ["SOFT_AVOID_REPAIR_CANDIDATE", 2]]`
- `action_quality_top`: `[["debug", 10], ["soft_avoid", 2]]`
- `setup_v72_top`: `[["none", 12]]`
- `confidence_top`: `[["none", 12]]`
- `entry_tag_top`: `[["normal", 8], ["avoid", 2], ["low_liquidity_confirm", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 6], ["[-2,0)", 2], ["[2,5)", 2], ["<-5", 2]]`
- `auction_amount_bucket`: `[["missing", 5], ["500-1000w", 3], ["<500w", 2], ["3000-8000w", 1], [">=8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 12, "min": -11.9, "p25": -0.05, "median": 0.89, "p75": 1.89, "max": 2.85, "avg": -0.92}, "auction_strength": {"count": 12, "min": 2.27, "p25": 3.58, "median": 7.67, "p75": 14.3, "max": 38.78, "avg": 11.65}, "auction_amount_wan": {"count": 7, "min": 372.0, "p25": 387.0, "median": 660.0, "p75": 6875.0, "max": 18538.0, "avg": 4043.71}, "liquidity_score": {"count": 12, "min": 20.0, "p25": 20.0, "median": 21.59, "p75": 33.28, "max": 100.0, "avg": 33.49}, "theme_strength_t0": {"count": 12, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 95.0, "avg": 26.25}, "source_evidence_score": {"count": 12, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 2.39, "max": 6.67, "avg": 0.99}, "source_family_count": {"count": 12, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 1.0, "avg": 0.25}, "final_score": {"count": 12, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 12, "min": -68.43, "p25": -67.72, "median": -65.88, "p75": -46.75, "max": -18.51, "avg": -55.86}, "action_score": {"count": 12, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 8.57, "avg": 1.22}}`
- `top_names`: `["688059 华锐精密", "603095 越剑智能", "300302 同有科技", "000973 佛塑科技", "600156 华升股份", "002709 天赐材料", "301396 宏景科技", "688655 迅捷兴", "002652 扬子新材", "603779 威龙股份", "002498 汉缆股份", "000901 航天科技"]`

### debug_missed_winners

- `count`: `10`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 7], ["LOW_OPEN_WEAK", 3]]`
- `action_type_top`: `[["DEBUG_ONLY", 10]]`
- `action_quality_top`: `[["debug", 10]]`
- `setup_v72_top`: `[["none", 10]]`
- `confidence_top`: `[["none", 10]]`
- `entry_tag_top`: `[["normal", 8], ["low_liquidity_confirm", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 5], ["[2,5)", 2], ["<-5", 2], ["[-2,0)", 1]]`
- `auction_amount_bucket`: `[["500-1000w", 3], ["missing", 3], ["<500w", 2], ["3000-8000w", 1], [">=8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 10, "min": -11.9, "p25": -0.05, "median": 1.44, "p75": 1.89, "max": 2.85, "avg": -1.0}, "auction_strength": {"count": 10, "min": 3.54, "p25": 5.38, "median": 8.67, "p75": 14.3, "max": 38.78, "avg": 13.4}, "auction_amount_wan": {"count": 7, "min": 372.0, "p25": 387.0, "median": 660.0, "p75": 6875.0, "max": 18538.0, "avg": 4043.71}, "liquidity_score": {"count": 10, "min": 20.0, "p25": 20.0, "median": 22.97, "p75": 33.28, "max": 100.0, "avg": 36.19}, "theme_strength_t0": {"count": 10, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 10, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 2.39, "max": 6.67, "avg": 1.19}, "source_family_count": {"count": 10, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 1.0, "avg": 0.3}, "final_score": {"count": 10, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 10, "min": -68.43, "p25": -67.8, "median": -67.06, "p75": -63.53, "max": -46.75, "avg": -63.08}, "action_score": {"count": 10, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}}`
- `top_names`: `["688059 华锐精密", "300302 同有科技", "000973 佛塑科技", "002709 天赐材料", "301396 宏景科技", "688655 迅捷兴", "002652 扬子新材", "603779 威龙股份", "002498 汉缆股份", "000901 航天科技"]`

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
- `auction_pct_bucket`: `[["[-2,0)", 1], ["[0,2)", 1]]`
- `auction_amount_bucket`: `[["missing", 2]]`
- `numeric_stats`: `{"auction_pct": {"count": 2, "min": -1.0, "p25": -1.0, "median": -0.5, "p75": 0.0, "max": 0.0, "avg": -0.5}, "auction_strength": {"count": 2, "min": 2.27, "p25": 2.27, "median": 2.92, "p75": 3.58, "max": 3.58, "avg": 2.92}, "auction_amount_wan": {"count": 0}, "liquidity_score": {"count": 2, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "theme_strength_t0": {"count": 2, "min": 20.0, "p25": 20.0, "median": 57.5, "p75": 95.0, "max": 95.0, "avg": 57.5}, "source_evidence_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 2, "min": -20.99, "p25": -20.99, "median": -19.75, "p75": -18.51, "max": -18.51, "avg": -19.75}, "action_score": {"count": 2, "min": 6.05, "p25": 6.05, "median": 7.31, "p75": 8.57, "max": 8.57, "avg": 7.31}}`
- `top_names`: `["603095 越剑智能", "600156 华升股份"]`

### fake_strength_watch_winners

- empty

### broad_repair_winners

- `count`: `7`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 4], ["LOW_OPEN_WEAK", 3]]`
- `action_type_top`: `[["BROAD_REPAIR_MOMENTUM", 7]]`
- `action_quality_top`: `[["broad_repair", 7]]`
- `setup_v72_top`: `[["none", 7]]`
- `confidence_top`: `[["none", 7]]`
- `entry_tag_top`: `[["normal", 7]]`
- `auction_pct_bucket`: `[["[2,5)", 3], ["<-5", 2], ["[-2,0)", 1], ["[0,2)", 1]]`
- `auction_amount_bucket`: `[["3000-8000w", 2], ["500-1000w", 2], ["1000-3000w", 2], [">=8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 7, "min": -17.23, "p25": -5.11, "median": 0.1, "p75": 3.9, "max": 4.89, "avg": -1.72}, "auction_strength": {"count": 7, "min": 12.84, "p25": 13.0, "median": 28.98, "p75": 37.55, "max": 41.21, "avg": 26.02}, "auction_amount_wan": {"count": 7, "min": 804.0, "p25": 864.0, "median": 2849.0, "p75": 6261.0, "max": 18168.0, "avg": 5160.71}, "liquidity_score": {"count": 7, "min": 30.08, "p25": 31.28, "median": 70.98, "p75": 100.0, "max": 100.0, "avg": 66.75}, "theme_strength_t0": {"count": 7, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 7, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.44, "max": 0.96, "avg": 0.2}, "source_family_count": {"count": 7, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 1.0, "avg": 0.29}, "final_score": {"count": 7, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 7, "min": 36.16, "p25": 36.36, "median": 44.79, "p75": 55.31, "max": 56.47, "avg": 45.7}, "action_score": {"count": 7, "min": 26.17, "p25": 26.55, "median": 53.27, "p75": 61.16, "max": 71.0, "avg": 46.46}}`
- `top_names`: `["301291 明阳电气", "301205 联特科技", "300745 欣锐科技", "002081 金螳螂", "600186 莲花控股", "688717 艾罗能源", "301662 宏工科技"]`

### broad_repair_false_positives

- `count`: `1`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 1]]`
- `action_type_top`: `[["BROAD_REPAIR_MOMENTUM", 1]]`
- `action_quality_top`: `[["broad_repair", 1]]`
- `setup_v72_top`: `[["none", 1]]`
- `confidence_top`: `[["none", 1]]`
- `entry_tag_top`: `[["normal", 1]]`
- `auction_pct_bucket`: `[["[0,2)", 1]]`
- `auction_amount_bucket`: `[["1000-3000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 1, "min": 0.81, "p25": 0.81, "median": 0.81, "p75": 0.81, "max": 0.81, "avg": 0.81}, "auction_strength": {"count": 1, "min": 20.64, "p25": 20.64, "median": 20.64, "p75": 20.64, "max": 20.64, "avg": 20.64}, "auction_amount_wan": {"count": 1, "min": 1890.0, "p25": 1890.0, "median": 1890.0, "p75": 1890.0, "max": 1890.0, "avg": 1890.0}, "liquidity_score": {"count": 1, "min": 51.8, "p25": 51.8, "median": 51.8, "p75": 51.8, "max": 51.8, "avg": 51.8}, "theme_strength_t0": {"count": 1, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 1, "min": 40.61, "p25": 40.61, "median": 40.61, "p75": 40.61, "max": 40.61, "avg": 40.61}, "action_score": {"count": 1, "min": 42.8, "p25": 42.8, "median": 42.8, "p75": 42.8, "max": 42.8, "avg": 42.8}}`
- `top_names`: `["002290 禾盛新材"]`

### high_cost_repair_watch_winners

- `count`: `1`
- `auction_setup_type_top`: `[["FAKE_STRENGTH", 1]]`
- `action_type_top`: `[["HIGH_COST_REPAIR_WATCH", 1]]`
- `action_quality_top`: `[["high_cost_repair_watch", 1]]`
- `setup_v72_top`: `[["none", 1]]`
- `confidence_top`: `[["none", 1]]`
- `entry_tag_top`: `[["avoid", 1]]`
- `auction_pct_bucket`: `[["[7,9)", 1]]`
- `auction_amount_bucket`: `[[">=8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 1, "min": 8.78, "p25": 8.78, "median": 8.78, "p75": 8.78, "max": 8.78, "avg": 8.78}, "auction_strength": {"count": 1, "min": 23.25, "p25": 23.25, "median": 23.25, "p75": 23.25, "max": 23.25, "avg": 23.25}, "auction_amount_wan": {"count": 1, "min": 12738.0, "p25": 12738.0, "median": 12738.0, "p75": 12738.0, "max": 12738.0, "avg": 12738.0}, "liquidity_score": {"count": 1, "min": 90.0, "p25": 90.0, "median": 90.0, "p75": 90.0, "max": 90.0, "avg": 90.0}, "theme_strength_t0": {"count": 1, "min": 95.0, "p25": 95.0, "median": 95.0, "p75": 95.0, "max": 95.0, "avg": 95.0}, "source_evidence_score": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 1, "min": -5.53, "p25": -5.53, "median": -5.53, "p75": -5.53, "max": -5.53, "avg": -5.53}, "action_score": {"count": 1, "min": 45.06, "p25": 45.06, "median": 45.06, "p75": 45.06, "max": 45.06, "avg": 45.06}}`
- `top_names`: `["301666 大普微-UW"]`

### false_positives

- `count`: `3`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 2], ["LOW_OPEN_WEAK", 1]]`
- `action_type_top`: `[["THEME_CATCHUP", 2], ["BROAD_REPAIR_MOMENTUM", 1]]`
- `action_quality_top`: `[["weak", 2], ["broad_repair", 1]]`
- `setup_v72_top`: `[["T0-GENERAL", 2], ["none", 1]]`
- `confidence_top`: `[["low", 2], ["none", 1]]`
- `entry_tag_top`: `[["normal", 3]]`
- `auction_pct_bucket`: `[["[0,2)", 2], ["[-2,0)", 1]]`
- `auction_amount_bucket`: `[["missing", 2], ["1000-3000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 3, "min": -0.97, "p25": -0.97, "median": 0.0, "p75": 0.81, "max": 0.81, "avg": -0.05}, "auction_strength": {"count": 3, "min": 6.88, "p25": 6.88, "median": 8.88, "p75": 20.64, "max": 20.64, "avg": 12.13}, "auction_amount_wan": {"count": 1, "min": 1890.0, "p25": 1890.0, "median": 1890.0, "p75": 1890.0, "max": 1890.0, "avg": 1890.0}, "liquidity_score": {"count": 3, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 51.8, "max": 51.8, "avg": 30.6}, "theme_strength_t0": {"count": 3, "min": 20.0, "p25": 20.0, "median": 95.0, "p75": 95.0, "max": 95.0, "avg": 70.0}, "source_evidence_score": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 3, "min": 0.0, "p25": 0.0, "median": 25.32, "p75": 26.49, "max": 26.49, "avg": 17.27}, "expected_return_score": {"count": 3, "min": 19.09, "p25": 19.09, "median": 19.45, "p75": 40.61, "max": 40.61, "avg": 26.38}, "action_score": {"count": 3, "min": 24.69, "p25": 24.69, "median": 29.2, "p75": 42.8, "max": 42.8, "avg": 32.23}}`
- `top_names`: `["001358 兴欣新材", "002290 禾盛新材", "601828 美凯龙"]`

### high_cost_confirmation_failures

- `count`: `1`
- `auction_setup_type_top`: `[["SUSTAINED_PLUS_LAST_SECOND", 1]]`
- `action_type_top`: `[["CONFIRMATION_WATCH", 1]]`
- `action_quality_top`: `[["high_cost_watch", 1]]`
- `setup_v72_top`: `[["T0-GENERAL", 1]]`
- `confidence_top`: `[["low", 1]]`
- `entry_tag_top`: `[["normal", 1]]`
- `auction_pct_bucket`: `[["[7,9)", 1]]`
- `auction_amount_bucket`: `[["3000-8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 1, "min": 7.67, "p25": 7.67, "median": 7.67, "p75": 7.67, "max": 7.67, "avg": 7.67}, "auction_strength": {"count": 1, "min": 49.9, "p25": 49.9, "median": 49.9, "p75": 49.9, "max": 49.9, "avg": 49.9}, "auction_amount_wan": {"count": 1, "min": 4697.0, "p25": 4697.0, "median": 4697.0, "p75": 4697.0, "max": 4697.0, "avg": 4697.0}, "liquidity_score": {"count": 1, "min": 74.0, "p25": 74.0, "median": 74.0, "p75": 74.0, "max": 74.0, "avg": 74.0}, "theme_strength_t0": {"count": 1, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 1, "min": 39.68, "p25": 39.68, "median": 39.68, "p75": 39.68, "max": 39.68, "avg": 39.68}, "source_family_count": {"count": 1, "min": 3.0, "p25": 3.0, "median": 3.0, "p75": 3.0, "max": 3.0, "avg": 3.0}, "final_score": {"count": 1, "min": 35.35, "p25": 35.35, "median": 35.35, "p75": 35.35, "max": 35.35, "avg": 35.35}, "expected_return_score": {"count": 1, "min": 20.39, "p25": 20.39, "median": 20.39, "p75": 20.39, "max": 20.39, "avg": 20.39}, "action_score": {"count": 1, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}}`
- `top_names`: `["000425 徐工机械"]`

## candidate_pools counts

- `main_attack_pool`: 1
- `momentum_catchup_pool`: 1
- `theme_rotation_pool`: 1
- `theme_catchup_pool`: 15
- `low_open_reversal_pool`: 9
- `broad_repair_momentum_pool`: 15
- `board_watch_pool`: 7
- `confirmation_watch_pool`: 7
- `fake_strength_watch_pool`: 6
- `high_cost_repair_watch_pool`: 1
- `soft_avoid_repair_pool`: 15
- `avoid_or_risk_pool`: 10
- `debug_only_pool`: 15

## 绩效补充口径

- `auction_pct`: 竞价涨幅，优先使用 v7.3 顶层 `auction_pct`，缺失时回退 `auction_detail.latest_change_pct`
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return = close_pct - auction_pct`

## 收盘涨幅 / 超额收益（全量）

- `dailyline_matched`: `299 / 308`
- `avg_close_pct`: `4.67`
- `med_close_pct`: `4.75`
- `avg_excess_return`: `2.68`
- `med_excess_return`: `2.27`
- `pos_close_count`: `246/299`
- `pos_excess_count`: `237/299`

## 收盘涨幅 / 超额收益（Action Order Top30）

- `dailyline_matched`: `30 / 30`
- `avg_close_pct`: `4.21`
- `med_close_pct`: `4.16`
- `avg_excess_return`: `4.09`
- `med_excess_return`: `3.0`
- `pos_close_count`: `22/30`
- `pos_excess_count`: `24/30`

## 收盘涨幅 / 超额收益（Expected Return Proxy Top30）

- `dailyline_matched`: `30 / 30`
- `avg_close_pct`: `4.21`
- `med_close_pct`: `4.0`
- `avg_excess_return`: `5.35`
- `med_excess_return`: `4.61`
- `pos_close_count`: `23/30`
- `pos_excess_count`: `26/30`

## setup_v72 分布

- `T0-ROTATE`: `1`
- `T0-GENERAL`: `32`
- `T0-REVERSAL`: `9`
- `none`: `266`

## action_type 分布

- `AUCTION_FOLLOW`: `1`
- `MOMENTUM_CATCHUP`: `1`
- `THEME_CATCHUP`: `20`
- `LOW_OPEN_REVERSAL`: `9`
- `BROAD_REPAIR_MOMENTUM`: `42`
- `BOARD_WATCH`: `7`
- `CONFIRMATION_WATCH`: `7`
- `FAKE_STRENGTH_WATCH`: `6`
- `HIGH_COST_REPAIR_WATCH`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `27`
- `AVOID`: `10`
- `DEBUG_ONLY`: `177`

## action_quality 分布

- `main_attack`: `1`
- `momentum`: `1`
- `medium`: `6`
- `strong`: `2`
- `weak`: `12`
- `repair`: `9`
- `broad_repair`: `42`
- `watch_only`: `7`
- `watch`: `6`
- `high_cost_watch`: `1`
- `repair_watch`: `6`
- `high_cost_repair_watch`: `1`
- `soft_avoid`: `27`
- `hard_avoid`: `10`
- `debug`: `177`

## confidence 分布

- `high`: `1`
- `low`: `41`
- `none`: `266`

## auction_setup_type 分布

- `SUSTAINED_PLUS_LAST_SECOND`: `3`
- `GENERAL_WATCH`: `212`
- `LOW_OPEN_WEAK`: `39`
- `LOW_OPEN_REVERSAL`: `9`
- `BOARD_LOCK_WATCH`: `1`
- `FAKE_STRENGTH`: `44`

## Action Order Top30（交易动作顺序，不等于纯收益预测）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 000709 | 河钢股份 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-ROTATE | high | 61.32 | 5.75 | 7.52 | 1.77 | main_attack_pool|theme_rotation_pool |
| 2 | 300769 | 德方纳米 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 37.72 | 2.86 | 20.0 | 17.14 | momentum_catchup_pool |
| 3 | 300821 | 东岳硅材 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 32.26 | 1.21 | 5.32 | 4.11 | theme_catchup_pool |
| 4 | 600338 | 西藏珠峰 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 40.76 | 0.56 | 10.0 | 9.44 | theme_catchup_pool |
| 5 | 002352 | 顺丰控股 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 39.19 | 1.03 | -0.49 | -1.52 | theme_catchup_pool |
| 6 | 600111 | 北方稀土 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 39.91 | 2.55 | 10.0 | 7.45 | theme_catchup_pool |
| 7 | 603083 | 剑桥科技 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 36.64 | -0.44 | 10.0 | 10.44 | theme_catchup_pool |
| 8 | 002027 | 分众传媒 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 46.79 | 2.95 | 5.25 | 2.3 | theme_catchup_pool |
| 9 | 300475 | 香农芯创 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 36.63 | -1.49 | 1.57 | 3.06 | theme_catchup_pool |
| 10 | 000890 | 法尔胜 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 34.78 | 2.26 | 8.18 | 5.92 | theme_catchup_pool |
| 11 | 001332 | 锡装股份 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 24.79 | 1.0 | -0.51 | -1.51 | theme_catchup_pool |
| 12 | 603317 | 天味食品 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.88 | 1.21 | 9.99 | 8.78 | theme_catchup_pool |
| 13 | 002034 | 旺能环境 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.88 | 0.0 | -0.95 | -0.95 | theme_catchup_pool |
| 14 | 601828 | 美凯龙 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.49 | 0.0 | -3.7 | -3.7 | theme_catchup_pool |
| 15 | 300120 | 经纬辉开 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 24.24 | 2.09 | 4.75 | 2.66 | theme_catchup_pool |
| 16 | 600717 | 天津港 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 30.2 | 3.09 | 4.19 | 1.1 | theme_catchup_pool |
| 17 | 603399 | 永杉锂业 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.66 | -0.69 | 9.98 | 10.67 | theme_catchup_pool |
| 18 | 301007 | 德迈仕 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 20.27 | 1.27 | 3.01 | 1.74 |  |
| 19 | 601101 | 昊华能源 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 25.06 | -0.92 | 0.55 | 1.47 |  |
| 20 | 001358 | 兴欣新材 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 25.32 | -0.97 | -5.78 | -4.81 |  |
| 21 | 000599 | 青岛双星 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 24.21 | 2.41 | 6.67 | 4.26 |  |
| 22 | 002795 | 永和智控 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.11 | 2.99 | 3.56 | 0.57 |  |
| 23 | 601778 | 晶科科技 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 42.23 | -7.72 | -2.25 | 5.47 | low_open_reversal_pool |
| 24 | 600410 | 华胜天成 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 38.29 | -6.75 | -3.8 | 2.95 | low_open_reversal_pool |
| 25 | 002176 | 江特电机 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 37.17 | -5.88 | 9.98 | 15.86 | low_open_reversal_pool |
| 26 | 301308 | 江波龙 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 27.32 | -0.16 | 8.54 | 8.7 | low_open_reversal_pool |
| 27 | 600105 | 永鼎股份 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.9 | -0.54 | -3.41 | -2.87 | low_open_reversal_pool |
| 28 | 002594 | 比亚迪 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.91 | -0.01 | 3.86 | 3.87 | low_open_reversal_pool |
| 29 | 002384 | 东山精密 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.96 | -1.79 | 0.19 | 1.98 | low_open_reversal_pool |
| 30 | 300857 | 协创数据 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.59 | -2.3 | 4.13 | 6.43 | low_open_reversal_pool |

## Expected Return Proxy Top30（盘前可见字段的收益预期展示）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 601778 | 晶科科技 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 42.23 | -7.72 | -2.25 | 5.47 | low_open_reversal_pool |
| 2 | 002176 | 江特电机 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 37.17 | -5.88 | 9.98 | 15.86 | low_open_reversal_pool |
| 3 | 002594 | 比亚迪 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.91 | -0.01 | 3.86 | 3.87 | low_open_reversal_pool |
| 4 | 300769 | 德方纳米 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 37.72 | 2.86 | 20.0 | 17.14 | momentum_catchup_pool |
| 5 | 600410 | 华胜天成 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 38.29 | -6.75 | -3.8 | 2.95 | low_open_reversal_pool |
| 6 | 300476 | 胜宏科技 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 24.3 | -2.26 | 6.65 | 8.91 | low_open_reversal_pool |
| 7 | 002384 | 东山精密 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.96 | -1.79 | 0.19 | 1.98 | low_open_reversal_pool |
| 8 | 300857 | 协创数据 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.59 | -2.3 | 4.13 | 6.43 | low_open_reversal_pool |
| 9 | 301291 | 明阳电气 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -5.11 | 7.11 | 12.22 | broad_repair_momentum_pool |
| 10 | 301308 | 江波龙 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 27.32 | -0.16 | 8.54 | 8.7 | low_open_reversal_pool |
| 11 | 600105 | 永鼎股份 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 25.9 | -0.54 | -3.41 | -2.87 | low_open_reversal_pool |
| 12 | 301205 | 联特科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -17.23 | -5.33 | 11.9 | broad_repair_momentum_pool |
| 13 | 000709 | 河钢股份 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-ROTATE | high | 61.32 | 5.75 | 7.52 | 1.77 | main_attack_pool|theme_rotation_pool |
| 14 | 002081 | 金螳螂 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.44 | 10.06 | 11.5 | broad_repair_momentum_pool |
| 15 | 000938 | 紫光股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.91 | 2.66 | -0.25 | broad_repair_momentum_pool |
| 16 | 603318 | 水发燃气 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.02 | 5.59 | 6.61 | broad_repair_momentum_pool |
| 17 | 002851 | 麦格米特 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 4.66 | 10.0 | 5.34 | broad_repair_momentum_pool |
| 18 | 002506 | 协鑫集成 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -6.9 | -1.38 | 5.52 |  |
| 19 | 002428 | 云南锗业 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.54 | -0.66 | 0.88 | broad_repair_momentum_pool |
| 20 | 300308 | 中际旭创 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.06 | 2.55 | 3.61 | broad_repair_momentum_pool |
| 21 | 002463 | 沪电股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.86 | 0.1 | 0.96 | broad_repair_momentum_pool |
| 22 | 603501 | 豪威集团 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.0 | -2.21 | -1.21 | broad_repair_momentum_pool |
| 23 | 600028 | 中国石化 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.93 | 1.31 | 0.38 | broad_repair_momentum_pool |
| 24 | 600111 | 北方稀土 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 39.91 | 2.55 | 10.0 | 7.45 | theme_catchup_pool |
| 25 | 002655 | 共达电声 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.4 | 10.0 | 7.6 | broad_repair_momentum_pool |
| 26 | 600186 | 莲花控股 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.1 | 9.96 | 9.86 | broad_repair_momentum_pool |
| 27 | 300323 | 华灿光电 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 4.91 | 6.67 | 1.76 | broad_repair_momentum_pool |
| 28 | 300475 | 香农芯创 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 36.63 | -1.49 | 1.57 | 3.06 | theme_catchup_pool |
| 29 | 002517 | 恺英网络 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.2 | 5.2 | 3.0 |  |
| 30 | 600160 | 巨化股份 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.66 | 1.66 | 0.0 | broad_repair_momentum_pool |

