# 121544_analysis_v7_3.json 全量候选摘要

- source_report: `121544_analysis_v7_3.json`
- version: `premarket_v7_3`
- date_t0: `2026-05-19`
- generated_at: `2026-05-19T12:15:43+08:00`
- candidate_count: `215`
- regime: `{'label': 'normal', 'reason': 'qx=32.0, lbbx=4.08, ztbx=1.74, breadth=0.4339457567804024', 'qx_t0': 32.0, 'qx_t1': 30.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 1984.0, 'xd_t0': 2588.0, 'breadth_t0': 0.4339457567804024, 'lbbx_t0': 4.08, 'lbbx_t1': 4.76, 'ztbx_t0': 1.74, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']}`
- regime reason: `qx=32.0, lbbx=4.08, ztbx=1.74, breadth=0.4339457567804024`

## action_stats

- `AUCTION_FOLLOW`: `1`
- `MOMENTUM_CATCHUP`: `2`
- `THEME_CATCHUP`: `18`
- `LOW_OPEN_REVERSAL`: `8`
- `BROAD_REPAIR_MOMENTUM`: `28`
- `BOARD_WATCH`: `4`
- `CONFIRMATION_WATCH`: `11`
- `FAKE_STRENGTH_WATCH`: `3`
- `HIGH_COST_REPAIR_WATCH`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `17`
- `AVOID`: `10`
- `DEBUG_ONLY`: `112`

## action_quality_stats

- `AUCTION_FOLLOW:main_attack`: `1`
- `MOMENTUM_CATCHUP:momentum`: `2`
- `THEME_CATCHUP:strong`: `3`
- `THEME_CATCHUP:medium`: `5`
- `THEME_CATCHUP:weak`: `10`
- `LOW_OPEN_REVERSAL:repair`: `8`
- `BROAD_REPAIR_MOMENTUM:broad_repair`: `28`
- `BOARD_WATCH:watch_only`: `4`
- `CONFIRMATION_WATCH:watch`: `10`
- `CONFIRMATION_WATCH:high_cost_watch`: `1`
- `FAKE_STRENGTH_WATCH:repair_watch`: `3`
- `HIGH_COST_REPAIR_WATCH:high_cost_repair_watch`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE:soft_avoid`: `17`
- `AVOID:hard_avoid`: `10`
- `DEBUG_ONLY:debug`: `112`

## pool_performance

- `AUCTION_FOLLOW`: `{"count": 1, "with_performance": 1, "avg_close_pct": 9.99, "med_close_pct": 9.99, "avg_excess_return": 3.29, "med_excess_return": 3.29, "positive_excess_count": 1, "negative_excess_count": 0}`
- `MOMENTUM_CATCHUP`: `{"count": 2, "with_performance": 2, "avg_close_pct": 4.49, "med_close_pct": 4.49, "avg_excess_return": 2.31, "med_excess_return": 2.31, "positive_excess_count": 2, "negative_excess_count": 0}`
- `THEME_CATCHUP`: `{"count": 18, "with_performance": 18, "avg_close_pct": 1.48, "med_close_pct": 1.18, "avg_excess_return": -0.25, "med_excess_return": -0.67, "positive_excess_count": 8, "negative_excess_count": 10}`
- `LOW_OPEN_REVERSAL`: `{"count": 8, "with_performance": 8, "avg_close_pct": -1.62, "med_close_pct": -1.27, "avg_excess_return": 3.8, "med_excess_return": 1.3, "positive_excess_count": 6, "negative_excess_count": 2}`
- `BROAD_REPAIR_MOMENTUM`: `{"count": 28, "with_performance": 28, "avg_close_pct": 2.11, "med_close_pct": 0.91, "avg_excess_return": 1.02, "med_excess_return": 0.4, "positive_excess_count": 16, "negative_excess_count": 12}`
- `BOARD_WATCH`: `{"count": 4, "with_performance": 4, "avg_close_pct": 10.24, "med_close_pct": 10.0, "avg_excess_return": 0.24, "med_excess_return": 0.03, "positive_excess_count": 3, "negative_excess_count": 0}`
- `CONFIRMATION_WATCH`: `{"count": 11, "with_performance": 11, "avg_close_pct": 3.88, "med_close_pct": 2.92, "avg_excess_return": 1.78, "med_excess_return": 3.13, "positive_excess_count": 7, "negative_excess_count": 3}`
- `FAKE_STRENGTH_WATCH`: `{"count": 3, "with_performance": 3, "avg_close_pct": 0.78, "med_close_pct": 1.33, "avg_excess_return": -2.95, "med_excess_return": -0.53, "positive_excess_count": 1, "negative_excess_count": 2}`
- `HIGH_COST_REPAIR_WATCH`: `{"count": 1, "with_performance": 1, "avg_close_pct": 9.99, "med_close_pct": 9.99, "avg_excess_return": 0.0, "med_excess_return": 0.0, "positive_excess_count": 1, "negative_excess_count": 0}`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `{"count": 17, "with_performance": 17, "avg_close_pct": 3.45, "med_close_pct": 2.08, "avg_excess_return": 2.45, "med_excess_return": 1.74, "positive_excess_count": 12, "negative_excess_count": 5}`
- `AVOID`: `{"count": 10, "with_performance": 10, "avg_close_pct": 8.95, "med_close_pct": 9.96, "avg_excess_return": 0.54, "med_excess_return": -0.0, "positive_excess_count": 4, "negative_excess_count": 6}`
- `DEBUG_ONLY`: `{"count": 112, "with_performance": 110, "avg_close_pct": 2.25, "med_close_pct": 1.76, "avg_excess_return": 0.75, "med_excess_return": 0.09, "positive_excess_count": 55, "negative_excess_count": 54}`

## review_diagnostics

- `missed_winners`: `9`
- `debug_missed_winners`: `5`
- `avoid_missed_winners`: `2`
- `soft_avoid_missed_winners`: `2`
- `fake_strength_watch_winners`: `0`
- `false_positives`: `11`
- `high_cost_confirmation_failures`: `0`
- `broad_repair_winners`: `3`
- `broad_repair_false_positives`: `5`
- `high_cost_repair_watch_winners`: `0`

## review_profiles

### missed_winners

- `count`: `9`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 5], ["FAKE_STRENGTH", 4]]`
- `action_type_top`: `[["DEBUG_ONLY", 5], ["SOFT_AVOID_REPAIR_CANDIDATE", 2], ["AVOID", 2]]`
- `action_quality_top`: `[["debug", 5], ["soft_avoid", 2], ["hard_avoid", 2]]`
- `setup_v72_top`: `[["none", 9]]`
- `confidence_top`: `[["none", 9]]`
- `entry_tag_top`: `[["avoid", 4], ["low_liquidity_confirm", 3], ["normal", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 5], [">=9", 2], ["[2,5)", 1], ["[7,9)", 1]]`
- `auction_amount_bucket`: `[["<500w", 3], ["missing", 3], ["500-1000w", 2], [">=8000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 9, "min": 0.0, "p25": 1.26, "median": 1.68, "p75": 7.31, "max": 10.62, "avg": 3.94}, "auction_strength": {"count": 9, "min": 3.4, "p25": 3.58, "median": 5.39, "p75": 8.6, "max": 21.16, "avg": 7.13}, "auction_amount_wan": {"count": 6, "min": 253.0, "p25": 323.0, "median": 524.5, "p75": 819.0, "max": 39071.0, "avg": 6919.17}, "liquidity_score": {"count": 9, "min": 19.06, "p25": 20.0, "median": 20.46, "p75": 25.22, "max": 90.0, "avg": 29.88}, "theme_strength_t0": {"count": 9, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 100.0, "avg": 35.56}, "source_evidence_score": {"count": 9, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 11.82, "avg": 1.35}, "source_family_count": {"count": 9, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 1.0, "avg": 0.22}, "final_score": {"count": 9, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 9, "min": -68.08, "p25": -67.44, "median": -65.44, "p75": -39.83, "max": -18.96, "avg": -51.88}, "action_score": {"count": 9, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 8.12, "max": 76.42, "avg": 18.41}}`
- `top_names`: `["300616 尚品宅配", "301120 新特电气", "688182 灿勤科技", "688691 灿芯股份", "605218 伟时电子", "300657 弘信电子", "300976 达瑞电子", "603206 嘉环科技", "688507 索辰科技"]`

### debug_missed_winners

- `count`: `5`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 5]]`
- `action_type_top`: `[["DEBUG_ONLY", 5]]`
- `action_quality_top`: `[["debug", 5]]`
- `setup_v72_top`: `[["none", 5]]`
- `confidence_top`: `[["none", 5]]`
- `entry_tag_top`: `[["low_liquidity_confirm", 3], ["normal", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 3], ["[2,5)", 1], ["[7,9)", 1]]`
- `auction_amount_bucket`: `[["<500w", 3], ["500-1000w", 2]]`
- `numeric_stats`: `{"auction_pct": {"count": 5, "min": 1.15, "p25": 1.49, "median": 1.68, "p75": 2.41, "max": 7.31, "avg": 2.81}, "auction_strength": {"count": 5, "min": 3.4, "p25": 5.39, "median": 5.58, "p75": 8.6, "max": 9.28, "avg": 6.45}, "auction_amount_wan": {"count": 5, "min": 253.0, "p25": 323.0, "median": 488.0, "p75": 561.0, "max": 819.0, "avg": 488.8}, "liquidity_score": {"count": 5, "min": 19.06, "p25": 20.46, "median": 23.76, "p75": 25.22, "max": 30.38, "avg": 23.78}, "theme_strength_t0": {"count": 5, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 5, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.36, "max": 11.82, "avg": 2.44}, "source_family_count": {"count": 5, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 1.0, "avg": 0.4}, "final_score": {"count": 5, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 5, "min": -68.08, "p25": -67.69, "median": -67.44, "p75": -66.28, "max": -65.44, "avg": -66.99}, "action_score": {"count": 5, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}}`
- `top_names`: `["300616 尚品宅配", "301120 新特电气", "688182 灿勤科技", "688691 灿芯股份", "300976 达瑞电子"]`

### avoid_missed_winners

- `count`: `2`
- `auction_setup_type_top`: `[["FAKE_STRENGTH", 2]]`
- `action_type_top`: `[["AVOID", 2]]`
- `action_quality_top`: `[["hard_avoid", 2]]`
- `setup_v72_top`: `[["none", 2]]`
- `confidence_top`: `[["none", 2]]`
- `entry_tag_top`: `[["avoid", 2]]`
- `auction_pct_bucket`: `[[">=9", 2]]`
- `auction_amount_bucket`: `[[">=8000w", 1], ["missing", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 2, "min": 9.52, "p25": 9.52, "median": 10.07, "p75": 10.62, "max": 10.62, "avg": 10.07}, "auction_strength": {"count": 2, "min": 3.58, "p25": 3.58, "median": 12.37, "p75": 21.16, "max": 21.16, "avg": 12.37}, "auction_amount_wan": {"count": 1, "min": 39071.0, "p25": 39071.0, "median": 39071.0, "p75": 39071.0, "max": 39071.0, "avg": 39071.0}, "liquidity_score": {"count": 2, "min": 20.0, "p25": 20.0, "median": 55.0, "p75": 90.0, "max": 90.0, "avg": 55.0}, "theme_strength_t0": {"count": 2, "min": 20.0, "p25": 20.0, "median": 60.0, "p75": 100.0, "max": 100.0, "avg": 60.0}, "source_evidence_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 2, "min": -52.4, "p25": -52.4, "median": -46.11, "p75": -39.83, "max": -39.83, "avg": -46.11}, "action_score": {"count": 2, "min": 74.84, "p25": 74.84, "median": 75.63, "p75": 76.42, "max": 76.42, "avg": 75.63}}`
- `top_names`: `["300657 弘信电子", "688507 索辰科技"]`

### soft_avoid_missed_winners

- `count`: `2`
- `auction_setup_type_top`: `[["FAKE_STRENGTH", 2]]`
- `action_type_top`: `[["SOFT_AVOID_REPAIR_CANDIDATE", 2]]`
- `action_quality_top`: `[["soft_avoid", 2]]`
- `setup_v72_top`: `[["none", 2]]`
- `confidence_top`: `[["none", 2]]`
- `entry_tag_top`: `[["avoid", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 2]]`
- `auction_amount_bucket`: `[["missing", 2]]`
- `numeric_stats`: `{"auction_pct": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.63, "p75": 1.26, "max": 1.26, "avg": 0.63}, "auction_strength": {"count": 2, "min": 3.58, "p25": 3.58, "median": 3.58, "p75": 3.58, "max": 3.58, "avg": 3.58}, "auction_amount_wan": {"count": 0}, "liquidity_score": {"count": 2, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "theme_strength_t0": {"count": 2, "min": 20.0, "p25": 20.0, "median": 50.0, "p75": 80.0, "max": 80.0, "avg": 50.0}, "source_evidence_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "source_family_count": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "final_score": {"count": 2, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 2, "min": -20.76, "p25": -20.76, "median": -19.86, "p75": -18.96, "max": -18.96, "avg": -19.86}, "action_score": {"count": 2, "min": 6.32, "p25": 6.32, "median": 7.22, "p75": 8.12, "max": 8.12, "avg": 7.22}}`
- `top_names`: `["605218 伟时电子", "603206 嘉环科技"]`

### fake_strength_watch_winners

- empty

### broad_repair_winners

- `count`: `3`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 2], ["LOW_OPEN_WEAK", 1]]`
- `action_type_top`: `[["BROAD_REPAIR_MOMENTUM", 3]]`
- `action_quality_top`: `[["broad_repair", 3]]`
- `setup_v72_top`: `[["none", 3]]`
- `confidence_top`: `[["none", 3]]`
- `entry_tag_top`: `[["normal", 3]]`
- `auction_pct_bucket`: `[["[2,5)", 1], ["[-2,0)", 1], ["[0,2)", 1]]`
- `auction_amount_bucket`: `[["500-1000w", 1], [">=8000w", 1], ["1000-3000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 3, "min": -0.19, "p25": -0.19, "median": 0.32, "p75": 4.47, "max": 4.47, "avg": 1.53}, "auction_strength": {"count": 3, "min": 19.36, "p25": 19.36, "median": 26.69, "p75": 40.87, "max": 40.87, "avg": 28.97}, "auction_amount_wan": {"count": 3, "min": 925.0, "p25": 925.0, "median": 2774.0, "p75": 12046.0, "max": 12046.0, "avg": 5248.33}, "liquidity_score": {"count": 3, "min": 58.5, "p25": 58.5, "median": 74.0, "p75": 95.48, "max": 95.48, "avg": 75.99}, "theme_strength_t0": {"count": 3, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.09, "max": 0.09, "avg": 0.03}, "source_family_count": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 1.0, "avg": 0.33}, "final_score": {"count": 3, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 3, "min": 37.67, "p25": 37.67, "median": 46.72, "p75": 50.43, "max": 50.43, "avg": 44.94}, "action_score": {"count": 3, "min": 31.59, "p25": 31.59, "median": 61.02, "p75": 61.21, "max": 61.21, "avg": 51.27}}`
- `top_names`: `["688360 德马科技", "002281 光迅科技", "300259 新天科技"]`

### broad_repair_false_positives

- `count`: `5`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 4], ["LOW_OPEN_WEAK", 1]]`
- `action_type_top`: `[["BROAD_REPAIR_MOMENTUM", 5]]`
- `action_quality_top`: `[["broad_repair", 5]]`
- `setup_v72_top`: `[["none", 5]]`
- `confidence_top`: `[["none", 5]]`
- `entry_tag_top`: `[["normal", 5]]`
- `auction_pct_bucket`: `[["[0,2)", 2], ["[2,5)", 2], ["[-2,0)", 1]]`
- `auction_amount_bucket`: `[["3000-8000w", 2], ["1000-3000w", 2], ["500-1000w", 1]]`
- `numeric_stats`: `{"auction_pct": {"count": 5, "min": -1.5, "p25": 1.09, "median": 1.12, "p75": 2.41, "max": 2.61, "avg": 1.15}, "auction_strength": {"count": 5, "min": 13.36, "p25": 19.17, "median": 20.02, "p75": 31.05, "max": 36.2, "avg": 23.96}, "auction_amount_wan": {"count": 5, "min": 999.0, "p25": 1086.0, "median": 1260.0, "p75": 4311.0, "max": 4397.0, "avg": 2410.6}, "liquidity_score": {"count": 5, "min": 33.98, "p25": 39.2, "median": 61.72, "p75": 74.0, "max": 100.0, "avg": 61.78}, "theme_strength_t0": {"count": 5, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 20.0, "max": 20.0, "avg": 20.0}, "source_evidence_score": {"count": 5, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.13, "max": 0.44, "avg": 0.11}, "source_family_count": {"count": 5, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 1.0, "max": 1.0, "avg": 0.4}, "final_score": {"count": 5, "min": 0.0, "p25": 0.0, "median": 0.0, "p75": 0.0, "max": 0.0, "avg": 0.0}, "expected_return_score": {"count": 5, "min": 36.8, "p25": 38.09, "median": 38.76, "p75": 49.37, "max": 50.43, "avg": 42.69}, "action_score": {"count": 5, "min": 27.42, "p25": 33.56, "median": 38.16, "p75": 61.26, "max": 65.5, "avg": 45.18}}`
- `top_names`: `["603156 养元饮品", "688726 拉普拉斯", "300806 斯迪克", "301683 慧谷新材", "002297 博云新材"]`

### high_cost_repair_watch_winners

- empty

### false_positives

- `count`: `11`
- `auction_setup_type_top`: `[["GENERAL_WATCH", 9], ["LOW_OPEN_WEAK", 1], ["LOW_OPEN_REVERSAL", 1]]`
- `action_type_top`: `[["BROAD_REPAIR_MOMENTUM", 5], ["THEME_CATCHUP", 5], ["LOW_OPEN_REVERSAL", 1]]`
- `action_quality_top`: `[["broad_repair", 5], ["weak", 3], ["repair", 1], ["strong", 1], ["medium", 1]]`
- `setup_v72_top`: `[["none", 5], ["T0-GENERAL", 5], ["T0-REVERSAL", 1]]`
- `confidence_top`: `[["low", 6], ["none", 5]]`
- `entry_tag_top`: `[["normal", 9], ["low_liquidity_confirm", 2]]`
- `auction_pct_bucket`: `[["[0,2)", 5], ["[2,5)", 4], ["[-2,0)", 1], ["<-5", 1]]`
- `auction_amount_bucket`: `[["1000-3000w", 3], ["3000-8000w", 2], ["500-1000w", 2], [">=8000w", 2], ["<500w", 2]]`
- `numeric_stats`: `{"auction_pct": {"count": 11, "min": -6.46, "p25": 1.02, "median": 1.59, "p75": 2.41, "max": 3.16, "avg": 0.85}, "auction_strength": {"count": 11, "min": 3.7, "p25": 12.25, "median": 19.17, "p75": 36.2, "max": 40.23, "avg": 21.08}, "auction_amount_wan": {"count": 11, "min": 204.0, "p25": 582.0, "median": 1178.0, "p75": 4397.0, "max": 19802.0, "avg": 3966.09}, "liquidity_score": {"count": 11, "min": 18.08, "p25": 25.64, "median": 39.2, "p75": 74.0, "max": 100.0, "avg": 53.32}, "theme_strength_t0": {"count": 11, "min": 20.0, "p25": 20.0, "median": 20.0, "p75": 100.0, "max": 100.0, "avg": 55.45}, "source_evidence_score": {"count": 11, "min": 0.0, "p25": 0.0, "median": 0.13, "p75": 3.78, "max": 10.02, "avg": 1.8}, "source_family_count": {"count": 11, "min": 0.0, "p25": 0.0, "median": 1.0, "p75": 1.0, "max": 1.0, "avg": 0.55}, "final_score": {"count": 11, "min": 0.0, "p25": 0.0, "median": 26.56, "p75": 35.77, "max": 52.86, "avg": 19.88}, "expected_return_score": {"count": 11, "min": 19.14, "p25": 21.83, "median": 38.09, "p75": 49.37, "max": 57.99, "avg": 37.12}, "action_score": {"count": 11, "min": 27.42, "p25": 38.16, "median": 49.36, "p75": 63.81, "max": 75.88, "avg": 50.82}}`
- `top_names`: `["603156 养元饮品", "688726 拉普拉斯", "300806 斯迪克", "301683 慧谷新材", "002297 博云新材", "002708 光洋股份", "600118 中国卫星", "002870 香山股份", "002600 领益智造", "002048 宁波华翔", "603777 来伊份"]`

### high_cost_confirmation_failures

- empty

## candidate_pools counts

- `main_attack_pool`: 1
- `momentum_catchup_pool`: 2
- `theme_rotation_pool`: 1
- `theme_catchup_pool`: 15
- `low_open_reversal_pool`: 8
- `broad_repair_momentum_pool`: 15
- `board_watch_pool`: 4
- `confirmation_watch_pool`: 11
- `fake_strength_watch_pool`: 3
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

- `dailyline_matched`: `213 / 215`
- `avg_close_pct`: `2.74`
- `med_close_pct`: `2.0`
- `avg_excess_return`: `0.95`
- `med_excess_return`: `0.4`
- `pos_close_count`: `144/213`
- `pos_excess_count`: `116/212`

## 收盘涨幅 / 超额收益（Action Order Top30）

- `dailyline_matched`: `30 / 30`
- `avg_close_pct`: `1.09`
- `med_close_pct`: `1.18`
- `avg_excess_return`: `1.18`
- `med_excess_return`: `0.9`
- `pos_close_count`: `17/30`
- `pos_excess_count`: `18/30`

## 收盘涨幅 / 超额收益（Expected Return Proxy Top30）

- `dailyline_matched`: `30 / 30`
- `avg_close_pct`: `1.27`
- `med_close_pct`: `1.31`
- `avg_excess_return`: `2.32`
- `med_excess_return`: `1.69`
- `pos_close_count`: `17/30`
- `pos_excess_count`: `22/30`

## setup_v72 分布

- `T0-GENERAL`: `33`
- `T0-REVERSAL`: `8`
- `none`: `173`
- `T0-ROTATE`: `1`

## action_type 分布

- `AUCTION_FOLLOW`: `1`
- `MOMENTUM_CATCHUP`: `2`
- `THEME_CATCHUP`: `18`
- `LOW_OPEN_REVERSAL`: `8`
- `BROAD_REPAIR_MOMENTUM`: `28`
- `BOARD_WATCH`: `4`
- `CONFIRMATION_WATCH`: `11`
- `FAKE_STRENGTH_WATCH`: `3`
- `HIGH_COST_REPAIR_WATCH`: `1`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `17`
- `AVOID`: `10`
- `DEBUG_ONLY`: `112`

## action_quality 分布

- `main_attack`: `1`
- `momentum`: `2`
- `strong`: `3`
- `medium`: `5`
- `weak`: `10`
- `repair`: `8`
- `broad_repair`: `28`
- `watch_only`: `4`
- `watch`: `10`
- `high_cost_watch`: `1`
- `repair_watch`: `3`
- `high_cost_repair_watch`: `1`
- `soft_avoid`: `17`
- `hard_avoid`: `10`
- `debug`: `112`

## confidence 分布

- `low`: `41`
- `high`: `1`
- `none`: `173`

## auction_setup_type 分布

- `GENERAL_WATCH`: `143`
- `LOW_OPEN_WEAK`: `28`
- `LOW_OPEN_REVERSAL`: `8`
- `BOARD_LOCK_WATCH`: `2`
- `SUSTAINED_PLUS_LAST_SECOND`: `3`
- `FAKE_STRENGTH`: `31`

## Action Order Top30（交易动作顺序，不等于纯收益预测）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 600903 | 贵州燃气 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-GENERAL | low | 50.55 | 6.7 | 9.9886 | 3.2886 | main_attack_pool |
| 2 | 002580 | 圣阳股份 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 44.26 | 2.27 | 3.9698 | 1.6998 | momentum_catchup_pool |
| 3 | 300537 | 广信材料 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 47.0 | 2.09 | 5.0083 | 2.9183 | momentum_catchup_pool |
| 4 | 002196 | 方正电机 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 59.64 | 0.5 | 4.703 | 4.203 | theme_catchup_pool |
| 5 | 002600 | 领益智造 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 52.86 | 2.4 | -0.8211 | -3.2211 | theme_catchup_pool |
| 6 | 002979 | 雷赛智能 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 53.23 | -0.14 | 1.9972 | 2.1372 | theme_catchup_pool |
| 7 | 002048 | 宁波华翔 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 37.5 | 1.95 | -1.1995 | -3.1495 | theme_catchup_pool |
| 8 | 600143 | 金发科技 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 46.02 | 2.98 | 2.0654 | -0.9146 | theme_catchup_pool |
| 9 | 688590 | 新致软件 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 44.29 | 2.79 | 0.0 | -2.79 | theme_catchup_pool |
| 10 | 002195 | 岩山科技 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 40.63 | 2.62 | 3.3373 | 0.7173 | theme_catchup_pool |
| 11 | 300454 | 深信服 | THEME_CATCHUP | medium | low_cost_t0_theme_catchup | T0-GENERAL | low | 36.25 | 2.43 | 4.1157 | 1.6857 | theme_catchup_pool |
| 12 | 603051 | 鹿山新材 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 34.19 | 0.99 | 2.1127 | 1.1227 | theme_catchup_pool |
| 13 | 002870 | 香山股份 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 30.8 | 1.02 | -2.4349 | -3.4549 | theme_catchup_pool |
| 14 | 002823 | 凯中精密 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 34.19 | 0.79 | 0.3632 | -0.4268 | theme_catchup_pool |
| 15 | 603028 | 赛福天 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 32.14 | 1.27 | 0.0 | -1.27 | theme_catchup_pool |
| 16 | 688255 | 凯尔达 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 30.28 | 1.22 | 2.293 | 1.073 | theme_catchup_pool |
| 17 | 300635 | 中达安 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 29.55 | 1.38 | -1.2467 | -2.6267 | theme_catchup_pool |
| 18 | 301502 | 华阳智能 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 30.62 | 2.0 | 3.5062 | 1.5062 | theme_catchup_pool |
| 19 | 603777 | 来伊份 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.56 | 1.59 | -1.4911 | -3.0811 |  |
| 20 | 002708 | 光洋股份 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 35.16 | 3.16 | -0.6953 | -3.8553 |  |
| 21 | 603311 | 金海高科 | THEME_CATCHUP | weak | low_cost_t0_theme_catchup | T0-GENERAL | low | 34.66 | 2.24 | 9.9961 | 7.7561 |  |
| 22 | 600396 | 华电辽能 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 43.65 | -9.82 | 2.9318 | 12.7518 | low_open_reversal_pool |
| 23 | 002081 | 金螳螂 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 42.48 | -4.53 | -2.4045 | 2.1255 | low_open_reversal_pool |
| 24 | 002181 | 粤传媒 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | high | 53.28 | -7.62 | -9.9887 | -2.3687 | low_open_reversal_pool |
| 25 | 603773 | 沃格光电 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 27.52 | -9.04 | 4.8007 | 13.8407 | low_open_reversal_pool |
| 26 | 002407 | 多氟多 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 40.54 | -2.04 | -1.6142 | 0.4258 | low_open_reversal_pool |
| 27 | 600118 | 中国卫星 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 35.77 | -6.46 | -9.9979 | -3.5379 | low_open_reversal_pool |
| 28 | 002428 | 云南锗业 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 29.37 | -1.4 | -0.9296 | 0.4704 | low_open_reversal_pool |
| 29 | 600584 | 长电科技 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 28.61 | -2.43 | 4.2517 | 6.6817 | low_open_reversal_pool |
| 30 | 600667 | 太极实业 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.6 | 0.0764 | 1.6764 | broad_repair_momentum_pool |

## Expected Return Proxy Top30（盘前可见字段的收益预期展示）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 600396 | 华电辽能 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 43.65 | -9.82 | 2.9318 | 12.7518 | low_open_reversal_pool |
| 2 | 002081 | 金螳螂 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 42.48 | -4.53 | -2.4045 | 2.1255 | low_open_reversal_pool |
| 3 | 603773 | 沃格光电 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 27.52 | -9.04 | 4.8007 | 13.8407 | low_open_reversal_pool |
| 4 | 300537 | 广信材料 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 47.0 | 2.09 | 5.0083 | 2.9183 | momentum_catchup_pool |
| 5 | 002407 | 多氟多 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 40.54 | -2.04 | -1.6142 | 0.4258 | low_open_reversal_pool |
| 6 | 002181 | 粤传媒 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | high | 53.28 | -7.62 | -9.9887 | -2.3687 | low_open_reversal_pool |
| 7 | 600118 | 中国卫星 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 35.77 | -6.46 | -9.9979 | -3.5379 | low_open_reversal_pool |
| 8 | 002428 | 云南锗业 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 29.37 | -1.4 | -0.9296 | 0.4704 | low_open_reversal_pool |
| 9 | 002580 | 圣阳股份 | MOMENTUM_CATCHUP | momentum | strong_auction_momentum_incomplete_theme_or_source | T0-GENERAL | low | 44.26 | 2.27 | 3.9698 | 1.6998 | momentum_catchup_pool |
| 10 | 600584 | 长电科技 | LOW_OPEN_REVERSAL | repair | low_open_repair_with_premarket_support | T0-REVERSAL | low | 28.61 | -2.43 | 4.2517 | 6.6817 | low_open_reversal_pool |
| 11 | 002971 | 和远气体 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -6.65 | -5.1932 | 1.4568 | broad_repair_momentum_pool |
| 12 | 600667 | 太极实业 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.6 | 0.0764 | 1.6764 | broad_repair_momentum_pool |
| 13 | 002409 | 雅克科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.07 | 1.55 | 2.62 | broad_repair_momentum_pool |
| 14 | 603738 | 泰晶科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.31 | -2.7778 | -2.4678 | broad_repair_momentum_pool |
| 15 | 002297 | 博云新材 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.5 | -5.4374 | -3.9374 | broad_repair_momentum_pool |
| 16 | 002281 | 光迅科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.19 | 10.0 | 10.19 | broad_repair_momentum_pool |
| 17 | 002915 | 中欣氟材 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -1.75 | -2.3333 | -0.5833 | broad_repair_momentum_pool |
| 18 | 601138 | 工业富联 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.99 | -0.6089 | 0.3811 | broad_repair_momentum_pool |
| 19 | 603156 | 养元饮品 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.09 | -6.0241 | -7.1141 | broad_repair_momentum_pool |
| 20 | 300166 | 东方国信 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 2.62 | 9.9487 | 7.3287 | broad_repair_momentum_pool |
| 21 | 002600 | 领益智造 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 52.86 | 2.4 | -0.8211 | -3.2211 | theme_catchup_pool |
| 22 | 002979 | 雷赛智能 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 53.23 | -0.14 | 1.9972 | 2.1372 | theme_catchup_pool |
| 23 | 300259 | 新天科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.32 | 9.5238 | 9.2038 | broad_repair_momentum_pool |
| 24 | 300184 | 力源信息 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.77 | 2.9272 | 1.1572 | broad_repair_momentum_pool |
| 25 | 300623 | 捷捷微电 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.78 | 4.5194 | 2.7394 | broad_repair_momentum_pool |
| 26 | 002196 | 方正电机 | THEME_CATCHUP | strong | low_cost_t0_theme_catchup | T0-GENERAL | low | 59.64 | 0.5 | 4.703 | 4.203 | theme_catchup_pool |
| 27 | 600903 | 贵州燃气 | AUCTION_FOLLOW | main_attack | healthy_cost_auction_follow_through | T0-GENERAL | low | 50.55 | 6.7 | 9.9886 | 3.2886 | main_attack_pool |
| 28 | 002506 | 协鑫集成 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.15 | -1.1494 | -2.2994 | broad_repair_momentum_pool |
| 29 | 601778 | 晶科科技 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | -0.3 | 1.0606 | 1.3606 |  |
| 30 | 603163 | 圣晖集成 | BROAD_REPAIR_MOMENTUM | broad_repair | no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 3.62 | 10.0018 | 6.3818 |  |

