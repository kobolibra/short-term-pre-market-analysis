# 144258_analysis_v7_3.json 全量候选摘要

- source_report: `144258_analysis_v7_3.json`
- version: `premarket_v7_3`
- date_t0: `2026-04-29`
- generated_at: `2026-05-09T14:42:58+08:00`
- candidate_count: `308`
- regime: `{'label': 'cold', 'reason': 'qx=20.0, dt=11.0, kqxy=0.0, breadth=0.27373612823674476', 'qx_t0': 20.0, 'qx_t1': 20.0, 'dt_t0': 11.0, 'kqxy_t0': 0.0, 'sz_t0': 1332.0, 'xd_t0': 3534.0, 'breadth_t0': 0.27373612823674476, 'lbbx_t0': 0.2, 'lbbx_t1': 0.88, 'ztbx_t0': 0.73, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']}`
- regime reason: `None`

## action_stats

- `AUCTION_FOLLOW`: `1`
- `MOMENTUM_CATCHUP`: `1`
- `THEME_CATCHUP`: `20`
- `LOW_OPEN_REVERSAL`: `9`
- `BOARD_WATCH`: `7`
- `CONFIRMATION_WATCH`: `7`
- `FAKE_STRENGTH_WATCH`: `6`
- `AVOID`: `38`
- `DEBUG_ONLY`: `219`

## action_quality_stats

- `AUCTION_FOLLOW:main_attack`: `1`
- `MOMENTUM_CATCHUP:momentum`: `1`
- `THEME_CATCHUP:medium`: `6`
- `THEME_CATCHUP:strong`: `2`
- `THEME_CATCHUP:weak`: `12`
- `LOW_OPEN_REVERSAL:repair`: `9`
- `BOARD_WATCH:watch_only`: `7`
- `CONFIRMATION_WATCH:watch`: `7`
- `FAKE_STRENGTH_WATCH:repair_watch`: `6`
- `AVOID:avoid`: `38`
- `DEBUG_ONLY:debug`: `219`

## pool_performance

- `AUCTION_FOLLOW`: `{"count": 1, "with_performance": 1, "avg_close_pct": 7.52, "med_close_pct": 7.52, "avg_excess_return": 1.77, "med_excess_return": 1.77, "positive_excess_count": 1, "negative_excess_count": 0}`
- `MOMENTUM_CATCHUP`: `{"count": 1, "with_performance": 1, "avg_close_pct": 20.0, "med_close_pct": 20.0, "avg_excess_return": 17.14, "med_excess_return": 17.14, "positive_excess_count": 1, "negative_excess_count": 0}`
- `THEME_CATCHUP`: `{"count": 20, "with_performance": 20, "avg_close_pct": 4.08, "med_close_pct": 4.47, "avg_excess_return": 3.07, "med_excess_return": 2.48, "positive_excess_count": 15, "negative_excess_count": 5}`
- `LOW_OPEN_REVERSAL`: `{"count": 9, "with_performance": 9, "avg_close_pct": 2.65, "med_close_pct": 3.86, "avg_excess_return": 5.7, "med_excess_return": 5.47, "positive_excess_count": 8, "negative_excess_count": 1}`
- `BOARD_WATCH`: `{"count": 7, "with_performance": 5, "avg_close_pct": 11.53, "med_close_pct": 10.0, "avg_excess_return": -0.24, "med_excess_return": 0.0, "positive_excess_count": 2, "negative_excess_count": 1}`
- `CONFIRMATION_WATCH`: `{"count": 7, "with_performance": 7, "avg_close_pct": 3.74, "med_close_pct": 3.0, "avg_excess_return": 2.44, "med_excess_return": 1.97, "positive_excess_count": 5, "negative_excess_count": 2}`
- `FAKE_STRENGTH_WATCH`: `{"count": 6, "with_performance": 6, "avg_close_pct": 6.14, "med_close_pct": 7.93, "avg_excess_return": 2.7, "med_excess_return": 2.99, "positive_excess_count": 4, "negative_excess_count": 2}`
- `AVOID`: `{"count": 38, "with_performance": 38, "avg_close_pct": 3.21, "med_close_pct": 1.62, "avg_excess_return": 1.24, "med_excess_return": 0.44, "positive_excess_count": 23, "negative_excess_count": 13}`
- `DEBUG_ONLY`: `{"count": 219, "with_performance": 212, "avg_close_pct": 4.81, "med_close_pct": 4.95, "avg_excess_return": 2.79, "med_excess_return": 2.5, "positive_excess_count": 178, "negative_excess_count": 33}`

## review_diagnostics

- `missed_winners`: `20`
- `debug_missed_winners`: `17`
- `avoid_missed_winners`: `3`
- `fake_strength_watch_winners`: `0`
- `false_positives`: `2`

## candidate_pools counts

- `main_attack_pool`: 1
- `momentum_catchup_pool`: 1
- `theme_rotation_pool`: 1
- `theme_catchup_pool`: 15
- `low_open_reversal_pool`: 9
- `board_watch_pool`: 7
- `confirmation_watch_pool`: 7
- `fake_strength_watch_pool`: 6
- `avoid_or_risk_pool`: 15
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
- `avg_excess_return`: `2.73`
- `med_excess_return`: `2.41`
- `pos_close_count`: `246/299`
- `pos_excess_count`: `237/294`

## 收盘涨幅 / 超额收益（Top30）

- `avg_close_pct_top30`: `4.21`
- `med_close_pct_top30`: `4.16`
- `avg_excess_return_top30`: `4.09`
- `med_excess_return_top30`: `3.0`
- `pos_close_count_top30`: `22/30`
- `pos_excess_count_top30`: `24/30`

## setup_v72 分布

- `T0-ROTATE`: 1
- `T0-GENERAL`: 32
- `T0-REVERSAL`: 9
- `none`: 266

## action_type 分布

- `AUCTION_FOLLOW`: 1
- `MOMENTUM_CATCHUP`: 1
- `THEME_CATCHUP`: 20
- `LOW_OPEN_REVERSAL`: 9
- `BOARD_WATCH`: 7
- `CONFIRMATION_WATCH`: 7
- `FAKE_STRENGTH_WATCH`: 6
- `AVOID`: 38
- `DEBUG_ONLY`: 219

## action_quality 分布

- `main_attack`: 1
- `momentum`: 1
- `medium`: 6
- `strong`: 2
- `weak`: 12
- `repair`: 9
- `watch_only`: 7
- `watch`: 7
- `repair_watch`: 6
- `avoid`: 38
- `debug`: 219

## confidence 分布

- `high`: 1
- `low`: 41
- `none`: 266

## auction_setup_type 分布

- `SUSTAINED_PLUS_LAST_SECOND`: 3
- `GENERAL_WATCH`: 212
- `LOW_OPEN_WEAK`: 39
- `LOW_OPEN_REVERSAL`: 9
- `BOARD_LOCK_WATCH`: 1
- `FAKE_STRENGTH`: 44

## Top 30 候选（含动作 / 质量 / 收盘涨幅 / 超额收益）

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
