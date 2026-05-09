# 111108_analysis_v7_2.json 全量候选摘要

- source_report: `111108_analysis_v7_2.json`
- date_t0: `2026-04-29`
- generated_at: `2026-05-09T11:11:08+08:00`
- candidate_count: `308`
- regime: `cold`
- regime reason: `qx=20.0, dt=11.0, kqxy=0.0, breadth=0.27373612823674476`

## action_stats

- `AUCTION_FOLLOW`: 1
- `THEME_CATCHUP`: 20
- `CONFIRMATION_WATCH`: 227
- `BOARD_WATCH`: 7
- `LOW_OPEN_REVERSAL`: 9
- `AVOID`: 44

## candidate_pools counts

- `main_attack_pool`: 1
- `theme_rotation_pool`: 1
- `theme_catchup_pool`: 15
- `low_open_reversal_pool`: 9
- `board_watch_pool`: 7
- `confirmation_watch_pool`: 15
- `avoid_or_risk_pool`: 15

## 绩效补充口径

- `auction_pct`: 使用 `auction_detail.latest_change_pct`
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

## 收盘涨幅 / 超额收益（Top30）

- `avg_close_pct_top30`: `5.97`
- `med_close_pct_top30`: `6.42`
- `avg_excess_return_top30`: `4.23`
- `med_excess_return_top30`: `3.0`
- `pos_close_count_top30`: `24/30`
- `pos_excess_count_top30`: `23/30`

## setup_v72 分布

- `none`: 266
- `T0-GENERAL`: 32
- `T0-REVERSAL`: 9
- `T0-ROTATE`: 1

## action_type 分布

- `CONFIRMATION_WATCH`: 227
- `AVOID`: 44
- `THEME_CATCHUP`: 20
- `LOW_OPEN_REVERSAL`: 9
- `BOARD_WATCH`: 7
- `AUCTION_FOLLOW`: 1

## confidence 分布

- `none`: 266
- `low`: 41
- `high`: 1

## auction_setup_type 分布

- `GENERAL_WATCH`: 212
- `FAKE_STRENGTH`: 44
- `LOW_OPEN_WEAK`: 39
- `LOW_OPEN_REVERSAL`: 9
- `SUSTAINED_PLUS_LAST_SECOND`: 3
- `BOARD_LOCK_WATCH`: 1

## Top 30 候选（含动作 / 收盘涨幅 / 超额收益）

| rank | code | name | action_type | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | auction_type |
|---:|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 000709 | 河钢股份 | AUCTION_FOLLOW | healthy_cost_auction_follow_through | T0-ROTATE | high | 61.32 | 5.75 | 7.52 | 1.77 | SUSTAINED_PLUS_LAST_SECOND |
| 2 | 002027 | 分众传媒 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 46.79 | 2.95 | 5.25 | 2.3 | GENERAL_WATCH |
| 3 | 000651 | 格力电器 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 46.35 | 4.03 | 5.28 | 1.25 | GENERAL_WATCH |
| 4 | 002867 | 周大生 | BOARD_WATCH | near_limit_or_locked_board_watch | T0-GENERAL | low | 44.9 | 9.62 | 10.03 | 0.41 | GENERAL_WATCH |
| 5 | 300067 | 安诺其 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 43.45 | -3.65 | -3.78 | -0.13 | LOW_OPEN_WEAK |
| 6 | 601778 | 晶科科技 | LOW_OPEN_REVERSAL | low_open_repair_with_premarket_support | T0-REVERSAL | low | 42.23 | -7.72 | -2.25 | 5.47 | LOW_OPEN_REVERSAL |
| 7 | 600338 | 西藏珠峰 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 40.76 | 0.56 | 10.0 | 9.44 | GENERAL_WATCH |
| 8 | 600111 | 北方稀土 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 39.91 | 2.55 | 10.0 | 7.45 | GENERAL_WATCH |
| 9 | 002352 | 顺丰控股 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 39.19 | 1.03 | -0.49 | -1.52 | GENERAL_WATCH |
| 10 | 600410 | 华胜天成 | LOW_OPEN_REVERSAL | low_open_repair_with_premarket_support | T0-REVERSAL | low | 38.29 | -6.75 | -3.8 | 2.95 | LOW_OPEN_REVERSAL |
| 11 | 300394 | 天孚通信 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 37.76 | -1.55 | 0.42 | 1.97 | LOW_OPEN_WEAK |
| 12 | 300769 | 德方纳米 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 37.72 | 2.86 | 20.0 | 17.14 | GENERAL_WATCH |
| 13 | 002176 | 江特电机 | LOW_OPEN_REVERSAL | low_open_repair_with_premarket_support | T0-REVERSAL | low | 37.17 | -5.88 | 9.98 | 15.86 | LOW_OPEN_REVERSAL |
| 14 | 603083 | 剑桥科技 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 36.64 | -0.44 | 10.0 | 10.44 | LOW_OPEN_WEAK |
| 15 | 300475 | 香农芯创 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 36.63 | -1.49 | 1.57 | 3.06 | LOW_OPEN_WEAK |
| 16 | 000402 | 金 融 街 | BOARD_WATCH | near_limit_or_locked_board_watch | T0-GENERAL | low | 36.42 | 9.92 | 9.92 | 0.0 | GENERAL_WATCH |
| 17 | 603538 | 美诺华 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 36.09 | 1.9 | 8.62 | 6.72 | GENERAL_WATCH |
| 18 | 000425 | 徐工机械 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 35.35 | 7.67 | 2.59 | -5.08 | SUSTAINED_PLUS_LAST_SECOND |
| 19 | 000890 | 法尔胜 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 34.78 | 2.26 | 8.18 | 5.92 | GENERAL_WATCH |
| 20 | 300821 | 东岳硅材 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 32.26 | 1.21 | 5.32 | 4.11 | GENERAL_WATCH |
| 21 | 603933 | 睿能科技 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 31.17 | 5.11 | 10.02 | 4.91 | GENERAL_WATCH |
| 22 | 300632 | 光莆股份 | BOARD_WATCH | near_limit_or_locked_board_watch | T0-GENERAL | low | 30.37 | 20.01 | 20.01 | 0.0 | BOARD_LOCK_WATCH |
| 23 | 600717 | 天津港 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 30.2 | 3.09 | 4.19 | 1.1 | GENERAL_WATCH |
| 24 | 600770 | 综艺股份 | CONFIRMATION_WATCH | incomplete_or_single_factor_signal | T0-GENERAL | low | 29.49 | -4.44 | 3.0 | 7.44 | LOW_OPEN_WEAK |
| 25 | 603317 | 天味食品 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.88 | 1.21 | 9.99 | 8.78 | GENERAL_WATCH |
| 26 | 002034 | 旺能环境 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.88 | 0.0 | -0.95 | -0.95 | GENERAL_WATCH |
| 27 | 301308 | 江波龙 | LOW_OPEN_REVERSAL | low_open_repair_with_premarket_support | T0-REVERSAL | low | 27.32 | -0.16 | 8.54 | 8.7 | LOW_OPEN_REVERSAL |
| 28 | 002795 | 永和智控 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 27.11 | 2.99 | 3.56 | 0.57 | GENERAL_WATCH |
| 29 | 603399 | 永杉锂业 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.66 | -0.69 | 9.98 | 10.67 | LOW_OPEN_WEAK |
| 30 | 601828 | 美凯龙 | THEME_CATCHUP | low_cost_t0_theme_catchup | T0-GENERAL | low | 26.49 | 0.0 | -3.7 | -3.7 | GENERAL_WATCH |
