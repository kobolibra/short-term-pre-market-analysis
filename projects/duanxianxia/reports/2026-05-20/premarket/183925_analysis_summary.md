# 183925_analysis_v7_3.json 全量候选摘要

- source_report: `183925_analysis_v7_3.json`
- version: `premarket_v7_3`
- date_t0: `2026-05-20`
- generated_at: `2026-05-20T18:39:24+08:00`
- candidate_count: `213`
- regime: `{'label': 'cold', 'reason': 'qx=27.0, dt=4.0, kqxy=0.0, breadth=0.21292460646230324', 'qx_t0': 27.0, 'qx_t1': 32.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 1028.0, 'xd_t0': 3800.0, 'breadth_t0': 0.21292460646230324, 'lbbx_t0': 3.41, 'lbbx_t1': 4.08, 'ztbx_t0': 1.65, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']}`
- regime reason: `qx=27.0, dt=4.0, kqxy=0.0, breadth=0.21292460646230324`

## action_stats

- `DYNAMIC_THEME_REPAIR`: `1`
- `LOW_COST_20CM_ELASTIC`: `15`
- `LOW_COST_ELASTIC_CATCHUP`: `19`
- `LOW_AMOUNT_VRATIO_ELASTIC`: `1`
- `CONFIRMATION_WATCH`: `18`
- `THEME_CATCHUP_CONFIRMATION`: `1`
- `GENERIC_REPAIR_WATCH`: `2`
- `DEEP_LOW_OPEN_WATCH`: `3`
- `STRUCTURAL_AVOID`: `4`
- `RETREAT_OR_HIGH_COST_WATCH`: `33`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `29`
- `DEBUG_ONLY`: `76`
- `QUALITY_WATCH`: `7`
- `HIGH_CONVICTION_BUY`: `4`

## action_quality_stats

- `DYNAMIC_THEME_REPAIR:dynamic_theme_repair`: `1`
- `LOW_COST_20CM_ELASTIC:low_cost_20cm_elastic`: `15`
- `LOW_COST_ELASTIC_CATCHUP:low_cost_elastic`: `19`
- `LOW_AMOUNT_VRATIO_ELASTIC:low_amount_vratio_elastic`: `1`
- `CONFIRMATION_WATCH:watch`: `18`
- `THEME_CATCHUP_CONFIRMATION:watch_only`: `1`
- `GENERIC_REPAIR_WATCH:watch_only`: `2`
- `DEEP_LOW_OPEN_WATCH:watch_only`: `3`
- `STRUCTURAL_AVOID:avoid`: `4`
- `RETREAT_OR_HIGH_COST_WATCH:watch_only`: `33`
- `SOFT_AVOID_REPAIR_CANDIDATE:soft_avoid`: `29`
- `DEBUG_ONLY:debug`: `76`
- `QUALITY_WATCH:watch_only`: `7`
- `HIGH_CONVICTION_BUY:broad_repair`: `4`

## pool_performance

- `HIGH_CONVICTION_BUY`: `{"count": 4, "with_performance": 0}`
- `CONFIRMATION_WATCH`: `{"count": 18, "with_performance": 0}`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `{"count": 29, "with_performance": 0}`
- `QUALITY_WATCH`: `{"count": 7, "with_performance": 0}`
- `STRUCTURAL_AVOID`: `{"count": 4, "with_performance": 0}`
- `LOW_AMOUNT_VRATIO_ELASTIC`: `{"count": 1, "with_performance": 0}`
- `DEEP_LOW_OPEN_WATCH`: `{"count": 3, "with_performance": 0}`
- `DYNAMIC_THEME_REPAIR`: `{"count": 1, "with_performance": 0}`
- `LOW_COST_20CM_ELASTIC`: `{"count": 15, "with_performance": 0}`
- `GENERIC_REPAIR_WATCH`: `{"count": 2, "with_performance": 0}`
- `RETREAT_OR_HIGH_COST_WATCH`: `{"count": 33, "with_performance": 0}`
- `THEME_CATCHUP_CONFIRMATION`: `{"count": 1, "with_performance": 0}`
- `LOW_COST_ELASTIC_CATCHUP`: `{"count": 19, "with_performance": 0}`
- `DEBUG_ONLY`: `{"count": 76, "with_performance": 0}`

## review_diagnostics

- `missed_winners`: `0`
- `debug_missed_winners`: `0`
- `avoid_missed_winners`: `0`
- `soft_avoid_missed_winners`: `0`
- `fake_strength_watch_winners`: `0`
- `false_positives`: `0`
- `high_cost_confirmation_failures`: `0`
- `quality_gate_rejected_winners`: `0`
- `selective_buy_false_positives`: `0`

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

- `main_attack_pool`: 0
- `momentum_catchup_pool`: 0
- `theme_rotation_pool`: 0
- `theme_catchup_pool`: 0
- `low_open_reversal_pool`: 0
- `board_watch_pool`: 0
- `confirmation_watch_pool`: 15
- `fake_strength_watch_pool`: 0
- `soft_avoid_repair_pool`: 15
- `avoid_or_risk_pool`: 0
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

- `dailyline_matched`: `0 / 4`
- `avg_close_pct`: `None`
- `med_close_pct`: `None`
- `avg_excess_return`: `None`
- `med_excess_return`: `None`
- `pos_close_count`: `None/0`
- `pos_excess_count`: `None/0`

## 收盘涨幅 / 超额收益（Expected Return Proxy Top30）

- `dailyline_matched`: `0 / 4`
- `avg_close_pct`: `None`
- `med_close_pct`: `None`
- `avg_excess_return`: `None`
- `med_excess_return`: `None`
- `pos_close_count`: `None/0`
- `pos_excess_count`: `None/0`

## setup_v72 分布

- `none`: `173`
- `T0-GENERAL`: `29`
- `T0-REVERSAL`: `11`

## action_type 分布

- `HIGH_CONVICTION_BUY`: `4`
- `QUALITY_WATCH`: `7`
- `CONFIRMATION_WATCH`: `18`
- `SOFT_AVOID_REPAIR_CANDIDATE`: `29`
- `STRUCTURAL_AVOID`: `4`
- `RETREAT_OR_HIGH_COST_WATCH`: `33`
- `LOW_COST_20CM_ELASTIC`: `15`
- `DEEP_LOW_OPEN_WATCH`: `3`
- `LOW_COST_ELASTIC_CATCHUP`: `19`
- `GENERIC_REPAIR_WATCH`: `2`
- `LOW_AMOUNT_VRATIO_ELASTIC`: `1`
- `DYNAMIC_THEME_REPAIR`: `1`
- `THEME_CATCHUP_CONFIRMATION`: `1`
- `DEBUG_ONLY`: `76`

## action_quality 分布

- `broad_repair`: `4`
- `watch_only`: `46`
- `watch`: `18`
- `soft_avoid`: `29`
- `avoid`: `4`
- `low_cost_20cm_elastic`: `15`
- `low_cost_elastic`: `19`
- `low_amount_vratio_elastic`: `1`
- `dynamic_theme_repair`: `1`
- `debug`: `76`

## confidence 分布

- `none`: `173`
- `low`: `39`
- `high`: `1`

## auction_setup_type 分布

- `LOW_OPEN_WEAK`: `62`
- `GENERAL_WATCH`: `91`
- `LOW_OPEN_REVERSAL`: `11`
- `FAKE_STRENGTH`: `40`
- `BOARD_LOCK_WATCH`: `6`
- `SUSTAINED_PLUS_LAST_SECOND`: `3`

## Action Order Top30（交易动作顺序，不等于纯收益预测）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 300166 | 东方国信 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.35 |  |  | selective_buy_pool|conviction_leaderboard |
| 2 | 002081 | 金螳螂 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.14 |  |  | selective_buy_pool|conviction_leaderboard |
| 3 | 002709 | 天赐材料 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.81 |  |  | selective_buy_pool|conviction_leaderboard |
| 4 | 688233 | 神工股份 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.25 |  |  | selective_buy_pool|conviction_leaderboard |

## Expected Return Proxy Top30（盘前可见字段的收益预期展示）

| rank | code | name | action_type | action_quality | action_reason | setup | conf | final | auction_pct | close_pct | excess_return | pool_hint |
|---:|---|---|---|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 300166 | 东方国信 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.35 |  |  | selective_buy_pool|conviction_leaderboard |
| 2 | 002081 | 金螳螂 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.14 |  |  | selective_buy_pool|conviction_leaderboard |
| 3 | 002709 | 天赐材料 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 0.81 |  |  | selective_buy_pool|conviction_leaderboard |
| 4 | 688233 | 神工股份 | HIGH_CONVICTION_BUY | broad_repair | BUY:BROAD_REPAIR_MOMENTUM:no_theme_no_source_broad_repair_momentum | none | none | 0.0 | 1.25 |  |  | selective_buy_pool|conviction_leaderboard |

