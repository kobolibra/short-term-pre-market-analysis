# 111108_analysis_v7_2.json 字段说明

## 源报告

- source_report: `111108_analysis_v7_2.json`
- derived_from: `all_candidates_debug`

## 新版 v7.2 新结构字段

- `action_stats`: 顶层动作分布统计
- `actionable_candidates`: 新版动作口径下的可执行候选列表
- `legacy_top_candidates`: 兼容旧口径的 Top 候选列表
- `candidate_pools.theme_catchup_pool`: 题材追涨补涨池
- `candidate_pools.low_open_reversal_pool`: 低开反转池
- `candidate_pools.board_watch_pool`: 板上观察池

## 导出补充绩效字段

- `auction_pct`: 竞价涨幅，采用 `auction_detail.latest_change_pct`
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return`: `close_pct - auction_pct`
- `dailyline_found`: 是否在 `dailyline/stocks/{code}.csv` 找到 `2026-04-29` 行

## 扁平化列分组

### 1) 基础标识 / 排名
- `code`, `name`, `trade_date`
- `setup_v72`, `action_label`, `action_reason`, `setup_v71_compat`, `confidence`
- `setup_reason`, `auction_setup_type`, `regime`, `entry_tag`, `entry_reason`
- `final_score`, `today_signal_raw`, `auction_strength`, `theme_strength_t0`, `hotness_score`
- `t1_multiplier`, `regime_multiplier`, `risk_penalty`, `risk_flag`
- `score_weight_auction`, `score_weight_theme`, `score_weight_hotness`

### 2) 当日表现补充
- `prev_close`, `day_open`, `day_high`, `day_low`, `day_close`
- `auction_pct`, `open_pct`, `close_pct`, `excess_return`, `dailyline_found`

### 3) 题材 / 板块
- `theme_best_theme`, `theme_matched`, `theme_matched_via`, `theme_matched_plate`, `theme_matched_tags`
- `theme_strength_raw`, `theme_plate_pct`, `theme_inflow_pct`, `theme_limitup_count_pct`
- `theme_yesterday_plate_rank`, `theme_history_label`, `theme_streak_days`
- `theme_no_theme_base_applied`, `theme_broad_theme_cap_applied`, `theme_ignored_fields`

### 4) 竞价细节
- `auction_alpha_score`, `auction_source_evidence_score`, `auction_price_intent_score`, `auction_money_intent_score`
- `auction_orderbook_quality_score`, `auction_resonance_score`, `auction_liquidity_score`
- `auction_risk_multiplier`, `auction_tradability_multiplier`, `auction_liquidity_multiplier`, `auction_amount_multiplier`
- `auction_amount_missing`, `auction_source_family_count`, `auction_top_rank_family_count`, `auction_source_families`, `auction_risk_flags`
- `auction_amount_wan`, `auction_turnover_pct`, `auction_turnover_state`, `auction_latest_change_pct`
- `auction_net_pressure`, `auction_amount_pressure`, `auction_vratio_rank`
- `auction_qiangchou_rank`, `auction_qiangchou_grab_rank`, `auction_qiangchou_920_925_rank`, `auction_qiangchou_last_second_rank`, `auction_qiangchou_primary_signal`
- `auction_net_amount_rank`, `auction_fengdan_rank`, `auction_fengdan_status`, `auction_fengdan_consume_type`, `auction_fengdan_behavior_reason`
- `auction_fengdan_amount_915_yi`, `auction_fengdan_amount_920_yi`, `auction_fengdan_amount_925_yi`, `auction_fengdan_ratio_920_915`, `auction_fengdan_ratio_925_920`, `auction_hits_count`

### 5) 信号 / 标签 / 风险
- `signal_matched_plate`, `signal_matched_tags`, `signal_t0_plate_strength_raw`
- `label_longtou_status`, `label_cashflow_continuity`, `label_tech_profile`
- `stock_t1_label`, `stock_t1_reason`, `stock_t1_super_ratio`, `stock_t1_main_inflow_wan`, `stock_t1_float_market_value_yi`, `stock_t1_super_ratio_3day`, `stock_t1_main_inflow_wan_3day`
- `cashflow_raw_label`, `cashflow_raw_today_wan`, `cashflow_raw_three_wan`, `cashflow_raw_five_wan`, `cashflow_raw_ten_wan`, `cashflow_raw_effective_min_wan_per_day`
- `tech_raw_label`, `tech_raw_profile`, `tech_raw_reason`, `tech_raw_ma20`, `tech_raw_vol_ma20`, `tech_raw_vol_ratio_t1`, `tech_raw_volume_ratio`, `tech_raw_distance_to_ma20`, `tech_raw_pct_to_recent_high`, `tech_raw_pct_chg_t1`, `tech_raw_churn_type`
- `risk_main_flow_wan`, `risk_heavy_outflow`, `risk_outflow_ratio`, `risk_float_market_value_yi`, `risk_outflow_method`, `risk_t1_review_context_used`

### 6) 原始 JSON 回填列
- `t1_adjustments_json`, `auction_detail_json`, `signal_summary_json`, `label_snapshot_json`, `theme_detail_json`, `risk_detail_json`, `derived_performance_json`


- `action_type`: 新版动作分层类型（如 `THEME_CATCHUP` / `LOW_OPEN_REVERSAL` / `BOARD_WATCH` 等）
- `action_reason`: 动作归类原因
- `action_score`: 动作评分
- `action_priority`: 动作优先级
- `action_confidence`: 动作置信级别
- `action_tags`: 动作附加标签
