# 145419_analysis_v7_3.json field catalog

- `version`: 报告版本，应为 `premarket_v7_3`
- `action_type`: 动作分层类型
- `action_quality`: v7.3 动作质量分层；描述盘前信号质量，不是事后收益质量
- `signal_quality`: `action_quality` 的语义化别名，强调其为盘前信号质量
- `action_reason`: 动作归类原因
- `action_score`: 动作评分
- `action_priority`: 动作优先级；用于 action-order，不等于收益预测排序
- `action_confidence`: 动作置信级别
- `action_tags`: 动作附加标签，例如 `high_cost_confirmation` / `needs_intraday_repair`
- `expected_return_candidates`: 用盘前可见字段生成的展示型收益预期排序，不使用收盘收益
- `expected_return_watch_tier`: expected-return proxy 的观察层
- `all_candidates_expected_return_ranked`: 全量 expected-return proxy 排序
- `pool_performance`: 池级表现摘要，仅复盘使用
- `review_diagnostics`: 复盘诊断列表，包括 missed/false-positive/high-cost failure
- `review_profiles`: missed winners / false positives 的字段画像，用于发现下一轮规则共性
- `soft_avoid_repair_pool`: 非盘前交易池；用于避免 moderate avoid 被误称 hard avoid
- `auction_pct`: 竞价涨幅
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return`: `close_pct - auction_pct`
- `anchors`: 盘中观察锚点文本拼接

