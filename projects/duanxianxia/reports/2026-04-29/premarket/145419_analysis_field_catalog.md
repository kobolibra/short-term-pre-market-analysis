# 145419_analysis_v7_3.json field catalog

- `version`: 报告版本，应为 `premarket_v7_3`
- `action_type`: 动作分层类型
- `action_quality`: v7.3 动作质量分层
- `action_reason`: 动作归类原因
- `action_score`: 动作评分
- `action_priority`: 动作优先级
- `action_confidence`: 动作置信级别
- `action_tags`: 动作附加标签
- `auction_pct`: 竞价涨幅
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return`: `close_pct - auction_pct`
- `anchors`: 盘中观察锚点文本拼接
- `pool_performance`: v7.3 新增池级表现摘要（源 JSON 顶层）
- `review_diagnostics`: v7.3 新增复盘诊断摘要（源 JSON 顶层）

