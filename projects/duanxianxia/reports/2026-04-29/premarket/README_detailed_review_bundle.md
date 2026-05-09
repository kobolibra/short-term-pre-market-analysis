# 2026-04-29 Premarket v7.2 Detailed Review Bundle

这是一份针对 `2026-04-29` 盘前新版分析结果 `201743_analysis_v7_2.json` 的详细复盘导出，目的是把**全量候选、关键打分字段、标签、中间指标、统计摘要**都摊开，便于后续一起分析“为什么选得差 / 哪里该改权重 / 哪些 setup 应该砍掉”。

## 文件清单

- `201743_analysis_v7_2.json`
  - 原始新版分析结果
  - 版本：`premarket_v7_2`

- `201743_all_candidates_ranked_list.md`
  - 308 只候选全量排名清单
  - 按 `final_score` 从高到低排列
  - 每行附带核心字段摘要

- `201743_all_candidates_flat.csv`
  - 308 只候选的扁平化明细表
  - 适合 Excel / Numbers / pandas / DuckDB 直接筛选分析

- `201743_all_candidates_flat.jsonl`
  - 308 只候选的 JSONL 导出
  - 保留较多结构化字段，适合程序继续处理

- `201743_analysis_summary.md`
  - 全局统计摘要
  - 包含 setup 分布、confidence 分布、auction 形态分布、题材分布、Top30 摘要

- `201743_analysis_field_catalog.md`
  - 字段字典与复盘说明
  - 说明顶层结构、嵌套字段、`meta/regime`、candidate pools 等

## 本次结果的几个关键现象

- 总候选数：`308`
- `setup_v72` 分布：
  - `none`: 266
  - `T0-GENERAL`: 32
  - `T0-REVERSAL`: 9
  - `T0-ROTATE`: 1
- `confidence` 分布：
  - `high`: 1
  - `low`: 41
  - `none`: 266
- 市场环境：`cold`
- 主要竞价形态分布：
  - `GENERAL_WATCH`: 212
  - `FAKE_STRENGTH`: 44
  - `LOW_OPEN_WEAK`: 39
  - `LOW_OPEN_REVERSAL`: 9

这些特征说明：

1. 真正强确认型候选非常少；
2. 大量样本停留在观察/弱确认层；
3. 这份导出更适合拿来做“失败样本与成功样本切片”，而不是只看 Top10 就下结论。

## 建议的后续分析方向

1. 单独分析 `T0-REVERSAL` 的胜率和回撤特征
2. 检查 `confidence=low` 却高排名的票，是否被题材强度过度拉分
3. 检查 `auction_hits_count / source_family_count / qiangchou_last_second_rank` 与收盘表现的关系
4. 对比成功票与失败票在以下字段上的共性：
   - `auction_latest_change_pct`
   - `auction_turnover_pct`
   - `auction_source_evidence_score`
   - `theme_strength_t0`
   - `label_tech_profile`
   - `stock_t1_label`
   - `entry_tag`

## 备注

本目录中的文件是为了复盘与模型改进导出的“详细结果包”，并不是生产链路正式输入。后续如果继续做切片分析，建议基于 `201743_all_candidates_flat.csv` 继续扩展对照列，例如补入当日收盘涨跌幅、开盘涨跌幅、分组统计等。

## 新增绩效补充列

- `auction_pct`: 竞价涨幅（采用 `auction_detail.latest_change_pct`）
- `open_pct`: 当日开盘相对昨收涨幅
- `close_pct`: 当日收盘相对昨收涨幅
- `excess_return`: 对齐项目 review backfill 口径，按 `close_pct - auction_pct` 计算的超额收益
