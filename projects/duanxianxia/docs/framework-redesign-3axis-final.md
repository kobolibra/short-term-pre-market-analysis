# 盘前框架三轴重构 — 定稿（peer round-2 答复 × 我方辩证裁决）

> 前置：两边数据口径不同，peer 不了解我方具体字段。以下每条均对照 canonical-field-dictionary 做了裁决，不盲信。
> 已定案：bidStrength 分母 = FF（自由流通），bidStrength = 竞价成交额 / FF。

## 逐条裁决

### 1. amount_920 背离 — [采纳方向，但先验 raw]
- peer：70% 口径差 + 30% 条件性；全池别当主因子；子池条件因子。
- 我方现实：A5 fengdan amount_915/920/925 语义本身是 OPEN（委买 vs 成交未定）；全池 IC 负。
- **裁决**：全池降级为**活跃度辅助因子**，不当委买强度因子；子池测试可行；但“昨涨停 open_num>0 分歧”子池**我方缺 per-stock open_num**，只能用状态=炸二元近似。需查 raw 确定委买/成交后再谈对齐。

### 2. 阶梯分组三维拆分 — [采纳，并纠正我上轮错误]
- 纠正：上轮我把“状态=炸”同时当 origin 与 outcome，是混维度。
- `target_bucket`(T日)：首板/1进2/2进3/**3plus 合并**（采纳坑2）— ztpool 分组名直给，✅可建。
- `outcome`(T日)：sealed/broken/failed — ✅可建。
- `source_bucket`(**只 T-1**，采纳坑1)：broken_prev/sealed_prev/non_limitup_prev — **我方做不到四分**（缺 per-stock 开板/一字标，仅 aggregate yizi_count + 二元炸）→ 降级粗桶三分。
- 坑3（互斥+维度拆开，状态/题材单独挂）：采纳。

### 3. 题材双轴（sync + persist） — [采纳，sync 换数据源重建]
- peer：theme_sync(竞价高开一致性，R0)、theme_persist(涨停宽度/资金，R1/R2)，两轴都做看 IC。
- 我方现实：kaipan.plate.summary 无“高开≥3%成员数”口径。
- **裁决**：theme_sync 不用 summary，改用 **per-stock concept + 竞价涨幅≥3%，按题材聚合**重建（等价口径、不同来源，✅可建）；theme_persist 用 kaipan 涨停数/主力资金 + ltgd。P0a 先上 theme_sync。

### 4. 环境软分数 expanding(min15) — [采纳，重要修正我方 P0b]
- peer：20 天全冷下分位只是“冷市内部排强弱”，不是跨周期冷热 → 只当**软特征**，不做硬门槛/策略分叉。
- **裁决**：纠正我上轮“用分位派生 regime 标签替掉 v7.1”的计划。market_env_score 仅作连续软特征（expanding min15, z-score/分位），**不驱动任何 hot/cold 硬路由**；离散 regime 推迟到历史补齐。

### 5. 龙头权重先验>等权 — [采纳原则，但剔除“承接”维度]
- 辩证：peer 5 维里“承接”在我方**无干净盘前代理**（盘后才实现，当盘前特征=未来函数）。
- **裁决**：起步剔除承接，只用 4 维，把 0.30/0.25/0.20/0.15 去承接后重归一 → **主线 0.33 / 板位 0.28 / 竞价 0.22 / 核心度 0.17**；承接改由 R1/R2 评估。核心度用 ltgd 板块内排名近似。

## 定稿改造顺序

- **P0a（不依赖长历史，立即可做、不受全冷污染）**：
  - 收益轴 R0/R1/R2（由 dailyline 取后续交易日），按打法拆；hot_open/trend 看 R0，weak_to_strong/leader 看 R1/R2。
  - 复合排序：**bidStrength(FF) + 换手率 + 量比(vratio.item11) + 跳空/竞价涨幅**（与本地 0055 comp_SD 一致）；分池排序。
  - 首板打板买入价改涨停价(+滑点)；拆命中率(封/成/炸)+收益率。
  - theme_sync（concept 聚合竞价涨幅≥3%）作为主线同步轴。
- **P0b（软使用）**：market_env_score 连续软分数（expanding min15），仅作特征，不做硬开关。
- **P1**：theme_persist（涨停宽度/资金）+ 阶梯三维标签（source粗桶/target/outcome）+ 龙头 4 维复合分。
- **P2**：历史竞价库补齐前，不对 S2/S4/龙头下最终统计结论；不上离散 regime 路由与动态切换门槛。

## 仍需核实/缺口（自查或回传）
- amount_915/920/925 raw 语义（委买/成交/是否限涨停价位）— 自查 raw。
- per-stock open_num 计数缺失 — 子池划分与 source 四分受限。
- 承接的盘前干净代理是否存在 — 否则永远只能当 outcome(R1/R2)。
