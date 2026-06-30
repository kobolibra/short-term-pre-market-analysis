# 短线侠盘前分析 — 彻底重构详细规划 v11

> 状态：**生效中（权威）**。本文取代 v10 中关于"逐步打补丁"的执行口径，明确为 **canonical-first 彻底重构**。字段语义以 `v10-field-alignment-decisions.md` + `field-rename-map.md` 的 FINAL 结论为准，本文不改字段口径。

## 0. 决策结论

**彻底重构 = 把"解析/口径/特征/因子"四层推倒重做，建立单一事实源（canonical），让旧的 105KB/145KB 巨型脚本的解析产物不再被任何下游消费。**

它**不是**两种东西：

- 不是"修修补补"：不再在 `duanxianxia_fetcher.py` / `duanxianxia_batch.py` 里用 sed/改标签的方式逐个救字段。
- 不是"推倒一切重写"：采集层（抓取+解密+落 raw）、定时/agent_job 调度、以及已验证的因子学习成果（edge_core 权重、REGIME_ACTION_GATE、IC）是资产，保留并在新口径上复用。

## 1. 为什么这样切

- 巨型脚本无法用单文件 API 安全整体重写：会出现静默转写漂移，`py_compile` 抓不到被改动的字面量。
- 真正的病根在**解析与口径层**（单位/口径/标签错配），不在抓取层。抓取拿到的 `raw[]` 是干净的事实。
- 因此：**冻结旧解析产物 → 从 raw 用 canonical 重新派生一切**，是唯一既彻底又可验证的路径。

## 2. 目标架构（四层）

```
[L1 采集层]  fetch + AES 解密 + 落盘 raw[]（KEEP，仅做"采集完整性"补丁）
     │  捕获文件持久化 raw[]/board_state/hotlist
     ▼
[L2 口径层]  duanxianxia_canonical.py + duanxianxia_canonical_routing.py（DONE，单一事实源）
     │  raw_row → 规范字段名 + 规范单位（万/亿→元，pct/ratio 透传）
     ▼
[L3 特征层]  ★新建 feature builder（REBUILD，替代 transform-2 错配）
     │  读持久化 captures → canonicalize_rows() → 扁平、时间隔离的特征表
     │  严格时间隔离：T0≤9:29, T-1/T-2≤9:33；自检
     ▼
[L4 因子层]  edge_core 在 canonical 输入上重拟合 + 接入新因子（REBUILD-refit）
```

## 3. KEEP / REBUILD 清单

**KEEP（保留并复用）**
- 采集与解密：endpoints、AES（key `secretkey322...`、iv、CBC/PKCS7）、raw 原样保存、persistEveryFetch。
- 时间隔离加载器约束：T0≤9:29、T-1/T-2≤9:33。
- 调度基础设施：cron / agent_job_runner.sh / agent_job_worker.py / git-as-queue。
- 已验证因子学习：edge_core 权重雏形、REGIME_ACTION_GATE、IC 结论（在 canonical 上重新验证后采用）。

**REBUILD（推倒重做）**
- L3 特征/加载层：新模块从 raw 重新派生，输出规范名+规范口径，**不读旧 transform-2 的错配标签**。
- L4 因子/打分层：在 canonical 输入上重拟合，接入新因子。

**仅允许的就地编辑（patch-script，非整体重写）**
- 采集完整性修复：weimai 封单展示、pool.hot 落 raw[]+item[7]板态、pool.surge turnover 取站点 item[10]、hotlist 读 hot_stock_hour。**不做**给 105KB 文件批量改名。

## 4. 里程碑与排期

| 里程碑 | 内容 | 产出 | 状态 |
|---|---|---|---|
| M0 | 本规划 + HANDOFF §9 路线图刷新 | rebuild-plan-v11.md | 进行中 |
| L2 | canonical + routing 单一事实源 | canonical.py / canonical_routing.py | ✅ 已上线并验证 |
| 0091 | seal_amount 单位探针 server gate | 0091.result.json | 待 cron 跑 |
| 0092 | routing 模块 vs live canonical 校验 | 0092.result.json | 待 cron 跑 |
| M1 | 特征/加载层重构（feature builder） | duanxianxia_feature_builder.py + 自检 | 待办 |
| M2 | 采集完整性补丁落地 | patch-scripts | 待办 |
| M3 | 历史回溯重导 + 重生成 CSV | server job 0093-pre | 待办 |
| 0093 | 因子重拟合（edge_core on canonical + 新因子） | refit 结果 + 系数 | 待办 |
| 0094 | 上线校验：QX-live ~9:25 pin | 校验报告 | 待办 |
| 0095 | T-1 滞后特征（延后项） | — | 延后 |

## 5. 数据回溯与一致性

- 对**有 raw 的 captures**：用 canonical_routing 重新派生 canonical，重生成 `_all_candidates_flat.csv` / `feature_matrix_v21.csv`。
- 对**无 raw 的历史 pool.hot**：标记 `legacy_unrecoverable`，不臆造、不 sed 改历史。
- 所有重导一律**从 raw 出发**，禁止对历史结果做就地字符串替换。
- 数据量大的步骤（重导历史、重生成 CSV、重拟合）一律走 server 端队列任务执行（sandbox 无法访问 workspace captures）；本地只负责写代码、入队、并通过 agent-results 验收。

## 6. 新因子（L4 接入）

origin / themeConsistency / themeConcentration / auctionSealAmount / marketSealRate / prevDayLimitUpSealRate / stockMainlineFit；设定 minBidAmount、auctionChgMin 阈值；在 canonical 输入上重拟合 edge_core 系数：
`0.23·auction_amount_pct + 0.19·auction_strength + 0.18·liquidity + 0.14·money + 0.14·pressure_score + 0.08·weimai_strength + 0.05·orderbook − risk_penalty`（系数重拟合后更新）。

## 7. 执行机制（git-as-queue）

- 队列：`scripts/agent_jobs/queue/<id>.json` = `{id, script, args, timeout, note}`；worker `python3 <script> <args>`，cwd=WS，幂等（有结果则跳过）。
- 结果落 `agent-results` 分支 `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json`。
- 同分支提交**串行**（避免 409）。下一个空闲 id = 0093。

## 8. 验收标准（DoD）

1. 任一下游表/因子的字段单位与口径可被 canonical 自检与 server gate 双重证明一致。
2. feature builder 自检在真实 0089 行上通过；时间隔离边界被强制。
3. 历史 CSV 全部由 raw 经 canonical 重导，无就地改写痕迹。
4. edge_core 在 canonical 输入上重拟合，IC/回测不劣于旧口径。
5. 9:25 live pin 校验通过。

## 9. 红线

- 不改 field-rename-map §4 的口径语义（seal_amount = 万→×1e4→元，已定）。
- 不整体重写 105KB/145KB 巨型脚本。
- canonical.py import 时跑 `_self_test()`，损坏即阻断导入 —— 禁止推送损坏的 canonical。
- 历史数据禁止 sed 改写，只能从 raw 重导。
