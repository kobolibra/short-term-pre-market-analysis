# duanxianxia 项目

长期项目：围绕 `https://duanxianxia.com/` 建立可复用的数据抓取、结构化输出、以及每次抓取后的持久化存储能力。

## 接续入口（必读）

**开新对话前必须读以下文件，顺序不要改变：**

1. **`docs/HANDOFF.md`** ← 必读第一份！当前进度、待办事项、字段修正关键结论全在这里
2. `docs/canonical-field-dictionary.md` ← 字段 source of truth
3. `docs/v10-field-alignment-decisions.md` ← 因子 source of truth
4. `docs/rebuild-design-v10.md` ← 架构决策
5. `reports/_audit/agent_jobs/0089_unit_probe_20260629.result.json`（agent-results 分支）← 读完再推进 Task 0090

其他参考：`docs/project-handbook-current.md`（V9 抗取规范，部分付 V10 未覆盖）

---

## 项目目标

1. 对用户点名的页面/榜单/表格建立稳定抓取器。
2. **每次抓取都要落盘存储**，保留时间戳与原始结构化结果。
3. 后续支持：即时查询、历史回看、多表批量抓取、飞书自动推送。
4. 最终输出 v10 edge_core 评分，通过飞书 webhook 推送。

## 当前规则（用户明确要求）

- 这是一个**长期项目**，不是一次性任务。
- 用户要求抓取的表格数据：**每次都要存储**。
- 默认输出尽量完整，**不得擅自截断**，除非用户明确要求精简。
- 时间展示默认按**北京时间**理解与输出。
- 取数时必须以用户指定的**准确路径和截图字段**为准。
- 所有用户可见表格默认使用**完整中文表头**。
- 数据集口径一律按 **`dataset_id + source_path`** 锁定。
- `pool.hot`（热门）与 `rank.hot_stock_day`（热度榜（日））必须严格区分。
- 后续若无用户明确确认：**不得随意改抓取源、落盘结构、正式输出字段、字段顺序、表拆分方式、时段分组与执行时点**。

## 目录说明

- `captures/` — 每次抓取的落盘数据（`YYYY-MM-DD/<dataset-id>/<HHMMSS>.json`）
- `config/` — 数据集定义、别名、抓取配置
- `docs/` — 字段说明、路径说明、项目设计说明
- `reports/` — 批量报告与审计结果
