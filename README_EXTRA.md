# 在其他 LLM 中继续分析 duanxianxia 盘前下载/选股代码的建议

## 背景
本仓库为「duanxianxia」短线侠盘前数据下载与分析相关代码，目标是稳定抓取盘前五表并输出可用于选股的结构化数据。

## 快速上手
- 核心脚本：
  - `scripts/duanxianxia_batch.py`：统一入口，按分组抓取数据
  - `scripts/duanxianxia_fetcher.py`：抓取与字段抽取逻辑（核心）
  - `scripts/duanxianxia_cron_runner.sh`：定时执行入口
  - `scripts/duanxianxia_review_backfill.py`：复盘回填脚本
- 配置：
  - `projects/duanxianxia/config/datasets.json`
- 规则与契约：
  - `projects/duanxianxia/docs/project-handbook-current.md`
  - `projects/duanxianxia/docs/fixed-table-contract.md`
  - `projects/duanxianxia/docs/user-contract.md`

## 本次提交范围
本次仅包含与 duanxianxia 盘前下载-选股链路直接相关文件，不包含其他仓库（MacroGodEye、MediaCrawler、backups）。

## 关键业务口径（请勿改）
- 盘前时段：
  - `auction_vratio`
  - `auction_qiangchou`
  - `auction_net_amount`
  - `auction_fengdan`
  - `home_qxlive_plate_summary`
  - `home_qxlive_top_metrics`
- 默认 `dataset_id + source_path` 为口径主键。
- 落盘必须保留原始结果（按项目约定）。
- 选股/分析建议尽量在不改动抓取契约的前提下进行。

## 常用运行方式
- 盘前完整抓取（示例）：
  ```bash
  python3 scripts/duanxianxia_batch.py premarket --json
  ```
- 仅抓取某单表：可直接扩展/调用 batch 脚本分组与数据集映射。

## 需要重点关注的技术点
1. `scripts/duanxianxia_fetcher.py` 的字段提取与容错（重试/重试次数/时间字段）
2. `scripts/duanxianxia_batch.py` 的 `GROUPS` 与 `SEQUENCE` 是否与 `docs` 约定一致
3. `datasets.json` 与抓取结果是否一一对应
4. `persist_capture` 落盘路径与字段。
