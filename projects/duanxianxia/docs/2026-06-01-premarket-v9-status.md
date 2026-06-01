# 2026-06-01 盘前运行与 v9 问题记录

## 背景

2026-06-01 对 `duanxianxia` 的盘前 cron 链路做了切换与验证：

- 旧入口：`scripts/duanxianxia_cron_agent_entry.py`（文件头已标记 `DEPRECATED ENTRYPOINT`）
- 新入口：
  - `bash scripts/duanxianxia_cron_runner.sh premarket`
  - `bash scripts/duanxianxia_cron_runner.sh intraday_cashflow`
  - `bash scripts/duanxianxia_postmarket_chain_runner.sh postmarket_capture`
  - `bash scripts/duanxianxia_postmarket_chain_runner.sh postmarket_dailyline`
  - `bash scripts/duanxianxia_postmarket_chain_runner.sh postmarket_analysis`

OpenClaw cron 配置已改到新 runner，并手动执行了一次新的 premarket runner 做验证。

## 手动运行结果

手动执行命令：

```bash
cd /home/investmentofficehku/.openclaw/workspace && bash scripts/duanxianxia_cron_runner.sh premarket
```

运行结果：

- 退出码：`0`
- 盘前批量下载：成功
- 完整性：完整
- 预期表数：9
- 成功表数：9
- 失败表数：0
- 遗漏表数：0

逐表成功项：

- 飙升榜
- 热度榜（日）
- 竞价异动/竞价爆量
- 竞价异动/竞价抢筹
- 竞价异动/竞价净额
- 竞价封单/当日封单表
- 竞价异动/涨停委买
- 主页板块强度全主标签汇总表
- 主页 qxlive 顶部指标按钮组

## 今日盘前数据落盘

今日新增/相关 captures 位于：

- `projects/duanxianxia/captures/2026-06-01/auction.jjlive.fengdan/092805.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjlive.fengdan/204950.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.net_amount/092804.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.net_amount/204949.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.qiangchou/092803.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.qiangchou/204949.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.vratio/092803.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.vratio/204948.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.weimai/092806.json`
- `projects/duanxianxia/captures/2026-06-01/auction.jjyd.weimai/204951.json`
- `projects/duanxianxia/captures/2026-06-01/cashflow.stock.10day/101010.json`
- `projects/duanxianxia/captures/2026-06-01/cashflow.stock.3day/100642.json`
- `projects/duanxianxia/captures/2026-06-01/cashflow.stock.5day/100757.json`
- `projects/duanxianxia/captures/2026-06-01/cashflow.stock.today/100448.json`
- `projects/duanxianxia/captures/2026-06-01/home.kaipan.plate.summary/092815.json`
- `projects/duanxianxia/captures/2026-06-01/home.kaipan.plate.summary/204955.json`
- `projects/duanxianxia/captures/2026-06-01/home.qxlive.top_metrics/093025.json`
- `projects/duanxianxia/captures/2026-06-01/home.qxlive.top_metrics/100352.json`
- `projects/duanxianxia/captures/2026-06-01/home.qxlive.top_metrics/205250.json`
- `projects/duanxianxia/captures/2026-06-01/pool.hot/100118.json`
- `projects/duanxianxia/captures/2026-06-01/pool.surge/100119.json`
- `projects/duanxianxia/captures/2026-06-01/rank.hot_stock_day/092802.json`
- `projects/duanxianxia/captures/2026-06-01/rank.hot_stock_day/100117.json`
- `projects/duanxianxia/captures/2026-06-01/rank.hot_stock_day/204947.json`
- `projects/duanxianxia/captures/2026-06-01/rank.rocket/092801.json`
- `projects/duanxianxia/captures/2026-06-01/rank.rocket/100116.json`
- `projects/duanxianxia/captures/2026-06-01/rank.rocket/204946.json`

## 今日分析结果文件

今日相关 reports 位于：

- `projects/duanxianxia/reports/2026-06-01/premarket/093027.json`
- `projects/duanxianxia/reports/2026-06-01/premarket/093157_analysis_v7_2.json`
- `projects/duanxianxia/reports/2026-06-01/premarket/093818_analysis_v9.json`
- `projects/duanxianxia/reports/2026-06-01/premarket/205258_analysis_v9.json`
- `projects/duanxianxia/reports/2026-06-01/premarket/205259.json`
- `projects/duanxianxia/reports/2026-06-01/premarket/intraday_anchors.json`
- `projects/duanxianxia/reports/2026-06-01/premarket/intraday_anchors_v9.json`

其中本次手动验证对应的核心文件是：

- `projects/duanxianxia/reports/2026-06-01/premarket/205258_analysis_v9.json`

确认字段：

- `version = premarket_v9`
- `meta.engine = premarket_v9`
- `meta.generated_at = 2026-06-01T20:52:58+08:00`
- `candidate_count = 407`

## 今日 v9 Top 10（按当前输出排序）

1. 华天科技 (002185) | alpha=AUCTION_ORDERFLOW | edge=58.43 | final=54.94 | concepts=先进封装 / 并购重组
2. 兆龙互连 (300913) | alpha=AUCTION_ORDERFLOW | edge=52.17 | final=41.93 | concepts=高速连接 / CPO/MPO
3. 京东方Ａ (000725) | alpha=AUCTION_ORDERFLOW | edge=51.32 | final=0.0 | concepts=玻璃基板 / 面板
4. 中京电子 (002579) | alpha=AUCTION_ORDERFLOW | edge=50.74 | final=35.61 | concepts=印制电路板 / 通信
5. 红星发展 (600367) | alpha=AUCTION_ORDERFLOW | edge=50.21 | final=46.35 | concepts=-
6. 风华高科 (000636) | alpha=LOW_OPEN_REVERSAL | edge=49.33 | final=47.78 | concepts=电阻电容 / 元器件
7. 珈伟新能 (300317) | alpha=AUCTION_ORDERFLOW | edge=49.30 | final=37.48 | concepts=绿色电力 / 储能电池厂商
8. 宝新能源 (000690) | alpha=AUCTION_ORDERFLOW | edge=49.25 | final=0.0 | concepts=电力 / 火电
9. 博杰股份 (002975) | alpha=AUCTION_ORDERFLOW | edge=48.36 | final=53.28 | concepts=电阻电容 / 端侧AI
10. 浪潮信息 (000977) | alpha=AUCTION_ORDERFLOW | edge=47.86 | final=29.59 | concepts=服务器 / 算力

## 发现的问题（核心）

### 1. cron 切换已成功，但 v9 结果不是完整动作版

这次手动运行确认：

- 新 premarket runner 已真正执行
- 新 runner 的 active engine 已切到 `build_premarket_analysis_v9`
- `analysis_v9.json` 与 `intraday_anchors_v9.json` 已产出

所以链路切换本身是成功的。

### 2. v9 当前只完成“重打分/排序”，没有完成“动作决策层”

排查结果：

- `duanxianxia_v9_assemble.py` 负责装配：
  - `weimai_detail`
  - `theme_detail`
  - `context_detail`
  - 然后调用 `duanxianxia_v9_edge.compute_edge_v9(...)`
- `duanxianxia_v9_edge.py` 当前只返回：
  - `edge_score`
  - `alpha_type`
  - `edge_components`
  - `risk_flag`
  - `risk_detail`

也就是说，当前 v9 根本没有生成这些最终动作字段：

- `action_type`
- `action_score`
- `setup`
- `buy/watch/drop` 等最终动作分类

### 3. 证据

对 `205258_analysis_v9.json` 实查结果：

- 总候选：`407`
- `full.action_type != null` 的个数：`0`
- `full.final_score != 0` 的个数：`54`
- `setup_stats = {}`
- `action_stats = {}`

说明：

- 分数系统已经在工作
- 排序已经在工作
- 但动作决策层没有接上

### 4. 额外说明

`duanxianxia_premarket_v9_runner.py` 里的 `_adapt_for_batch()` 会在 batch 适配层做一个兜底：

```python
out["action_type"] = row.get("action_type") or alpha
```

也就是说，batch 文本视图里理论上可以拿 `alpha_type` 临时顶成 `action_type`，
但这不是原始 v9 分析文件里的真实动作决策结果，只是显示适配。

## 当前判断

当前 v9 状态可归纳为：

- **已完成：**
  - 全量数据装配
  - weimai/theme/context/market_env 融合
  - edge 重打分
  - alpha 分类
  - 候选排序
- **未完成：**
  - 最终动作决策层
  - setup 归类层
  - `action_stats` / `setup_stats`
  - 成品化的买入/观察/放弃输出

## 建议下一步

建议优先补一个轻量动作映射层，基于现有字段：

- `edge_score`
- `risk_flag`
- `alpha_type`
- regime buy gate

先快速产出：

- `BUY`
- `WATCH`
- `DROP`

等动作字段，先把 v9 结果从“排序版”补到“可用动作版”。
