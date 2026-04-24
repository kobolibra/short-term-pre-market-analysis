# duanxianxia 项目手册（2026-04-11）

这份手册用于后续快速接续项目，避免每次重新判断口径。

## 1. 项目定位

`duanxianxia` 是一个长期抓取项目，目标是围绕 `https://duanxianxia.com/` 建立：
- 稳定抓取
- 每次抓取自动落盘
- 固定中文表头输出
- 可回看历史结果
- 可按盘前 / 盘中 / 盘后分组执行

项目目录：`projects/duanxianxia`
抓取器：`scripts/duanxianxia_fetcher.py`

## 2. 当前纳入范围（共 10 张）

### 盘前（4 张）
7. `auction.jjyd.vratio`｜竞价爆量
8. `auction.jjyd.qiangchou`｜竞价抢筹
9. `auction.jjyd.net_amount`｜竞价净额
10. `auction.jjlive.fengdan`｜竞价封单（仅当天 live）

### 盘中（4 张）
1. `rank.rocket`｜飙升榜
4. `rank.hot_stock_day`｜热度榜（日）
2. `pool.hot`｜热门
3. `pool.surge`｜冲涨

### 盘后（2 张）
6. `review.daily.top_metrics`｜每日复盘顶部指标
5. `review.ltgd.range`｜龙头高度区间涨幅

## 3. 固定执行时间

### 盘前
- 工作日北京时间 `09:25`
- 随机延迟 `5-15 秒`
- 自动抓取并保存

### 盘中
- 不自动抓
- 由用户盘中随时下指令后抓取并保存

### 盘后
- 工作日北京时间 `17:20`
- 随机延迟 `0-5 分钟`
- 自动抓取并保存

## 4. 固定抓取与落盘规则

### 抓取锁定
一律按 `dataset_id + source_path` 锁定，不按中文名猜。

### 落盘路径
每次抓取必须落盘到：
`projects/duanxianxia/captures/YYYY-MM-DD/<dataset-id>/<HHMMSS>.json`

### 落盘 JSON 最少字段
- `project`
- `dataset_kind`
- `dataset_id`
- `dataset_label`
- `source_path`
- `source_url`
- `fetched_at`
- `fetched_at_utc`
- `timezone`
- `row_count`
- `headers`
- `rows`
- `meta`

### 下载完成后的固定回执要求
- 每次下载/抓取完成后，必须发出 **webhook 推送通知**。
- 同时必须给用户明确回执，让用户一眼知道：
  - 是否下载成功
  - 是否完整
  - 是否有失败
  - 是否有遗漏
- 回执最少字段固定为：
  - `dataset_id`
  - `dataset_label`
  - `source_url`
  - `fetched_at`
  - `row_count`
  - `capture_path`
  - `saved`
  - `success`
  - `complete`
  - `failed_items`
  - `missing_items`
- 若任一字段无法确认，不允许含糊写“应该没问题”；必须明确标记 `unknown`。

## 5. 固定正式输出字段

### 1) 飙升榜
`排名 | 代码 | 名称 | 飙升值`

### 2) 热门
`代码 | 名称 | 涨幅 | 主力 | 实际换手 | 成交 | 流通 | 概念`

### 3) 冲涨
`排名 | 代码 | 名称 | 涨幅 | 换手比 | 成交 | 流通市值 | 概念1 | 概念2`

### 4) 热度榜（日）
`排名 | 代码 | 名称 | 热度值`

### 5) 每日复盘顶部指标
`序号 | 指标键 | 指标名称 | 日期 | 数值`

### 6) 龙头高度区间涨幅
固定 4 个周期表：`5日 | 10日 | 20日 | 50日`
每表内分：`主板 | 创业科创板 | 北交所`
表头：`周期 | 板块 | 排名 | 代码 | 名称 | 区间涨幅 | 概念 | 日期区间`

### 7) 竞价爆量
`名称 | 代码 | 涨幅 | 竞额 | 昨竞额 | 竞价换手 | 竞价量比 | 竞涨 | 流通值 | 概念`

### 8) 竞价抢筹
必须拆成两张：
- `9:20 - 9:25的抢筹幅度`
- `竞价最后1秒的抢筹幅度`
两张表头都固定为：
`名称 | 代码 | 涨幅 | 竞额 | 抢筹幅度 | 竞价换手 | 竞涨 | 流通值 | 概念`

### 9) 竞价净额
`名称 | 代码 | 涨幅 | 竞价换手 | 竞涨 | 主力净买 | 竞额 | 流通值 | 概念1 | 概念2`

### 10) 竞价封单
`名称 | 代码 | 题材1 | 题材2 | 连板标签 | 9:15 | 9:20 | 9:25 | 涨幅`

## 6. 当前固定抓取源

- 飙升榜：`x.duanxianxia.cn/vendor/stockdata/hotlist.json -> skyrocket_hour`
- 热门：`duanxianxia.com/data/getFxPoolData/`
- 冲涨：`duanxianxia.com/data/getCzPoolData/`
- 热度榜（日）：`x.duanxianxia.cn/vendor/stockdata/hotlist.json -> hot_stock_day`
- 每日复盘顶部指标：`duanxianxia.com/api/getChartByQingxu`
- 龙头高度区间涨幅：`duanxianxia.com/api/getZfByDate`
- 竞价爆量：`duanxianxia.com/data/getVratioData/11`
- 竞价抢筹：`duanxianxia.com/data/getQiangchouData/11`
- 竞价净额：`duanxianxia.com/vendor/stockdata/jjzhuli.json`
- 竞价封单：
  - 表结构：`vendor/stockdata/jjlive.json`
  - 当天 live 涨幅：`qt.gtimg.cn`

## 7. 关键口径提醒

- `pool.hot` = **热门**，不是热度榜（日）
- `rank.hot_stock_day` = **热度榜（日）**，不是热门
- `竞价抢筹` 必须双表输出，不能只发一组
- `竞价净额` 主源固定为 `jjzhuli.json`，旧 `getDabanData` 不再作为正式默认来源
- `竞价封单` 数值列顺序固定：`9:15 | 9:20 | 9:25 | 涨幅`
- 默认所有用户可见输出都用完整中文表头
- 除非用户明确确认，否则不得随意改：
  - 主抓取源
  - 正式表头
  - 字段顺序
  - 拆表方式
  - 落盘结构
  - 时段分组与执行时点

## 8. 后续回答时默认先看
1. `projects/duanxianxia/docs/project-handbook-2026-04-11.md`
2. `projects/duanxianxia/docs/fixed-table-contract.md`
3. `projects/duanxianxia/docs/user-contract.md`
4. `projects/duanxianxia/README.md`
5. 当天相关 `memory/YYYY-MM-DD.md`
