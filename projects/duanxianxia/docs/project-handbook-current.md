# duanxianxia 项目总手册（当前版）

最后更新：2026-04-13 UTC

这份文档用于后续接续 `duanxianxia` 项目时，**一次性掌握当前已确认的规则、口径、调度、落盘、推送和风险点**。后续讨论这个项目，优先先看这份，再看 `user-contract.md` / `fixed-table-contract.md`。

---

## 1. 项目定位

`duanxianxia` 是一个长期抓取项目，围绕 `https://duanxianxia.com/` 建立：
- 稳定抓取
- 每次抓取自动落盘
- 固定中文表头输出
- 支持盘前 / 盘中 / 盘后 / 资金流向多组批量执行
- 支持 webhook / 飞书自动推送

### 关键路径
- 项目目录：`projects/duanxianxia`
- 抓取器：`scripts/duanxianxia_fetcher.py`
- 批量抓取：`scripts/duanxianxia_batch.py`
- cron runner：`scripts/duanxianxia_cron_runner.sh`
- 调度配置：`~/.openclaw/cron/jobs.json`

---

## 2. 当前纳入范围（15 张固定表 + 2 张补充/研究表）

### 盘前（4 张固定表 + 2 张独立补充表）
1. `auction.jjyd.vratio`｜竞价爆量
2. `auction.jjyd.qiangchou`｜竞价抢筹
3. `auction.jjyd.net_amount`｜竞价净额
4. `auction.jjlive.fengdan`｜竞价封单（仅当天 live）
16. `home.kaipan.plate.summary`｜主页板块强度全主标签汇总表
17. `home.qxlive.top_metrics`｜主页 qxlive 顶部指标按钮组

### 盘中（5 张）
5. `rank.rocket`｜飙升榜
6. `rank.hot_stock_day`｜热度榜（日）
7. `pool.hot`｜热门
8. `pool.surge`｜冲涨
17. `home.qxlive.top_metrics`｜主页 qxlive 顶部指标按钮组

### 盘后（3 张固定表 + 1 张新增题材研究表 + 2 张盘后刷新榜单）
9. `review.daily.top_metrics`｜每日复盘顶部指标
10. `review.ltgd.range`｜龙头高度区间涨幅
15. `review.fupan.plate`｜涨停复盘（按概念）
18. `home.ztpool`｜主页涨停股票池
1. `rank.rocket`｜飙升榜（盘后最终刷新）
4. `rank.hot_stock_day`｜热度榜（日）（盘后最终刷新）

### 资金流向（4 张）
11. `cashflow.stock.today`｜个股资金流向 / 今日排行
12. `cashflow.stock.3day`｜个股资金流向 / 3日排行
13. `cashflow.stock.5day`｜个股资金流向 / 5日排行
14. `cashflow.stock.10day`｜个股资金流向 / 10日排行

### 日线与复盘回填（闭环层）
19. `dailyline.stock.manifest`｜复盘日线下载清单
20. `review_backfill`｜基于日线与快照回填 Feishu 推荐复盘结果

---

## 3. 当前调度与下载规则

## 3.1 盘前自动抓取
- 任务名：`duanxianxia_盘前自动抓取`
- cron：`25 9 * * 1-5`
- 时区：`Asia/Shanghai`
- 随机延迟：**5–15 秒**
- runner：`premarket`
- 抓取内容：盘前 4 张固定表 + qxlive 全主标签汇总表 + 主页 qxlive 顶部指标按钮组

## 3.2 10:01 自动抓取
- 任务名：`duanxianxia_1001盘中+资金净流入自动抓取`
- cron：`1 10 * * 1-5`
- 时区：`Asia/Shanghai`
- 随机延迟：**0–45 秒**
- runner：`intraday_cashflow`
- 抓取内容：
  - 盘中 4 张表 + 主页 qxlive 顶部指标按钮组
  - 资金净流入前100（4 张）
- 当前这是用户最新确认版本，不再是“只抓资金净流入”

## 3.3 盘中手动抓取
- 除 10:01 之外，盘中默认**不自动抓**
- 用户盘中随时下指令后，立即抓取并保存
- 默认盘中口径：
  - 飙升榜
  - 热度榜（日）
  - 热门
  - 冲涨
  - 主页 qxlive 顶部指标按钮组
- `home.kaipan.plate.summary` 虽然源自 qxlive 页面，但当前用户已明确要求：**原来的股票覆盖型第5表不要了，直接替换为主标签汇总型第5表，并继续放进盘前一起下载**。
- 若用户明确要求“抓资金净流入”，则抓 4 张资金流向表

## 3.4 盘后自动抓取
- 任务名：`duanxianxia_盘后自动抓取`
- cron：`20 17 * * 1-5`
- 时区：`Asia/Shanghai`
- 随机延迟：**0–5 分钟**
- runner：`postmarket_cashflow`
- 抓取内容：
  - 盘后 3 张表（含新增题材研究表）
  - 盘后最终刷新榜单 2 张：`rank.rocket`、`rank.hot_stock_day`
  - 资金净流入前100（4 张）
- 新增正式规则（2026-04-16 用户确认）：
  - `rank.rocket` 与 `rank.hot_stock_day` 虽然也属于盘中榜单，但由于盘中持续变化，**盘后必须重新抓取一次最终态**；
  - 后续盘后分析、盘后推送、以及日线下载池构建，默认都应优先使用这两张榜单的**盘后最终刷新版本**，而不是沿用盘中旧快照。

---

## 4. 当前 batch group 定义

`duanxianxia_batch.py` 当前支持这些关键 group：

- `premarket`
  - 盘前 4 张固定表 + qxlive 全主标签汇总表 + 主页 qxlive 顶部指标按钮组
- `intraday`
  - 盘中 4 张 + 主页 qxlive 顶部指标按钮组
- `postmarket`
  - 盘后 3 张（含新增题材研究表） + `home.ztpool` + 盘后最终刷新榜单 2 张（`飙升榜`、`热度榜（日）`）
- `cashflow`
  - 资金流向 4 张
- `postmarket_cashflow`
  - 盘后 3 张（含新增题材研究表） + `home.ztpool` + 盘后最终刷新榜单 2 张（`飙升榜`、`热度榜（日）`） + 资金流向 4 张
- `intraday_cashflow`
  - 盘中 4 张 + 主页 qxlive 顶部指标按钮组 + 资金流向 4 张
- `dailyline`
  - 当日正式 `premarket / intraday / intraday_cashflow / postmarket / postmarket_cashflow` 引用的 capture 去重股票池
  - 若盘后已刷新 `rank.rocket` / `rank.hot_stock_day`，默认使用盘后最终快照
  - 额外并入上一交易日 Feishu 多维表中的**所有正式推荐股票**（过滤 `推荐分级` 含“不建议”）
  - 使用 `baostock` 下载/补齐个股日线，并落盘 `dailyline.stock.manifest`

---

## 5. 关键口径（后续绝不能再混）

### 5.1 热门 vs 热度榜（日）
- `pool.hot` = **热门**
- `rank.hot_stock_day` = **热度榜（日）**
- 后续凡是提到“热门 / 热度 / 热度榜”，必须先按 `dataset_id + source_path` 自检后再回答

### 5.2 龙头高度区间涨幅
- `review.ltgd.range` 的正式结果固定为：
  - 4 个周期：`5日 / 10日 / 20日 / 50日`
  - 每周期固定分：`主板 / 创业科创板 / 北交所`
- 用户最新要求：
  - **默认列全**
  - 不要只给前几条预览
  - 格式要简洁整齐
- 当前文本紧凑样式：
  - `排名. 名称（代码）｜区间涨幅｜概念`

### 5.3 涨停复盘（按概念）
- `review.fupan.plate` 的正式语义是：**动态题材标签 + 题材下对应股票明细**。
- 它不是现有盘前 / 盘中 / 资金流向榜单的换皮，也不是 `review.daily.top_metrics` / `review.ltgd.range` 的重组。
- 固定请求口径：`duanxianxia.com/api/getFupanByYidong`，并使用 `type=plate`。
- 默认保留两层信息：
  - 题材层：`题材名称 | 题材说明 | 题材涨停数 | 题材股票数`
  - 股票层：`名称 | 代码 | 涨幅 | 板数 | 连板 | 封单额 | 成交额 | 异动原因`
- `meta` 中需额外保留：
  - `topic_count`
  - `topics`
  - `htmlcopy_metrics`（情绪指标、涨停家数、跌停家数、封板率、涨停表现、连板表现等）
  - `fine_tag_summary`（用于跟踪更细粒度标签，如数据中心 / 液冷服务器 / 算力租赁 / 东数西算 / 国资云 等）
- 每条股票明细允许额外保留：
  - `细标签`
  - `细标签列表`

### 5.4 主页板块强度全主标签汇总表
- `home.kaipan.plate.summary` 属于主页 `/web/qxlive` 链路，不属于 `review.fupan.plate`，也不是 `/web/pool`。
- 该数据集的正式 dataset_id 为 `home.kaipan.plate.summary`，不再沿用旧的 `home.kaipan.subplate.stock` 兼容名，避免把历史“股票覆盖表”与当前“主标签汇总表”混淆。
- 前端真实组装路径：
  - `https://duanxianxia.com/web/qxlive`
  - `https://duanxianxia.com/api/getLiveByStrong`（`platetype=strong` 取板块强度）
  - `https://duanxianxia.com/api/getLiveByStrong`（`platetype=money` 取主力流入）
  - `https://duanxianxia.com/data/getKaipanSubPlate`
- 当前固定字段合同：
  - `主标签序号 | 主标签名称 | 主标签代码 | 板块强度 | 主力流入 | 涨停数量 | 子标签数量 | 子标签列表`
- 当前固定实现口径：默认抓取 qxlive **全部主标签**，每个主标签输出一行，并附带该主标签下全部子标签列表。
- 当前用户最新确认口径：**原来的第5表不要了，直接替换成这个主标签汇总表，并继续纳入盘前分析。**
- 当前实现已落地到 `scripts/duanxianxia_batch.py` 的 `premarket` 回执里：
  - `analysis.top_candidates` 仍主要由盘前前4表生成；
  - 第5表改为在 `analysis.market_themes` 中提供主标签强度、主力流入、涨停数量、子标签列表。
- 由于用户明确要求“替换”而非“新增并存”，旧版基于第5表按个股代码补题材/子题材/龙头标签/破板风险的逻辑已退出默认语义。

### 5.5 主页 qxlive 顶部指标按钮组
- `home.qxlive.top_metrics` 属于主页 `/web/qxlive` 顶部按钮链路，不属于 `review.daily.top_metrics`，也不能再用复盘 `core11` 代替。
- 当前固定抓取链路：
  - `https://duanxianxia.com/web/qxlive`
  - `https://duanxianxia.com/vendor/stockdata/platechart1.json`（解密后读 `qxlive.series`）
  - `https://duanxianxia.com/api/getLastQxlive`（读 `qxlast`）
  - 页面 DOM `button.chart`
- 当前固定按钮共 12 项：
  - `QX | ZT | DT | KQXY | HSLN | LBGD | SZ | XD | PB | ZTBX | LBBX | PBBX`
- 当前正式字段口径：
  - `当前值` 以主页按钮当前显示值为准
  - `图表尾值` 为 `qxlive.series` 尾点
  - `PBBX` 在这里是 **沪深5分钟量能**，不是复盘链路里的“连板晋级率”
  - `PBBX 当前值 = qxlast.PBBX`，`图表尾值 = qxlive.series.JRLN`，`对照值 = qxlast.ZRLN`

### 5.6 资金净流入
- 4 张固定表：
  - 今日 / 3日 / 5日 / 10日
- 默认每张只下载**前100名**
- 当前自动抓取频次：
  - 10:01 一次（与盘中 4 表一起）
  - 盘后一次（与盘后 3 表一起）

### 5.7 竞价抢筹
- 必须拆成两张：
  - `9:20 - 9:25的抢筹幅度`
  - `竞价最后1秒的抢筹幅度`
- 不能只发其中一组

### 5.8 竞价净额
- 当前正式默认主抓取源：`jjzhuli.json`
- 旧 `getDabanData` 不再作为正式默认主源

### 5.9 竞价封单
- 数值列顺序固定：`9:15 | 9:20 | 9:25 | 涨幅`
- 题材标签与连板标签必须保留

---

## 6. 固定正式输出字段（用户可见）

### 飙升榜
`排名 | 代码 | 名称 | 飙升值`

### 热门
`代码 | 名称 | 涨幅 | 主力 | 实际换手 | 成交 | 流通 | 概念`

### 冲涨
`排名 | 代码 | 名称 | 涨幅 | 换手比 | 成交 | 流通市值 | 概念1 | 概念2`

### 热度榜（日）
`排名 | 代码 | 名称 | 热度值`

### 每日复盘顶部指标
`序号 | 指标键 | 指标名称 | 日期 | 数值`

### 龙头高度区间涨幅
`周期 | 板块 | 排名 | 代码 | 名称 | 区间涨幅 | 概念 | 日期区间`

### 涨停复盘（按概念）
`日期 | 题材序号 | 题材名称 | 题材说明 | 题材涨停数 | 题材内序号 | 名称 | 代码 | 涨幅 | 板数 | 连板 | 封单额 | 成交额 | 异动原因`

### 主页板块强度全主标签汇总表
`主标签序号 | 主标签名称 | 主标签代码 | 板块强度 | 主力流入 | 涨停数量 | 子标签数量 | 子标签列表`

### 主页 qxlive 顶部指标按钮组
`序号 | 指标键 | 指标名称 | 日期 | 时间点 | 当前值 | 按钮显示值 | 图表尾值 | 对照值 | 按钮ID | 按钮文本`

### 竞价爆量
`名称 | 代码 | 涨幅 | 竞额 | 昨竞额 | 竞价换手 | 竞价量比 | 竞涨 | 流通值 | 概念`

### 竞价抢筹
`名称 | 代码 | 涨幅 | 竞额 | 抢筹幅度 | 竞价换手 | 竞涨 | 流通值 | 概念`

### 竞价净额
`名称 | 代码 | 涨幅 | 竞价换手 | 竞涨 | 主力净买 | 竞额 | 流通值 | 概念1 | 概念2`

### 竞价封单
`名称 | 代码 | 题材1 | 题材2 | 连板标签 | 9:15 | 9:20 | 9:25 | 涨幅`

### 资金净流入
当前抓取结果以结构化 JSON 为主；用户可见输出默认仍需使用**完整中文表头**，不得直接把 `rank/code/name/...` 英文字段甩给用户。

---

## 7. 抓取源（当前固定）

- 飙升榜：`x.duanxianxia.cn/vendor/stockdata/hotlist.json -> skyrocket_hour`
- 热门：`duanxianxia.com/data/getFxPoolData/`
- 冲涨：`duanxianxia.com/data/getCzPoolData/`
- 热度榜（日）：`x.duanxianxia.cn/vendor/stockdata/hotlist.json -> hot_stock_day`
- 每日复盘顶部指标：`duanxianxia.com/api/getChartByQingxu`
- 龙头高度区间涨幅：`duanxianxia.com/api/getZfByDate`
- 涨停复盘（按概念）：`duanxianxia.com/api/getFupanByYidong`（`type=plate`）
- 主页板块强度全主标签汇总表：`duanxianxia.com/web/qxlive -> /api/getLiveByStrong(strong) + /api/getLiveByStrong(money) + /data/getKaipanSubPlate`
- 竞价爆量：`duanxianxia.com/data/getVratioData/11`
- 竞价抢筹：`duanxianxia.com/data/getQiangchouData/11`
- 竞价净额：`duanxianxia.com/vendor/stockdata/jjzhuli.json`
- 竞价封单：
  - `vendor/stockdata/jjlive.json`
  - `qt.gtimg.cn`（补 live 涨幅）
- 资金流向：`https://stock.9fzt.com/cashFlow/stock.html`

---

## 8. 落盘与报告规则

### 8.1 落盘路径
每次抓取必须落盘到：
`projects/duanxianxia/captures/YYYY-MM-DD/<dataset-id>/<HHMMSS>.json`

### 8.2 落盘 JSON 最少字段
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

### 8.3 报告路径
批量报告输出到：
`projects/duanxianxia/reports/YYYY-MM-DD/<group>/<HHMMSS>.json`

### 8.4 日线下载池与复盘回填规则（2026-04-16 更新）
- `dailyline` 默认股票池由两部分合并去重构成：
  1. 当日正式 `premarket / intraday / intraday_cashflow / postmarket / postmarket_cashflow` report 所引用 capture 中出现过的全部股票；
  2. 上一交易日在 Feishu 多维表 `短线侠推荐复盘` 中写入的**所有正式推荐股票**。
- 过滤规则：
  - `home.kaipan.plate.summary` 这类非个股表不贡献股票代码；
  - `推荐分级` 含 `不建议` 的记录不并入上一交易日正式推荐股票；
  - 仅保留可识别的 A 股代码。
- 若当天盘后已重新抓取 `rank.rocket` / `rank.hot_stock_day`，则日线池默认采用它们的**盘后最终刷新版本**。
- manifest 标记字段已升级为：`前一交易日正式推荐`。
- 推荐的闭环执行顺序：
  1. 先跑 `dailyline`
  2. 再跑 `duanxianxia_review_backfill.py`
  3. 最后再看策略统计与结果回填是否完整

---

## 9. 通知与推送规则

### 9.1 当前已通
- cron 自动任务通过 OpenClaw `delivery.mode=announce`
- 自动推送到当前飞书用户

### 9.2 当前 webhook 状态
当前已配置：
- `DUANXIANXIA_WEBHOOK_URL`（飞书自定义机器人 webhook）

当前未配置：
- `DUANXIANXIA_WEBHOOK_BEARER`
- `DUANXIANXIA_WEBHOOK_SECRET`

因此当前状态是：
- **飞书自动 announce：已通**
- **飞书 bot webhook：已接入并已验证成功**

### 9.3 用户可见回执强制要求
每次下载/抓取完成后，必须明确告诉用户：
- 是否下载成功
- 是否完整
- 是否有失败
- 是否有遗漏

最少字段：
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

### 9.4 盘后推送额外要求
盘后下载完成后的 webhook / announce 推送，必须列出**明细数据**，不能只给摘要回执。

当前已落实：
- `每日复盘顶部指标`：全量明细
- `龙头高度区间涨幅`：全量展开，不只给预览

### 9.5 Feishu webhook 卡片正式展示规则（2026-04-16 确认版）
后续盘前 / 盘中 / 盘后 / 资金流向的 Feishu 自定义机器人 webhook 明细卡片，统一按以下规则执行：

1. **大多数数据集默认单表单卡展示**
   - 不再把同一张表机械切成很多卡。
   - 默认目标是：**每张表一张卡**。

2. **默认最多展示 30 行**
   - 若原表行数超过 30，则卡片内默认只展示前 `30` 行；
   - 卡片备注里必须明确写清：`仅展示前 30/N 行`。

3. **存在平台限制时允许进一步自动缩行**
   - Feishu 自定义机器人对单张卡片内容大小存在硬限制；
   - 因此若某张卡虽然只有 30 行，但 JSON 内容仍然过大，则允许在发送前**自动继续减少展示行数**，直到卡片大小降到安全范围；
   - 当前这类情况主要出现在单行文本特别长的表，例如：
     - `review.fupan.plate`
     - `home.ztpool`
   - 这属于平台兼容性兜底，不代表总原则变回“随意拆卡”。

4. **`auction.jjyd.qiangchou`（竞价抢筹）属于明确特例**
   - 该表不能压扁成一个总表；
   - 必须同时保留两个分组：
     - `9:20-9:25 抢筹幅度`
     - `竞价最后1秒 抢筹幅度`
   - 两个分组分别独立展示；
   - **每个分组各自最多 30 行**，若分组内容过长，也允许仅对该分组继续缩行。

5. **`home.kaipan.plate.summary`（主页板块强度全主标签汇总表）主力流入口径已纠正**
   - 当前抓到的 `platetype=money` 原始 `val`，按**万元**理解；
   - 用户可见展示再换算为 `万 / 亿`；
   - 例如：`1312315 -> 131.2亿`；
   - 不允许再沿用旧的错误展示，例如把它显示成 `131万`。

6. **中文表头继续强制保留**
   - 对用户可见的 webhook 卡片表头，继续按长期规则使用中文字段名；
   - 不要直接把内部抓取键名原样抛给用户。

---

## 10. 当前已验证状态

### 已验证成功
- `intraday_cashflow`：8/8 成功
- `postmarket_cashflow`：6/6 成功
- `cashflow`：4/4 成功，每表 100 行
- 盘后文本输出已带明细
- 龙头高度区间涨幅在盘后推送里已全量展开
- 飞书 bot webhook 已验证成功：
  - 直接 POST 测试成功（HTTP 200 / success）
  - `python3 scripts/duanxianxia_batch.py cashflow --json` 实际触发 webhook 成功

### 需要继续注意
- 当前已接的是**飞书 bot webhook**；若后续要接 n8n / 自建 HTTP 接口，仍需另配 webhook URL
- 若后续改 schema / 口径 / 字段顺序 / 调度，必须同步更新代码、文档、旧映射/旧落盘说明，否则不算 fixed

---

## 11. 后续讨论这个项目时的默认阅读顺序

1. `projects/duanxianxia/docs/project-handbook-current.md` ← **优先看这个**
2. `projects/duanxianxia/docs/user-contract.md`
3. `projects/duanxianxia/docs/fixed-table-contract.md`
4. `projects/duanxianxia/README.md`
5. 当天 `memory/YYYY-MM-DD.md`

---

## 12. 当前接续建议

如果后面继续讨论 `duanxianxia`，默认按以下顺序接：
1. 先确认用户讨论的是哪个时段（盘前 / 10:01 / 盘中 / 盘后 / 资金流向）
2. 再按 `dataset_id + source_path` 锁定口径
3. 先看已落盘文件，再决定是否重抓
4. 若是盘后推送，默认带明细，不要只发摘要
5. 若是 `review.ltgd.range`，默认全量展开，不要只发前几条
