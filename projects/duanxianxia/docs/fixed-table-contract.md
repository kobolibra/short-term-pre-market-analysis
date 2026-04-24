# duanxianxia 固定表格合同（2026-04-11）

这份文件用于把当前已确认的表格规则一次性固定下来。

原则：
- **抓取源固定**：除非用户明确确认，不得随意更换主抓取源。
- **落盘结构固定**：每次抓取必须落盘，路径和 JSON 基本结构固定。
- **正式输出字段固定**：对用户可见的中文表头、字段顺序、拆表方式固定。
- **口径变更必须成套处理**：代码、文档、落盘、正式输出模板必须同步更新；否则不算修好。

---

## 一、统一抓取与落盘规则

### 1. 抓取单位
后续所有表都按 `dataset_id + source_path` 唯一锁定。

### 2. 落盘路径
每次抓取必须落盘到：

`projects/duanxianxia/captures/YYYY-MM-DD/<dataset-id>/<HHMMSS>.json`

### 3. 落盘 JSON 固定字段
每次抓取落盘至少保留：
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

### 4. 用户可见输出规则
- 默认使用**完整中文表头**。
- 默认使用**固定列顺序**。
- 默认不输出英文字段名。
- 只有用户明确要求 `原始字段 / 英文表头 / CSV / JSON 原样` 时，才切回原始字段。
- **每次下载/抓取完成后，必须有 webhook 推送通知。**
- **每次通知与用户可见回执，都必须明确说明：是否下载成功、是否完整、是否有失败、是否有遗漏。**
- 回执至少应包含：`dataset_id / dataset_label / source_url / fetched_at / row_count / capture_path / saved / success / complete / failed_items / missing_items`。

---

## 二、固定数据集合同

### 1) `rank.rocket`｜飙升榜
- `source_path`: `飙升榜`
- 主抓取源：`https://x.duanxianxia.cn/vendor/stockdata/hotlist.json`
- 取数字段：`skyrocket_hour`
- 正式表头：`排名 | 代码 | 名称 | 飙升值`

### 2) `pool.hot`｜热门
- `source_path`: `股票池/热门`
- 主抓取源：`https://duanxianxia.com/data/getFxPoolData/`
- 正式表头：`代码 | 名称 | 涨幅 | 主力 | 实际换手 | 成交 | 流通 | 概念`
- 说明：`热门` 与 `热度榜（日）` 严格区分，不得混用。

### 3) `pool.surge`｜冲涨
- `source_path`: `股票池/冲涨`
- 主抓取源：`https://duanxianxia.com/data/getCzPoolData/`
- 正式表头：`排名 | 代码 | 名称 | 涨幅 | 换手比 | 成交 | 流通市值 | 概念1 | 概念2`

### 4) `rank.hot_stock_day`｜热度榜（日）
- `source_path`: `热度榜（日）`
- 主抓取源：`https://x.duanxianxia.cn/vendor/stockdata/hotlist.json`
- 取数字段：`hot_stock_day`
- 正式表头：`排名 | 代码 | 名称 | 热度值`

### 5) `review.daily.top_metrics`｜每日复盘顶部指标
- `source_path`: `复盘/每日复盘`
- 主抓取源：`https://duanxianxia.com/api/getChartByQingxu`
- 正式表头：`序号 | 指标键 | 指标名称 | 指标分组 | 分类 | 展示名称 | 日期 | 数值 | 晋级率 | 晋级数 | 样本数 | 比值 | 原值`
- `连板晋级率` 不能再只保留旧的单个字符串值；必须同时保留：
  - **总体行**：`连板总体晋级率`
  - **分层行**：`最高板晋级率 | 1进2晋级率 | 2进3晋级率 | 3进4晋级率 | 4板+晋级率`
- 连板晋级率相关行必须补充字段：
  - `指标分组` = `连板晋级率`
  - `分类` = `总体` / `分层`
  - `展示名称`
  - `晋级率`
  - `晋级数`
  - `样本数`
  - `比值`
  - `原值`（仅总体行保留原网页旧串，如 `2:55`）

### 6) `review.ltgd.range`｜龙头高度区间涨幅
- `source_path`: `复盘/龙头高度`
- 主抓取源：`https://duanxianxia.com/api/getZfByDate`
- 必须固定输出为 **4 个周期表**：`5日 | 10日 | 20日 | 50日`
- 每个周期表内固定分组：`主板 | 创业科创板 | 北交所`
- 正式表头：`周期 | 板块 | 排名 | 代码 | 名称 | 区间涨幅 | 概念 | 日期区间`

### 6.1) `review.fupan.plate`｜涨停复盘（按概念）
- `source_path`: `复盘/涨停复盘（按概念）`
- 主抓取源：`https://duanxianxia.com/api/getFupanByYidong`
- 固定请求口径：`type=plate`
- 数据本质：**动态题材标签 + 题材下对应股票明细**，不是现有盘前/盘中/资金流向榜单的换皮。
- 每条明细必须保留：
  - 题材层：`日期 | 题材序号 | 题材名称 | 题材说明 | 题材涨停数 | 题材股票数`
  - 股票层：`题材内序号 | 名称 | 代码 | 股价 | 涨幅 | 涨停类型 | 板数 | 连板 | 首次封板 | 最后封板 | 开板 | 封单额 | 成交额 | 换手率 | 实际流通 | 流通市值 | 总市值 | 异动原因 | 异动原因详情 | 龙虎榜`
- 正式表头默认输出：`日期 | 题材序号 | 题材名称 | 题材说明 | 题材涨停数 | 题材内序号 | 名称 | 代码 | 涨幅 | 板数 | 连板 | 封单额 | 成交额 | 异动原因`
- `meta` 中必须额外保留：
  - `topic_count`
  - `topics`
  - `htmlcopy_metrics`（如情绪指标、涨停家数、跌停家数、封板率、涨停表现、连板表现）
  - `fine_tag_summary`（从题材名称 / 题材说明 / 异动原因 / 异动原因详情中抽取细标签命中汇总）
- 每条股票明细允许额外保留：
  - `细标签`
  - `细标签列表`
- 当前重点细标签观察词包括：`数据中心`、`液冷服务器`、`算力租赁`、`GPUNAS`、`英伟达概念`、`训推一体机`、`东数西算`、`云计算`、`算力调度`、`国资云`、`液冷`、`算力`、`服务器`

### 7) `auction.jjyd.vratio`｜竞价爆量
- `source_path`: `竞价/竞价异动/竞价爆量`
- 主抓取源：`https://duanxianxia.com/data/getVratioData/11`
- 正式表头：`名称 | 代码 | 涨幅 | 竞额 | 昨竞额 | 竞价换手 | 竞价量比 | 竞涨 | 流通值 | 概念`

### 8) `auction.jjyd.qiangchou`｜竞价抢筹
- `source_path`: `竞价/竞价异动/竞价抢筹`
- 主抓取源：`https://duanxianxia.com/data/getQiangchouData/11`
- 必须固定拆成两张表：
  - `9:20 - 9:25的抢筹幅度`
  - `竞价最后1秒的抢筹幅度`
- 两张表正式表头一致：`名称 | 代码 | 涨幅 | 竞额 | 抢筹幅度 | 竞价换手 | 竞涨 | 流通值 | 概念`
- 不得只发其中一组。

### 9) `auction.jjyd.net_amount`｜竞价净额
- `source_path`: `竞价/竞价异动/竞价净额`
- 默认主抓取源固定为：`https://duanxianxia.com/vendor/stockdata/jjzhuli.json`
- 旧 `getDabanData` 只保留作历史对照，不再作为正式默认主源
- 正式表头：`名称 | 代码 | 涨幅 | 竞价换手 | 竞涨 | 主力净买 | 竞额 | 流通值 | 概念1 | 概念2`

### 10) `auction.jjlive.fengdan`｜竞价封单
- `source_path`: `竞价/竞价封单`
- 主抓取源：
  - 表结构与 `9:15/9:20/9:25`：`vendor/stockdata/jjlive.json`
  - 当天 live `涨幅`：`qt.gtimg.cn` 实时报价覆盖
- 正式表头：`名称 | 代码 | 题材1 | 题材2 | 连板标签 | 9:15 | 9:20 | 9:25 | 涨幅`
- 数值列顺序固定为：`9:15 | 9:20 | 9:25 | 涨幅`

---

### 6.2) `home.kaipan.plate.summary`｜主页板块强度全主标签汇总表
- `source_path`: `主页/qxlive/全主标签/主标签字段+子标签列表`
- 主抓取入口：`https://duanxianxia.com/web/qxlive`
- 前端真实链路：
  - 顶层题材强度：`/api/getLiveByStrong`（`platetype=strong`）
  - 顶层主力流入：`/api/getLiveByStrong`（`platetype=money`）
  - 子题材列表：`/data/getKaipanSubPlate`
- 数据语义：**主页中间左侧 qxlive iframe 的全主标签汇总结果**，每个主标签一行，不再包含子标签个股明细。
- 当前固化口径：**默认遍历页面全部主标签**，不再只抓首个主标签；同时按主标签代码合并 `strong` 与 `money` 两套结果。
- 每条明细至少保留：
  - `主标签序号 | 主标签名称 | 主标签代码`
  - `板块强度`
  - `主力流入`
  - `涨停数量`
  - `子标签数量`
  - `子标签列表`
- 正式表头默认输出：`主标签序号 | 主标签名称 | 主标签代码 | 板块强度 | 主力流入 | 涨停数量 | 子标签数量 | 子标签列表`
- `meta` 中必须额外保留：
  - `table_headers`
  - `selected_top_plate`
  - `top_plates`
  - `top_plate_summaries`
  - `subplate_count`
  - `subplates`
  - `failed_items`
  - `missing_items`
  - `complete`

### 6.3) `home.qxlive.top_metrics`｜主页 qxlive 顶部指标按钮组
- `source_path`: `主页/qxlive/顶部指标按钮组`
- 主抓取入口：`https://duanxianxia.com/web/qxlive`
- 前端真实链路：
  - 当前图表序列：`/vendor/stockdata/platechart1.json`（解密后读取 `qxlive.series`）
  - 按钮当前值：`/api/getLastQxlive`（读取 `qxlast`）
  - 页面按钮 DOM：`button.chart`
- 数据语义：**主页 qxlive 顶部那组按钮的当前显示值与对应序列尾点**，不是 `review.daily.top_metrics`，也不是复盘链路替代品。
- 当前固定按钮口径：`QX | ZT | DT | KQXY | HSLN | LBGD | SZ | XD | PB | ZTBX | LBBX | PBBX`
- 当前固定正式表头：`序号 | 指标键 | 指标名称 | 日期 | 时间点 | 当前值 | 按钮显示值 | 图表尾值 | 对照值 | 按钮ID | 按钮文本`
- 字段解释：
  - `当前值`：以主页按钮当前显示值为准
  - `按钮显示值`：直接从按钮文本解析出的显示值
  - `图表尾值`：`qxlive.series` 尾点值
  - `对照值`：仅在需要专门对照时保留
- `PBBX` 特殊规则：
  - 这里的 `PBBX` 指 **沪深5分钟量能**，不是复盘里的“连板晋级率”
  - `当前值` 取 `qxlast.PBBX`（如 `2:55`）
  - `图表尾值` 取 `qxlive.series.JRLN`
  - `对照值` 取 `qxlast.ZRLN`
- `meta.mapping_notes` 中必须明确写出 `PBBX` 的特殊映射，避免后续再次和复盘 `PBBX` 混淆。

---

## 三、按交易时段固定分组

后续固定使用下面这套编号与分组，不得混：

### 盘前（4 张固定表 + 2 张独立补充表）
- 7 = `auction.jjyd.vratio`｜竞价爆量
- 8 = `auction.jjyd.qiangchou`｜竞价抢筹
- 9 = `auction.jjyd.net_amount`｜竞价净额
- 10 = `auction.jjlive.fengdan`｜竞价封单
- 16 = `home.kaipan.plate.summary`｜主页板块强度全主标签汇总表
- 17 = `home.qxlive.top_metrics`｜主页 qxlive 顶部指标按钮组

**执行规则**
- 北京时间每个工作日上午 **9:25** 自动抓取并保存
- 随机延迟：**5–15 秒**
- 动作：自动抓取 + 自动落盘保存
- 用户最新确认：原来的股票覆盖型第5表不要了，直接替换为主标签汇总型第5表；继续放进盘前一起下载，并作为盘前分析的市场主标签视角输入。

### 盘中（5 张）
- 1 = `rank.rocket`｜飙升榜
- 4 = `rank.hot_stock_day`｜热度榜（日）
- 2 = `pool.hot`｜热门
- 3 = `pool.surge`｜冲涨
- 17 = `home.qxlive.top_metrics`｜主页 qxlive 顶部指标按钮组

**执行规则**
- 盘中不自动定时抓取
- 由用户在盘中随时下指令后再抓取并保存
- 后续回答时必须知道这 4 张属于盘中，不能和盘前/盘后混淆

### 盘后（2 张）
- 6 = `review.daily.top_metrics`｜每日复盘顶部指标
- 5 = `review.ltgd.range`｜龙头高度区间涨幅

### 盘后补充研究表（新增）
- 15 = `review.fupan.plate`｜涨停复盘（按概念）

### 盘后最终刷新榜单（2026-04-16 新增正式规则）
- 1 = `rank.rocket`｜飙升榜（盘后最终刷新）
- 4 = `rank.hot_stock_day`｜热度榜（日）（盘后最终刷新）

**执行规则**
- 先接入为可手动抓取、可随盘后批量抓取的研究型数据集。
- 在用户进一步确认前，默认不单独新增新的 cron 时点；但可并入现有 `postmarket` / `postmarket_cashflow` 批量链路。

**执行规则**
- 北京时间每个工作日 **17:20** 自动抓取并保存
- 随机延迟：**0–5 分钟**
- 动作：自动抓取 + 自动落盘保存
- `rank.rocket` 与 `rank.hot_stock_day` 虽然属于盘中动态榜单，但用户已于 2026-04-16 明确要求：**盘后也必须重新抓取**，因为这两张榜单在交易时段持续变化，盘后分析应使用收盘后的最终态。
- 因此后续 `postmarket` / `postmarket_cashflow` 的正式口径中，默认包含这两张榜单的盘后最终刷新版本。

### 日线下载池构建契约（2026-04-16 更新）
- 当日日线下载池默认由两部分合并去重构成：
  1. 当日正式盘前 / 盘中 / 盘后 / 资金流向 report 所引用 capture 中出现过的全部股票；
  2. 上一交易日在 Feishu 多维表 `短线侠推荐复盘` 中写入的**所有正式推荐股票**。
- 过滤规则：
  - `home.kaipan.plate.summary` 这类非个股表不贡献股票代码；
  - `推荐分级` 中包含 `不建议` 的记录不并入“上一交易日正式推荐股票”；
  - 仅保留可识别的 A 股代码。
- 若当天盘后 report 已包含重新抓取的 `rank.rocket` / `rank.hot_stock_day`，则日线池构建时默认采用它们的**盘后最终刷新版本**。
- manifest 中的标记字段已从 `前一交易日盘后正式推荐` 升级为 `前一交易日正式推荐`。

### webhook Feishu 卡片展示契约（2026-04-16 固化）
- 默认目标：**每个数据集对应一张明细卡**，而不是把同一张表拆成很多张。
- 默认行数上限：**30 行**。
- 若原表超过 30 行：
  - 只展示前 `30` 行；
  - 备注中必须明确写：`仅展示前 30/N 行`。
- 若某张卡虽然不超过 30 行，但仍因为内容过大而触发 Feishu 自定义机器人卡片大小限制，则允许发送前继续**自动减少展示行数**，直到进入安全范围。
- 这一自动缩行兜底目前尤其适用于单行文本特别长的数据集，例如：
  - `review.fupan.plate`
  - `home.ztpool`
- `auction.jjyd.qiangchou` 为强制特例：
  - 必须同时保留两个分组：
    - `9:20-9:25 抢筹幅度`
    - `竞价最后1秒 抢筹幅度`
  - 两个分组分别独立展示；
  - 每个分组各自最多 30 行，并可在必要时继续自动缩行。
- `home.kaipan.plate.summary` 的 `主力流入` 字段当前正式口径：
  - `platetype=money` 返回的原始 `val` 按**万元**理解；
  - 用户可见展示再换算为 `万/亿`；
  - 示例：`1312315 -> 131.2亿`；
  - 旧的 `131万` 口径已确认错误，不得继续沿用。
- 对用户可见的 webhook 卡片表头，默认继续使用**中文表头**。

---

## 四、变更门槛

后续若要改以下任一项，必须视为**正式口径变更**：
- 主抓取源
- dataset_id / source_path 绑定
- 正式表头
- 字段顺序
- 拆表方式
- 落盘结构

一旦变更，必须同步处理：
1. 抓取代码
2. 文档（`README.md` / `user-contract.md` / 本文件）
3. 历史错误落盘的隔离或说明
4. 用户可见正式导出模板

否则不算修好。
