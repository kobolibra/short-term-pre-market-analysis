# duanxianxia v10 重构 · 全量交接文档

> **新对话开场必读顺序**：
> 1. 本文件（`docs/HANDOFF.md`）← 先读这个
> 2. `docs/canonical-field-dictionary.md` ← 字段 source of truth
> 3. `docs/v10-field-alignment-decisions.md` ← 因子 source of truth
> 4. `docs/rebuild-design-v10.md` ← 架构决策
> 5. agent-results 分支：`reports/_audit/agent_jobs/0089_unit_probe_20260629.result.json` ← **读完再推进 Task 0090**
>
> 读完以上 5 份即可无缝衔接。其余文档（project-handbook-current.md / field-rename-map.md）按需查阅。

最后更新：2026-06-29（对话结束时全量更新）

---

## 一、项目定位与整体架构

从 `https://duanxianxia.com/` 抓取 15+ 张盘前/盘中/盘后数据集，目标是建立完整量化盘前分析流水线：

```
raw[] 位置数组（ground truth）
  └─[transform 1]─> fetcher parse -> capture 落盘（named rows，当前有 mislabel 待修正）
       └─[transform 2]─> loader 时间切片 -> flat feature tables
            └─> v10 edge_core 评分 -> 飞书 webhook 推送
```

**当前阶段**：正在做 transform 1 的修正（canonical 层），Jobs 0001–0089 已完成。  
**下一步**：Task 0090 — 新建 `scripts/duanxianxia_canonical.py`。

**Repo**: `kobolibra/short-term-pre-market-analysis`  
**main HEAD**: `d3320d163411bca4ee22de8cd59ca155f76d1831`  
**agent-results 分支**: `a9af5a4b732e3f4196c82d513d7a9e81e341d7fd`  
**服务器项目根**: `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia`  
**fetcher 现行版本 SHA**: `d61c7be5`（`scripts/duanxianxia_fetcher.py`）

### 1.1 与"对接方"的协作背景

有一个 **other-agent**（对接方因子框架），它消费我们的数据并使用特定字段名（bidAmount、bidStrength、volumeRatio 等）。v10 rebuild 的目标之一是把我们的数据口径与对接方对齐，避免字段名和单位不一致导致因子计算错误。所有字段语义 FINAL，对接方已被告知 circMcap = FF。

---

## 二、数据集全览（15 张表）

### 2.1 竞价五表（盘前核心）

| dataset_id | 中文名 | 端点 | 认证 |
|---|---|---|---|
| auction.jjyd.vratio | 竞价爆量 | `POST duanxianxia.com/data/getVratioData/11` | 无 |
| auction.jjyd.qiangchou | 竞价抢筹 | `POST duanxianxia.com/data/getQiangchouData/11` | 无 |
| auction.jjyd.net_amount | 竞价净额 | `GET ds.duanxianxia.com/vendor/stockdata/jjzhuli.json` | AES |
| auction.jjyd.weimai | 涨停委买 | `GET duanxianxia.com/vendor/stockdata/daban.json` | AES |
| auction.jjlive.fengdan | 竞价封单 | `GET ds.duanxianxia.com/vendor/stockdata/jjlive.json` + `qt.gtimg.cn` | AES+行情 |

### 2.2 其余盘前

| dataset_id | 端点 |
|---|---|
| home.kaipan.plate.summary | `/api/getLiveByStrong(strong/money)` + `/data/getKaipanSubPlate` |
| home.qxlive.top_metrics | `/vendor/stockdata/platechart1.json` + `/api/getLastQxlive` |

### 2.3 盘中

| dataset_id | 端点 | 备注 |
|---|---|---|
| pool.hot | `POST /data/getFxPoolData/{sort}` | 无历史 raw！ |
| pool.surge | `POST /data/getCzPoolData/{sort}` | raw 有存 |
| rank.rocket | `GET x.duanxianxia.cn/vendor/stockdata/hotlist.json → skyrocket_hour` | |
| rank.hot_stock_day | 同上 → `hot_stock_hour` | ⚠️ 旧代码读 hot_stock_day（不存在）→ 0 行 |

### 2.4 盘后

| dataset_id | 端点 |
|---|---|
| review.fupan.plate | `POST /api/getFupanByYidong?type=plate` |
| review.ltgd.range | `POST /api/getZfByDate` |
| review.daily.top_metrics | `POST /api/getChartByQingxu` |
| home.ztpool | `GET duanxianxia.com/vendor/stockdata/jinjidata.json`（playwright） |
| cashflow.stock.{today/3day/5day/10day} | `stock.9fzt.com/cashFlow/stock.html` |

### 2.5 cron 调度

| 任务 | cron（Asia/Shanghai）| 内容 |
|---|---|---|
| 盘前 | `25 9 * * 1-5` | premarket：竞价5表+kaipan+qxlive，随机延迟5–15秒 |
| 10:01 盘中+资金 | `1 10 * * 1-5` | intraday_cashflow，随机延迟0–45秒 |
| 盘后 | `20 17 * * 1-5` | postmarket_cashflow，随机延迟0–5分钟 |

cron worker 幂等；队列 `scripts/agent_jobs/queue/<id>.json`；results 推 agent-results 分支（publish 有 ~10 分钟延迟）。

### 2.6 AES 解密参数

```python
key = 'secretkey322yes!!aaaaaaaaaaaaaaa'
iv  = 'fixediv_16valued'
# CBC + PKCS7 unpad + base64
```

---

## 三、最关键结论（不要再验证，直接使用）

### 3.1 vratio / qiangchou raw[2] = FF 自由流通市值（亿），不是量比

**三次 live API 实证一致**：
- 多氟多（002407）raw[2]=462 ≈ 461.78亿（weimai raw[12] 已确认 FF）✓
- 长虹美菱（000521）raw[2]=34 ≈ 34亿小盘股 ✓
- 华工科技（000988）raw[2]=1441 ≈ 大市值科技股 ✓

`auction_volume_ratio` 这个字段名是 **mislabel**，实际存的是 FF 市值（亿）。  
**raw[11]** 才是真正的量比（vratio 里是 `volume_ratio_multiple`，qiangchou 里是 `grab_strength`）。

`field-rename-map.md` 的改造方向一直是正确的，直接按它执行，不要动摇。

### 3.2 全局市值口径：circMcap = FF（已告知对接方）

所有市值相关因子分母统一用 FF，单位统一转为**元**存储（亿×1e8，万×1e4）。

| 表 | 市值字段位置 | 口径 | 原标签（旧/错） |
|---|---|---|---|
| vratio | raw[2] | FF/亿 | auction_volume_ratio（MISLABEL）|
| qiangchou | raw[2] | FF/亿 | auction_volume_ratio（MISLABEL）|
| net_amount | raw[6] | FF/亿 | market_cap_yi |
| weimai | raw[12] | FF/元 | market_cap（job 0078 实证）|
| pool.hot | item[9] | FF | "流通"（MISLABEL，实为FF）|
| pool.surge | item[9] | FLOAT | float_market_cap（标签正确，唯一用FLOAT的表）|
| review.fupan.plate | 多列 | FF+FLOAT+TOTAL | 实际流通=FF / 流通市值=FLOAT / 总市值=TOTAL |

`review.fupan.plate` 是唯一同时有三种市值的表，是**校准锚表**。

---

## 四、字段口径全量修正表

> 完整版见 `docs/canonical-field-dictionary.md`，这里列修正要点和快查。

### 4.1 vratio（raw 13列）

```
[0]  code
[1]  name
[2]  free_float_mktcap    FF/亿→元    ← 旧标签 auction_volume_ratio，MISLABEL
[3]  seal_amount          万→元       封单额（覆盖率仅3%，大多数 null）
[4]  auction_change_pct   %
[5]  latest_change_pct    %
[6]  auction_turnover     万→元       = bidAmount
[7]  concept              text
[8]  文本重复（latest_change_pct text）
[9]  文本重复（auction_turnover text）
[10] yesterday_auction_turnover_wan  万→元
[11] volume_ratio         倍          ← 这才是量比（volumeRatio）
[12] turnover_rate        %
```

### 4.2 qiangchou（raw 13列，同 vratio 除）

- raw[11] = `grab_strength`（抢筹幅度，不是量比）
- response 结构：`{list: {grab: [...], qiangchou: [...]}}`
- **必须保留两个 group**：`grab`（竞价最后1秒）+ `qiangchou`（9:20–9:25）

### 4.3 net_amount（raw 9列）

```
[0]code [1]name
[2]auction_change_pct(%) [3]latest_change_pct(%)
[4]main_net_inflow(万→元) [5]auction_turnover(万→元)
[6]free_float_mktcap(FF/亿→元)  ← 旧标签 market_cap_yi
[7]concept [8]turnover_rate(%)
```

### 4.4 weimai（raw 18列）

```
[0]code [1]name [2]price [3]latest_change_pct(%)
[4]auction_turnover(元)   [5]auction_change_pct(%)
[6]main_net_inflow(元)    [7]turnover_rate(%)
[8]seal_volume            [9]auction_amount
[10]seal_volume_again     [11]concept
[12]free_float_mktcap(FF/元)   ← 旧标签 market_cap，job 0078 实证 FF
[13]main_net_inflow_full        ← 单位待 0089 确认（万 or 元？）
[14]super_large_order           ← 单位待 0089 确认；与[13] spearman=−0.919 高度冗余
[15]large_order                 ← 单位待 0089 确认
[16]board_label          连板标签  ← IC 分析显示昨3连板最优（见 §六）
[17]seal_amount           万→元？  ← 单位待 0089 确认
```

### 4.5 fengdan（jjlive，AES）

结构：section 聚合行 + per-stock 明细行。

**Section header（每个涨停价委买阶段的汇总）**：
```
section_date / kind / yizi_count / seal_total / t15_total / t20_total / t25_total / has_change_pct
```
- `t15/t20/t25_total` = **金额**（元），不是计数！（job 0083 实证：t15=150.4亿 > t20=39.1亿 > seal_total=54.4亿）
- `yizi_count` = 唯一的计数字段（亿字股票数）

**Per-stock 明细**：
```
rank / code / name / tag_1 / tag_2 / tag_3 / board_label
/ amount_915 / amount_920 / amount_925  ← 9:15/9:20/9:25 各时间点涨停价委买/封单累计额（元）
/ latest_change_pct / latest_change_pct_source / tags
```
- `amount_920` = limitBuyAmountAfter920（9:20 不可撤委买，非成交；job 0082 实证）
- `amount_925` 值为 `-` 表示该股 9:25 时**未封板**，需做 null 处理
- 默认使用 `amount_920`（第一个不可撤快照）
- latest_change_pct 从 `qt.gtimg.cn` 实时覆盖

### 4.6 pool.hot（item 索引）

```
item[2]change_pct  item[6]concept  item[7]board_state（⚠️当前被丢弃！）
item[8]turnover    item[9]free_float_mktcap(FF)   ← 旧标签"流通"，MISLABEL
item[10]main_net   item[11]real_turnover_rate
```
⚠️ **无历史 raw**，历史数据 legacy_unrecoverable，从现在起必须存 raw[]。

### 4.7 pool.surge（item 索引）

```
item[2]change_pct  item[6]concept  item[7]board_state（当前被丢弃！）
item[8]turnover_amount  item[9]float_mktcap(FLOAT)  item[10]turnover_rate_site
```
- item[9] 是 FLOAT（流通市值），标签正确，是本项目**唯一用 FLOAT 的表**
- ⚠️ BUG：当前用 item[8]/item[9]*100 重算换手率，与 site item[10] 不符 → Fix：取 site item[10]

### 4.8 ztpool（涨停晋级阶梯）

数据来自 `jinjidata.json`，结构是 `{html: "...", date: "..."}` 中的 HTML 字符串，需用 token grammar 解析：

```
col1 阶梯分组: 首板 / 1进2 / 2进3 / ...
col2 晋级率: 晋级数/样本数=百分比
col3 个股 token 格式: <@>{市场} <#'{code}'>{name}（{状态: 成/炸/败}）[{涨幅}] {题材}
```

canonical 字段：日期/分组序号/分组名称/组内序号/晋级率文本/晋级数/样本数/晋级率/市场/代码/名称/**状态**/涨幅/题材  
`状态`：成=封住 / 炸=炸板 / 败=未涨停

⚠️ source_url 存储时含字面量 `" + "`，需 fix 字符串拼接。

---

## 五、v10 因子框架（FINAL）

### 5.1 edge_core 公式

```
0.23 × auction_amount_pct
+ 0.19 × auction_strength
+ 0.18 × liquidity
+ 0.14 × money
+ 0.14 × pressure_score
+ 0.08 × weimai_strength
+ 0.05 × orderbook
− risk_penalty
```

用校正后的 canonical 输入**重拟合**系数（Task 0093）。

### 5.2 逐因子 canonical 对应（FINAL）

| 因子 | canonical 来源 | 单位/说明 |
|---|---|---|
| bidAmount | auction_turnover（竞价五表均有）| 万→元 |
| bidStrength | auction_turnover / free_float_mktcap × 10000 | 两者同元基准 |
| volumeRatio | vratio raw[11]（volume_ratio）| **不是 raw[2]！** 倍 |
| changeRate | auction_change_pct（竞价涨幅）| % |
| limitBuyAmountAfter920 | fengdan amount_920 | 9:20不可撤委买，元；amount_925="-"=未封 |
| prevStreak | fupan 连板 / ztpool 阶梯 | |
| prevOpenNum | fupan 开板数 | |
| brokenLimitUp | ztpool 状态=炸 | |
| origin | 派生布尔（见5.3）| |
| stockMainlineFit | concept vs kaipan top 板块强度 | |
| sentimentSignal | QX-live QX 值 | |
| themeConsistency | count(H)/count(Q)（见5.4）| 题材内一致性 |
| themeConcentration | 题材 bidAmount / 全市场 bidAmount | |
| prevDayLimitUpSealRate | sealedLimitUp / touchedLimitUp T-1 EOD（见5.5）| |
| auctionSealAmount | fengdan section_t25_total / section_seal_total | 9:25封单强度，金额比 |
| marketSealRate | QX-live PB 今日封板率 | ~9:25 premarket-safe，metric_key=PB |

### 5.3 origin 精确定义

```python
fromPrevBrokenLimitUp         = (prev_day_状态 in {'炸', '败'})       # ztpool/jinjidata
fromPrevSealedLimitUpWithOpen = (prev_day_状态 == '成') AND (fupan_开板 > 0)
```

### 5.4 themeConsistency / themeConcentration 精确定义

```python
# themeConsistency（题材内高开一致性）
M(theme) = {股票 | concept == theme}                       # 来自竞价五表
Q(theme) = {i in M | auction_turnover_i >= minBidAmount    # 过滤小额，排除ST
             AND NOT ST(i)}
H(theme) = {i in Q | auction_change_pct_i > 0}             # 高开（竞价涨幅>0）
themeConsistency(theme) = len(H) / len(Q)

# Strict variant（我们自己的 threshold）：
#   将 auction_change_pct_i > 0  改为  auction_change_pct_i >= auctionChgMin

# themeConcentration（资金集中度）
themeBidAmount(theme) = sum(auction_turnover_i for i in M(theme))
themeConcentration(theme) = themeBidAmount(theme) / sum(all themes' themeBidAmount)
```

数据来源：vratio/qiangchou/net_amount/weimai 均有 concept + auction_turnover + auction_change_pct，可直接计算。

**minBidAmount / auctionChgMin**：我们自己调参（尚未定，Task 0093 时确定）。

### 5.5 三种封板率精确定义（DO NOT 混用）

```python
# 3a. prevDayLimitUpSealRate（T-1 EOD，市场级，premarket-safe）
sealedLimitUp  = num          # T-1 收盘仍封住数
touchedLimitUp = num + open   # 曾触及 = 封住 + 炸板/开板（已验证：64+42=106）
prevDayLimitUpSealRate = sealedLimitUp / touchedLimitUp
# 数据来源：review.fupan.plate 或 ztpool 历史数据（T-1）

# 3b-i. auctionSealAmount（T0 ~9:25，fengdan，资金强度，非计数比）
auctionSealAmount = section_t25_total / section_seal_total
# section_t25_total = 9:25时涨停价委买金额总量（元）
# section_seal_total = 封板总金额（元）
# job 0083 验证：全是金额，唯一计数字段=yizi_count

# 3b-ii. marketSealRate（T0 ~9:25，QX-live PB，计数比，premarket-safe）
# metric_key='PB'，value 例如 63.0%（= 封板数/曾触及涨停数）
# 随其他 QX-live 指标在 ~9:25 盘前批次一起抓取
# 偶尔出现 10:04 时间戳 = 调度偏差，不是设计问题 → Task 0094 pin 到竞价窗口
```

**三者互补，各自独立，永远不要互相替代。**

---

## 六、关键 IC 发现与因子结论

### 6.1 IC 汇总

| 因子 | mean_IC | ICIR | 来源 | 备注 |
|---|---|---|---|---|
| rocket_rank | 0.222 | 1.027 | v36 | 最强单因子 |
| seal_to_mcap_ratio | 0.123 | 0.318 | v36 | |
| big_order_share | 0.111 | 0.514 | v36 | |
| latest_change_pct（vratio）| 0.119 | 0.369 | v48 | |
| auction_turnover_wan（vratio）| 0.093 | 0.487 | v48 | |
| auction_volume_ratio（vratio）| 0.058 | 0.335 | v48 | ⚠️ 实为FF市值的IC，字段名是mislabel |
| main_net_inflow_full（weimai）| 0.103 | — | v50 | 最高 weimai 因子 |
| super_large_order（weimai）| 0.094 | — | v50 | 与 main_net_inflow_full spearman=−0.919 冗余 |
| market_cap_yi（weimai）| 0.069 | — | v50 | = FF市值 |
| board_label（weimai）| — | — | v50 | **昨3连板最优**，离散因子 |
| ic_amount（firstprinciples）| 0.134 | 0.596 | v65 | |
| ic_turnrate（firstprinciples）| 0.122 | 0.664 | v65 | |
| ic_composite（amount+turnrate）| 0.136 | 0.764 | v65 | 轻微优于单一 |
| ic_gap 全样本 | 0.168 | 0.494 | v65 | |
| ic_gap 冷场景 | 0.273 | 0.809 | v65 | 热场景 IC 的 ~6× |
| ic_gap 热场景 | 0.044 | 0.158 | v65 | |
| 小市值三分组超额（<100亿）| +0.110 | — | v65 | 去掉2个离群日后 +0.262 |

### 6.2 关键结论

- `super_large` ~ `large_order` spearman = −0.919：**高度冗余，合并或选其一**
- amount 与 turnover_rate 相关 r=0.627：正交化后 IC 均降低，**composite 轻微最优**
- gap 因子：**冷场景 IC 是热场景 2× 以上** → 必须 REGIME_ACTION_GATE 分场景加权
- 小市值股超额持续 → 市值分层不可忽略
- board_label 昨3连板最优（离散变量，需 one-hot 或分组处理）

### 6.3 REGIME_ACTION_GATE（场景门控）

根据 QX 情绪值（median=29.0）将市场划分为热/冷场景：
- **热场景**（QX 高）：总体 IC 较低，top5 原始超额 +0.43%
- **冷场景**（QX 低）：总体 IC 高，top5 原始超额 +3.25%
- `corr(QX, core_ic)` = −0.024（接近0），说明 QX 不直接 drive IC
- 结论：edge_core 在冷场景显著更有效，需在冷场景加大权重或提高置信阈值

---

## 七、已完成工作（Jobs 0001–0089）

| Jobs | 关键内容 |
|---|---|
| 0001–0044 | 抓取器建立（fetcher.py）、AES 解密、落盘规范（captures/YYYY-MM-DD/...）、飞书推送全链路 |
| 0045 | premarket_raw_capture_audit_v36：22 交易日 × 9 数据集，schema IC 全量审计 |
| 0058–0063 | v10 factor IC 矩阵初版（lagged_context_ic_v43 等）|
| 0075–0078 | 市值口径全表核查；job 0078 用 real data 实证 net_amount item[6] / weimai item[12] 均为 FF |
| 0079 | blast_radius（9.2MB）：所有下游消费方字段防御键全清单，影响评估 |
| 0080 | field_caliber_dump（2026-06-29 08:10）：当日可抓表 mcap_fields_by_name 核查（注：当日未开市，不含竞价表）|
| 0082 | limitBuyAmountAfter920 = fengdan amount_920 确认（非成交；non-monotonic 9:15→9:20 drop 证明）|
| 0083 | fengdan section_* 全是金额（t15=150.4亿 > t20=39.1亿 > seal_total=54.4亿；yizi_count 唯一计数）|
| 0084 | QX-live PB = 今日封板率（metric_key=PB，value=63.0%）✓，~9:25 premarket-safe |
| 0085 | weimai deepdive v50：main_net_inflow_full IC=0.103（最高）；board_label 昨3连板最优 |
| 0086 | firstprinciples v65：amount/换手正交化（r=0.627）；gap 非线性（冷场景 IC=0.273）；小市值超额 |
| 0087 | PR#28 squash→main（commit 638dacf6）|
| 0088 | PR#29 squash→main（commit f20acdf）；fetcher 现行版本 SHA = d61c7be5 |
| 0089 | 探针脚本 0089_unit_probe_20260629.py 推 main（56aad925）；锁定4个未决单位 |

---

## 八、待验证 / 未落实项（新对话第一步必须处理）

### 8.1 ⚠️ 读 0089 探针结果（最高优先级）

**路径**（agent-results 分支）：
```
projects/duanxianxia/reports/_audit/agent_jobs/0089_unit_probe_20260629.result.json
```

探针目标是锁定以下未决单位，读完后才能写 canonical.py：

| 字段 | 表 | 当前疑问 |
|---|---|---|
| raw[17] seal_amount | weimai | 万 还是 元？（旧标签 seal_amount_wan 暗示万，但 raw 值不确定）|
| raw[13] main_net_inflow_full | weimai | 万 还是 元？|
| raw[14] super_large_order | weimai | 万 还是 元？|
| raw[15] large_order | weimai | 万 还是 元？|
| item[9] free_float_mktcap | pool.hot | 字符串"182亿"，parse 后是 182 还是 18200000000？|
| item[9] float_mktcap | pool.surge | 字符串"325亿"，parse 后单位？|

### 8.2 qt.gtimg 双花括号 bug

`scripts/duanxianxia_fetcher.py` 中 `_fetch_realtime_quotes`：
```python
url = f"{https://qt.gtimg.cn/q={symbols}}"  # 双花括号疑似 f-string bug
```
需运行时验证是否影响实际抓取。

### 8.3 pool.hot 历史数据问题

`pool.hot` **无历史 raw**，历史捕获数据 tag=`legacy_unrecoverable`，无法从 raw 重派生 canonical。  
从 Task 0091 起必须存 raw[]，store item[7] 板态。

---

## 九、任务路线图（按优先级）

### Task 0090 ← 立即开始（读完 0089 result 后）

**新建 `scripts/duanxianxia_canonical.py`**

```python
# 核心接口
def raw_to_canonical(dataset_id: str, raw_row: list) -> dict:
    """按 registry 把 raw[] 转为 canonical dict（含单位转换、字段重命名）"""
    ...

def _self_test():
    """硬编码真实样本断言，必须通过才允许 import"""
    # 多氟多 vratio raw[2]=462 → free_float_mktcap=46_200_000_000 元
    # vratio raw[11]=6.1 → volume_ratio=6.1 倍（不做单位转换）
    # 任何把 raw[2] 当量比或 raw[11] 当市值 → AssertionError
```

功能要求：
1. **Registry**：`dataset_id → {raw_kind, parse_spec, fields: [{canonical, caliber, unit, raw_ref}]}`
2. **`raw_to_canonical()`**：返回 canonical dict
3. 单位转换：亿×1e8→元，万×1e4→元，全在这里
4. 字段重命名：全在这里（按 canonical-field-dictionary.md）
5. **`_self_test()`**：硬编码真实样本断言，防止 mislabel 复发
6. **Caliber validator**：market_cap 类字段缺 caliber tag → build 报错

### Task 0091

**fetcher 接 canonical 层**
- parse 结果统一走 `canonical.raw_to_canonical()`
- 消费方防御键前置 canonical 名，保留旧 fallback 键名兼容（避免破坏现有消费方）
- `pool.hot`：开始存 raw[]，存 item[7] 板态
- fix `pool.surge`：turnover 取 site item[10]，stop recompute
- fix `rank.hot_stock_day`：改读 `hot_stock_hour`（旧读 `hot_stock_day` → 0 行）

### Task 0092

**历史重派生**
- 有 raw 的 capture（vratio/qiangchou/surge/net_amount/weimai）→ 从 raw 重派生 canonical 行（DO NOT 原地 sed 历史 JSON）
- `pool.hot` 无历史 raw → 打标 `legacy_unrecoverable`
- 重生 flat 特征 CSV（`_all_candidates_flat.csv`, `feature_matrix_v21.csv`）

### Task 0093

**新因子接线**
- origin（fromPrevBrokenLimitUp / fromPrevSealedLimitUpWithOpen）
- themeConsistency / themeConcentration（含 minBidAmount / auctionChgMin 阈值调参）
- auctionSealAmount / marketSealRate / prevDayLimitUpSealRate
- stockMainlineFit（concept vs kaipan top 板块强度）
- 接入 edge_core，用校正后 canonical 输入**重拟合系数**

### Task 0094

**Pin QX-live 抓取时间**：确保 `home.qxlive.top_metrics` 固定在 ~9:25 竞价窗口内抓取，避免偶发的盘后时间戳（如 10:04）污染 premarket 特征。

### Task 0095（Deferred）

T-1 lagged 表，搁置，后续专门处理。

---

## 十、代码 Bug 全清单（field-rename-map.md §10 完整版）

| # | 位置 | Bug | Fix |
|---|---|---|---|
| 1 | fetcher.py hotlist | 读 `hot_stock_day`（不存在）→ 始终 0 行 | 改读 `hot_stock_hour` |
| 2 | fetcher.py vratio/qiangchou | raw[2] 标签 `auction_volume_ratio`，是 mislabel | rename → `free_float_mktcap` |
| 3 | fetcher.py pool.hot | item[9]"流通" mislabel + item[7]板态丢弃 + 无 raw 存储 | 修正标签 + 存 raw[] + 保留 item[7] |
| 4 | fetcher.py pool.surge | 换手率重算（item[8]/item[9]*100）≠ site item[10] | 取 site item[10] |
| 5 | fetcher.py ztpool | `source_url` 含字面量 `" + "`（字符串拼接错误）| fix 字符串拼接 |
| 6 | fetcher.py fengdan | `_fetch_realtime_quotes` URL 双花括号 f-string 疑 bug | 运行时验证并修复 |

---

## 十一、关键审计文件索引

### docs/（main 分支，所有 source of truth 在这里）

| 文件 | 作用 | SHA |
|---|---|---|
| `HANDOFF.md` | 本文件，全量交接 | 本次更新 |
| `canonical-field-dictionary.md` | 字段口径最终 source of truth | eff62b07 |
| `v10-field-alignment-decisions.md` | 因子对接方案 FINAL | 672644348 |
| `rebuild-design-v10.md` | 四层架构 / keep vs rebuild / 迁移规则 | a450c931 |
| `field-rename-map.md` | 改造清单 + 代码 bug（方向全对，直接执行）| 62e94ff6 |
| `project-handbook-current.md` | 抓取规范（V9时代，V10部分未覆盖）| 7bac4fc3 |

### reports/_audit/（agent-results 分支）

| 文件 | 内容 | 注意 |
|---|---|---|
| `0089_unit_probe_20260629.result.json` | **待读！4 个未决单位** | 新对话必须先读 |
| `vratio_deepdive_v48.json` | vratio IC/覆盖率 | field_ic 里 `auction_volume_ratio` = FF市值的IC，不是量比的IC |
| `qiangchou_deepdive_v46.json` | qiangchou IC | 同上 |
| `weimai_deepdive_v50.json` | weimai IC | main_net_inflow_full=0.103最高；board_label昨3连板最优 |
| `firstprinciples_v65.json` | amount/换手正交、gap非线性、小市值 | qx_median=29.0 |
| `field_census_0076.json` | 各表 headers/sample（含 raw 样本）| mcap_fields_by_name 按名字搜索，vratio/qiangchou 会漏（名字是 mislabel）|
| `premarket_raw_capture_audit_v36.md` | 22 交易日全量 schema | |

---

## 十二、错误记录（勿重蹈）

| 错误 | 实际情况 | 教训 |
|---|---|---|
| 看到 raw[2]=17/499/1983 断言"是量比" | 这些就是各股 FF 市值（亿） | 数值判断必须交叉核对 live API + 已知 FF 值，不能凭感觉 |
| 用 `field_census.mcap_fields_by_name=[]` 证明"该表无市值" | census 按字段名关键词搜索，mislabel 的字段名就是搜不到 | census 的结论不等于值域结论，要结合值域或 live 比对验证 |
| 推翻 field-rename-map.md 的正确结论，导致多轮翻烧饼 | rename-map 是 jobs 0075–0081 + live endpoint probe 写下的，一直正确 | canonical-field-dictionary.md 和 field-rename-map.md 权威性高于单次名称普查，不要轻易推翻 |
