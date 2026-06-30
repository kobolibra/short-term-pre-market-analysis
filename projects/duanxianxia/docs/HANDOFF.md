# duanxianxia v10 重构 · 全量交接文档

> **新对话开场必读顺序**：
> 1. 本文件（`docs/HANDOFF.md`）← 先读这个
> 2. `docs/rebuild-plan-v11.md` ← **★ 最新决策：canonical-first 彻底重构，权威执行口径（覆盖 v10 的"逐步打补丁"做法）**
> 3. `docs/canonical-field-dictionary.md` ← 字段 source of truth
> 4. `docs/v10-field-alignment-decisions.md` ← 因子对着 FINAL
> 5. `docs/rebuild-design-v10.md` ← KEEP vs REBUILD + 迁移规则
>
> 读完以上即可无缝衔接。0089 探针结论已落地（见下），其余文档按需查阅。

最后更新：2026-06-30（v11 彻底重构定调 + canonical/routing 已上线）

---

## 一、项目定位与整体架构

从 `https://duanxianxia.com/` 抓取 15+ 张盘前/盘中/盘后数据集，目标是建立完整量化盘前分析流水线：

```
raw[] 位置数组（ground truth）
  └─[transform 1]─> fetcher parse -> capture 落盘（named rows，当前有 mislabel 待修正）
       └─[transform 2]─> loader 时间切片 -> flat feature tables
            └─> v10 edge_core 评分 -> 飞书 webhook 推送
```

**当前阶段**：transform-1 修正已落地——`duanxianxia_canonical.py`（口径单一事实源）+ `duanxianxia_canonical_routing.py`（kind→dataset 路由入口）均已上线并验证；seal_amount 单位（万→元）已实证确认并修复。
**v11 决策**：转入 **canonical-first 彻底重构**（详见 `docs/rebuild-plan-v11.md`）——冻结旧解析产物，一切从 raw 经 canonical 重新派生；不再对 105KB/145KB 巨型脚本做整体重写或 sed 改标签，让旧解析产物对下游不再产生影响。
**下一步（M1）**：新建 feature builder（L3 特征/加载层重构），读持久化 captures → `canonicalize_rows()` → 时间隔离特征表。

**Repo**: `kobolibra/short-term-pre-market-analysis`  
**main HEAD**: `e1ad579c0c7bbfd2ef461b82799428425e49917f`  
**agent-results 分支**: `a9af5a4b732e3f4196c82d513d7a9e81e341d7fd`  
**服务器项目根**: `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia`  
**fetcher 现行版本 SHA**: `d61c7be5`（`scripts/duanxianxia_fetcher.py`）

### 1.1 与对接方的协作背景

有一个 **other-agent**（对接方因子框架），它消费我们的数据并使用特定字段名（bidAmount、bidStrength、volumeRatio 等）。v10 rebuild 的目标之一是把我们的数据口径与对接方对齐。所有字段语义 FINAL，对接方已被告知 circMcap = FF。

### 1.2 KEEP vs REBUILD（rebuild-design-v10.md 精华；v11 进一步收紧，见 rebuild-plan-v11.md）

**保留不动（KEEP）——这些是经过验证的建设：**
- 抓取+解密+落盘流水线（endpoints、AES、raw 保留、persistEveryFetch）
- **严格时间隔离 loader**：T0 数据 <= 09:29，T-1/T-2 数据 <= 09:33，自动屌弃盘后展望（post-market lookahead）
- cron / agent_job runner 基础设施
- 已验证因子学习结果：v10 edge_core 权重、REGIME_ACTION_GATE 阈值、逐因子 IC

**重建（REBUILD）——这些有错误：**
- Parse / schema 层：全部由 canonical-field-dictionary.md 驱动，发 canonical 名称 + caliber 标签（已由 canonical.py + canonical_routing.py 落地）
- Factor / scoring 层：在正确 caliber + 对接方框架上重建

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
| pool.hot | `POST /data/getFxPoolData/{sort}` | **无历史 raw！** |
| pool.surge | `POST /data/getCzPoolData/{sort}` | raw 已存 |
| rank.rocket | `GET x.duanxianxia.cn/vendor/stockdata/hotlist.json` → key `skyrocket_hour` | OK |
| rank.hot_stock_day | 同上 → key `hot_stock_hour` | ⚠️ 旧代码读 `hot_stock_day`（不存在）→ 0 行 |

### 2.4 盘后

| dataset_id | 端点 |
|---|---|
| review.fupan.plate | `POST /api/getFupanByYidong?type=plate` |
| review.ltgd.range | `POST /api/getZfByDate` |
| review.daily / review_daily_core11 | `POST /api/getChartByQingxu` |
| home.ztpool | `GET duanxianxia.com/vendor/stockdata/jinjidata.json`（playwright） |
| cashflow.stock.{today/3day/5day/10day} | `stock.9fzt.com/cashFlow/stock.html` |

cashflow 来量：today=**50** 条，3day/5day/10day=**150** 条。

### 2.5 cron 调度

| 任务 | cron（Asia/Shanghai）| 内容 |
|---|---|---|
| 盘前 | `25 9 * * 1-5` | premarket：竞价5表+kaipan+qxlive，随机延迟5–15s |
| 10:01 盘中+资金 | `1 10 * * 1-5` | intraday_cashflow，随机延迟0–45s |
| 盘后 | `20 17 * * 1-5` | postmarket_cashflow，随机延迟0–5min |

cron worker 幂等；队列 `scripts/agent_jobs/queue/<id>.json`；results 推 agent-results 分支（publish ~10min 延迟）。

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
- 华工科技（000988）raw[2]=1441 ≈ 大市值科抈股 ✓

`auction_volume_ratio` 这个字段名是 **mislabel**，实际存的是 FF 市值（亿）。  
**raw[11]** 才是真正的量比（vratio = `volume_ratio`，qiangchou = `grab_strength`）。

`field-rename-map.md` 的改造方向一直是正确的，不要动摇。

### 3.2 全局市值口径：circMcap = FF（已告知对接方）

所有市值相关因子分母统一用 FF，单位统一转为**元**存储（亿×1e8，万×1e4）。

| 表 | 市值字段位置 | 口径 | 原标签（旧/错） |
|---|---|---|---|
| vratio | raw[2] | FF/亿 | `auction_volume_ratio`（MISLABEL）|
| qiangchou | raw[2] | FF/亿 | `auction_volume_ratio`（MISLABEL）|
| net_amount | raw[6] | FF/亿 | `market_cap_yi` |
| weimai | raw[12] | FF/元 | `market_cap`（job 0078 实证）|
| pool.hot | item[9] | FF | "流通"（MISLABEL，实为 FF）|
| pool.surge | item[9] | FLOAT | `float_market_cap`（标签正确，唯一用 FLOAT 的表）|
| review.fupan.plate | 多列 | FF+FLOAT+TOTAL | 实际流通=FF / 流通市值=FLOAT / 总市值=TOTAL，校准锚表 |

---

## 四、字段口径全量修正表

> 完整版见 `docs/canonical-field-dictionary.md`。这里列**修正要点**和容易踱坑的细节。

### 4.1 vratio（raw 13列）

```
[0]  code
[1]  name
[2]  free_float_mktcap    FF/亿→元    ← 旧名 auction_volume_ratio，MISLABEL
[3]  seal_amount_wan      万→元       封单额（覆盖率 ~3%，大多 null）
[4]  auction_change_pct   %
[5]  latest_change_pct    %
[6]  auction_turnover_wan 万→元       = bidAmount
[7]  concept              text
[8]  文本重复（latest_change_pct text）
[9]  文本重复（auction_turnover text）
[10] yesterday_auction_turnover_wan  万→元
[11] volume_ratio         倍          ← 这才是量比（volumeRatio）
[12] turnover_rate        %
```

### 4.2 qiangchou（raw 13列，同 vratio 除）

- raw[2] 同样是 FF 市值（同一 MISLABEL）
- raw[11] = `grab_strength`（抢筹幅度，不是量比）
- **response 结构：`{list: {grab: [...], qiangchou: [...]}}`**
- **两个 group 必须分别保留**：`grab`（9:25 最后 1 秒）+ `qiangchou`（9:20–9:25）

### 4.3 net_amount（raw 9列）

```
[0]code [1]name
[2]auction_change_pct(%)  [3]latest_change_pct(%)
[4]main_net_inflow_wan(万→元)  [5]auction_turnover_wan(万→元)
[6]free_float_mktcap(FF/亿→元)  ← 旧名 market_cap_yi
[7]concept  [8]turnover_rate(%)
```

### 4.4 weimai（raw 18列）

```
[0]code  [1]name  [2]price  [3]latest_change_pct(%)
[4]auction_turnover(元)   ← 注意：无 _wan 后缀，已是元
[5]auction_change           ← 注意：名字是 auction_change，不是 auction_change_pct
[6]main_net_inflow(元)
[7]turnover_rate(%)
[8]seal_volume  [9]auction_amount  [10]seal_volume_again
[11]concept
[12]free_float_mktcap(FF/元)   ← 旧名 market_cap，job 0078 实证 FF
[13]main_net_inflow_full        ← 单位待 0089 确认
[14]super_large_order           ← 单位待 0089；与[13] spearman=−0.919 冗余
[15]large_order                 ← 单位待 0089
[16]board_label          连板标签  ← IC 显示昨3连板最优（见六）
[17]seal_amount_wan       万→元    ← 0089 已实证：万→×1e4→元（封板个股）
```

### 4.5 fengdan（jjlive，AES）

结构：section 聚合行 + per-stock 明细行。

**重要：`canonical-field-dictionary.md` §A5 写着 "OPEN: Confirm 委买 vs 成交"——这是过时的 stale marker，job 0082 已经 RESOLVED：amount_915/920/925 = 涨停价委买/封单额（非成交）。证据：9:15→9:20 非单调降假 non-monotonic drop，成交不会降。**

**Section header**：
```
section_date / kind / yizi_count / seal_total / t15_total / t20_total / t25_total / has_change_pct
```
- `t15/t20/t25_total` = **金额**（元），不是计数！（job 0083 实证：t15=150.4亿 > t20=39.1亿 > seal_total=54.4亿）
- `yizi_count` = 唯一计数字段（亿字股数）

**Per-stock 明细**：
```
rank / code / name / tag_1 / tag_2 / tag_3 / board_label
/ amount_915 / amount_920 / amount_925   ← 9:15/9:20/9:25 涨停价委买/封单累计额（元）
/ latest_change_pct / latest_change_pct_source / tags
```
- `amount_920` = limitBuyAmountAfter920（9:20 不可撤委买，非成交）
- `amount_925` 值为 `-` 表示 9:25 时**未封板**，需做 null 处理
- 默认用 `amount_920`（第一个不可撤快照）
- `latest_change_pct` 从 `qt.gtimg.cn` 实时覆盖

### 4.6 pool.hot（item 索引）

```
item[0]code  item[1]name  item[2]change_pct
item[6]concept  item[7]board_state  ← ⚠️ 当前代码丢弃此字段！
item[8]turnover_amount  item[9]free_float_mktcap(FF)← 旧名"流通"，MISLABEL
item[10]main_net  item[11]real_turnover_rate
```
⚠️ **无历史 raw**，历史数据 tag=`legacy_unrecoverable`，从现在起必须存 raw[]。

### 4.7 pool.surge（item 索引）

```
item[0]code  item[1]name  item[2]change_pct
item[6]concept  item[7]board_state  ← ⚠️ 当前代码丢弃此字段！
item[8]turnover_amount  item[9]float_mktcap(FLOAT)  item[10]turnover_rate_site
```
- item[9] = FLOAT（流通市值，标签正确，本项目**唯一用 FLOAT 的表**）
- raw 已存，历史可重派生
- ⚠️ BUG：当前用 item[8]/item[9]*100 重算换手率 → 与 site item[10] 不符 → Fix：取 item[10]

### 4.8 ztpool（涨停晋级阶梯）

数据来自 `jinjidata.json` 的 `{html: "...", date: "..."}`，用 token grammar 解析：

```
col1 阶梯分组: 首板 / 1进2 / 2进3 / ...
col2 晋级率: 晋级数/样本数=百分比
col3 个股 token 格式:
  <@>{市场} <#'{code}'>{name}（{状态: 成/炸/败}）[{涨幅}] {题材}
```

canonical 字段：日期/分组序号/分组名/组内序号/晋级率文本/晋级数/样本数/晋级率/市场/代码/名称/**状态**/涨幅/题材  
`状态`：成=封住 / 炸=炸板 / 败=未涨停  
⚠️ `source_url` 存储时含字面量 `" + "`，需 fix 字符串拼接。

### 4.9 其他表字段备忘

**home.qxlive.top_metrics**（共 12 个 metric_key）：  
`QX情绪 / HSLN主力流入 / PB今日封板率 / PBBX沪深5分钟量能 / ZTBX / LBBX连板晋级率 / ZT涨停 / DT跌停 / KQXY开板溢价 / LBGD连板高度 / SZ水位 / XD`  
metric_key=**PB** = 今日封板率（marketSealRate），示例值 63.0%。

**review.daily**同上 12 个市场宽度指标。

**review.fupan.plate** 字段包含：封单额/成交额/换手率/实际流通（FF）/流通市值（FLOAT）/总市值（TOTAL）/开板数/连板/涨停类型/首次封板时间/最后封板时间…  

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

用校正后 canonical 输入**重拟合系数**（Task 0093）。

### 5.2 逐因子 canonical 对应（FINAL）

| 因子 | canonical 来源 | 单位/说明 |
|---|---|---|
| bidAmount | auction_turnover_wan（竞价五表均有）| 万→元存储 |
| bidStrength | auction_turnover / free_float_mktcap × 10000 | 两者同元基准 |
| volumeRatio | vratio raw[11]（volume_ratio）| **不是 raw[2]！** 倍 |
| changeRate | auction_change_pct（竞价涨幅）| % |
| limitBuyAmountAfter920 | fengdan amount_920 | 9:20 不可撤委买，元；amount_925="-"=未封 |
| prevStreak | fupan 连板 / ztpool 阶梯 | |
| prevOpenNum | fupan 开板数 | |
| brokenLimitUp | ztpool 状态=炸 | |
| origin | 派生布尔（见 5.3）| |
| stockMainlineFit | concept vs kaipan top 板块强度 | |
| sentimentSignal | QX-live metric_key=QX | |
| themeConsistency | count(H)/count(Q)（见 5.4）| 题材内高开一致性 |
| themeConcentration | 题材 bidAmount / 全市场 bidAmount | |
| prevDayLimitUpSealRate | sealedLimitUp / touchedLimitUp T-1 EOD（见 5.5）| |
| auctionSealAmount | fengdan section_t25_total / section_seal_total | 9:25 封单强度，金额比 |
| marketSealRate | QX-live metric_key=PB 今日封板率 | ~9:25 premarket-safe |

### 5.3 origin 精确定义

```python
fromPrevBrokenLimitUp         = (prev_day_状态 in {'炸', '败'})       # ztpool/jinjidata
fromPrevSealedLimitUpWithOpen = (prev_day_状态 == '成') AND (fupan_开板 > 0)
```

### 5.4 themeConsistency / themeConcentration 精确定义

```python
# themeConsistency（题材内高开一致性）
M(theme) = {股票 | concept == theme}                       # 来自竞价五表
Q(theme) = {i in M | auction_turnover_i >= minBidAmount    # 过滤小额，排除 ST
             AND NOT ST(i)}
H(theme) = {i in Q | auction_change_pct_i > 0}             # 高开内生
themeConsistency(theme) = len(H) / len(Q)

# Strict variant（我们自己阈值）：
#   将 auction_change_pct_i > 0 改为 >= auctionChgMin

# themeConcentration（资金集中度）
themeBidAmount(theme) = sum(auction_turnover_i for i in M(theme))
themeConcentration(theme) = themeBidAmount(theme) / sum(all themes' themeBidAmount)
```

数据来源：vratio/qiangchou/net_amount/weimai 均有 concept + auction_turnover + auction_change_pct。  
**minBidAmount / auctionChgMin**：尊未定，Task 0093 时确定。

### 5.5 三种封板率（DO NOT 混用）

```python
# 3a. prevDayLimitUpSealRate（T-1 EOD，premarket-safe）
sealedLimitUp  = num            # T-1 收盘仍封住
touchedLimitUp = num + open     # 已验证：64+42=106
prevDayLimitUpSealRate = sealedLimitUp / touchedLimitUp
# 来源：review.fupan.plate 或 ztpool 历史数据（T-1）

# 3b-i. auctionSealAmount（T0 ~9:25，fengdan，资金强度比，非计数）
auctionSealAmount = section_t25_total / section_seal_total
# 全是金额（元），job 0083 实证：唯一计数字段 = yizi_count

# 3b-ii. marketSealRate（T0 ~9:25，QX-live PB，计数比，premarket-safe）
# metric_key='PB'，value 例如 63.0%
# 高频备注：偶尔出现 10:04 时间戳 = 调度偏差，不是设计问题
# → Task 0094 pin 到竞价窗口
```

**三者互补、各自独立，永远不要互相替代。**

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
| auction_volume_ratio（vratio）| 0.058 | 0.335 | v48 | ⚠️ 名字是 mislabel，实为 FF 市值的 IC |
| main_net_inflow_full（weimai）| 0.103 | — | v50 | 最高 weimai 因子 |
| super_large_order（weimai）| 0.094 | — | v50 | 与 main_net_inflow_full spearman=−0.919，冗余 |
| market_cap_yi（weimai）| 0.069 | — | v50 | = FF 市值 |
| board_label（weimai）| — | — | v50 | **昨3连板最优**，离散变量 |
| ic_amount（firstprinciples）| 0.134 | 0.596 | v65 | |
| ic_turnrate（firstprinciples）| 0.122 | 0.664 | v65 | |
| ic_composite（amount+turnrate）| 0.136 | 0.764 | v65 | 轻微优于单一 |
| ic_gap 全样本 | 0.168 | 0.494 | v65 | |
| ic_gap 冷场景 | 0.273 | 0.809 | v65 | 热场景 IC ~6× |
| ic_gap 热场景 | 0.044 | 0.158 | v65 | |
| 小市值三分组<100亿 | +0.110 | — | v65 | 去掉 2 个离群日后 +0.262 |

### 6.2 关键结论

- `super_large` ~ `large_order` spearman = −0.919：**高度冗余，合并或取其一**
- amount 与 turnover_rate 相关 r=0.627：正交化后 IC 均降，**composite 轻微最优**
- gap 因子：**冷场景 IC 是热场景 2× 以上** → 必须 REGIME_ACTION_GATE 分场景加权
- board_label 昨3连板最优（需 one-hot 或分组处理）

### 6.3 REGIME_ACTION_GATE（场景门控）

根据 QX 情绪值（历史 median=29.0）划分热/冷场景：
- **热场景**（QX 高）：总体 IC 低，top5 原始超额 +0.43%
- **冷场景**（QX 低）：总体 IC 高，top5 原始超额 +3.25%
- corr(QX, core_ic) = −0.024（接近 0），QX 不直接 drive IC
- 结论：edge_core 在冷场景显著更有效，需在冷场景加大权重或提高置信阈值

---

## 七、已完成工作（Jobs 0001–0089）

| Jobs | 内容 |
|---|---|
| 0001–0044 | fetcher.py、AES、落盘规范（captures/YYYY-MM-DD/〦）、飞书 webhook |
| 0045 | premarket_raw_capture_audit_v36：22 交易日 × 9 数据集全量 IC 审计 |
| 0058–0063 | v10 factor IC 矩阵初版 |
| 0075–0078 | 市值口径全表核查；job 0078 实证 net_amount[6] / weimai[12] = FF |
| 0079 | blast_radius（9.2MB）：下游消费方字段防御键全清单 |
| 0080 | field_caliber_dump（2026-06-29 08:10，未开市）：mcap_fields_by_name 核查 |
| 0082 | limitBuyAmountAfter920 = fengdan amount_920 确认（非成交）|
| 0083 | fengdan section_* 全是金额，yizi_count 唯一计数 |
| 0084 | QX-live PB = 今日封板率，metric_key=PB，value=63.0% |
| 0085 | weimai deepdive v50：main_net_inflow_full IC=0.103；board_label 昨3连板最优 |
| 0086 | firstprinciples v65：gap非线性，冷场景 IC=0.273；小市值超额 |
| 0087 | PR#28 squash→main (638dacf6) |
| 0088 | PR#29 squash→main (f20acdf)；fetcher SHA=d61c7be5 |
| 0089 | 探针脚本推 main (56aad925)；锁定 4 个未决单位，结论见 §8.1 |

---

## 八、待验证 / 未落实项

### 8.1 ✅ 0089 探针结果（已落地，仅存档）

> **已解决**：raw[17] seal_amount = **万 → ×1e4 → 元**（封板个股），已在 `duanxianxia_canonical.py` 实证修复并写入 registry + 自检。其余单位见下表，均已并入 canonical。新会话**无需**再"先读 0089 才能动手"，下表保留作存档参考。

**路径**（agent-results 分支）：
```
projects/duanxianxia/reports/_audit/agent_jobs/0089_unit_probe_20260629.result.json
```

| 字段 | 表 | 当前疑问 |
|---|---|---|
| raw[17] seal_amount_wan | weimai | 万 还是 元？ |
| raw[13] main_net_inflow_full | weimai | 万 还是 元？ |
| raw[14] super_large_order | weimai | 万 还是 元？ |
| raw[15] large_order | weimai | 万 还是 元？ |
| item[9] free_float_mktcap | pool.hot | "字符串如 182亿"，parse 后是 182 还是 18200000000？ |
| item[9] float_mktcap | pool.surge | "字符串如 325亿"，parse 后单位？ |

### 8.2 qt.gtimg 双花括号 bug

```python
# _fetch_realtime_quotes 里：
url = f"{https://qt.gtimg.cn/q={symbols}}"  # 疑似 f-string 写法错误
```

需运行时验证 URL 是否正确撩取。

### 8.3 pool.hot 历史数据无法恢复

`pool.hot` 无历史 raw → 打标 `legacy_unrecoverable`。从 Task 0091 起必须存 raw[]。

---

## 九、任务路线图（v11，详见 docs/rebuild-plan-v11.md）

> 总纲：四层架构 L1 采集 / L2 口径(canonical) / L3 特征(feature builder) / L4 因子(edge_core)。L1、L2 已就位，重心转入 L3、L4 的彻底重建。

### ✅ 已完成
- **Task 0090**：新建 `scripts/duanxianxia_canonical.py`（registry + `raw_to_canonical()` + import 期 `_self_test()` + caliber validator）。已上线（blob faa1ab9b）。
- **seal_amount 单位修复**：实证 raw[17]=万→×1e4→元，canonical 自检 #4 断言 208089×10000；探针 0091 已入队。
- **canonical routing 入口**：新建 `scripts/duanxianxia_canonical_routing.py`（KIND_TO_DATASET / canonicalize_rows()），已上线（blob ad3cd6a7）并对真实 0089 行验证通过。

### ⏳ 待 cron 跑（server gate）
- **0091**：seal_amount 单位探针校验 → agent-results。
- **0092**：routing 模块 vs live canonical 字节级一致性校验 → agent-results。

### M1 — L3 特征/加载层重构（下一步，核心）
新建 `duanxianxia_feature_builder.py`：读持久化 captures → `canonicalize_rows()` → 输出扁平、时间隔离（T0≤9:29 / T-1,T-2≤9:33）的特征表，发 canonical 名称 + v10 因子原语（bidStrength / volumeRatio=raw[11] / changeRate / limitBuyAmountAfter920 等）。**不读旧 transform-2 的错配标签**；硬编码真实样本自检。先读 captures 落盘格式（在 batch / premarket_v7* 内）再动手。

### M2 — 采集完整性补丁（patch-script，非整体重写）
weimai 封单展示落地；pool.hot 存 raw[]+item[7]板态；pool.surge turnover 取 site item[10]；hotlist 改读 `hot_stock_hour`。先审计现状（多数已修），仅对确未修项打小补丁。

### M3 — 历史回溯重导 + 重生成 CSV（server job）
有 raw 的 capture 从 raw 经 canonical 重派生；pool.hot 无 raw → tag `legacy_unrecoverable`；重生 `_all_candidates_flat.csv` / `feature_matrix_v21.csv`。**禁止原地 sed 改历史**。

### Task 0093 — L4 因子重拟合
接线新因子（origin / themeConsistency / themeConcentration / auctionSealAmount / marketSealRate / prevDayLimitUpSealRate / stockMainlineFit）；定 minBidAmount / auctionChgMin；在 canonical 输入上**重拟合 edge_core 系数**。

### Task 0094 — 上线校验
Pin QX-live 抓取到 ~9:25 竞价窗口，避免偶发 10:04 时间戳污染 premarket 特征；跑 v11 DoD 验收。

### Task 0095（Deferred）
T-1 lagged 表，搁置。

---

## 十、代码 Bug 全清单

| # | 位置 | Bug | Fix |
|---|---|---|---|
| 1 | fetcher.py hotlist | 读 `hot_stock_day`（不存在）→ 0 行 | 改读 `hot_stock_hour`（~699 处）|
| 2 | fetcher.py vratio/qiangchou | raw[2] 标签 `auction_volume_ratio`，是 mislabel（~380 处）| rename → `free_float_mktcap` |
| 3 | fetcher.py pool.hot | item[9]"流通" mislabel + item[7]板态丢弃 + 无 raw | 修正 + 存 raw[] + 保 item[7] |
| 4 | fetcher.py pool.surge | 换手率重算≠site item[10] | 取 site item[10] |
| 5 | fetcher.py ztpool | `source_url` 含 `" + "` | fix 字符串拼接 |
| 6 | fetcher.py fengdan | qt.gtimg URL 双花括号疑 bug | 运行时验证 |
| 7 | canonical-dict §A5 | 写着 "OPEN: Confirm 委买 vs 成交" | 该 OPEN 已 RESOLVED（job 0082），下次更新 canonical-dict 时删除 |

---

## 十一、审计文件索引

### docs/（main 分支）

| 文件 | SHA | 作用 |
|---|---|---|
| `rebuild-plan-v11.md` | 本次 push | **v11 彻底重构权威执行口径** |
| `HANDOFF.md` | 本次 push | 本文件，全量交接 |
| `canonical-field-dictionary.md` | eff62b07 | 字段口径 source of truth（注意 §A5 OPEN 标记已 RESOLVED）|
| `v10-field-alignment-decisions.md` | 672644348 | 因子对应 FINAL |
| `rebuild-design-v10.md` | a450c931 | KEEP/REBUILD + 迁移规则 |
| `field-rename-map.md` | 62e94ff6 | 改造清单 + 代码 bug（方向全对）|
| `project-handbook-current.md` | 7bac4fc3 | V9 抓取规范（部分 V10 未覆盖）|

### reports/_audit/（agent-results 分支）

| 文件 | 内容 | 要点 |
|---|---|---|
| `0089_unit_probe_20260629.result.json` | 4 个未决单位（已落地，见 §8.1）| 存档 |
| `vratio_deepdive_v48.json` | vratio IC | `auction_volume_ratio` 字段名 = mislabel，其 IC=0.058 是 FF市值的 IC |
| `qiangchou_deepdive_v46.json` | qiangchou IC | 同上 |
| `weimai_deepdive_v50.json` | weimai IC | main_net_inflow_full=0.103最高；board_label昨3连板最优 |
| `firstprinciples_v65.json` | amount/换手正交、gap非线性 | qx_median=29.0 |
| `field_census_0076.json` | 各表 headers/sample | mcap_fields_by_name 按名字搜索，mislabel 表会漏 |

---

## 十二、错误记录（勿重蹈）

| 错误 | 实际情况 | 教训 |
|---|---|---|
| 看到 raw[2]=17/499/1983 断言"是量比" | 这些就是各股 FF 市值（亿） | 数值判断必须交叉核对 live API + 已知 FF，不能凭感觉 |
| `field_census.mcap_fields_by_name=[]` 证明"该表无市值" | census 按字段名搜索，mislabel 则搜不到 | census 结论 ≠ 值域结论，要结合 live 比对验证 |
| 推翻 field-rename-map.md 的正确结论 | rename-map 一直是正确的 | canonical-dict + rename-map 权威性高于单次名称普查，不要轻易推翻 |
| 巨型脚本整体重写 | 单文件 API 会静默转写漂移，py_compile 抓不到改动的字面量 | 用小补丁或在 raw 下游建新模块，别整体重写 105KB/145KB |
