# duanxianxia v10 重构 · 全量交接文档

> **新对话开场必读顺序**：
> 0. **`docs/HANDOFF.md` §零「最新状态」← ★★ 先读这一节（0099 之后的最新增量：v9 生产引擎上线 + P0 打分缺陷）**
> 1. 本文件其余章节（§一~§十二，v10/v11 canonical 重构历史底座，仍有效）
> 2. `docs/rebuild-plan-v11.md` ← canonical-first 彻底重构，权威执行口径
> 3. `docs/canonical-field-dictionary.md` ← 字段 source of truth
> 4. `docs/v10-field-alignment-decisions.md` ← 因子对着 FINAL
> 5. `docs/rebuild-design-v10.md` ← KEEP vs REBUILD + 迁移规则

最后更新：2026-07-01 18:xx（★ 新增 §零：**v9 生产决策引擎已 monkey-patch 上线**；盘前「抓数据→v9分析选股→webhook推送」全链路跑通，0104 实盘 webhook HTTP 200；**发现 P0 打分缺陷**：7连板妖股 002674 被打成 score 100 首选买入，根因已定位；下一步 Task 0105 加高位/连板风险闸门 + 修 qxlive T0。v11 底座：M1–M4 完成，0099 已把 S5 权重持久化到 git main commit 6186957；Task 0094 上线校验仍待办）

---

## 零、最新状态（2026-07-01 晚，★ 新对话先读这一节）

> 本节是 0099 之后的最新增量。§一~§十二 是 v10/v11 canonical 重构的历史底座（仍有效）；本节记录「v9 生产决策引擎上线 + 盘前全链路打通 + 发现 P0 打分缺陷」。所有事实已核对代码/运行结果。

### 0.1 一句话现状
盘前「抓数据 → v9 分析选股 → webhook 推送」全链路已跑通（0104 实盘 webhook HTTP 200 成功）。但 7/1 实盘暴露**打分模型 P0 结构性缺陷**：把 7 连板见顶妖股 002674 打成 score 100 首选买入。根因已定位到代码。下一步 **0105：加高位/连板风险闸门 + 修 qxlive T0**。

### 0.2 生产引擎已切到 v9（重要）
- `scripts/duanxianxia_premarket_v7_runner.py` 运行时 monkey-patch：`duanxianxia_batch.build_premarket_analysis = build_premarket_analysis_v9`（`ACTIVE_ENGINE`）。
- 文件名仍叫 `_v7_runner` 只为不动 crontab / cron_runner。**回退**：把 `ACTIVE_ENGINE` 改回 `build_premarket_analysis_v7_3` 一行 + 服务器 pull。
- 与 §五 的 S5 edge 权重是同一套 `duanxianxia_v9_edge.py`；v9 引擎在其上加了六层装配 + 动作层。

### 0.3 v9 引擎原理逻辑（已逐文件核对代码）
入口 `build_premarket_analysis_v9`（`duanxianxia_premarket_v9_runner.py`）→ 六层装配 `assemble_v9`（`duanxianxia_v9_assemble.py`）→ 打分 `compute_edge_v9`（`duanxianxia_v9_edge.py`，S5 权重见 §5.1）→ 动作层 `shape_v9_output`（`duanxianxia_v9_output.py`）。

全链路 5 步：
1. 跑完整 v7.2 得候选池 + 全量 bundle（v9 不重算 v7）。
2. 装配六层 detail：竞价订单流 / 委买 weimai / 题材强度(T0 回退 T1) / 市场环境(qxlive 12 指标→regime+market_env_score+risk_flags) / 个股上下文(T-1 资金连续性、复盘题材、龙头高度、涨停池/连板标签)。
3. `compute_edge_v9` 打分（0–100，S5 权重）。⚠️ 题材/市场环境/资金连续性/龙头高度被 v10 从**加分项剔除**，只做诊断标签，不加分；v11 再把「换手率+跳空」正交复合 z 分按 λ=0.4 混进排序。
4. alpha 类型贴标签：AUCTION_ORDERFLOW / LOW_OPEN_REVERSAL / ORDERBOOK_WEIMAI / THEME_BACKGROUND。规则：竞价 pct<0 且 main_factor≥40 → 贴「低开反包」。
5. 动作层 BUY/WATCH/DROP：`REGIME_ACTION_GATE` regime 自适应「分位数+绝对下限+数量上限」。normal=前5%且edge≥45最多4只；hot=前8%且edge≥42最多5只；cold=前1.5%且edge≥50最多1只。风险行(risk_flag)买入需额外 +8 edge 余量(RISK_EXTRA_MARGIN)。

**当前 risk_penalty 只有 5 项**（`duanxianxia_v9_edge.py`）：竞价高开≥7%(−14)、流动性≤35(−12)、假封单/消耗封单(−16)、假强度 FAKE_STRENGTH(−18)、市场级接力恶化 relay_deteriorating(−8)。市场环境层另有 many_limit_down 等**市场级**旗标。

### 0.4 🔴 P0 缺陷：002674 妖股被打成 score 100 买入（根因定位）
- 事件：0104 补跑（7/1 17:31）选出 **002674 兴业科技 score 100 rank1 risks=[]**，reasons=[低开反包, 竞价-6.5%, 资金连续:unknown]。该股是 **7 连板妖股**，6/30 见顶跌停（公司回应+龙虎榜），7/1 继续跌停。
- 根因（代码级）：
  1. **风险层没有任何「高位/连板/前一日跌停/涨幅过大」规则**。002674 是低开，high_open_cost(≥7%高开)不触发；反而因 pct<0 被贴「低开反包」买点 → risks=[] 完全符合代码，不是漏输出，是没这条防线。
  2. context 层其实算了连板标签/涨停池/龙头高度，但 edge 公式里被 v10 剔除、一分不扣 → 系统「看见了妖股」却在打分时丢弃该信息。
  3. many_limit_down 是市场级，不管个股自己连续跌停。
  4. qxlive T0 缺失 → 资金连续性=unknown=45 分（仍是正分，不扣）。
- 净效应：7 连板见顶妖股 → 低开反包标签 + 竞价成交额百分位极高(放天量) + 流动性满分 → edge 顶到 100、风险扣分 0 → 直接 BUY。

### 0.5 🟠 P1：其他待修
- **qxlive top_metrics T0 抓取时序**：WARN `No home.qxlive.top_metrics capture at 2026-07-01 <= 093300; refusing after-cutoff fallback`。导致 market_env / 资金连续性走中性/unknown。0102 装了抓取 cron，但 T0(≤09:33)落地时点仍需验证。
- **飞书多维表未同步**：0104 结果 `bitable_sync enabled=false, reason="Meta file not found: .../memory/feishu_bitable/duanxianxia_review.json"`。meta 在运行时 `memory/`(不在 git)，服务器缺失。**待用户确认**：多维表是否已存在(给 app_token+table_id)还是新建。目前只有 webhook 成功。

### 0.6 后续计划 — Task 0105（等用户点头即开工）
红线：全部加法式修改；不重写 105KB fetcher / 145KB batch；不做 §四 字段改名口径变更；0095(T-1 lagged)仍搁置。
0105 三件事：
1. **加高位/连板/前一日跌停风险闸门**（在 `duanxianxia_v9_edge.py`）：把 context 已算的连板标签/龙头高度/前一日状态接进 risk_penalty 与 risk_flag，让 002674 类被否决或降级。
2. **修 qxlive T0 抓取时序**（≤09:33 前落地），让资金连续性不再 unknown。
3. 重跑验证 002674 类被 veto/flag，webhook 推送复核。

### 0.7 关键坐标（0100+ 增量）
- **main HEAD 链**：`45ab786a`(0102) → `e7538fdd`(0103) → `b0cbd17e`(0104)。**下一空闲 job id = 0105**。（注：§一 1.0 里的 6186957 是 0099 时点，已被 0102–0104 推进。）
- **agent-results tip**：`02fa83d1`（7/1 17:50:11 SH）。
- **0104 结果**：`projects/duanxianxia/reports/_audit/agent_jobs/0104_premarket_backfill_20260701.result.json`；报告 `projects/duanxianxia/reports/2026-07-01/premarket/173143.json`。
- **GitHub MCP 连接**：用 `mcpServer_github3`（旧 github/github2 token 过期作废）。server URL `https://api.githubcopilot.com/mcp/`（Bearer/PAT）。
- 🔒 用户曾在聊天粘贴明文 Fine-grained PAT，建议尽快 revoke + 轮换。
- **关键路径**：`REPORT_ROOT=/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/reports`；`RUNNER=…/scripts/duanxianxia_premarket_v7_runner.py`；缺失 bitable meta=`…/memory/feishu_bitable/duanxianxia_review.json`。
- **v9 模块族**（scripts/，@b0cbd17e）：`duanxianxia_premarket_v9_runner.py` / `_v9_edge.py` / `_v9_assemble.py` / `_v9_context.py` / `_v9_market_env.py` / `_v9_theme_strength.py` / `_v9_weimai.py` / `_v9_output.py` / `_v9_from_report.py`。

---

## 一、项目定位与整体架构

从 `https://duanxianxia.com/` 抓取 15+ 张盘前/盘中/盘后数据集，目标是建立完整量化盘前分析流水线：

```
raw[] 位置数组（ground truth）
  └─[L1 采集]─> fetcher parse -> capture 落盘（带 raw[]）
       └─[L2 口径]─> canonical.py + canonical_routing.py 统一口径
            └─[L3 特征]─> feature_builder 时间隔离 T0 特征 + v10 因子原语
                 └─[L4 因子]─> edge_core(S5) 评分 -> 飞书 webhook 推送
```

**当前阶段**：canonical-first 四层架构 L1采集/L2口径/L3特征/L4因子已全部打通并验证。
**v11 决策**：canonical-first 彻底重构——冻结旧解析产物，一切从 raw 经 canonical 重新派生；不对 105KB/145KB 巨型脚本做整体重写或 sed 改标签。
**下一步**：Task 0094 上线校验（pin QX-live 到 9:25 竞价窗口 + v11 DoD 验收）。S5 权重已由 0099 持久化到 git main（0098 服务器工作副本 + 0099 git 双落地完成）。

### 1.0 v11 进度总览（2026-07-01）

| 里程碑 | 状态 | 证据 |
|---|---|---|
| L2 canonical | ✅ | canonical.py(faa1ab9b) + canonical_routing.py(ad3cd6a7)；0091/0092 rc=0 |
| M1 feature_builder | ✅ | m1b_feature_builder_probe rc=0：288 features，0 canonical errors，no mislabel leak |
| M2 采集补丁 | ✅ | fetcher 4 bug 全修（commit d61c7be5）|
| M3 历史 backfill | ✅ | 0097_m3_backfill rc=0：21 日 5910 行，0 errors；_all_candidates_flat_v11.csv |
| M4 canonical 接入生产 | ✅ | m1b_auction_source_probe rc=0：auction_source=canonical_feature_builder，Spearman 0.8009，347 候选 |
| L4 因子重拟合(0093) | ✅ | walk-forward rc=0，推荐 S5_amt_liq_core（OOS IC 0.129 vs 0.1278，beats 8/12）|
| S5 上线(0098) | ✅ | 服务器工作副本 v9_edge/v10_optimize 已改，self_test passed，rc=0 |
| S5 持久化 git(0099) | ✅ | 0099 rc=0：幂等 patch + git commit 6186957 push 到 main；v9 self_test passed |
| Task 0094 上线校验 | ⬜ | 待办 |

**Repo**: `kobolibra/short-term-pre-market-analysis`  
**main HEAD**: `b0cbd17e`（0104；0099 时点为 6186957，见 §零 0.7）  
**agent-results 分支**: 含 0093/0097/0098/0099/0104 result  
**服务器项目根**: `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia`  
**fetcher 现行版本 SHA**: `d61c7be5`（`scripts/duanxianxia_fetcher.py`）

### 1.1 与对接方的协作背景

有一个 **other-agent**（对接方因子框架），它消费我们的数据并使用特定字段名（bidAmount、bidStrength、volumeRatio 等）。v10 rebuild 目标之一是把数据口径与对接方对齐。所有字段语义 FINAL，对接方已被告知 circMcap = FF。

### 1.2 KEEP vs REBUILD

**保留不动（KEEP）：**
- 抓取+解密+落盘流水线（endpoints、AES、raw 保留、persistEveryFetch）
- **严格时间隔离 loader**：T0 数据 <= 09:29，T-1/T-2 <= 09:33，自动丢弃盘后展望
- cron / agent_job runner 基础设施
- 已验证因子学习结果：edge_core 权重、REGIME_ACTION_GATE 阈值、逐因子 IC

**重建（REBUILD）：**
- Parse/schema 层：由 canonical-field-dictionary.md 驱动（已由 canonical.py + canonical_routing.py 落地）
- Factor/scoring 层：在正确 caliber 上重建（0093 已重拟合）

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
| rank.hot_stock_day | 同上 → key `hot_stock_hour` | 旧代码读 `hot_stock_day`（不存在）已修 |

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
- 多氟多（002407）raw[2]=462 ≈ 461.78亿 ✓
- 长虹美菱（000521）raw[2]=34 ≈ 34亿小盘股 ✓
- 华工科技（000988）raw[2]=1441 ≈ 大市值科技股 ✓

`auction_volume_ratio` 是 **mislabel**，实际存的是 FF 市值（亿）。**raw[11]** 才是真正的量比（vratio=`volume_ratio`，qiangchou=`grab_strength`）。`field-rename-map.md` 的改造方向一直正确。

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

> 完整版见 `docs/canonical-field-dictionary.md`。这里列**修正要点**和易踩坑细节。

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
[4]auction_turnover(元)   ← 无 _wan 后缀，已是元
[5]auction_change           ← 名字是 auction_change，不是 auction_change_pct
[6]main_net_inflow(元)
[7]turnover_rate(%)
[8]seal_volume  [9]auction_amount  [10]seal_volume_again
[11]concept
[12]free_float_mktcap(FF/元)   ← 旧名 market_cap，job 0078 实证 FF
[13]main_net_inflow_full        ← 单位同元
[14]super_large_order           ← 单位元；与[13] spearman=−0.919 冗余
[15]large_order                 ← 单位元
[16]board_label          连板标签  ← IC 显示昨3连板最优（见六）
[17]seal_amount_wan       万→元    ← 0091 实证：万→×1e4→元（封板个股）
```

### 4.5 fengdan（jjlive，AES）

结构：section 聚合行 + per-stock 明细行。

**`canonical-field-dictionary.md` §A5 的 "OPEN: Confirm 委买 vs 成交" 是过时 stale marker，job 0082 已 RESOLVED：amount_915/920/925 = 涨停价委买/封单额（非成交）。证据：9:15→9:20 非单调降，成交不会降。**

**Section header**：
```
section_date / kind / yizi_count / seal_total / t15_total / t20_total / t25_total / has_change_pct
```
- `t15/t20/t25_total` = **金额**（元），不是计数！（job 0083 实证：t15=150.4亿 > t20=39.1亿 > seal_total=54.4亿）
- `yizi_count` = 唯一计数字段（一字股数）

**Per-stock 明细**：
```
rank / code / name / tag_1 / tag_2 / tag_3 / board_label
/ amount_915 / amount_920 / amount_925   ← 9:15/9:20/9:25 涨停价委买/封单累计额（元）
/ latest_change_pct / latest_change_pct_source / tags
```
- `amount_920` = limitBuyAmountAfter920（9:20 不可撤委买，非成交）
- `amount_925` 值为 `-` 表示 9:25 未封板，需 null 处理
- 默认用 `amount_920`（第一个不可撤快照）
- `latest_change_pct` 从 `qt.gtimg.cn` 实时覆盖

### 4.6 pool.hot（item 索引）

```
item[0]code  item[1]name  item[2]change_pct
item[6]concept  item[7]board_state  ← 旧代码丢弃（M2 已修，存 raw+保 item[7]）
item[8]turnover_amount  item[9]free_float_mktcap(FF)← 旧名"流通"，MISLABEL
item[10]main_net  item[11]real_turnover_rate
```
**历史无 raw** → 历史数据 tag=`legacy_unrecoverable`，从现在起存 raw[]。

### 4.7 pool.surge（item 索引）

```
item[0]code  item[1]name  item[2]change_pct
item[6]concept  item[7]board_state
item[8]turnover_amount  item[9]float_mktcap(FLOAT)  item[10]turnover_rate_site
```
- item[9] = FLOAT（流通市值，标签正确，唯一用 FLOAT 的表）
- raw 已存，历史可重派生；换手率取 site item[10]（M2 已修）

### 4.8 ztpool（涨停晋级阶梯）

数据来自 `jinjidata.json` 的 `{html, date}`，token grammar 解析：

```
col1 阶梯分组: 首板 / 1进2 / 2进3 / ...
col2 晋级率: 晋级数/样本数=百分比
col3 个股: 市场 / code / name /（状态: 成/炸/败）/[涨幅]/ 题材
```

canonical 字段：日期/分组序号/分组名/组内序号/晋级率文本/晋级数/样本数/晋级率/市场/代码/名称/**状态**/涨幅/题材  
`状态`：成=封住 / 炸=炸板 / 败=未涨停。`source_url` 拼接 bug 已修。

### 4.9 其他表字段备忘

**home.qxlive.top_metrics**（12 个 metric_key）：`QX情绪 / HSLN主力流入 / PB今日封板率 / PBBX / ZTBX / LBBX / ZT / DT / KQXY / LBGD / SZ / XD`。metric_key=**PB** = 今日封板率（marketSealRate），示例值 63.0%。

**review.fupan.plate** 字段：封单额/成交额/换手率/实际流通(FF)/流通市值(FLOAT)/总市值(TOTAL)/开板数/连板/涨停类型/首末封板时间…

---

## 五、v10/v11 因子框架（FINAL）

### 5.1 edge_core 公式

**v11 生产权重（S5_amt_liq_core，Task 0093 walk-forward 重拟合 → 0098 上线 → 0099 落 git）：**

```
0.3232 × auction_amount_pct
+ 0.0909 × auction_strength
+ 0.2424 × liquidity
+ 0.1616 × money
+ 0.1414 × pressure_score
+ 0.0303 × weimai_strength
+ 0.0202 × orderbook
− risk_penalty
```

0093 OOS（12 测试日）：mean_IC **0.129** vs baseline 0.1278；ICIR 0.903 vs 0.895；capture@30 0.167；**beats 8/12 days**。robust_better_than_baseline = [S5_amt_liq_core, S3_lean_amt, S4_mild_reweight]。

**旧 baseline（S0，已被 S5 取代，仅存档）**：0.23 / 0.19 / 0.18 / 0.14 / 0.14 / 0.08 / 0.05。

**权重落地位置**：`duanxianxia_v9_edge.py` compute_edge_v9 的 `p.get("edge_w_*", default)` 默认值；`v10_optimize.py` 的 `V10AMT_W`。0098 已改服务器工作副本；**0099 rc=0 已把它们持久化进 git main（commit 6186957），git 与运行时一致，下次 pull 不再回退 baseline**。

> ⚠️ 见 §零 0.4：edge 公式**只减 risk_penalty 的 5 项**，缺高位/连板/前一日跌停闸门 → 002674 类妖股会被打满分。0105 要补。

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

### 5.4 themeConsistency / themeConcentration

```python
M(theme) = {股票 | concept == theme}
Q(theme) = {i in M | auction_turnover_i >= minBidAmount AND NOT ST(i)}
H(theme) = {i in Q | auction_change_pct_i > 0}
themeConsistency(theme) = len(H) / len(Q)

themeBidAmount(theme) = sum(auction_turnover_i for i in M(theme))
themeConcentration(theme) = themeBidAmount(theme) / sum(all themes' themeBidAmount)
```

数据来源：vratio/qiangchou/net_amount/weimai 均有 concept + auction_turnover + auction_change_pct。

### 5.5 三种封板率（DO NOT 混用）

```python
# 3a. prevDayLimitUpSealRate（T-1 EOD，premarket-safe）
sealedLimitUp  = num            # T-1 收盘仍封住
touchedLimitUp = num + open     # 已验证：64+42=106
prevDayLimitUpSealRate = sealedLimitUp / touchedLimitUp

# 3b-i. auctionSealAmount（T0 ~9:25，fengdan，资金强度比，非计数）
auctionSealAmount = section_t25_total / section_seal_total

# 3b-ii. marketSealRate（T0 ~9:25，QX-live PB，计数比，premarket-safe）
# metric_key='PB'，value 例如 63.0%；Task 0094 pin 到竞价窗口
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
| auction_volume_ratio（vratio）| 0.058 | 0.335 | v48 | 名字是 mislabel，实为 FF 市值 IC |
| main_net_inflow_full（weimai）| 0.103 | — | v50 | 最高 weimai 因子 |
| super_large_order（weimai）| 0.094 | — | v50 | 与 main_net_inflow_full spearman=−0.919，冗余 |
| market_cap_yi（weimai）| 0.069 | — | v50 | = FF 市值 |
| board_label（weimai）| — | — | v50 | **昨3连板最优**，离散变量 |
| ic_composite（amount+turnrate）| 0.136 | 0.764 | v65 | 轻微优于单一 |
| ic_gap 冷场景 | 0.273 | 0.809 | v65 | 热场景 IC ~6× |
| ic_gap 热场景 | 0.044 | 0.158 | v65 | |
| 小市值三分组<100亿 | +0.110 | — | v65 | 去掉 2 离群日后 +0.262 |

### 6.2 关键结论

- `super_large` ~ `large_order` spearman = −0.919：高度冗余，取其一
- amount 与 turnover_rate r=0.627：composite 轻微最优
- gap 因子：冷场景 IC 是热场景 2× 以上 → 必须 REGIME_ACTION_GATE 分场景加权
- board_label 昨3连板最优（one-hot 或分组处理）

### 6.3 REGIME_ACTION_GATE（场景门控）

按 QX 情绪值（历史 median=29.0）划分热/冷场景：
- 热场景：总体 IC 低，top5 原始超额 +0.43%
- 冷场景：总体 IC 高，top5 原始超额 +3.25%
- corr(QX, core_ic)=−0.024，QX 不直接 drive IC
- 结论：edge_core 冷场景显著更有效，冷场景加大权重或提高置信阈值

---

## 七、已完成工作（Jobs 0001–0104）

| Jobs | 内容 |
|---|---|
| 0001–0044 | fetcher.py、AES、落盘规范、飞书 webhook |
| 0045 | premarket_raw_capture_audit_v36：22 交易日 × 9 数据集 IC 审计 |
| 0058–0063 | v10 factor IC 矩阵初版 |
| 0075–0078 | 市值口径全表核查；0078 实证 net_amount[6]/weimai[12]=FF |
| 0079 | blast_radius：下游消费方字段防御键清单 |
| 0082 | limitBuyAmountAfter920 = fengdan amount_920（非成交）|
| 0083 | fengdan section_* 全是金额，yizi_count 唯一计数 |
| 0084 | QX-live PB = 今日封板率，value=63.0% |
| 0085 | weimai v50：main_net_inflow_full IC=0.103；board_label 昨3连板最优 |
| 0086 | firstprinciples v65：gap非线性，冷场景 IC=0.273 |
| 0087–0088 | PR#28/#29 squash→main；fetcher SHA=d61c7be5 |
| 0089 | 单位探针；结论已并入 canonical（见 §8.1）|
| 0090–0092 | canonical.py + canonical_routing.py 上线；0091 seal_amount 单位门 rc=0；0092 routing 自检 rc=0 |
| M1 | feature_builder 上线；m1b_feature_builder_probe rc=0（288 features，0 errors，no mislabel leak）|
| M2 | fetcher 4 bug 修复（commit d61c7be5）|
| M3 | 0097_m3_backfill rc=0：21 日 5910 行，0 canonical errors；_all_candidates_flat_v11.csv |
| M4 | m1b_auction_source_probe rc=0：auction_source=canonical_feature_builder，Spearman 0.8009，347 候选 |
| 0093 | L4 walk-forward 重拟合 rc=0：推荐 S5_amt_liq_core（OOS IC 0.129 vs 0.1278，beats 8/12）|
| 0098 | S5 权重写入服务器工作副本 rc=0（v9_edge self_test passed；V10AMT_W 已更新）|
| 0099 | rc=0：把 S5 持久化到 git main（幂等 patch + git commit 6186957 push；v9 self_test passed）|
| 0101 | qxlive top_metrics T0 抓取时序侦查（见 §零 0.5 P1）|
| 0102 | cron 安装（premarket 9:25 / intraday 10:01 / postmarket 17:20 + worker + IPO），commit `45ab786a`，执行 15:00:07 rc=0 |
| 0103 | batch.py 侦查（commit `e7538fdd`）；已被直接读码取代 |
| 0104 | 盘前补抓+v9 选股+推送全链路，commit `b0cbd17e`：rc=0，analysis_version=premarket_v9，**webhook HTTP 200 成功**；bitable_sync=false(meta 缺失)；candidate_count=1，top=002674 score100 risks=[] ← 暴露 P0（见 §零 0.4）|

---

## 八、待验证 / 未落实项

### 8.1 ✅ 0089 探针结果（已落地，仅存档）

> raw[17] seal_amount = **万 → ×1e4 → 元**（封板个股），已在 `duanxianxia_canonical.py` 修复 + 自检。weimai raw[13/14/15] 单位=元，pool.hot/surge item[9] parse 单位已并入 canonical。新会话无需再"先读 0089 才能动手"。

路径（agent-results）：`projects/duanxianxia/reports/_audit/agent_jobs/0089_unit_probe_20260629.result.json`

### 8.2 qt.gtimg URL（已确认非 bug，存档）

`_fetch_realtime_quotes` 的 f-string URL 经 hex 核对确认拼接正确，非 bug。

### 8.3 pool.hot 历史数据无法恢复

无历史 raw → 打标 `legacy_unrecoverable`。M2 起存 raw[]。

### 8.4 🔴 P0/P1 待修（见 §零 0.4/0.5，0105 处理）

- P0：v9 edge 缺高位/连板/前一日跌停风险闸门（002674 妖股被打 score 100 买入）。
- P1：qxlive T0 抓取时序；飞书多维表 meta 缺失（待用户给 app_token+table_id 或确认新建）。

---

## 九、任务路线图（v11）

> 四层架构 L1 采集 / L2 口径 / L3 特征 / L4 因子。L1–L4 主线已打通。

### ✅ 已完成
- **L2**：canonical.py（faa1ab9b）+ canonical_routing.py（ad3cd6a7）；0091/0092 rc=0。
- **M1 feature_builder**：canonical-first L3，时间隔离 T0≤9:29 内置；m1b_feature_builder_probe rc=0（288 features，0 canonical errors，no mislabel leak，FF caliber）。已接为竞价源唯一消费入口。
- **M2 采集补丁**：fetcher 4 bug 全修（hotlist→hot_stock_hour、pool.hot 存 raw+item[7]、pool.surge turnover→item[10]、ztpool url）。
- **M3 历史 backfill**：0097_m3_backfill rc=0，21 日 5910 行，0 canonical errors，_all_candidates_flat_v11.csv。
- **M4 canonical 接入生产**：m1b_auction_source_probe rc=0，auction_source=canonical_feature_builder，Spearman 0.8009。
- **Task 0093 L4 重拟合**：walk-forward rc=0，推荐 S5_amt_liq_core（见 §5.1）。
- **Task 0098 S5 上线**：服务器工作副本已改，self_test passed。
- **Task 0099 S5 持久化 git**：rc=0，幂等 patch v9_edge/v10_optimize + git commit 6186957 push 到 main，git 成为持久 source of truth。
- **v9 生产引擎上线（0.2）**：v7_runner monkey-patch build_premarket_analysis_v9。
- **0102 cron 安装 / 0104 盘前全链路 + webhook 200**（见 §七）。

### ⬜ 待办
- **🔴 Task 0105（P0，最高优先，等用户点头）**：v9 edge 加高位/连板/前一日跌停风险闸门 + 修 qxlive T0 抓取时序 + 重跑验证 002674 类被 veto/flag（见 §零 0.6）。
- **Task 0094 上线校验**：pin QX-live 抓取到 ~9:25 竞价窗口，避免偶发 10:04 时间戳污染 premarket 特征；跑 v11 DoD 验收。
- **飞书多维表 meta**：待用户确认 app_token+table_id 或新建，写 `memory/feishu_bitable/duanxianxia_review.json`。
- **Task 0095（Deferred）**：T-1 lagged 表，搁置。

---

## 十、代码 Bug 全清单（M2 已全部修复）

| # | 位置 | Bug | Fix | 状态 |
|---|---|---|---|---|
| 1 | fetcher hotlist | 读 `hot_stock_day`（不存在）→ 0 行 | 改读 `hot_stock_hour` | ✅ |
| 2 | fetcher vratio/qiangchou | raw[2] 标签 mislabel | rename → `free_float_mktcap` | ✅ canonical |
| 3 | fetcher pool.hot | item[9] mislabel + item[7] 丢弃 + 无 raw | 修正 + 存 raw[] + 保 item[7] | ✅ |
| 4 | fetcher pool.surge | 换手率重算≠site item[10] | 取 site item[10] | ✅ |
| 5 | fetcher ztpool | `source_url` 拼接 | fix | ✅ |
| 6 | fetcher fengdan | qt.gtimg URL 疑 bug | 运行时确认正确，非 bug | ✅ 存档 |
| 7 | canonical-dict §A5 | "OPEN: Confirm 委买 vs 成交" | 已 RESOLVED（0082），下次更新删除 | ⬜ |
| 8 | v9_edge risk_penalty | 缺高位/连板/前一日跌停闸门 → 妖股打满分（002674）| 0105 补 risk 规则 | ⬜ P0 |

---

## 十一、审计文件索引

### docs/（main 分支）

| 文件 | 作用 |
|---|---|
| `rebuild-plan-v11.md` | v11 彻底重构权威执行口径 |
| `HANDOFF.md` | 本文件，全量交接（§零 为最新增量）|
| `canonical-field-dictionary.md` | 字段口径 source of truth（§A5 OPEN 已 RESOLVED）|
| `v10-field-alignment-decisions.md` | 因子对应 FINAL |
| `rebuild-design-v10.md` | KEEP/REBUILD + 迁移规则 |
| `field-rename-map.md` | 改造清单 + 代码 bug（方向全对）|

### scripts/（main 分支，L2–L4 + v9 引擎核心）

| 文件 | 作用 |
|---|---|
| `duanxianxia_premarket_v7_runner.py` | 生产入口，monkey-patch ACTIVE_ENGINE=v9（§零 0.2）|
| `duanxianxia_premarket_v9_runner.py` | v9 引擎编排入口 |
| `duanxianxia_v9_assemble.py` | v9 六层候选装配 |
| `duanxianxia_v9_edge.py` | L4 edge_core（S5 权重，0098/0099）+ risk_penalty（缺高位/连板闸门，0105 补）|
| `duanxianxia_v9_context.py` / `_v9_market_env.py` / `_v9_theme_strength.py` / `_v9_weimai.py` | v9 各层特征 |
| `duanxianxia_v9_output.py` | v9 动作层 shape_v9_output（REGIME_ACTION_GATE, BUY/WATCH/DROP）|
| `duanxianxia_canonical.py` | L2 registry + raw_to_canonical + 自检 |
| `duanxianxia_canonical_routing.py` | L2 kind→dataset 路由 |
| `duanxianxia_feature_builder.py` | L3 canonical-first 特征层 |
| `v10_optimize.py` | L4 walk-forward 优化 + V10AMT_W(S5) |
| `duanxianxia_batch.py` | 生产链：抓数据→build_premarket_analysis→bitable→webhook（无 9:25 门）|

### reports/_audit/（agent-results 分支）

| 文件 | 要点 |
|---|---|
| `0093_factor_refit_20260630.result.json` | rc=0，推荐 S5_amt_liq_core |
| `0097_m3_backfill_20260701.result.json` | rc=0，21 日 5910 行 |
| `0098_s5_weight_apply_20260701.result.json` | rc=0，S5 上线服务器 |
| `0099_s5_persist_git_20260701.result.json` | rc=0，S5 持久化 git main（commit 6186957）|
| `0104_premarket_backfill_20260701.result.json` | rc=0，v9 全链路+webhook 200；top=002674 score100 risks=[]（P0）|
| `m1b_feature_builder_probe_20260701.result.json` | rc=0，288 features |
| `m1b_auction_source_probe_20260630.result.json` | rc=0，Spearman 0.8009 |
| `weimai_deepdive_v50.json` | main_net_inflow_full=0.103；board_label 昨3连板最优 |
| `firstprinciples_v65.json` | gap 非线性；qx_median=29.0 |

---

## 十二、错误记录（勿重蹈）

| 错误 | 实际情况 | 教训 |
|---|---|---|
| 看到 raw[2] 数值断言"是量比" | 这些是各股 FF 市值（亿）| 数值判断必须交叉核对 live API + 已知 FF |
| `mcap_fields_by_name=[]` 证明"无市值" | census 按名字搜索，mislabel 搜不到 | census 结论 ≠ 值域结论 |
| 推翻 field-rename-map.md | rename-map 一直正确 | canonical-dict + rename-map 权威性高于单次名称普查 |
| 巨型脚本整体重写 | 单文件 API 静默转写漂移 | 用小补丁或在 raw 下游建新模块，别整体重写 105KB/145KB |
| HEREDOC `python3 - <<PY` | 转义/引号易坏 | 用 writeFile + `python3 file.py` |
| 0098 只改服务器工作副本 | git main 仍 baseline，下次 pull 覆盖 S5 | 改生产权重必须同时落 git（0099 git commit/push），否则运行时与 git 漂移 |
| 引用 6/30 收盘数据但当天已是 7/1 | 工具抓 7/1 实时失败，误用缓存旧数据 | 日期基准必须以当前 SH 日期为准，跨日务必核对 |
| 002674(score100,risks=[]) 次日跌停仍当好票 | v9 缺高位/连板风险闸门；诊断信息被 edge 剔除 | 「打分剔除的诊断字段」≠「可以不用于风控」；风险层要独立于打分 |
| 想把交接写成 Notion 页面 | 项目衔接靠 repo 内 `docs/HANDOFF.md` | 交接文档放项目里、按既有格式更新，不要另起炉灶 |
