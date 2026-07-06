# 竞价指标口径 + 维度框架 · 会话交接（2026-07-06）

> 本文件 = 2026-07-06「竞价字段口径厘清 + 维度指标重设计」专题交接，与 `docs/HANDOFF.md`（edge 引擎 / 风控闸门主线）**并列**。新对话处理竞价字段本质、真封单口径、fengdan 接入、D1–D6 维度重构时，先读本文件。
>
> **权威源码**：`scripts/duanxianxia_canonical.py`（REGISTRY，SHA `5bfe80c4`）= 字段定义唯一真源；特征层 `scripts/duanxianxia_feature_builder.py`（`feature_builder_v11.0`，blob `d658156f`）。
>
> **⚠️ 勘误**：`docs/HANDOFF.md §4.4` 的 weimai raw4/raw8/raw17 映射是**改名前旧版、已过时**（它写 raw4=auction_turnover、raw17=seal_amount_wan）。现行权威 = 本文件第二节 / canonical REGISTRY / Notion 框架页 §2.1，且已被 0164/0165 实测坐实：**raw4=涨停委托(未剔成交)、raw8=竞价成交、raw17=动态封单**。

---

## 〇、一句话现状
竞价核心字段口径已**全部交叉验证敲定**：真封单 = 涨停委托(weimai raw4) − 竞价成交(weimai raw8) = fengdan f925；raw17 动态封单弃用；raw8=竞价成交==vratio raw6；竞价涨幅/竞价成交四表皆有；抢筹强度保留（非量比重复）；资金占比=主力净流入÷竞价成交。

下一步：**先改文档 → feature_builder 接入 fengdan（915/920/925 + 真封单 + 分歧）→ indicator_builder 落地 D1–D6 → import-time self-test → 排验证 job 0166。**

**维度「口径」已敲定；维度「结构」（下方六维正交）为提案，待用户最终拍板**（定价与量能是否合并、连板高度是否独立成维、分歧取哪一/两个角度）。

---

## 一、已敲定（cross-validated，直接用，勿再验证）

### 1.1 真封单口径（核心）
- `raw4` = `seal_amount_wan_raw` = 涨停委托（**未剔竞价成交**，元，9:25 竞价时点固定）。
- `raw8` = `auction_turnover` = 竞价成交（元，9:25 竞价时点固定）**== vratio raw6**（0165：median raw8/vbid = 0.99998）。
- **真封单 = raw4 − raw8 = fengdan f925**。证据：
  - 0164：median (raw4 − 竞价成交)/f925 = **0.9997**，resid **−0.03%**（fengdan 与 weimai 两独立源）。
  - 0165：median (raw4 − raw8)/f925 = **1.0003**。
- 三者同为 9:25 竞价时点值，**与抓取时间无关**，恒非负。→ D4 真封单/承接用此，永不出负值。

### 1.2 raw17 弃用
- `raw17` = `seal_amount`（落盘 万→×1e4 元）是**动态实时封单**，随抓取时间变。
- 0165：median raw17/f925 = **0.30**（且离散）；恒尚 raw17=3.489亿 ≠ 真封单 27.7亿。
- **不用于任何指标**。落盘口径不变（`fixed-table-contract.md §9.5` storage 规则照旧，仅指标层不采用）。

### 1.3 竞价涨幅 / 竞价成交 来源（四表皆有）
- 竞价涨幅 `auction_change_pct`：vratio raw4 / qiangchou raw4 / net_amount raw2 / **weimai raw5（auction_change）**。
- 竞价成交 `auction_turnover`：vratio raw6 / **qiangchou raw6** / net_amount raw5 / weimai raw8。

### 1.4 抢筹强度保留
- `grab_strength`（qiangchou raw11）与 `volume_ratio`（vratio raw11）是**不同字段、不同表、不同语义，非重复**。此前「重复」结论无证据，已撤销。**两者都保留。**

### 1.5 资金占比口径
- 资金占比 = **主力净流入 ÷ 竞价成交额(raw8/bidAmount)**，不是 ÷FF。

### 1.6 删除的旧指标（口径混乱/重复）
- `d1_auction_amount_pct`（= 委托/FF = 换手，重复）。
- d3 里的 超大单/大单 进竞价维度 + `d3_money` 合计（全天口径，混时间）。
- `d5_weimai_strength`（= 委买/FF；委买已在真封单用，重复）。
- raw17 动态封单。

### 1.7 FF 覆盖（纠错）
- 0165：n_fengdan=113，in_any_of_4=46，**fengdan_with_FF=46**，fengdan_with_bid=46。
- fengdan 票在竞价四表并集（合并大表）按 code 取 FF（46/113 有）。
- ⚠️ 此前「只有 8 只有 FF」**是错的**：8 = `fengdan_925_nonzero`（封住数），非 FF 覆盖。

---

## 二、weimai 18 列字典（canonical REGISTRY，SHA `5bfe80c4`，权威）

| raw | 规范字段 | 含义 | 单位 |
|---|---|---|---|
| 0 | code | 代码 | — |
| 1 | name | 名称 | — |
| 2 | price | 股价 | 元 |
| 3 | latest_change_pct | 最新涨幅 | % |
| **4** | **seal_amount_wan_raw** | **涨停委托（未剔竞价成交）= f925 + 竞价成交** | **元** |
| **5** | **auction_change_pct（源码名 auction_change）** | **竞价涨幅** | **%** |
| 6 | main_net_inflow | 主力净流入（竞价口径） | 元 |
| 7 | turnover_rate | 换手率 | % |
| **8** | **auction_turnover** | **竞价成交（真实撮合，== vratio raw6）** | **元** |
| 9 | auction_amount | 竞价量 | 股/手 |
| 10 | auction_turnover_dup | == raw8（冗余，不落库） | 元 |
| 11 | concept | 概念题材 | — |
| 12 | free_float_mktcap | 流通市值 FF | 元 |
| 13 | main_net_inflow_full | 主力净流入（全口径） | 元 |
| 14 | super_large_order | 超大单净流入（**全天口径**） | 元 |
| 15 | large_order | 大单净流入（**全天口径**） | 元 |
| 16 | board_label | 板态（首板/几板） | — |
| **17** | **seal_amount** | **动态实时封单（随抓取时间变，已弃用）** | 万→元 |

- 恒等式 `raw4 − raw8 = raw17` **仅在 self_test（合成数据）成立**；真实一字板不成立（raw17 动态）。**真封单以 raw4 − raw8 = f925 为准。**
- 超大单(14)/大单(15) 是**全天累计**口径，与竞价时点不一致，**不进竞价维度**。

---

## 三、竞价其余表 + fengdan 字段速查

- **vratio / qiangchou**（`auction.jjyd.{vratio,qiangchou}`，raw[0..12]，两表结构一致，仅 raw11 不同）：
  - raw2 FF(亿) / raw3 seal_amount(万) / raw4 竞价涨幅 / raw5 最新涨幅 / raw6 竞价成交(万=bidAmount) / raw7 concept / raw10 昨日竞价成交(万) / **raw11 = vratio:volume_ratio 量比 ｜ qiangchou:grab_strength 抢筹强度** / raw12 换手率(%)。
  - qiangchou response 有两组 `grab`(9:25 最后1秒) + `qiangchou`(9:20–9:25)，均须保留。
- **net_amount**（`auction.jjyd.net_amount`，raw[0..8]）：raw2 竞价涨幅 / raw3 最新涨幅 / raw4 主力净流入(万) / raw5 竞价成交(万) / raw6 FF(亿) / raw8 换手率(%)。
- **fengdan**（`auction.jjlive.fengdan`，named dict，AES + qt.gtimg 实时涨幅）：`code/name/rank/board_label`、`seal_bid_915`=amount_915、`seal_bid_920`=amount_920、`seal_bid_925`=amount_925（涨停价委买/封单累计额，元；口径 commit_bid；`-` = 未封需 null）、`latest_change_pct`、concept=tag_1、section 层 `section_yizi_count / section_seal_total / section_t15/t20/t25_total`（t15/20/25 是金额非计数）。**当前未接入 feature_builder。**

---

## 四、已证伪 / 已纠正（勿重蹈）

| 错误说法 | 事实 |
|---|---|
| raw8 ≈ 27.6亿 | 我用错误恒等式 raw4−raw8=raw17 反推的假数。真实 raw8=竞价成交，恒尚仅 0.065亿；raw17 才是那个动态大数 3.489亿 |
| 竞价涨幅无 weimai 来源 | weimai raw5 有 |
| 竞价成交无 qiangchou 来源 | qiangchou raw6 有 |
| 抢筹强度与量比重复 | 无证据，非重复，均保留 |
| 只有 8 只 fengdan 有 FF | 46 只有 FF；8 是封住数(925≠0) |
| raw17 = 真封单 | raw17 动态，弃用；真封单=raw4−raw8=f925 |
| HANDOFF §4.4 weimai 映射 | 改名前旧版，已过时；以 canonical REGISTRY(SHA 5bfe80c4)为准 |

---

## 五、维度框架（提案：六维正交；**口径已敲定，结构待用户拍板**）

| 维度 | 本质 | 指标 | 口径 |
|---|---|---|---|
| **D1 定价强度** | 竞价定价意愿 | 竞价涨幅；(连板高度作上下文) | 竞价时点 |
| **D2 量能/参与** | 参与规模 | 竞价成交额(绝对) + 量比 + 换手率；**抢筹强度保留** | 竞价 |
| **D3 资金质量** | 主力主导度 | 主力净流入；资金占比 = 主力净流入 ÷ 竞价成交额 | 竞价口径 |
| **D4 封板承接** | 封单厚度 | 真封单 = raw4 − raw8；承接率 = 真封单 ÷ FF | 竞价时点，非负 |
| **D5 分歧** | 买盘一致性 | 时间：(raw4 − f920)/f920（仅 925≠0 时）；成交/委托：raw8 ÷ raw4 | 竞价，同口径 |
| **D6 情绪/环境** | 板块背景 | section 一字数、封单总额、t15-25 截面、题材/梯队 | 板块层面 |

**待用户拍板**：① 定价(D1)与量能(D2)是否合并；② 连板高度是否独立成维；③ 分歧(D5)最终取时间维度、成交/委托维度还是两者都要。

> 旧版 D1–D6（D1 竞价量能占比=委托/FF 等）见 Notion 框架页 §5，已标记「取代」，仅作历史对照。

---

## 六、代码现状（敲定 vs 待改）

- **canonical REGISTRY**（`duanxianxia_canonical.py`，SHA `5bfe80c4`）：weimai/vratio/qiangchou/net_amount/fengdan 定义齐全，✅ 权威，勿再探针问定义。
- **feature_builder**（`duanxianxia_feature_builder.py`，blob `d658156f`，`feature_builder_v11.0`）：`AUCTION_DATASETS` = 4 张 jjyd 表，**fengdan 未接入** → 需加。`_MERGE_PRIORITY` 逐字段。输出 bidAmount(元)/bidStrength/volumeRatio/grabStrength/changeRate/turnoverRate/mainNetInflow/mainNetInflowFull/superLargeOrder/largeOrder/sealAmount(raw17)/sealAmountRaw(raw4)/free_float_mktcap。**import 时跑 `_self_test()`（阻塞 import，改后必须保持绿）。**
- **当前(待替换)指标**：listing `duanxianxia_indicator_listing_0157.py`（SHA `a3a67b82`），367 行，按 `d1_auction_amount_pct` 排序（该指标将删）。
- **edge 生产权重 S5**（见 `HANDOFF.md §5.1`）：本次不动，维度重构后再评估。

---

## 七、下一步（新对话按序接手）

1. **先改文档（SSOT）**：本 doc ✅ + Notion 框架页 §5 已标记取代/重大修订 ✅；`fixed-table-contract.md §9.5` raw17 落盘不变（仅指标层不用）。
2. **feature_builder 接入 fengdan**：暴露 `seal_bid_915/920/925`、真封单=raw4−raw8、分歧（时间 (raw4−f920)/f920 仅 925≠0；成交/委托 raw8/raw4）。保持 `_self_test` 绿。
3. **indicator_builder 落地 D1–D6**：删第 1.6 清单；资金占比=主力/竞价成交；承接=真封单/FF；保留抢筹强度。
4. **push + 排验证 job 0166**（git-queue，cron ~10min，结果读 agent-results）。
5. **监控**次日 09:25 抓取 + 09:30 self-check `ok:true`。

---

## 八、运行 / 坐标

- **Repo** `kobolibra/short-term-pre-market-analysis`；main HEAD `64951794`（0165 queue 后）。读 `mcpServer_github3` / 写 `mcpServer_github7`（`create_or_update_file` 改已存在文件须带 sha；串行提交同分支避免 409；`create_or_update_file` 大文件 ~39KB 截断——大文件勿整写）。
- **Job 机制**：git-queue + GCP cron worker ~10min。队列 `scripts/agent_jobs/queue/<id>.json`；结果 agent-results 分支 `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json`。**下一空闲 id = 0166**（0164/0165 已完成）。VM 时钟 +68min；stdout_tail 从前截断→关键输出放最后。
- **本次 commits**：0164 probe `17c684cc` / queue `cf4126bd`；0165 probe `1171e996` / queue `64951794`。
- **0164 result** SHA `38d43d2c`；**0165 result**（worker 2026-07-06T19:30，ok，3.5s）。
- **Notion**：框架页（🧭，private，full_access）= 维度 SSOT；**勿动** raw 页（📊，do-not-edit）。Notion 编辑须带 editDescriptionVariableName + `<edit_reference>`；GitHub push 不需要。
- **安全**：用户曾在聊天粘贴明文 Fine-grained PAT → 建议尽快 revoke + 轮换。

---

## 九、0164 / 0165 关键数字（存档）

**0165 coverage**：`{n_fengdan:113, n_weimai:150, n_vratio:175, n_net_amount:49, n_qiangchou:68, fengdan_925_nonzero:8, in_weimai:27, in_vratio:25, in_net_amount:7, in_qiangchou:3, in_any_of_4:46, fengdan_with_FF:46, fengdan_with_bid:46}`

**0165 stats（medians）**：raw8/(raw4−f925)=0.998；raw8/vbid=0.99998；(raw4−raw8)/f925=1.0003；raw17/f925=0.30；raw17/(raw4−f925)=4.00；f920/raw4=0.878；f920/(f925+vbid)=0.285。

**样本行（亿；f915/f920/f925 | raw4 | raw8 | raw17 | FF）**：
- 603137 恒尚节能 | 67.3/27.6/27.7 | 27.784 | 0.065 | 3.489 | 15.768 → raw4−raw8=27.719≈f925 ✓
- 603722 阿科力 | 53.5/18.7/18.8 | 18.886 | 0.043 | 2.187 | 24.241
- 002607 中公教育 | 2.2/2.7/5.2 | 5.786 | 0.612 | 2.918 | 108.779
- 002396 星网锐捷 | 7.3/3.5/2.9 | 3.966 | 1.063 | 2.006 | 143.236
- 000656 金科股份 | 5.5/3.2/2.3 | 3.516 | 1.239 | 1.024 | 75.667

**0164**：H1 confirmed，f925 = raw4 − 竞价成交，median 0.9997，resid −0.03%；925 = 真封单（fengdan × weimai 两独立源一致，恒非负）。
