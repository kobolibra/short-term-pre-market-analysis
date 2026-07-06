# 固定表字段契约 · fixed-table-contract

> **单一事实来源 (SSOT)**：竞价四表 + 封单表的字段登记、跨表同一性映射、合并取数优先级与单位归一规则。
>
> **改动纪律**：任何字段改名 / 新增 / 合并逻辑变动，**必须先改本文件，再改代码**。
>
> **关联代码**：`scripts/duanxianxia_feature_builder.py`（`_MERGE_PRIORITY` / `_assemble`）、`scripts/duanxianxia_canonical.py`、`scripts/duanxianxia_fetcher.py`

---

## 0. 单位约定（极重要）

| 数据源 | 金额单位 | 市值单位 |
|---|---|---|
| weimai | 元 | 元 |
| vratio / qiangchou / net_amount | 万 | 亿 |

**合并大表统一归一为：金额 = 元、市值 = 元**（万 × 1e4，亿 × 1e8）。
单位归一必须在任何比率 / 占比计算**之前**完成。

---

## 1. weimai 字段登记（源：`auction.jjyd.weimai` / daban.json）

| raw idx | canonical name | 含义 | 单位 |
|---|---|---|---|
| 0 | code | 股票代码（6 位，主键） | - |
| 1 | name | 股票名称 | - |
| 2 | price | 现价 | 元 |
| 3 | latest_change_pct | 最新涨幅 | % |
| 4 | seal_amount_wan_raw | **委买额（未剔除竞价成交）** | 元 |
| 5 | auction_change_pct | 竞价涨幅 | % |
| 6 | main_net_inflow | 主力净流入 | 元 |
| 7 | turnover_rate_pct | 换手率 | % |
| 8 | auction_turnover | 竞价成交额（真撮合） | 元 |
| 9 | auction_amount | 竞价成交量 | 股/手 |
| 10 | auction_turnover_dup | == raw8，冗余，**合并时剔除** | 元 |
| 11 | concept | 概念 / 题材 | - |
| 12 | free_float_mktcap | 流通市值 FF | 元 |
| 13 | main_net_inflow_full | 主力净流入（全） | 元 |
| 14 | super_large_net_inflow | 超大单净流入 | 元 |
| 15 | large_order_net_inflow | 大单净流入 | 元 |
| 16 | board_label | 板块标签 | - |
| 17 | seal_amount | 封单额（网站口径） | 万 |

> 命名遗留：raw4 名为 `seal_amount_wan_raw` 但单位是**元**，语义是**未剔除竞价成交的委买额**（≠ 真封单）。真封单 ≈ raw4 − raw8（委买 − 竞价成交）。

---

## 2. vratio / qiangchou 字段登记（源：getVratioData / getQiangchouData）

两表结构一致，仅 raw11 不同。

| raw idx | canonical name | 含义 | 单位 |
|---|---|---|---|
| 0 | code | 股票代码 | - |
| 1 | name | 股票名称 | - |
| 2 | free_float_mktcap_yi | 流通市值 FF | 亿 |
| 3 | seal_amount_wan | 封单额 | 万 |
| 4 | auction_change_pct | 竞价涨幅 | % |
| 5 | latest_change_pct | 最新涨幅 | % |
| 6 | auction_turnover_wan | 竞价成交额（= bidAmount） | 万 |
| 7 | concept | 概念 / 题材 | - |
| 10 | yesterday_auction_turnover_wan | 昨日竞价成交额 | 万 |
| 11 | volume_ratio_multiple（vratio）/ grab_strength（qiangchou） | 量比倍数 / 抢筹强度 | 倍 / - |
| 12 | turnover_rate_pct | 换手率 | % |

---

## 3. net_amount 字段登记（源：jjzhuli.json）

| raw idx | canonical name | 含义 | 单位 |
|---|---|---|---|
| 0 | code | 股票代码 | - |
| 1 | name | 股票名称 | - |
| 2 | auction_change_pct | 竞价涨幅 | % |
| 3 | latest_change_pct | 最新涨幅 | % |
| 4 | main_net_inflow_wan | 主力净流入 | 万 |
| 5 | auction_turnover_wan | 竞价成交额 | 万 |
| 6 | free_float_mktcap_yi | 流通市值 FF | 亿 |
| 8 | turnover_rate_pct | 换手率 | % |

---

## 4. fengdan 字段登记（源：jjlive.json + qt.gtimg 实时）

解析后为逐行表，唯一能反映**竞价封单时间演变**的表。

| 字段 | 含义 |
|---|---|
| code / name | 代码 / 名称 |
| board_label | 板块标签 |
| concept | 概念 |
| amount_915 | 09:15 封单额 |
| amount_920 | 09:20 封单额 |
| amount_925 | 09:25 封单额 |
| latest_change_pct | 最新涨幅（qt.gtimg 实时） |

---

## 5. 跨表字段同一性映射 + 取数优先级（核心）

> 同一语义在多表出现、命名/单位不同时的权威裁决表。合并时按“取数优先级”从左到右取首个非空值。

| 语义列 | 出现位置 | 取数优先级 | 理由 |
|---|---|---|---|
| 流通市值 FF | weimai12(元) / vratio2(亿) / qc2(亿) / na6(亿) | weimai → vratio → net_amount | weimai 直接给元、精度最高 |
| 竞价成交额 bidAmount | vratio6(万) / qc6(万) / na5(万) / weimai8(元) | **vratio → net_amount → weimai** | vratio 口径最稳；weimai8 为真撮合可兜底 |
| 封单额 | weimai17(万) / vratio3(万) / qc3(万) | weimai17 → vratio3 | weimai 网站口径优先 |
| 委买额（未剔竞价） | weimai4(元) | 仅 weimai | 独有字段，算真封单必需 |
| 主力净流入 | na4(万) / weimai6(元) | net_amount → weimai | net_amount 为专表 |
| 竞价涨幅 | 四表均有 | net_amount → vratio → weimai | - |
| 最新涨幅 | 四表 + fengdan | fengdan(qt.gtimg) → 其余 | fengdan 为实时 |
| 换手率 | vratio12 / qc12 / na8 / weimai7 | vratio → net_amount → weimai | - |
| concept | 四表均有 | weimai → net_amount | - |
| board_label | weimai16 / fengdan | weimai → fengdan | - |

---

## 6. 合并规则（大表装配）

1. **主键**：6 位 `code`。
2. **行集合**：竞价四表（vratio, qiangchou, net_amount, weimai）并集。
3. **去重**：按 `code` 去重，字段按第 5 节优先级填充（代码：`_MERGE_PRIORITY`）。
4. **单位归一前置**：万 × 1e4、亿 × 1e8，先归一再算任何比率。
5. **缺失回填**：按优先级链取首个非空值。
6. **剔除冗余**：`auction_turnover_dup`（weimai10 == raw8）。
7. **self-test 必绿**：
   - 合并行数 == 四表并集去重行数；
   - `sealAmount` == weimai raw17 × 1e4；
   - `sealAmountRaw` == weimai raw4；
   - `bidAmount` == vratio raw6 × 1e4（vratio 胜出）；
   - 关键列缺失率超阈值告警。

---

## 7. 自检样例（feature_builder v11）

- weimai 002407：raw[4]=2339609266，raw[8]=258717139，raw[17]=208089
- vratio 002407：raw[6]="1779" → bidAmount == 17_790_000（vratio 胜出）
- sealAmount == 208089 × 10000；sealAmountRaw == 2339609266
- qiangchou 300279：grab_strength == 11.93
