# duanxianxia v10 重构 · 全量交接文档

> **新对话开场必读顺序**：
> 1. 本文件（HANDOFF.md）← 先读这个
> 2. `docs/canonical-field-dictionary.md` ← 字段 source of truth
> 3. `docs/v10-field-alignment-decisions.md` ← 因子 source of truth
> 4. `docs/rebuild-design-v10.md` ← 架构决策
> 5. `reports/_audit/agent_jobs/0089_unit_probe_20260629.result.json`（agent-results 分支）← **读完再推进 Task 0090**
>
> 读完以上 5 份，即可无缝衔接继续推进。

最后更新：2026-06-29

---

## 一、项目定位

从 `https://duanxianxia.com/` 抓取 15+ 张盘前/盘中/盘后数据集，目标是建立一套：

```
raw[] 位置数组
  -> fetcher parse  -> capture 落盘（named rows）     [transform 1，当前有 mislabel]
    -> loader 时间切片 -> flat feature tables           [transform 2]
      -> v10 edge_core 评分 -> 飞书 webhook 推送
```

**当前阶段**：正在做 transform 1 的修正（canonical 层），Jobs 0001–0089 已完成，下一步是 Task 0090。

**Repo**: `kobolibra/short-term-pre-market-analysis`  
**main HEAD**: `56aad925b299c3589bc3e05d64fbc60f56cbcbf0`  
**agent-results 分支**: `a9af5a4b732e3f4196c82d513d7a9e81e341d7fd`  
**服务器项目根**: `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia`  
**fetcher 现行版本 SHA**: `d61c7be5`（`scripts/duanxianxia_fetcher.py`）

---

## 二、最关键结论（不要再验证，直接使用）

### vratio / qiangchou item[2] = FF 自由流通市值（亿），不是量比

**三次实证一致**：
- 多氟多 item[2]=462 ≈ 461.78亿（weimai 已确认 FF）✓
- 长虹美菱 item[2]=34 ≈ 34亿小盘股 ✓
- 华工科技 item[2]=1441 ≈ 大市值科技股 ✓

**原因**：`auction_volume_ratio` 这个字段名是 **mislabel**，实际存的是 FF 市值。  
**item[11]** 才是真正的量比（vratio 里是 `volume_ratio_multiple`，qiangchou 里是 `grab_strength`）。

`field-rename-map.md` 的改造方向一直是正确的，直接按它执行。

### 全局市值口径：circMcap = FF

所有市值相关因子分母统一用 FF（自由流通市值），单位统一转为**元**存储。

| 表 | FF 字段位置 | 备注 |
|---|---|---|
| vratio | raw[2] | 旧标签 auction_volume_ratio，是 mislabel |
| qiangchou | raw[2] | 同上 |
| net_amount | raw[6] | 旧标签 market_cap_yi |
| weimai | raw[12] | 旧标签 market_cap，job 0078 实证 FF |
| pool.hot | item[9] | 旧标签"流通"，实为 FF |
| pool.surge | item[9] | float_market_cap，标签正确，是 FLOAT（唯一用 FLOAT 的表）|
| review.fupan.plate | 多列 | 实际流通=FF / 流通市值=FLOAT / 总市值=TOTAL，校准锚表 |

---

## 三、已完成工作（Jobs 0001–0089）

| Jobs | 内容 |
|---|---|
| 0001–0044 | 抓取器、AES 解密、落盘规范、飞书推送全链路 |
| 0045 | premarket_raw_capture_audit_v36：22 交易日、9 数据集全量 IC 审计 |
| 0058–0063 | v10 factor IC 矩阵初版 |
| 0075–0078 | 市值口径全表核查；FF/FLOAT/TOTAL 三口径 weimai/net_amount 实证锁定 |
| 0079 | blast_radius（9.2MB）：下游消费方字段防御键全清单 |
| 0080 | field_caliber_dump（2026-06-29）：当日可抓表 mcap 核查 |
| 0082 | limitBuyAmountAfter920 = fengdan amount_920（委买/封单，非成交）|
| 0083 | fengdan section_* 全是金额（t15=150.4亿 > t20=39.1亿 ✓）|
| 0084 | QX-live PB = 今日封板率 ✓，~9:25 premarket-safe |
| 0085 | weimai deepdive v50：main_net_inflow_full IC=0.103 最高，super_large~large_order spearman=−0.919 高度冗余 |
| 0086 | firstprinciples v65：amount/换手正交化（r=0.627），gap 非线性（冷场景 IC=0.273，热场景 0.044），小市值超额 |
| 0087 | PR#28 squash→main（638dacf6）|
| 0088 | PR#29 squash→main（f20acdf）：fetcher d61c7be5 |
| 0089 | 探针脚本推 main（56aad925）：待验证 4 个未决单位 |

---

## 四、待验证项（新对话第一步）

### 4.1 读 0089 探针结果

**路径**（agent-results 分支）：  
`projects/duanxianxia/reports/_audit/agent_jobs/0089_unit_probe_20260629.result.json`

探针目标锁定以下 4 个未决单位：

| 字段 | 表 | 疑问 |
|---|---|---|
| seal_amount raw[17] | weimai | 万 还是 元？ |
| main_net_inflow_full raw[13] | weimai | 万 还是 元？ |
| super_large raw[14] / large_order raw[15] | weimai | 万 还是 元？ |
| free_float_mktcap item[9] | pool.hot | 显示"182亿"字符串，parse 后单位？ |
| float_mktcap item[9] | pool.surge | 显示"325亿"字符串，parse 后单位？ |

**读完 0089 result，锁定这些单位，再推进 Task 0090。**

### 4.2 qt.gtimg 双花括号 bug

`scripts/duanxianxia_fetcher.py` 中 `_fetch_realtime_quotes` 里：  
`url=f"{https://qt.gtimg.cn/q={symbols}}"` — 双花括号是否为 Python f-string bug，需运行时验证。

---

## 五、下一步任务列表

### Task 0090 ← 立即开始

**新建 `scripts/duanxianxia_canonical.py`（canonical 层脚本，核心交付物）**

功能要求：

1. **Registry**：`dataset_id -> { raw_kind, parse_spec, fields: [{canonical, caliber, unit, raw_ref}] }`
2. **`raw_to_canonical(dataset_id, raw_row)`** → 返回 canonical dict
3. 单位转换全在这里：亿×1e8 → 元，万×1e4 → 元
4. 字段重命名全在这里（按 `canonical-field-dictionary.md`）
5. **`_self_test()` 硬编码真实样本断言**（必须包含）：
   - vratio 多氟多 raw[2]=462 → `free_float_mktcap=46_200_000_000`（元）✓
   - vratio raw[11]=6.1 → `volume_ratio=6.1`（倍，不做单位转换）✓
   - 任何把 raw[2] 当量比或把 raw[11] 当市值的映射 → **FAIL**
6. **Caliber validator**：任何 market_cap 类字段缺少 caliber tag → build 报错

### Task 0091

**fetcher 接 canonical 层**
- `fetcher.parse` 结果统一走 `canonical.raw_to_canonical()`
- 消费方防御键前置 canonical 名，保留旧 fallback 兼容
- `pool.hot`：开始存 raw[]，存 item[7] 板态
- fix：`pool.surge` turnover 取 site item[10]，stop recompute
- fix：`rank.hot_stock_day` 改读 `hot_stock_hour`（旧代码读 `hot_stock_day` → 始终 0 行）

### Task 0092

**历史重派生**
- 有 raw 的 capture（vratio/qiangchou/surge/net_amount/weimai）→ 从 raw 重派生 canonical 行
- `pool.hot` 无历史 raw → 标记 `legacy_unrecoverable`
- 重生 flat 特征 CSV（`_all_candidates_flat.csv`, `feature_matrix_v21.csv`）

### Task 0093

**新因子接线**
- `origin`（fromPrevBrokenLimitUp / fromPrevSealedLimitUpWithOpen）
- `themeConsistency` / `themeConcentration`
- `auctionSealAmount` / `marketSealRate` / `prevDayLimitUpSealRate`
- `stockMainlineFit`
- 接入 edge_core，用校正后的 canonical 输入**重拟合系数**

### Task 0094

**Pin QX-live 抓取时间**：确保 `home.qxlive.top_metrics` 固定在 ~9:25 竞价窗口内抓取，避免盘后时间戳污染 premarket 特征。

### Task 0095（Deferred）

T-1 lagged 表，搁置，后续专门处理。

---

## 六、v10 因子框架（FINAL）

### edge_core 公式

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

### 逐因子 canonical 来源

| 因子 | canonical 来源 | 单位/说明 |
|---|---|---|
| bidAmount | auction_turnover（竞价五表均有）| 万→元 |
| bidStrength | auction_turnover / free_float_mktcap × 10000 | 两者同元基准 |
| volumeRatio | vratio raw[11]（volume_ratio）| **不是 raw[2]！** |
| changeRate | auction_change_pct | % |
| limitBuyAmountAfter920 | fengdan amount_920 | 9:20 不可撤委买→元 |
| prevStreak | fupan 连板 / ztpool 阶梯 | |
| prevOpenNum | fupan 开板数 | |
| brokenLimitUp | ztpool 状态=炸 | |
| origin | fromPrevBrokenLimitUp / fromPrevSealedLimitUpWithOpen | ztpool+fupan 派生 |
| stockMainlineFit | concept vs kaipan top 板块强度 | |
| sentimentSignal | QX-live QX 值 | |
| themeConsistency | count(高开)/count(合格标的) | 题材内一致性 |
| themeConcentration | 题材 bidAmount / 全市场 bidAmount | |
| prevDayLimitUpSealRate | num/(num+open) T-1 EOD | |
| auctionSealAmount | fengdan section_bid_amount_t25 / section_seal_amount | |
| marketSealRate | QX-live PB 今日封板率 | ~9:25 premarket-safe |

### 关键 IC 结论

| 因子 | mean_IC | ICIR | 来源 |
|---|---|---|---|
| rocket_rank | 0.222 | 1.027 | v36 |
| seal_to_mcap_ratio | 0.123 | 0.318 | v36 |
| big_order_share | 0.111 | 0.514 | v36 |
| latest_change_pct（vratio）| 0.119 | 0.369 | v48 |
| auction_turnover_wan（vratio）| 0.093 | 0.487 | v48 |
| main_net_inflow_full（weimai）| 0.103 | — | v50 |
| ic_amount（firstprinciples）| 0.134 | 0.596 | v65 |
| ic_gap 冷场景 | 0.273 | 0.809 | v65 |
| 小市值三分组超额 | +0.131 | — | v65 |

- `super_large` ~ `large_order` spearman = −0.919：高度冗余，合并或选其一
- 情绪冷场景 IC 是热场景 2×：需 REGIME_ACTION_GATE 分场景权重

---

## 七、数据集字段速查（修正后）

完整版见 `docs/canonical-field-dictionary.md`，这里只列修正要点。

### vratio raw[13 cols]
```
[0]code [1]name
[2]free_float_mktcap (FF/亿→元)  ← 旧标签 auction_volume_ratio，MISLABEL
[3]seal_amount (万→元)
[4]auction_change_pct (%)
[5]latest_change_pct (%)
[6]auction_turnover (万→元)
[7]concept
[8–10] 文本/昨日成交重复
[11]volume_ratio (倍) ← 这才是量比
[12]turnover_rate (%)
```

### qiangchou raw[13 cols]
同 vratio，除：`[11]=grab_strength`（不是量比）  
两个 group：`grab`（9:25最后1秒）+ `qiangchou`（9:20–9:25）

### weimai raw[18 cols]
```
[4]auction_turnover(元) [6]main_net_inflow(元)
[12]free_float_mktcap(FF/元) ← 旧标签 market_cap
[13]main_net_inflow_full [14]super_large_order [15]large_order  ← 单位待 0089 确认
[16]board_label [17]seal_amount(万→元, 待确认)
```

### net_amount raw[9 cols]
```
[4]main_net_inflow(万→元) [5]auction_turnover(万→元)
[6]free_float_mktcap(FF/亿→元) ← 旧标签 market_cap_yi
```

### pool.hot / pool.surge
- hot item[9] = FF（旧标签"流通"，是 mislabel；**无历史 raw**）
- surge item[9] = FLOAT（标签正确，唯一用流通市值的表）
- surge turnover：fix → 取 site item[10]，不重算

### rank.hot_stock_day
⚠️ **严重 bug**：代码读 `hot_stock_day`（不存在）→ 0 行。  
Fix：改读 `hot_stock_hour`。

---

## 八、代码 bug 清单（field-rename-map.md §10 完整版）

1. **hotlist_day**：`hot_stock_day` → 改 `hot_stock_hour`（~699 处引用）
2. **vratio/qiangchou**：raw[2] rename `auction_volume_ratio` → `free_float_mktcap`
3. **pool.hot**：item[9] 流通 mislabel + item[7] 板态丢弃 + 无 raw
4. **pool.surge**：换手率重算 → 取 site item[10]；item[7] 板态丢弃
5. **ztpool**：`source_url` 含字面量 `" + "`，需 fix 字符串拼接
6. **qt.gtimg**：`_fetch_realtime_quotes` 双花括号疑 f-string bug

---

## 九、关键审计文件（agent-results 分支 reports/_audit/）

| 文件 | 内容 | 注意 |
|---|---|---|
| `vratio_deepdive_v48.json` | vratio IC/覆盖率 | field_ic 里的 `auction_volume_ratio` 实为 FF 市值，IC=0.058 是市值的 IC |
| `qiangchou_deepdive_v46.json` | qiangchou IC | 同上 |
| `weimai_deepdive_v50.json` | weimai IC | main_net_inflow_full IC=0.103 最高 |
| `firstprinciples_v65.json` | amount/换手正交、gap 非线性 | 冷场景 IC=0.273 |
| `field_census_0076.json` | 各表 headers/sample | `mcap_fields_by_name` 仅按字段名搜索，vratio/qiangchou 会漏（名字是 mislabel）|
| `0089_unit_probe_20260629.result.json` | **待读！** 4 个未决单位 | **新对话必须先读这个** |

---

## 十、AES 解密参数

```python
key = 'secretkey322yes!!aaaaaaaaaaaaaaa'
iv  = 'fixediv_16valued'
# CBC + PKCS7 unpad + base64
```

---

## 十一、cron 调度

| 任务 | cron（Asia/Shanghai）| 内容 |
|---|---|---|
| 盘前 | `25 9 * * 1-5` | premarket：竞价4表+fengdan+kaipan+qxlive |
| 10:01 | `1 10 * * 1-5` | intraday_cashflow |
| 盘后 | `20 17 * * 1-5` | postmarket_cashflow |

cron worker 幂等；队列 `scripts/agent_jobs/queue/<id>.json`；results 推 agent-results 分支（publish 有 ~10 分钟延迟）。

---

## 十二、错误记录（勿重蹈）

> 记录本对话中发生的错误，供后续参考。

1. **错误**：看到 raw[2] 值=17/499/1983 就断言"是量比不是市值"，没有交叉核对实际公司市值。  
   **教训**：数值判断必须交叉核对（live API + 已知 FF 值），不能凭感觉。

2. **错误**：用 `field_census.mcap_fields_by_name=[]` 作为"该表无市值"的证据。  
   **教训**：census 按字段名关键词搜索，字段名本身是 mislabel 时会漏；要结合值域验证。

3. **错误**：轻信有缺陷的两份证据，推翻了 `field-rename-map.md` 的正确结论，导致多轮反复。  
   **教训**：`canonical-field-dictionary.md` 和 `field-rename-map.md` 是 jobs 0075–0081 + live endpoint probe 后写下的，权威性高于单次名称普查。
