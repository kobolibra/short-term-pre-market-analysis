# durable_premarket_signal_selector_v2 效果复盘（2026-05-19 / 2026-05-20 / 2026-05-21）

## 说明

- 代码版本：`748b5df refactor: rebuild v7.3 around durable premarket signals`
- selector：`durable_premarket_signal_selector_v2`
- 口径：同日超额收益 = 收盘涨幅 - 竞价涨幅
- `2026-05-21` 收盘涨幅使用盘后重新下载的 5 张“竞价异动”表中的网页 `涨幅` 列。
- **注意：`2026-05-21` 本地 5 张“竞价异动”表已经被盘后数据覆盖，因此这次重跑产物里该日的 `auction_pct` 与盘后 `latest_change_pct` 同源，导致 `2026-05-21` 的超额收益整体退化为 `0`。所以这一天当前只能用于看新版 selector 的名单分层，不能当成严格的真实绩效评估。**

## 2026-05-21 使用的盘后 5 表

- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.vratio/092543.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.qiangchou/092543.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.net_amount/092544.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjlive.fengdan/092546.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.weimai/092547.json`

## 2026-05-19

- 市场状态：`normal`
- 候选数：`215`
- BUY 数量：`3`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 3 | 3 | -0.016 | 1.417 | 1.700 | 3 | 0 |
| WATCH | 8 | 8 | -0.241 | 4.112 | 2.527 | 6 | 2 |
| REJECT | 18 | 18 | 1.922 | -0.191 | -0.671 | 8 | 10 |
| AVOID | 11 | 11 | 9.041 | 0.487 | -0.002 | 5 | 6 |

### BUY 明细

| code | name | pre_action | auction_pct | close_pct | excess_return | reason |
|---|---|---|---:|---:|---:|---|
| 002081 | 金螳螂 | LOW_OPEN_REVERSAL | -4.530 | -2.405 | 2.126 | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |
| 002407 | 多氟多 | LOW_OPEN_REVERSAL | -2.040 | -1.614 | 0.426 | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |
| 002580 | 圣阳股份 | MOMENTUM_CATCHUP | 2.270 | 3.970 | 1.700 | BUY:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM |

### 漏选 / 错杀

- WATCH 中最强：
  - `603773 沃格光电`｜超额 `13.841%`｜WATCH:LOW_OPEN_REVERSAL:reversal_cost_not_discounted_or_too_deep
  - `600396 华电辽能`｜超额 `12.752%`｜WATCH:LOW_OPEN_REVERSAL:reversal_cost_not_discounted_or_too_deep
  - `600584 长电科技`｜超额 `6.682%`｜WATCH:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL
  - `300537 广信材料`｜超额 `2.918%`｜WATCH:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM
  - `002979 雷赛智能`｜超额 `2.137%`｜WATCH:THEME_CATCHUP:theme_is_watch_only
- REJECT 中最强：
  - `603311 金海高科`｜超额 `7.756%`｜REJECT:THEME_CATCHUP:theme_unstable_no_buy
  - `002196 方正电机`｜超额 `4.203%`｜REJECT:THEME_CATCHUP:theme_is_watch_only
  - `600903 贵州燃气`｜超额 `3.289%`｜REJECT:AUCTION_FOLLOW:follow_cost_bad
  - `300454 深信服`｜超额 `1.686%`｜REJECT:THEME_CATCHUP:theme_unstable_no_buy
  - `301502 华阳智能`｜超额 `1.506%`｜REJECT:THEME_CATCHUP:theme_unstable_no_buy

## 2026-05-20

- 市场状态：`cold`
- 候选数：`213`
- BUY 数量：`1`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 1 | 1 | 3.026 | -0.004 | -0.004 | 0 | 1 |
| WATCH | 7 | 7 | -5.083 | -0.000 | -0.001 | 2 | 5 |
| REJECT | 3 | 3 | 0.165 | 0.001 | 0.001 | 2 | 1 |
| AVOID | 19 | 19 | -2.658 | -0.001 | -0.001 | 6 | 10 |

### BUY 明细

| code | name | pre_action | auction_pct | close_pct | excess_return | reason |
|---|---|---|---:|---:|---:|---|
| 002208 | 合肥城建 | MOMENTUM_CATCHUP | 3.030 | 3.026 | -0.004 | BUY:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM |

### 漏选 / 错杀

- WATCH 中最强：
  - `600578 京能电力`｜超额 `0.004%`｜WATCH:LOW_OPEN_REVERSAL:reversal_cost_not_discounted_or_too_deep
  - `605299 舒华体育`｜超额 `0.004%`｜WATCH:LOW_OPEN_REVERSAL:reversal_cost_not_discounted_or_too_deep
  - `600208 衢州发展`｜超额 `-0.001%`｜WATCH:LOW_OPEN_REVERSAL:reversal_cost_not_discounted_or_too_deep
  - `301696 三瑞智能`｜超额 `-0.001%`｜WATCH:LOW_OPEN_REVERSAL:reversal_cost_not_discounted_or_too_deep
  - `301379 天山电子`｜超额 `-0.002%`｜WATCH:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL
- REJECT 中最强：
  - `002785 万里石`｜超额 `0.004%`｜REJECT:THEME_CATCHUP:theme_unstable_no_buy
  - `603311 金海高科`｜超额 `0.001%`｜REJECT:THEME_CATCHUP:theme_is_watch_only
  - `301023 奕帆传动`｜超额 `-0.001%`｜REJECT:THEME_CATCHUP:theme_unstable_no_buy

## 2026-05-21

- 市场状态：`cold_to_warming`
- 候选数：`374`
- BUY 数量：`3`
- 收盘涨幅覆盖：`374/374`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 3 | 3 | -3.090 | 0.000 | 0.000 | 0 | 0 |
| WATCH | 8 | 8 | -1.660 | 0.000 | 0.000 | 0 | 0 |
| REJECT | 15 | 15 | 0.585 | 0.000 | 0.000 | 0 | 0 |
| AVOID | 18 | 18 | -0.355 | 0.000 | 0.000 | 0 | 0 |

### BUY 明细

| code | name | pre_action | auction_pct | close_pct | excess_return | reason |
|---|---|---|---:|---:|---:|---|
| 600863 | 华能蒙电 | MOMENTUM_CATCHUP | 3.720 | 3.720 | 0.000 | BUY:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM |
| 002428 | 云南锗业 | LOW_OPEN_REVERSAL | -7.910 | -7.910 | 0.000 | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |
| 300657 | 弘信电子 | LOW_OPEN_REVERSAL | -5.080 | -5.080 | 0.000 | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |

### 漏选 / 错杀

- WATCH 中最强：
  - `601991 大唐发电`｜超额 `0.000%`｜WATCH:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL
  - `601138 工业富联`｜超额 `0.000%`｜WATCH:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL
  - `000100 TCL科技`｜超额 `0.000%`｜WATCH:AUCTION_FOLLOW:AUCTION_FOLLOW_THROUGH
  - `600758 辽宁能源`｜超额 `0.000%`｜WATCH:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM
  - `300166 东方国信`｜超额 `0.000%`｜WATCH:LOW_OPEN_REVERSAL:reversal_cost_not_discounted_or_too_deep
- REJECT 中最强：
  - `600584 长电科技`｜超额 `0.000%`｜REJECT:THEME_CATCHUP:theme_is_watch_only
  - `600903 贵州燃气`｜超额 `0.000%`｜REJECT:THEME_CATCHUP:theme_is_watch_only
  - `300776 帝尔激光`｜超额 `0.000%`｜REJECT:THEME_CATCHUP:theme_is_watch_only
  - `688008 澜起科技`｜超额 `0.000%`｜REJECT:THEME_CATCHUP:theme_is_watch_only
  - `688082 盛美上海`｜超额 `0.000%`｜REJECT:THEME_CATCHUP:theme_is_watch_only

## 合并结论（3天）

- BUY 平均超额收益：`0.607%`（样本 `7`，中位数 `0.000%`）
- WATCH 平均超额收益：`1.430%`（样本 `23`，中位数 `0.000%`）
- REJECT 平均超额收益：`-0.095%`（样本 `36`，中位数 `0.000%`）
- AVOID 平均超额收益：`0.111%`（样本 `48`，中位数 `0.000%`）

## 结论

- `2026-05-19`：BUY 平均超额 `1.417%`，表现为正，但 WATCH 里仍留有不少强票。
- `2026-05-20`：BUY 只剩 `1` 只（合肥城建），超额 `-0.004%`，这天本质上还是没有 alpha。
- `2026-05-21`：由于本地 `2026-05-21` 的 5 张“竞价异动”表已经被盘后数据覆盖，本次重跑里该日 `auction_pct` 与 `close_pct` 同源，所以 BUY / WATCH / REJECT / AVOID 的超额收益统一退化到 `0`，**不能作为严格真实绩效**。
- 三天合并后，当前表面上 BUY 平均超额 `0.607%`，WATCH 平均超额 `1.430%`；但这个合并值已经被 `2026-05-21` 的同源数据污染，**严格可比的真实评估应只信 `2026-05-19 / 2026-05-20`，或等恢复 `2026-05-21` 的盘前原始 09:25 capture 后再重算。**
- 这版 durable selector 相比上一版更保守，BUY 更少；从 `2026-05-19 / 2026-05-20` 看，分层方向不差，但 BUY 的优势还不够大，仍有明显漏到 WATCH 的强票。
- 如果继续优化，优先该处理的是：如何把 `CONFIRMED_MOMENTUM` / `LOW_OPEN_NET_REVERSAL` 里的高质量 WATCH 再提纯进 BUY，而不是继续一味缩表。

