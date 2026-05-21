# durable_premarket_signal_selector_v2 效果复盘（2026-05-19 / 2026-05-20 / 2026-05-21）

## 更正说明

- 上一版复盘是我算错了：把 `latest_change_pct` 错当成了 `auction_pct` 的兜底。
- 正确口径：`auction_change_pct` = 竞涨 / 竞价涨幅；`latest_change_pct` = 涨幅 / 实时涨幅 / 盘后收盘涨幅。
- `2026-05-19 / 2026-05-20`：竞价涨幅优先取当日 `09:25` capture 的 `auction_change_pct`；若该股在 09:25 capture 里该列为空，再回退到同一批 `09:25` capture 的 `latest_change_pct`。
- `2026-05-21`：竞价涨幅取保留在 capture 里的 `auction_change_pct`；收盘涨幅取盘后 5 表的 `latest_change_pct`。

## 2026-05-19

- 市场状态：`normal`
- BUY 数量：`3`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 3 | 3 | -0.016 | 1.417 | 1.700 | 3 | 0 |
| WATCH | 8 | 8 | -0.241 | 4.112 | 2.527 | 6 | 2 |
| REJECT | 18 | 18 | 1.922 | -0.191 | -0.671 | 8 | 10 |
| AVOID | 11 | 11 | 9.041 | 0.487 | -0.002 | 5 | 6 |

### BUY 明细

| code | name | auction_pct | close_pct | excess_return | auction_source | close_source | reason |
|---|---|---:|---:|---:|---|---|---|
| 002081 | 金螳螂 | -4.530 | -2.405 | 2.126 | auction.jjyd.net_amount:auction_change_pct | dailyline.stocks | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |
| 002407 | 多氟多 | -2.040 | -1.614 | 0.426 | auction.jjyd.net_amount:auction_change_pct | dailyline.stocks | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |
| 002580 | 圣阳股份 | 2.270 | 3.970 | 1.700 | auction.jjyd.qiangchou:latest_change_pct@0925_fallback | dailyline.stocks | BUY:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM |

## 2026-05-20

- 市场状态：`cold`
- BUY 数量：`1`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 1 | 1 | 3.026 | 7.365 | 7.365 | 1 | 0 |
| WATCH | 7 | 7 | -5.083 | -3.647 | -1.440 | 0 | 7 |
| REJECT | 3 | 3 | 0.165 | 1.658 | -0.001 | 1 | 2 |
| AVOID | 19 | 19 | -2.658 | -0.189 | 0.000 | 7 | 9 |

### BUY 明细

| code | name | auction_pct | close_pct | excess_return | auction_source | close_source | reason |
|---|---|---:|---:|---:|---|---|---|
| 002208 | 合肥城建 | -4.340 | 3.026 | 7.365 | auction.jjyd.net_amount:auction_change_pct | dailyline.stocks | BUY:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM |

## 2026-05-21

- 市场状态：`cold_to_warming`
- BUY 数量：`3`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 3 | 3 | -3.090 | -2.763 | -6.130 | 1 | 2 |
| WATCH | 8 | 8 | -1.660 | -2.917 | -4.040 | 3 | 5 |
| REJECT | 15 | 15 | 0.585 | -2.277 | -2.310 | 2 | 13 |
| AVOID | 18 | 11 | -0.355 | -8.666 | -9.840 | 2 | 8 |

### BUY 明细

| code | name | auction_pct | close_pct | excess_return | auction_source | close_source | reason |
|---|---|---:|---:|---:|---|---|---|
| 600863 | 华能蒙电 | -3.720 | 3.720 | 7.440 | auction.jjyd.net_amount:auction_change_pct | auction.jjyd.net_amount | BUY:MOMENTUM_CATCHUP:CONFIRMED_MOMENTUM |
| 002428 | 云南锗业 | 1.690 | -7.910 | -9.600 | auction.jjyd.vratio:auction_change_pct | auction.jjyd.vratio | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |
| 300657 | 弘信电子 | 1.050 | -5.080 | -6.130 | auction.jjyd.qiangchou:auction_change_pct | auction.jjyd.qiangchou | BUY:LOW_OPEN_REVERSAL:LOW_OPEN_NET_REVERSAL |

## 合并结论（纠正后）

- BUY 平均超额收益：`0.475%`（样本 `7`，中位数 `1.700%`）
- WATCH 平均超额收益：`-0.695%`（样本 `23`，中位数 `-0.403%`）
- REJECT 平均超额收益：`-0.906%`（样本 `36`，中位数 `-1.408%`）
- AVOID 平均超额收益：`-2.282%`（样本 `41`，中位数 `-0.002%`）

## 结论

- `2026-05-19`：BUY 平均超额 `1.417%`（有效样本 `3` / `3`）。
- `2026-05-20`：BUY 平均超额 `7.365%`（有效样本 `1` / `1`）。
- `2026-05-21`：BUY 平均超额 `-2.763%`（有效样本 `3` / `3`）。
- 三天合并后，BUY 平均超额 `0.475%`，WATCH 平均超额 `-0.695%`。
- 之前“5/21 同源所以全是 0”的说法是错的；错误原因是我评估脚本字段取错，不是网页字段问题。

