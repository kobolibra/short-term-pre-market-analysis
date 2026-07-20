# 盘前特征审计 v35 — next-level 地基 (job 0044)

- 生成: 2026-06-27T08:00:27
- 有效交易日: **15** ｜样本: 4603
- 口径: excess=(close-open)/preclose*100; 不改生产逻辑, 纯描述性

## 1. 原始/新因子横截面 IC (按 |IC| 排序)

| 因子 | mean_ic | icir | 覆盖率 | n_days |
|---|---|---|---|---|
| qiangchou_920_925_rank | 0.2657 | None | 0.01 | 1 |
| auction_amount_wan | 0.0957 | 0.483 | 0.525 | 15 |
| seal_to_mcap_ratio | 0.0506 | 0.116 | 0.03 | 10 |
| big_order_share | 0.0409 | 0.26 | 0.27 | 15 |
| net_inflow_pct | 0.0387 | 0.333 | 0.464 | 15 |
| wm_net_pressure | -0.0191 | -0.183 | 0.46 | 14 |
| qiangchou_last_second_rank | -0.0143 | -0.072 | 0.127 | 13 |
| turnover_intensity | -0.0132 | -0.072 | 0.143 | 15 |

## 2. 条件 IC: 成交额×开盘位置 (池化 spearman, 描述性)

### auction_amount_wan

| 开盘桶 | pooled_spearman | n |
|---|---|---|
| 1_low_open(<0) | 0.4 | 656 |
| 2_flat_open(0-3) | 0.1393 | 1128 |
| 3_high_open(3-7) | 0.1032 | 445 |
| 4_veryhigh_open(>=7) | -0.1197 | 167 |

### turnover_intensity (换手强度)

| 开盘桶 | pooled_spearman | n |
|---|---|---|
| 1_low_open(<0) | -0.0562 | 217 |
| 2_flat_open(0-3) | 0.1078 | 208 |
| 3_high_open(3-7) | -0.0436 | 136 |
| 4_veryhigh_open(>=7) | -0.0373 | 98 |

## 结论

- 最强因子: **qiangchou_920_925_rank** (IC 0.2657, 覆盖 0.01)
- 换手强度是否优于绝对成交额: **False** (turnover IC -0.0132 vs amt IC 0.0957)
- 有信号的被埋比率: []
- amt 是否随开盘位置变号: 见上表(低开桶 vs 高开桶 符号/强度差异)

> 谨慎: 15 天小样本; 条件 IC 为池化(未逐日去均值), 仅作方向探针。几何意义均需后续 walk-forward 复验。