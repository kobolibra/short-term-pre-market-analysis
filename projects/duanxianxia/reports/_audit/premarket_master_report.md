# 盘前选股优化主报告

- 生成: 2026-07-20T16:02:10
- 有效交易日: **23** ｜样本: 7705 ｜补生成: None ｜失败: None
- 出样本天数(walk-forward): 18

## walk-forward 出样本表现(主口径 excess_ret)

| 模型 | mean_ic | icir | capture@30 |
|---|---|---|---|
| 学到的权重 | 0.0832 | 0.553 | 0.156 |
| v10_amt(固定) | 0.0899 | 0.595 | 0.163 |
| 现行 edge(stored) | 0.0843 | 0.584 | 0.157 |
| final(stored) | 0.0759 | 0.528 | - |

## 推荐生产权重(全样本 IC 归一化)

- `amt_pct`: 0.1439
- `auction_strength`: 0.2084
- `liquidity`: 0.1854
- `money`: 0.1242
- `pressure_score`: 0.1675
- `weimai_strength`: 0.0583
- `orderbook`: 0.1124

## 赢家倒推 Top-30(原始字段,按|mean_sep|)

| 字段 | mean_sep | 正向天数/总 | solo_hit |
|---|---|---|---|
| qiangchou_920_925_rank | 8.52 | 2/3 | 0.127 |
| net_pressure | 7.03 | 13/19 | 0.161 |
| liquidity | 6.52 | 19/23 | 0.154 |
| theme_strength_t0 | -6.14 | 3/23 | 0.157 |
| market_env_score | -6.14 | 3/23 | 0.157 |
| longtou_score | -6.14 | 3/23 | 0.157 |
| money | 5.97 | 21/23 | 0.152 |
| qiangchou_last_second_rank | 5.52 | 4/5 | 0.093 |
| auction_strength | 5.51 | 19/23 | 0.158 |
| auction_amount_wan | 4.45 | 16/23 | 0.114 |
| orderbook | -3.93 | 4/23 | 0.148 |
| amt_pct | 3.67 | 17/23 | 0.114 |
| low_cost | -3.01 | 8/23 | 0.086 |
| cashflow_continuity_score | -2.37 | 9/23 | 0.119 |
| pressure_score | 1.78 | 14/23 | 0.161 |
| weimai_strength | -1.6 | 13/23 | 0.109 |
| source_evidence_score | -1.46 | 8/23 | 0.091 |
| net_amount_rank | -0.27 | 8/19 | 0.11 |
| latest_change_pct | 0.23 | 10/23 | 0.117 |

## 赢家倒推(衰生指标)

| 指标 | mean_sep | 正向天数/总 | solo_hit |
|---|---|---|---|
| deriv.money_x_liq | 6.19 | 21/23 | 0.164 |
| deriv.amt_x_auc | 5.08 | 18/23 | 0.136 |
| deriv.auc_minus_8xopen | 2.96 | 16/23 | 0.138 |
| deriv.lowopen_strength | -0.44 | 10/23 | 0.132 |

## 逐字段 IC(excess_ret)

| 字段 | mean_ic | icir | n_days |
|---|---|---|---|
| qiangchou_920_925_rank | 0.1584 | 1.599 | 7 |
| auction_amount_wan | 0.0721 | 0.381 | 23 |
| auction_strength | 0.0708 | 0.501 | 23 |
| deriv.amt_x_auc | 0.0695 | 0.484 | 23 |
| liquidity | 0.063 | 0.452 | 23 |
| pressure_score | 0.0569 | 0.459 | 23 |
| amt_pct | 0.0489 | 0.335 | 23 |
| deriv.money_x_liq | 0.0433 | 0.379 | 23 |
| money | 0.0422 | 0.368 | 23 |
| orderbook | 0.0382 | 0.5 | 22 |
| deriv.auc_minus_8xopen | 0.0235 | 0.102 | 23 |
| weimai_strength | 0.0198 | 0.198 | 23 |
| net_pressure | -0.0127 | -0.073 | 23 |
| qiangchou_last_second_rank | -0.0127 | -0.068 | 15 |
| latest_change_pct | 0.0124 | 0.057 | 23 |
| cashflow_continuity_score | 0.0098 | 0.143 | 23 |
| source_evidence_score | 0.0084 | 0.141 | 23 |
| deriv.lowopen_strength | 0.008 | 0.057 | 23 |
| low_cost | -0.004 | -0.028 | 23 |
| net_amount_rank | 0.0022 | 0.019 | 23 |