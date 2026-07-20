# 低开反包 / 风险位 alpha 反推

- 生成: 2026-07-20T16:05:58 ｜交易日: 23 ｜低开阈值: latest_change_pct<2.0

## cohort excess 对比

| cohort | n | mean | median | win_rate | p90 | 涨停率 |
|---|---|---|---|---|---|---|
| 低开 | 4871 | -0.154 | -0.485 | 0.455 | 6.84 | 0.039 |
| 高开 | 2834 | 0.306 | 0.0 | 0.489 | 6.82 | 0.04 |
| risk_flag=True | 6441 | -0.155 | -0.27 | 0.449 | 6.56 | 0.033 |
| risk_flag=False | 1264 | 0.882 | 1.157 | 0.56 | 8.0 | 0.07 |

- 每日 Top-30 赢家中低开票占比(均值): **0.613**
- 每日 Top-30 赢家中风险位票占比(均值): **0.799**
- 低开内: 现行 edge capture@30 = **0.196** ; 新选择器(字段=['net_pressure', 'qiangchou_920_925_rank', 'auction_strength']) capture@30 = **0.191**

## 低开 cohort 内字段 IC

| 字段 | mean_ic | icir | n_days |
|---|---|---|---|
| qiangchou_920_925_rank | 0.1542 | 0.925 | 5 |
| net_pressure | -0.0543 | -0.302 | 23 |
| auction_strength | 0.0439 | 0.312 | 23 |
| deriv.amt_x_auc | 0.04 | 0.283 | 23 |
| liquidity | 0.0395 | 0.259 | 23 |
| orderbook | 0.0362 | 0.431 | 22 |
| pressure_score | 0.0356 | 0.248 | 23 |
| net_amount_rank | 0.03 | 0.255 | 23 |
| qiangchou_last_second_rank | -0.0294 | -0.199 | 14 |
| amt_pct | 0.0239 | 0.157 | 23 |
| source_evidence_score | 0.0226 | 0.254 | 23 |
| auction_amount_wan | 0.0223 | 0.106 | 23 |
| deriv.money_x_liq | 0.0208 | 0.157 | 23 |
| money | 0.0195 | 0.149 | 23 |
| weimai_strength | 0.0099 | 0.069 | 23 |
| low_cost | 0.0088 | 0.04 | 23 |
| cashflow_continuity_score | 0.0074 | 0.098 | 23 |

## 低开 cohort 赢家倒推(字段百分位 - 50)

| 字段 | mean_sep | 正向天数/总 | hit_rate |
|---|---|---|---|
| net_pressure | 8.5 | 18/23 | 0.198 |
| qiangchou_920_925_rank | 5.3 | 2/2 | 0.093 |
| auction_strength | 3.53 | 17/23 | 0.192 |
| deriv.amt_x_auc | 3.49 | 19/23 | 0.182 |
| liquidity | 3.46 | 17/23 | 0.198 |
| theme_strength_t0 | -3.3 | 6/23 | 0.195 |
| market_env_score | -3.3 | 6/23 | 0.195 |
| longtou_score | -3.3 | 6/23 | 0.195 |
| auction_amount_wan | 3.17 | 16/23 | 0.171 |
| orderbook | -2.66 | 7/23 | 0.195 |
| amt_pct | 2.5 | 13/23 | 0.169 |
| deriv.money_x_liq | 2.07 | 15/23 | 0.197 |
| pressure_score | 1.89 | 14/23 | 0.203 |
| money | 1.87 | 15/23 | 0.204 |
| low_cost | -1.7 | 9/23 | 0.138 |
| source_evidence_score | -0.91 | 8/23 | 0.159 |
| qiangchou_last_second_rank | -0.87 | 5/8 | 0.082 |
| weimai_strength | 0.77 | 11/23 | 0.163 |
| net_amount_rank | 0.62 | 14/23 | 0.155 |
| cashflow_continuity_score | -0.11 | 10/23 | 0.178 |