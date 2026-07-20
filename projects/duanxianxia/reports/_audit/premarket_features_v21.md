# v21 特征矩阵导出 (重构训练集地基)

- 生成: 2026-07-18T21:55:13
- 训练日: 23 ｜样本行: **7663** ｜特征数: 46 (23原始+23截面秩)
- CSV: `reports/_audit/feature_matrix_v21.csv`

## 特征 IC 先验 (每日横截面 Spearman, 对两个标签)

| 特征 | IC(当日超额) | ICIR | IC(次收持仓) | ICIR | n_days |
|---|---|---|---|---|---|
| qiangchou_920_925_rank | 0.1655 | 1.918 | 0.1122 | 0.699 | 7 |
| auction_amount_wan | 0.0717 | 0.38 | 0.063 | 0.333 | 23 |
| auction_strength | 0.071 | 0.502 | 0.0718 | 0.634 | 23 |
| deriv.amt_x_auc | 0.0694 | 0.484 | 0.0718 | 0.59 | 23 |
| liquidity | 0.0622 | 0.446 | 0.061 | 0.476 | 23 |
| pressure_score | 0.0559 | 0.451 | 0.0542 | 0.374 | 23 |
| amt_pct | 0.049 | 0.338 | 0.0449 | 0.32 | 23 |
| deriv.money_x_liq | 0.0425 | 0.374 | 0.0564 | 0.516 | 23 |
| money | 0.0414 | 0.363 | 0.0567 | 0.518 | 23 |
| orderbook | 0.0397 | 0.506 | 0.043 | 0.586 | 22 |
| deriv.auc_minus_8xopen | 0.0232 | 0.101 | -0.0278 | -0.126 | 23 |
| weimai_strength | 0.0186 | 0.187 | 0.0187 | 0.186 | 23 |
| net_pressure | -0.0123 | -0.071 | -0.0039 | -0.022 | 23 |
| latest_change_pct | 0.0123 | 0.056 | 0.0543 | 0.257 | 23 |
| qiangchou_last_second_rank | -0.0118 | -0.064 | -0.0612 | -0.419 | 15 |
| cashflow_continuity_score | 0.0093 | 0.135 | -0.0064 | -0.097 | 23 |
| source_evidence_score | 0.009 | 0.15 | 0.0382 | 0.53 | 23 |
| deriv.lowopen_strength | 0.0075 | 0.054 | -0.0103 | -0.07 | 23 |
| low_cost | -0.0027 | -0.019 | -0.0234 | -0.17 | 23 |
| net_amount_rank | 0.0027 | 0.023 | 0.0016 | 0.014 | 23 |
| theme_strength_t0 | None | None | None | None | 0 |
| market_env_score | None | None | None | None | 0 |
| longtou_score | None | None | None | None | 0 |

## 关键特征覆盖率(非空占比)

- `y_excess`: 1.0
- `y_hold`: 0.882
- `y_t1close`: 0.882
- `amt_pct`: 1.0
- `auction_strength`: 1.0
- `liquidity`: 1.0
- `money`: 1.0
- `pressure_score`: 1.0
- `weimai_strength`: 1.0
- `orderbook`: 1.0
- `low_cost`: 1.0
- `theme_strength_t0`: 1.0
- `market_env_score`: 1.0
- `cashflow_continuity_score`: 1.0
- `longtou_score`: 1.0
- `net_pressure`: 0.274
- `latest_change_pct`: 0.989
- `source_evidence_score`: 1.0
- `auction_amount_wan`: 0.632
- `net_amount_rank`: 0.274
- `qiangchou_920_925_rank`: 0.056
- `qiangchou_last_second_rank`: 0.096
- `deriv.auc_minus_8xopen`: 0.989
- `deriv.lowopen_strength`: 0.989
- `deriv.money_x_liq`: 1.0
- `deriv.amt_x_auc`: 1.0

> 用途: v22 直接读此 CSV 训练 torch 截面排序模型; IC先验用于特征筛选/正则先验。
> 注: 样本仅 ~13 日, 模型须强正则 + walk-forward; 数据随每交易日自动累积。