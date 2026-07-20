# 去相关多源组合 v44

- 生成: 2026-06-27T09:40:16

## 单因子

| 因子 | mean_ic | icir | n_days |
|---|---|---|---|
| A_turnover | 0.1629 | 0.665 | 16 |
| B_turnrate | 0.1349 | 0.731 | 16 |
| C_gap | 0.1295 | 0.357 | 16 |
| D_fp_amount | 0.1034 | 0.523 | 17 |
| E_fp_turnrate | 0.0868 | 0.649 | 17 |
| F_ztpool_promo | -0.0696 | -0.614 | 17 |

## 组合(vs 单因子 0.163)

| 组合 | mean_ic | icir | n_days | 成员 |
|---|---|---|---|---|
| comp_SD | 0.179 | 0.926 | 16 | A_turnover,B_turnrate,C_gap |
| comp_SD_FP | 0.1448 | 0.877 | 22 | A_turnover,B_turnrate,C_gap,D_fp_amount,E_fp_turnrate |
| comp_ALL | 0.1268 | 0.829 | 22 | A_turnover,B_turnrate,C_gap,D_fp_amount,E_fp_turnrate,F_ztpool_promo |
| comp_AB_D | 0.1176 | 0.716 | 22 | A_turnover,B_turnrate,D_fp_amount |
| comp_A_D | 0.1013 | 0.58 | 22 | A_turnover,D_fp_amount |

## 交叉相关

| 因子对 | avg_spearman | n_days |
|---|---|---|
| A_turnover ~ D_fp_amount | 0.75 | 2 |
| B_turnrate ~ E_fp_turnrate | 0.618 | 2 |
| A_turnover ~ B_turnrate | 0.608 | 16 |
| D_fp_amount ~ E_fp_turnrate | 0.535 | 17 |
| C_gap ~ E_fp_turnrate | -0.259 | 2 |
| B_turnrate ~ C_gap | 0.21 | 16 |
| B_turnrate ~ D_fp_amount | 0.194 | 2 |
| A_turnover ~ F_ztpool_promo | 0.144 | 6 |
| C_gap ~ F_ztpool_promo | 0.12 | 6 |
| A_turnover ~ C_gap | 0.114 | 16 |
| D_fp_amount ~ F_ztpool_promo | -0.107 | 17 |
| E_fp_turnrate ~ F_ztpool_promo | -0.106 | 17 |
| C_gap ~ D_fp_amount | 0.079 | 2 |
| A_turnover ~ E_fp_turnrate | 0.059 | 2 |
| B_turnrate ~ F_ztpool_promo | 0.0 | 6 |