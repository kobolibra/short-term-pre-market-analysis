# 盘前选股 v29 混合公式实验 (加入高ICIR交互项)

- 生成: 2026-06-26T15:50:50
- 有效交易日: **15** ｜样本: 4603 ｜OOS天数: 10 ｜Top-N: 30

## 出样本对比 (walk-forward, 主口径 excess)

| 策略 | mean_ic | icir | capture@30 | Top5均值 | Top5胜率 | Top3均值 | Top3胜率 |
|---|---|---|---|---|---|---|---|
| v10_amt_raw | 0.1263 | 0.824 | 0.18 | 0.683 | 0.5 | 1.246 | 0.6 |
| v10_amt_pctl | 0.0886 | 0.77 | 0.137 | 1.246 | 0.48 | 1.133 | 0.367 |
| sparse_ic | 0.0829 | 0.79 | 0.153 | 1.15 | 0.46 | 0.357 | 0.333 |
| ext_fixed | 0.0848 | 0.773 | 0.143 | 1.105 | 0.46 | 0.85 | 0.333 |
| ext_learned | 0.082 | 0.718 | 0.14 | 1.072 | 0.44 | 0.722 | 0.367 |

## 结论

- OOS IC 最优: **v10_amt_raw**
- Top5 实际超额最优: **v10_amt_pctl**
- Top3 实际超额最优: **v10_amt_raw**
- ext_learned 在 IC 上超 v10_amt: **False**｜Top5: **True**
- ext_fixed 在 IC 上超 v10_amt: **False**｜Top5: **True**

## ext_learned 全样本权重 (若推荐上线 v11)

- `amt_pct`: 0.1195
- `auction_strength`: 0.1423
- `liquidity`: 0.1369
- `money`: 0.0935
- `pressure_score`: 0.1305
- `weimai_strength`: 0.0799
- `orderbook`: 0.0494
- `deriv.amt_x_auc`: 0.1502
- `deriv.money_x_liq`: 0.0978

> 注: 复合均为逐日横截面百分位加权; v10_amt_raw 为现行生产口径(原始分-风险)。
> 仅当 ext_* 在 OOS IC 与 Top5/Top3 实际超额上同时不劣于 v10_amt 时, 才推荐上线 v11。