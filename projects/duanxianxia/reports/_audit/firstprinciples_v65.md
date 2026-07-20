# 第一性原理四问验证 v65

- 生成: 2026-06-28T13:10:12
- days=15 qx_med=29.0

## Q1 换手率 vs 成交额

| 指标 | ic | icir | n |
|---|---|---|---|
| ic_amount | 0.1341 | 0.596 | 15 |
| ic_turnrate | 0.1216 | 0.664 | 15 |
| corr_amount_turnrate | 0.6274 | 5.332 | 15 |
| ic_turnrate_resid_after_amount | 0.0583 | 0.302 | 15 |
| ic_amount_resid_after_turnrate | 0.0414 | 0.167 | 15 |
| ic_composite_amt_plus_turn | 0.1364 | 0.764 | 15 |

## Q2 gap 非线性 (bin low->high)

| bin | pooled_dm | perday | icir | n |
|---|---|---|---|---|
| 0 | -0.5792 | -1.1008 | -0.385 | 138 |
| 1 | -0.5305 | -0.637 | -0.342 | 131 |
| 2 | -0.2916 | -0.2065 | -0.137 | 135 |
| 3 | -0.0071 | 0.2143 | 0.103 | 130 |
| 4 | 1.3649 | 1.427 | 0.454 | 139 |

ic_gap all/hot/cold = 0.1683 / 0.0438 / 0.2726
monotonic=2.5278 hump=-0.3696

## Q3 情绪 regime

core_ic hot/cold = 0.1323 / 0.2568
top5_raw hot/cold = 0.43 / 3.2477
top5_demeaned hot/cold = 1.1299 / 2.2715
breadth hot/cold = -0.6999 / 0.9762
corr(QX,top5raw)=-0.15244185832977605 corr(QX,coreIC)=-0.024390697332764166

## Q4 小盘稳健性

ic_cap raw/winsor = 0.0445 / 0.0444
small tercile raw/winsor/ex_outlier = 0.131 / 0.1246 / 0.2615
abs<100亿 raw/winsor/ex_outlier = 0.1097 / 0.1145 / 0.1769
outlier days = ['2026-06-17', '2026-06-18']