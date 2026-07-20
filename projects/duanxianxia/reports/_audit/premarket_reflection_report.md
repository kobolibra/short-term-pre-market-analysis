# 盘前选股反思迭代主报告

- 生成: 2026-07-20T16:03:53
- 有效交易日: **23** ｜样本: 7705 ｜Top-N: 30
- 覆盖日期: 2026-05-21, 2026-06-01, 2026-06-02, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-15, 2026-06-16, 2026-06-18, 2026-06-23, 2026-06-24, 2026-06-26, 2026-06-29, 2026-06-30, 2026-07-01, 2026-07-02, 2026-07-03, 2026-07-08, 2026-07-09, 2026-07-10

## 1. 选出来的票当日表现 (按 action)

| action | n | mean_excess | median | win_rate | 跌停率 |
|---|---|---|---|---|---|
| BUY | 27 | 4.442 | 3.715 | 0.667 | 0.074 |
| DROP | 5698 | -0.179 | -0.257 | 0.459 | 0.027 |
| UNKNOWN | 362 | -0.355 | -0.596 | 0.436 | 0.03 |
| WATCH | 1618 | 0.706 | 0.0 | 0.499 | 0.036 |

### 历次 BUY 明细

| 日期 | 代码 | excess | edge | regime | risk |
|---|---|---|---|---|---|
| 2026-05-21 | 000100 | -4.49 | 86.72 | {'label': 'cold', 'reason': 'qx=27.0, dt=4.0, kqxy=0.0, breadth=0.21292460646230324', 'qx_t0': 27.0, 'qx_t1': 32.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 1028.0, 'xd_t0': 3800.0, 'breadth_t0': 0.21292460646230324, 'lbbx_t0': 3.41, 'lbbx_t1': 4.08, 'ztbx_t0': 1.65, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-06-02 | 000636 | 11.68 | 61.07 | {'label': 'cold', 'reason': 'qx=29.0, dt=4.0, kqxy=0.0, breadth=0.4950917626973965', 'qx_t0': 29.0, 'qx_t1': 29.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 2320.0, 'xd_t0': 2366.0, 'breadth_t0': 0.4950917626973965, 'lbbx_t0': 2.17, 'lbbx_t1': 1.55, 'ztbx_t0': 1.88, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-02 | 002552 | 5.18 | 54.04 | {'label': 'cold', 'reason': 'qx=29.0, dt=4.0, kqxy=0.0, breadth=0.4950917626973965', 'qx_t0': 29.0, 'qx_t1': 29.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 2320.0, 'xd_t0': 2366.0, 'breadth_t0': 0.4950917626973965, 'lbbx_t0': 2.17, 'lbbx_t1': 1.55, 'ztbx_t0': 1.88, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-02 | 300913 | 3.72 | 53.45 | {'label': 'cold', 'reason': 'qx=29.0, dt=4.0, kqxy=0.0, breadth=0.4950917626973965', 'qx_t0': 29.0, 'qx_t1': 29.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 2320.0, 'xd_t0': 2366.0, 'breadth_t0': 0.4950917626973965, 'lbbx_t0': 2.17, 'lbbx_t1': 1.55, 'ztbx_t0': 1.88, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-09 | 300975 | 3.7 | 59.65 | {'label': 'cold_to_warming', 'reason': 'qx 7.0→42.0', 'qx_t0': 42.0, 'qx_t1': 7.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 4143.0, 'xd_t0': 858.0, 'breadth_t0': 0.8284343131373725, 'lbbx_t0': 1.33, 'lbbx_t1': 1.67, 'ztbx_t0': 2.16, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-09 | 002969 | 6.29 | 51.11 | {'label': 'cold_to_warming', 'reason': 'qx 7.0→42.0', 'qx_t0': 42.0, 'qx_t1': 7.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 4143.0, 'xd_t0': 858.0, 'breadth_t0': 0.8284343131373725, 'lbbx_t0': 1.33, 'lbbx_t1': 1.67, 'ztbx_t0': 2.16, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-09 | 000725 | 0.48 | 50.87 | {'label': 'cold_to_warming', 'reason': 'qx 7.0→42.0', 'qx_t0': 42.0, 'qx_t1': 7.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 4143.0, 'xd_t0': 858.0, 'breadth_t0': 0.8284343131373725, 'lbbx_t0': 1.33, 'lbbx_t1': 1.67, 'ztbx_t0': 2.16, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-10 | 002354 | 7.32 | 61.79 | {'label': 'cold', 'reason': 'qx=19.0, dt=0.0, kqxy=0.0, breadth=0.13453973249409915', 'qx_t0': 19.0, 'qx_t1': 42.0, 'dt_t0': 0.0, 'kqxy_t0': 0.0, 'sz_t0': 684.0, 'xd_t0': 4400.0, 'breadth_t0': 0.13453973249409915, 'lbbx_t0': 2.32, 'lbbx_t1': 1.33, 'ztbx_t0': 1.09, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-06-11 | 002636 | 9.51 | 60.57 | {'label': 'cold', 'reason': 'qx=21.0, dt=1.0, kqxy=0.0, breadth=0.16581632653061223', 'qx_t0': 21.0, 'qx_t1': 19.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 845.0, 'xd_t0': 4251.0, 'breadth_t0': 0.16581632653061223, 'lbbx_t0': 2.34, 'lbbx_t1': 2.32, 'ztbx_t0': 0.94, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-06-15 | 002842 | 4.41 | 56.12 | {'label': 'cold_to_warming', 'reason': 'qx 21.0→37.0', 'qx_t0': 37.0, 'qx_t1': 21.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 3625.0, 'xd_t0': 1244.0, 'breadth_t0': 0.7445060587389608, 'lbbx_t0': 1.18, 'lbbx_t1': 2.34, 'ztbx_t0': 1.11, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-15 | 002119 | 5.48 | 53.98 | {'label': 'cold_to_warming', 'reason': 'qx 21.0→37.0', 'qx_t0': 37.0, 'qx_t1': 21.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 3625.0, 'xd_t0': 1244.0, 'breadth_t0': 0.7445060587389608, 'lbbx_t0': 1.18, 'lbbx_t1': 2.34, 'ztbx_t0': 1.11, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-15 | 300706 | 5.16 | 52.73 | {'label': 'cold_to_warming', 'reason': 'qx 21.0→37.0', 'qx_t0': 37.0, 'qx_t1': 21.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 3625.0, 'xd_t0': 1244.0, 'breadth_t0': 0.7445060587389608, 'lbbx_t0': 1.18, 'lbbx_t1': 2.34, 'ztbx_t0': 1.11, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-16 | 002051 | 7.66 | 55.57 | {'label': 'cold_to_warming', 'reason': 'qx 21.0→37.0', 'qx_t0': 37.0, 'qx_t1': 21.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 3625.0, 'xd_t0': 1244.0, 'breadth_t0': 0.7445060587389608, 'lbbx_t0': 1.18, 'lbbx_t1': 2.34, 'ztbx_t0': 1.11, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-23 | 000823 | -13.89 | 81.22 | {'label': 'cold', 'reason': 'qx=32.0, dt=4.0, kqxy=0.0, breadth=0.24561403508771928', 'qx_t0': 32.0, 'qx_t1': 20.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 1218.0, 'xd_t0': 3741.0, 'breadth_t0': 0.24561403508771928, 'lbbx_t0': 4.0, 'lbbx_t1': 1.55, 'ztbx_t0': 2.94, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-06-24 | 002167 | 9.53 | 82.38 | {'label': 'cold', 'reason': 'qx=26.0, dt=2.0, kqxy=0.0, breadth=0.33802225488137727', 'qx_t0': 26.0, 'qx_t1': 32.0, 'dt_t0': 2.0, 'kqxy_t0': 0.0, 'sz_t0': 1610.0, 'xd_t0': 3153.0, 'breadth_t0': 0.33802225488137727, 'lbbx_t0': 0.25, 'lbbx_t1': 4.0, 'ztbx_t0': 0.45, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-26 | 002141 | 8.59 | 76.49 | {'label': 'cold', 'reason': 'qx=29.0, dt=0.0, kqxy=0.0, breadth=0.2550091074681239', 'qx_t0': 29.0, 'qx_t1': 26.0, 'dt_t0': 0.0, 'kqxy_t0': 0.0, 'sz_t0': 1260.0, 'xd_t0': 3681.0, 'breadth_t0': 0.2550091074681239, 'lbbx_t0': 2.84, 'lbbx_t1': 0.25, 'ztbx_t0': 1.27, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-06-29 | 002579 | -8.21 | 77.56 | {'label': 'cold', 'reason': 'qx=19.0, dt=1.0, kqxy=0.0, breadth=0.3686655405405405', 'qx_t0': 19.0, 'qx_t1': 29.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 1746.0, 'xd_t0': 2990.0, 'breadth_t0': 0.3686655405405405, 'lbbx_t0': 2.46, 'lbbx_t1': 2.84, 'ztbx_t0': 2.01, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-06-30 | 002354 | 0.55 | 83.68 | {'label': 'cold', 'reason': 'qx=18.0, dt=4.0, kqxy=0.0, breadth=0.35742887249736566', 'qx_t0': 18.0, 'qx_t1': 19.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 1696.0, 'xd_t0': 3049.0, 'breadth_t0': 0.35742887249736566, 'lbbx_t0': 1.62, 'lbbx_t1': 2.46, 'ztbx_t0': 1.91, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-07-01 | 002674 | -3.51 | 100.0 | {'label': 'cold', 'reason': 'qx=18.0, dt=4.0, kqxy=0.0, breadth=0.35742887249736566', 'qx_t0': 18.0, 'qx_t1': 19.0, 'dt_t0': 4.0, 'kqxy_t0': 0.0, 'sz_t0': 1696.0, 'xd_t0': 3049.0, 'breadth_t0': 0.35742887249736566, 'lbbx_t0': 1.62, 'lbbx_t1': 2.46, 'ztbx_t0': 1.91, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-07-02 | 001248 | 23.24 | 100.0 | {'label': 'cold', 'reason': 'qx=24.0, dt=2.0, kqxy=0.0, breadth=0.29661188882126194', 'qx_t0': 24.0, 'qx_t1': None, 'dt_t0': 2.0, 'kqxy_t0': 0.0, 'sz_t0': 1462.0, 'xd_t0': 3467.0, 'breadth_t0': 0.29661188882126194, 'lbbx_t0': 1.23, 'lbbx_t1': None, 'ztbx_t0': 1.38, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-07-03 | 002979 | 0.0 | 100.0 | {'label': 'cold', 'reason': 'qx=24.0, dt=2.0, kqxy=0.0, breadth=0.29661188882126194', 'qx_t0': 24.0, 'qx_t1': None, 'dt_t0': 2.0, 'kqxy_t0': 0.0, 'sz_t0': 1462.0, 'xd_t0': 3467.0, 'breadth_t0': 0.29661188882126194, 'lbbx_t0': 1.23, 'lbbx_t1': None, 'ztbx_t0': 1.38, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-07-08 | 000977 | 0.0 | 100.0 | {'label': 'normal', 'reason': 'qx=36.0, lbbx=1.86, ztbx=1.96, breadth=0.4708539470853947', 'qx_t0': 36.0, 'qx_t1': None, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 2189.0, 'xd_t0': 2460.0, 'breadth_t0': 0.4708539470853947, 'lbbx_t0': 1.86, 'lbbx_t1': None, 'ztbx_t0': 1.96, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-07-08 | 603001 | -6.47 | 100.0 | {'label': 'normal', 'reason': 'qx=36.0, lbbx=1.86, ztbx=1.96, breadth=0.4708539470853947', 'qx_t0': 36.0, 'qx_t1': None, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 2189.0, 'xd_t0': 2460.0, 'breadth_t0': 0.4708539470853947, 'lbbx_t0': 1.86, 'lbbx_t1': None, 'ztbx_t0': 1.96, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-07-08 | 002185 | -6.38 | 93.37 | {'label': 'normal', 'reason': 'qx=36.0, lbbx=1.86, ztbx=1.96, breadth=0.4708539470853947', 'qx_t0': 36.0, 'qx_t1': None, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 2189.0, 'xd_t0': 2460.0, 'breadth_t0': 0.4708539470853947, 'lbbx_t0': 1.86, 'lbbx_t1': None, 'ztbx_t0': 1.96, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | False |
| 2026-07-08 | 301251 | 3.33 | 92.21 | {'label': 'normal', 'reason': 'qx=36.0, lbbx=1.86, ztbx=1.96, breadth=0.4708539470853947', 'qx_t0': 36.0, 'qx_t1': None, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 2189.0, 'xd_t0': 2460.0, 'breadth_t0': 0.4708539470853947, 'lbbx_t0': 1.86, 'lbbx_t1': None, 'ztbx_t0': 1.96, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-07-09 | 000524 | -11.78 | 100.0 | {'label': 'cold', 'reason': 'qx=20.0, dt=3.0, kqxy=0.0, breadth=0.4084448160535117', 'qx_t0': 20.0, 'qx_t1': 36.0, 'dt_t0': 3.0, 'kqxy_t0': 0.0, 'sz_t0': 1954.0, 'xd_t0': 2830.0, 'breadth_t0': 0.4084448160535117, 'lbbx_t0': 0.35, 'lbbx_t1': 1.86, 'ztbx_t0': 0.68, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |
| 2026-07-10 | 301583 | 58.85 | 100.0 | {'label': 'cold', 'reason': 'qx=24.0, dt=1.0, kqxy=0.0, breadth=0.37433269271834296', 'qx_t0': 24.0, 'qx_t1': 20.0, 'dt_t0': 1.0, 'kqxy_t0': 0.0, 'sz_t0': 1753.0, 'xd_t0': 2930.0, 'breadth_t0': 0.37433269271834296, 'lbbx_t0': 2.42, 'lbbx_t1': 0.35, 'ztbx_t0': 3.29, 'promo_t0': None, 'ignored_metrics': ['HSLN', 'PB', 'PBBX']} | True |

## 2. edge_score 分层有效性 (每日排名十分位, 0=最高 edge)

| 十分位(0=最高) | n | mean_excess | win_rate |
|---|---|---|---|
| 0 | 782 | 1.264 | 0.508 |
| 1 | 769 | 0.102 | 0.488 |
| 2 | 771 | 0.283 | 0.484 |
| 3 | 769 | 0.009 | 0.481 |
| 4 | 768 | -0.234 | 0.456 |
| 5 | 774 | -0.393 | 0.45 |
| 6 | 771 | -0.094 | 0.47 |
| 7 | 769 | -0.143 | 0.449 |
| 8 | 771 | -0.19 | 0.437 |
| 9 | 761 | -0.477 | 0.452 |

## 3. 赢家捕获 (每日 Top-30 真实赢家)

- edge 排名 Top-30 捕获率: **0.157** (模型*排序*能力)
- 实际 BUY 捕获率: **0.01** (模型*最终选股*能力)
- 赢家 action 分布: {'WATCH': 196, 'DROP': 457, 'UNKNOWN': 30, 'BUY': 7}
- 门控问题(排进 Top-30 却没买): **101** 例
- 排名问题(模型根本没排上): **582** 例

### 最大遗漏赢家 Top-40 (为什么没选出来)

| 日期 | 代码 | excess | action | edge | edge_rank | risk | 抢筹信号 | setup |
|---|---|---|---|---|---|---|---|---|
| 2026-06-24 | 688797 | 307.45 | WATCH | 60.34 | 28 | ! |  | GENERAL_WATCH |
| 2026-06-26 | 001399 | 102.77 | WATCH | 60.07 | 27 | ! |  | GENERAL_WATCH |
| 2026-07-10 | 301117 | 26.2 | WATCH | 100.0 | 2 |  |  | LOW_OPEN_WEAK |
| 2026-06-08 | 301319 | 24.85 | DROP | 3.22 | 206 | ! |  | LOW_OPEN_WEAK |
| 2026-06-15 | 688432 | 21.48 | DROP | 7.19 | 221 | ! |  | LOW_OPEN_WEAK |
| 2026-06-08 | 301313 | 20.24 | DROP | 12.37 | 99 | ! |  | LOW_OPEN_WEAK |
| 2026-06-30 | 603823 | 20.0 | WATCH | 69.97 | 10 |  |  | LOW_OPEN_WEAK |
| 2026-06-29 | 300706 | 19.54 | DROP | 8.74 | 190 | ! |  | LOW_OPEN_WEAK |
| 2026-06-05 | 688056 | 18.66 | DROP | 33.45 | 62 | ! |  | GENERAL_WATCH |
| 2026-07-03 | 300580 | 18.54 | DROP | 0.0 | 370 | ! | 9:20-9:25 | GENERAL_WATCH |
| 2026-06-11 | 300264 | 18.52 | DROP | 9.53 | 214 | ! |  | LOW_OPEN_WEAK |
| 2026-06-30 | 301379 | 18.44 | DROP | 4.36 | 254 | ! |  | GENERAL_WATCH |
| 2026-06-11 | 300263 | 18.36 | DROP | 29.09 | 75 | ! | last_second | GENERAL_WATCH |
| 2026-06-16 | 301055 | 18.31 | DROP | 9.93 | 259 | ! |  | GENERAL_WATCH |
| 2026-07-09 | 300821 | 18.27 | WATCH | 66.06 | 22 |  |  | LOW_OPEN_WEAK |
| 2026-06-24 | 301366 | 18.22 | DROP | 10.16 | 116 | ! |  | GENERAL_WATCH |
| 2026-06-29 | 301307 | 18.07 | DROP | 1.26 | 308 | ! |  | GENERAL_WATCH |
| 2026-06-18 | 688333 | 18.01 | DROP | 8.68 | 242 | ! |  | GENERAL_WATCH |
| 2026-06-30 | 300540 | 17.83 | WATCH | 40.77 | 66 |  | last_second | GENERAL_WATCH |
| 2026-07-01 | 300287 | 17.76 | DROP | 1.43 | 329 | ! |  | GENERAL_WATCH |
| 2026-06-09 | 301013 | 17.75 | DROP | 13.28 | 131 | ! |  | GENERAL_WATCH |
| 2026-06-18 | 688485 | 17.71 | DROP | 9.73 | 221 | ! | last_second | GENERAL_WATCH |
| 2026-06-18 | 688729 | 17.71 | DROP | 28.46 | 93 | ! |  | GENERAL_WATCH |
| 2026-06-24 | 301526 | 17.57 | DROP | 9.01 | 150 | ! |  | GENERAL_WATCH |
| 2026-05-21 | 600156 | 17.55 | WATCH | 60.3 | 34 | ! |  | GENERAL_WATCH |
| 2026-06-30 | 301165 | 17.53 | WATCH | 35.59 | 72 |  |  | GENERAL_WATCH |
| 2026-07-09 | 688432 | 17.5 | DROP | 19.92 | 184 | ! | 9:20-9:25 | GENERAL_WATCH |
| 2026-06-30 | 300975 | 17.42 | DROP | 7.7 | 179 | ! |  | LOW_OPEN_WEAK |
| 2026-06-08 | 688322 | 17.37 | DROP | 34.43 | 45 | ! |  | LOW_OPEN_WEAK |
| 2026-06-02 | 301013 | 17.34 | DROP | 8.77 | 256 | ! |  | LOW_OPEN_WEAK |
| 2026-06-29 | 301328 | 17.18 | DROP | 0.0 | 334 | ! |  | GENERAL_WATCH |
| 2026-06-30 | 688728 | 17.0 | WATCH | 52.68 | 43 |  |  | GENERAL_WATCH |
| 2026-07-02 | 603928 | 16.81 | WATCH | 74.33 | 8 | ! |  | LOW_OPEN_WEAK |
| 2026-06-11 | 300505 | 16.8 | DROP | 14.24 | 98 | ! |  | GENERAL_WATCH |
| 2026-07-02 | 688068 | 16.67 | DROP | 19.04 | 144 | ! |  | GENERAL_WATCH |
| 2026-06-02 | 300427 | 16.6 | DROP | 2.57 | 297 | ! |  | LOW_OPEN_WEAK |
| 2026-06-09 | 688584 | 16.48 | DROP | 11.93 | 192 | ! |  | GENERAL_WATCH |
| 2026-06-30 | 688220 | 16.47 | DROP | 30.33 | 77 |  |  | GENERAL_WATCH |
| 2026-06-15 | 301526 | 16.46 | DROP | 12.63 | 123 | ! |  | GENERAL_WATCH |
| 2026-07-09 | 688328 | 16.43 | DROP | 13.07 | 215 | ! |  | GENERAL_WATCH |

## 4. 字段预测力 (每日横截面 Spearman IC, 主口径 excess)

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

## 5. 权重调整建议 (现行 v10_amt vs IC 实证)

| 字段 | 现权重 | IC | IC占比 | Δ(目标-现) | 建议 |
|---|---|---|---|---|---|
| amt_pct | 0.3232 | 0.0489 | 0.144 | -0.179 | 降权 |
| liquidity | 0.2424 | 0.063 | 0.185 | -0.057 | 降权 |
| money | 0.1616 | 0.0422 | 0.124 | -0.037 | 维持 |
| pressure_score | 0.1414 | 0.0569 | 0.167 | 0.026 | 维持 |
| weimai_strength | 0.0303 | 0.0198 | 0.058 | 0.028 | 维持 |
| orderbook | 0.0202 | 0.0382 | 0.112 | 0.092 | 加权 |
| auction_strength | 0.0909 | 0.0708 | 0.208 | 0.117 | 加权 |

> 注: *_rank 字段已做方向翻转(秩越小越好), 故正 IC 才算有效; IC<=0 的核心字段建议降权或剔除。
> 调整需经 walk-forward 出样本验证(见 v10_optimize)后再改线上 edge 公式, 勿凭单日改模型。