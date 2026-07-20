# v20 环境 + 数据存量探针 (重构地基)

- 生成: 2026-07-19T20:14:36
- Python: 3.10.12 (main, Jun 22 2026, 18:55:27) [GCC 11.4.0]
- 平台: Linux-6.8.0-1063-gcp-x86_64-with-glibc2.35 ｜CPU: 2

## 可用 ML 库

- 已装: numpy, pandas, torch, sympy
- 缺失: scipy, sklearn, lightgbm, xgboost, statsmodels, joblib

| 库 | 版本/状态 |
|---|---|
| numpy | 2.2.6 |
| pandas | 2.2.3 |
| scipy | MISSING (ModuleNotFoundError) |
| sklearn | MISSING (ModuleNotFoundError) |
| lightgbm | MISSING (ModuleNotFoundError) |
| xgboost | MISSING (ModuleNotFoundError) |
| statsmodels | MISSING (ModuleNotFoundError) |
| torch | 2.11.0+cpu |
| joblib | MISSING (ModuleNotFoundError) |
| sympy | 1.14.0 |

## 数据存量

- 有 v9 快照的交易日: **24** ｜范围: ['2026-05-20', '2026-07-10']
- 原始候选行总数: 8004
- 可训练交易日(含 excess, 日>=30行): **23** ｜可训练样本行: **7663**
- dailyline CSV 文件数: 3971
- 可训练日期: 2026-05-21, 2026-06-01, 2026-06-02, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-15, 2026-06-16, 2026-06-18, 2026-06-23, 2026-06-24, 2026-06-26, 2026-06-29, 2026-06-30, 2026-07-01, 2026-07-02, 2026-07-03, 2026-07-08, 2026-07-09, 2026-07-10

> 用途: 定模型形态(有无 lightgbm/sklearn 决定能否上学习排序) 与 量化数据瓶颈(样本太少则首要任务是累积/回填数据)。