# 干净幸存因子: 相关性 + 组合 IC v42

- 生成: 2026-06-27T09:00:21

## 单因子

| 因子 | mean_ic | icir | n_days | avg_rows |
|---|---|---|---|---|
| QC.turnover | 0.1629 | 0.665 | 16 | 42 |
| QC.turnrate | 0.1349 | 0.731 | 16 | 42 |
| QC.chg | 0.1295 | 0.357 | 16 | 42 |
| VR.chg | 0.119 | 0.369 | 16 | 85 |
| WM.mainflow | 0.1034 | 0.554 | 18 | 142 |
| WM.xlflow | 0.0937 | 0.548 | 18 | 142 |
| VR.turnover | 0.0928 | 0.487 | 16 | 85 |

## 等权 z 组合

- mean_ic=0.1515 icir=0.736 n_days=18

## 两两相关(Spearman, 越高越重复)

| 因子对 | avg_spearman | n_days |
|---|---|---|
| QC.turnover ~ VR.turnover | 1.0 | 14 |
| QC.chg ~ VR.chg | 1.0 | 14 |
| QC.turnover ~ WM.xlflow | 0.947 | 13 |
| VR.turnover ~ WM.xlflow | 0.89 | 16 |
| QC.turnover ~ WM.mainflow | 0.623 | 13 |
| QC.turnover ~ QC.turnrate | 0.608 | 16 |
| QC.turnrate ~ VR.turnover | 0.565 | 14 |
| VR.chg ~ WM.mainflow | 0.441 | 16 |
| VR.turnover ~ VR.chg | 0.337 | 16 |
| QC.turnrate ~ VR.chg | 0.325 | 14 |
| WM.mainflow ~ WM.xlflow | 0.31 | 18 |
| VR.turnover ~ WM.mainflow | 0.303 | 16 |
| VR.chg ~ WM.xlflow | 0.268 | 16 |
| QC.turnover ~ VR.chg | 0.261 | 14 |
| QC.chg ~ VR.turnover | 0.261 | 14 |
| QC.turnrate ~ QC.chg | 0.21 | 16 |
| QC.chg ~ WM.mainflow | 0.178 | 13 |
| QC.turnrate ~ WM.xlflow | 0.151 | 13 |
| QC.chg ~ WM.xlflow | 0.13 | 13 |
| QC.turnover ~ QC.chg | 0.114 | 16 |
| QC.turnrate ~ WM.mainflow | 0.074 | 13 |