# simple premarket signal selector 效果复盘（2026-05-19 / 2026-05-20 / 2026-05-21）

## 说明

- 新版本：`d9e320b refactor: replace v7.3 with simple premarket signal selector`
- 口径：同日超额收益 = 收盘涨幅 - 竞价涨幅
- 评价重点：BUY / WATCH / REJECT / AVOID 四个桶，检验新 selector 是否真的把“盘前 edge”收敛到了 BUY
- `2026-05-21` 的收盘涨幅不再依赖 `dailyline`，而是直接使用当日盘后重新下载的 5 张“竞价异动”表中的网页 `涨幅` 列。

## 本次盘后重新下载并覆盖的 5 个 capture

- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.vratio/092543.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.qiangchou/092543.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.net_amount/092544.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjlive.fengdan/092546.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/captures/2026-05-21/auction.jjyd.weimai/092547.json`

## 对应重跑分析文件

- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/reports/2026-05-19/premarket/162527_analysis_v7_3.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/reports/2026-05-20/premarket/162527_analysis_v7_3.json`
- `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/reports/2026-05-21/premarket/162529_analysis_v7_3.json`

## 2026-05-19

- 市场状态：`normal`
- selector：`simple_premarket_signal_selector_v1`
- BUY 数量：`5`
- 候选数：`215`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 5 | 5 | 1.391 | 1.861 | 2.126 | 5 | 0 |
| WATCH | 10 | 10 | 0.828 | 3.283 | 2.003 | 7 | 3 |
| REJECT | 14 | 14 | 1.242 | -0.602 | -1.093 | 5 | 9 |
| AVOID | 11 | 11 | 9.041 | 0.487 | -0.002 | 5 | 6 |

### BUY 明细

| code | name | pre_action | auction_pct | close_pct | excess_return | close_source | reason |
|---|---|---|---:|---:|---:|---|---|
| 300537 | 广信材料 | MOMENTUM_CATCHUP | 2.090 | 5.0083 | 2.918 | dailyline.stocks | BUY:MOMENTUM_CATCHUP:passed |
| 002081 | 金螳螂 | LOW_OPEN_REVERSAL | -4.530 | -2.4045 | 2.126 | dailyline.stocks | BUY:LOW_OPEN_REVERSAL:passed |
| 002407 | 多氟多 | LOW_OPEN_REVERSAL | -2.040 | -1.6142 | 0.426 | dailyline.stocks | BUY:LOW_OPEN_REVERSAL:passed |
| 002979 | 雷赛智能 | THEME_CATCHUP | -0.140 | 1.9972 | 2.137 | dailyline.stocks | BUY:THEME_CATCHUP:passed |
| 002580 | 圣阳股份 | MOMENTUM_CATCHUP | 2.270 | 3.9698 | 1.700 | dailyline.stocks | BUY:MOMENTUM_CATCHUP:passed |

### 桶内完整名单

- BUY（5）：`300537 广信材料`；`002081 金螳螂`；`002407 多氟多`；`002979 雷赛智能`；`002580 圣阳股份`
- WATCH（10）：`002428 云南锗业`；`600584 长电科技`；`600118 中国卫星`；`002196 方正电机`；`002600 领益智造`；`603773 沃格光电`；`002181 粤传媒`；`600903 贵州燃气`；`600396 华电辽能`；`002195 岩山科技`
- REJECT（14）：`600143 金发科技`；`688590 新致软件`；`300454 深信服`；`002048 宁波华翔`；`002870 香山股份`；`300635 中达安`；`301502 华阳智能`；`688255 凯尔达`；`603051 鹿山新材`；`002823 凯中精密`；`603028 赛福天`；`603777 来伊份`；`002708 光洋股份`；`603311 金海高科`
- AVOID（11）：`002173 创新医疗`；`600860 京城股份`；`600376 首开股份`；`003018 金富科技`；`000417 合百集团`；`603082 北自科技`；`600506 统一股份`；`300657 弘信电子`；`001258 立新能源`；`688507 索辰科技`；`002421 达实智能`

### 漏选 / 错杀

- WATCH 中最强：
  - `603773 沃格光电`｜超额 `13.841%`｜WATCH:LOW_OPEN_REVERSAL:score_too_low
  - `600396 华电辽能`｜超额 `12.752%`｜WATCH:LOW_OPEN_REVERSAL:reversal_bad_cost
  - `600584 长电科技`｜超额 `6.682%`｜WATCH:LOW_OPEN_REVERSAL:score_too_low
  - `002196 方正电机`｜超额 `4.203%`｜WATCH:THEME_CATCHUP:score_too_low
  - `600903 贵州燃气`｜超额 `3.289%`｜WATCH:AUCTION_FOLLOW:auction_follow_bad_cost
- REJECT 中最强：
  - `603311 金海高科`｜超额 `7.756%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
  - `300454 深信服`｜超额 `1.686%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
  - `301502 华阳智能`｜超额 `1.506%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
  - `603051 鹿山新材`｜超额 `1.123%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
  - `688255 凯尔达`｜超额 `1.073%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
- AVOID 中仍然走强：
  - `300657 弘信电子`｜超额 `9.380%`｜fake_strength_or_entry_avoid
  - `688507 索辰科技`｜超额 `8.524%`｜fake_strength_or_entry_avoid
  - `002173 创新医疗`｜超额 `4.199%`｜fake_strength_or_entry_avoid
  - `603082 北自科技`｜超额 `0.004%`｜fake_strength_or_entry_avoid
  - `600506 统一股份`｜超额 `0.002%`｜fake_strength_or_entry_avoid

## 2026-05-20

- 市场状态：`cold`
- selector：`simple_premarket_signal_selector_v1`
- BUY 数量：`2`
- 候选数：`213`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 2 | 2 | 2.807 | -0.003 | -0.003 | 0 | 2 |
| WATCH | 5 | 5 | -5.120 | -0.000 | -0.001 | 2 | 3 |
| REJECT | 4 | 4 | -3.018 | 0.002 | 0.002 | 2 | 2 |
| AVOID | 19 | 19 | -2.658 | -0.001 | -0.001 | 6 | 10 |

### BUY 明细

| code | name | pre_action | auction_pct | close_pct | excess_return | close_source | reason |
|---|---|---|---:|---:|---:|---|---|
| 603108 | 润达医疗 | MOMENTUM_CATCHUP | 2.590 | 2.5879 | -0.002 | dailyline.stocks | BUY:MOMENTUM_CATCHUP:passed |
| 002208 | 合肥城建 | MOMENTUM_CATCHUP | 3.030 | 3.0255 | -0.004 | dailyline.stocks | BUY:MOMENTUM_CATCHUP:passed |

### 桶内完整名单

- BUY（2）：`603108 润达医疗`；`002208 合肥城建`
- WATCH（5）：`301379 天山电子`；`300302 同有科技`；`603311 金海高科`；`600578 京能电力`；`600208 衢州发展`
- REJECT（4）：`605299 舒华体育`；`301696 三瑞智能`；`002785 万里石`；`301023 奕帆传动`
- AVOID（19）：`002226 江南化工`；`000966 长源电力`；`603687 大胜达`；`603636 南威软件`；`603082 北自科技`；`603206 嘉环科技`；`600758 辽宁能源`；`002611 东方精工`；`600506 统一股份`；`600769 祥龙电业`；`605488 福莱新材`；`600936 北投科技`；`002940 昂利康`；`002173 创新医疗`；`002375 亚厦股份`；`600360 华微电子`；`600831 广电网络`；`000417 合百集团`；`300069 金利华电`

### 漏选 / 错杀

- WATCH 中最强：
  - `600578 京能电力`｜超额 `0.004%`｜WATCH:LOW_OPEN_REVERSAL:reversal_bad_cost
  - `603311 金海高科`｜超额 `0.001%`｜WATCH:THEME_CATCHUP:score_too_low
  - `600208 衢州发展`｜超额 `-0.001%`｜WATCH:LOW_OPEN_REVERSAL:reversal_bad_cost
  - `301379 天山电子`｜超额 `-0.002%`｜WATCH:LOW_OPEN_REVERSAL:score_too_low
  - `300302 同有科技`｜超额 `-0.003%`｜WATCH:LOW_OPEN_REVERSAL:score_too_low
- REJECT 中最强：
  - `605299 舒华体育`｜超额 `0.004%`｜REJECT:LOW_OPEN_REVERSAL:reversal_bad_cost
  - `002785 万里石`｜超额 `0.004%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
  - `301696 三瑞智能`｜超额 `-0.001%`｜REJECT:LOW_OPEN_REVERSAL:score_too_low
  - `301023 奕帆传动`｜超额 `-0.001%`｜REJECT:THEME_CATCHUP:theme_bad_cost
- AVOID 中仍然走强：
  - `002940 昂利康`｜超额 `0.005%`｜fake_strength_or_entry_avoid
  - `600831 广电网络`｜超额 `0.004%`｜fake_strength_or_entry_avoid
  - `002226 江南化工`｜超额 `0.003%`｜fake_strength_or_entry_avoid
  - `600769 祥龙电业`｜超额 `0.003%`｜fake_strength_or_entry_avoid
  - `002173 创新医疗`｜超额 `0.002%`｜fake_strength_or_entry_avoid

## 2026-05-21

- 市场状态：`cold_to_warming`
- selector：`simple_premarket_signal_selector_v1`
- BUY 数量：`4`
- 候选数：`372`
- 收盘涨幅来源：`网页盘后重抓的竞价异动 5 表`，覆盖命中 `372` / `372`

| 桶 | 样本数 | 有绩效样本 | 平均收盘涨幅 | 平均超额收益 | 中位超额收益 | 正超额 | 负超额 |
|---|---:|---:|---:|---:|---:|---:|---:|
| BUY | 4 | 4 | 0.755 | -0.352 | -3.365 | 2 | 2 |
| WATCH | 10 | 10 | -1.451 | -4.751 | -5.415 | 1 | 9 |
| REJECT | 70 | 69 | -4.135 | -5.903 | -7.150 | 7 | 62 |
| AVOID | 15 | 15 | 0.543 | -7.756 | -8.380 | 2 | 12 |

### BUY 明细

| code | name | pre_action | auction_pct | close_pct | excess_return | close_source | reason |
|---|---|---|---:|---:|---:|---|---|
| 600909 | 华安证券 | MOMENTUM_CATCHUP | 4.680 | 7.5500 | 2.870 | auction.jjyd.vratio | BUY:MOMENTUM_CATCHUP:passed |
| 688820 | 盛合晶微 | MOMENTUM_CATCHUP | 3.570 | -6.5700 | -10.140 | auction.jjyd.vratio | BUY:MOMENTUM_CATCHUP:passed |
| 002428 | 云南锗业 | THEME_CATCHUP | 1.690 | -7.9100 | -9.600 | auction.jjyd.vratio | BUY:THEME_CATCHUP:passed |
| 600578 | 京能电力 | LOW_OPEN_REVERSAL | -5.510 | 9.9500 | 15.460 | auction.jjyd.net_amount | BUY:LOW_OPEN_REVERSAL:passed |

### 桶内完整名单

- BUY（4）：`600909 华安证券`；`688820 盛合晶微`；`002428 云南锗业`；`600578 京能电力`
- WATCH（10）：`002759 天际股份`；`603186 华正新材`；`301021 英诺激光`；`000021 深科技`；`688323 瑞华泰`；`300863 卡倍亿`；`300938 信测标准`；`301308 江波龙`；`600584 长电科技`；`002077 大港股份`
- REJECT（70）：`688593 新相微`；`002384 东山精密`；`605358 立昂微`；`002156 通富微电`；`601991 大唐发电`；`300782 卓胜微`；`603019 中科曙光`；`688525 佰维存储`；`300395 菲利华`；`002119 康强电子`；`688082 盛美上海`；`600522 中天科技`；`300223 北京君正`；`001896 豫能控股`；`300398 飞凯材料`；`002049 紫光国微`；`001270 铖昌科技`；`603501 豪威集团`；`688766 普冉股份`；`688502 茂莱光学`；`002081 金螳螂`；`605111 新洁能`；`300373 扬杰科技`；`300458 全志科技`；`301392 汇成真空`；`688380 中微半导`；`600703 三安光电`；`600206 有研新材`；`300567 精测电子`；`600770 综艺股份`；`688485 九州一轨`；`688518 联赢激光`；`688052 纳芯微`；`688401 路维光电`；`688262 国芯科技`；`300474 景嘉微`；`002685 华东重机`；`300481 濮阳惠成`；`688416 恒烁股份`；`301099 雅创电子`；`000859 国风新材`；`002456 欧菲光`；`688486 龙迅股份`；`600962 国投中鲁`；`002158 汉钟精机`；`603061 金海通`；`002725 跃岭股份`；`688378 奥来德`；`688230 芯导科技`；`300183 东软载波`；`300831 派瑞股份`；`603687 大胜达`；`002379 宏桥控股`；`688693 锴威特`；`688720 艾森股份`；`688138 清溢光电`；`688286 敏芯股份`；`688325 赛微微电`；`688709 成都华微`；`603165 荣晟环保`；`600876 凯盛新能`；`301369 联动科技`；`002952 亚世光电`；`000045 深纺织Ａ`；`300389 艾比森`；`688125 安达智能`；`603353 和顺石油`；`300554 三超新材`；`600246 万通发展`；`600714 金瑞矿业`
- AVOID（15）：`000066 中国长城`；`603773 沃格光电`；`000813 德展健康`；`002258 利尔化学`；`002629 仁智股份`；`000880 潍柴重机`；`603324 盛剑科技`；`688181 八亿时空`；`000536 华映科技`；`600676 交运股份`；`000809 和展能源`；`301322 绿通科技`；`600604 市北高新`；`603005 晶方科技`；`300069 金利华电`

### 漏选 / 错杀

- WATCH 中最强：
  - `688323 瑞华泰`｜超额 `0.320%`｜WATCH:MOMENTUM_CATCHUP:score_too_low
  - `600584 长电科技`｜超额 `-0.060%`｜WATCH:THEME_CATCHUP:score_too_low
  - `002759 天际股份`｜超额 `-4.330%`｜WATCH:MOMENTUM_CATCHUP:score_too_low
  - `002077 大港股份`｜超额 `-4.580%`｜WATCH:THEME_CATCHUP:score_too_low
  - `300938 信测标准`｜超额 `-5.360%`｜WATCH:MOMENTUM_CATCHUP:score_too_low
- REJECT 中最强：
  - `001270 铖昌科技`｜超额 `7.050%`｜REJECT:THEME_CATCHUP:theme_not_confirmed_by_auction
  - `002952 亚世光电`｜超额 `6.900%`｜REJECT:THEME_CATCHUP:theme_bad_cost
  - `300831 派瑞股份`｜超额 `5.850%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
  - `600246 万通发展`｜超额 `4.990%`｜REJECT:THEME_CATCHUP:theme_bad_cost
  - `301369 联动科技`｜超额 `2.610%`｜REJECT:THEME_CATCHUP:theme_weak_strength_or_amount
- AVOID 中仍然走强：
  - `603324 盛剑科技`｜超额 `2.090%`｜fake_strength_or_entry_avoid
  - `000536 华映科技`｜超额 `0.260%`｜fake_strength_or_entry_avoid
  - `300069 金利华电`｜超额 `0.000%`｜fake_strength_or_entry_avoid
  - `603773 沃格光电`｜超额 `-2.570%`｜fake_strength_or_entry_avoid
  - `600604 市北高新`｜超额 `-2.590%`｜fake_strength_or_entry_avoid

## 合并结论（现已统计 3 天）

- BUY 平均超额收益：`0.717%`（样本 `11`，中位数 `1.700%`）
- WATCH 平均超额收益：`-0.587%`（样本 `25`，中位数 `-0.003%`）
- REJECT 平均超额收益：`-4.778%`（样本 `87`，中位数 `-5.670%`）
- AVOID 平均超额收益：`-2.467%`（样本 `45`，中位数 `-0.003%`）

## 结论

- `2026-05-19`：BUY 桶有效，5 只 BUY 全部为正超额，平均超额明显为正。
- `2026-05-20`：新 selector 明显收缩，只保留 2 只 BUY，但这 2 只当日超额基本为 0，说明这一天没有真实 alpha。
- `2026-05-21`：用盘后网页 5 表回填后，BUY 平均超额为 `-0.352%`，但 4 只里只有 `2` 只正超额、`2` 只负超额，离散度很大。
- `2026-05-21` 的 BUY 里，`600578 京能电力` 超额 `15.460%` 非常强，但 `688820 盛合晶微` / `002428 云南锗业` 分别超额 `-10.140%` / `-9.600%`，说明 selector 选中与错杀同时都很剧烈。
- 3 天合并后，BUY 平均超额 `0.717%`，WATCH 平均超额 `-0.587%`。BUY 虽然仍然更高，但优势并不大，而且 WATCH 里依然残留不少强票。
- 这版 selector 的真实表现更像：**保守、集中，但稳定性不够**。它能抓到极强个股，也会把明显的大亏票一起放进 BUY。
- 下一步如果要继续改，优先应该检查 `THEME_CATCHUP` 与高位动量票在冷转暖日的负反馈过滤，而不是继续单纯缩 BUY 数量。

