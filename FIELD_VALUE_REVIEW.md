# 盘前数据逐表字段价值评审 (FIELD VALUE REVIEW)

> 目的: 逐张盘前下载数据表, 判定每个字段“有价值/无价值”, 既看数据本身(IC/分档), 也讲信息原理。
> 配套 HANDOFF.md。这是持续记录的讨论台账。最后更新: 2026-06-28。

## 方法与口径
- 预测口径: `excess = (close - open) / preclose * 100`(开盘买入->收盘的超额)。
- 只用真盘前快照(HHMMSS<=093000, 当日竞价批次最后一张), 避免泄漏。
- 数值字段: 逐日横截面 Spearman IC + 跨日 mean/ICIR(日内>=8对)。
- 分类字段(板位/题材): 按档分组的(去市场均值)平均超额。
- 情绪/总量字段: 日序列 vs 当日均值收益的时序相关(小样本~18-22天, 弱证据)。
- **字段语义口径(0060 教训): 下结论前必须回看 raw 原始向量并跨表对照, 不能仅凭 header 名采信(采集脚本的 header 名可能是推测)。**
- **universe 口径(0064 教训): 同一字段在不同 cohort(窄/宽 universe)有效性可能反转; 必须分 cohort 评估, 不能只看全池 IC。**

## 泄漏教训(必须牢记)
- pool.*/cashflow.* 抓于 ~10:01 盘中; review.*/home.ztpool 抓于 ~17:20 盘后; dailyline ~18:xx。这些“同日”用即泄漏, 只能 T-1 滞后用。
- rank.rocket/hot_stock_day 每天重抓40+次(含盘后), 旧脚本取 files[-1] -> 泄漏。rocket 早期 0.26 IC 是泄漏, 干净后 <0.065。
- 真盘前(~09:25-09:29): auction.jjyd.*(net_amount/qiangchou/vratio/weimai)、home.kaipan.plate.summary、auction.jjlive.fengdan、home.qxlive.top_metrics。

## 干净盘前 IC 排行(job 0050, <=09:30)
| 表/字段 | IC | ICIR |
|---|---|---|
| qiangchou auction_turnover_wan | 0.163 | 0.665 |
| qiangchou turnover_rate_pct | 0.135 | 0.731 |
| qiangchou auction_change_pct(gap) | 0.130 | 0.357 |
| vratio change(gap) | 0.119 | 0.369 |
| weimai main_net_inflow/turnover | 0.120 | 0.667 |
| vratio auction_turnover_wan | 0.093 | 0.487 |
| net_amount gap(auction_change_pct) | -0.068 | -0.43 |
| fengdan amount_915 | -0.065 | -0.52 |

## 因子冗余(job 0053)
- 抢筹成交额 ~ 量比成交额 corr 1.0; 抢筹涨幅 ~ 量比涨幅 1.0; 成交额 ~ 主力净流入 0.62。
- 独立维度仅 ~3: 资金规模 / 换手率 / 涨幅gap。等权堆 7 因子 IC 0.151 < 单最强 0.163。

## 去相关组合里程碑(job 0055)
- comp_SD {成交额+换手率+gap} 去相关等权 z: **IC 0.179 / ICIR 0.93**(16天), 首次稳超单因子 0.163, ICIR 0.67->0.93。
- 叠加 T-1 复盘(comp_SD_FP) 反降到 0.145; comp_ALL 0.127。等权叠不同源信号无效, 需条件化使用。

## T-1 滞后价值(job 0054)
- review.fupan.plate 昨日成交额 IC 0.103 / 换手率 0.087(正); home.ztpool 晋级率 -0.07(反向退潮); cashflow/pool 滞后 ~0.05 基本无价值。

---

## 表① 竞价封单表 auction.jjlive.fengdan — ✅ 完成(0050+0056)
源 jjlive.json, ~09:28 抓取, 每日~80-107行(已封板/近封板强势股)。
字段: section_*(表头全市场聚合)、code/name、tag_1/2/3+tags、board_label、amount_915/920/925、latest_change_pct。
- 覆盖率: amount_915 98.5%; amount_920 12.5%; amount_925 7.4%。
- 数值 IC: amount_915 -0.065/ICIR-0.52; 920 -0.077(n12); 925 -0.049(n3); latest_change_pct +0.048; 撤单率 920/915 -0.046(假设证伪)。
- board_label 板位(去市场均值超额): 昨3板 +1.76、昨2板 +0.84、昨首板 +0.01、今日首板 -1.51、今日2板 -1.23、今日3/4板 负。
- 表头情绪择时(vs当日均值收益, n18): seal_total -0.44、t25 -0.48、yizi_count -0.37、t20 -0.35、t15 -0.22。
- **结论**: 丢弃所有金额字段+latest_change_pct+撤单率; 保留 board_label(昨日板正/今日封板剔除); 保留表头总量作过热反向仓位信号。

## 表② 竞价抢筹表 auction.jjyd.qiangchou — ✅ 完成(0050+0053+0057) — 全项目最强信号源
源 ~09:28, 每日~59行; group=grab(全榜)+group=qiangchou(精选子集~5只)。
字段: group/rank/code/name、auction_volume_ratio(量比)、auction_change_pct(竞价涨幅★用这个)、latest_change_pct(不用)、auction_turnover_wan(竞价成交额)、grab_strength(厂商合成分)、turnover_rate_pct(换手率)。
- 字段 IC(n16): auction_turnover_wan 0.163/0.665; turnover_rate_pct 0.135/0.731; grab_strength 0.027/0.15(无预测力)。
- **保留**: auction_turnover_wan、turnover_rate_pct、**auction_change_pct(gap, 固定基准)**; group=='qiangchou'。
- **丢弃**: latest_change_pct、grab_strength、auction_volume_ratio(冗余)、*_text/raw。

## 表③ 竞价量比/竞价爆量表 auction.jjyd.vratio — ✅ 完成(0058+0059) — 价值降级为条件过滤
- volume_ratio_multiple(放量倍数) IC -0.028(证伪)、auction_turnover_wan 0.093/0.487(抢筹表已有更强)。
- 交互: 高成交额+不过热(低/中量比) 最优; 放量倍数条件化也无增量。
- **结论**: 表③ 不作独立 alpha 源; 量比降级为条件过滤规则(高量集合内甸0。

## 表④ 涨停委买表 auction.jjyd.weimai — ✅ 完成(0060) ⚠ 字段语义已更正
**重要更正**: 本表的资金流字段**只有一个真实独立量 = 竞价主力净额 main_net_inflow**。被标为 super_large/large_order 的两列仅是主力净额的机械二分拆, 两列相关 -0.919, 逢加必=主力净额。真正的主力/超大单/大单 多档分解属于盘中 cashflow 表(泄漏不可用)。
- 字段 IC(n18): main_net_over_turnover **0.120/0.667**(最佳); main_net_over_mktcap 0.105/0.581。
- 净流出符号过滤: pos +0.372 vs neg -0.479, 差异 0.85。
- board_label: 昨3连板 +0.614; 当日连板 -0.9~-1.5。
- **保留**: 竞价主力净额(归一化)、符号过滤(净流出剔除)、board_label。
- **丢弃**: super_large/large_order功能(机械拆分非独立)、auction_turnover/turnover_rate(本宽universe转负)、其他。

## 表⑤ 竞价净额表 auction.jjyd.net_amount — ✅ 完成(0064) — 竞价主力正主表+gap全覆盖来源
源 ~09:28, 18天1091行; raw 9字段: code/name/竞价涨幅(gap)/最新涨幅/主力净流入(万)/竞价成交额(万)/市值/概念/换手率。Universe 比 weimai 涨停候选子集更全更中性。

**字段 IC(0064, n18)**:
| 字段 | IC | ICIR | 评价 |
|---|---|---|---|
| gap(auction_change_pct) | **-0.068** | **-0.429** | ✅ 中等强度负向信号, 全覆盖100% |
| mktcap | 0.059 | 0.295 | ✅ 弱正, 用于分层过滤 |
| turnover_wan | 0.039 | 0.323 | ⚠ 弱, 与抢筹表重叠优先用后者 |
| main_wan | 0.002 | 0.015 | ❌ 全池几乎为零 |
| main_over_turnover | -0.036 | -0.277 | ❌ 全池无正效 |
| turnover_rate | -0.041 | -- | ❌ |
| main_over_mktcap | -0.041 | -- | ❌ |

**关键发现**:
1. **gap(竞价涨幅) = 本表核心价值**: IC -0.068/ICIR -0.429, 覆盖率100%。竞价涨幅越高开盘后 excess 越低(过热兑现)。这是目前全项目**gap 字段覆盖率最全的来源**(抢筹表 auction_change_pct 仅覆盖 6%).
2. **cohort 依赖**: 同一“竞价主力/成交额”在 weimai(涨停候选窄 cohort) IC +0.120, 但在本表宽池 -0.036≈0。-> **主力净额必须在涨停候选 cohort 内条件化使用**。
3. **市值层**: small(<100亿) mean excess = **-0.338**, mid(100-500亿) = +2.230。-> 小盘竞价异动是陷阱。
4. **交互(raw)**: HH(高turnover+高main_over_turnover) = 4.198(最优), 但未去市场均值, 谨慎采用。

**结论**:
- ✅ **保留(gap)**: auction_change_pct, 全覆盖100%, IC -0.068/ICIR-0.429, 是全项目 gap 的最好来源; 用负向(低 gap 刀片加分, 高 gap 减分)。
- ✅ **保留(分层过滤)**: 小盘(<100亿)剔除; 市值字段可用。
- ✅ **保留(数据来源)**: 作为竞价主力净额满覆盖来源, 配合 cohort 过滤(涨停候选内)使用。
- ❌ **丢弃**: main_wan/main_over_turnover/main_over_mktcap 全池线性; 最新涨幅; concept; raw。
- 💡 **洞察**: 同一字段在窄cohort有效、宽池失效 = “因子有效性依赖universe”; 只看全池IC会误杀真信号。

## 表⑥ 开盘啦板块表 home.kaipan.plate.summary — ✅ 完成(0065) ❌ 选股无正价值
源 ~09:25-09:29 真盘前, **板块级(非个股)**; 需 join 到个股。

**个股级 IC(0065, n17)**:
- sector_rank -0.013 / sector_strength_norm 0.014 / sector_inflow_wan -0.026 / sector_zt_count -0.002
- **in_top1 -0.066 / ICIR -1.171** / in_top3 -0.037/-0.466

**分桶(raw excess)**: in_top1 **-1.273** < in_top3 -0.290 < in_top5 +0.032 < in_top10 +0.078 < **no_match +0.938**
→ 居最强板块的个股反而最差, 不在任何强板块的反而最好!

**join 命中率**: qiangchou **0%**(题材名口径不匹配); net_amount 21.8%; weimai 22.1%。

**信息原理**: 开盘啦“最强板块”=当日最拥挤/最一致预期方向; 龙头早已高开透支, 板块内跟风票开盘即被兑现 = 典型“利好兑现”。

**结论**:
- ❌ 板块级所有字段(强度/排名/主力流入/涨停数)对个股 excess 无正预测力; in_top1 显著负(ICIR -1.17)。
- ⚠ join 口径问题尚未修复(qiangchou 0% 命中)。
- ✅ 唯一潜在用法(待验证): “in_top1” 作轻度**减分/规避**标识(拥挤兑现)。
- 💡 全项目第 N 次印证: 过热/拥挤反向, 真实底层量正向。

---

## Job 台账
| id | 脚本 | 状态 | 备注 |
|---|---|---|---|
| 0050 | v39 干净IC | ✓ | |
| 0051 | v41 | ✗ | f-string 反斜杠报错 |
| 0052 | v41修复 时点审计 | ✓ | |
| 0053 | v42 冗余/组合 | ✓ | |
| 0054 | v43 T-1滞后 | ✓ | |
| 0055 | v44 去相关组合 | ✓ | comp_SD IC 0.179/ICIR 0.93 |
| 0056 | v45 封单深挖 | ✓ | 表① |
| 0057 | v46 抢筹深挖 | ✓ | 表② |
| 0058 | v48 量比深挖 | ✓ | 表③ |
| 0059 | v49 量比交互 | ✓ | 表③ 交互层 |
| 0060 | v50 委买深挖 | ✓ | 表④, 字段语义经用户校正 |
| 0061 | v51 net_amount早期 | ✗ | Daily(root) bug |
| 0062 | v52 net_amount早期 | ✗ | 同上 |
| 0063 | v53 kaipan join早期 | ✗ | 不可用 |
| 0064 | v54 net_amount修复 | ✓ | 表⑤ 正式结果 |
| 0065 | v55 kaipan修复 | ✓ | 表⑥ 正式结果 |

**下一个 job id = 0066** (表⑦ rank.rocket / rank.hot_stock_day 盘前雷达; 干净盘前后 IC<0.065 待复核)
之后: 表⑧ home.qxlive.top_metrics (市场级指标)。
