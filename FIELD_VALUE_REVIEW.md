# 盘前数据逐表字段价值评审 (FIELD VALUE REVIEW)

> 目的: 逐张盘前下载数据表, 判定每个字段“有价值/无价值”, 既看数据本身(IC/分档), 也讲信息原理。
> 配套 HANDOFF.md。这是持续记录的讨论台账。最后更新: 2026-06-28。

## 方法与口径
- 预测口径: `excess = (close - open) / preclose * 100`(开盘买入->收盘的超额)。
- 只用真盘前快照(HHMMSS<=093000, 当日竞价批次最后一张), 避免泄漏。
- 数值字段: 逐日横截面 Spearman IC + 跨日 mean/ICIR(日内>=8对)。
- 分类字段(板位/题材): 按档分组的(去市场均值)平均超额。
- **字段语义口径(0060 教训): 下结论前必须回看 raw 原始向量并跨表对照, 不能仅凭 header 名采信。**
- **universe 口径(0064 教训): 同一字段在不同 cohort 有效性可能反转; 必须分 cohort 评估。**

## 泄漏教训(必须牢记)
- pool.*/cashflow.* 抓于 ~10:01 盘中; review.*/home.ztpool 抓于 ~17:20 盘后; dailyline ~18:xx。这些“同日”用即泄漏, 只能 T-1 滞后用。
- rank.rocket/hot_stock_day 每天重抓40+次(含盘后), 旧脚本取 files[-1] -> 泄漏。干净盘前 IC≈0(已验证)。
- 真盘前(~09:25-09:29): auction.jjyd.*(net_amount/qiangchou/vratio/weimai)、home.kaipan.plate.summary、auction.jjlive.fengdan、home.qxlive.top_metrics。

## 干净盘前 IC 排行(job 0050, <=09:30)
| 表/字段 | IC | ICIR |
|---|---|---|
| qiangchou auction_turnover_wan | 0.163 | 0.665 |
| qiangchou turnover_rate_pct | 0.135 | 0.731 |
| weimai main_net_over_turnover | 0.120 | 0.667 |
| qiangchou auction_change_pct(gap) | 0.130 | 0.357 |
| vratio auction_turnover_wan | 0.093 | 0.487 |
| net_amount gap(auction_change_pct) | -0.068 | -0.43 |
| fengdan amount_915 | -0.065 | -0.52 |

## 去相关组合里程碑(job 0055)
- comp_SD {成交额+换手率+gap} 去相关等权 z: **IC 0.179 / ICIR 0.93**(16天)。

## T-1 滞后价值(job 0054)
- review.fupan.plate 昨日成交额 IC 0.103; home.ztpool 晋级率 -0.07(反向); cashflow/pool ~0.05 无效。

---

## 表① 竞价封单表 auction.jjlive.fengdan — ✅ 完成
- board_label(昨日板正/今日封板剔除): ✅ 保留
- 表头总量(过热反向择时): ✅ 保留
- 所有金额字段/latest_change_pct/撤单率: ❌ 丢弃

## 表② 竞价抢筹表 auction.jjyd.qiangchou — ✅ 完成 — 全项目最强信号源
- auction_turnover_wan(IC 0.163)、turnover_rate_pct(0.135)、auction_change_pct(gap固定基准): ✅ 保留
- grab_strength(厂商合成分, IC 0.027)、latest_change_pct、auction_volume_ratio(冗余): ❌ 丢弃
- group=='qiangchou' 精选标签: ✅ 弱证据保留

## 表③ 竞价量比/竞价爆量表 auction.jjyd.vratio — ✅ 完成 — 价值降级为条件过滤
- volume_ratio_multiple(放量倍数, IC -0.028): ❌ 证伪; auction_turnover_wan(0.093): 与抢筹表冗余
- 高成交额集合内优先低/中量比: ✅ 条件过滤规则
- 表③ 不作独立 alpha 源

## 表④ 涨停委买表 auction.jjyd.weimai — ✅ 完成 ⚠ 字段语义已更正
- 竞价主力净额 main_net_inflow 归一化(/成交额, IC 0.120/0.667): ✅ 唯一真实资金流因子
- 净流出符号过滤(差异 0.85): ✅ 强二元过滤
- board_label: ✅ 保留
- super_large/large_order(机械拆分, 非独立): ❌ 丢弃

## 表⑤ 竞价净额表 auction.jjyd.net_amount — ✅ 完成 — gap全覆盖来源+主力正主表
- **gap(auction_change_pct, IC -0.068/ICIR -0.429, 100%覆盖)**: ✅ 全项目 gap 最好来源
- 市值分层: ✅ 小盘(<100亿)剔除(-0.338); mid(100-500亿)最优
- 主力净额 全池 IC≈0: ❌ 必须在涨停候选 cohort 内条件化用(weimai 已覆盖)
- 最新涨幅/concept/raw: ❌ 丢弃

## 表⑥ 开盘啦板块表 home.kaipan.plate.summary — ✅ 完成 ❌ 选股无正价值
- in_top1 IC -0.066/ICIR -1.171; 分桶: in_top1 -1.273 vs no_match +0.938
- 板块强度/排名/主力流入/涨停数: ❌ 全部丢弃
- 唯一潜在: in_top1 作轻度减分/规避标识(待 join 口径修复同再验证)
- join 命中率问题: qiangchou 0%(题材名口径不匹配)

## 表⑦ 盘前雷达 rank.rocket + rank.hot_stock_day — ✅ 完成(0066) ❌ 全部丢弃
源: 每日~90-100行; 字段: rank、code、name、value(+75w格式数值解析失败)、raw_rate(原始分値, 100%覆盖)。

**IC 结果(0066, n=17, 去日均值)**:
| 表 | 字段 | IC | ICIR | 结论 |
|---|---|---|---|---|
| rocket | rank | 0.015 | 0.163 | ❌ ≈0 |
| rocket | raw_rate(雷达分) | -0.015 | -0.164 | ❌ ≈0 |
| hot_stock | rank | -0.023 | -0.204 | ❌ ≈0 |
| hot_stock | raw_rate(热度分) | 0.023 | 0.204 | ❌ ≈0 |

**rank 分桶(raw excess, 含市场水平)**:
- rocket top10=2.665 看似好, 但去日均值 IC≈0 -> 是市场水平假象(高雷达天正好是市场向上天)
- hot_stock 三档完全平坦: top10=1.011 ≈ r11_30=1.251 ≈ r31+=1.130

**信息原理**:
- rocket = 涨停雷达, 预测的是“涨停概率”而非“开盘->收盘超额”; 高分股已被市场定价高开, 超额在开盘前被消耗
- hot_stock = 人气热度 = 拥挤度代理 -> 过热反向, 与全项目主题一致
- 早期泄漏 IC=0.26 已验证为泄漏假象; 干净后≈0 ✅ 复核通过

**结论: 两张表全部字段丢弃。**

---

## Job 台账
| id | 脚本 | 状态 | 备注 |
|---|---|---|---|
| 0050-0056 | v39-v45 | ✓ | 干净IC/封单/小里程碑 |
| 0057 | v46 抢筹深挖 | ✓ | 表② |
| 0058-0059 | v48-v49 量比 | ✓ | 表③ |
| 0060 | v50 委买 | ✓ | 表④ |
| 0061-0063 | v51-v53 早期版 | ✗ | bug 失败 |
| 0064 | v54 net_amount修复 | ✓ | 表⑤ |
| 0065 | v55 kaipan修复 | ✓ | 表⑥ |
| 0066 | v56 rocket+hotstock | ✓ | 表⑦, 干净盘前 IC≈0 确认 |

**下一个 job id = 0067** (表⑧ home.qxlive.top_metrics 市场级指标; 盘前字段探测 + 择时测试)
