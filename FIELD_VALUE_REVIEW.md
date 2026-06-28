# 盘前数据逐表字段价值评审 (FIELD VALUE REVIEW)

> 目的: 逐张盘前下载数据表, 判定每个字段“有价值/无价值”, 既看数据本身(IC/分档), 也讲信息原理。
> 配套 HANDOFF.md。这是持续记录的讨论台账。最后更新: 2026-06-28。

## 方法与口径
- 预测口径: `excess = (close - open) / preclose * 100`(开盘买入->收盘的超额)。
- 只用真盘前快照(HHMMSS<=093000, 当日竞价批次最后一张), 避免泄漏。
- 数值字段: 逐日横截面 Spearman IC + 跨日 mean/ICIR(日内>=8对)。
- 分类字段(板位/题材): 按档分组的(去市场均值)平均超额。
- **字段语义口径(0060 教训): 下结论前必须回看 raw 原始向量并跨表对照。**
- **universe 口径(0064 教训): 同一字段在不同 cohort 有效性可能反转; 必须分 cohort 评估。**
- **分档均值口径(0068 教训): raw 分档均值会被极少数异常日灌大; 必须 per-date 去均值 + 看胜率 + 异常日敏感性, 否则会把肥尾噪声误判为非线性 alpha。**

## 泄漏教训(必须牢记)
- pool.*/cashflow.* 抓于 ~10:01 盘中; review.*/home.ztpool 抓于 ~17:20 盘后。只能 T-1 滞后用。
- rank.rocket/hot_stock_day 每天重抓40+次(含盘后), 旧脚本取 files[-1] -> 泄漏。干净盘前 IC≈0(已验证)。
- 真盘前(~09:25-09:29): auction.jjyd.*(net_amount/qiangchou/vratio/weimai)、home.kaipan.plate.summary、auction.jjlive.fengdan、home.qxlive.top_metrics。

## 干净盘前 IC 排行(job 0050, <=09:30)
| 表/字段 | IC | ICIR |
|---|---|---|
| qiangchou auction_turnover_wan | 0.163 | 0.665 |
| qiangchou turnover_rate_pct | 0.135 | 0.731 |
| weimai main_net_over_turnover | 0.120 | 0.667 |
| qiangchou auction_change_pct(gap) | 0.130 | 0.357 |
| net_amount gap(auction_change_pct) | -0.068 | -0.43 |
| fengdan amount_915 | -0.065 | -0.52 |

## 去相关组合里程碑(job 0055)
- comp_SD {成交额+换手率+gap} 去相关等权 z: **IC 0.179 / ICIR 0.93**(16天)。

## T-1 滞后价值(job 0054)
- review.fupan.plate 昨日成交额 IC 0.103; home.ztpool 晋级率 -0.07; cashflow/pool ~0.05 无效。

---

## 表① 竞价封单表 auction.jjlive.fengdan — ✅ 完成
- board_label(昨日板正/今日封板剔除): ✅; 表头总量(过热反向择时): ✅; 金额字段/latest/撤单率: ❌

## 表② 竞价抢筹表 auction.jjyd.qiangchou — ✅ 完成 — 全项目最强信号源
- auction_turnover_wan(0.163)、turnover_rate_pct(0.135)、auction_change_pct(gap): ✅; group=='qiangchou': ✅弱
- grab_strength(0.027)、latest_change_pct、auction_volume_ratio(冗余): ❌

## 表③ 竞价量比/爆量表 auction.jjyd.vratio — ✅ 完成 — 降级为条件过滤
- volume_ratio_multiple(-0.028证伪): ❌; 高成交额内优先低/中量比: ✅条件过滤; 不作独立alpha

## 表④ 涨停委买表 auction.jjyd.weimai — ✅ 完成 ⚠ 字段语义已更正
- 竞价主力净额/成交额(0.120/0.667): ✅唯一真实资金流; 净流出符号过滤(0.85): ✅; board_label: ✅
- super_large/large_order(机械拆分非独立): ❌

## 表⑤ 竞价净额表 auction.jjyd.net_amount — ✅ 完成 — gap全覆盖来源
- gap(auction_change_pct, -0.068/-0.429, 100%): ✅全项目gap最佳来源; 小盘(<100亿)剔除(-0.338): ✅
- 主力净额全池IC≈0: ❌ 必须在涨停候选cohort内条件化(weimai已覆盖)

## 表⑥ 开盘啦板块表 home.kaipan.plate.summary — ✅ 完成 ❌ 选股无正价值
- in_top1 IC -0.066/ICIR-1.171; 分桶 in_top1 -1.273 vs no_match +0.938; 板块字段全丢弃; 唯一潜在:in_top1轻度减分(待join修复)

## 表⑦ 盘前雷达 rank.rocket + rank.hot_stock_day — ✅ 完成(0066+0068) ❌ 无可靠edge
字段: rank/code/name/value(+75w格式解析失败)/raw_rate(原始分, 100%)。每日~90-100行。

### 线性 IC(0066, n=17, 去日均值)
rocket rank 0.015/0.163; raw_rate -0.015/-0.164; hot_stock rank -0.023; hot_stock raw_rate 0.023 — 全部≈0。

### 非线性严格复核(0068) — 推翻“top10有价值”假设
背景: 0066 raw分档 top10=2.665 / r11_30=0.048 / r31+=0.854 看似非线性(top10优,r11_30陷), 疑线性IC≈0掩盖了阈值价值。作 per-date 去均值 + 胜率 + 异常日检验:
- **top10 去均值**: mean_dm=+1.794 但 std=6.759, ICIR仅0.265, **win_rate仅41.2%**, **binary IC=-0.017(负!)**。
- **致命发现**: +1.794 几乎完全由2个异常日撑起 — 06-24(+25.258)+06-26(+12.418); 其余15天多为负。这是**肥尾/彩票型**(偶尔爆发,平时跑输), 非稳定edge。raw bucket 2.665 也是被这2天灌大。
- 阈值扫描: top3 ICIR0.169 / top5 ICIR0.320(win47%) / top10 ICIR0.265 / top20 ICIR0.111 — 全部低ICIR+~41-47%胜率, 均outlier驱动, 无可靠截断。
- **r11-30 陷阱区**: mean_dm=-0.837, std=1.864, **win_rate35.3%(即64.7%天为负)**, t≈-1.85(边际显著)。比top10更一致, 但仍弱+小样本。
- top10×qiangchou重叠: 全期仃3只(n_days=3), 样本过小。
- top10×weimai主力方向: 反直觉(main_pos -0.947 vs main_neg +0.410, n小), 噪声。
- **结论(三轮修正后定论)**: 非线性结构在raw层面“存在”但**不可交易** — top10是肥尾彩票(ICIR0.265/胜率41%/binary IC负/2异常日驱动); r11-30弱负向(t-1.85)可作边际规避但证据弱。
- ❌ rocket 无可靠独立edge; 丢弃作选股加分。若用, 仅“避开r11-30”作弱负filter(待更多样本)。
- 💡 元教训: 我先判“无价值”(线性IC), 再判“top10有+1.8价值”(raw bucket), 都错; 严格检验(per-date去均值+胜率+异常日)才见真相=肥尾不可交易。raw分档均值被极少数异常日灌大是陷阱。

### rank.hot_stock_day
- 三档完全平坦(top10 1.011/r11_30 1.251/r31+ 1.130), 维持无价值。

---

## Job 台账
| id | 脚本 | 状态 | 备注 |
|---|---|---|---|
| 0050-0056 | v39-v45 | ✓ | 干净IC/封单/小里程碑 |
| 0057 | v46 | ✓ | 表②抢筹 |
| 0058-0059 | v48-v49 | ✓ | 表③量比 |
| 0060 | v50 | ✓ | 表④委买 |
| 0061-0063 | v51-v53 | ✗ | bug失败 |
| 0064 | v54 | ✓ | 表⑤net_amount |
| 0065 | v55 | ✓ | 表⑥kaipan |
| 0066 | v56 | ✓ | 表⑦ rocket/hotstock 线性IC≈0 |
| 0067 | v57 | ⏳ | 表⑧ qxlive(已队列) |
| 0068 | v58 | ✓ | rocket非线性复核: top10肥尾不可交易 |

**下一个 job id = 0069** (表⑧ qxlive 结果待读; 之后 T-1滞后表汇总收尾)
