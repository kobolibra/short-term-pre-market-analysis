# 盘前数据逐表字段价值评审 (FIELD VALUE REVIEW)

> 目的: 逐张盘前下载数据表, 判定每个字段“有价值/无价值”, 既看数据本身(IC/分档), 也讲信息原理。
> 配套 HANDOFF.md。这是持续记录的讨论台账。最后更新: 2026-06-27。

## 方法与口径
- 预测口径: `excess = (close - open) / preclose * 100`(开盘买入->收盘的超额)。
- 只用真盘前快照(HHMMSS<=093000, 当日竞价批次最后一张), 避免泄漏。
- 数值字段: 逐日横截面 Spearman IC + 跨日 mean/ICIR(日内>=8对)。
- 分类字段(板位/题材): 按档分组的(去市场均值)平均超额。
- 情绪/总量字段: 日序列 vs 当日均值收益的时序相关(小样本~18-22天, 弱证据)。
- **字段语义口径(0060 教训): 下结论前必须回看 raw 原始向量并跨表对照, 不能仅凭 header 名采信(采集脚本的 header 名可能是推测)。**

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
| weimai main_net_inflow(竞价主力) | 0.103 | 0.554 |
| vratio auction_turnover_wan | 0.093 | 0.487 |
| net_amount auction_change_pct | -0.068 | -0.43 |
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
- 数值 IC: amount_915 -0.065/ICIR-0.52; 920 -0.077(n12); 925 -0.049(n3); latest_change_pct +0.048; 撤单率 920/915 -0.046(假设证伪)。-> 金额类全部无可用线性价值。
- board_label 板位(去市场均值超额): 昨3板 +1.76、昨2板 +0.84、昨首板 +0.01、今日首板 -1.51、今日2板 -1.23、今日3/4板 负。-> 昨日板接力=正向, 今日已封板=强负向(已到顶回落)。
- 表头情绪择时(vs当日均值收益, n18): seal_total -0.44、t25 -0.48、yizi_count -0.37、t20 -0.35、t15 -0.22。-> 竞价过热=当日整体更差(反向择时, 小样本待确认)。
- **结论**: 丢弃所有金额字段+latest_change_pct+撤单率; 保留 board_label 作分类过滤(昨日板正/今日封板剔除); 保留表头总量作过热反向仓位信号。

## 表② 竞价抢筹表 auction.jjyd.qiangchou — ✅ 完成(0050+0053+0057)
源 竞价/竞价异动/竞价抢筹, ~09:28 抓取, 每日~59行, 合并两子表: group=grab(竞价抢筹全榜) + group=qiangchou(同源精选子集, 每日~5只, 同一code可同时出两组; 精选语义为推断)。**全项目最强信号源。**
字段: group/rank/code/name、auction_volume_ratio(量比)、seal_amount_wan、auction_change_pct(竞价涨幅=固定基准gap)、latest_change_pct(最新涨幅=依赖拓取时点)、auction_turnover_wan(竞价成交额)、concept(题材)、*_text 副本、yesterday_auction_turnover_wan、grab_strength(抢筹强度)、turnover_rate_pct(换手率)、raw。
- 覆盖率(0057, 681行): seal_amount_wan 1.6%(11/681) -> 基本空; yesterday_auction_turnover_wan 0%(全空)。
- 字段 IC(0057, n16): auction_turnover_wan 0.163/ICIR0.665; turnover_rate_pct 0.135/ICIR0.731(最稳); latest_change_pct 0.130/0.357(满覆盖但时点依赖); auction_volume_ratio 0.068/0.228(弱); auction_change_pct 0.038/0.501(仅n6, 该列多日未填=采集问题); **grab_strength 0.027/0.15(厂商合成分, 几乎无预测力!)**。
- 冗余(0057 avg spearman): auction_change_pct~latest_change_pct 0.78(同一概念); turnover~量比 0.66; turnover~换手率 0.64; turnover~涨幅 0.20; 换手率~涨幅 0.27; 其余<0.2。-> 独立维度仍是 资金规模/换手率/涨幅gap 三维。
- 分组桶(0057, 去市场均值超额): grab_only -0.013(n630) vs qiangchou 精选 +0.159(n51) -> 精选标签可能带正向加分(样本小n51, 弱证据, 且精选语义未证实)。
- **GAP 字段选型(用户拍板)**: 用 **auction_change_pct**(竞价定盘固定基准, 时点无关), **不用** latest_change_pct(依赖拓取时点, 早拓/盘后重拓会偏离真实开盘)。A股微观: 9:25 集合竞价定盘产生开盘价, 9:25-9:30 静默不撮合; 理论上盘前 auction==latest。auction_change_pct 覆盖低是上游采集问题, 需补全, 不是概念问题。(已否决“自算开盘价”方案。)
- **结论**: 
  - ✅ 保留(核心三因子): auction_turnover_wan、turnover_rate_pct、**auction_change_pct(=gap, 固定基准)**。撑起 comp_SD 0.179。
  - ✅ 保留(分类, 弱): group=='qiangchou' 精选标签。
  - ❌ 丢弃: latest_change_pct(时点依赖, 用 auction_change_pct 替代)、grab_strength(厂商合成分=噪声)、auction_volume_ratio(量比 弱且与成交额冗余0.66)、*_text 副本、raw、rank、seal_amount_wan(空)、yesterday_auction_turnover_wan(空)。
  - 💡 关键洞察: 厂商最显眼的“抢筹强度”(grab_strength)预测力几乎为零(IC0.027), 真正有价值的是底层原始量(成交额/换手率/高开)。
  - ⚠ 待办(上游): 补全 auction_change_pct 采集覆盖率(当前多日未填)。

## 表③ 竞价量比/竞价爆量表 auction.jjyd.vratio — ✅ 完成(0050+0058+0059)
源 竞价异动/竞价爆量(getVratioData/11), ~09:28 抓取, 每日~85行(快照单日155行), 按 volume_ratio_multiple(放量倍数)降序。字段与抢筹表大体重叠, 但有两个抢筹表没有/没法用的字段, 是它存在的理由: (1) yesterday_auction_turnover_wan 这里满覆盖(抢筹表全空); (2) 多出 volume_ratio_multiple(放量倍数=今日竞价额/昨日竞价额)。
字段: rank/code/name、auction_volume_ratio(量比)、seal_amount_wan、auction_change_pct(竞价涨幅gap)、latest_change_pct、auction_turnover_wan(竞价成交额)、concept、*_text 副本、yesterday_auction_turnover_wan、volume_ratio_multiple(放量倍数)、turnover_rate_pct(换手率)、raw。
- 覆盖率(0058, 16天1364行): yesterday_auction_turnover_wan 100%、volume_ratio_multiple 100%(均满覆盖, 区别于抢筹表); seal_amount_wan 3.4%(基本空)。
- 字段 IC(0058, n16): latest_change_pct 0.119/0.369; auction_turnover_wan 0.093/0.487(本表最稳正); auction_volume_ratio 0.058/0.335; turnover_rate_pct 0.041/0.239; auction_change_pct -0.026/-0.231(仅n6, 采集稀疏不可信); **volume_ratio_multiple -0.028/-0.264(负!)**。
- 放量倍数独立复算(today/yesterday turnover IC, n16): -0.029/-0.271 -> 与字段值一致, 确认为负。
- 冗余(0058 avg spearman): auction_turnover_wan ~ volume_ratio_multiple -0.038(正交); auction_change~latest 0.787; turnover~量比 0.634; turnover~换手 0.58。
- 与抢筹表重叠(0058, 16天): vratio 日均85.2行 vs qiangchou 47.7行, 重叠仅13.6行 -> 仅16%的vratio出现在抢筹; 84%为vratio-only。
- 分桶(去市场均值超额): vratio_only +0.058(n1146) vs in_qiangchou -0.306(n218); 原始超额两桶皆负(-0.48/-0.85)。

### 交互/条件检验(0059 v49, 回应用户“非线性/双高”质疑, n16, baseline 去均值=0)
用户质疑: 放量倍数/量比单因子线性无效, 不代表无价值; 量比与量绝对值“都很大”可能有增量。结论: **方向部分成立——确有条件结构, 单因子线性确实漏看; 但都不能把表③升级为独立alpha源, 稳健载体仍是绝对竞价成交额。**
- **绝对成交额 × 放量倍数 象限(去均值超额)**: HH双高 +0.219(n337) > HL +0.096(n346) > LL -0.117(n334) > **LH(倍数高但量小) -0.196(n347, 最差)**。高成交额内放量倍数 条件IC **-0.005(≈0)**; 双高闸口 top33 -0.222 / top20 -0.684, top10 +0.422(n12噪声)。-> 即便用绝对量门槛过滤掉微盘, 放量倍数在高量集合内仍无增量。高比值子集内 成交额 条件IC 0.113/0.532。价值全在成交额。
- **绝对成交额 × 量比 象限**: **HL(高量+低量比) +0.754(n173, 最优!)** >> HH双高 -0.046 > LH -0.099 > LL -0.177。高量内 量比 IC -0.027; 乘积交互 IC **-0.045/-0.613(稳定负)**; 唯独 both_top10 +0.966(n68)为正。-> 量比应作降温过滤而非加分(高成交额+不过热最优), 仅极端双高尾部转正但样本不足。
- **结论(交互层)**: 放量倍数维持丢弃(条件化仍无增量); 量比降级为一条条件过滤规则(高量集合内偏好低/中量比); 元教训: 单因子线性 IC 会漏 regime 结构, 但表③仍不作独立alpha源。
- **结论(字段层, 维持0058)**: 
  - ❌ volume_ratio_multiple(放量倍数)证伪(微盘基数污染), 丢弃。
  - ❌ vratio 不提供独立 alpha; 正IC的 auction_turnover_wan/turnover_rate_pct 即抢筹表更强已捕获。
  - ❌ 丢弃: volume_ratio_multiple、yesterday_auction_turnover_wan、latest_change_pct、auction_change_pct、*_text、raw、rank、seal_amount_wan。
  - ✅ 新增可用(条件过滤): 高成交额集合内优先非过热(低/中量比)标的。
  - 表③相对表② = 冗余偏劣, 价值降级为一条条件过滤规则。

## 表④ 涨停委买表 auction.jjyd.weimai — ✅ 完成(0050+0060) ⚠ 字段语义已更正(用户校正)
源 竞价/竞价异动/涨停委买(daban.json), ~09:28 抓取, 每日~143行(快照单日150行)。

### ⚠ 重要更正(用户校正, 逐行核对 raw 向量 + 跨表对照 net_amount 确认)
本表的资金流字段**只有一个真实独立量 = 竞价主力净额 main_net_inflow**。我上一版误把它当成“主力/超大单/大单 三路分解的独有卖点”, 撤回。证据:
- main_net_inflow_full == main_net_inflow(raw[13]==raw[6], 完全重复列)。
- 被脚本标为 super_large_net_inflow(raw[14]) + large_order_net_inflow(raw[15]) **≡ main_net_inflow**, 逐行精确到元: 春秋电子 17838754+(-4606551)=13232203; 晋控电力 77398975+(-132386496)=-54987521; 风华高科 590630017+(-307050665)=283579352。-> 这两列只是主力净额的一个**机械二分拆**, large_order 近乎恒负、与 super_large 相关 -0.919 = 机械产物, **非独立信号**; 且“超大单/大单”是采集脚本推测标签, 未经厂商口径证实。
- 跨表对照 **表⑤ 竞价净额 net_amount(jjzhuli.json)**: 其 raw 仅 9 字段(code,name,竞价涨幅,最新涨幅,主力净流入万,竞价成交额万,市值,概念,换手率), 同样**只有竞价主力净额一个资金流量, 也没有超大单/大单**。
- 结论: 整个“竞价”族(net_amount + weimai)只提供“竞价主力净额”这**一个**资金流维度; 真正的 主力/超大单/大单/中单/小单 多档分解属于盘中 cashflow 表(泄漏, 不可盘前用), 不在任何竞价表里。撤回上一版“超大单买/大单卖微观结构”叙事(过度解读恒等式)。

字段(33, 实际有效): rank/code/name/price、latest_change_pct、auction_turnover(_wan)、auction_change_pct、**main_net_inflow(=竞价主力, 唯一真实资金流)**、turnover_rate_pct、seal_volume(封单量)、auction_amount(_wan)、seal_volume_again(=seal_volume 重复)、concept、market_cap、board_label、seal_amount_wan、*_text 副本、raw。
- 覆盖率(0060, 18天2570行): 主力/被标超大单/被标大单、market_cap、auction_amount_wan 均100%; seal_amount_wan 仅6.2%(基本空)。
- 字段 IC(0060, n18, 数值保留, 仅更正解读): main_net_inflow_full(=竞价主力) 0.103/0.554(本表核心); super_large 0.094、large_order -0.068 = 主力净额机械二分拆两侧, **非独立超大单/大单**, 不单列采用; market_cap_yi 0.069; seal_volume 0.064; auction_turnover_wan -0.014; turnover_rate_pct -0.019; auction_change_pct -0.102(n6不可信)。
- 归一化(仍有效, 理解为“竞价主力强度”): main_net_over_turnover 0.120/0.667(本表最佳可用形态); main_net_over_mktcap 0.105/0.581。(superlarge_over_turnover 0.135 是同一主力数的拆分变体, 不另算独立因子。) -> 归一化后不降反升 = 竞价主力净额是真信号, 非体量代理(与表③量比相反)。
- 净流入符号桶(基于竞价主力符号): pos +0.372(n1447) vs neg **-0.479(n1123)**, 去均值价差 **0.85** -> 约44%涨停委买票竞价主力净流出且系统跑输 = 强二元过滤。信息原理: 委买量可虚挂诱多, 主力净额符号区分真承接 vs 诱多对倒。
- 交互 成交额 × 竞价主力(0060): HH +0.469(n687,最优) > LH +0.428 > LL -0.311 > **HL(高量+主力净流出) -0.611(最差)**; 高量内主力 条件IC 0.099/0.505(主力是载体), 高主力内成交额 -0.031(成交额次要)。
- board_label(0060): 昨3连板 +0.614 > (none) +0.248 > 昨日首板 -0.344 > 当日连板(2/3/4/首板) -0.9~-1.5。同表①(当日连板强负, 昨日高位/无标签正)。
- 与抢筹重叠(0060, 16天): 9.2/143 = 6.5% -> universe 基本独立。
- **结论(更正版)**: 
  - ✅ 保留(唯一真实资金流因子): 竞价主力净额 main_net_inflow, 用归一化形态 主力/成交额(IC 0.120/0.667)。但它与**表⑤ 竞价净额 net_amount 同概念**, 应在 net_amount(更全75行 universe)统一评估, 在 weimai 上属重叠。
  - ✅ 保留(强二元过滤): 竞价主力净额符号(净流出剔除)。
  - ✅ 保留(分类): board_label。
  - ❌ 丢弃/降级: super_large_net_inflow、large_order_net_inflow(主力净额机械拆分, 非独立, 标签未证实)、main_net_inflow_full / main_net_inflow_wan(=main 重复/单位变体)、auction_turnover_wan / turnover_rate_pct(本宽universe转负, 抢筹表更优)、latest_change_pct / auction_amount(噪声)、auction_change_pct(n6)、market_cap(体量基线)、seal_amount_wan(6%空)、*_text/raw/rank。
  - 💡 更正后关键洞察: ①竞价族只有“竞价主力净额”一个资金流信号, weimai 与 net_amount 同源同概念, 不要重复计入; ②真正的超大单/大单多档分解在盘中 cashflow 表(泄漏不可盘前用); ③竞价主力净额归一化(/成交额)后更强 = 真信号非体量代理; ④符号过滤(净流出剔除)价值高。
  - ⚠ 自查教训: 字段定论前必须回看 raw 原始向量 + 跨表对照, 不能凭 header 名直接采信(本表 super_large/large_order 的 header 名是采集脚本推测, 实为主力净额恒等拆分)。

---

## Job 台账
- 0050 v39 干净IC ✓; 0051 v41 失败(f-string反斜杠); 0052 v41修复 时点审计 ✓; 0053 v42 冗余/组合 ✓; 0054 v43 T-1滞后 ✓; 0055 v44 去相关组合 ✓; 0056 v45 封单深挖 ✓; 0057 v46 抢筹深挖 ✓; 0058 v48 量比深挖 ✓; 0059 v49 量比交互/条件 ✓; 0060 v50 委买深挖 ✓(资金流字段语义经用户校正: 仅竞价主力净额为真, super_large/large_order 系机械拆分)。
- v47 自算开盘gap方案 已废弃(用户否决, 改用 auction_change_pct)。
- **下一个 job id = 0061**(下一张: 表⑤ 竞价净额 net_amount — 竞价主力净额的正主表, 75行)。
