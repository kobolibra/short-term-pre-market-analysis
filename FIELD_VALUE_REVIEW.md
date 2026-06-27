# 盘前数据逐表字段价值评审 (FIELD VALUE REVIEW)

> 目的: 逐张盘前下载数据表, 判定每个字段“有价值/无价值”, 既看数据本身(IC/分档), 也讲信息原理。
> 配套 HANDOFF.md。这是持续记录的讨论台账。最后更新: 2026-06-27。

## 方法与口径
- 预测口径: `excess = (close - open) / preclose * 100`(开盘买入->收盘的超额)。
- 只用真盘前快照(HHMMSS<=093000, 当日竞价批次最后一张), 避免泄漏。
- 数值字段: 逐日横截面 Spearman IC + 跨日 mean/ICIR(日内>=8对)。
- 分类字段(板位/题材): 按档分组的(去市场均值)平均超额。
- 情绪/总量字段: 日序列 vs 当日均值收益的时序相关(小样本~18-22天, 弱证据)。

## 泄漏教训(必须牢记)
- pool.*/cashflow.* 抓于 ~10:01 盘中; review.*/home.ztpool 抓于 ~17:20 盘后; dailyline ~18:xx。这些“同日”用即泄漏, 只能 T-1 滞后用。
- rank.rocket/hot_stock_day 每天重抒40+次(含盘后), 旧脚本取 files[-1] -> 泄漏。rocket 早期 0.26 IC 是泄漏, 干净后 <0.065。
- 真盘前(~09:25-09:29): auction.jjyd.*(net_amount/qiangchou/vratio/weimai)、home.kaipan.plate.summary、auction.jjlive.fengdan、home.qxlive.top_metrics。

## 干净盘前 IC 排行(job 0050, <=09:30)
| 表/字段 | IC | ICIR |
|---|---|---|
| qiangchou auction_turnover_wan | 0.163 | 0.665 |
| qiangchou turnover_rate_pct | 0.135 | 0.731 |
| qiangchou auction_change_pct(gap) | 0.130 | 0.357 |
| vratio change(gap) | 0.119 | 0.369 |
| weimai main_net_inflow_full | 0.103 | 0.554 |
| weimai super_large_net_inflow | 0.094 | 0.548 |
| vratio auction_turnover_wan | 0.093 | 0.487 |
| net_amount auction_change_pct | -0.068 | -0.43 |
| fengdan amount_915 | -0.065 | -0.52 |

## 因子冗余(job 0053)
- 抢筹成交额 ~ 量比成交额 corr 1.0; 抢筹涨幅 ~ 量比涨幅 1.0; 成交额 ~ 超大单 0.95; 成交额 ~ 主力净流入 0.62。
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
- 冗余(0058 avg spearman): auction_turnover_wan ~ volume_ratio_multiple -0.038(正交, 印证“水平 vs 变化”两维度独立, 但变化维无正IC故无用); auction_change~latest 0.787; turnover~量比 0.634; turnover~换手 0.58。
- 与抢筹表重叠(0058, 16天): vratio 日均85.2行 vs qiangchou 47.7行, 重叠仅13.6行 -> 仅16%的vratio出现在抢筹; 84%为vratio-only。
- 分桶(去市场均值超额): vratio_only +0.058(n1146, 近乎持平) vs in_qiangchou -0.306(n218, 反而更差); 原始超额两桶皆负(-0.48/-0.85), 即爆量高关注票整体倾向回吐。

### 交互/条件检验(0059 v49, 回应用户“非线性/双高”质疑, n16, baseline 去均值=0)
用户质疑: 放量倍数/量比单因子线性无效, 不代表无价值; 量比与量绝对值“都很大”可能有增量。结论: **方向部分成立——确有条件结构, 单因子线性确实漏看; 但都不能把表③升级为独立alpha源, 稳健载体仍是绝对竞价成交额。**
- **绝对成交额 × 放量倍数 象限(去均值超额)**: HH双高 +0.219(n337) > HL +0.096(n346) > LL -0.117(n334) > **LH(倍数高但量小) -0.196(n347, 最差)**。-> 高成交额=正, 低成交额=负; LH 最差再次坐实微盘基数污染(放量倍数高+绝对量小=垃圾)。但: 高成交额子集内放量倍数 条件IC **-0.005(≈0)**; 双高闸口 top33 -0.222 / top20 -0.684(负), top10 +0.422(n12噪声)。-> **即便用绝对量门槛过滤掉微盘, 放量倍数在高量集合内仍无增量**。反向: 高比值子集内 成交额 条件IC 0.113/0.532(强)。价值全在成交额。
- **绝对成交额 × 量比(auction_volume_ratio) 象限**: **HL(高量+低量比) +0.754(n173, 最优!)** >> HH双高 -0.046(≈0) > LH -0.099 > LL -0.177。条件: 高量内 量比 IC -0.027(略负, 量比越高略差); 乘积交互 IC **-0.045/-0.613(稳定负)**; min(rank)双高单调因子 0.072/0.375(主要由量驱动)。唯独极端尾 both_top10 +0.966(n68)为正。-> **量比的有效结构与“双高=好”相反: 高成交额 + 不过热(低量比)最优(“安静的大票”), 量比应作降温过滤而非加分**; 仅在极端双高尾部(top10%)才转正, 但 n68 小样本易碎。
- **结论(交互层)**: 
  - ❌ 放量倍数: 即便条件化(高量门槛)仍无增量(条件IC≈0, 闸口负), 维持丢弃; 仅可作 *低量区避雷* 负向标记(LH -0.196)。
  - ⚠ 量比: 不是“越大越好”, 而是 *高成交额内偏好非过热(低/中量比)* 的条件过滤规则(HL +0.754); 极端双高尾部有微弱正向但样本不足, 不单独做多。
  - 💡 元教训: 用户对“多探索非线性”的坚持是对的——单因子线性 IC 会漏掉 regime 依赖(放量倍数符号随成交额翻转; 量比存在反直觉“非过热更优”结构)。但表③仍 **不作独立alpha源纳入**, 升级产出为 *一条条件过滤规则*(高量集合内剔除过热量比)。
- **结论(字段层, 维持0058)**: 
  - ❌ 本表独有卖点 volume_ratio_multiple(放量倍数)证伪: IC -0.028(负), 独立复算 -0.029 印证; 条件化亦无救。信息原理: 极端放量倍数被昨日竞价额极小的票主导(昨1万->今197万=倍数197), 分母不稳 -> 微盘垃圾 -> 均值回归。
  - ❌ vratio 不提供独立 alpha; 唯二为正的 auction_turnover_wan(0.093)/turnover_rate_pct(0.041)即抢筹表已捕获且更强(0.163/0.135), 本表更弱因 universe 更宽更杂(85 vs 48)。
  - ❌ 丢弃: volume_ratio_multiple、yesterday_auction_turnover_wan(满覆盖但派生比值负IC)、latest_change_pct(时点依赖)、auction_change_pct(本表n6符号翻负)、*_text、raw、rank、seal_amount_wan(空)。
  - ✅ 新增可用(条件过滤, 非做多因子): 在高成交额集合内, 优先非过热(低/中 auction_volume_ratio)标的。
  - 表③相对表② = 冗余偏劣, 不单独纳入; 价值降级为上述一条条件过滤规则。

---

## Job 台账
- 0050 v39 干净IC ✓; 0051 v41 失败(f-string反斜杠); 0052 v41修复 时点审计 ✓; 0053 v42 冗余/组合 ✓; 0054 v43 T-1滞后 ✓; 0055 v44 去相关组合 ✓; 0056 v45 封单深挖 ✓; 0057 v46 抢筹深挖 ✓; 0058 v48 量比深挖 ✓; 0059 v49 量比交互/条件 ✓。
- v47 自算开盘gap方案 已废弃(用户否决, 改用 auction_change_pct)。
- **下一个 job id = 0060**。
