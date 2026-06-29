# Duanxianxia Canonical Field Dictionary (v10 rebuild source of truth)

> Single source of truth for every dataset field, its caliber, raw provenance,
> and the label corrections required by the v10 rebuild.
> Server-grounded verification (jobs 0075-0081 + live endpoint probes), 2026-06-29.

## Principles
1. raw (positional array / delimited token stream) is the ONLY ground truth. Named rows are a derived view.
2. Every market-cap field MUST carry an explicit caliber tag. Never use bare market-cap / circulating.
3. Historical captures keep OLD names; do NOT mutate them in place. Re-derive canonical rows from raw.

## Caliber legend
- FF    = free-float market cap (自由流通市值)
- FLOAT = circulating market cap (流通市值)
- TOTAL = total market cap (总市值)
- units in 亿 unless marked (万 / 元)

---
## A. Auction family (positional raw arrays)

### A1. auction.jjyd.vratio  -- POST /data/getVratioData/11
| raw idx | canonical | caliber/unit | old label | note |
|---|---|---|---|---|
| 0 | code | | | |
| 1 | name | | | |
| 2 | free_float_market_cap | FF / 亿 | auction_volume_ratio | MISLABEL FIX: not 量比, it is 自由流通市值 |
| 3 | seal_amount_wan | 万 | seal_amount_wan | 封单额 (correct) |
| 4 | auction_change_pct | % | | 竞价涨幅 |
| 5 | latest_change_pct | % | | 最新涨幅 |
| 6 | auction_turnover_wan | 万 | | 竞价成交额 |
| 7 | concept | | | 题材 |
| 11 | volume_ratio | x | volume_ratio_multiple | the REAL 量比 |
| 12 | turnover_rate | % | | 换手率 |

### A2. auction.jjyd.qiangchou -- POST /data/getQiangchouData/11
Same as vratio except:
| 2 | free_float_market_cap | FF/亿 | auction_volume_ratio | MISLABEL FIX |
| 11 | grab_strength | | grab_strength | 抢筹幅度 (correct) |

### A3. auction.jjyd.net_amount -- GET ds.../jjzhuli.json (AES)
| 0 code | 1 name | 2 auction_change_pct | 3 latest_change_pct | 4 main_net_inflow_wan (万) | 5 auction_turnover_wan (万) | 6 free_float_market_cap (FF/亿; OLD market_cap_yi) | 7 concept | 8 turnover_rate (%) |

item6 measured = FF (job 0078). OLD market_cap_yi had no caliber -> free_float_market_cap.

### A4. auction.jjyd.weimai -- GET duanxianxia.com/.../daban.json (AES) -- raw 18 cols
0 code | 1 name | 2 price | 3 latest_change_pct | 4 auction_turnover | 5 auction_change | 6 main_net_inflow | 7 turnover_rate_pct | 8 seal_volume | 9 auction_amount | 10 seal_volume_again | 11 concept | 12 free_float_market_cap (FF; OLD market_cap) | 13 main_net_inflow_full | 14 super_large_order | 15 large_order | 16 board_label | 17 seal_amount_wan

item12 measured = FF (job 0078).

### A5. auction.jjlive.fengdan -- GET ds.../jjlive.json (AES) + qt.gtimg.cn change override
封单/竞价资金阶梯表; NO market-cap field.
section header: section_date / kind / yizi_count / seal_total / t15_total / t20_total / t25_total / has_change_pct
per row: rank / code / name / tag_1 / tag_2 / tag_3 / board_label / amount_915 / amount_920 / amount_925 / latest_change_pct / latest_change_pct_source / tags

OPEN: amount_915/920/925 = 9:15/9:20/9:25 竞价金额阶段累计. Confirm 委买 vs 成交 before aligning with other-agent limitBuyAmountAfter920.

---
## B. Pool family

### B1. pool.hot 热门池 -- POST /data/getFxPoolData/{sort}
0 code | 1 name | 2 change_pct | 6 concept | 8 turnover_amount (成交) | 9 free_float_market_cap (FF; OLD label 流通) | 10 main_net (主力) | 11 real_turnover_rate (实际换手)

- MISLABEL: item9 OLD 流通 is actually FF -> free_float_market_cap.
- DATA LOSS: item7 板态 dropped; this table stores NO raw -> rebuild MUST store raw + keep 板态.

### B2. pool.surge 冲涨池 -- POST /data/getCzPoolData/{sort}
0 code | 1 name | 2 change_pct | 6 concept | 8 turnover_amount | 9 float_market_cap (FLOAT/亿; label CORRECT)

- item9 = FLOAT (correct; the ONLY table in this cluster using 流通市值).
- BUG: turnover_rate is RECOMPUTED = 成交[8]/[9]*100, disagrees with site item[10] (荣昌 8.29 vs 8.9). FIX: take site item[10] directly, or keep both site/derived columns.
- DATA LOSS: item7 板态 dropped (raw IS stored, recoverable).

---
## C. Rank family -- GET x.../vendor/stockdata/hotlist.json
- C1. rank.rocket 飞升榜 -- key skyrocket_hour (OK)
- C2. rank.hot_stock_day 热度榜 -- CRITICAL BUG: code reads data[hot_stock_day], key does not exist; real key = hot_stock_hour -> always 0 rows. FIX: read hot_stock_hour; rename dataset to clarify 热度榜 = hot_stock_hour.

---
## D. Clean tables (verified 2026-06-29, no caliber issue)

### D1. cashflow.stock.{today,3day,5day,10day} -- stock.9fzt.com
排名 / 名称 / 代码 / 股圈 / 最新价 / 涨跌幅 / 主力净流入 / 特大单净流入 / 大单净流入 / 中单净流入 / 小单净流入 (values in 亿). today n=50, others n=150.

### D2. home.kaipan.plate.summary -- duanxianxia.com/web/qxlive (top10 主标签)
主标签序号 / 名称 / 代码 / 板块强度(index) / 板块强度原值 / 主力流入(text) / 主力流入原值 / 主力流入真实金额(元) / 涨停数量 / 子标签数量 / 子标签列表.

### D3. home.qxlive.top_metrics -- qxlive top 12 metric buttons
order / metric_key / metric_label / date / time_point / value / button_display_value / chart_tail_value / compare_value / source_series / display_series / compare_series / button_id / button_text / raw_value / raw_chart_tail_value / raw_compare_value.
metric_key set: QX情绪 / HSLN主力流入 / PB今日封板率 / PBBX沪深5分钟量能 ... (12 total, see REVIEW_METRIC_DEFS).

### D4. home.ztpool 涨停晋级阶梯 -- GET duanxianxia.com/vendor/stockdata/jinjidata.json
Upstream = object with html (a 3-column markdown table string) + date. NOT a positional array.
Parsed via delimiter grammar (REQUIRED, not a fallback):
- col1 阶梯分组: 首板 / 1进2 / 2进3 / ...
- col2 晋级率: 晋级数/样本数=百分比
- col3 stock tokens: <@>{市场} <#'{code}'>{name}（{状态: 成/炸/败}）[{涨幅}] {题材}
canonical: 日期 / 分组序号 / 分组名称 / 组内序号 / 晋级率文本 / 晋级数 / 样本数 / 晋级率 / 市场 / 代码 / 名称 / 状态 / 状态样式 / 涨幅 / 题材. (faithful, no mislabel)

### D5. review.fupan.plate 涨停复盘 -- POST /api/getFupanByYidong
... 封单额 / 成交额 / 换手率 / 实际流通 (FF alias) / 流通市值 (FLOAT) / 总市值 (TOTAL) / 异动原因 / 龙虎榜 ...
Three calibers explicitly present -> this is the GOLDEN anchor table for cross-table caliber joins.

### D6. review.ltgd.range 龙头高度区间涨幅 -- POST /api/getZfByDate
周期(5/10/20/50日) / 板块 / 板块顺序 / 排名 / 代码 / 名称 / 区间涨幅 / 概念 / 概念键 / 日期区间.

### D7. review_daily / review_daily_core11 -- POST /api/getChartByQingxu
Market-breadth metrics: QX情绪 / ZT涨停 / DT跌停 / KQXY开板溢价 / HSLN主力流入 / LBGD连板高度 / SZ / XD / PB今日封板率 / ZTBX / LBBX连板晋级率 / PBBX.

---
## Corrections summary (the rename/fix set)
1. vratio.item2 / qiangchou.item2: auction_volume_ratio -> free_float_market_cap (FF). [~380 refs]
2. vratio.item11: keep volume_ratio (real 量比); ensure factors point here, NOT item2.
3. net_amount.item6: market_cap_yi -> free_float_market_cap (FF).
4. weimai.item12: market_cap -> free_float_market_cap (FF).
5. hot.item9: 流通 -> free_float_market_cap (FF); + store raw; + keep item7 板态.
6. surge.item9: float_market_cap (FLOAT) label OK; FIX turnover = site item10 (stop recompute); keep item7 板态.
7. rank.hot_stock_day: read hot_stock_hour (was reading nonexistent hot_stock_day -> 0 rows). [~699 refs]
8. Downstream flat tables (*_all_candidates_flat.csv, feature_matrix_v21.csv): regenerate from corrected canonical layer, do NOT edit in place.
