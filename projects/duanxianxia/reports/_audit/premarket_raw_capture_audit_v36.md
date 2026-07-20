# 全量盘前原始数据审计 v36 (job 0045)

- 生成: 2026-06-27T07:51:15
- 有效交易日: **22** ｜样本: 7472 ｜数据源: 原始 captures (9 个盘前数据集)

## 1. 第一性原理特征横截面 IC (按 |IC| 排序)

| 特征 | mean_ic | icir | 覆盖率 | n_days |
|---|---|---|---|---|
| rocket_rank | 0.2219 | 1.027 | 0.231 | 21 |
| seal_to_mcap_ratio | 0.1225 | 0.318 | 0.024 | 12 |
| big_order_share | 0.1109 | 0.514 | 0.201 | 18 |
| turnover_rate_pct | 0.0521 | 0.335 | 0.364 | 18 |
| net_inflow_pressure | -0.0424 | -0.227 | 0.146 | 18 |
| hot_rank | 0.0337 | 0.328 | 0.279 | 21 |
| turnover_intensity | -0.0302 | -0.176 | 0.105 | 18 |
| auction_amt_growth | -0.0291 | -0.271 | 0.183 | 16 |
| volume_ratio_multiple | -0.0282 | -0.264 | 0.183 | 16 |
| wm_net_pressure | -0.0273 | -0.363 | 0.341 | 17 |
| grab_strength | 0.0271 | 0.15 | 0.091 | 16 |
| main_net_inflow_wan | 0.0017 | 0.015 | 0.146 | 18 |
| fengdan_build_slope | None | None | 0.0 | 0 |
| fengdan_late_change | None | None | 0.0 | 0 |
| fengdan_925_wan | None | None | 0.0 | 0 |
| weimai_to_seal_ratio | None | None | 0.0 | 0 |

## 2. 每个下载数据集的实际 schema (字段覆盖率)

### rank.rocket — 2012 rows
字段: rank(1.0), code(1.0), name(1.0), value(1.0), raw_rate(1.0)
样本: {"rank": "1", "code": "002208", "name": "合肥城建", "value": "+75w", "raw_rate": "746056"}

### rank.hot_stock_day — 2100 rows
字段: rank(1.0), code(1.0), name(1.0), value(1.0), raw_rate(1.0)
样本: {"rank": "1", "code": "601991", "name": "大唐发电", "value": "5460w", "raw_rate": "54598574"}

### auction.jjyd.vratio — 1659 rows
字段: rank(1.0), code(1.0), name(1.0), auction_volume_ratio(1.0), auction_change_pct(1.0), latest_change_pct(1.0), auction_turnover_wan(1.0), auction_change_pct_text(1.0), auction_turnover_wan_text(1.0), yesterday_auction_turnover_wan(1.0), volume_ratio_multiple(1.0), turnover_rate_pct(1.0), raw(1.0), concept(0.946), seal_amount_wan(0.044)
样本: {"rank": "1", "code": "688623", "name": "双元科技", "auction_volume_ratio": "19", "seal_amount_wan": "None", "auction_change_pct": "none", "latest_change_pct": "4.70", "auction_turnover_wan": "876", "concept": "专精特新", "auction_change_pct_text": "4.70", "auction_turnover_wan_text": "876", "yesterday_auction_turnover_wan": "10", "volume_ratio_multiple": "87.6", "turnover_rate_pct": "0.45", "raw": "['688

### auction.jjyd.qiangchou — 810 rows
字段: group(1.0), rank(1.0), code(1.0), name(1.0), auction_volume_ratio(1.0), auction_change_pct(1.0), latest_change_pct(1.0), auction_turnover_wan(1.0), auction_change_pct_text(1.0), auction_turnover_wan_text(1.0), grab_strength(1.0), turnover_rate_pct(1.0), raw(1.0), concept(0.886), seal_amount_wan(0.032)
样本: {"group": "grab", "rank": "1", "code": "300649", "name": "杭州园林", "auction_volume_ratio": "21", "seal_amount_wan": "None", "auction_change_pct": "none", "latest_change_pct": "1.68", "auction_turnover_wan": "224", "concept": "RWA", "auction_change_pct_text": "1.68", "auction_turnover_wan_text": "224", "yesterday_auction_turnover_wan": "None", "grab_strength": "9.80", "turnover_rate_pct": "0.11", "ra

### auction.jjyd.net_amount — 1105 rows
字段: rank(1.0), code(1.0), name(1.0), auction_change_pct(1.0), latest_change_pct(1.0), main_net_inflow_wan(1.0), auction_turnover_wan(1.0), market_cap_yi(1.0), concept(1.0), turnover_rate_pct(1.0), concept_1(1.0), raw(1.0), concept_2(0.999)
样本: {"rank": "1", "code": "603773", "name": "沃格光电", "auction_change_pct": "-9.04", "latest_change_pct": "-9.04", "main_net_inflow_wan": "38830", "auction_turnover_wan": "91922", "market_cap_yi": "92.5", "concept": "玻璃基板|钙钛矿电池", "turnover_rate_pct": "9.93", "concept_1": "玻璃基板", "concept_2": "钙钛矿电池", "raw": "['603773', '沃格光电', -9.04, -9.04, 38830, "}

### auction.jjlive.fengdan — 1542 rows
字段: section_date(1.0), section_kind(1.0), section_yizi_count(1.0), section_seal_total(1.0), section_t15_total(1.0), section_t20_total(1.0), section_t25_total(1.0), section_has_change_pct(1.0), rank(1.0), code(1.0), name(1.0), latest_change_pct(1.0), latest_change_pct_source(1.0), tags(1.0), amount_915(0.986), tag_1(0.97), board_label(0.49), tag_2(0.479), amount_920(0.119), amount_925(0.068)
样本: {"section_date": "2026-05-19", "section_kind": "live", "section_yizi_count": "5", "section_seal_total": "58.2亿", "section_t15_total": "233.9亿", "section_t20_total": "52.7亿", "section_t25_total": "58.2亿", "section_has_change_pct": "True", "rank": "1", "code": "603779", "name": "威龙股份", "tag_1": "酿酒", "tag_2": "", "tag_3": "", "board_label": "5板", "amount_915": "121.7亿", "amount_920": "37.4亿", "amoun

### auction.jjyd.weimai — 2691 rows
字段: rank(1.0), code(1.0), name(1.0), price(1.0), latest_change_pct(1.0), latest_change_pct_text(1.0), auction_turnover(1.0), auction_turnover_wan(1.0), auction_turnover_text(1.0), auction_change_pct(1.0), auction_change_pct_text(1.0), main_net_inflow(1.0), main_net_inflow_wan(1.0), main_net_inflow_text(1.0), turnover_rate_pct(1.0), turnover_rate_pct_text(1.0), seal_volume(1.0), auction_amount(1.0), auction_amount_wan(1.0), seal_volume_again(1.0), market_cap(1.0), market_cap_yi(1.0), market_cap_text(1.0), main_net_inflow_full(1.0)
样本: {"rank": "1", "code": "603779", "name": "威龙股份", "price": "12", "latest_change_pct": "9.99", "latest_change_pct_text": "9.99%", "auction_turnover": "3782282400", "auction_turnover_wan": "378228.24", "auction_turnover_text": "37.8亿", "auction_change_pct": "none", "auction_change_pct_text": "none%", "main_net_inflow": "7186800", "main_net_inflow_wan": "718.68", "main_net_inflow_text": "719万", "turnov

### home.kaipan.plate.summary — 180 rows
字段: 主标签序号(1.0), 主标签名称(1.0), 主标签代码(1.0), 板块强度(1.0), 板块强度原值(1.0), 主力流入(1.0), 主力流入原值(1.0), 主力流入真实金额(1.0), 涨停数量(1.0), 子标签数量(1.0), 子标签列表(0.411)
样本: {"主标签序号": "1", "主标签名称": "机器人", "主标签代码": "801159", "板块强度": "2060", "板块强度原值": "2060", "主力流入": "0万", "主力流入原值": "0", "主力流入真实金额": "0.0", "涨停数量": "3", "子标签数量": "10", "子标签列表": "宇树机器人、滚珠丝杠、灵巧手、外骨骼机器人、小米机器人、小鹏机器人、众擎机器人、"}

### home.qxlive.top_metrics — 216 rows
字段: order(1.0), metric_key(1.0), metric_label(1.0), date(1.0), time_point(1.0), chart_tail_value(1.0), source_series(1.0), display_series(1.0), button_id(1.0), button_text(1.0), raw_chart_tail_value(1.0), value(0.926), raw_value(0.926), button_display_value(0.611), compare_value(0.083), compare_series(0.083), raw_compare_value(0.083)
样本: {"order": "1", "metric_key": "QX", "metric_label": "情绪指标", "date": "2026-05-19", "time_point": "09:28", "value": "32", "button_display_value": "32", "chart_tail_value": "32", "compare_value": "", "source_series": "QX", "display_series": "QX", "compare_series": "", "button_id": "QX_btn", "button_text": "情绪指标：32", "raw_value": "32", "raw_chart_tail_value": "32", "raw_compare_value": ""}

> 谨慎: 15 天小样本, 纯描述性。几何意义特征须后续 walk-forward 复验。
> 覆盖率低的特征 = 该原始字段在 capture 里键名与此脚本猜的不一致, 下轮按 schema 表纠正键名重跑。