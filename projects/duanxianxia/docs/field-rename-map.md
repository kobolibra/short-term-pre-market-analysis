# Field Rename Map + Code-Fix Checklist (implementation bridge)

> Actionable diff from CURRENT code labels -> NEW canonical names, plus the
> parser bugs to fix. Drives the fetcher/loader patch. Source of truth for
> semantics = canonical-field-dictionary.md; decisions = v10-field-alignment-decisions.md.
> All market-cap relative factors standardized on FF (元). Updated 2026-06-29.

## Legend
- caliber: FF = 自由流通市值, FLOAT = 流通市值, TOTAL = 总市值.
- unit: store base in 元 (convert 亿 x1e8, 万 x1e4 at parse).
- ACTION: rename / fix / add.

## 1. vratio (竞价量比表 getVratioData/11) -- positional raw
| raw idx | CURRENT label (wrong) | NEW canonical | caliber/unit | ACTION |
|--|--|--|--|--|
| [2] | auction_volume_ratio | free_float_mktcap | FF / 亿->元 | rename (was MISLABELED as 量比; it is FF mktcap) |
| [3] | seal_amount_wan | seal_amount | 万->元 | rename |
| [4] | (ok) | auction_change_pct | % | keep |
| [5] | (ok) | latest_change_pct | % | keep |
| [6] | auction_turnover_wan | auction_turnover (=bidAmount) | 万->元 | rename |
| [7] | (ok) | concept | text | keep |
| [11] | (ok) | volume_ratio (=volumeRatio) | x | keep -- THIS is the real 量比 |
| [12] | (ok) | turnover_rate | % | keep |

## 2. qiangchou (抢筹表 getQiangchouData/11) -- same as vratio EXCEPT
| [11] | (ok) | grab_strength | x | keep (NOT volume_ratio here) |
(plus [2] same FF rename as vratio.)

## 3. net_amount (竞价主力 jjzhuli, AES) -- positional raw
| [2] auction_change -> auction_change_pct (%) |
| [3] latest_change -> latest_change_pct (%) |
| [4] main_net_inflow_wan -> main_net_inflow (万->元) |
| [5] auction_turnover_wan -> auction_turnover (万->元) |
| [6] market_cap_yi -> free_float_mktcap (FF, 亿->元) |
| [7] -> concept ; [8] -> turnover_rate |

## 4. weimai (委买/打板 daban, AES) -- raw[18]
| [4] -> auction_turnover (元) ; [5] -> auction_change_pct (%) |
| [6] -> main_net ; [7] -> turnover_rate ; [8] -> seal_vol |
| [11] -> concept ; [12] market_cap -> free_float_mktcap (FF) |
| [16] -> board_label ; [17] seal_amount_wan -> seal_amount (万->元) |

## 5. hot (热门池 getFxPoolData) -- item-indexed, NO raw stored
| item9 标 "流通" -> free_float_mktcap | FF | rename (is 自由流通, NOT 流通) |
| item7 板态 | board_state | ADD (currently DROPPED) |
| (whole row) | raw[] | ADD (store positional raw like other datasets) |
| item2 涨幅 -> change_pct ; item6 -> concept ; item8 -> turnover ; item10 -> main_net ; item11 -> real_turnover_rate |

## 6. surge (冲涨池 getCzPoolData) -- item-indexed, raw stored
| item9 float_market_cap -> float_mktcap | FLOAT | keep caliber (this one IS 流通, correct) |
| item10 turnover -> turnover | 元 | FIX: use SITE item10, do NOT recompute |
| item7 板态 | board_state | ADD (currently DROPPED) |
| item2 -> change_pct ; item6 -> concept |

## 7. fengdan (封单 jjlive, AES) -- per-row + section aggregates
| amount_915/920/925 -> limit_bid_amount_915/920/925 | 元 | MONEY = 涨停价委买/封单额 (NOT 成交). limitBuyAmountAfter920 = amount_920. amount_925 `-` => not sealed |
| section_t15/t20/t25_total -> section_bid_amount_t15/t20/t25 | 元 | MONEY totals (NOT counts!) |
| section_seal_total -> section_seal_amount | 元 | money |
| section_yizi_count -> section_yizi_count | count | keep (only real count) |

## 8. ztpool (jinjidata, HTML-token JSON) -- positional, NO raw
- Keep token grammar parse; expose per-stock status 成/炸/败 -> seal_status; 阶梯 -> ladder; 晶级率 -> promo_rate. (origin booleans derive from 状态.)
- NOTE: source_url stored malformed with literal " + " -> fix string build.

## 9. review.fupan.plate (getFupanByYidong) -- EOD
- 开板 -> prev_open_num (=prevOpenNum); 连板 -> streak; 涨停类型 -> limit_up_type; 首次/最后封板 -> first/last_seal_time; 实际流通/流通市值/总市值 -> float_actual/float_mktcap/total_mktcap (caliber anchor table).

## 10. CODE BUGS to fix (fetcher.py)
1. **hotlist_day**: reads nonexistent key `hot_stock_day` -> real key is `hot_stock_hour` -> currently returns 0 rows. FIX key.
2. vratio/qiangchou item[2] mislabeled `auction_volume_ratio` (it is FF mktcap) -> see §1-2.
3. hot item9 流通 mislabel + item7 板态 dropped + no raw -> see §5.
4. surge turnover RECOMPUTED instead of site item10 + item7 板态 dropped -> see §6.
5. ztpool source_url malformed (literal " + ") -> fix.

## 11. Derived factors to ADD (post-rename)
- origin: fromPrevBrokenLimitUp / fromPrevSealedLimitUpWithOpen (from ztpool 状态 + fupan 开板).
- themeConsistency = count(H)/count(Q); themeConcentration = themeBidAmount/Σall.
- auctionSealAmount (fengdan section_bid_amount_t25 / section_seal_amount).
- marketSealRate (QX-live PB) + prevDayLimitUpSealRate (num/hist).
- stockMainlineFit (concept vs kaipan top 板块强度).

## 12. Migration rule
- Re-derive canonical rows from `raw` for every capture that has it (vratio/qiangchou/surge/net_amount/weimai). Do NOT in-place sed historical JSON.
- hot has NO historical raw -> tag legacy_unrecoverable; store raw going forward.
- Regenerate flat feature CSVs from the canonical layer.
