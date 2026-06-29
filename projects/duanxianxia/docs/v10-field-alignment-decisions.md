# v10 Field Alignment Decisions (other-agent framework)

> Locked decisions reconciling the other agent factor framework with duanxianxia true calibers.
> Companion to canonical-field-dictionary.md and rebuild-design-v10.md. Date 2026-06-29.

## Locked global decisions
- **Market-cap caliber for ALL market-cap-relative factors = FF (自由流通市值).** circMcap in the other-agent formulas is treated as FF on our side; the other agent has been informed that bidStrength denominator is effectively free-float. (User decision, 2026-06-29.) Note: this overrides the other agent stated preference for FLOAT, chosen because FF is the only caliber with full per-stock coverage at premarket time.
- **Units at canonical/base layer = 元** for amounts (bidAmount, limit-buy amounts) and market cap; display layer converts to 亿. No mixed units at base.
- **bidAmount = 集合竞价成交额 -> auction_turnover.** Distinct from limitBuyAmountAfter920 (委买/封单).
- **volumeRatio = vratio raw item11 (volume_ratio_multiple), NOT the mislabeled item2 (=FF).**

## Per-field mapping
| other-agent field | our source | status | note |
|---|---|---|---|
| changeRate | auction_change_pct / latest_change_pct | OK | pick time-point: 竞价场景用 auction_change |
| bidStrength = bidAmount/circMcap x10000 | auction_turnover / FF_market_cap x10000 | OK | both in 元; FF caliber (locked) |
| volumeRatio | vratio item11 | OK | real 量比 |
| limitUpSealRate | market-day num/hist (hist = num + open) | OK (market-level) | sealed=num, touched=hist; premarket use prevDayLimitUpSealRate; intraday version = intradayLimitUpSealRate |
| limitBuyAmountAfter920 | fengdan amount_920/925 OR seal_amount_wan | PENDING | await job 0082: 成交额 vs 委买/封单额. If 成交额, leave empty (no hard substitution per other agent). |
| prevStreak | fupan 连板 / ztpool 阶梯分组 | OK | |
| prevOpenNum | fupan 开板 | OK | 0 = 没开板; per-stock open count (0082 confirms distribution) |
| origin | DERIVED: fromPrevBrokenLimitUp (昨状态炸/败), fromPrevSealedLimitUpWithOpen (昨成 且 开板>0) | DERIVED | from ztpool 状态 + fupan 开板 |
| brokenLimitUp | ztpool 状态=炸 / review DT | OK | |
| sentimentSignal | review QX 情绪 | OK | |
| themes | concept / 题材 / 子标签列表 (multi tables) | OK | rich; stock-level + theme-level (kaipan 板块强度, fupan 题材涨停数) |
| consistency | DERIVED (definition pending other agent) | OPEN | 3 candidate formulas; need definition |

## Open / pending
1. **limitBuyAmountAfter920 caliber** -- job 0082 (成交 vs 委买/封单). If our amount_91x are turnover, prefer seal_amount_wan only if it is a 9:20-after seal amount; else leave factor empty.
2. **consistency** -- exact definition from other agent: (a) 个股题材 vs 当日最强题材吻合度, (b) 当日题材集中度, or (c) 个股多标签内部一致性. All 3 derivable from our data; formula differs.
