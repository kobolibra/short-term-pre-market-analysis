# v10 Field Alignment Decisions (other-agent framework) -- FINAL

> Locked decisions reconciling the other-agent factor framework with duanxianxia true calibers.
> Companion to canonical-field-dictionary.md and rebuild-design-v10.md.
> Verified via jobs 0077-0084 + live endpoint probes. Updated 2026-06-29. All field semantics FINAL.

## 1. Locked global decisions
- **Market-cap caliber for ALL market-cap-relative factors = FF (自由流通市值).** circMcap is treated as FF on our side; the other agent has been informed that bidStrength denominator is effectively free-float. Reason: FF is the only caliber with full per-stock coverage at premarket time.
- **Base-layer units = 元** (amounts + market cap); display layer converts to 亿. No mixed units at base.
- **bidAmount = 集合竞价成交额 -> auction_turnover.** Distinct from limitBuyAmountAfter920 (委买/封单).
- **volumeRatio = vratio raw item11 (volume_ratio_multiple), NOT the mislabeled item2 (=FF).**

## 2. Per-field mapping (FINAL)
| other-agent field | our source | status |
|---|---|---|
| changeRate | auction_change_pct (竞价场景) / latest_change_pct | OK |
| bidStrength = bidAmount/circMcap x10000 | auction_turnover / FF x10000 (both 元) | OK |
| volumeRatio | vratio item11 | OK |
| limitBuyAmountAfter920 | fengdan amount_920 | RESOLVED (0082) |
| prevStreak | fupan 连板 / ztpool 阶梯 | OK |
| prevOpenNum | fupan 开板 | RESOLVED (0082) |
| origin | derived booleans (see 5) | DERIVED |
| brokenLimitUp | ztpool 状态=炸 / review DT | OK |
| sentimentSignal | QX-live 情绪指标 QX / review QX | OK |
| themes | concept / 题材 / 子标签列表 | OK |

### limitBuyAmountAfter920 (RESOLVED, job 0082)
- = fengdan **amount_920** (9:20 不可撤单阶段后的涨停价委买/封单额). Proven NOT 成交额 by non-monotonic 9:15->9:20 drop. Reconfirmed at aggregate level by 0083 (section_t15=150.4亿 > t20=39.1亿).
- amount_925 = 9:25 final seal amount; value `-` = not sealed at 9:25. **Default to amount_920** (first non-cancellable snapshot).

## 3. Limit-up seal rate: distinct features (DO NOT conflate)
### 3a. prevDayLimitUpSealRate (EOD, T-1) -- count-based, premarket-safe
```
sealedLimitUp = num         # 收盘仍封住数
touchedLimitUp = hist = num + open   # 曾触及 = 封住 + 炸板/开板 (verified: 64+42=106)
prevDayLimitUpSealRate = sealedLimitUp / touchedLimitUp
```
- Market-level; premarket uses the PREVIOUS trading day value. Always safe (T-1).

### 3b. Today seal features (T0, ~9:25) -- per jobs 0083 + 0084 (FINAL)
**(i) auctionSealAmount (fengdan, 委买/封单资金强度 -- NOT a count rate).** 0083: fengdan section_* are MONEY (t15=150.4亿 > t20=39.1亿 > t25=54.4亿=seal_total); only yizi_count(5) is a count. => `auctionSealAmount = section_t25_total / section_seal_total` (or raw 总量). Available at 9:25, within T0.

**(ii) marketSealRate = QX-live `PB` 今日封板率 -- count-based market seal rate (THE "类似 limitUpSealRate", premarket).** CONFIRMED present (0084): metric_key=PB, value e.g. 63.0% (= market sealed/touched). Captured together with the other QX-live metrics in the **~9:25 premarket pull** (an occasional later stamp such as 10:04 is a fetch-scheduling discrepancy, NOT a design issue -> pin the QX-live fetch to the auction window). **Treated as premarket-safe.** FINAL.
- prevDayLimitUpSealRate (3a, T-1) and marketSealRate (3b-ii, ~9:25) are complementary; keep BOTH as separate named features. Never substitute one for the other.

## 4. Theme metrics: THREE distinct fields (per other agent; do not mix)
### 4a. themeConsistency  (= other agent consistency: 题材内部高开一致性)
```
M(theme) = members whose concept == theme              # from 竞价五表 concept
Q(theme) = { i in M | bidAmount_i >= minBidAmount, exclude ST }   # bidAmount = auction_turnover
H(theme) = { i in Q | auctionChg_i > 0 }                # auctionChg = auction_change_pct (高开)
themeConsistency(theme) = count(H) / count(Q)
```
- Strict variant (OUR threshold, label as variant): replace `auctionChg_i > 0` with `auctionChg_i >= auctionChgMin`.
- Data: vratio/qiangchou/net_amount/weimai give per-stock concept + auction_turnover + auction_change_pct. Available.

### 4b. themeConcentration  (资金集中度)
```
themeBidAmount(theme) = sum of bidAmount over members of theme
themeConcentration(theme) = themeBidAmount(theme) / sum over ALL themes of themeBidAmount
```
- Data: group 竞价五表 by concept, sum auction_turnover.

### 4c. stockMainlineFit  (个股 vs 当日主线; SEPARATE, NOT consistency)
- 当日主线 = top 板块 by 板块强度 (kaipan.plate.summary).
- stockMainlineFit(stock) = graded match of stock.concept against top-N strongest 板块. Use for per-stock scoring, NOT a theme metric.

## 5. origin (derived candidate-source label)
```
fromPrevBrokenLimitUp        = (prev-day 状态 in {炸, 败})            # ztpool/jinjidata 状态
fromPrevSealedLimitUpWithOpen = (prev-day 状态 == 成) AND (fupan 开板 > 0)
```

## 6. Remaining open (minor tuning only -- all field semantics FINAL)
- minBidAmount / auctionChgMin thresholds -> OUR tuning choice.
- Operational: pin the QX-live fetch to the ~9:25 auction window (avoid the occasional post-open stamp).
