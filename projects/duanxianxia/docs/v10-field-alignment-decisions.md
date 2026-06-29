# v10 Field Alignment Decisions (other-agent framework) -- FINAL

> Locked decisions reconciling the other-agent factor framework with duanxianxia true calibers.
> Companion to canonical-field-dictionary.md and rebuild-design-v10.md.
> Verified via jobs 0077-0083 + live endpoint probes. Updated 2026-06-29.

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
| sentimentSignal | review QX 情绪 | OK |
| themes | concept / 题材 / 子标签列表 | OK |

### limitBuyAmountAfter920 (RESOLVED, job 0082)
- = fengdan **amount_920** (9:20 不可撤单阶段后的涨停价委买/封单额). Proven NOT 成交额 by non-monotonic 9:15->9:20 drop (成交额 不可能下降; 委买额 9:20 前可撤单 -> 会跌). Reconfirmed at aggregate level by 0083 (section_t15=150.4亿 > t20=39.1亿).
- amount_925 = 9:25 final seal amount; value `-` = not sealed at 9:25 (竞价开板).
- Parse 亿/万 display strings to 元 at canonical layer.

## 3. Limit-up seal rate: TWO distinct features (DO NOT conflate)
### 3a. prevDayLimitUpSealRate (EOD, T-1)
```
sealedLimitUp = num         # 收盘仍封住数
touchedLimitUp = hist = num + open   # 曾触及 = 封住 + 炸板/开板 (verified identity: 64+42=106)
prevDayLimitUpSealRate = sealedLimitUp / touchedLimitUp
```
- Market-level; premarket uses the PREVIOUS trading day value. Source: market-day limit-up breadth (num/hist/open).

### 3b. Premarket-auction seal feature (today 9:25, T0) -- REVISED per job 0083
**0083 finding (2026-06-29 fengdan): the section aggregates are MONEY amounts (委买/封单额), NOT seal counts:**
```
section_t15_total  = 150.4亿   # 9:15 涨停价委买额总量
section_t20_total  = 39.1亿    # 9:20 (可撤单后回落)
section_t25_total  = 54.4亿    # 9:25 final  (= section_seal_total)
section_seal_total = 54.4亿
section_yizi_count = 5          # the ONLY count (一字/竞价封住数)
```
- Same non-monotonic 9:15 > 9:20 drop -> reconfirms 委买 (cancellable before 9:20).
- fengdan 110-row pool is a MIXED watchlist (5 首板 sealed + 昨X板 tracked + 71 blank), NOT a clean "竞价曾冲板" set. amount_925 non-null = 5 (= yizi_count) = sealed@9:25; 105 are `-`.
- **=> fengdan yields `auctionSealAmount` (竞价封单资金强度 = section_t25_total / section_seal_total, or raw 总量), NOT a clean count-based seal RATE.** This is a legitimate T0 (<=09:29) same-day strength feature, complementary to prevDay; never substitute for prevDay.
- **OPEN: a count-based 盘前竞价封板率 (sealedCount / touchedCount) denominator is NOT in fengdan.** Candidate true source = QX-live "今日封板率" (PB) sampled at 9:25 (live-updating metric) -- TO VERIFY whether PB updates during the auction. Decision pending (see section 6).

## 4. Theme metrics: THREE distinct fields (per other agent; do not mix)
### 4a. themeConsistency  (= other agent consistency: 题材内部高开一致性)
```
M(theme) = members whose concept == theme              # from 竞价五表 concept
Q(theme) = { i in M | bidAmount_i >= minBidAmount, exclude ST }   # bidAmount = auction_turnover
H(theme) = { i in Q | auctionChg_i > 0 }                # auctionChg = auction_change_pct (竞价涨幅 = 高开)
themeConsistency(theme) = count(H) / count(Q)
```
- Strict variant (OUR threshold, label as variant, NOT the default semantic): replace `auctionChg_i > 0` with `auctionChg_i >= auctionChgMin`.
- Data: vratio/qiangchou/net_amount/weimai give per-stock concept + auction_turnover + auction_change_pct. Available.

### 4b. themeConcentration  (资金集中度)
```
themeBidAmount(theme) = sum of bidAmount over members of theme
themeConcentration(theme) = themeBidAmount(theme) / sum over ALL themes of themeBidAmount
```
- Data: group 竞价五表 by concept, sum auction_turnover. (Alt: kaipan 板块强度/主力流入 aggregates.)

### 4c. stockMainlineFit  (个股 vs 当日主线; SEPARATE, NOT consistency)
- 当日主线 = top 板块 by 板块强度 (kaipan.plate.summary).
- stockMainlineFit(stock) = graded match of stock.concept against top-N strongest 板块 (binary in/out, or 板块强度-weighted).
- Data: kaipan.plate.summary (板块强度 + 子标签列表) + per-stock concept. Available. Use for per-stock scoring, NOT as a theme-level metric.

## 5. origin (derived candidate-source label)
```
fromPrevBrokenLimitUp        = (prev-day 状态 in {炸, 败})            # ztpool/jinjidata 状态
fromPrevSealedLimitUpWithOpen = (prev-day 状态 == 成) AND (fupan 开板 > 0)
```

## 6. Remaining open
- **auctionLimitUpSealRate (count-based) source** -- not in fengdan (0083). Need to confirm QX-live 今日封板率 (PB) has a 9:25 auction sample, OR adopt `auctionSealAmount` (money-strength) instead. AWAITING user/other-agent decision.
- minBidAmount / auctionChgMin thresholds -> OUR tuning choice (not other-agent default).
- limitBuyAmountAfter920: confirm amount_920 vs amount_925 (920 = first non-cancellable snapshot; 925 = final seal).
- Everything else is FINAL.
