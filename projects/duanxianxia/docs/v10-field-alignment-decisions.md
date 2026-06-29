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
- = fengdan **amount_920** (9:20 不可撤单阶段后的涨停价委买/封单额). Proven NOT 成交额 by non-monotonic 9:15->9:20 drop (成交额 不可能下降; 委买额 9:20 前可撤单 -> 会跌).
- amount_925 = 9:25 final seal amount; value `-` = not sealed at 9:25 (竞价开板).
- Parse 亿/万 display strings to 元 at canonical layer.

## 3. Limit-up seal rate: TWO distinct features (DO NOT conflate)
### 3a. prevDayLimitUpSealRate (EOD, T-1)
```
sealedLimitUp = num         # 收盘仍封住数
touchedLimitUp = hist = num + open   # 曾触及 = 封住 + 炸板/开板 (verified identity: 64+42=106)
prevDayLimitUpSealRate = sealedLimitUp / touchedLimitUp
```
- Market-level; premarket uses the PREVIOUS trading day value.

### 3b. auctionLimitUpSealRate (today 9:25, T0)
- Premarket-auction seal breadth at 9:25 (集合竞价结束).
- numerator = 9:25 竞价封住数 (fengdan section_t25_total / 一字+竞价封板); denominator = 竞价曾冲板数 (fengdan 封单池行数 / section_seal_total).
- Available at 9:25, inside loader T0 window (<=09:29): legitimate same-day feature, NOT lookahead; explains only post-9:25.
- COMPLEMENTARY to prevDay (yesterday regime vs today opening strength). Keep as SEPARATE named features; never substitute one for the other.
- PENDING job 0083: confirm whether section_t25_total is cumulative vs time-point, and denominator completeness (does fengdan pool include 竞价曾冲板但 9:25 未封).

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
- auctionLimitUpSealRate exact numerator/denominator -> job 0083 (numeric confirmation imminent).
- minBidAmount / auctionChgMin thresholds -> OUR tuning choice (not other-agent default).
- Everything else is FINAL.
