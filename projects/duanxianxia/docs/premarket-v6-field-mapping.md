# v6 Premarket Scoring — Field Mapping (from real 2026-04-23 captures)

**Status:** Audit baseline. Before touching `scripts/duanxianxia_premarket_v6.py`
or `projects/duanxianxia/config/premarket_scoring.yaml`, verify every field
read against this table.

**Source of truth:** `samples/2026-04-23/*.json` (committed `31ff0a1`).
Upstream webpage: <https://duanxianxia.com/web/main> and sub-pages.

---

## 1. `home.qxlive.top_metrics` — 开盘前情绪指标

UI path: `/web/qxlive` 顶部 17 个指标卡片。

Row shape:
```json
{
  "metric_key": "HSLN",            // UPPERCASE
  "metric_label": "量能",
  "value": "-331亿",               // may be string with CN unit, or bare number
  "button_display_value": "...",
  "chart_tail_value": "...",
  "raw_chart_tail_value": -331,    // BEST numeric source when present
  "date": "2026-04-23"
}
```

### Metric keys (confirmed from real sample)

| key    | label        | value unit/shape            | regime signal                          |
|--------|--------------|-----------------------------|----------------------------------------|
| `QX`   | 情绪指标     | `"40"` → number (0–100)     | ≤35 cold, ≥65 hot                      |
| `ZT`   | 涨停家数     | `"50"` → int                | informational                          |
| `DT`   | 跌停家数     | `"15"` → int                | ≥20 cold                               |
| `KQXY` | 亏钱效应     | `"5"` → number              | higher = colder; ≥10 cold, ≤0 hot      |
| `HSLN` | 量能         | premarket `"-331亿"` (day-over-day Δ in 亿) / postmarket `"28045"` bare | ≤-200亿 cold, ≥+200亿 hot |
| `LBGD` | 连板高度     | `"5"` → int                 | ≤3 cold, ≥7 hot                        |
| `SZ`   | 上涨家数     | `"1304"` → int              | `SZ / (SZ+XD)` → 涨跌比                |
| `XD`   | 下跌家数     | `"3797"` → int              | 同上                                   |
| `PB`   | 今日封板率   | `71.4` or `"65.7%"` → %     | ≤40 cold, ≥70 hot                      |
| `ZTBX` | **昨涨停表现** | `"1.78"` or `"2.17%"` → %  | ≤0 cold, ≥3 hot (NOT "晋级率")          |
| `LBBX` | **昨连板表现** | `"8.23"` or `"7.17%"` → %  | ≤0 cold, ≥5 hot                        |
| `PBBX` | 连板晋级率   | `"50.0%"` string            | ≤40 cold, ≥60 hot                      |

### PBBX — nested structure (only in postmarket `review.daily.top_metrics`)

```json
{
  "metric_key": "PBBX",
  "value": "50.0%",
  "metric_group": "连板晋级率",
  "metric_category": "总体",
  "raw_value": "5:45",     // zt_today : zt_yesterday
  "ratio": "4/8",           // jinji_count / sample_count
  "jinji_count": 4,
  "sample_count": 8
}
```

With sibling rows `PBBX_TOP` (最高板晋级率), `PBBX_1_2` (1进2), `PBBX_2_3`,
`PBBX_3_4`, `PBBX_4P` — each with its own `ratio`/`jinji_count`/`sample_count`.

### v6 bugs to fix here

- `_classify_regime` reads `current_value` → **field does not exist**. Use
  `raw_chart_tail_value` (best) → `value` (fallback). Strip `亿`/`%`/`万` before float.
- YAML `ZTBX_max: 15` assumes it's a 0-100 晋级率. It's actually a % return
  usually in range −10 … +10. Retune.
- `HSLN` in premarket is **signed day-over-day delta in 亿** (e.g. `-331亿` = 今日比昨日缩量 331 亿).
  In postmarket it's a **bare total turnover number** (e.g. `28045` in 万元).
  Handle both.

---

## 2. `home.kaipan.plate.summary` — 全主标签板块强度

UI path: `/web/qxlive` 右侧「全主标签」表 + 子标签抽屉。

Row shape (all keys Chinese):
```json
{
  "主标签序号": 1,
  "主标签名称": "一季报增长",
  "主标签代码": "801571",
  "板块强度": "4980",           // string
  "板块强度原值": "4980",
  "主力流入": "6.3亿",            // display string
  "主力流入原值": "62753",       // STRING, UNIT: 万
  "主力流入真实金额": 627530000.0, // FLOAT, UNIT: 元
  "涨停数量": "2",               // STRING
  "子标签数量": 0,
  "子标签列表": "光刻胶、磷化铟、..."  // pipe/comma separated when non-empty; often ""
}
```

### Richer nested structure in `meta.top_plates[i].subplates[j]`

```json
{
  "子题材序号": 1,
  "子题材名称": "光刻胶",
  "子题材代码": "801222",
  "top_plate_name": "芯片",
  "top_plate_code": "801001"
}
```

**Prefer the structured `meta.top_plates[].subplates[]` over splitting `子标签列表`.**
`meta.subplates` is a flat denormalized copy across all top_plates.

### v6 bugs to fix here

- `_build_theme_catalog` reads `main_plate_name` / `plate_strength` /
  `main_inflow_wan` / `sub_plate_list` → **none exist**. All Chinese.
- Inflow unit: use `主力流入原值` (wan) or `主力流入真实金额 / 10000` (wan). YAML
  thresholds currently in wan — keep them but read the right field.
- Subplate position: use `子题材序号` directly; stop splitting strings.

---

## 3. `auction.jjyd.vratio` — 竞价爆量

UI: `/web/jjyd` 「竞价异动」 → 「竞价爆量」 tab.

```json
{
  "rank": 1, "code": "300721", "name": "怡达股份",
  "auction_volume_ratio": 17,                    // INT
  "seal_amount_wan": 12542,                      // INT or null
  "auction_change_pct": 20.01,                   // FLOAT
  "latest_change_pct": "20.01",                  // STRING float
  "auction_turnover_wan": "6344",                // STRING int
  "concept": "环氧丙烷",                         // single concept
  "yesterday_auction_turnover_wan": "18",        // STRING int
  "volume_ratio_multiple": "352.4",              // STRING float (key signal!)
  "turnover_rate_pct": 3.84                      // FLOAT
}
```

No `section_kind`. Entire table = live auction snapshot at fetch time.

### v6 bugs

- v6 assumed `volume_ratio_multiple` / `auction_turnover_wan` are numeric;
  must `float(str)` them. Strip `%` if encountered.
- Concept is a **single string**, not pipe-separated like net_amount.

---

## 4. `auction.jjyd.qiangchou` — 竞价抢筹

UI: `/web/jjyd` 「竞价异动」 → 「竞价抢筹」 tab.

Same columns as vratio, plus:
- `group`: **`"grab"`** (v6 code checking `=="qiangchou"` will never match!)
- `grab_strength`: STRING float like `"7.10"` (v6 key signal)
- `yesterday_auction_turnover_wan`: always `null` here (unlike vratio)

---

## 5. `auction.jjyd.net_amount` — 竞价净额

UI: `/web/jjyd` 「竞价异动」 → 「竞价净额」 tab.

```json
{
  "rank": 1, "code": "002428", "name": "云南锗业",
  "auction_change_pct": 1.4,                // FLOAT
  "latest_change_pct": 9.99,                // FLOAT (not string here)
  "main_net_inflow_wan": 9747,              // INT, unit 万
  "auction_turnover_wan": 28461,            // INT, unit 万
  "market_cap_yi": 432.3,                   // FLOAT, unit 亿
  "concept": "磷化铟|金属锗",                // PIPE-separated concepts
  "turnover_rate_pct": 0.66,
  "concept_1": "磷化铟",
  "concept_2": "金属锗"
}
```

### Usable for v6 as-is (field names match!). But:
- To match themes, split `concept` by `|` OR use `concept_1` / `concept_2`.
- `market_cap_yi` can serve as the **liquidity filter** (e.g. `< 50亿` = 小盘).

---

## 6. `auction.jjlive.fengdan` — 竞价封单

UI: `/web/jjlive` 「竞价封单」 → 「当日封单」 section.

```json
{
  "section_date": "2026-04-23",
  "section_kind": "live",                   // filter key ✓
  "section_yizi_count": 8,
  "section_seal_total": "45.8亿",          // STRING, 单位亿/万/元
  "section_t15_total": "119.9亿",          // 9:15 aggregate
  "section_t20_total": "32.1亿",
  "section_t25_total": "45.8亿",
  "rank": 1, "code": "300067", "name": "安诺其",
  "tag_1": "并购重组", "tag_2": "算力", "tag_3": "",
  "board_label": "3板",                     // 首板/2板/3板/昨首板/昨2板/昨4板/""
  "amount_915": "32.3亿",                   // STRING CN-unit or "-"
  "amount_920": "14.4亿",
  "amount_925": "14.6亿",
  "latest_change_pct": "19.97%",           // STRING with %
  "tags": ["并购重组", "算力", "3板"]       // array, may include board_label
}
```

### v6 bugs

- `amount_915/920/925` must go through a `_parse_cn_amount()` → wan/yi converter.
  Handle `"-"` → null.
- `latest_change_pct` strip `%` → float.
- Use `board_label` to filter 首板/连板 instead of parsing tags.

---

## 7. `review.daily.top_metrics` — 每日复盘情绪（昨日收盘后）

Same schema as qxlive `home.qxlive.top_metrics`, with 2 differences:

1. `HSLN` value is bare number (`"28045"`), not `"-331亿"`.
2. `PBBX` row has extra `raw_value`/`ratio`/`jinji_count`/`sample_count` +
   sibling `PBBX_TOP` / `PBBX_1_2` / `PBBX_2_3` / `PBBX_3_4` / `PBBX_4P` rows.
   Use these for **tiered 晋级率** (高度板更稀缺，权重更高）.

### v6 bugs
- `_evaluate_yesterday_signals` only reads top-level `value`. Extend to read
  `jinji_count`/`sample_count` for信度加权, and pull `PBBX_1_2` / `PBBX_TOP`
  as separate features.

---

## 8. `review.fupan.plate` — 涨停复盘（按概念）

UI: `/web/fupan` 「涨停复盘（按概念）」 tab.

```json
{
  "日期": "2026-04-23",
  "题材序号": 1,
  "题材名称": "电力",
  "题材说明": "工信部正在开展算电协同政策研究和标准制定",
  "题材涨停数": 12,
  "题材内序号": 1,
  "名称": "华电辽能", "代码": "600396",
  "股价": "9.83", "涨幅": "9.96%",
  "涨停类型": "强势板",              // 强势板/一字板/分歧板/回封板
  "板数": "9天5板", "连板": "3",
  "首次封板": "13:43:22", "最后封板": "13:43:22", "开板": "0",
  "封单额": "8861万", "成交额": "36.4亿", "换手率": "27.5%",
  "实际流通": "66亿", "流通市值": "145亿", "总市值": "145亿",
  "异动原因": "绿色电力+氢能+海上风电+央企+借壳猜想",  // "+" separated concepts
  "异动原因详情": "...（长文本，个股基本面解释）",
  "细标签": "数据中心|算力租赁|算力",  // pipe-separated when non-empty, "" otherwise
  "细标签列表": ["数据中心", "算力租赁", "算力"],  // array (empty [] when 细标签="")
  "龙虎榜": "查看" | "无榜",
  "题材股票数": 12
}
```

### v6 bugs
- All keys Chinese, v6 reads English. Totally wrong.
- To anchor yesterday's hot themes: group by `题材名称`, take top N by `题材涨停数`.
- Yesterday's leader per theme = min `题材内序号` with `连板` >= 2.
- Fine-tag expansion: use `细标签列表` (array) directly; split `异动原因` by `+`
  for additional concept signals.

---

## 9. `review.ltgd.range` — 龙头高度区间涨幅

UI: `/web/fupan` 「龙头高度」 → 区间涨幅.

```json
{
  "周期": "5日",                    // "5日"/"10日"/"20日"/"50日"
  "板块": "主板",                  // "主板"/"创业科创板"/"北交所"
  "板块顺序": 0,
  "排名": 3,
  "代码": "002081", "名称": "金螳螂",
  "区间涨幅": "61%",               // STRING with %
  "概念": "商业航天",
  "概念键": "商业航天",
  "日期区间": "2026-04-16 - 2026-04-23"
}
```

### v6 bugs
- Keys are Chinese; `code` field does not exist → use `代码`.
- `周期` is a string like `"5日"`, not a number.
- `区间涨幅` strip `%` → float.
- Recommended usage: per-stock lookup of `区间涨幅` in period=5日, used as
  "近 5 日累计涨幅" for risk cooling penalty (>=30% → high-risk chase).

---

## 10. `rank.rocket` & `rank.hot_stock_day`

Currently **unused by v6**. Listed for completeness. See sample files for
schema. Safe to ignore in this fix pass.

---

## Directory naming gotcha (v6 runtime path bug)

`_resolve_prev_trading_day_captures` in `scripts/duanxianxia_premarket_v6.py`
iterates a hardcoded list of dataset names using **underscores**
(`review_daily_top_metrics`, `review_fupan_plate`, `review_ltgd_range`,
`home_ztpool`). Real capture directories use **dots**
(`review.daily.top_metrics`, `review.fupan.plate`, `review.ltgd.range`). Fix
by switching the hardcoded list to the dot form (same as `dataset_id`).

---

## Test data

- Full captures: `samples/2026-04-23/*.json` (commit `31ff0a1`).
- Regression test fixture should load these in
  `tests/test_premarket_v6_with_real_sample.py` once the module is fixed.
