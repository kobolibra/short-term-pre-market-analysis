# v6.3 — theme alias canonicalization

## Why

Openclaw's 2026-04-24 live run produced top-10 with **zero `yesterday` bonus**, despite T-1 data being correctly loaded (`hot_themes=8, leaders=9, ltgd=31`). Jade + openclaw's joint diagnosis found:

- **Case 1 (naming mismatch, dominant cause)**: `home.kaipan.plate.summary.主标签名称` (today's plate labels) and `review.fupan.plate.题材名称` (T-1 theme labels) use different vocabularies. The two lists from 04-24 / 04-23 **only share one exact string (`业绩增长`)**. Semantically related pairs like `一季报增长` ↔ `业绩增长` or `芯片` ↔ `半导体产业链` were missed by the exact-equality matcher.
- **Case 2 (rotation)**: 04-24 top-10 is dominated by new breakouts that weren't in yesterday's leader pool (`连板>=2`) or 5/10-day range leaders. This is expected and not something tuning should mask.
- **Case 3 (logic bug)**: ruled out. Openclaw verified that `300067` (their current top-10) correctly receives `leader+ltgd=7` bonus when it matches T-1 records.

v6.3 addresses case 1 without masking case 2.

## What

Two mechanisms, both configured under `theme_overlay` in `premarket_scoring.yaml`.

### 1. `theme_aliases` (primary)

User-editable YAML list of equivalence groups. First element is the canonical form; all others map to it. Default groups:

```yaml
theme_aliases:
  - ["业绩增长", "一季报增长", "年报增长", "业绩预增", "预增", "半年报增长"]
  - ["半导体产业链", "芯片", "集成电路", "元器件"]
  - ["光通信", "通信", "光模块", "CPO"]
  - ["华为", "华为海思", "华为产业链", "鸿蒙"]
  - ["算力", "算力概念", "AI算力", "AI服务器", "IDC"]
```

Applied in two places:

- **`_evaluate_theme_for_candidate`**: candidate concept (from auction rows) ↔ kaipan plate name. Now canonicalized before set comparison. Also applies to subplate names.
- **`_yesterday_bonus`**: today's theme hits ↔ T-1 hot_themes.

### 2. Broader hot_theme source set (always on)

v6.2 `_yesterday_bonus` only checked `matched_themes` against `hot_themes`. That missed a subtle case: a candidate has concept `光通信` (which IS a fupan hot_theme), but kaipan today has no `光通信` plate, so it never lands in `matched_themes`.

v6.3 uses **`matched_themes ∪ candidate.concepts`** as the source set for hot_theme matching. Both sides are canonicalized.

### 3. `fuzzy_substring` (opt-in, default false)

Bidirectional substring match with minimum-length guard. Catches e.g. `华为` ⊂ `华为海思产业链` when the user hasn't added that pair to `theme_aliases`. Off by default because `病院` ⊂ `医院信息化` kind of false positives can occur. Turn on via:

```yaml
theme_overlay:
  fuzzy_substring: true
  fuzzy_substring_min_len: 3  # Chinese chars (Python len), default 3
```

## Expected directional impact on 2026-04-24 live top-10

Given T-1 hot_themes canonicalized to `{业绩增长, 光通信, 其他概念, 半导体产业链, 大消费, 电力, 算力, 航天}` and the default alias groups:

| rank | code | name | v6.2 score | likely v6.3 delta | why |
|---|---|---|---|---|---|
| 4 | 688041 | 海光信息 | 24.73 | **+3 (27.73)** | matched `一季报增长` → canon `业绩增长` ∈ hot_themes |
| 2 | 000066 | 中国长城 | 27.37 | **+3 (30.37)** | concept likely includes `芯片` → canon `半导体产业链` ∈ hot_themes |
| 7 | 002156 | 通富微电 | 20.40 | **+3 (23.40)** | concept `芯片` → canon `半导体产业链` ∈ hot_themes |
| others | — | — | — | 0 | top-10 new breakouts often don't share themes with T-1 |

The ranking change is expected to be modest — deliberately, since case 2 (rotation) is a real constraint. If case 1 were fully fixed and case 2 didn't exist, we'd expect more hits.

## How to verify after merge

```bash
# sanity: run on 04-24 with T-1 auto-inferred; confirm yesterday.hot_theme_bonus fires
python scripts/duanxianxia_premarket_v6.py captures/2026-04-24 --project-root . --top 20

# verify baseline on samples (no T-1 sibling available → yesterday should still be 0)
python scripts/duanxianxia_premarket_v6.py samples/2026-04-23 --project-root . --top 10

# optional: enable fuzzy substring to check over-triggering
# (edit premarket_scoring.yaml: theme_overlay.fuzzy_substring: true)
```

Specifically check that:

1. `海光信息` / `中国长城` / `通富微电` get `yesterday.hot_theme_bonus > 0` with `hot_themes` listed in their breakdown.
2. `300067` still gets `leader+ltgd=7` (the path that worked in v6.2 must still work).
3. Aliases didn't over-trigger: candidates with concept `业绩预增` shouldn't explode to 50+ hits; check top-20 variance is sensible.

## What to tune after live data accumulates

- If `hot_theme_bonus` fires on >50% of top-20, the bonus may be too broad. Consider:
  - Tightening `theme_weight: 3 → 2`
  - Removing over-broad aliases (e.g. drop `通信` from the `光通信` group if it over-matches)
- If rarely fires even after this PR, revisit:
  - `min_theme_stock_count: 3 → 2` to widen the hot_themes set
  - Enable `fuzzy_substring: true`
- Alias groups are workspace-specific intelligence. Expand them as new themes rotate in (e.g. add `机器人` alias group when that cycle starts).

## Not changed in v6.3

- `rank_scores`, `numeric_signals`, `source_hit_bonuses`, `direction_consistency`, `untradable`, `risk_penalty`, `market_regime`, `yesterday_postmarket` — all identical to v6.2.
- Public API: `VERSION`, `load_premarket_config`, `build_premarket_analysis_v6`, aliases unchanged.
