# Premarket v6.2 — threshold tuning from 2026-04-24 live run

Context: openclaw ran the v6.1 pipeline against `captures/2026-04-24` (mid-day
snapshot ~11:05) and shared the top-10 score breakdown. This doc explains
what v6.2 changes and why, grounded in that breakdown.

## Top-10 breakdown evidence (2026-04-24)

| #  | Code   | Name    | Score | Sources                      | source_hit | direction | theme |
|----|--------|---------|-------|------------------------------|------------|-----------|-------|
| 1  | 301116 | 益客食品 | 35.07 | vratio, qiangchou            | 3          | 2         | 0     |
| 2  | 688269 | 凯立新材 | 26.83 | vratio, qiangchou            | 3          | 2         | 0     |
| 3  | 000066 | 中国长城 | 26.37 | vratio, net_amount, fengdan  | 6          | 2         | 2     |
| 4  | 688041 | 海光信息 | 23.73 | vratio, net_amount           | 3          | 2         | 6 (−3 crowded) |
| 5  | 688787 | 海天瑞声 | 23.10 | vratio, fengdan              | 3          | 2         | 0     |
| 6  | 300397 | 天和防务 | 22.30 | vratio, qiangchou            | 3          | 2         | 0     |
| 7  | 002916 | 深南电路 | 21.27 | vratio, qiangchou, net_amount| 6          | 0         | 0     |
| 8  | 603666 | 亿嘉和   | 21.07 | vratio, fengdan              | 3          | 2         | 0     |
| 9  | 600433 | 冠豪高新 | 19.67 | vratio, fengdan              | 3          | 2         | 0     |
| 10 | 002156 | 通富微电 | 19.40 | vratio, net_amount           | 3          | 2         | 4     |

**Observations:**

- `source_hit=3` appears 7/10 times — acts as baseline, not signal.
- `direction=2` appears 9/10 times — in a normal regime, auction>0 + latest>0 is the default.
- `theme` fires only 3/10 — when it fires it matters (2/6/4 points), issue is coverage.
- `yesterday=0` everywhere — prev_date not inferred, fixed in v6.2.

## Changes

### 1. `source_hit_bonuses.two_sources: 3 → 2`

Keeps three/four-source bonuses (6, 10) because three-way cross-confirmation is genuinely rare and informative.

### 2. `direction_consistency` adds `min_pct: 2.0`

New gate: both `auction_change_pct >= 2.0` AND `latest_change_pct >= 2.0`. Makes the 2pt bonus a real follow-through signal.

### 3. `theme_overlay`: wider catalog, heavier match bonus

- `top_n_themes: 15 → 25`
- `strength_floor: 1.0 → 0.5`
- `match_bonus: 4 → 5`
- `subplate_match_bonus: 2 → 3`

### 4. CLI: `_infer_prev_trading_day`

When `--prev-date` is not given, the CLI now scans `capture_dir`'s parent for sibling `YYYY-MM-DD` dirs and picks the most recent one strictly before `capture_dir.name`. So `python scripts/duanxianxia_premarket_v6.py captures/2026-04-24` now auto-loads T-1 from `captures/2026-04-23`.

## Expected directional impact

- #1 益客食品 and #2 凯立新材 each lose 1pt (two_sources bonus).
- Candidates that coasted on "+0.3% auction / +0.4% latest" lose the 2pt direction bonus; real 3%+ starters keep it.
- #4 海光信息 and #10 通富微电 gain from match_bonus 4 → 5.
- Candidates with concepts in themes ranked 16–25 should newly surface.
- T-1 bonuses start firing once 2026-04-23 is populated alongside 04-24.

## Recalibration once T-1 is live

- If `yesterday.hot_theme_bonus` is very sparse in top-30 → relax `min_theme_stock_count: 3 → 2`.
- If `leader_bonus=5` fires on >20% of top-30 → reduce to 3.
- If nothing matches `ltgd_min_range_pct: 20` → reduce to 15.

## Not tuned this round

- `rank_scores` max_scores — working as intended.
- `numeric_signals` thresholds — firing correctly (e.g. `volume_ratio_multiple >= 150` fired strong on #1 益客食品; `fengdan_925_yi` correctly didn't fire on #9 冠豪高新).
- `crowded_penalty` — fired correctly on #4 海光信息.
- `market_regime` thresholds — regime was `normal` as expected.
