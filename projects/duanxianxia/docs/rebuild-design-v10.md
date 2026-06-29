# Duanxianxia v10 Rebuild Design

> Companion to canonical-field-dictionary.md and v10-field-alignment-decisions.md.
> Decides what to keep, what to rebuild, how to migrate history without corrupting it,
> and the final factor layer. Updated 2026-06-29.

## Data lineage (4 layers)
```
raw[] / delimited token stream            <- GROUND TRUTH
  -> fetcher parse  -> capture rows{}      (named; currently mislabeled)   [transform 1]
    -> loader time-slice -> flat feature tables                            [transform 2]
       (*_all_candidates_flat.csv ~1MB each, feature_matrix_v21.csv ~3MB)
      -> v7/v9 scoring
```
Mislabels live in BOTH transform layers (capture rows AND flat feature tables).

## Keep vs Rebuild
### KEEP (correct + valuable IP)
- Acquisition + decrypt + persist pipeline: endpoints, AES (key/iv/CBC), raw preservation, persistEveryFetch.
- Strict time-isolation loader: T0 <= 09:29, T-1/T-2 <= 09:33, reject post-market lookahead.
- cron / agent_job runner infra.
- Validated factor learnings: v10 edge_core weights, REGIME_ACTION_GATE thresholds, per-factor IC findings.

### REBUILD
- Parse / schema layer: drive entirely from canonical-field-dictionary.md; emit canonical names + caliber tags; re-derive rows from raw.
- Factor / scoring layer: rebuild on correct calibers + other-agent framework (see below).

## Historical migration (NO in-place rewrite)
- For every capture that has raw: re-derive canonical rows from raw using the corrected parse map. History is fully recoverable from raw.
- Datasets WITHOUT raw (legacy pool.hot): tag provenance = legacy_unrecoverable; store raw going forward.
- Regenerate flat feature tables from the canonical layer (do NOT sed historical JSON).

## Canonical schema layer (new module)
- One registry: dataset_id -> { source, raw_kind (array | tokens | json | html), parse_spec, fields: [ { canonical, caliber, unit, raw_ref } ] }.
- Validators: any market-cap field MUST declare caliber (FF / FLOAT / TOTAL); build fails otherwise.
- Cross-table caliber join anchored on review.fupan.plate (exposes FF + FLOAT + TOTAL).

## Factor layer (v10) -- FINAL inputs
Caliber: all market-cap-relative factors standardized on **FF (元)**. Full formulas in v10-field-alignment-decisions.md.

Per-stock factors:
- bidStrength = auction_turnover / FF x10000
- volumeRatio = vratio item11
- changeRate  = auction_change_pct
- limitBuyAmountAfter920 = fengdan amount_920 (委买/封单, parse to 元)
- prevStreak (fupan 连板), prevOpenNum (fupan 开板), brokenLimitUp (ztpool 状态=炸)
- origin -> fromPrevBrokenLimitUp / fromPrevSealedLimitUpWithOpen
- stockMainlineFit (concept vs kaipan top 板块强度)

Market / theme features:
- prevDayLimitUpSealRate  (EOD T-1; num/hist)
- auctionLimitUpSealRate  (today 9:25 T0; fengdan auction breadth) -- SEPARATE feature, never substitute prevDay
- sentimentSignal (review QX 情绪)
- themeConsistency  = count(H)/count(Q)  (题材内高开一致性)
- themeConcentration = themeBidAmount / sum(all themeBidAmount)

Keep edge_core structure; re-fit coefficients on corrected inputs.

## Open items
- auctionLimitUpSealRate exact numerator/denominator -> job 0083.
- minBidAmount / auctionChgMin thresholds (our tuning).
- limitBuyAmountAfter920: confirm amount_920 vs amount_925 choice for the 9:20-after caliber (920 = first non-cancellable snapshot; 925 = final).
