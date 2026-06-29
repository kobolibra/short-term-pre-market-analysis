# Duanxianxia v10 Rebuild Design

> Companion to canonical-field-dictionary.md. Decides what to keep, what to rebuild,
> and how to migrate history without corrupting it.

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
- Strict time-isolation loader: T0 <= 09:29, T-1/T-2 <= 09:33, reject post-market lookahead. (correct anti-lookahead logic)
- cron / agent_job runner infra.
- Validated factor learnings: v10 edge_core weights, REGIME_ACTION_GATE thresholds, per-factor IC findings.

### REBUILD
- Parse / schema layer: drive entirely from canonical-field-dictionary.md; emit canonical names + caliber tags; re-derive rows from raw.
- Factor / scoring layer: rebuild on correct calibers + other-agent framework + IC findings.

## Historical migration (NO in-place rewrite)
- For every capture that has raw: re-derive canonical rows from raw using the corrected parse map. History is fully recoverable from raw.
- Datasets WITHOUT raw (legacy pool.hot): rows cannot be fully recovered -> tag provenance = legacy_unrecoverable; going forward store raw.
- Regenerate the flat feature tables from the canonical layer (do not sed the tens-of-thousands of old keys in historical JSON).

## Canonical schema layer (new module)
- One registry: dataset_id -> { source, raw_kind (array | tokens | json | html), parse_spec, fields: [ { canonical, caliber, unit, raw_ref } ] }.
- Validators: any market-cap field MUST declare caliber (FF / FLOAT / TOTAL). Build fails otherwise.
- Cross-table caliber join anchored on review.fupan.plate (it exposes FF + FLOAT + TOTAL simultaneously).

## Factor layer (v10)
- Standardize all market-cap-relative factors on ONE caliber = FF (free-float): FF is available across the 竞价五表 + 热门池; convert surge (FLOAT) only where explicitly needed.
- bidStrength = bidAmount / FF_market_cap. bidAmount source (竞价成交额 auction_turnover vs 委买/seal) MUST be declared per factor.
- Keep edge_core structure; re-fit coefficients on corrected inputs.

## Open items (carry into factor design / other-agent alignment)
- limitBuyAmountAfter920 caliber: fengdan amount_920/925 = 委买 or 成交?
- limitUpSealRate window: PB intraday-rolling vs EOD.
- prevOpenNum capture availability; origin source field.
