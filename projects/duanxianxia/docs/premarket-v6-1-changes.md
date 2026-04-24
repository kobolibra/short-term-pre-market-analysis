# Premarket v6.1 — changes vs the superseded PR #1

This branch (`fix/premarket-v6-field-alignment`) supersedes the closed
[PR #1](https://github.com/kobolibra/short-term-pre-market-analysis/pull/1).
The earlier v6 code was written against assumed field names; this version is
aligned to the real capture schema in `samples/2026-04-23/`.

See [`premarket-v6-field-mapping.md`](./premarket-v6-field-mapping.md) for the
full code ↔ data ↔ page map.

## Bugs fixed

| # | Symptom in v6 PR #1 | Root cause | Fix in v6.1 |
|---|---|---|---|
| 1 | `regime gate` always returns `normal` | `_classify_regime` read `current_value`; real field is `value` / `raw_chart_tail_value` | Read `raw_chart_tail_value` (numeric) first, fall back to `value`; parse units per-metric |
| 2 | Theme risk penalty always 0 | `_build_theme_catalog` read English `main_inflow_wan`, `plate_name` | Read Chinese `主标签名称`, `板块强度原值`, `主力流入原值`(万), `主力流入真实金额`(元) |
| 3 | Yesterday theme anchoring never fires | `_evaluate_yesterday_signals` read `plate_name` / `leader_codes` | Read Chinese `题材名称` / `代码` / `连板` / `异动原因` / `细标签列表` |
| 4 | Previous trading day captures not found | `_resolve_prev_trading_day_captures` used underscore dirs `review_ltgd_range` | Use dot-separated names `review.ltgd.range`, `review.fupan.plate`, `review.daily.top_metrics`, `home.ztpool` |
| 5 | Numeric fields treated as 0 | `auction_turnover_wan`, `volume_ratio_multiple`, `grab_strength` are **string** in captures | New `_parse_float` tolerates strings and `%`/`,` chars |
| 6 | Qiangchou rows ignored | v6 filtered on `group == "qiangchou"` | Real value is `group == "grab"` |
| 7 | Fengdan amount signals always 0 | `amount_915/920/925` are Chinese-unit strings (`"32.3亿"` / `"4860万"` / `"-"`) | New `_parse_chinese_amount_to_yi` parses 亿/万/dash |
| 8 | HSLN regime check misinterpreted | Premarket `HSLN` `value` is `"+2.3亿"` / `"-1.5亿"` signed string | Parsed via `_parse_chinese_amount_to_yi` to float 亿 |
| 9 | ZTBX/LBBX thresholds wrong | Treated as raw counts; real is percentage string like `"1.78"` or `"2.17%"` | YAML: cold_max 0, hot_min 3/5; parser strips `%` |
| 10 | Untradable filter over-aggressive | Filtered on `涨幅` as if absolute change | Check only `latest_change_pct >= 9.7` (封死) |
| 11 | PBBX not used | v6 ignored nested tier keys | YAML now references PBBX as percentage; postmarket regime reads it |

## Schema cheat-sheet

For the exact sample values, see `premarket-v6-field-mapping.md`. Quick highlights:

- `home.qxlive.top_metrics` rows: `metric_key` (UPPERCASE), `value` (string), `raw_chart_tail_value` (numeric), for metrics `QX / KQXY / HSLN / LBGD / SZ / XD / PB / ZTBX / LBBX / PBBX (+ tier keys)`.
- `home.kaipan.plate.summary` rows: Chinese keys only. Subplates available both as pipe-separated `子标签列表` **and** as a structured `meta.top_plates[].subplates[]` array — prefer the structured form.
- `auction.jjyd.vratio` rows: mixed types, `auction_turnover_wan` **string int**, `volume_ratio_multiple` **string float**, `concept` single string (not pipe).
- `auction.jjyd.qiangchou` rows: `group == "grab"`, `grab_strength` **string float**.
- `auction.jjyd.net_amount` rows: closest to v6 assumptions — numeric fields, `concept` pipe-separated.
- `auction.jjlive.fengdan` rows: `section_kind == "live"`, `amount_915/920/925` Chinese-unit strings or `"-"`, `board_label` like `"首板"` / `"2板"` / `"昨首板"` / `""`.
- `review.fupan.plate` rows: Chinese keys throughout.
- `review.ltgd.range` rows: Chinese keys, `周期` ∈ {`"5日"`, `"10日"`, `"20日"`, `"50日"`}, `板块` ∈ {`"主板"`, `"创业科创板"`, `"北交所"`}.

## Not yet validated

Thresholds in `premarket_scoring.yaml` (e.g. `strong_min: 150.0` for volume
ratio, `hot_min: 2.0` 亿 for HSLN) are **initial estimates from one day of
sample data**. They should be recalibrated against the actual empirical
distribution across multiple trading days before relying on them in
production. This is best done on the openclaw-deployed server where it can
backfill from historical captures.

## Integration note

The corrected `scripts/duanxianxia_premarket_v6.py` keeps the same public API:

- `VERSION = "premarket_5table_v6.1"`
- `DEFAULT_CONFIG_PATH = Path("projects/duanxianxia/config/premarket_scoring.yaml")`
- `load_premarket_config(path=None, project_root=None) -> dict`
- `build_premarket_analysis_v6(report, project_root=None, config=None) -> dict`
- Aliases: `build_premarket_analysis`, `compute_premarket_analysis`

So `scripts/duanxianxia_batch.py` does not need changes — just swap the import
to the v6.1 module, or keep the existing import (same filename). The
integration doc in `projects/duanxianxia/docs/premarket-v6-integration.md`
will be updated in a follow-up commit once the module is merged.
