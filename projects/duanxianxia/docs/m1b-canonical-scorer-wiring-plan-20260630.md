# M1 验收 + M1b canonical 评分接线方案 (2026-06-30)

> 本文档基于 main@c8d408be 的源码逐文件核对得出，作为 rebuild-plan-v11 的 M1/M1b 执行细化。

## 1. M1 验收：GREEN（实盘已证）

探针 `m1_feature_builder_probe_20260630r2`（worker_time 17:40:09，rc=0）在 **2026-06-30 真实早盘 captures** 上跑通：

- `n_features=288`；四源 `canonical_error` 全 0：vratio 92/92、qiangchou 66/66、net_amount 55/55、weimai 150/150。
- 四源 `row0_has_raw=true`，raw 长度 13/13/9/18，与各表定义吻合。
- 校验：`no_mislabel_leak=true`、`all_ff_caliber=true`、`canonical_error_total=0`、多源命中 61。
- provenance 键为 canonical 名（auction_turnover/main_net_inflow/free_float_mktcap）。

**结论**：`duanxianxia_feature_builder.py`（VERSION=feature_builder_v11.0，blob 8e39fe96）确立为唯一 canonical 特征源；入口 `build_feature_table(capture_dir, *, cutoff)`。M1 关闭。

## 2. 现状架构（已核对）

canonical 评分层其实**已建好**：v9 家族（assemble / edge / weimai / theme_strength / market_env / context / output）+ `v9_from_report` 适配器。

- `edge_core` = `duanxianxia_v9_edge.compute_edge_v9`，v10 IC 加权七因子：
  `amt .23 / auction .19 / liquidity .18 / money .14 / pressure .14 / weimai .08 / orderbook .05 − risk_penalty`。
- 现数据流：
  `duanxianxia_batch.build_premarket_analysis(report)`〔legacy v6/v7 `_merge_candidates`/`_dataset_rows`〕
  → `top_candidates`
  → `v9_from_report.build_v9_block`
  → `assemble_v9`（注入 weimai/theme/env/context + 横截面 `auction_amount_pct`）
  → `compute_edge_v9`
  → `shape_v9_output`。

## 3. M1b 精确缺口

`edge_core` 消费的是**派生分**，不是原始字段：
`auction_strength`、`money_intent_score`、`net_pressure`、`liquidity_score`、`orderbook_quality_score`、`weimai_strength`、`auction_amount_wan`（用于横截面 pct）。

这些目前由 **legacy v6/v7 引擎**产出。而 `feature_builder` 产出 canonical **原始字段**：bidAmount、bidStrength、volumeRatio、mainNetInflow(+full)、superLargeOrder、largeOrder、sealAmount、free_float_mktcap、price、turnoverRate、grabStrength、boardLabel、`_field_sources`。

**所以接线不是机械替换，需要一层 canonical→decision 适配器**，把 feature_builder 行映射/派生成 `auction_detail`（含上述派生分）+ `weimai_detail`，喂给 `assemble_v9`/`edge_core`。

## 4. 顺序计划

- **M1b-1**：核对 `duanxianxia_v7_2_auction_strength.py` / `duanxianxia_v7_1_setup_engine.py` 现有派生口径（auction_strength / money_intent_score / liquidity_score / net_pressure / orderbook_quality_score），作为适配器基线，避免凭空设计导致 IC 漂移。
- **M1b-2**：新建 `duanxianxia_canonical_decision_adapter.py`：`feature_builder 行 → decision{code, auction_detail, weimai_detail, ...}`；带 import-time `_self_test()`（对齐 canonical/feature_builder 的 _self_test 模式，回归即阻断 import）。
- **M1b-3**：把候选来源从 legacy 合并切到该适配器；v9 edge/theme/env/context/output 不动。
- **M1b-4**：探针在实盘 captures 上比对新旧候选与打分（rank 相关性 / 重合率），确认无回归后删除 legacy `_merge_candidates`/`_dataset_rows`（v6）。

- **0093（紧随）**：在真实特征分布上重拟合 edge 七权重 + 各派生分阈值（origin/themeConsistency/themeConcentration/auctionSealAmount/marketSealRate/prevDayLimitUpSealRate/stockMainlineFit；minBidAmount/auctionChgMin）。

## 5. 不可触碰 / 硬约束

- weimai 已取代 fengdan：封单 9:25 信号迁移到 weimai `sealAmount` / 封板率，不再依赖 `auction.jjlive.fengdan`。
- `auction_amount_pct` 由 `assemble_v9` 按全体候选横截面注入（适配器须提供 `auction_detail.auction_amount_wan`，对应竞价成交额）。
- field-rename-map §4 口径不得更改；`auction_volume_ratio` 不就地改名。
- raw[17] seal_amount 单位=万（×1e4→元）。
- 凡含 import-time `_self_test` 的模块，推送前必须用确切已提交依赖 blob 本地跑通自测（M1 修复教训）。
