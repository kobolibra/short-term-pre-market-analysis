# M1b 落地纪要 · canonical-first 接入生产打分路径（2026-06-30）

> 本文是 `m1b-canonical-scorer-wiring-plan-20260630.md` 的**修正与落地纪要**。
> 该计划文档把注入点定在 `duanxianxia_v9_from_report.build_v9_block`，**这是错的**——
> 该模块在生产中被绕过。真正的注入点是 v7.2 runner 的 `build_v72_decisions`，已落地。
> 凡两者冲突，以本文为准。

## 一、关键架构发现（务必先读）

盘前分析的**真实生产链路**（逐层 read 源码确认）：

```
duanxianxia_batch.build_premarket_analysis(report)
  -> (lazy import) duanxianxia_premarket_v7_3_runner.build_premarket_analysis_v7_3(report)
       -> run_v7_3(trade_date, root)
            -> shaped_v72 = run_v7_2(date, project_root)
                 -> build_v72_decisions(date, project_root)        # ← 候选集与打分的单一事实源
                      -> load_premarket_v72_bundle(...)
                      -> build_candidates_from_auction(v71, ..., extra_auction_rows={"weimai": weimai_rows})
                      -> compute_auction_strengths(codes, vratio, qiangchou, net_amount, fengdan, params, weimai_rows=...)   # ← 竞价强度引擎
                      -> compute_hotness / compute_theme_strengths / classify_candidates_v72
            -> upgrade_shaped_v72_to_v73(...) -> _adapt_for_batch(...)
```

**`duanxianxia_batch.py` 从不调用 `duanxianxia_v9_from_report.build_v9_block` / `write_v9_json`。**
因此早先 M1b-1（adapter，commit `e96e2bfd`）+ M1b-2（`v9_from_report` 改写，commit `7a457f13`）
虽然代码正确且自检通过，但落在**生产被绕过**的模块上——保留为安全、永不抛错的模块，但不在实时路径中。

## 二、已落地的 canonical-first 生产接线

**改动文件**：`scripts/duanxianxia_premarket_v7_2_runner.py`（commit `aa53dbb3`，blob `c6b84f22`）。

在 `build_v72_decisions` 中，喂给 `compute_auction_strengths` 的竞价源行改为
**从 canonical raw[] 重建**（口径/单位防错），而非 fetcher 的 named 行：

1. 防御式导入 `duanxianxia_feature_builder`（`v9feat`）+ `duanxianxia_canonical_decision_adapter`（`v9adapter`），任一缺失即静默回退。
2. 由 bundle 组装 `rows_by_dataset = {vratio, qiangchou, net_amount, weimai}`（均带 raw[]）。
3. `_canonical_auction_source_rows()`：`v9feat.build_from_datasets(rows_by_dataset)` -> `v9adapter.build_source_rows_from_features(features)`，得到 canonical 源行（`vratio_rows/qiangchou_rows/netamount_rows/weimai_rows`）。
4. 用 canonical 源行调用 `compute_auction_strengths`，**fengdan 仍传 legacy `v71.auction_fengdan`**（canonical 层未覆盖 `auction.jjlive.fengdan`，换成空会丢掉 source_evidence 0.20 + fengdan_status，属回归）。
5. **任何失败 / 无特征**（如某次 capture 缺 raw[]）-> 自动回退到 legacy named 行，**worst case = 改动前的行为**。
6. `meta["auction_source"]` 记录实际走的路径：`"canonical_feature_builder"` 或 `"legacy_named_fields"`。

本地端到端验证（002407 fixture + 空 fengdan）：canonical 路径 `auction_strength=51.78`、
`seal_amount_wan=208089`、`auction_amount_wan=1779`（单位口径正确）；回退路径亦正确。

### 已知保留项（非阻塞，记入 0093 因子重拟合）
- canonical `qiangchou` 源行承载 9:20–9:25 主抢筹信号；**9:25 最后一秒 `grab` 子组**的细分目前未单独透传（canonical qiangchou 单值）。`compute_auction_strengths` 中 `qiangchou_920_925`(0.35) 保留，`qiangchou_last_second`(0.18) 可能弱化。由 M1b-3 探针的排序相关性量化其影响，必要时在 0093 补回。
- `build_candidates_from_auction` 仍走 legacy 行（候选集/题材不受数值错配影响，刻意不动以保持候选全集稳定）。

## 三、服务器验证门 M1b-3（已入队）

- 脚本：`scripts/duanxianxia_m1b_auction_source_probe_20260630.py`（commit `0b9d0777`，只读）。
- 队列：`scripts/agent_jobs/queue/m1b_auction_source_probe_20260630.json`（commit `4054d1c1`）。
- 行为：在最新实盘 captures 上跑 `build_v72_decisions`，**硬断言 `meta.auction_source=="canonical_feature_builder"`**；
  独立重算 legacy 路径强度，报告 canonical vs legacy 的 **Spearman 排序相关性** + top-15 并排 + 源行覆盖。
- 通过判据（rc=0）：canonical-first 已在生产生效；排序相关性高（无回归）。
- 结果落 agent-results 分支（cron 整点跑，publish ~10min 延迟）。

## 四、下一步
1. 读 M1b-3 探针结果：确认 `auction_source=canonical_feature_builder`、强度非退化、Spearman 与 legacy 高度一致（无回归）。若回退到 `legacy_named_fields`，按结果中的 `canonical_source_row_coverage` / warnings 定位 raw[] 缺失或特征为空的原因。
2. （0093）因子重拟合时评估并按需补回 qiangchou 最后一秒子组。
3. M2 fetcher 封单展示补丁随下次 fetcher 提交一并落地。

## 五、相关 commit（本批，main）
- `aa53dbb3` build_v72_decisions canonical-first + 防御回退 + meta.auction_source
- `0b9d0777` M1b-3 只读服务器探针脚本
- `4054d1c1` M1b-3 探针入队
- （前序）`7a457f13` v9_from_report 改写（生产被绕过，保留为安全模块）、`e96e2bfd` canonical decision adapter
