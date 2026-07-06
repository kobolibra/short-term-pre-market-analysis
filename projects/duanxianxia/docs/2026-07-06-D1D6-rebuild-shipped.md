# 2026-07-06 — D1–D6 重建与 fengdan 接入（落盘记录）

> 本文是同目录下 `2026-07-06-auction-indicator-caliber-and-dimensions-handoff.md` 的**代码落地补记**。口径与维度的完整推导以那份 handoff 为准；本文只记录“本次会话 push 了什么”。

## 状态
- ✅ 已 push 到 `main`（两个 commit：先 feature_builder，后 indicator_builder + listing + job + 本文）。
- ✅ 已排验证任务 **0166**（`scripts/agent_jobs/queue/0166_indicator_rebuild_verify_20260706.json`）。
- ⚠️ **六维结构仍为默认实现口径，未经最终拍板**（见末尾“待确认”）。

## feature_builder → `feature_builder_v12.0`
接入 `auction.jjlive.fengdan`（named_dict）：
- 新增 `FENGDAN_DATASET`；`build_from_datasets` 用 canonical 将 fengdan **按 code 叠加**到已合并特征上，注入 `sealBid915 / sealBid920 / sealBid925`（元）+ `fengdan_hit`。
- fengdan **不参与 4 源合并**，不影响 `source_hits / source_hit_count`（002407 仍 = 3）。
- `build_feature_table` 读 `captures/<date>/auction.jjlive.fengdan/`（目录名兼容含 “fengdan” 的子目录）。
- `coverage` 新增 fengdan 行；结果新增 `n_fengdan / n_fengdan_merged`。
- `_self_test()` 新增 002407 fengdan 合成行断言（915=1.5亿/920=0.3亿/925=0.2亿），保持阻塞式绿。

## indicator_builder → `indicator_builder_v13.0`
D1–D6 按敏定口径重建：

| 维 | 名 | 指标 | 口径 |
|---|---|---|---|
| D1 | 定价 | `d1_auction_change_pct` | 竞价涨幅% = changeRate |
| D2 | 量能 | `d2_bid_amount` / `d2_bid_strength` / `d2_volume_ratio` / `d2_turnover_rate` / `d2_grab_strength` | 竞价成交额；/FF；量比；换手；抢筹强度（与量比不同，**保留**） |
| D3 | 资金质量 | `d3_main_net_inflow` / `d3_fund_ratio` | 主力净额；**资金占比 = 主力净流入 ÷ 竞价成交额** |
| D4 | 封板承接 | `d4_true_seal` / `d4_seal_ratio` / `d4_fengdan_925` | **真封单 = raw4 − raw8**；承接 = 真封单 ÷ FF；fengdan 9:25 交叉校验 |
| D5 | 分歧 | `d5_fill_ratio` / `d5_time_divergence` | 成交/委托 = raw8 ÷ raw4；时间分歧 = (raw4 − f920)/f920，**仅 f925≠0** |
| D6 | 情绪环境 | （无逐股标量） | 市场/板块层，外接；本层仅透传 concept |

**删除**：`d1_auction_amount_pct`、`d3_super_large_order`、`d3_large_order`、`d3_money`、`d3_money_pct`、`d4_seal_amount`（raw17 动态封单弃用）、`d5_weimai_strength`、`d5_orderbook`。

`_self_test()` 全重写：校验 002407（含 fengdan）与 300279（仅 qiangchou）的每个 d*_ 口径与“删除键不存在”，`set(coverage)==set(INDICATOR_KEYS)`。

## 验证 job 0166
- `scripts/duanxianxia_indicator_listing_0166.py`（替代 0157）：按 `d4_true_seal` 降序列 D1–D6，RECAP 放最后以防 stdout 尾部截断。
- 结果回写 `projects/duanxianxia/reports/_audit/agent_jobs/0166.result.json`（agent-results 分支）。

## ⚠️ 待确认（六维结构）
实现已按 handoff §五 的默认六维落地，但以下仍需你拍板，如需改只动 `indicator_builder.DIMENSIONS` + `INDICATOR_KEYS`（口径函数已就位）：
1. 定价(D1) 与 量能(D2) 是否合并？
2. 连板/首板是否单独成维（现在只在 boardLabel 透传）？
3. 分歧(D5) 保留哪一/两个角度（成交/委托、时间分歧）？
