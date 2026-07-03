# 框架验证结论 — 0111 收益轴 × 打法 × 多周期回测 & 数据供给根因

生成: 2026-07-03 ｜ 依据: 0111(=0110 重试, rc=0, worker_time 2026-07-02T23:50) 只读回测
样本: 19 交易日(2026-05-21 ~ 2026-07-02), regime 分布 cold=16 / cold_to_warming=3, 6150 候选行
口径: R0/R1/R2 = 买入 T 开盘、持到 T/T+1/T+2 收盘, 分母 preclose_T; composite = 当日全候选逐因子 z 相加

---

## 0. TL;DR(决策级)

1. **真正的瓶颈是数据供给, 不是模型, 也不是探针**(源码级证据见 §3)。
2. **主线轴(P1)与 bidStrength/FF 在历史上不可回测, 只能前向(live)验证**。据此正式关闭
   "用历史 regen 数据跑主线轴/FF" 这条路线(0112 原设计作废)。
3. **收益轴结论可锁定**: S1 首板是全冷窗口内唯一稳健 +EV 日内打法; 多周期持有(T+1/T+2)
   系统性修正当日口径对复合排序的低估。

---

## 1. 收益轴 × 打法 × 多周期 矩阵结论(辩证)

> 注意: 本轮 composite 因 FF/换手/量比 全缺, 实际退化为 z(amt)+z(竞价涨幅), **不是**设计中的
> bidStrength 复合。下述 composite 列须按此口径解读。

- **S1 首板 = 唯一稳健 +EV 日内打法**: cold 下 amt Top3_R0 **+9.39**、Top5 +4.66、跌停率 0、n=80。
  跨 0109/0110/0111 三轮一致。
- **多周期修正当日低估(核心发现)**: cold 下
  - S1 composite: R0 +4.67 → R1 **+6.65** → R2 **+8.53**
  - S2 composite: R0 -1.72 → R1 +1.25 → R2 **+3.12**
  → 持有到 T+1/T+2 显著改善, 尤其复合排序; 验证 0110 的多周期假设。**当日兑现口径系统性低估连板/接力。**
- **S2 低位连板**: 日内 -EV(Top3_R0 -2.5~-2.8, 胜率 0.19), 但 T+2 转正 → 不适合当日兑现。
- **S3 低吸**: cold_to_warming 最佳(amt Top3 +3.70, 胜率 0.87)。
- **S4 高标龙头**: 样本太薄(8 天, 池 13~14), Top5_R2 -3.7~-4.1, **无法定论**(P2 caveat 成立)。
- **风控警示**: baseline v9 BUY 在 cold 下 **跌停率 0.273** → 现行生产买入在冷市有 27% 崩盘率, 风险门需收紧。
- **小样本噪声**: cold_to_warming 的 S1 composite Top3_R0 -14.2, 但仅 3 天/15 样本, 不定论, 仅标记。

---

## 2. 字段覆盖率自检(6150 行)

| 字段 | 覆盖率 | 结论 |
|---|---|---|
| auction_pct | 98.7% | ✅ 可用 |
| amt(竞价成交额) | 57.2% | ⚠️ 部分缺, 排序可用 |
| FF(自由流通市值) | **0%** | ❌ 从未逐股落库 |
| 换手率 | **0%** | ❌ 从未逐股落库 |
| 量比 | **0%** | ❌ 从未逐股落库 |
| bidStrength(amt/FF) | **0%** | ❌ 因 FF 缺而不可算 |
| matched_plate | **0%** | ❌ theme_detail 整体为空 |

---

## 3. 数据供给根因(源码级证据, 非推测)

证据来自 `scripts/duanxianxia_v9_output.py` 的 `_compact()` / `_full()`:

1. **matched_plate 探针无误**: `_compact` 顶层即 `"matched_plate": theme_detail.get("matched_plate")`;
   0110 读 `rec.get("matched_plate")` 路径正确 → 0% 是真为空, **整个 theme_detail 在 regen 输出里没被填充**
   (t0_plate_inflow_wan / t0_limitup_count / plate_strength_rank 同源, 一并为空)。
2. **auction_detail 有值但无 FF/换手/量比**: auction_pct(=auction_detail.latest_change_pct)覆盖 98.7%,
   证明 auction_detail 被填充; 但其中**不含** free_float_market_cap / turnover_rate / volume_ratio 任何键 →
   这些字段从未逐股下载落库(即便 live 也没有)。
3. **对照证据**: board 分类(来自 context_detail / weimai_detail)正常产出 S1~S4 → 这两个 detail 在 regen 里是活的;
   **唯独 theme_detail 与 FF/换手/量比 缺失**, 因其依赖 captures 中不存在的板块数据下载。

→ **根因 = regen 管线(v9r.run_v9 from captures)不重建板块富集 + FF/换手/量比 从未落库。** 非模型、非探针问题。

---

## 4. 决策与去向

- **关闭历史路线**: 不再尝试用 regen 历史数据回测主线轴 / bidStrength / FF。0112 原设计(theme 历史回测)作废。
- **FF-vs-Float 之争在历史上无解, 只能前向**: bidStrength=amt/FF 的历史验证不可行; FF 口径(已锁定为分母)
  的实证只能靠 live 累积。
- **前向验证路径(下一步的正确方向, additive)**:
  1. live v9 运行时 theme_detail 已填充(matched_plate 等), 需开始**逐日快照留存**以累积主线轴样本。
  2. FF/换手/量比需在 auction 源层**增量落库**(additive, 不重写大脚本), 之后前向累积 bidStrength 证据。
- **收益轴结论锁定**: S1 首板(cold)为主力日内打法; 多周期持有作为兑现优化项。S2/S4 在有历史前不下最终结论。

---

## 5. 对 0112 / 主线轴的处置

- **作废** 0112 的 "theme 历史回测" 设计(会输出全空表)。
- **改为** 前向: 主线轴监控器 — 每日从 live theme_detail 采集 theme_persist / 阶梯交叉表 / 龙头核心度 快照,
  逐日落到 reports/_audit, 待样本累积后再统计。历史 regen 不参与。
