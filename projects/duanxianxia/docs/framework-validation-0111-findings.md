# 框架验证结论 — 0111 收益轴 × 多周期回测 & 字段排查

生成: 2026-07-03 ｜ 重大更正: 2026-07-03(见 §0)

---

## 0. ⚠️ 重大更正(推翻之前的"数据缺失"结论)

**之前本文写的"FF/换手/量比/matched_plate 历史不可回测、只能前向"是错的。原始数据全部下载了。**

铁证(`captures/2026-06-01/auction.jjyd.vratio/092803.json`, 155 行, 每行均含):
- `volume_ratio_multiple` = **量比**
- `turnover_rate_pct` = **换手率**
- `auction_turnover_wan` = **竞价成交额**
- `concept` = **题材/概念**(逐股)

`captures/<date>/home.kaipan.plate.summary/` 含 **板块**(主力流入/涨停数/强度排名)。

**那 0110/0111 报的 0% 覆盖率是什么?** 是回测脚本的**取数 bug**, 非数据缺失:
1. 回测读的是 v9 json 而非原始 capture; 且 `load_days` 取 `sorted(*_analysis_v9.json)[-1]` = **晚间 20:xx 文件**(非早盘 live)。
2. captures 的 volume_ratio_multiple/turnover_rate_pct/concept 未被 v9 json 承接到回测探针读的位置
   (`full.auction_detail.*` / 顶层 `matched_plate`)。

**例外**: vratio capture 里 **没有 FF/流通市值**。FF 在另一数据集(疑似 review.fupan.plate, EOD), 盘前能否拿到需单独确认。
量比/换手/题材/成交额 均已落库。

---

## 1. 决策更正

- **主线轴(P1)、量比、换手、题材同步 可以用历史回测** — 只要回测**直接从 captures 读**、不依赖 v9 json 的承接。
  撤销"只能前向验证"的错误结论。
- **0112 主线轴回测 复活**, 但改为直接从 captures/(auction.jjyd.vratio + home.kaipan.plate.summary + …) 取数。
- **FF/bidStrength**: 需先定位 FF 所在数据集与盘前可得性; 在那之前 bidStrength 暂无法回测。
- **需修复 0110/0111 回测取数**: 至少选 files[-1] → 选早盘 live 文件(或直读 captures); 并从 capture 字段名取 量比/换手/题材。

---

## 2. 收益轴 × 打法 × 多周期 矩阵结论(仍有效)

> 收益 R0/R1/R2 来自 dailyline, 不受上述取数 bug 影响, 结论仍成立。
> 但 composite 列本轮退化为 z(amt)+z(竞价涨幅)(因量比/换手未被承接), 非设计复合。

- **S1 首板 = 全冷窗口唯一稳健 +EV 日内打法**: cold amt Top3_R0 **+9.39**, Top5 +4.66, 跌停率 0, n=80。三轮一致。
- **多周期修正当日低估(核心)**: cold S1 composite R0 +4.67→R1 +6.65→R2 +8.53; S2 composite R0 -1.72→R2 +3.12。
- **S2** 日内 -EV、T+2 转正; **S3** 转暖市最佳; **S4** 样本太薄无法定论。
- **风控**: baseline v9 BUY 在 cold 跌停率 0.273, 需收紧。

---

## 3. 待办

1. 修 0110/0111 取数(直读 captures) → 重跑, 拿到真正的 量比/换手 复合回测 + matched_plate 题材同步。
2. 定位 FF 数据集 + 盘前可得性 → 才能回测 bidStrength(=amt/FF)。
3. 0112 主线轴回测: 改为直读 captures/home.kaipan.plate.summary。
