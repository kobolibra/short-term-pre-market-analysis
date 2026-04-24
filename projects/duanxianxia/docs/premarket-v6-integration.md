# Premarket v6 集成指南

> Branch: `feat/premarket-v6-scoring`  
> Version bump: `premarket_5table_v5` → `premarket_5table_v6`  
> 主要新增文件:
> - `scripts/duanxianxia_premarket_v6.py` — 新打分核心, 自包含
> - `projects/duanxianxia/config/premarket_scoring.yaml` — 配置
> - `tests/test_premarket_v6.py` — smoke test

本文档解释:
1. v6 让 v5 的哪些问题得到修复
2. 在 `scripts/duanxianxia_batch.py` 里需要作的 **最小侵入性改动** (4 处)
3. 如何跳过 / 回滚
4. 调参建议

---

## 1. v6 修复清单 ↔ 原分析里提出的 11 个问题

| #  | 原问题                                                      | v6 修复点                                                                                                                                             |
|----|--------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | fetcher 拿到的数值字段被丢                                   | `_compute_numeric_score` 对 量比 / 今昨竞额比 / 换手 / 主力净买 / 抢筹幅度 / 9:25封单 / 封单递增 / 流通值 近7项里的值做候选池内 percentile 打分 |
| 2  | rank−N 线性加分太均匀, 魔数没依据                       | rank 分保留但 weight 降为 0.6 (辅助项); 魔数全移到 yaml, 使数值分成为主导                                                                                     |
| 3  | 主力净买 / 抢筹幅度 被当标签而非数值                     | 同 #1, `main_net_inflow_wan` weight 5.0, `grab_strength` weight 4.0, 成为权重最高的两项                                                                 |
| 4  | 第 17 表 qxlive 不进决策                                     | `_classify_regime` 读 ZTBX/LBBX/HSLN/LBGD, 冷 / 热 / 常态三档 multiplier, 冷档同时压缩 top N 为 5                                                                   |
| 5  | 主题只做字符串词包含                                   | 保留词包含+灰名单+主标签排名, 新增 `sub_leader_bonus`: 股票在子标签里的位次给 +2.5 / +1.5 / +0.8                                                      |
| 6  | untradable 只看离涨停 0.2%                                | `_is_untradable_v6`: 近涨停时 封单厚度 / 封单递增 / 主力净买 / 流通值小 任一满足即放行; 封单缩水1.5倍 → 真烂板                                                              |
| 7  | 信号方向不一致无校验                                    | `direction_consistency.min_positive_signals` 强制至少 2 项数值同向才给完整数值分, 否则 ×0.3 + 额外 10% 风险惩罚                                                     |
| 8  | 4 表共振奖励10 分太少                                     | `source_hit_bonuses`: 1/2/3/4/5 表 = 0/4/10/18/25, 四表共振的模式奖励翻倍                                                                                |
| 9  | 不读昨日盘后                                              | `_resolve_prev_trading_day_captures` 读 `home.ztpool / review.ltgd.range / review.fupan.plate / review.daily.top_metrics`, 产出4 类加减分: 断板回封 +4 / 延续+3 / 题材龙头 +2.5 / 高位顶断 −2 |
| 10 | 魔数未版本化                                                | `premarket_scoring.yaml` + `schema_version` + 打印到输出                                                                                                   |
| 11 | tiebreaker 按代码 / board_label 不用 / fengdan 不入 untradable / 单位漂移 | tiebreaker 默认 `market_cap_yi_asc`; `board_label` 进 reasons; fengdan live 已纳入主候选池经由 sources, `_is_untradable_v6` 对任何来源的 cand 都会评估; 万/亿 单位由 `_parse_chinese_amount_wan` 统一 |

---

## 2. `scripts/duanxianxia_batch.py` 需要的 4 处改动

> 由于 batch.py 单文件 150KB, v6 删除与内嵌打分相关的几千行代码改动量太大, 本分支采用 **外部模块 + 外界轻补丁** 的集成方式, 保证滚回成本极低.

### 改动 A — 顶部 import

```diff
 # scripts/duanxianxia_batch.py
 import json
 import re
 from datetime import datetime, timedelta
 ...
+# v6 premarket scoring (feat/premarket-v6-scoring)
+try:
+    from duanxianxia_premarket_v6 import build_premarket_analysis_v6
+    _PREMARKET_V6_ENABLED = True
+except Exception:
+    build_premarket_analysis_v6 = None
+    _PREMARKET_V6_ENABLED = False
```

### 改动 B — 引入开关 (环境变量 / 命令行 flag)

在 `parse_args` 或启动读配置的地方加:

```diff
+import os
+PREMARKET_SCORING_VERSION = os.environ.get("PREMARKET_SCORING_VERSION", "v6").strip().lower()
```

或更简单地直接写 `"v6"` 在算法入口里.

### 改动 C — 在现有 `build_premarket_analysis` (或等同名字) 函数第一行旁路

找到现有的 v5 入口 (例如):

```python
def build_premarket_analysis(report, *args, **kwargs):
    # ... v5 原逻辑 几千行 ...
```

改为:

```diff
 def build_premarket_analysis(report, *args, **kwargs):
+    if _PREMARKET_V6_ENABLED and PREMARKET_SCORING_VERSION in {"v6", "premarket_5table_v6"}:
+        from pathlib import Path as _Path
+        _project_root = kwargs.get("project_root") or _Path(__file__).resolve().parent.parent
+        return build_premarket_analysis_v6(report, project_root=_project_root)
+    # 以下为 v5 原逻辑, 保留作为 fallback
     # ... v5 原逻辑 不变 ...
```

这样 v5 代码 **一行都不用删**, 通过环境变量 `PREMARKET_SCORING_VERSION=v5` 随时回滚到旧逻辑.

### 改动 D — cron 脚本

项目的 `25 9 * * 1-5` cron 调用点在 `scripts/` 目录里白名单包含 `duanxianxia_premarket_v6.py`, 或在部署配置里确保新文件被含在发布包里. `project_root` 按缺省指向 `projects/duanxianxia`, 无需配置改动.

---

## 3. 跳过 / 回滚

```bash
# 回滚到 v5
export PREMARKET_SCORING_VERSION=v5

# 或直接 revert 分支
git revert <commit-range>
```

由于 v6 全部新文件, 回滚仅需删除引入点 (改动 C) 或设置环境变量.

---

## 4. 调参建议

### 4.1 先零改动跳两周

1. 直接用默认 yaml 跑 10 个交易日, 每天 top 10 和 v5 top 10 并排对比, 看是否出现: 真龙被 v6 抓出来, v5 因 untradable 杀错; v5 top 里的烂板被 v6 剥离.

### 4.2 重要权重

- `numeric_signals.main_net_inflow_wan.weight` (当前 5.0) — 你认为主力净买最重要就可以再加 1-2 点.
- `numeric_signals.auction_turnover_ratio.weight` (当前 4.5) — 今竞/昨竞 5 倍以上极具参考价值, 可调高.
- `market_regime.cold_score_multiplier` (0.65) — 冷档时全小要多紧. 如果你实战习惯冷不做, 设为 0.5 或直接再降.
- `direction_consistency.min_positive_signals` (2) — 可以提升到 3 以要求更严的共振.

### 4.3 阈值

- `untradable.keep_if_seal_thick_wan: 3000` — 如果你自己注重封单绝对量, 可以调为 5000.
- `untradable.keep_if_small_cap_yi: 80` — 你对小盘容忍度, 可以上调至 100.
- `market_regime.cold_thresholds.ZTBX_max: 15` — 结合你过去的冷点判断准度微调.

### 4.4 观察点

输出里 `result["top_candidates"][i]["breakdown"]` 会托底出每股的:
- `numeric` 各项子分
- `rank` 各表 rank 分

配合 `theme_matches / yesterday_reasons / reasons`, 上来一眼就能确认每支股的分来自哪里, 便于盯着打错样本迭代.

---

## 5. 给第二波改进占位 (暂不在该分支)

- **主题冷热周期识别**: 通过连续 3−5 日 plate.summary 的主标签活跃度自动加减灰名单.
- **连板标签细分打分**: `board_label` 目前只进 reasons, 后续可对 "昨首板 / 3 天 2 板 / 7 天 5 板" 分档加分.
- **回测框架**: `scripts/duanxianxia_backtest.py` (占位), 每天 top 10 的 T+1 开盘涨幅 + 收盘涨幅 汇总, 方便指数化评估每次调参.
