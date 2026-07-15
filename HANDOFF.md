# D6 情绪周期 v3 简化版 — 项目交接文档

> 最后更新: 2026-07-15 上海时间
> GitHub: https://github.com/kobolibra/short-term-pre-market-analysis
> 分支: `main` (HEAD: `4e6b18e`)
> 服务器结果分支: `agent-results`

---

## 一、最近的 Git 提交历史

```
4e6b18e fix: D6历史分位从过去分析结果加载, 不再每次空起跑
daea6ce fix: _build_bundle_from_report 不再调用 load_premarket_bundle(trade_date) 避免 T0 captures 缺失报错, 直接加载 T-1 captures
dd8e7b5 fix: premarket_daily 从已有report构建bundle而非从captures加载(与飞书推送同逻辑)
677ab9a D6 情绪周期 v3 简化版: 9相位→7相位, 等权水位, 日变化方向, 2硬否决, QX退出核心
7ea9c3f 数据不重新下载: build_premarket_analysis_v4_2用report已有数据
31177d9 rerun13: 用新代码重跑今天盘前分析+选股
9725c02 三个深度bug修复: 滞回/加权中位数/CHOP一字封/分位0.0陷阱
```

本次会话共修改 7 个文件，774 行新增，441 行删除。

---

## 二、系统架构总览

### 2.1 数据抓取 (cron → batch.py)

```
9:25 cron (工作日)
  → duanxianxia_cron_runner.sh premarket
  → duanxianxia_premarket_v7_runner.py
    → monkey-patch duanxianxia_batch.build_premarket_analysis = build_premarket_analysis_v4_2
    → duanxianxia_batch.main(["premarket"])
      → 抓取所有 premarket 数据集 (auction.jjyd.*, auction.jjlive.*, home.qxlive.*, etc.)
      → 生成 report JSON → 写入 reports/YYYY-MM-DD/premarket/HHMMSS.json
      → 调用 build_premarket_analysis(report) → 推飞书 webhook
```

### 2.2 数据存储位置

| 数据类型 | 存储位置 | 说明 |
|---------|---------|------|
| **当天 T0 竞价数据** | `reports/YYYY-MM-DD/premarket/HHMMSS.json` | 9:25 cron 生成的 premarket 报告, items[].capture_path 指向 captures/ 下的原始文件 |
| **当天 T0 原始文件** | `captures/YYYY-MM-DD/HHMMSS_<dataset>.json` | 竞价抓取的原始 capture 文件 |
| **T-1 盘后数据** | `captures/YYYY-MM-DD/` | ztpool (涨停池晋级率), review.fupan.plate (板块复盘), ltgd, cashflow 等。这些由 postmarket cron 抓取, 不在 premarket report 里 |
| **D6 历史分位数据** | `reports/_audit/v4_2_premarket/YYYY-MM-DD.json` 和 `reports/_audit/v4_2_backtest/YYYY-MM-DD.json` | 过去每天管线输出的 ztbx_925, lbbx_925, advance_share, dt_925, relay_health 五个值, 用于滚动分位计算 |

### 2.3 重跑分析 (agent 定时任务)

```
agent_daily_refresh.py (每天定时触发)
  → 入队到 scripts/agent_jobs/queue/
    → daily_YYYY-MM-DD_duanxianxia_v4_2_premarket_daily.json
    → daily_YYYY-MM-DD_duanxianxia_v4_2_backtest_daily.json
    → daily_YYYY-MM-DD_feishu_analysis_push.json

agent_job_worker.py (持续轮询)
  → git pull origin main   (拉最新代码)
  → 执行队列中的脚本
  → git push origin agent-results   (推送结果)
```

### 2.4 飞书推送重跑

```
duanxianxia_v4_2_premarket_feishu_rerun.py
  → 找到 reports/YYYY-MM-DD/premarket/ 下最新 report
  → monkey-patch duanxianxia_batch.build_premarket_analysis = build_premarket_analysis_v4_2
  → duanxianxia_batch.main(["premarket", "--report-path", report_path, "--webhook-url", webhook_url])
    → batch.py 读取已有 report, 不重新抓取
    → 调用 build_premarket_analysis_v4_2(report) → 推飞书
```

---

## 三、D6 情绪周期 v3 设计 (核心模块)

### 3.1 文件: `scripts/duanxianxia_v4_2_d6_emotion.py`

**设计哲学: 少数指标、清晰分工、最少规则、完整刻画水位和方向**

### 3.2 三大家族

```
1. 强势股兑现(P):  median(pct(ZTBX), pct(LBBX))
2. 市场广度(B):    median(pct(advance_share), 1-pct(DT))
3. 接力生态(R):    pct(relay_health)

总水位 = median(P, B, R)   # 三家族等权中位数
方向 = majority(dP, dB, dR)  # 日分位变化, 2-of-3 共识, epsilon=0.03
```

### 3.3 七宫格

```
              UP              FLAT            DOWN
HIGH    HIGH_ACTIVE      HIGH_STAGNATION   RETREAT
MID     EXPANSION        CHOP              RETREAT
LOW     REPAIR           ICE               ICE
```

### 3.4 七相位 → 风险预算

| 相位 | 风险等级 | 仓位上限 | 买点模式 | 开放池 |
|------|----------|----------|----------|--------|
| ICE | CRISIS | 0% | EMPTY | 全关 |
| REPAIR | WARNING | 35% | BOARD_ONLY | 换手封+分歧封+非板 |
| EXPANSION | NORMAL | 80% | AUCTION_AND_BOARD | 全开(一字+换手+分歧+非板) |
| HIGH_ACTIVE | WARNING | 50% | BOARD_ONLY | 一字封+换手封+分歧封 |
| HIGH_STAGNATION | WARNING | 30% | BOARD_ONLY | 仅换手封 |
| RETREAT | CRISIS | 0% | EMPTY | 全关 |
| CHOP | WARNING | 30% | BOARD_ONLY | 换手封+分歧封+非板 |
| UNKNOWN | WARNING | 10% | OBSERVE_ONLY | 全关 |

### 3.5 两个极端否决 (硬止损)

1. **profit_collapse**: ZTBX_t0<0 and LBBX_t0<0 and ZTBX_t1>0 and LBBX_t1>0 (强势股集体翻负)
2. **breadth_panic**: advance_share 极低 (15分位以下) AND DT 极高 (85分位以上)

触发后 → risk_tier=CRISIS, position_cap=0, 全池关闭, 覆盖相位判定。

### 3.6 接力健康度

```
relay_health = 0.55 × smoothed_rate(1进2) + 0.45 × smoothed_rate(2进3)
smoothed_rate = (promoted + 1) / (eligible + 2) × 100   # Laplace 平滑, Beta(1,1) 先验
```

3进4以上不纳入 (样本太小)。

### 3.7 水位阈值 (含滞回)

```
LOW:  level_score < 0.30  →  退出 LOW 需要 > 0.40
HIGH: level_score > 0.70  →  退出 HIGH 需要 < 0.60
```

### 3.8 历史分位

- 需要至少 20 天历史数据才能计算分位
- 不足 20 天: 使用静态阈值回退 (advance_share_15pct=0.20, dt_85pct=20.0)
- 数据质量: 3个家族全有效=GOOD, 2个=DEGRADED, <2个=UNKNOWN

### 3.9 与旧版对比

| 维度 | 旧版 | 新版 |
|------|------|------|
| 相位数 | 9个 | 7个 |
| 水位 | relay 2x加权中位数 | 三家族等权中位数 |
| 方向 | 多日斜率 | 日分位变化 |
| QX | 核心判定参与 | 完全退出 |
| T0冲击 | 多套 shock cap | 2个硬否决 |
| 状态迁移图 | 复杂状态机 | 删除, 每日独立判定 |
| 成熟度/双速/背离 | 多层标签 | 全部删除 |
| 池乘子 | 动态调整 | 恒为1.0 (保留接口) |
| 子阶段 | 无 | ICE(FALLING/BASING), RETREAT(EARLY/SPREADING) 仅诊断标签 |

---

## 四、本次修复的 Bug

### Bug 1: premarket_daily.py 从 captures 读 T0 数据 (已修复: `dd8e7b5`)
- **现象**: 2026-07-15 盘前分析报错 `No capture for auction.jjyd.vratio`
- **根因**: `premarket_daily.py` 调 `run_v4_2_pipeline` 不传 `bundle`, 内部走 `load_premarket_bundle` → 去 `captures/` 目录找 T0 数据。但 9:25 cron 把数据存在了 `reports/` 的 premarket report 里, 不在 `captures/` 目录。
- **修复**: `premarket_daily.py` 现在与飞书推送共用同款逻辑: 找到 `reports/YYYY-MM-DD/premarket/*.json` → `_build_bundle_from_report(report)` → 传 bundle 给 pipeline。

### Bug 2: _build_bundle_from_report 内部调 load_premarket_bundle 导致 T0 缺失时 T-1 也加载失败 (已修复: `daea6ce`)
- **根因**: `_build_bundle_from_report` 内部调了 `load_premarket_bundle(trade_date)` 来获取 T-1 数据。`load_premarket_bundle` 对 T0 数据集用了 `raise_if_missing=True`, 找不到就抛 `DataLoaderError`。即使我们不需要它读 T0 (T0 从 report items 读), T-1 也跟着加载失败。
- **修复**: `_build_bundle_from_report` 不再调用 `load_premarket_bundle`。直接用 `load_capture_at_time` 加载 T-1 的 captures (`raise_if_missing=False`)。T0 数据仍从 report items 的 `capture_path` 读取。

### Bug 3: D6 历史分位每次空起跑 (已修复: `4e6b18e`)
- **现象**: 回测和盘前分析永远输出"数据不足", 仓位永远 0.10
- **根因**: `premarket_daily.py` 和 `backtest_daily.py` 每次运行都从空的 `D6History()` 开始, 历史分位永远到不了 20 天阈值, 永远回退到静态阈值。
- **修复**: 运行前先从 `_audit/v4_2_premarket/` 和 `_audit/v4_2_backtest/` 读取过去分析结果, 从中提取 ztbx_925, lbbx_925, advance_share, dt_925, relay_health 五个值, 构建 `D6History` 传入管线。

---

## 五、数据流完整链路 (盘前分析正确路径)

```
T0 数据来源:
  reports/YYYY-MM-DD/premarket/HHMMSS.json
    → items[].capture_path → captures/YYYY-MM-DD/<file>.json
    → _extract_t0_rows_from_item() 读取 rows

T-1 数据来源:
  captures/YYYY-MM-DD/  (直接 load_capture_at_time, raise_if_missing=False)
    - home.qxlive.top_metrics (T-1 qxlive)
    - home.ztpool (涨停池晋级率)
    - review.fupan.plate (板块复盘)
    - review.ltgd.range (龙头个股)
    - cashflow.stock.* (资金流)
    - home.kaipan.plate.summary (开盘板块)

D6 历史分位来源:
  reports/_audit/v4_2_premarket/*.json  +  reports/_audit/v4_2_backtest/*.json
    → _build_history_from_past_results() 构建 D6History
    → 传入 run_v4_2_pipeline(history=history)
```

---

## 六、已修改的文件清单

| 文件 | 修改内容 |
|------|---------|
| `scripts/duanxianxia_v4_2_d6_emotion.py` | 核心 D6 模块 v3 简化版重写 (805行变更) |
| `scripts/duanxianxia_v4_2_runner.py` | _build_bundle_from_report 重写, emotion 输出字段更新 |
| `scripts/duanxianxia_v4_2_premarket_daily.py` | 从 report 构建 bundle, 加载历史分位 |
| `scripts/duanxianxia_v4_2_backtest_daily.py` | 从 past results 加载历史分位 |
| `scripts/duanxianxia_v4_2_backtest.py` | 字段更新 (t0_impulse → hard_veto) |
| `scripts/duanxianxia_v4_2_batch.py` | 字段更新 |
| `scripts/duanxianxia_v4_2_risk_exec.py` | 无需修改 (RiskTier/BuyMode 枚举不变) |

**未修改的文件 (不需要改):**
- `scripts/duanxianxia_v4_2_d7_router.py` — 无 Phase 依赖
- `scripts/duanxianxia_v4_2_pool_ranker.py` — 无 Phase 依赖
- `scripts/duanxianxia_v4_2_premarket_feishu_rerun.py` — 使用 build_premarket_analysis_v4_2, 已自动适配

---

## 七、服务器当前状态

### 7.1 agent-results 分支最新状态

- 2026-07-15 盘前分析: **error** — 跑的是旧代码 (`load_premarket_bundle` 报错), 服务器还没拉到新代码
- 2026-07-08 ~ 2026-07-14 回测: 全部"数据不足" (旧代码 D6History 空起跑)
- agent_jobs 队列: 最后一次入队是 `hourly_2026-07-15_11`, 之后没有新入队记录

### 7.2 等待服务器拉新代码后验证

服务器下次 `agent_job_worker` 轮询时 `git pull origin main` 会拉到以下新提交:
- `677ab9a` D6 v3 简化版
- `dd8e7b5` premarket_daily 数据源修复
- `daea6ce` _build_bundle_from_report 不再依赖 load_premarket_bundle
- `4e6b18e` D6 历史分位从 past results 加载

拉取后 `agent_daily_refresh` 会重新入队 `premarket_daily` 和 `backtest_daily`, worker 执行后结果推送到 `agent-results` 分支。

### 7.3 首次运行预期

由于 `_audit/v4_2_premarket/` 和 `_audit/v4_2_backtest/` 目录下目前只有旧版 error 结果, 历史分位可能仍然不足 20 天。需要累积几天正常输出后, D6 才会进入完整的 7 相位判定。期间会使用 DEGRADED 模式 + 静态阈值回退。

---

## 八、关键文件路径速查

| 用途 | 路径 |
|------|------|
| 项目根目录 | `/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia/` |
| 脚本目录 | `scripts/` |
| 当日数据 (reports) | `reports/YYYY-MM-DD/premarket/HHMMSS.json` |
| 历史数据 (captures) | `captures/YYYY-MM-DD/` |
| 盘前分析结果 | `reports/_audit/v4_2_premarket/YYYY-MM-DD.json` |
| 回测结果 | `reports/_audit/v4_2_backtest/YYYY-MM-DD.json` |
| Agent 任务队列 | `scripts/agent_jobs/queue/` |
| Agent 任务结果 | `reports/_audit/agent_jobs/` |
| 调度器 | `scripts/agent_daily_refresh.py` |
| Worker | `scripts/agent_job_worker.py` |
| Worker 启动脚本 | `scripts/agent_job_runner.sh` |
| 飞书推送重跑 | `scripts/duanxianxia_v4_2_premarket_feishu_rerun.py` |
| Cron 入口 | `scripts/duanxianxia_premarket_v7_runner.py` (ACTIVE_ENGINE = build_premarket_analysis_v4_2) |
| 主 batch 文件 | `scripts/duanxianxia_batch.py` |

---

## 九、D6 自测

```bash
cd scripts && python3 duanxianxia_v4_2_d6_emotion.py
# 8 个测试全部通过
```

---

## 十、待办事项

- [ ] 服务器拉新代码后验证 premarket_daily 和 backtest_daily 正常产出
- [ ] 累积 20 天以上历史分位数据后验证 D6 7 相位判定准确性
- [ ] 观察飞书推送重跑是否正常 (feishu_analysis_push 任务)