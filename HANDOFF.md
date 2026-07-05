# AGENT HANDOFF — 盘前选股 + IPO 日历项目交接文档

> 这份文档是为**新对话 / 新 agent** 写的。只要读完本文 + `FIELD_VALUE_REVIEW.md`, 你就能: (1) 明白怎么"连上"部署在
> Google Cloud 上的这个项目、怎么让代码跑在服务器上并拿到真实数据; (2) 了解项目目前进展、多轮验证发现、结论;
> (3) 知道还差什么、下一步往哪走, 无缝接上继续迭代。作者: 历任会话的 Notion AI agent。最后更新: 2026-07-05。

---

## 0. TL;DR (一分钟上手)

- **仓库**: GitHub `kobolibra/short-term-pre-market-analysis`。两个分支: `main`(代码+任务队列), `agent-results`(服务器跑出的结果)。当前 `main` HEAD = `f112fd0`。
- **你(agent)不能直接 SSH 登服务器**。你和 GCP 服务器之间的唯一通道是 **GitHub**(用 GitHub MCP 工具读写)。读用 `mcpServer_github3`, 写用 `mcpServer_github7`。
- **连服务器看原始数据的方式 = "git 队列 + 服务器上的 cron worker"**:
  1. 你把一个 Python 脚本 push 到 `main/scripts/*.py`;
  2. 再 push 一个任务描述文件到 `main/scripts/agent_jobs/queue/<id>.json`;
  3. 服务器上的 cron 每隔几分钟拉一次 `main`, 跑你的脚本(脚本能读服务器本地的全部原始数据), 把输出写回 `agent-results` 分支;
  4. 你从 `agent-results` 分支读结果。
- **你自己的 web 抓取工具抓不了某些站(如 9fzt)** ——只有服务器上的 urllib 脚本能读。凡是"读服务器数据/抓外部站/跑回测"都走排队作业。
- **两条并行工作线**: (A) 盘前选股模型迭代; (B) IPO 日历自动推送。
- **最新阶段(2026-07-05, jobs ~0148–0155)**: 用户明确纠偏「现有数据都没吃透, 别急着加新数据」→ 转入**现有候选池数据全维度榨干**, 以真实买入档盈亏做最终裁判。已系统性证明 **因子权重/线性空间已到相关性天花板**(0151–0153, 别再重跑, 数值在 §7)。**0154 买入级 A/B 已出结果: `risk_strict`(过闸门前剔除任何 risk_flag)全面胜出——overall 胜率 0.615→0.769、payoff 1.744→2.322(不塌)、avg_loss −3.69→−2.97;`profile`=no-op;`cap_auction` 有害。** 已按 §4.1 顺延排 **0155 风险剔除严格度参数敏感性 A/B(结果待取)**。详见 §2.9 与 §7。**下一个 job id = 0156。**

---

## 1. 怎么"连上"服务器项目 (最重要)

### 1.1 架构: git-as-queue + cron worker

```
  你(agent)                GitHub                         GCP VM (服务器)
  --------                  ------                         ----------------
  写 scripts/X.py   --push-->  main  <----git pull (cron)---- agent_job_runner.sh
  写 queue/<id>.json --push--> main                          -> git reset --hard origin/main
                                                              -> agent_daily_refresh.py (自动排队日常刷新)
                                                              -> agent_job_worker.py (跑 queue 里的脚本)
                                                              -> 脚本读服务器本地原始数据 + 写输出
  读结果       <--pull----  agent-results  <--force push-- publish_results() (commit-tree)
```

### 1.2 服务器上的关键路径与脚本

- 工作目录 `WS = /home/investmentofficehku/.openclaw/workspace`
- cron 入口: `scripts/agent_job_runner.sh` — `git reset --hard origin/main`(硬同步) -> `agent_daily_refresh.py`(排日常刷新) -> `agent_job_worker.py`(执行队列) -> `publish_results()`(用 `git commit-tree`+`push --force` 发布到 `agent-results`)。
- worker: `scripts/agent_job_worker.py`
  - 扫 `scripts/agent_jobs/queue/*.json`; 结果写 `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json`。
  - **幂等**: 结果文件已存在则跳过(同一 id 不重跑)。**重跑请用新 id**。
  - **安全白名单**: 只能跑 `scripts/` 下的 `.py`。默认 timeout 1800s, stdout/stderr 各截断 16000 字符。
  - 心跳: `.../agent_jobs/_worker_heartbeat.json`。
- **节奏**: cron 大约每 10 分钟一次(不是实时)。push 后要等下一个 tick 才会跑。**别刚 push 完就期待立刻有结果**。
- ⚠ **发布(publish)管线可能积压数小时**: 观察到"脚本按时跑完(如 01:20)但 `agent-results` 迟迟不更新(~7h 后才推; 后收敛到 ~2.6h)"。**`agent-results` 的 HEAD 冻结 ≠ 计算停了**, 多半是推送积压。想强制前台跑一遍并立即发布, 在 VM 上执行 `bash scripts/agent_recover.sh --run`(诊断 + 前台跑一轮 + 推送)。
- ⚠ **VM 时钟曾比真实时间快约 68 分钟**, 解读提交/结果时间戳时按 VM 时钟算。

### 1.3 怎么提交一个作业 (标准流程)

1. 先把脚本 commit 到 `main/scripts/<name>.py`。
2. **等脚本 commit 成功后**, 再 commit 队列文件 `main/scripts/agent_jobs/queue/<id>.json`。
3. **切记: 对 main 的提交必须串行**(一个一个来)。并行双提交会 409 冲突。(用 `push_files` 可在单次提交里放多文件, 规避串行冲突。)
4. 队列文件格式: `{ "id":"0156", "script":"scripts/xxx.py", "args":[], "timeout":900, "note":"人话描述" }`。`id` 必须唯一且与文件名主干一致。
   - **已用到的最大 id: `0155`。下一个用 `0156`。**
   - ⚠ 覆盖一个已存在的 queue/脚本路径要先读出其 `sha` 再传; 否则报 "File already exists"。
5. 读结果: 从 `agent-results` 读 `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json`(`ok`/`rc`/`stdout_tail`/`stderr_tail`/`duration_s`), 以及脚本写出的报告文件(同在 agent-results)。
   - ⚠ stdout_tail 是"尾部"截断; **把最关键的输出放在脚本最后打印**, 大 dump 易被从前端截掉(0072 踩过)。单行紧凑输出更稳(`chr(10).join`, 避免对可能为 None 的值用 `%f`)。

### 1.4 人工在 VM 上跳过队列直接跑(调试用)
```bash
cd /home/investmentofficehku/.openclaw/workspace \
  && git fetch origin main && git reset --hard origin/main \
  && /usr/bin/python3 scripts/<X>.py [args]
```

### 1.5 用 GitHub MCP 怎么操作 (你手边的工具)
- 连接键: **读 = `mcpServer_github3`**(只读工具), **写 = `mcpServer_github7`**(可写工具)。均经 `connections.mcpServer_githubN.runTool({ toolName, toolArguments })` 调用。
- 读: `get_file_contents({ owner, repo, path, ref })`。`ref`=`refs/heads/main` 读代码/队列; `refs/heads/agent-results` 读结果。也可列目录(传目录 path)。
- 写: `create_or_update_file({ owner, repo, branch, path, message, content[, sha] })`(改已有要先读 `sha`); 或 `push_files({ owner, repo, branch, files:[{path,content}], message })`(单次多文件, 整文件覆盖)。
- 找文件: `search_code` 只索引默认分支(main); 非默认分支的文件搜不到, 要用列目录/`get_commit` 定位。`list_commits(sha:agent-results)` 在结果分支只返回 1 条(每次 publish 重写历史)。
- GitHub 提交**不是** Notion 变更, **不要**加 `editDescriptionVariableName`。
- owner=`kobolibra`, repo=`short-term-pre-market-analysis`。

---

## 2. 项目 A — 盘前选股模型 (核心)

### 2.1 最终目标
顶级的盘前分析选股代码模型, 迭代方向 **高胜率 + 高赔率**。用服务器上每日下载的真实数据, 以 walk-forward 出样本验证为准绳, 迭代出更好的选股效果。
**原则: 数据驱动, 不猜测; 改线上公式前必须过 walk-forward / A/B 出样本验证。**

### 2.2 唯一正确的预测口径
```
excess_ret = 收盘涨幅 - 竞价涨幅 = (close - open) / preclose * 100
```
- open 就是集合竞价(开盘)价。盘前选股赚的是"开盘买入 -> 收盘"的超额, 所以才减掉竞价涨幅。
- 真盘前过滤: 抓取文件名 `HHMMSS <= 093000`(排除 10:02 的 pool.* 等盘中表)。
- 助手: `v10_optimize.py` 里 `Daily(root).excess(code,date)` 直接给该口径。

### 2.3 生产线公式 (已验证为最优, 勿随意改)
- **v10_amt edge** (生产中, `duanxianxia_v9_edge.py::compute_edge_v9`):
  ```
  edge = clip(0.32*amt_pct + 0.09*auction_strength + 0.24*liquidity
            + 0.16*money + 0.14*pressure_score + 0.03*weimai_strength
            + 0.02*orderbook - risk_penalty, 0, 100)
  ```
  - 7 个权重全部可被 params 覆盖(`edge_w_amt` 0.3232 / `edge_w_auction` 0.0909 / `edge_w_liquidity` 0.2424 / `edge_w_money` 0.1616 / `edge_w_pressure` 0.1414 / `edge_w_weimai` 0.0303 / `edge_w_orderbook` 0.0202)。
  - `amt_pct` = 竞价成交额当日横截面百分位(0–100)。
  - **0152 已证: 线性重配这 7 个权重无增量, 不要再动权重**(见 §2.9 / §7.3)。
- **决策层**(`duanxianxia_v9_output.py::_assign_actions` + `REGIME_ACTION_GATE`): regime 自适应分位闸门打 BUY/WATCH/DROP。现行闸门: cold{买入分位0.015, floor50, max_buys1} / cold_to_warming{0.030,48,3} / warming{同} / normal{0.050,45,4} / hot{0.080,42,5}; risk 行买入需 +8 余量。**cold 每天只买 top-1。**
- `edge_components.sub` 每个候选都带全部 7 因子值 + 诊断字段(风险闸门: 高开/低流/假封/FAKE_STRENGTH/高位连板/前日炸败/leader_fade, 阈值见 §7.6), 分析时可直接读, 无需重算原始。

### 2.4 生产代码链路 (谁算什么)
- `duanxianxia_premarket_v9_runner.py::run_v9(date, root)` — 入口。
- `duanxianxia_v9_assemble.py::assemble_v9(...)` — 六层装配, 注入 amt_pct 百分位, 逐行调 compute_edge_v9。
- `duanxianxia_v9_edge.py::compute_edge_v9(...)` — 算 `edge_score`(干净 v10 公式)。**不算 final_score**。
- `duanxianxia_v9_output.py::shape_v9_output(...)` — 排序 + `_assign_actions` 打 BUY/WATCH/DROP(regime 自适应分位闸门), 输出 `all_candidates`。
- **输出**: `projects/duanxianxia/reports/<date>/premarket/*_analysis_v9.json`。日线: `projects/duanxianxia/dailyline/stocks/<code>.csv`(需 `tradestatus==1`)。

### 2.5 分析/迭代脚本助手 (只读, 跨全史)
- `scripts/v10_optimize.py` — 主优化管线 + 复用助手: `Daily(PROJECT_ROOT).excess(code,date)`, `spearman(xs,ys)`(对数少/零方差返回 None), `daily_ic`, `mean_icir`, `_norm`, `code_of`, 常量 `CORE_FIELDS`/`RANK_FIELDS`, `DEFAULT_PROJECT_ROOT`。
- `scripts/v12_reflection.py`, `v28_risk_filter_validation.py`, `v29_blend_optimize.py`, `v30_marginal_ic.py` — 反思/风控/混合/边际 IC 实验。
- 环境无 scipy/sklearn/lightgbm/statsmodels → 纯 numpy/手写统计。

### 2.6 早期关键结论 (jobs 0038-0039, 模型层)
- **v11 混合公式否决**: 加两个高 ICIR 交互项(amt_x_auc, money_x_liq)不提升 OOS(共线冗余)。生产公式不改。
- **原始加权和 >> 逐日百分位复合**(OOS IC 0.126 vs 0.089)。不要再用纯百分位复合口径。
- **final_score 谜题定论**: 全链路从不算 final_score, 决策只走 edge_score。
- BUY 动作极稀但极优(n=15, mean_excess 4.1, win 0.87)。

### 2.7 环境 (GCP VM)
Python 3.10.12; numpy 2.2.6 / pandas 2.2.3 / torch 2.11.0+cpu / sympy 1.14.0。**缺** scipy/sklearn/lightgbm/xgboost/statsmodels/joblib。

### 2.8 逐表字段价值评审 (jobs 0050–0074, 2026-06-28) — ★ 已全部完成

> 用户的标准任务: "盘前下载数据表逐一过, 每张表告诉我哪些指标有价值、哪些没价值, 既解读数据本身又分析背后信息原理, 逐表分析, 没完成前不看下一张。"
> **完整逐字段台账 = 根目录 `FIELD_VALUE_REVIEW.md`(权威, 先读它)。** 下面是浓缩版。

#### 评审方法与口径(踩坑后定下的硬规矩)
- 数值字段: 逐日横截面 Spearman IC + 跨日 mean/ICIR(日内>=8对)。分类字段: 按档分组的去市场均值平均超额。
- **0068 教训**: raw 分档均值会被极少数异常日灌大 -> 必须 **per-date 去均值 + 看胜率 + 异常日敏感性**, 否则把肥尾噪声误判为非线性 alpha。
- **0060 教训**: 下字段结论前必须回看 raw 原始向量并跨表对照(语义易认错)。
- **0064 教训**: 同字段在不同 cohort 有效性可能反转, 必须分 cohort。

#### 八张盘前表逐表定论
| # | 表 (dataset_id) | 抓取 | 定论 |
|---|---|---|---|
| ① | 封单 auction.jjlive.fengdan | 09:2x | board_label(剔除已封/昨板)✅; 表头总量过热反向择时✅; 金额/撤单率❌ |
| ② | 抢筹 auction.jjyd.qiangchou | 09:2x | **全项目最强源**: 竞价成交额万 IC0.163✅、换手率 0.135✅、gap✅; grab_strength/量比冗余❌ |
| ③ | 量比 auction.jjyd.vratio | 09:2x | 量比独立无效(-0.028证伪)❌; 仅作"高成交额内偏低量比"条件过滤✅ |
| ④ | 委买 auction.jjyd.weimai | 09:2x | 竞价主力净额/成交额 IC0.120✅(唯一真实资金流); 净流出剔除✅; 拆单字段❌ |
| ⑤ | 净额 auction.jjyd.net_amount | 09:2x | gap 全覆盖最佳来源✅; 小盘(<100亿)剔除✅; 主力净额需在涨停cohort内条件化 |
| ⑥ | 开盘啦板块 home.kaipan.plate.summary | 09:2x | ❌ 选股无正价值(in_top1 IC-0.066, 分桶反向); 板块字段全丢弃 |
| ⑦ | 热度榜 rank.hot_stock_day + 飙升榜 rank.rocket | **09:25** | 名次❌, 价值在两榜交叉(见下) |
| ⑧ | 情绪指标盘 home.qxlive.top_metrics | 09:28 | ✅ **首个市场级逆向择时**: QX/ZT/ZTBX/LBBX/LBGD timing IC -0.6~-0.74 |

#### 表⑦ 多轮验证发现(含一次自我纠正)
- **命名(0073 官方 dataset_label)**: `rank.hot_stock_day`=**热度榜(日)**=绝对热度 LEVEL(千万级元); `rank.rocket`=**飙升榜**=小时增量 DELTA/动量(带符号万级)。二者均 **09:25 真盘前**抓取。勿与 10:02 盘中的 `pool.hot/pool.surge` 混淆。
- **名次无效(0066+0074)**: 两榜 rank/raw_rate 线性 IC 与 per-day spearman 全 ≈0 -> ❌ 只保留"是否上榜"布尔 + 交叉关系。
- **level×delta 交叉(0074, edge 真正所在)**: C 热度top10且非飙升(滞涨主线大票) ICIR 0.385/胜率71% ✅稳健底仓; B 飙升top10且非热度top20(新晋边际资金) 均值+4.1但胜率44% ⚠进攻彩票仓(×抢筹优先, 须仓控); A 两榜霸榜龙头 -0.69/胜率35% ❌回避。

#### 跨全表"有价值字段"汇总(给 v10 用)
- 🟢 独立选股 alpha: 竞价成交额万(0.163)、竞价换手率(0.135)、竞价主力净额/成交额(0.120)、gap。**组合 comp_SD{成交额+换手率+gap}去相关 IC 0.179/ICIR 0.93**。
- 🟡 条件/过滤: 高成交额内偏低量比、涨停cohort内主力净额、小盘剔除、净流出剔除、板位剔除、封单过热反向。
- 🔵 市场级择时 overlay: qxlive 情绪逆向(QX/ZT/ZTBX/LBBX/LBGD)。情绪高减仓、冷加仓。
- 🟣 卫星仓: 热度/飙升交叉(看C / 搏B×抢筹 / 躲A)。
- ⚪ T-1 滞后: 仅 review.fupan.plate 昨日成交额 IC 0.103 有价值。
- ❌ 明确废弃: 各种榜的 rank/raw_rate/value、grab_strength、量比倍数、拆单字段、kaipan 板块全字段、HSLN/KQXY/PB 死字段、飙升 top10 肥尾彩票。

### 2.9 现有数据全维度榨干 + 买入级 A/B (jobs ~0148–0155, 2026-07-05) — ★ 当前主线

> 用户本轮纠偏(原话意译): "现有的数据你都没利用好, 天天想着加数据, 现有的数据你还没搞明白呢!" → 方向从"加数据(龙虎榜/L2/北向)"转为**先把现有候选池数据全维度榨干**, 用真实买入档盈亏做最终裁判。

**研究链**: IC 挖掘(0151) → 线性权重 A/B(0152) → 共线/去相关/交互/顶档诊断(0153) → 买入级 A/B(0154, ✅risk_strict 胜出) → 风险剔除严格度 sweep(0155, 结果待取)。**核心逻辑: IC/横截面只是中间量, 最终裁判是买入档(实际就买 top1–2)的真实胜率/赔率/回撤。改线上公式必过 A/B(0144 教训: IC 涨 ≠ 钱涨)。**

#### 战略结论(本轮最重要产出): 因子权重/线性空间已被榨干
- 核心 5 因子高度共线(Spearman 0.54–0.83, 如 amt–auction .825 / auction–liq .814 / liq–money .833) → 线性重配无效。
- 交互项无独立 alpha, 去相关残差微弱(auction 控 liquidity 后偏 IC 0.075→0.038) → 非线性/正交也基本走不通。
- 增量只可能来自: ① 选股画像(高流动 + 高资金 + **中档**竞价) ② 风险层(顶档 59% 带险) ③ 真正正交的新数据(仅在现有数据榨干后才解禁)。

#### 逐 job
| job | 状态 | 定论 |
|---|---|---|
| 0148 replay v2 | ✅ | 修竞价泄漏(leak)口径后重放, 回撤显著收敛; 样本含污染窗口, 仅参考 |
| 0149 每日覆盖审计 | ✅ | `auction_amount_pct` 06-01~06-18 因历史 bug 恒 0(回测里约 11/20 天死), 06-23 起生产已每日 live(IC≈0.048) → **切勿据回测把 amt 权重砍到 0** |
| 0150 replay v3(干净 8 天/全 cold) | ✅ | 当前代码不赢 STORED 原始收益; ctw「利润引擎」在干净数据上**未证实** → 自我纠正过度乐观, n 小只作方向 |
| 0151 全宇宙因子 IC | ✅ | 最强单因子: 竞价成交额万 0.0815、auction_strength 0.0749(cov1.0)、liquidity 0.0673; risk_penalty 方向正确(负相关) |
| 0152 edge 权重 A/B(5 组) | ✅ | mean_ic 全距仅 0.004(噪声内); 集中化把顶档 spread 腰斩(1.47→0.86) → **不上线任何重配**, baseline 最优 |
| 0153 共线/去相关/交互/顶档 | ✅ | 相关性天花板坐实; 交互全 ≤ 单因子; 顶档画像=高 liq(88.8)+高 money(87.9)+**中档 auction(41.4)**; 顶档 risk_flag 命中 59% |
| 0154 买入级 A/B | ✅ | **risk_strict 全面胜出**(overall win 0.615→0.769 / payoff 1.744→2.322 / avg_loss −3.69→−2.97, n 26→13);profile=no-op(赢家本就 liq/money 双高);cap_auction 有害(win→0.417, 高竞价强度行在买入档恰是赢家, 推翻横截面"中档竞价"画像)。原始数值见 §7.7 |
| **0155 风险严格度 sweep** | 🟡 **结果待取** | risk_strict 后续参数敏感性: risk_penalty 阈值(≥10/20/30/45)+ 按类别(veto/lowliq/highboard/highopen)渐进剔除, 定位胜率提升的驱动源与最优严格度; 变体定义见 §7.8 |

#### 0154 读法(已想好, 新对话直接用)
- 若 `risk_strict` 或 `profile` 的 **win_rate↑ 且 payoff 不塌** → 找到可上线买入过滤器 → 参数敏感性 A/B → 经 `_assign_actions` 前置过滤或 `compute_edge_v9` 上线(**必过 A/B**, 保留"顶档只买 top1-2、不稀释"洞见)。
- 若三个变体都不优于 baseline → 现有数据在买入层已近最优, 增量必须来自**真正正交的新数据**(此时才解禁"加数据"红线)。
- **预防针**: cold 每天只买 top-1(`max_buys=1`), 20 天买入样本≈20, 过滤后更少 → 只作**方向性证据**, 按 n 加权, 禁止再过度乐观。

#### 0154 实际读出 → 0155(已执行)
- 结果: `risk_strict` win_rate↑(0.615→0.769)且 payoff 不塌(1.744→2.322, 回撤同步改善) → 命中"可上线买入过滤器"分支。`profile` 与 baseline 三档全同(在实际 BUY 集不 bind)→ 无独立增量。`cap_auction` 有害, 否决。
- 代价: risk_strict 把买入样本减半(overall 13/cold 6/ctw 7, 且 ctw 仅 3 天)→ 只作方向性证据, **上线前须先做参数敏感性 A/B**。
- 故排 **0155**: 不再一刀切剔除任何 risk_flag, 而是按 risk_penalty 阈值 + 单一风险类别渐进剔除, 判断(a)胜率提升是否单调、(b)温和阈值能否保样本、(c)哪类风险驱动改善(见 §7.8)。取到 0155 后按其结果决定最终上线的过滤器强度。

#### 关键脚本(本轮)
- `scripts/duanxianxia_factor_ic_study.py`(0151) 全宇宙 Spearman 因子 IC。
- `scripts/duanxianxia_edge_weight_sweep.py`(0152) 5 组权重 IC + 分位单调性。
- `scripts/duanxianxia_factor_decorrelation.py`(0153) 相关矩阵 + 边际/偏 IC + 交互 + 顶/底档画像 + risk_flag 率。
- `scripts/duanxianxia_buy_level_ab.py`(0154) 买入级 A/B, 输出 `reports/_audit/buy_level_ab_0154.json`。
- `scripts/duanxianxia_risk_strictness_sweep.py`(0155) 风险剔除严格度 sweep(复用 0154 helpers), 输出 `reports/_audit/risk_strictness_sweep_0155.json`。

---

## 3. 项目 B — IPO 日历自动推送

### 3.1 需求
从 9fzt 取数据, 每天 8:00 把当日「申购/网上申购缴款/上市日期=今天」的股票推送到飞书。只用 9fzt 来源, 不引用未核实日期。

### 3.2 已破解的 9fzt IPO API (权威)
- URL: `https://api-hq.chongnengjihua.com/news/api/1/stock/a/ipo/list` (GET, `/news` 前缀必须有)。
- 参数: `{ listedSector(0全部/1沪主/2深主/3科创/4创业), pageNum, pageSize, sortField:"onlineStartDate", sortType:0 }`。响应 `{code,data:{count,ipoList},message}`, 成功 code=0。
- **签名(关键)**: header `signature = md5("sjdxfnqogbzoun13d971ckh8p" + 参数值按key字典序拼接.join("") + msTimestamp)`(小写hex); header `timestamp = msTimestamp`(毫秒)。
- 字段: prodName(名称)/symbol(代码)/onlineStartDate(申购日)/payDateOnline(网上缴款日)/listedDate(上市日)/issuePrice 等。
- 参考实现已跑通: `scripts/ipo_calendar_fetch_real.py`(job 0037 返回 500 行/count 5550)。

### 3.3 脚本/路径/飞书
- 主推送脚本: `scripts/ipo_calendar_notify.py`(最后可用提交 `28da81d`)。**待接入签名 API**。
- 输出: `projects/ipo_calendar/reports/_audit/`。数据: `projects/ipo_calendar/data/<date>/`。
- 飞书 webhook 在服务器 `.env`(`/home/investmentofficehku/.openclaw/.env` 与 `.../workspace/.env`); 变量名按顺序读 `IPO_FEISHU_WEBHOOK_URL`/`FEISHU_WEBHOOK_URL`/`LARK_WEBHOOK_URL`/... ; 签名密钥 `IPO_FEISHU_SIGN_SECRET` 等。

---

## 4. 还需要完善的 + 下一步方向 (接手后继续)

### 4.1 盘前模型 (主线, 当前状态见 §2.9)
**按此顺序推进:**
1. **[已完成]** 取 `0154.result.json`(on `agent-results`): `risk_strict` 胜出(win↑ 且 payoff 不塌, 见 §2.9/§7.7)。
2. **[进行中]** 参数敏感性 A/B 已排 **0155**(风险剔除严格度 sweep, 见 §7.8)。取 `0155` 结果(agent-results 上 `risk_strictness_sweep_0155.json` + `0155.result.json`)后: 若 risk_strict 或某温和阈值稳健(win↑、payoff 不塌、样本损失可接受)→ 经 `_assign_actions` 前置过滤上线(剔除对应 risk 行, **必过 A/B**, 保留"顶档只买 top1-2、不稀释"洞见)。若结果缺失且 HEAD 冻结, 多为 publish 积压, 提示用户在 VM 跑 `agent_recover.sh --run`。
3. **[继续榨现有数据]** regime 条件化(ctw 三天 ctw_ic≈0.20 >> cold, 虽 n=3, 提示"择态 > 择权重"); 顶档非单调 auction_strength 处理(压极值)。
4. **[仅在上述榨干后]** 才考虑真正正交的新数据(龙虎榜席位/L2 逐笔/北向分钟流)。

**历史里程碑(已完成)**: 逐表字段评审(0050–0074, 见 `FIELD_VALUE_REVIEW.md`) → `compute_edge_v9`/v10_amt 上线 → 0144 门控改动经 A/B 证明净负、已回退。

**低优先 backlog**: master `build_master_panel` ltgd 多窗口塌陷修复; v9 `matched_plate=0.0` 修复; 原始盘前 capture 归档(raw_capture_days=0 阻塞全链路 replay); 周一(07-06)盘前9:25/盘中10:01/盘后17:20 回执监控(确认 retry+60s floor+cron 存活, 注意 publish 滞后 + VM 时钟偏移)。

### 4.2 IPO
1. 把 `ipo_calendar_notify.py` 接入签名 API(复用 `ipo_calendar_fetch_real.py` 的签名+分页), 映射字段, 筛"申购/缴款/上市=今天", 推飞书, 保留 HTML/debug fallback。
2. 排一个 no-send 测试: args `["--date","<today>","--no-send","--run-weekends"]`, 验 raw_rows>0 / event_count 合理 / send_result.skipped。

---

## 5. 用户偏好 / 踩过的坑
- 用户要**结果**, **不要反复问确认**("不要反复问我确认, 直接给结果"), 不要"什么都靠试"——先看明白源码/页面再动手。要求**顶级效率 + 专业水准 + 独立分析**; 授权 agent 自行判断门槛/参数取值。
- **无原始证据不下结论**; 上线前必过 A/B 回测; 一切改动**加法式、可回退**; **不重写** `fetcher.py`/`batch.py`; 不改口径/字段名。
- "随时记录前面的讨论"——重要进展要及时写进 `FIELD_VALUE_REVIEW.md` / 本文档(HANDOFF.md)。**交接记录放 GitHub, 不放 Notion。**
- 用**中文**回复; n=20 样本务必标注不确定性; 不猜输入类型。
- 访问 9fzt 的权限用户已反复授权过, **不要再反复要求授权**。
- push 到 main 即时; VM 只在 worker tick 时 pull, 结果有延迟; publish 管线可能积压数小时(HEAD 冻结≠没跑)。并行提交 main 会 409 -> 脚本先、队列后、串行(或用 push_files 单提交多文件)。
- worker 幂等(结果存在即跳过) -> 重跑用新 id; 覆盖已存在路径要带 sha。
- stdout_tail 尾部截断 -> 关键输出放最后, 单行紧凑。
- **方法论元教训**: 线性 IC 判无价值 -> raw 分档均值判有价值 -> 严格检验(per-date 去均值/胜率/异常日)才见真相; 单表名次常无用, 价值常在跨表 level+Δ 交叉; **IC 涨不等于钱涨, 最终以买入档真实盈亏为准**。
- 不要引用未核实的日期。
- **安全待办**: 用户曾在对话里明文粘贴 Fine-grained PAT → 建议 **revoke + 轮换**(尚未处理)。

---

## 6. 快速参考 — 关键路径速查
| 用途 | 路径 / 值 |
|---|---|
| 仓库 | `kobolibra/short-term-pre-market-analysis` |
| 代码/队列分支 | `main` (当前 HEAD `f112fd0`) |
| 结果分支 | `agent-results` |
| **逐表字段评审台账** | 根目录 `FIELD_VALUE_REVIEW.md` (先读) |
| 本交接文档 | 根目录 `HANDOFF.md` |
| 服务器工作目录 | `/home/investmentofficehku/.openclaw/workspace` |
| 队列目录 | `scripts/agent_jobs/queue/<id>.json` |
| 结果目录(盘前) | `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json` |
| worker 心跳 | `projects/duanxianxia/reports/_audit/agent_jobs/_worker_heartbeat.json` |
| 评审/研究报告输出 | `projects/duanxianxia/reports/_audit/*.{json,md}` |
| 每日 v9 分析 | `projects/duanxianxia/reports/<date>/premarket/*_analysis_v9.json` |
| 日线 csv | `projects/duanxianxia/dailyline/stocks/<code>.csv` |
| IPO 输出 | `projects/ipo_calendar/reports/_audit/` |
| **下一个 job id** | **`0156`** |
| cron 入口 | `scripts/agent_job_runner.sh` |
| 发布恢复(HEAD 冻结时) | `bash scripts/agent_recover.sh --run` (VM 上强制前台跑+推送) |
| worker | `scripts/agent_job_worker.py` |
| 生产 edge 公式 | `scripts/duanxianxia_v9_edge.py::compute_edge_v9` (7 权重可 params 覆盖) |
| 决策闸门 | `scripts/duanxianxia_v9_output.py::_assign_actions` + `REGIME_ACTION_GATE` |
| 当前主线脚本 | `scripts/duanxianxia_risk_strictness_sweep.py` (0155 风险严格度 sweep); 前序 `scripts/duanxianxia_buy_level_ab.py` (0154) |
| MCP 连接键 | 读=`mcpServer_github3` / 写=`mcpServer_github7` |

---

## 7. 原始结果附录 (防重跑) + 关键 SHA / 回测口径

> 这些是已完成研究的**完整原始数值**, 放这里就是为了新对话**不必重跑 0151–0154**。要重现口径见对应脚本。

### 7.1 回测窗口与 regime
- 窗口 = 20 个交易日(2026-05-21 ~ 07-03)。regime 构成: cold 17 天 + cold_to_warming(ctw) 3 天(无 warming/normal/hot 天)。
- 因此 ctw 结论 n=3, cold 结论 n≈17; 买入档(cold max_buys=1)全窗口买入样本 ≈ 20, 过滤后更少 → 一切结论按 n 加权, 标注不确定性。

### 7.2 0151 全宇宙因子 IC(Spearman, 逐日横截面均值)
- 竞价成交额万 0.0815 | auction_strength 0.0749(cov 1.0) | liquidity 0.0673 | auction_amount_pct 0.0546(近期才 live) | money / pressure_score 中等 | weimai_strength 弱 | orderbook 弱。
- risk_penalty 与超额负相关(方向正确, 风险惩罚有效)。
- edge_score 综合 IC ≈ 0.064; 信号高度集中于顶档(第 8–10 十分位), 1–7 档基本噪声。

### 7.3 0152 edge 权重 A/B(5 组)
| 组 | mean_ic | IR | cold_ic | ctw_ic | topDec | spread |
|---|---|---|---|---|---|---|
| baseline | 0.0636 | 0.481 | 0.0396 | 0.1997 | **1.473** | **1.736** |
| ic_prop | 0.0602 | 0.473 | 0.0338 | 0.2098 | 1.341 | 1.443 |
| auction_heavy | 0.0609 | 0.493 | 0.0351 | 0.207 | 1.334 | 1.474 |
| full_cov | 0.0638 | 0.530 | 0.0388 | 0.2053 | 0.88 | 1.134 |
| drop_dead | 0.0641 | 0.532 | 0.0397 | 0.2024 | 0.86 | 1.104 |
- 结论: mean_ic 全距仅 0.004(SE≈0.03, 噪声内); IR 最高的 full_cov/drop_dead 反而把顶档 spread 腰斩(1.47→0.86)。`best_by_spread = baseline`。**任何重配都不上线, 保持 baseline 权重。**

### 7.4 0153 共线/去相关/交互/顶档诊断
- edge_ic = 0.0636。
- 边际 IC: auction_amount_pct 0.0482 | auction_strength 0.0749 | liquidity 0.0673 | money 0.0461 | pressure_score 0.0558 | weimai_strength 0.0158 | orderbook 0.0352。
- 偏 IC(auction | liquidity) = 0.0378(相对边际 0.0749 → 约一半独立)。
- 交互 IC(rank 乘积): auction×liq 0.0681 | auction×money 0.0621 | liq×money 0.0656(全部 ≤ 最强单因子 0.0749 → 无交互 alpha)。
- Spearman 相关矩阵(核心簇): amt_pct↔auction 0.825 | amt_pct↔liq 0.795 | auction↔liq 0.814 | auction↔money 0.658 | liq↔money 0.833 | liq↔pressure 0.68。weimai/orderbook 近正交但本身弱。
- 顶档赢家画像(平均百分位): liquidity 88.8 | money 87.9 | amt_pct 66.1 | **auction_strength 仅 41.4(中档!)** | pressure 49.5。底档: auction 8.5 | liq 25.6 | money 11.6。
- 顶档 risk_flag 命中率 = 0.59。
- 解读: 赢家 = 高流动 + 高资金 + **中档竞价**; auction_strength 在极端处饱和/反转(解释 0152 集中化把顶档打崩)。

### 7.5 0154 买入级 A/B — 变体精确定义(已完成, 结果见 §7.7)
`scripts/duanxianxia_buy_level_ab.py`: 用当前代码(无污染)对每日候选重算 `compute_edge_v9` 的 edge/risk, 再套真实 `_assign_actions` 闸门, 比较 4 个选股变体在**实际 BUY 集**上的次日超额(`daily.excess`), 分 overall / cold / ctw 三档, 用 `_agg` 汇总(n / mean_excess / win_rate / avg_win / avg_loss / payoff):
- `baseline` = 现行 REGIME_ACTION_GATE 的 BUY 集。
- `risk_strict` = 先剔除 risk_flag 行再过闸门(针对顶档 59% 带险)。
- `profile` = 要求 liquidity ≥ 当日中位数 **且** money ≥ 当日中位数, 再过闸门(赢家画像)。
- `cap_auction` = 剔除 auction_strength ≥ 当日 90 分位 的行, 再过闸门(赢家只需中档竞价)。
- 导入: `Daily, DEFAULT_PROJECT_ROOT, compute_edge_v9, _assign_actions, _regime_label`; 助手 `_decision, _median, _pct_threshold(q=0.90), _agg, _buy_exs`。输出 `projects/duanxianxia/reports/_audit/buy_level_ab_0154.json`。队列文件 `0154_buy_level_ab_20260705.json`。

### 7.6 关键文件 SHA / 提交链 / 风险闸门阈值
- `scripts/duanxianxia_v9_edge.py` = `87e2d8ff` — `compute_edge_v9`。风险闸门阈值: high_open_cost(≥7%, +14) / low_liquidity(≤35, +12) / fake_seal(+16) / FAKE_STRENGTH(+18) / relay(+8) / high_board(≥3板, 每板+12, cap 45) / prev_broken(+28) / hard_veto(≥5板 或 炸/败, ≥总惩60) / leader_fade(+30, hard_veto)。常量 RISK_EXTRA_MARGIN=8.0, WATCH_TOP_FRAC=0.25, WATCH_FLOOR=35.0。
- `scripts/duanxianxia_v9_output.py` = `0c23b60e`(gate 定档提交 `d1362db`) — `_assign_actions` + `REGIME_ACTION_GATE`。
- `scripts/v10_optimize.py` = `bfd1ac57` — `Daily`/`excess`/`DEFAULT_PROJECT_ROOT`。
- 本轮研究脚本提交: 0151 `863196e` / 0152 `e2431ec` / 0153 `da18071` / 0154 `b18c965` / 0155 `f112fd0`(script+queue)。
- `main` 提交链(近端): `da18071`(0153) → `b18c965`(0154) → `f112fd0`(0155, 当前 HEAD)。
- `agent-results` 结果分支(每次 publish 重写历史, 只留 1 条): 0154 publish HEAD ≈ `74c5771`; 0153 publish ≈ `a8fcb79`。**读结果只认 `agent-results` 分支上的 `<id>.result.json`, 不要靠 HEAD 判断是否跑完。**

### 7.7 0154 买入级 A/B 结果(已完成, 防重跑)
> worker_time 2026-07-05T14:40+08(VM 时钟), ok=true, rc=0, duration≈125s。n_days=20, regime_days={cold:17, cold_to_warming:3}。买入档次日 excess(去竞价超额):

| variant | scope | n | mean_ex | win | avg_win | avg_loss | payoff |
|---|---|---|---|---|---|---|---|
| baseline | overall | 26 | 2.538 | 0.615 | 6.429 | −3.686 | 1.744 |
| baseline | cold | 17 | 1.498 | 0.529 | 6.567 | −4.204 | 1.562 |
| baseline | ctw | 9 | 4.503 | 0.778 | 6.251 | −1.614 | 3.874 |
| **risk_strict** | overall | 13 | 4.625 | **0.769** | 6.904 | −2.973 | **2.322** |
| risk_strict | cold | 6 | 3.643 | 0.667 | 7.424 | −3.919 | 1.894 |
| risk_strict | ctw | 7 | 5.466 | 0.857 | 6.557 | −1.081 | 6.066 |
| profile | overall | 26 | 2.538 | 0.615 | 6.429 | −3.686 | 1.744 |
| profile | cold/ctw | == baseline 三档全同(no-op) |||||||
| cap_auction | overall | 24 | 0.717 | 0.417 | 5.196 | −2.483 | 2.093 |
| cap_auction | cold | 17 | 0.758 | 0.353 | 6.325 | −2.278 | 2.776 |
| cap_auction | ctw | 7 | 0.616 | 0.571 | 3.503 | −3.233 | 1.083 |

- **定论**: `risk_strict` 唯一全面胜出——win↑ 且 payoff 不塌(反升), avg_loss 收窄(回撤改善), 三档一致。代价: 买入样本减半(overall 26→13, cold 17→6, ctw 9→7, 且 ctw 仅 3 天)→ 只作**方向性证据**, 上线前须过参数敏感性 A/B(0155)。
- `profile` 在实际 BUY 集不 bind(赢家本就 liq/money 双高, §7.4)→ 无独立增量, 不必单列上线。
- `cap_auction` **有害**(overall win 0.615→0.417, mean 2.538→0.717): 买入档里高竞价强度行恰是赢家, 推翻 §7.4 横截面"中档竞价"画像 → 印证"IC/横截面 ≠ 钱", 一切以买入档真实盈亏为准。

### 7.8 0155 风险严格度 sweep — 变体定义(结果待取)
`scripts/duanxianxia_risk_strictness_sweep.py`(复用 0154 的 `_decision/_agg/_buy_exs` 同口径, 加法、可回退): 对每日候选用当前 `compute_edge_v9` 重算 edge/`edge_components.risk_penalty`/`risk_detail`, 各变体先做过滤子集再套真实 `_assign_actions`, 比较实际 BUY 集次日 excess(overall/cold/ctw, `_agg`):
- `baseline` / `risk_strict`(剔任何 risk_flag, 0154 冠军, 作参照上界)。
- `pen_ge_{10,20,30,45}`: 仅剔 `risk_penalty ≥ 阈值` 的行(渐进严格度 → 看胜率是否单调、能否在保样本前提下保住提升)。
- `veto_only` / `drop_lowliq` / `drop_highboard` / `drop_highopen`: 按单一风险类别(hard_veto / low_liquidity / high_board_position / high_open_cost)剔除(定位胜率提升的驱动源)。
- 输出 `projects/duanxianxia/reports/_audit/risk_strictness_sweep_0155.json`。队列 `0155_risk_strictness_sweep_20260705.json`。
