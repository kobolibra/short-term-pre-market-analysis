# AGENT HANDOFF — 盘前选股 + IPO 日历项目交接文档

> 这份文档是为**新对话 / 新 agent** 写的。只要读完本文, 你就能: (1) 明白怎么“连上”部署在
> Google Cloud 上的这个项目、怎么让代码跑在服务器上并拿到服务器上的真实数据; (2) 了解项目目前
> 进展与结论; (3) 无缝接上继续迭代。作者: 上一个会话的 Notion AI agent。最后更新: 2026-06-27。

---

## 0. TL;DR (一分钟上手)

- **仓库**: GitHub `kobolibra/short-term-pre-market-analysis`。两个分支: `main`(代码+任务队列), `agent-results`(服务器跑出的结果)。
- **你(agent)不能直接 SSH 登服务器**。你和 GCP 服务器之间的唯一通道是 **GitHub**。
- **连服务器看原始数据的方式 = “git 队列 + 服务器上的 cron worker”**:
  1. 你把一个 Python 脚本 push 到 `main/scripts/*.py`;
  2. 再 push 一个任务描述文件到 `main/scripts/agent_jobs/queue/<id>.json`;
  3. 服务器上的 cron 每隔几分钟拉一次 `main`, 跑你的脚本(脚本能读服务器本地的全部原始数据), 把输出写回 `agent-results` 分支;
  4. 你从 `agent-results` 分支读结果。
  > 这就是用户强调的“不可能全靠 GitHub, 你还是要去服务器看原始数据”的解法: 脚本替你去服务器读。
- **你自己的 web 抓取工具抓不了某些站(如 9fzt)** ——只有服务器上的 urllib 脚本能读。所以凡是“读服务器数据/抓外部站/跑回测”都走排队作业。
- **两条并行工作线**: (A) 盘前选股模型迭代; (B) IPO 日历自动推送。

---

## 1. 怎么“连上”服务器项目 (最重要)

### 1.1 架构: git-as-queue + cron worker

```
  你(agent)                GitHub                         GCP VM (服务器)
  --------                  ------                         ----------------
  写 scripts/X.py   --push-->  main  <----git pull (cron)---- agent_job_runner.sh
  写 queue/<id>.json --push--> main                          → git reset --hard origin/main
                                                              → agent_daily_refresh.py (自动排队日常刷新)
                                                              → agent_job_worker.py (跑 queue 里的脚本)
                                                              → 脚本读服务器本地原始数据 + 写输出
  读结果       <--pull----  agent-results  <--force push-- publish_results() (commit-tree)
```

### 1.2 服务器上的关键路径与脚本

- 工作目录 `WS = /home/investmentofficehku/.openclaw/workspace`
- cron 入口: `scripts/agent_job_runner.sh`
  - `git fetch origin main && git reset --hard origin/main` (硬同步, 本地改动会被覆盖)
  - 跑 `scripts/agent_daily_refresh.py` (自动把当日需要的刷新/复盘作业排进队列)
  - 跑 `scripts/agent_job_worker.py` (真正执行 queue)
  - `publish_results()`: 用 `git commit-tree` + `git push --force` 把两个结果目录发布到 `agent-results`
    - `projects/duanxianxia/reports/_audit` (盘前)
    - `projects/ipo_calendar/reports/_audit` (IPO)
- worker: `scripts/agent_job_worker.py`
  - 扫 `scripts/agent_jobs/queue/*.json`
  - 结果写 `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json`
  - **幂等**: 结果文件已存在则跳过(同一 id 不重跑)。**重跑请用新 id**。
  - **安全白名单**: 只能跑 `scripts/` 下的 `.py`, 路径必须解析在 workspace 内。
  - 默认 timeout 1800s, stdout/stderr 各截断 16000 字符写进 result。
  - 心跳: `.../agent_jobs/_worker_heartbeat.json` (看 worker 最近一次跑的时间与扫到多少 job)。
- **节奏**: cron 大约每 10–30 分钟一次(不是实时)。另有 `0 8 * * 1-5` 的 IPO cron(工作日 8:00)。
  push 后要等下一个 tick 才会跑。**别刚 push 完就期待立刻有结果**。

### 1.3 怎么提交一个作业 (标准流程)

1. 先把脚本 commit 到 `main/scripts/<name>.py`。
2. **等脚本 commit 成功后**, 再 commit 队列文件 `main/scripts/agent_jobs/queue/<id>_<desc>_<date>.json`。
3. **切记: 对 main 的提交必须串行**(一个一个来)。并行双提交会 409 冲突(曾经坏过 job 0034)。
4. 队列文件格式:
   ```json
   {
     "id": "0040_xxx_20260627",
     "script": "scripts/xxx.py",
     "args": ["--flag", "value"],
     "timeout": 900,
     "note": "人话描述这个 job 干什么。"
   }
   ```
   - `id` 必须唯一且与文件名主干一致(worker 用 id 定结果文件名)。
   - **已用到的最大 id: `0039`。下一个用 `0040`。**
5. 读结果: 从 `agent-results` 分支读 `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json`
   (`ok`/`rc`/`stdout_tail`/`stderr_tail`/`duration_s`), 以及脚本写出的报告文件(同在 agent-results)。

### 1.4 人工在 VM 上跳过队列直接跑(调试用, 告诉用户)

```bash
cd /home/investmentofficehku/.openclaw/workspace \
  && git fetch origin main && git reset --hard origin/main \
  && /usr/bin/python3 scripts/<X>.py [args]
```

### 1.5 用 GitHub MCP 怎么操作 (你手边的工具)

- 读: `connections.mcpServer_github.runTool({ toolName: "get_file_contents", toolArguments: { owner, repo, path, ref } })`
  - `ref` = `main` 读代码/队列; `ref` = `agent-results` 读结果。
- 写: `connections.mcpServer_github.runTool({ toolName: "create_or_update_file", toolArguments: { owner, repo, branch:"main", path, message, content[, sha] } })`
  - 新建文件不传 `sha`; 改已有文件要先读出 `sha` 再传。
  - GitHub 提交不是 Notion 变更, **不要**加 `editDescriptionVariableName`。
- owner=`kobolibra`, repo=`short-term-pre-market-analysis`。

---

## 2. 项目 A — 盘前选股模型 (核心)

### 2.1 最终目标
顶级的盘前分析选股代码模型。用服务器上每日下载的真实数据, 以 walk-forward 出样本验证为准绳, 迭代出更好的选股效果。
**原则: 数据驱动, 不猜测; 改线上公式前必须过 walk-forward 出样本验证。**

### 2.2 唯一正确的预测口径
```
excess_ret = 收盘涨幅 - 竞价涨幅 = (close - open) / preclose * 100
```
- open 就是集合竞价(开盘)价。盘前选股赚的是“开盘买入 -> 收盘”的超额, 所以才减掉竞价涨幅。

### 2.3 生产线公式 (已验证为最优, 勿随意改)
- **v10_amt edge** (生产中, 在 `duanxianxia_v9_edge.py::compute_edge_v9` 里):
  ```
  edge = clip(0.23*amt_pct + 0.19*auction_strength + 0.18*liquidity
            + 0.14*money + 0.14*pressure_score + 0.08*weimai_strength
            + 0.05*orderbook - risk_penalty, 0, 100)
  ```
  - `amt_pct` = 竞价成交额(auction_amount_wan)当日横截面百分位(0–100), 由 `assemble_v9` 在逐行 edge 前注入。
  - 权重可被 params 覆盖: `edge_w_amt/auction/liquidity/money/pressure/weimai/orderbook`。
- **决策拆分** (当前): 同日激进 Top3 用 v10_amt; T+1 Top30 用 v10_amt。无止损/无天闸门/无 ML rerank。

### 2.4 生产代码链路 (谁算什么)
- `duanxianxia_premarket_v9_runner.py::run_v9(date, root)` — 入口。补生成某日 v9 分析。
- `duanxianxia_v9_assemble.py::assemble_v9(...)` — 六层装配: weimai/theme/market_env/context, 注入 amt_pct 百分位, 逐行调 compute_edge_v9。
- `duanxianxia_v9_edge.py::compute_edge_v9(...)` — 算 `edge_score`(干净 v10 公式)。**注意: 这里不算 final_score**。
- `duanxianxia_v9_output.py::shape_v9_output(...)` — 排序 + `_assign_actions` 打 BUY/WATCH/DROP 动作闸门
  (regime 自适应分位数闸门 `REGIME_ACTION_GATE`), 输出 `all_candidates`(每行挂 `full` 全量明细)。
- **输出文件**: `projects/duanxianxia/reports/<date>/premarket/*_analysis_v9.json` (含 `all_candidates`)。
- **日线数据**: `projects/duanxianxia/dailyline/stocks/<code>.csv` (要 `tradestatus==1`, 含 open/close/preclose)。

### 2.5 分析/迭代脚本 (只读, 跨全史数据)
- `scripts/v10_optimize.py` — 主优化管线。提供复用助手: `Daily`, `extract`, `derived`, `pctl`, `field_value`,
  `daily_ic`, `spearman`, `mean_icir`, `score`, 常量 `V10AMT_W`/`CORE_FIELDS`/`RANK_FIELDS`, `DEFAULT_PROJECT_ROOT`。
  - `CORE_FIELDS` = amt_pct, auction_strength, liquidity, money, pressure_score, weimai_strength, orderbook。
  - `RANK_FIELDS` = net_amount_rank, qiangchou_920_925_rank, qiangchou_last_second_rank (方向要翻转: 秩越小越好)。
  - `field_value(r,fld)`: amt_pct->r["amt"]; deriv.*->r["d"]; 其余->r["f"]。
  - 输出 `reports/_audit/premarket_master_report.{json,md}`。最少有效日门槛: 每日≥7230 行(请看最新源码)。
- `scripts/v12_reflection.py` — 反思报告。提供 `load_days_plus(root, daily)`(行有 code/f/amt/d/risk/excess/
  action/risk_flag/edge_old/final/edge_rank), `RAW_FLDS`, `DERIV_FLDS`。输出 `premarket_reflection_report.{json,md}`。
- `scripts/v28_risk_filter_validation.py` — 风险门控验证。
- `scripts/v29_blend_optimize.py` — 混合公式实验(加入高 ICIR 交互项)。输出 `premarket_blend_v29.{json,md}`。
- `scripts/v30_marginal_ic.py` — 边际增量 IC 测试。输出 `premarket_marginal_ic_v30.{json,md}`。

### 2.6 已确认的关键结论 (按时间)
- 字段 ICIR 排序: `deriv.amt_x_auc` ICIR 0.531(最稳) > auction_amount_wan/auction_strength/liquidity > `deriv.money_x_liq` 0.437。
- BUY 动作极稀但质量极高: n=15, mean_excess 4.1, win_rate 0.87, 跌停率 0.067。
- **job 0038 (v29 混合实验, 10 个 OOS 日, 4603 样本) 结论**:
  | 策略 | OOS IC | ICIR | cap@30 | Top3 均/中位/胜率 |
  |---|---|---|---|---|
  | **v10_amt_raw(生产)** | **0.126** | **0.82** | **0.18** | **1.25 / +3.10 / 0.60** |
  | v10_amt_pctl | 0.089 | 0.77 | 0.14 | 1.13 / 0 / 0.37 |
  | sparse_ic | 0.083 | 0.79 | 0.15 | 0.36 / 0 / 0.33 |
  | ext_fixed(+derivs) | 0.085 | 0.77 | 0.14 | 0.85 / 0 / 0.33 |
  | ext_learned(+derivs) | 0.082 | 0.72 | 0.14 | 0.72 / 0 / 0.37 |
  - **结论1: 加入两个高 ICIR 交互项不提升模型**。原因: `deriv.amt_x_auc = amt_pct×auction_strength`,
    `deriv.money_x_liq = money×liquidity`, 成分早在 v10_amt 里, 共线冗余, 边际信息≈0。**v11 混合公式否决, 生产公式不改。**
  - **结论2: 原始加权和 ≫ 逐日百分位复合**(IC 0.126 vs 0.089, 同字段同权重)。百分位排名抹掉了
    sub-score 的幅度/基数信息, 反而丢信号。**启示: Top5 之前用 sparse_ic(百分位复合)的口径被这个框架拖累**。
- **final_score 谜题已从源码定论(重要纠正)**: 全链路从不计算 final_score; `_full()` 里的
  `d.get("final_score")` 取的是上游 v7.2 遗留字段。**生产选股根本不用它**(动作门门与排序全走 edge_score)。
  所以 final_stored 的低 IC(0.041) 不在决策路径上, 不用担心。真正的小缺口是: stored `edge_score`(0.113) <
  重算 v10_amt(0.127), 因为早期交易日存盘时用的是 v10 之前的旧 edge 公式 → 需要全史重跑 v9 让存量 edge 统一。

### 2.7 环境 (GCP VM)
Python 3.10.12; numpy 2.2.6 / pandas 2.2.3 / torch 2.11.0+cpu / sympy 1.14.0。**缺** scipy/sklearn/lightgbm/xgboost/statsmodels/joblib。
→ 纯 numpy/手写统计, 不要 import 缺的包。

---

## 3. 项目 B — IPO 日历自动推送

### 3.1 需求
从 `https://stock.9fzt.com/dataCenter/stockApply.html` 取数据, 每天 8:00 把当日「申购/网上申购缴款/上市日期=今天」的股票推送到飞书。只用这个 9fzt 来源。不要引用未核实的日期。

### 3.2 已破解的 9fzt IPO API (权威, 返回真实数据)
- URL: `https://api-hq.chongnengjihua.com/news/api/1/stock/a/ipo/list` (GET)。`/news` 前缀必须有。
- 参数: `{ listedSector, pageNum, pageSize, sortField:"onlineStartDate", sortType:0 }`
  - `listedSector`: 0=全部A股, 1=上海主板, 2=深证主板, 3=科创板, 4=创业板。
- 响应: `{ code, data:{ count, ipoList:[...] }, message }`。成功 code=0(20001=参数非法)。
- **签名** (最关键, 之前全部失败都是因为差这个):
  - header `signature = md5("sjdxfnqogbzoun13d971ckh8p" + 参数值按key字典序拼接.join("") + msTimestamp)` (小写 hex)
  - header `timestamp = msTimestamp` (毫秒)
- 真实字段名(29个, 部分): `prodName`(名称), `symbol`(代码), `onlineStartDate`(申购日),
  `onlineEndDate`, `payDateOnline`(网上缴款日), `listedDate`(上市日), `issueResultPublicDate`(中签率公告日),
  `issuePrice`(发行价), `firstClosePrice`(首日收盘), `lotRateOnline`(中签率), `listedSector`, `market` 等。
- 参考实现: `scripts/ipo_calendar_fetch_real.py` (已跑通, job 0037 返回 500 行/count 5550),
  写 `projects/ipo_calendar/reports/_audit/latest_9fzt_signed.json`。

### 3.3 IPO 脚本与路径
- `scripts/ipo_calendar_notify.py` — 主推送脚本(最后可用提交 `28da81d`)。**待接入签名 API**(见下)。
- `scripts/ipo_calendar_fetch_real.py` — 已验证的取数脚本(可直接复用其签名+分页逻辑)。
- 输出目录: `projects/ipo_calendar/reports/_audit/` (latest_9fzt_signed.json, <date>_ipo_calendar.json|.md, latest_ipo_calendar.json|.md)。
- 数据: `projects/ipo_calendar/data/<date>/`。

### 3.4 飞书 webhook 环境变量 (服务器 .env)
- .env 位置: `/home/investmentofficehku/.openclaw/.env` 和 `.../workspace/.env`。
- webhook(任选一, 代码按顺序读): `IPO_FEISHU_WEBHOOK_URL`, `FEISHU_WEBHOOK_URL`, `LARK_WEBHOOK_URL`,
  `IPO_FEISHU_WEBHOOK`, `FEISHU_WEBHOOK`, `LARK_WEBHOOK`, `WEBHOOK_URL`, `FEISHU_BOT_WEBHOOK`, `LARK_BOT_WEBHOOK`, `DXX_FEISHU_WEBHOOK_URL`。
- 签名密钥: `IPO_FEISHU_SIGN_SECRET`, `FEISHU_SIGN_SECRET`, `LARK_SIGN_SECRET`。

---

## 4. 待办事项 (接手后继续)

### 4.1 盘前模型
1. **读 job 0039 (v30 边际 IC) 结果** `agent-results: .../agent_jobs/0039_premarket_marginal_ic_v30_20260627.result.json`
   与 `premarket_marginal_ic_v30.{json,md}`:
   - 若“任何字段都不能边际提升 OOS IC” → 证实 v10_amt 在现有数据下是线性局部最优, **停止凑字段组合**,
     把迭代重心转到决策层(仓位/集中度/闸门)与“加数据”。
   - 若某字段两个 lambda 都稳健正增量 → 考虑把它以小权重加入 edge 公式, 再走 walk-forward 复验。
2. **生产小缺口**: 触发一次全历史 v9 重算(让存量 `edge_score` 统一到 v10 公式)。v10_optimize 的 regen_missing
   只补缺不覆盖; 要全重算需单写作业(调 v9 runner, 重的, 需服务器全数据 bundle)。
3. 不要再试纯百分位复合口径(已证实比原始加权和差)。

### 4.2 IPO
1. **把 `ipo_calendar_notify.py` 接入签名 API**: 改成调
   `https://api-hq.chongnengjihua.com/news/api/1/stock/a/ipo/list` + md5 签名 header + 参数
   `{listedSector:0, pageNum, pageSize:50, sortField:"onlineStartDate", sortType:0}`, 分页, 映射字段
   (prodName/symbol/onlineStartDate/payDateOnline/listedDate), 筛选事件 申购日/网上缴款日/上市日=今天, 推飞书;
   保留 HTML/debug fallback。可直接复用 `ipo_calendar_fetch_real.py` 的签名与分页代码。
2. 排一个 no-send 测试: `scripts/ipo_calendar_notify.py`, args `["--date","<today>","--no-send","--run-weekends"]`,
   验 raw_rows>0 / event_count 合理 / send_result.skipped。

---

## 5. 用户偏好 / 踩过的坑
- 用户要**结果**, 不要反复问确认, 不要“什么都靠试”——**先看明白源码/页面再动手**。
- 访问 9fzt 的权限用户已反复授权过, **不要再反复要求授权**。
- push 到 main 是即时的; 但 VM 只在每次 worker tick 开始时才 pull, 所以结果有延迟。
- 并行提交 main 会 409; **脚本先提交, 队列后提交, 串行**。
- worker 可能拉到的是你刚改之前的脚本版本(曾因此 job 0036 跑了旧版) → 提交要赶在 worker tick 之前。
- 不要引用未核实的日期。

---

## 6. 快速参考 — 关键路径速查
| 用途 | 路径 / 值 |
|---|---|
| 仓库 | `kobolibra/short-term-pre-market-analysis` |
| 代码/队列分支 | `main` |
| 结果分支 | `agent-results` |
| 服务器工作目录 | `/home/investmentofficehku/.openclaw/workspace` |
| 队列目录 | `scripts/agent_jobs/queue/<id>.json` |
| 结果目录(盘前) | `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json` |
| worker 心跳 | `projects/duanxianxia/reports/_audit/agent_jobs/_worker_heartbeat.json` |
| 盘前分析输出 | `projects/duanxianxia/reports/_audit/premarket_*.{json,md}` |
| 每日 v9 分析 | `projects/duanxianxia/reports/<date>/premarket/*_analysis_v9.json` |
| 日线 csv | `projects/duanxianxia/dailyline/stocks/<code>.csv` |
| IPO 输出 | `projects/ipo_calendar/reports/_audit/` |
| 下一个 job id | `0040` |
| cron 入口 | `scripts/agent_job_runner.sh` |
| worker | `scripts/agent_job_worker.py` |
| 生产 edge 公式 | `scripts/duanxianxia_v9_edge.py::compute_edge_v9` |
