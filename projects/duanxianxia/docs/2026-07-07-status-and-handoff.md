# duanxianxia 短线预市分析 — 项目交接 & 进展总录 (2026-07-07)

> 用途:本文件是 duanxianxia(短线预市/打板竞价分析)项目的完整交接快照,供新会话/接手者无缝接续。
> 真相来源优先级:**repo 代码 & repo docs = SSOT**。本文件即 repo 内 SSOT 之一;与代码冲突时以代码为准。

---

## 0. 现在的状态 (TL;DR)

- **刚完成**:修复 `scripts/duanxianxia_indicator_listing_0166.py` 的 project-root bug(之前 n_rows=0),并入队 0167 重跑。已推到 `origin/main`,commit `642bf15`。
- **正在等**:下一轮 GCP cron(约 10 分钟一轮)自动拾取 0167 运行,结果落到 `agent-results` 分支 `projects/duanxianxia/reports/_audit/agent_jobs/0167.result.json`,预期 **n_rows>0、fengdan_hit>0**。
- **验证方式**:`git show origin/agent-results:projects/duanxianxia/reports/_audit/agent_jobs/0167.result.json`(或 GitHub MCP 恢复后 `get_file_contents` ref=`agent-results`)。
- **6 维指标结构已敲定 (RATIFIED)**,见第 4 节。

---

## 1. 项目概述

- 目标:每个交易日**开盘前(集合竞价阶段)**,基于抓取的竞价/封单数据,给 A 股候选票打分排序,识别**打板/封单强度**最强的标的。
- 方法:**6 维指标框架 (D1–D6)**,数据流水线:
  - canonical rows(各数据源原始行归一)-> `feature_builder` v12(含 fengdan 合并)-> `indicator_builder` v13 -> listing 脚本(排版输出 + 落 audit json)。
- 原则:**真数据驱动,绝不编造**。captures 缺失/为空时,如实报告目录状态,不 fabricate。

---

## 2. 基础设施 & 运行机制

| 项 | 值 |
|---|---|
| Repo | `kobolibra/short-term-pre-market-analysis` (**PRIVATE**), remote `https://github.com/kobolibra/short-term-pre-market-analysis.git` |
| 分支 | `main` = 代码;`agent-results` = 运行结果(由 cron **force-push**,勿手动改) |
| 服务器 WORKSPACE | `/home/investmentofficehku/.openclaw/workspace`(GCP 服务器,cron 约 10min 一轮) |
| 本地 clone (2026-07-07 确认) | `/Users/time/Desktop/short-term-pre-market-analysis`(用户 MacBook-Air) |
| PROJECT_ROOT | `WORKSPACE/projects/duanxianxia` |
| DEFAULT_PROJECT_ROOT | 定义于 `scripts/v10_optimize.py`(第 31/33 行):优先 `from duanxianxia_premarket_v7_2_runner import DEFAULT_PROJECT_ROOT`,否则 `SCRIPTS_DIR.parent/"projects"/"duanxianxia"` |
| captures 路径 | `projects/duanxianxia/captures/<YYYY-MM-DD>/<dataset>/*.json` |
| results 路径 | `projects/duanxianxia/reports/_audit/agent_jobs/<id>.result.json`(在 `agent-results` 分支) |

**Job 队列机制**:在 `scripts/agent_jobs/queue/` 放 json(格式见第 7 节),cron worker 拾取运行,**按 id 幂等**(id 用过即消耗,重跑要新 id)。结果写到 `agent-results` 分支。

**⚠️ cron 关键行为**:每轮 `git fetch + git reset --hard origin/main`。**未 push 的改动(包括已 commit 但没 push 的)会被清光**。-> 本地改完必须**立刻 add+commit+push**,别只 commit。

**其它固定事实**:
- self-tests 是 **BLOCKING** 的,且挂在每日 cron 里。
- VM 时钟比真实时间 **快约 68 分钟**。
- 运行器 stdout_tail **从前面截断** -> 关键输出(RECAP)必须**最后打印**。
- cron 步骤:`git fetch + reset --hard origin/main` -> `agent_daily_refresh.py` -> `agent_job_worker.py` -> capture selfheal `--recent 6 --apply` -> `publish_results`(把 commit force-push 到 `agent-results`)。

---

## 3. 数据口径 / canonical registry (SHA `5bfe80c4692cde732e186ead72f632f35949b993`)

**各数据源原始数组下标 (canonical)**:

- **weimai `raw[18]`**:`[4]` seal_amount_wan_raw = 涨停委托未剔成交(元);`[5]` auction_change%;`[6]` main_net(元);`[8]` auction_turnover = 竞价成交(元,**== vratio raw6**);`[12]` FF(元);`[14]` super_large;`[15]` large;`[16]` board_label;`[17]` seal_amount = 动态封单(万,**已弃用**)。
- **vratio / qiangchou `raw[0..12]`**:`[2]` FF(亿);`[6]` auction_turnover(万 = bidAmount);`[11]` volume_ratio / grab_strength;`[12]` turnover_rate。
- **net_amount `raw[0..8]`**:`[2]` auction_change_pct;`[4]` main_net(万);`[5]` auction_turnover(万);`[6]` FF(亿);`[8]` turnover_rate。
- **fengdan named_dict**:`amount_915/920/925` -> `seal_bid_915/920/925`(元);`tag_1` -> concept。
- **`parse_cn_amount`**:亿->1e8,万->1e4,元->1,`"-"`/`""`->None。

**自有口径裁定 (owned calibers, 重要)**:
- **抢筹强度 != 量比**:两者都保留(不同口径)。
- weimai `raw5` = 竞价涨幅;`raw8` = 竞价成交(元,== vratio raw6)。
- `raw17` 动态封单 = **弃用**。
- **真封单 = raw4 - raw8 = f925**(经验证,见第 5 节)。
- FF coverage:46/113。

---

## 4. 6 维指标 (v13 INDICATOR_KEYS, RATIFIED)

> 以下 13 个 key 是**已敲定**结构。逐字生效,勿擅改。D6 为外接维度,尚未接线。

**D1 竞价涨幅**
- `d1_auction_change_pct` — 竞价涨幅%(weimai raw5)

**D2 量能 / 参与**
- `d2_bid_amount` — 竞价成交额(元)
- `d2_bid_strength` — 竞价强度 = bid / FF
- `d2_volume_ratio` — 量比
- `d2_turnover_rate` — 换手率
- `d2_grab_strength` — 抢筹强度(与量比不同口径,均保留)

**D3 资金**
- `d3_main_net_inflow` — 主力净流入(元)
- `d3_fund_ratio` — 资金占比 = main_net / bid

**D4 封单**
- `d4_true_seal` — 真封单 = seal_raw(raw4) - bid(raw8)(约等于 f925)
- `d4_seal_ratio` — = true_seal / FF
- `d4_fengdan_925` — = sealBid925(fengdan amount_925)

**D5 时间 / 微观**
- `d5_fill_ratio` — 成交/委托 = bid / seal_raw
- `d5_time_divergence` — 时间分歧 = (seal_raw - f920) / f920,**gate**:f925 not in {None,0} 且 f920 not in {None,0}

**D6 市场 / 板块** — 外接(external,尚未 wired)

**已删除 / 证伪的 key(勿复活)**:`d1_auction_amount_pct`、`d3_super_large`、`d3_large`、`d3_money`、`d3_money_pct`、`d4_seal_amount`(raw17 动态封单)、`d5_weimai_strength`、`d5_orderbook`。

---

## 5. 已验证 (0165 覆盖率 & 中位数)

- 计数:n_fengdan 113、n_weimai 150、n_vratio 175、n_net_amount 49、n_qiangchou 68;fengdan_925_nonzero 8;fengdan_with_FF 46。
- 中位数验证:
  - `(raw4 - raw8) / f925 = 1.0003` -> **真封单 = raw4 - raw8 = f925 成立** [OK]
  - `raw8 / vbid = 0.99998` -> weimai raw8 == vratio 竞价成交 [OK]
  - `f920 / raw4 = 0.878`
  - `raw17 / f925 = 0.30` -> raw17(动态封单)与真封单差异大,**弃用正确** [OK]

---

## 6. `build_indicators` 返回结构

返回 keys:`version`、`feature_version`、`date`、`t0_cutoff`、`dimensions`、`indicator_keys`、`n_rows`、`n_fengdan`、`n_fengdan_merged`、`coverage{k:{missing,present,missing_rate,warn}}`、**`rows`**(排序后;每行带 `fengdan_hit`、`source_hits`、`source_hit_count`、`_field_sources`)。

**feature_builder 内部**:`AUCTION_DATASETS=(vratio,qiangchou,net_amount,weimai)`;`FENGDAN_DATASET="auction.jjlive.fengdan"`;`T0_DEFAULT_CUTOFF="09:29"`;helpers `_norm_code/_rows_of/_pick_capture_file/canonical_rows_for_dataset`;`_assemble` 输出含 `bidAmount`、`bidStrength(=bid/FF*1e4)`、`sealAmountRaw(raw4)`、`sealAmount(raw17)`、`sealBid915/920/925`、`fengdan_hit`。

---

## 7. 当前代码状态(文件清单)

| 文件 | 状态 / 版本 | 最新 commit |
|---|---|---|
| `scripts/duanxianxia_feature_builder.py` | v12(fengdan merged) | `e464df49` |
| `scripts/duanxianxia_indicator_builder.py` | v13(6 维) | `e1bb436` |
| `scripts/duanxianxia_indicator_listing_0166.py` | D1–D6 listing,今日修好 project-root(第 52 行 `import v10_optimize as _v10; root = Path(_v10.DEFAULT_PROJECT_ROOT)`) | `642bf15` |
| `scripts/agent_jobs/queue/0167_indicator_listing_rerun_20260706.json` | 今日新建,重跑 0166 逻辑 | `642bf15` |
| `scripts/v10_optimize.py` | 定义 `DEFAULT_PROJECT_ROOT`(第 31/33 行) | — |
| `scripts/duanxianxia_indicator_listing_0157.py` | 旧版(引用已删 key),被 0166 取代 | `a3a67b82` |
| `scripts/agent_job_runner.sh` | job runner | `f8d18513` |
| `projects/duanxianxia/docs/2026-07-06-D1D6-rebuild-shipped.md` | D1–D6 重构交付文档 | — |
| `projects/duanxianxia/docs/...auction-indicator-caliber-and-dimensions-handoff.md` | 口径 & 维度 SSOT | `596bf1e0` |

**队列 json 格式**:`{"id":"0167","script":"scripts/*.py","args":["2026-07-06"],"timeout":600,"note":"..."}`(timeout 默认 1800;按 id 幂等)。

**0166 首跑结果(bug 版)**:SHA `2f677bc7...`;ok/rc=0;**n_rows=0(路径 fallback 到 scripts/ 的 bug,今日已修)**;13 个 indicator_keys。

---

## 8. 本次会话完成 (2026-07-07)

- 定位 0166 `n_rows=0` 根因:`_project_root()` 从 `scripts/` 往上找 `captures/` 目录,但真 captures 在 `projects/duanxianxia/captures`,找不到就退回 `scripts/`。
- 修复:listing_0166.py 第 52 行改用 `DEFAULT_PROJECT_ROOT`(来自 `v10_optimize`);其余逻辑不动(最小改动)。
- 入队 0167 重跑(args `2026-07-06`)。
- **GitHub MCP 本会话失联**(`mcpServer_github3/7` 不可用)-> 改用**本地 clone 手动 git push**,成功推到 `origin/main`,commit `642bf15`(`e1bb436..642bf15`)。
- 本地 git 提示 committer 身份未配(user.name/email),纯警告,不影响;用户后续可 `git config --global` 自行设置(可选)。

---

## 9. 下一步(待推进)

1. **[等结果]** cron 跑完 0167 -> 读 `0167.result.json`,核 **n_rows>0、fengdan_hit>0**、coverage。若仍为 0,查 `2026-07-06` 的 captures 目录是否存在/为空。
2. **[D6]** 市场/板块维度接线(目前外接、未 wired)。
3. **[重构]** v9 double-counts 的 edge refactor;`0155 risk_strictness_sweep` 可能待办。
4. **[每日]** 下一交易日 09:25 capture + 09:30 self-check `ok:true`。
5. **[安全]** 之前会话里**明文粘贴过 PAT** -> 应尽快 revoke / rotate,换新 token。

---

## 10. 关键陷阱 & 教训

- **cron `git reset --hard origin/main` 会清掉未 push 的改动** -> 改完立刻 add+commit+push。
- **大段代码经聊天粘贴会被 markdown 自动格式化搞乱**(自动加链接/加粗)-> 优先用极短的 surgical sed / 小改动,或让文件本就在 repo 里,或下载真实文件。今日教训。
- **GitHub MCP 本会话失联**(script 模式不可用)-> 恢复后写用 `push_files`(原子,无需 sha);`create_or_update_file` 会在约 39KB 截断且要 sha,避免。
- `notion.updatePage` 的 `newStr` 不带 `oldStr` = 整页清空,小心。
- stdout_tail 从前截断 -> RECAP 最后打印。
- VM 时钟 +68 分钟。
- self-tests 是 BLOCKING。
- **勿编辑**两个 Notion 页:原始页(任务0156 委卖/量比原始数据表)和框架页(短线竞价选股数据字典/维度指标框架)。

---

## 11. 关键引用

**本地 push 流程(已验证可用)**:
```bash
cd /Users/time/Desktop/short-term-pre-market-analysis
git checkout main && git pull origin main
# 改文件 ...
git add <files>
git commit -m "..."
git push origin main
git log --oneline -1
```

**服务器 pull 流程(备用)**:
```bash
cd /home/investmentofficehku/.openclaw/workspace && git fetch origin && git checkout main && git pull origin main
```

**读 agent-results 结果**:
```bash
git show origin/agent-results:projects/duanxianxia/reports/_audit/agent_jobs/0167.result.json
```

**本会话 commits (main)**:`e464df49`(fb v12)-> `e1bb436`(v13 + listing + queue0166 + shipped doc)-> **`642bf15`**(0166 路径修复 + 0167 入队,HEAD = origin/main)。

**相关旧对话(在 Notion 线程列表按标题搜)**:网页数据抓取与API查询、检查GitHub项目运行错误、duanxianxia3、v9 重构推送及 PR。
