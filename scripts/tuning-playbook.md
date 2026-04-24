# duanxianxia 题材打分调参与部署硬闸（SOP）

> 目标：保证调参有依据、有日志、有回归闸，减少盲改和部署不一致。

## 1) 信号缺失三分法（先诊断，后调参）

### Case 1：命名 / 概念别名不对齐（Alias mismatch）

- 现象：预期应命中的票，只有 `hot_theme_bonus` 没有打上；候选排序变化主要来自主题匹配层。
- 典型动作：
  1. 对比候选 `concept` 与 `fupan` 热门题材名是否同义词/同概念切片差异；
  2. 核实 `review.fupan.plate` 与 `home.kaipan.plate.summary` 的字段与口径；
  3. 扩展 `theme_aliases`（仅当确认为同一语义），避免在代码层直接加分歧。

### Case 2：当天题材轮动（无真实 hot_themes）

- 现象：候选中“本应上榜”票并不在昨日热点池中，`yesterday={}` 大量增加不是调参 bug。
- 典型动作：
  1. 查看当日 `fupan` 的 `hot themes`，确认是否真实出现；
  2. 避免为了凑命中把未进 hot_themes 的概念直接写入字典；
  3. 必要时只做评分策略微调（阈值、权重），不改字典。

### Case 3：评分逻辑 bug

- 现象：`theme`/`yesterday` 命中不稳定，且跨代码、跨样本都有异常；不局限于单个别名缺失。
- 典型动作：
  1. 回看 `_evaluate_theme_for_candidate()` 与 `_yesterday_bonus()`；
  2. 复核命名归一化（`_canonicalize`）与匹配集合构成；
  3. 对照历史样本、增加离线 smoke，确认逻辑层面确有修复。

## 2) 字典扩展决策门（Case 1 专用）

- **只做高可信扩展**：仅当满足以下条件再扩展。

1. 新 alias 在过去 N 天 `fupan.hot_themes` 中有真实出现；
2. 该 alias 在样本中至少影响 M 只候选；
3. 扩展后，按过拟合守护基线验证无新增误命中。

- 建议默认阈值（随样本量再调）：
  - N：3
  - M：2

## 3) 过拟合守护基线（Hard Negative）

- 目标：限制字典扩展副作用，避免“全局涨词”误伤。
- 当前基线（v6.3.1 对齐）：
  - `688269`、`300397`、`600433`、`603666`、`002407`、`920069`、`920974`
- 验证要求：每次 PR 后必须确认这 7 条在 top-30 中 `hot_theme_bonus` 保持为 0（除非明确重新定义策略）。
- 基线更新（季度 Review）：
  - 按月/季度汇总样本中的 `yesterday={}` 分布；
  - 对比是否出现稳定、重复且不应命中的条码，决定是否从基线移除或新增。

## 4) 部署硬闸（Deploy Gate）

> 目标：避免“本地代码与生产配置”错位导致静默回归。

```bash
set -euo pipefail

# 可选参数：SMOKE_DATE / EXPECTED_HEAD / 项目目录
SMOKE_DATE="${SMOKE_DATE:-$(ls -1 captures/ | sort | tail -1)}"
EXPECTED_HEAD="${EXPECTED_HEAD:-f308c6a}"

cd "${DEPLOY_DIR:-/opt/short-term-pre-market-analysis}"

git stash push -u -m "pre-deploy-backup-$(date +%F_%H%M%S)" || true

git fetch origin

git checkout main
git pull --ff-only origin main

current_head=$(git log -1 --format=%H)
if [[ "$current_head" != "$EXPECTED_HEAD" ]]; then
  echo "ERR: HEAD mismatch"
  echo "expected: $EXPECTED_HEAD"
  echo "actual:   $current_head"
  exit 1
fi

output="$(python scripts/duanxianxia_premarket_v6.py "captures/$SMOKE_DATE" --project-root . --top 30 | grep alias_ || true)"

echo "$output"

grep -q "alias_groups=6" <<<"$output" && grep -q "alias_entries=35" <<<"$output" || {
  echo "ERR: alias mismatch"
  echo "expected: alias_groups=6 alias_entries=35"
  echo "actual: $output"
  echo "HEAD: $(git log -1 --oneline)"
  exit 1
}
```

- 上线前：通过后执行你的服务重启命令（systemctl/supervisor 或其他）。
- 失败处理：出现 mismatch 时，不重启，先回滚到 GitHub 源码真相。

## 5) PR 规范（建议模板）

- PR 描述至少包含：
  - `VERSION`（如 `premarket_5table_v6.3`）；
  - `alias_groups` / `alias_entries`；
  - 受影响样本日期与样本量；
  - 候选 `code` 命中变化清单（哪些应该命中、哪些应保持 `yesterday={}`）。
- 提交 checklist（建议）：
  - `must hit`：明确哪些代码预期受益；
  - `must not change`：明确哪些代码应保持 0；
  - `may change`：允许波动范围（如 300067 的 3 分钟调整类变更）。

## 6) 字典修改日志（append-only）

- 新建后请按每次 alias 扩展追加一条记录：
  - 日期；
  - PR#；
  - 新增 alias 条目；
  - 验证样本（日期/文件）；
  - 过拟合基线影响说明。
- 推荐字段：`日期 | PR# | 版本 | 新增条目 | 验证样本 | alias_groups/alias_entries | 备注`

## 6.1) 参考配置基线（v6.3.1）

- version: `premarket_5table_v6.3`
- alias_groups: `6`
- alias_entries: `35`
- 过拟合守护基线（hard negative）：`688269/300397/600433/603666/002407/920069/920974`
- 版本承载：`yesterday` 打分逻辑与 `hot_theme_bonus` 在 v6.3.1 下可因真实 alias 命中出现 `may change`（见 300067）。
