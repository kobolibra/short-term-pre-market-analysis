# Premarket v7 — Setup Classifier 设计基线

> Status: 设计基线 (design baseline only). 本文档不引入代码改动. v7 模块 (`duanxianxia_premarket_v7.py`) 在本文档合入 main 后另起 PR.

## 0. 文档定位

v6 (含 v6.3.1) 是**线性加分**模型: `final_score = Σ rank_score + Σ numeric_signal + theme_overlay + risk_penalty + market_regime + yesterday_postmarket`. 在多次实盘观察后, 其根本局限是:

- **同维度堆叠 ≠ 不同 setup**: 一只"昨日已经强势的接力龙头" 与一只"今日新晋题材的首板首板"在 v6 评分里可能拿到相同分数, 但走法、胜率、止损位完全不同.
- **缺失数据 = NULL = 减分**: v6 把"昨日不在 top 20 主标签"当作信息缺失, 实际上**这恰恰是某些 setup 的核心正面信号**.
- **题材主线判定靠人工 alias 列表**: v6.3 的 35 条 alias entries 是手工维护的, 无法解释"为什么这两个题材属于同一主线".

v7 的核心改造: **把选股从"打分排序"改成"先分类后排序"**. 候选股先归入 0 或 1 个 setup (A/B/C/D/E), 然后只在该 setup 内部做评分排序; 不命中任何 setup 的股票直接淘汰.

---

## 1. 核心原则: 缺失即信号

**原则**: 数据缺失不等于 NULL, 而是一个分类标签. 每个"是否命中 T-1 数据"的判断都要显式映射到一个标签, 而不是用 NULL 走入默认分支.

### 1.1 现实约束 (来自实盘运维)

| 数据 | 约束 | 缺失含义 |
|---|---|---|
| `home.kaipan.plate.summary` (T-1) | 9:25 仅抓 top 20 主标签子标签资金流, 抓多了被反爬 | 今日某主标签若 T-1 不在 top 20 → 该主标签**昨日是冷门** |
| `cashflow.stock.today` (T-1) | 仅覆盖全市场资金流入 top 100 | 候选股若 T-1 不在 top 100 → 该股**昨日资金不强势** |
| `龙虎榜` 字段 (review.fupan.plate) | 仅 "查看" / "无榜" 二值, 无席位明细 | 不再尝试区分机构 vs 游资 |
| 公告 / 互动易 | 不抓取 | 题材识别完全依赖资金信号反推, 不识别基本面催化 |

### 1.2 命中/未命中的双路径

对每只候选股, 在 T-1 数据维度上生成 4 个分类标签:

```
industry_t1_hit ∈ {hit_strong, hit_weak, miss}
stock_t1_hit ∈ {hit_top, hit_mid, miss}
lhb_status ∈ {listed, none}
theme_alias_match ∈ {known, unknown}
```

这些标签**不参与加权求和**, 而是作为 setup 触发器的**条件分支**. 例如 Setup C (题材首板突破) 强制要求 `industry_t1_hit = miss`, Setup B (主升期接力) 强制要求 `industry_t1_hit = hit_strong`.

---

## 2. 数据全景 (18 张表 × 3 阶段)

### 2.1 盘前 (09:25 自动)
- `auction.jjyd.vratio` — 集合竞价量比 → **候选池主轴**
- `auction.jjyd.qiangchou` — 强抽 → **候选池主轴**
- `auction.jjyd.net_amount` — 净流入 → **候选池主轴**
- `auction.jjlive.fengdan` — 一字封单 → **候选池主轴**
- `home.kaipan.plate.summary` — T0 主标签 / 子标签 / 板块强度 / 主力流入 (top 20)
- `home.qxlive.top_metrics` — T0 大盘环境指标 12 项

**候选池定义**: 4 张 auction.* 表的股票代码 **并集**, 通常 150-300 只.

### 2.2 盘中 (10:01 自动 + 手动)
- `rank.rocket` — 冲涨榜 (100 只)
- `rank.hot_stock_day` — 热门股 (100 只)
- `pool.hot` — 热门股池
- `pool.surge` — 冲涨股池
- `home.qxlive.top_metrics` — 实时大盘指标 12 项
- `cashflow.stock.{today,3day,5day,10day}` — 个股资金流前 100 (4 个时间窗)

**盘中职能**: **不做新选股**, 仅做盘前候选池 setup 的兑现追踪.

### 2.3 盘后 (17:20 自动)
- `review.daily.top_metrics` — 复盘大盘指标 17 项 (含 PBBX_TOP/1_2/2_3/3_4/4P 连板分层晋级率)
- `review.ltgd.range` — 龙头高度区间涨幅 (4 周期 × 3 板块, ~80 只)
- `review.fupan.plate` — 复盘题材表 (28 字段, 含涨停时序/龙虎榜/异动原因)
- `home.ztpool` — 涨停股池
- `rank.rocket` (盘后版) / `rank.hot_stock_day` (盘后版)
- `cashflow.stock.*`

**盘后职能**: 生成 T-1 标签层, 喂给次日盘前.

### 2.4 闭环层
- `dailyline.stock.manifest` — 候选池股票日线
- `review_backfill` — 历史回填

---

## 3. 标签层规范

### 3.1 候选池主轴 (Layer 0)

输入: 4 张 auction.* 表

输出字段:
```
stock_code, stock_name
in_vratio: bool, vratio_value: float, vratio_rank: int
in_qiangchou: bool, qiangchou_score: float, qiangchou_rank: int
in_net_amount: bool, net_amount_wan: float, net_amount_rank: int
in_fengdan: bool, fengdan_amount: float, fengdan_rank: int
source_count: int  // 1-4, 命中几个 auction 源
auction_time_first: str  // 最早出现在 auction 表的时间戳
```

### 3.2 T-1 单点标签层 (Layer 1)

基于昨日盘后批次的 7 张表, 为今日候选池每只股票生成:

#### 3.2.1 行业 T-1 命中标签

```python
# 输入: T0 stock 所属主标签 (来自 T0 home.kaipan.plate.summary)
# 查询: T-1 home.kaipan.plate.summary 是否包含同一 主标签代码

if T-1 主标签命中:
    delta_strength = T0.板块强度 - T-1.板块强度
    delta_inflow   = T0.主力流入 - T-1.主力流入
    
    if T0.板块强度 >= 0.6 and delta_strength >= 0.3 and delta_inflow >= 0:
        industry_t1_label = 'hit_strong:acceleration'  # 接力加速
    elif T0.板块强度 >= 0.6 and delta_strength >= 0.3 and delta_inflow < 0:
        industry_t1_label = 'hit_strong:pump_no_money'  # 涨价无资金
    elif T0.板块强度 >= 0.6 and delta_strength < 0.3 and delta_inflow >= 5e8:
        industry_t1_label = 'hit_strong:absorb_dip'  # 吸筹回踩
    elif T0.板块强度 < 0.6 and delta_strength <= -0.3:
        industry_t1_label = 'hit_weak:fade'  # 退潮
    else:
        industry_t1_label = 'hit_weak:continuation_weak'  # 命中但弱
else:
    industry_t1_label = 'miss:new_entry'  # 新晋主线
```

#### 3.2.2 个股 T-1 资金流命中

```python
# 查询: T-1 cashflow.stock.today 是否包含 stock_code

if hit:
    pct = T-1.主力净流入分位  # rank/100
    if pct >= 0.7:
        stock_t1_label = 'hit_top'
    else:
        stock_t1_label = 'hit_mid'
else:
    stock_t1_label = 'miss'  # 昨日不在前 100, 'quiet_yesterday'
```

#### 3.2.3 个股 T-1 涨停时序 (来自 review.fupan.plate)

```python
# 查询: T-1 review.fupan.plate 是否包含 stock_code

if hit:
    板数 = row.板数  # int
    连板 = row.连板  # int
    首封时间 = row.首次封板  # 'HH:MM'
    最封时间 = row.最后封板  # 'HH:MM'
    开板次数 = row.开板  # int
    涨停类型 = row.涨停类型  # str: 一字/T字/换手/其它
    龙虎榜 = row.龙虎榜  # '查看' / '无榜'
    封单额 = row.封单额  # float
    异动原因 = row.异动原因  # str
else:
    # T-1 未涨停
    stock_t1_zt = 'no_zt'
```

衍生标签:
```
zt_pattern ∈ {首板, 二板, 三板+, 一字, 烂板, 反包板, 无}
zt_quality ∈ {clean, dirty, none}  # 一字/早封无开板=clean; 多次开板=dirty
lhb_status ∈ {listed, none}
```

### 3.3 T-N 累积标签层 (Layer 2)

#### 3.3.1 龙头高度归属 (来自 review.ltgd.range)

```python
# review.ltgd.range 包含 4 周期 × 3 板块 = 12 组排名
# 每组取前 N 只 (具体看样本, 通常 5-10)

for 周期 in [5日, 10日, 20日, 50日]:
    for 板块 in [主板, 创业科创板, 北交所]:
        if stock_code in 该组排名:
            ltgd_labels.add(f'{周期}_{板块}_rank{排名}')
            ltgd_concepts.update(row.概念键.split(','))

ltgd_count = len(ltgd_labels)  # 0-12
ltgd_max_period = max(命中周期)  # 命中的最长周期
```

衍生标签:
```
longtou_status ∈ {confirmed_longtou, mid_position, follower, none}
# confirmed_longtou: ltgd_count >= 4 且包含 5日 + 10日
# mid_position: ltgd_count >= 2
# follower: ltgd_count == 1
# none: ltgd_count == 0
```

#### 3.3.2 资金流多周期画像 (来自 cashflow.stock.{3day,5day,10day})

```python
for window in [3day, 5day, 10day]:
    if stock_code in cashflow.stock.{window}:
        cashflow_{window}_pct = rank/100
    else:
        cashflow_{window}_pct = None  # miss
```

衍生标签:
```
cashflow_continuity ∈ {accumulating, distributing, neutral, miss}
# accumulating: 3day, 5day, 10day 三个窗口都命中且分位都 >= 0.5
# distributing: 3day 命中且分位 < 0.3, 但 5day/10day 分位 >= 0.5 (近期撤资)
# neutral: 其它命中情形
# miss: 三个窗口都未命中
```

### 3.4 题材 / 大盘环境层 (Layer 3)

#### 3.4.1 大盘体温 (来自 T-1 review.daily.top_metrics + T0 home.qxlive.top_metrics)

```python
# T-1 review.daily.top_metrics 17 项
# T0 home.qxlive.top_metrics 12 项 (盘前快照)

T-1.ZTBX = T-1 涨停板比例 (晋级率)
T-1.LBGD = T-1 连板高度
T-1.PBBX = T-1 排板比 (=连板股数/今日涨停)
T-1.PBBX_TOP = T-1 顶部连板晋级率 (4P 以上)

if T-1.ZTBX < 30% and T-1.PBBX_TOP < 20%:
    market_regime = 'cold'  # 冰点
elif T-1.ZTBX > 60% and T-1.LBGD >= 6:
    market_regime = 'hot'  # 高潮
else:
    market_regime = 'normal'
```

#### 3.4.2 题材识别 (来自 T-1 review.fupan.plate + T0 home.kaipan.plate.summary + theme_aliases)

```python
# 对候选股的所属题材, 查询:
#   T-1 review.fupan.plate 中是否有该题材 → T-1 题材股票数 / 题材涨停数 / 题材说明
#   theme_aliases (v6.3 35 条) 是否能归并到主题

theme_history ∈ {fresh, day1_fermenting, day2_main, day3_high, fading}
# fresh: T-1 不存在该题材 (或 题材股票数 < 3)
# day1_fermenting: T-1 存在但 题材涨停数 <= 2
# day2_main: T-1 题材涨停数 3-7
# day3_high: T-1 题材涨停数 >= 8 且包含连板高度 >= 4
# fading: T-2/T-3 是 day3_high 但 T-1 题材涨停数明显回落
```

---

## 4. Setup 触发器 (field-level)

5 个 setup 互斥; 一只候选股归入 0 或 1 个 setup. 触发器按优先级 A→B→C→D→E 依次评估, 命中即停.

### Setup A: 冰点反弹首日龙头
**特征**: 大盘冷门期, 资金抱团少数标的形成首日龙头.

**触发条件 (全部满足)**:
```
market_regime == 'cold'
source_count >= 3  # auction 4 源至少命中 3 个
in_qiangchou == True AND qiangchou_rank <= 20  # 强抽前 20
vratio_value >= 2.5
auction_time_first <= '09:23:00'  # 强抽时间早
industry_t1_label IN {'miss:new_entry', 'hit_strong:acceleration', 'hit_strong:absorb_dip'}
theme_history IN {'fresh', 'day1_fermenting'}
lhb_status != 'listed'  # 昨日未上龙虎榜 (干净)
ltgd_count <= 2  # 不要已经是确认龙头 (那是 Setup B)
```

**淘汰条件 (任一满足)**:
```
stock_t1_label == 'hit_top' AND zt_pattern IN {'三板+', '一字'}  # 高位货
lhb_status == 'listed'
```

**setup 内排序键**: `qiangchou_rank ASC, vratio_value DESC, net_amount_wan DESC`

**预期 winrate / payoff**: 60-70% / 1:3-5

---

### Setup B: 主升期龙头接力
**特征**: 主线已确认, 龙头延续走强, 接力风险已经体现在价格里.

**触发条件**:
```
market_regime IN {'normal', 'hot'}
source_count >= 2
industry_t1_label == 'hit_strong:acceleration'  # 行业接力加速
theme_history IN {'day2_main', 'day3_high'}
longtou_status IN {'confirmed_longtou', 'mid_position'}
stock_t1_label IN {'hit_top', 'hit_mid'}  # 昨日已强势
zt_pattern IN {'首板', '二板', '三板+', '反包板'}  # T-1 有涨停
zt_quality != 'dirty'  # 排除烂板
in_qiangchou == True OR (in_vratio AND vratio_value >= 3.0)
```

**淘汰条件**:
```
theme_history == 'fading'
zt_pattern == '三板+' AND lhb_status == 'listed' AND market_regime != 'hot'  # 高位分歧
cashflow_continuity == 'distributing'  # 近期资金撤离
```

**setup 内排序键**: `ltgd_count DESC, qiangchou_score DESC, fengdan_amount DESC`

**预期 winrate / payoff**: 55% / 1:1.5-2

---

### Setup C: 题材首板突破
**特征**: 新晋题材的首板首板, 资金从冷门进入新主线.

**触发条件**:
```
market_regime IN {'normal', 'hot'}
source_count >= 2
industry_t1_label == 'miss:new_entry'  # 必须是新晋主线
T0.板块强度 >= 0.6
T0.子标签数量 >= 2  # 真主线, 不是个股噪音
theme_history == 'fresh' OR theme_history == 'day1_fermenting'
stock_t1_label == 'miss' OR stock_t1_label == 'hit_mid'  # 昨日不强或一般
zt_pattern IN {'无', '首板'}  # T-1 未涨停或首板
in_qiangchou == True
net_amount_wan >= 1000
```

**淘汰条件**:
```
longtou_status == 'confirmed_longtou'  # 已经是龙头不算首板
zt_pattern IN {'三板+', '一字'}
```

**setup 内排序键**: `T0.板块强度 DESC, qiangchou_rank ASC, net_amount_wan DESC`

**预期 winrate / payoff**: 50% / 1:2-3

---

### Setup D: 退潮反包
**特征**: 主线退潮但龙头股缩量回踩后反包, 短线博反弹.

**触发条件**:
```
market_regime IN {'cold', 'normal'}  # 不在高潮期博反包
source_count >= 2
industry_t1_label IN {'hit_weak:fade', 'hit_strong:absorb_dip'}
theme_history IN {'day3_high', 'fading'}
longtou_status IN {'confirmed_longtou', 'mid_position'}
zt_pattern == '反包板' OR (zt_pattern == '无' AND ltgd_max_period >= 10日)
cashflow_continuity != 'distributing'  # 资金不能撤离
in_qiangchou == True OR vratio_value >= 2.0
```

**淘汰条件**:
```
stock_t1_label == 'hit_top' AND zt_pattern == '三板+'  # 高位连板
lhb_status == 'listed' AND 异动原因 包含 '机构净卖'
```

**setup 内排序键**: `ltgd_count DESC, vratio_value DESC`

**预期 winrate / payoff**: 40% / 1:3

---

### Setup E: 一字板埋伏
**特征**: 一字 / T 字开盘, 强势封单, 抢筹埋伏次日溢价.

**触发条件**:
```
in_fengdan == True
fengdan_amount >= 5e7  # 5000万封单起
in_qiangchou == True AND qiangchou_rank <= 30
zt_pattern IN {'一字', 'T字'}  # 注意: 这是 T0 预判, 来自 fengdan 表
market_regime != 'cold'
```

**淘汰条件**:
```
lhb_status == 'listed' AND 异动原因 包含 '机构净卖'  # T-1 已经被砸
板数 >= 3  # 三板以上一字风险陡升
```

**setup 内排序键**: `fengdan_amount DESC, qiangchou_rank ASC`

**预期 winrate / payoff**: 80%+ / 0.5-1x (高胜率低赔率, 仓位需要小)

---

## 5. 跨阶段股票池流转

### 5.1 阶段股票范围对比

| 阶段 | 数据源 | 覆盖股票数 | 角色 |
|---|---|---|---|
| 盘前 09:25 | auction.* 4 张并集 | 150-300 | **候选池主轴** |
| 盘前 09:25 | home.kaipan.plate.summary | top 20 主标签 (~100-200 子标签股) | 行业环境标签 |
| 盘前 09:25 | home.qxlive.top_metrics | 全市场指标 | 大盘环境标签 |
| 盘中 10:01 | rank.rocket | 100 | 兑现追踪 |
| 盘中 10:01 | rank.hot_stock_day | 100 | 兑现追踪 |
| 盘中 实时 | pool.hot / pool.surge | 50-200 | 兑现追踪 |
| 盘后 17:20 | review.fupan.plate | 30-80 涨停股 | T-1 涨停标签 |
| 盘后 17:20 | review.ltgd.range | ~80 龙头 | T-N 龙头标签 |
| 盘后 17:20 | cashflow.stock.* | 各 100 | T-N 资金画像 |

### 5.2 流转规则

#### 规则 1: 候选池只来自盘前 auction.* 并集

盘后表用于**给候选股打标签**, 不用于扩展候选池. 即使一只股票在 review.ltgd.range 里是 50 日龙头, 但今天没出现在任何 auction 表里, 它**不进入今日候选池**.

#### 规则 2: 盘前 → 盘中 = setup 锚点继承

盘前给每只候选股标定 setup (A/B/C/D/E) 后, 生成 `intraday_anchors.json`:

```json
{
  "date": "2026-04-26",
  "anchors": [
    {
      "stock_code": "300067",
      "setup": "A",
      "qiangchou_rank": 5,
      "expected_validation": [
        {"window": "09:30-09:45", "check": "in_rank_rocket", "weight": 0.3},
        {"window": "09:30-10:30", "check": "in_pool_surge", "weight": 0.3},
        {"window": "09:30-11:30", "check": "net_inflow_pos", "weight": 0.4}
      ],
      "abort_signals": [
        "price_below_open_after_30min",
        "vol_below_50pct_avg_after_15min"
      ]
    }
  ]
}
```

盘中 cron 在 10:01 读取此 JSON, 比对 rank.rocket / rank.hot_stock_day / pool.hot / pool.surge / cashflow.stock.today, 输出每只候选股的兑现度.

#### 规则 3: 盘中 → 盘后 = 兑现度 + 实际表现归档

盘后 17:20 复盘批次读取早晨的 anchors + 当日 review.fupan.plate / review.daily.top_metrics, 计算每个 setup 的实际命中胜率, 用于次日参数微调.

#### 规则 4: 盘后 → 次日盘前 = 单向的 T-1 标签

盘后批次产生的所有 T-1 标签 (industry_t1_label / stock_t1_label / zt_pattern / longtou_status / cashflow_continuity / theme_history) 序列化为 `labels_T-1.json`, 次日 09:25 候选池主轴生成后 join 进来.

---

## 6. 与 v6 的差异

| 维度 | v6 (含 v6.3.1) | v7 |
|---|---|---|
| 模型 | 线性加分 | Setup 分类器 + 类内排序 |
| T-1 数据缺失 | NULL → 默认分支 | 显式分类标签 (`miss:new_entry` 等) |
| 题材识别 | 35 条手工 alias | alias + theme_history 状态机 |
| 输出 | 单一 final_score 排名 | 5 个 setup 各自的排名表 |
| 龙虎榜 | 未使用 | 仅二值 (listed / none) |
| 跨阶段 | 无显式流转 | anchors.json 序列化锚点 |
| 大盘环境 | market_regime 调系数 | market_regime 决定哪些 setup 被启用 |
| 候选池 | auction.* 并集 (同 v7) | auction.* 并集 (不变) |
| 候选池外标签 | 不区分阶段 | 严格区分: 盘后表只生成 T-1 标签 |

**保留**: alias_groups (6) / alias_entries (35) / numeric_signals 阈值经验 / yesterday_postmarket 思路 (并入 theme_history 和 cashflow_continuity).

**淘汰**: rank_scores 加权求和 / source_hit_bonuses 直接加分 / final_score 单一指标.

---

## 7. 实施路径 (后续 PR 拆分)

本文档 (PR 当前) 只是设计基线, 不改任何代码. 后续:

1. **PR-N+1** `feat(premarket-v7): label extractor module`
   - 新增 `scripts/duanxianxia_premarket_v7_labels.py`
   - 实现 Layer 0 / 1 / 2 / 3 标签生成
   - 输出 `captures/{date}/labels.json`
   - 不替换 v6, 并行运行

2. **PR-N+2** `feat(premarket-v7): setup classifier`
   - 新增 `scripts/duanxianxia_premarket_v7.py`
   - 读取 labels.json, 输出 5 个 setup 的候选股表
   - 输出 `captures/{date}/intraday_anchors.json`
   - 与 v6 双跑对比 1-2 周

3. **PR-N+3** `feat(intraday): anchor validator`
   - 新增 `scripts/duanxianxia_intraday_validator.py`
   - 10:01 cron 读取 anchors.json, 输出兑现度报告

4. **PR-N+4** `feat(postmarket-v7): setup performance backfill`
   - 在 `duanxianxia_review_backfill.py` 中加入 setup 历史归因
   - 用于参数微调

5. **PR-N+5** (双跑通过后) `chore(premarket): retire v6 from cron`
   - 切换 cron 从 v6 到 v7

---

## 8. 已知未决问题

1. **`pool.hot` / `pool.surge` 字段细节未通过样本验证** (samples/2026-04-23 仅含盘后批次, 无盘中). PR-N+1 实现前需要从 `scripts/duanxianxia_fetcher.py` 读取实际下载逻辑确认字段名.
2. **`theme_history` 状态机的具体阈值** (`day1_fermenting` 涨停数 ≤ 2 等) 需要在双跑期间根据实盘数据微调.
3. **Setup 之间是否真正互斥** 需要在双跑期间验证. 如果一只股票频繁同时满足 A 和 C, 触发条件需要进一步收紧.
4. **`market_regime == 'cold'` 时 Setup B/C/E 是否完全禁用** 需要回测确认; 当前设计是禁用, 但可能过于保守.

---

_本文档为 v7 设计基线. 任何代码改动请在后续 PR 中引用本文档对应的 setup 编号 (Setup A-E) 或标签名 (industry_t1_label 等)._
