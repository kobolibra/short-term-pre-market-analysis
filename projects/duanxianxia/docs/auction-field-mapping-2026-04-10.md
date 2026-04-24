# duanxianxia 竞价类最终用户可见字段映射（2026-04-10）

仅记录当前对外展示口径；默认中文表头。

## 1) 竞价封板 `auction.jjlive.fengdan`

**正式展示字段**
- 名称 ← `name`
- 代码 ← `code`
- 题材1 ← `tag_1`
- 题材2 ← `tag_2`
- 连板标签 ← `board_label`
- 9:15 ← `amount_915`
- 9:20 ← `amount_920`
- 9:25 ← `amount_925`
- 涨幅 ← `latest_change_pct`

**补充说明**
- 原始解析里 `tag_3` 仍保留在 capture，用于兼容最多 3 个标签；但正式默认表只展示 `题材1/题材2/连板标签`。
- 当前页面标签来自 `td.fd` 下的直接 `<p>`；最后一个若匹配 `首板 / 2板 / 3板 / 昨首板 / 昨2板 / 7天5板` 等板标签，则落到 `board_label`，其余前置标签落到 `tag_1/tag_2/tag_3`。
- 数值列头固定为：`9:15 | 9:20 | 9:25 | 涨幅`。
- 2026-04-10 14:33 UTC 现场复核原网页 DOM 后确认：封单表数值列取法应为 `td.fd` 下 `:scope > span` 的前 4 项，其中第 4 项（通常类名 `zf`）就是 `涨幅`。此前因解析器对该节点抓取不稳，导致涨幅列错位/漏取；该说法现已作废。

## 2) 竞价爆量 `auction.jjyd.vratio`

**正式展示字段**
- 名称 ← `name`
- 代码 ← `code`
- 涨幅 ← `latest_change_pct`
- 竞额 ← `auction_turnover_wan`
- 昨竞额 ← `yesterday_auction_turnover_wan`
- 竞价换手 ← `turnover_rate_pct`
- 竞价量比 ← `volume_ratio_multiple`
- 竞涨 ← `auction_change_pct`
- 流通值 ← `seal_amount_wan`
- 概念 ← `concept`

**备注**
- 这里“涨幅”按用户确认，使用 `latest_change_pct`（现涨）。
- 当前代码里的 `seal_amount_wan` 实际对应该接口第 4 位；按已核口径，对外视作“流通值”列使用。
- `auction_volume_ratio` 不作为正式默认表头输出。
- 2026-04-10 13:55 UTC 用户最新明确纠偏：`竞价爆量` 正式表需要补上 `竞价换手` 列；后续默认纳入正式展示字段。 

## 3) 竞价抢筹 `auction.jjyd.qiangchou`

必须拆成两张表：

### 3.1 `qiangchou` 组
- 表名：`9:20 - 9:25的抢筹幅度`
- 名称 ← `name`
- 代码 ← `code`
- 涨幅 ← `latest_change_pct`
- 竞额 ← `auction_turnover_wan`
- 抢筹幅度 ← `grab_strength`
- 竞价换手 ← `turnover_rate_pct`
- 竞涨 ← `auction_change_pct`
- 流通值 ← `seal_amount_wan`
- 概念 ← `concept`

### 3.2 `grab` 组
- 表名：`竞价最后1秒的抢筹幅度`
- 字段映射与上面完全相同，只是 `group = grab`

**备注**
- 当前 capture 里两组共存，必须按 `group` 拆开展示，不能再混成一张，也不能只发 `grab`。

## 4) 竞价净额 `auction.jjyd.net_amount`

**正式展示字段**
- 名称 ← `name`
- 代码 ← `code`
- 涨幅 ← `latest_change_pct`
- 竞价换手 ← `turnover_rate_pct`
- 竞涨 ← `auction_change_pct`
- 主力净买 ← `net_amount`
- 竞额 ← `auction_turnover`
- 流通值 ← `market_cap`
- 概念1 ← `concept` 拆分第 1 项
- 概念2 ← `concept` 拆分第 2 项

**备注**
- `concept` 原始值常为 `概念1、概念2`；对外需拆成两列。
- 当前代码还保留 `main_net_inflow / super_large_net_inflow / large_order_net_inflow / board_label` 等原始字段，但不作为默认正式表头。

## 5) 最小结论
- 抢筹正式输出 = 两张表：`qiangchou` + `grab`
- 封板正式输出 = `题材1 | 题材2 | 连板标签 | 9:15 | 9:20 | 9:25 | 涨幅`
- 以上为 2026-04-10 已核定口径，后续若页面改版，再以 live 页面 + capture 复核更新。
