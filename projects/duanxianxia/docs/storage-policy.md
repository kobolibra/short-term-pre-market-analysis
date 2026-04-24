# 存储策略

## 核心要求

用户已明确要求：

> 现在要求抓取的表格数据，每次都要存储；这是一个长期项目；建一个单独的项目。

因此从本文件起，`duanxianxia` 项目采用以下规则：

1. 每次抓取必须落盘。
2. 每次落盘都要保留抓取时间。
3. 时间默认以北京时间（Asia/Shanghai）记载。
4. 原则上至少存：
   - dataset id
   - dataset label
   - source path/url
   - fetched_at
   - row count
   - headers
   - rows
5. 若同一次抓取包含多个分组（例如主板/创业科创板/北交所），应保留分组结构，不要只保留扁平文本。

## 建议落盘路径

- `captures/YYYY-MM-DD/<dataset-id>/<HHMMSS>.json`

例如：

- `captures/2026-04-09/rank.rocket/162501.json`
- `captures/2026-04-09/pool.hot/162503.json`
- `captures/2026-04-09/review.daily.top_metrics/162530.json`

## 设计原则

- 落盘内容优先保留结构化 JSON，而不是只有自然语言结果。
- 用户可读输出和落盘结构应分离。
- 默认不要擅自截断返回结果。

## 当前落地状态（2026-04-09）

- `scripts/duanxianxia_fetcher.py` 已实现默认落盘。
- 当前已验证落盘样例：
  - `captures/2026-04-09/rank.rocket/194320.json`
- 输出 JSON 当前包含：
  - `project`
  - `dataset_kind`
  - `dataset_id`
  - `dataset_label`
  - `source_path`
  - `source_url`
  - `fetched_at`
  - `fetched_at_utc`
  - `timezone`
  - `row_count`
  - `headers`
  - `rows`
  - `meta`
