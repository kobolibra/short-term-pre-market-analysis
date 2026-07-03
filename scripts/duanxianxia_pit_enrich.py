#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_pit_enrich.py -- Task 0118 (additive, read-only).

把 v9 回测候选(all_candidates, 提供策略层: edge/action_type/board/matched_plate/ltgd)
按 code 与 PIT 面板(duanxianxia_pit_panel.build_pit_panel)对齐, 用 canonical 数据层
字段覆盖 v9 里长期缺失(coverage≈0)的 ff/换手/量比/amt/bidstrength。

关键纪律(防止单列内混单位破坏 z-score):
  - 数据层字段一律只来自 PIT(统一 canonical 单位=元/百分比); PIT 无匹配 -> 全部置 None,
    绝不回落到 v9 的旧单位(v9 amt=万, PIT amt=元)。
  - auction_pct 例外: v9/PIT 同为百分比, 单位安全, 故优先 PIT, 缺失保留 v9。
  - bidstrength = amt/ff, 两者同为元 -> 无量纲, 跨行可比。
只读; 不写 git。
"""
from __future__ import annotations
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from duanxianxia_pit_panel import build_pit_panel  # noqa: E402

# PIT(canonical INDICATORS) -> 回测数据层字段。
# ff/turnover/volume_ratio/amt 单位由 v9(万/探测) 切换为 canonical(元/百分比),
# 因此必须全行统一取 PIT, 不与 v9 混用。
DATA_FIELDS = ("ff", "turnover", "volume_ratio", "amt", "bidstrength")


def _num(x):
    try:
        if x in (None, "", "-", "None"):
            return None
        return float(str(x).replace("%", "").replace(",", "").strip())
    except Exception:  # noqa: BLE001
        return None


def build_pit_index(captures_root, date, as_of_slot="premarket", cutoff="09:29"):
    """返回 (index: code->panel_row, summary)。code 已按 PIT 内部规范化(6位)。"""
    res = build_pit_panel(captures_root, date, as_of_slot=as_of_slot, cutoff=cutoff)
    panel = res.get("panel") or {}
    return panel, res.get("summary") or {}


def enrich_fields(f, pit_row):
    """把回测行 f 的数据层字段替换为 PIT 值; pit_row 为空则数据层全部置 None。
    策略层(edge/action_type/board/matched_plate/ltgd_*/styles)保持 v9 不动。"""
    if not pit_row:
        for k in DATA_FIELDS:
            f[k] = None
        f["_pit"] = False
        return f
    ff = _num(pit_row.get("free_float_mktcap"))
    turn = _num(pit_row.get("turnover_rate"))
    vr = _num(pit_row.get("volume_ratio"))
    amt = _num(pit_row.get("auction_turnover"))
    apct = _num(pit_row.get("auction_change_pct"))
    f["ff"] = ff
    f["turnover"] = turn
    f["volume_ratio"] = vr
    f["amt"] = amt
    f["bidstrength"] = (amt / ff) if (amt is not None and ff and ff > 0) else None
    if apct is not None:
        f["auction_pct"] = apct  # 同为百分比, 单位安全
    f["_pit"] = True
    return f


def _self_test():
    # pit_row=None -> 数据层清空, 策略层保留
    f = {"code": "000001", "edge": 1.0, "auction_pct": 2.5, "amt": 999.0,
         "ff": 111.0, "matched_plate": "AI"}
    enrich_fields(f, None)
    assert all(f[k] is None for k in DATA_FIELDS), f
    assert f["auction_pct"] == 2.5 and f["matched_plate"] == "AI" and f["_pit"] is False
    # 有 PIT -> 元/元 => 无量纲 bidstrength; auction_pct 优先 PIT
    f2 = {"code": "000001", "edge": 1.0, "auction_pct": 2.5, "amt": 999.0, "ff": 111.0}
    enrich_fields(f2, {"free_float_mktcap": 4.24e9, "auction_turnover": 2.12e8,
                       "turnover_rate": 5.1, "volume_ratio": 2.3,
                       "auction_change_pct": 7.7})
    assert f2["ff"] == 4.24e9 and f2["amt"] == 2.12e8
    assert abs(f2["bidstrength"] - (2.12e8 / 4.24e9)) < 1e-12
    assert f2["turnover"] == 5.1 and f2["volume_ratio"] == 2.3
    assert f2["auction_pct"] == 7.7 and f2["_pit"] is True
    # PIT 缺 auction_change_pct -> 保留 v9 的 auction_pct
    f3 = {"code": "x", "auction_pct": 3.3}
    enrich_fields(f3, {"free_float_mktcap": 1e9})
    assert f3["auction_pct"] == 3.3 and f3["amt"] is None
    return True


_self_test()


if __name__ == "__main__":
    print("duanxianxia_pit_enrich self-test OK; DATA_FIELDS=%s" % (DATA_FIELDS,))
