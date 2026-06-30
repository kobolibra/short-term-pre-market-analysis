#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_seal_amount_probe_20260630.py -- Task 0091 seal_amount unit gate.

Confirms weimai raw[17] 'seal_amount' (封单额) is stored in 万 (x1e4 -> 元),
NOT 元, using REAL decrypted live rows from 2026-06-30
(https://duanxianxia.com/vendor/stockdata/daban.json, AES-256-CBC).

Only sealed (涨停封板) rows carry raw[17]. For each we require
  canonical seal_amount == raw[17] * 1e4   (unit 'wan', matches field-rename-map §4)
plus a money-scale floor. Treating raw[17] as 元 (the 0090 bug) makes a
limit-up board's 封单额 only a few 万元 -- impossible -- so the assert blocks it.

Standalone run executes the gate; rc=0 = pass. READ-ONLY, no writes, no push.
"""
from __future__ import annotations
import duanxianxia_canonical as C

# REAL rows decrypted from 2026-06-30 daban.json (weimai, raw[18]); sealed boards only.
ROWS_20260630 = [
    ["002407", "多氟多", 50.23, 10.01, 600173154, 9.05, -153407966, 1.27, 640697720, 190060274, 640697720, "氢氟酸、电解液", 50799828505, 1525864501, 5874720450, -4348855949, "2连板", 64230],
    ["002273", "水晶光电", 35.48, 10.02, 143647876, 9.61, 91855857, 0.5, 219901745, 122402452, 219901745, "CPO/MPO、AI眼镜", 43933658320, 638148801, 1222432829, -584284028, "首板", 149266],
    ["000603", "盛达资源", 23.1, 10, 499802599, 10, 5897064, 0.62, 56742840, 463370209, 56742840, "黄金、金属锌", 9123640011, 80342420, 126228538, -45886118, "首板", 34899],
    ["002175", "*ST东智", 2.26, 5.12, 180642026, 5.12, 0, 0.01, 224553, 9623984, 224553, "工业母机、元宇宙", 2331215197, 2198208, 2198208, 0, "2连板", 12471],
    ["600180", "*ST瑞茂", 1.25, 5.04, 62701625, 5.04, 0, 0.09, 450000, 432250, 450000, "转口贸易、东盟自贸区", 497811206, 1318250, 1633875, -315625, "3连板", 3027],
]


def run():
    print("seal_amount unit gate (2026-06-30 live weimai):")
    for r in ROWS_20260630:
        raw17 = r[17]
        cv = C.raw_to_canonical("auction.jjyd.weimai", r)
        sa = cv["seal_amount"]
        assert sa == round(raw17 * 1e4), (r[0], raw17, sa)
        assert sa != raw17, (r[0], "seal_amount wrongly left as yuan")
        assert sa >= 1e6, (r[0], "seal_amount too small to be 封单额 in yuan", sa)
        print("  %s %-6s raw[17]=%-8d -> seal_amount=%d yuan (%.2f yi)" % (
            r[0], r[1], raw17, sa, sa / 1e8))
    print("SEAL-AMOUNT GATE: PASS (raw[17] is wan; x1e4 -> yuan)")
    return True


run()

if __name__ == "__main__":
    print("duanxianxia_seal_amount_probe_20260630: PASS")
