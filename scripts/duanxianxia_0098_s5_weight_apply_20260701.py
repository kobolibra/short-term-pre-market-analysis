#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""duanxianxia_0098_s5_weight_apply_20260701.py -- Task 0098: 将 0093 推荐的 S5_amt_liq_core
权重写入生产文件 duanxianxia_v9_edge.py (default 参数) 和 v10_optimize.py (V10AMT_W)。

0093 walk-forward OOS 结论:
  S5_amt_liq_core: amt_pct 0.3232 / auction_strength 0.0909 / liquidity 0.2424
                   / money 0.1616 / pressure_score 0.1414 / weimai_strength 0.0303 / orderbook 0.0202
  OOS mean_IC 0.129 (baseline 0.1278) | ICIR 0.903 (baseline 0.895) | beats 8/12 days
  robust_better: [S5, S3, S4]; recommendation = S5

操作:
  1. patch duanxianxia_v9_edge.py: p.get("edge_w_xxx", OLD) -> NEW
  2. patch v10_optimize.py: V10AMT_W = OLD -> NEW
  3. 自我验证: 导入并运行 v9_edge._self_test() + 检查 V10AMT_W
  4. 输出 JSON 摘要 rc=0
"""
from __future__ import annotations
import importlib
import json
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.resolve()
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# S5 目标权重 (来自 0093 recommendation)
S5 = {
    "amt_pct": 0.3232,
    "auction_strength": 0.0909,
    "liquidity": 0.2424,
    "money": 0.1616,
    "pressure_score": 0.1414,
    "weimai_strength": 0.0303,
    "orderbook": 0.0202,
}
# 校验总和应约等于 baseline 总和 (1.01)
S5_SUM = sum(S5.values())  # ~1.0100

# 参数名映射: core_field -> edge param key
FIELD_TO_PARAM = {
    "amt_pct": "edge_w_amt",
    "auction_strength": "edge_w_auction",
    "liquidity": "edge_w_liquidity",
    "money": "edge_w_money",
    "pressure_score": "edge_w_pressure",
    "weimai_strength": "edge_w_weimai",
    "orderbook": "edge_w_orderbook",
}

# 旧默认值 (S0_baseline)
OLD_DEFAULTS = {
    "edge_w_amt": 0.23,
    "edge_w_auction": 0.19,
    "edge_w_liquidity": 0.18,
    "edge_w_money": 0.14,
    "edge_w_pressure": 0.14,
    "edge_w_weimai": 0.08,
    "edge_w_orderbook": 0.05,
}


def patch_v9_edge(path: Path) -> dict:
    """把 compute_edge_v9 里 p.get('edge_w_xxx', OLD) 的 OLD 换为 S5 值。"""
    text = path.read_text(encoding="utf-8")
    original = text
    changes = []

    for field, param in FIELD_TO_PARAM.items():
        old_val = OLD_DEFAULTS[param]
        new_val = S5[field]
        # 匹配: p.get("edge_w_xxx", 0.23) 或 p.get('edge_w_xxx', 0.23)
        pattern = r'(p\.get\(["\']' + re.escape(param) + r'["\'],\s*)' + re.escape(str(old_val)) + r'(\))'
        replacement = r'\g<1>' + str(new_val) + r'\g<2>'
        new_text, n = re.subn(pattern, replacement, text)
        if n > 0:
            changes.append({"param": param, "old": old_val, "new": new_val, "occurrences": n})
            text = new_text
        else:
            changes.append({"param": param, "old": old_val, "new": new_val, "occurrences": 0, "warn": "pattern not found"})

    if text != original:
        path.write_text(text, encoding="utf-8")

    return {"file": str(path), "changed": text != original, "changes": changes}


def patch_v10_optimize(path: Path) -> dict:
    """把 V10AMT_W = {...OLD...} 替换为 S5 值。"""
    text = path.read_text(encoding="utf-8")
    original = text

    new_dict_str = (
        '{"amt_pct": ' + str(S5["amt_pct"]) +
        ', "auction_strength": ' + str(S5["auction_strength"]) +
        ', "liquidity": ' + str(S5["liquidity"]) +
        ', "money": ' + str(S5["money"]) +
        ', "pressure_score": ' + str(S5["pressure_score"]) +
        ', "weimai_strength": ' + str(S5["weimai_strength"]) +
        ', "orderbook": ' + str(S5["orderbook"]) + '}'
    )

    # 匹配整行 V10AMT_W = {...}
    pattern = r'V10AMT_W\s*=\s*\{[^}]+\}'
    replacement = 'V10AMT_W = ' + new_dict_str
    new_text, n = re.subn(pattern, replacement, text)

    changed = False
    if n > 0 and new_text != original:
        path.write_text(new_text, encoding="utf-8")
        changed = True

    return {"file": str(path), "changed": changed, "occurrences": n, "new_V10AMT_W": new_dict_str}


def verify_imports() -> dict:
    """重新导入 v9_edge 和 v10_optimize 并验证权重。"""
    result = {}

    # v9_edge: 重新加载模块 + 跑 _self_test
    try:
        import duanxianxia_v9_edge as v9e
        importlib.reload(v9e)
        v9e._self_test()
        # 用 S5 权重跑一次 compute_edge_v9 并检查 edge_score 合理
        d = {
            "code": "600000",
            "auction_strength": 78,
            "auction_detail": {
                "latest_change_pct": 3.0,
                "money_intent_score": 70,
                "net_pressure": 0.0015,
                "orderbook_quality_score": 60,
                "liquidity_score": 70,
                "source_evidence_score": 25,
                "auction_amount_pct": 80.0,
            },
            "weimai_detail": {"weimai_strength": 65},
            "context_detail": {"cashflow_continuity": "strong", "market_longtou_height": 6},
        }
        out = v9e.compute_edge_v9(d, {"market_env_score": 68, "risk_flags": []}, {})
        result["v9_edge"] = {
            "ok": True,
            "self_test": "passed",
            "sample_edge_score": out["edge_score"],
            "sample_alpha_type": out["alpha_type"],
        }
    except Exception as e:
        result["v9_edge"] = {"ok": False, "error": str(e)}

    # v10_optimize: 重新加载并检查 V10AMT_W
    try:
        import v10_optimize as v10
        importlib.reload(v10)
        w = v10.V10AMT_W
        assert abs(w.get("amt_pct", 0) - S5["amt_pct"]) < 0.001, f"amt_pct mismatch: {w}"
        assert abs(w.get("liquidity", 0) - S5["liquidity"]) < 0.001, f"liquidity mismatch: {w}"
        result["v10_optimize"] = {"ok": True, "V10AMT_W": w}
    except Exception as e:
        result["v10_optimize"] = {"ok": False, "error": str(e)}

    return result


def main():
    v9_edge_path = SCRIPTS_DIR / "duanxianxia_v9_edge.py"
    v10_opt_path = SCRIPTS_DIR / "v10_optimize.py"

    assert v9_edge_path.exists(), f"NOT FOUND: {v9_edge_path}"
    assert v10_opt_path.exists(), f"NOT FOUND: {v10_opt_path}"

    r1 = patch_v9_edge(v9_edge_path)
    r2 = patch_v10_optimize(v10_opt_path)
    r3 = verify_imports()

    out = {
        "probe": "0098_s5_weight_apply",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "s5_weights": S5,
        "s5_sum": round(S5_SUM, 4),
        "baseline_weights": {FIELD_TO_PARAM[k]: v for k, v in zip(
            ["amt_pct", "auction_strength", "liquidity", "money", "pressure_score", "weimai_strength", "orderbook"],
            [0.23, 0.19, 0.18, 0.14, 0.14, 0.08, 0.05]
        )},
        "patch_v9_edge": r1,
        "patch_v10_optimize": r2,
        "verify": r3,
        "source": "0093 recommendation: S5_amt_liq_core, OOS IC 0.129 vs baseline 0.1278, beats 8/12 days",
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))

    # 硬闸门
    assert r3["v9_edge"]["ok"], "v9_edge import/self_test failed after patch"
    assert r3["v10_optimize"]["ok"], "v10_optimize import/V10AMT_W check failed after patch"
    warn_params = [c["param"] for c in r1["changes"] if c.get("occurrences", 0) == 0]
    assert not warn_params, f"以下参数 pattern 未在 v9_edge.py 中找到: {warn_params}"
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
