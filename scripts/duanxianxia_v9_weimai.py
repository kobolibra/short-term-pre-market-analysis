"""
duanxianxia_v9_weimai.py — 竞价涨停委买/撮合微观结构特征 (auction.jjyd.weimai)

把之前被分析层忽略的 auction.jjyd.weimai 转成一等盘口特征。
所有字段做防御式多键解析(对齐项目既有风格),原始行存入 detail['raw'],
另给派生数值特征 + 0-100 的 weimai_strength。

中文 → 防御式候选键:
  委买/撮合     weimai_amount_wan / 委买 / 委买额 / 撮合 / match_amount / weimai
  涨幅          latest_change_pct / 涨幅
  竞价主力      auction_main_wan / 竞价主力 / main_force
  封单          seal / 封单 / fengdan
  封单额        seal_amount_wan / 封单额 / fengdan_amount_wan
  流通值        market_cap / 流通值 / 流通市值 / seal_amount_wan(legacy)
  竞涨          auction_change_pct / 竞涨
  概念1/概念2   concept(split) / 概念1 / 概念2
  连板标签      board_label / 连板标签
  主力净流入    main_net_inflow_wan / 主力净流入 / net_amount
  特大单净流入  super_large_net_inflow_wan / 特大单净流入
  大单净流入    large_order_net_inflow_wan / 大单净流入
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def _norm_code(value: Any) -> str:
    s = str(value or "").strip()
    if "." in s:
        s = s.split(".")[-1]
    return s[-6:] if len(s) >= 6 else s


def _to_float(v: Any) -> Optional[float]:
    try:
        if v in (None, "", "-", "none", "None"):
            return None
        return float(str(v).replace("%", "").replace(",", "").strip())
    except Exception:
        return None


def _money_to_wan(v: Any) -> Optional[float]:
    """金额归一到“万”。支持 亿/万 后缀与纯数字(默认按元?)——这里按项目口径:带亿/万后缀换算,纯数字视作万。"""
    if v in (None, "", "-", "none", "None"):
        return None
    s = str(v).replace(",", "").strip()
    try:
        if "亿" in s:
            return float(s.replace("亿", "")) * 10000.0
        if "万" in s:
            return float(s.replace("万", ""))
        return float(s)
    except Exception:
        return None


def _first(row: Dict[str, Any], keys: List[str], conv=_to_float) -> Optional[float]:
    for k in keys:
        if k in row and row.get(k) not in (None, "", "-"):
            val = conv(row.get(k))
            if val is not None:
                return val
    return None


def _first_str(row: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in row and str(row.get(k) or "").strip():
            return str(row.get(k)).strip()
    return ""


def _concepts(row: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for k in ("概念1", "concept_1"):
        s = _first_str(row, [k])
        if s:
            out.append(s)
    for k in ("概念2", "concept_2"):
        s = _first_str(row, [k])
        if s:
            out.append(s)
    raw = _first_str(row, ["concept", "概念"])
    if raw:
        for part in raw.replace("，", "、").replace("|", "、").split("、"):
            p = part.strip()
            if p and p not in out:
                out.append(p)
    return out[:4]


AMT_KEYS_WEIMAI = ["weimai_amount_wan", "委买额", "委买", "撮合", "match_amount", "weimai"]
AMT_KEYS_SEAL = ["seal_amount_wan", "封单额", "fengdan_amount_wan", "fengdan_amount", "封单"]
AMT_KEYS_MAIN = ["auction_main_wan", "竞价主力", "main_force", "main_force_wan"]
AMT_KEYS_MCAP = ["market_cap", "流通值", "流通市值", "seal_amount_wan"]
AMT_KEYS_NET = ["main_net_inflow_wan", "主力净流入", "main_net_inflow", "net_amount"]
AMT_KEYS_SUPER = ["super_large_net_inflow_wan", "特大单净流入", "super_large_net_inflow"]
AMT_KEYS_LARGE = ["large_order_net_inflow_wan", "大单净流入", "large_order_net_inflow"]
PCT_KEYS = ["latest_change_pct", "涨幅"]
AUCTION_PCT_KEYS = ["auction_change_pct", "竞涨"]
BOARD_KEYS = ["board_label", "连板标签"]


def _index_weimai(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        code = _norm_code(r.get("code") or r.get("代码"))
        if code and code not in out:
            out[code] = r
    return out


def _percentiles(pairs: List[tuple]) -> Dict[str, float]:
    valid = [(c, v) for c, v in pairs if c and v is not None]
    if not valid:
        return {}
    valid.sort(key=lambda x: x[1], reverse=True)
    n = len(valid)
    out: Dict[str, float] = {}
    for i, (c, _v) in enumerate(valid):
        out.setdefault(c, round((n - i) / n * 100.0, 2))
    return out


def compute_weimai_features(
    candidate_codes: List[str],
    weimai_rows: List[Dict[str, Any]],
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    p = params or {}
    idx = _index_weimai(weimai_rows)

    # 跨候选/全表做百分位,用于把绝对额转成 0-100
    wm_pairs, net_pairs = [], []
    for code, r in idx.items():
        wm_pairs.append((code, _first(r, AMT_KEYS_WEIMAI, _money_to_wan)))
        net_pairs.append((code, _first(r, AMT_KEYS_NET, _money_to_wan)))
    wm_pct = _percentiles(wm_pairs)
    net_pct = _percentiles(net_pairs)

    out: Dict[str, Dict[str, Any]] = {}
    codes = candidate_codes if candidate_codes else list(idx.keys())
    for raw in codes:
        code = _norm_code(raw)
        if not code or code in out:
            continue
        r = idx.get(code)
        if r is None:
            out[code] = {"weimai_present": False, "weimai_strength": 0.0, "raw": None}
            continue

        weimai_wan = _first(r, AMT_KEYS_WEIMAI, _money_to_wan)
        seal_wan = _first(r, AMT_KEYS_SEAL, _money_to_wan)
        main_wan = _first(r, AMT_KEYS_MAIN, _money_to_wan)
        mcap_wan = _first(r, AMT_KEYS_MCAP, _money_to_wan)
        net_wan = _first(r, AMT_KEYS_NET, _money_to_wan)
        super_wan = _first(r, AMT_KEYS_SUPER, _money_to_wan)
        large_wan = _first(r, AMT_KEYS_LARGE, _money_to_wan)
        latest_pct = _first(r, PCT_KEYS)
        auction_pct = _first(r, AUCTION_PCT_KEYS)
        board_label = _first_str(r, BOARD_KEYS)
        concepts = _concepts(r)

        # 派生:委买/封单比 (委买相对封单的真实承接强度)
        weimai_to_seal = (weimai_wan / seal_wan) if (weimai_wan and seal_wan and seal_wan > 0) else None
        # 派生:封单/流通比 (封单相对流通盘的厚度)
        seal_to_mcap = (seal_wan / mcap_wan) if (seal_wan and mcap_wan and mcap_wan > 0) else None
        # 派生:大资金占比 (特大+大单 占主力净流入)
        big_sum = sum(x for x in (super_wan, large_wan) if x is not None)
        big_share = (big_sum / net_wan) if (net_wan and net_wan > 0 and big_sum) else None
        # 派生:净流入相对流通盘压强
        net_pressure = (net_wan / mcap_wan) if (net_wan and mcap_wan and mcap_wan > 0) else None

        board_bonus = 0.0
        if board_label:
            if any(t in board_label for t in ("首板", "1板")):
                board_bonus = float(p.get("weimai_board_first_bonus", 6))
            elif any(t in board_label for t in ("2板", "二板", "3板", "三板", "连板")):
                board_bonus = float(p.get("weimai_board_relay_bonus", 10))

        strength = (
            float(p.get("weimai_w_amount", 0.45)) * float(wm_pct.get(code, 0.0))
            + float(p.get("weimai_w_net", 0.30)) * float(net_pct.get(code, 0.0))
            + float(p.get("weimai_w_seal", 0.15)) * (min(100.0, (seal_to_mcap or 0.0) / float(p.get("seal_to_mcap_full", 0.02)) * 100.0))
            + float(p.get("weimai_w_big", 0.10)) * (min(100.0, (big_share or 0.0) * 100.0))
            + board_bonus
        )
        strength = max(0.0, min(100.0, strength))

        out[code] = {
            "weimai_present": True,
            "weimai_strength": round(strength, 2),
            "weimai_amount_wan": weimai_wan,
            "seal_amount_wan": seal_wan,
            "auction_main_wan": main_wan,
            "market_cap_wan": mcap_wan,
            "main_net_inflow_wan": net_wan,
            "super_large_net_inflow_wan": super_wan,
            "large_order_net_inflow_wan": large_wan,
            "latest_change_pct": latest_pct,
            "auction_change_pct": auction_pct,
            "board_label": board_label,
            "concepts": concepts,
            "weimai_amount_pct": wm_pct.get(code, 0.0),
            "net_inflow_pct": net_pct.get(code, 0.0),
            "weimai_to_seal_ratio": round(weimai_to_seal, 4) if weimai_to_seal is not None else None,
            "seal_to_mcap_ratio": round(seal_to_mcap, 6) if seal_to_mcap is not None else None,
            "big_order_share": round(big_share, 4) if big_share is not None else None,
            "net_pressure": round(net_pressure, 6) if net_pressure is not None else None,
            "raw": dict(r),   # 原样留底,供复盘/DeepSeek 审查
        }
    return out


def _self_test() -> None:
    rows = [
        {"code": "600000", "委买": "8000万", "封单额": "1.2亿", "流通值": "60亿",
         "主力净流入": "5000万", "特大单净流入": "3000万", "大单净流入": "1500万",
         "涨幅": "6.1", "竞涨": "3.2", "连板标签": "2板", "concept": "算力、液冷"},
        {"code": "000001", "委买": "1000万", "封单额": "2000万", "流通值": "30亿",
         "主力净流入": "800万", "涨幅": "1.0", "连板标签": "首板", "概念1": "消费"},
    ]
    out = compute_weimai_features(["600000", "000001", "999999"], rows, {})
    assert out["600000"]["weimai_present"] is True
    assert out["600000"]["weimai_strength"] > out["000001"]["weimai_strength"]
    assert out["999999"]["weimai_present"] is False
    assert out["600000"]["concepts"] == ["算力", "液冷"]
    print("v9_weimai _self_test passed")


if __name__ == "__main__":
    _self_test()
