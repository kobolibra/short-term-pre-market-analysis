#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_data_audit.py -- Task 0112 数据地基体检 (additive, read-only).

一次性、可复现地体检数据地基的三根支柱：
  1) 数据下载/保存是否完整准确  (captures inventory: 覆盖的日期 x 数据集、行数、
     JSON 合法性、抓取时刻 vs 时间隔离 cutoff 的 post-cutoff 泄漏文件数)
  2) 字段指标是否完全清楚      (canonical REGISTRY 覆盖了哪些数据集；每个字段的非空
     覆盖率；哪些已抓取数据集还没有 canonical 口径映射 = 字段未定型)
  3) 合并的宽表是否准确/完整   (跑 feature_builder：每个来源表 coverage、输出字段非空率、
     bidStrength 可算率；以及已抓取的表里有哪些没有 fold 进宽表)

只读；不写 git。完整报告打到 stdout（worker 只保留末 16KB，故末尾放 JSON 总表）。
用法: python3 scripts/duanxianxia_data_audit.py [captures_dir] [--cutoff HH:MM]
"""
from __future__ import annotations
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
WS = HERE.parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

CUTOFF = "09:29"
_args = sys.argv[1:]
_cap_override = None
_i = 0
while _i < len(_args):
    _a = _args[_i]
    if _a == "--cutoff" and _i + 1 < len(_args):
        CUTOFF = _args[_i + 1]
        _i += 2
        continue
    _cap_override = _a
    _i += 1

CAP_DIR = Path(_cap_override) if _cap_override else (WS / "projects" / "duanxianxia" / "captures")
if not CAP_DIR.is_absolute():
    CAP_DIR = WS / CAP_DIR
CFG = WS / "projects" / "duanxianxia" / "config" / "datasets.json"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def jload(p):
    try:
        return json.loads(Path(p).read_text(encoding="utf-8")), None
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {e}"


def rows_of(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("rows", "items", "data", "list"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


# ---- import the layer under test (best-effort; each module self-tests on import) ----
IMP = {}
try:
    from duanxianxia_canonical import REGISTRY
    IMP["REGISTRY"] = True
except Exception as e:  # noqa: BLE001
    REGISTRY = {}
    IMP["REGISTRY_err"] = str(e)
try:
    from duanxianxia_canonical_routing import canonicalize_row
    IMP["routing"] = True
except Exception as e:  # noqa: BLE001
    canonicalize_row = None
    IMP["routing_err"] = str(e)
try:
    from duanxianxia_feature_builder import build_feature_table, AUCTION_DATASETS
    IMP["feature_builder"] = True
except Exception as e:  # noqa: BLE001
    build_feature_table = None
    AUCTION_DATASETS = ()
    IMP["feature_builder_err"] = str(e)
try:
    from duanxianxia_feature_builder import _pick_capture_file, _cutoff_seconds
except Exception:  # noqa: BLE001
    _pick_capture_file = None
    _cutoff_seconds = None

cutoff_secs = _cutoff_seconds(CUTOFF) if _cutoff_seconds else (9 * 3600 + 29 * 60)


# ---- contract from datasets.json (best-effort; schema unknown) ----
contract_ids, contract_note = [], ""
cfg_obj, cfg_err = jload(CFG)
if cfg_err:
    contract_note = f"datasets.json unreadable: {cfg_err}"
elif isinstance(cfg_obj, dict):
    node = cfg_obj.get("datasets", cfg_obj)
    if isinstance(node, list):
        for d in node:
            contract_ids.append(d.get("id") or d.get("dataset") or d.get("name") if isinstance(d, dict) else d)
    elif isinstance(node, dict):
        contract_ids = list(node.keys())
    contract_note = "from datasets.json"
elif isinstance(cfg_obj, list):
    for d in cfg_obj:
        contract_ids.append(d.get("id") or d.get("dataset") or d.get("name") if isinstance(d, dict) else d)
    contract_note = "from datasets.json (list)"
contract_ids = sorted({c for c in contract_ids if c})


# ---- discover dates & dataset dirs ----
dates = []
if CAP_DIR.is_dir():
    dates = sorted(p.name for p in CAP_DIR.iterdir() if p.is_dir() and DATE_RE.match(p.name))
discovered = set()
for d in dates:
    for sub in (CAP_DIR / d).iterdir():
        if sub.is_dir():
            discovered.add(sub.name)
discovered = sorted(discovered)


# ---- PILLAR 1: captures inventory (completeness / accuracy) ----
inv = {}
matrix = {}
for ds in discovered:
    st = {"dates_present": [], "files": 0, "rows_latest_sum": 0,
          "json_err": 0, "post_cutoff_files": 0, "empty_dirs": 0}
    matrix[ds] = {}
    for d in dates:
        dd = CAP_DIR / d / ds
        if not dd.is_dir():
            matrix[ds][d] = None
            continue
        files = sorted(dd.glob("*.json"))
        if not files:
            st["empty_dirs"] += 1
            matrix[ds][d] = 0
            continue
        st["dates_present"].append(d)
        st["files"] += len(files)
        best = 0
        for f in files:
            payload, err = jload(f)
            if err:
                st["json_err"] += 1
                continue
            best = max(best, len(rows_of(payload)))
            digits = "".join(ch for ch in f.stem if ch.isdigit())
            if len(digits) >= 6:
                t = digits[-6:]
                try:
                    h, mi, se = int(t[:2]), int(t[2:4]), int(t[4:6])
                    if h < 24 and mi < 60 and se < 60 and (h * 3600 + mi * 60 + se) > cutoff_secs:
                        st["post_cutoff_files"] += 1
                except Exception:  # noqa: BLE001
                    pass
        matrix[ds][d] = best
        st["rows_latest_sum"] += best
    inv[ds] = st


def latest_dir_with_files(ds):
    for d in reversed(dates):
        dd = CAP_DIR / d / ds
        if dd.is_dir() and list(dd.glob("*.json")):
            return d, dd
    return None


# ---- PILLAR 2: canonical field coverage + unmapped datasets ----
reg_cov = {}
for ds in sorted(REGISTRY.keys()):
    picked = latest_dir_with_files(ds)
    if not picked:
        reg_cov[ds] = {"status": "no_capture"}
        continue
    d, dd = picked
    payload = None
    if _pick_capture_file:
        chosen, _meta = _pick_capture_file(dd, cutoff_secs)
        payload = chosen[1] if chosen else None
    if payload is None:
        payload, _ = jload(sorted(dd.glob("*.json"))[-1])
    rws = rows_of(payload)
    fields = REGISTRY[ds]["fields"]
    covc = {}
    ok = 0
    for row in rws:
        c = canonicalize_row(ds, row) if canonicalize_row else {}
        if not isinstance(c, dict) or c.get("_canonical_error"):
            continue
        ok += 1
        for fld in fields:
            if c.get(fld["canonical"]) is not None:
                covc[fld["canonical"]] = covc.get(fld["canonical"], 0) + 1
    reg_cov[ds] = {
        "date": d, "rows": len(rws), "canonical_ok": ok,
        "field_pct": {fld["canonical"]: (round(100 * covc.get(fld["canonical"], 0) / ok, 1) if ok else 0.0)
                      for fld in fields},
    }

unmapped_info = {}
for ds in [x for x in discovered if x not in REGISTRY]:
    picked = latest_dir_with_files(ds)
    info = {"mapped": False}
    if picked:
        _d, dd = picked
        payload, err = jload(sorted(dd.glob("*.json"))[-1])
        rws = rows_of(payload)
        info["rows"] = len(rws)
        if rws:
            r0 = rws[0]
            if isinstance(r0, dict):
                info["row_keys"] = sorted(map(str, r0.keys()))[:20]
                if isinstance(r0.get("raw"), list):
                    info["raw_len"] = len(r0["raw"])
            elif isinstance(r0, list):
                info["raw_len"] = len(r0)
    unmapped_info[ds] = info


# ---- PILLAR 3: merged wide table (feature_builder) ----
OUT_KEYS = ["free_float_mktcap", "bidAmount", "bidStrength", "volumeRatio", "grabStrength",
            "changeRate", "latestChangePct", "turnoverRate", "mainNetInflow", "sealAmount", "concept"]
wide = {}
if build_feature_table:
    for d in dates:
        try:
            res = build_feature_table(CAP_DIR / d, cutoff=CUTOFF)
        except Exception as e:  # noqa: BLE001
            wide[d] = {"error": f"{type(e).__name__}: {e}"}
            continue
        feats = res.get("features", []) or []
        nf = len(feats)
        fcov = {}
        bid_ok = 0
        for f in feats:
            for k in OUT_KEYS:
                if f.get(k) is not None:
                    fcov[k] = fcov.get(k, 0) + 1
            if f.get("bidStrength") is not None:
                bid_ok += 1
        wide[d] = {
            "n_features": nf,
            "coverage_sources": res.get("coverage"),
            "field_pct": {k: (round(100 * fcov.get(k, 0) / nf, 1) if nf else 0.0) for k in OUT_KEYS},
            "bidStrength_pct": round(100 * bid_ok / nf, 1) if nf else 0.0,
            "sources_present": {k: v.get("present") for k, v in (res.get("capture_meta") or {}).items()},
        }

folded = sorted(AUCTION_DATASETS)
not_folded = [ds for ds in discovered if ds not in folded]

report = {
    "task": "0112_data_foundation_audit",
    "captures_dir": str(CAP_DIR),
    "cutoff": CUTOFF,
    "imports": IMP,
    "n_dates": len(dates),
    "dates_covered": dates,
    "contract": {"note": contract_note, "n": len(contract_ids), "ids": contract_ids},
    "discovered_datasets": discovered,
    "contract_vs_captures": {
        "in_contract_not_captured": sorted([c for c in contract_ids if c not in discovered]),
        "captured_not_in_contract": sorted([d for d in discovered if d not in contract_ids]),
    },
    "pillar1_inventory": inv,
    "pillar1_row_matrix": matrix,
    "pillar2_registry_datasets": sorted(REGISTRY.keys()),
    "pillar2_canonical_coverage": reg_cov,
    "pillar2_unmapped_datasets": unmapped_info,
    "pillar3_folded": folded,
    "pillar3_not_folded": not_folded,
    "pillar3_wide_table": wide,
}

print("=== DUANXIANXIA DATA-FOUNDATION AUDIT (Task 0112) ===")
print(f"dates={len(dates)} discovered={len(discovered)} registry={len(REGISTRY)} "
      f"folded={len(folded)} not_folded={len(not_folded)}")
print(json.dumps(report, ensure_ascii=False, indent=1, default=str))
