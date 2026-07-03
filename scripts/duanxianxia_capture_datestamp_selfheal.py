#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
duanxianxia_capture_datestamp_selfheal.py -- 隔夜落盘自愈 (写侧根治, 附加/可逆)。

背景: persist_capture 按“运行墓钟时刻”盖 captures/<date>/<dataset>/<HHMMSS>.json
的 <date> 目录, 不看数据真实交易日。任何跨午夜运行的抓取(尤其凌晨
经 */10 任务执行器跑的临时复盘抓取)会把内容日期=D 的文件落到 D+1 目录。
0129 审计确认 6 个错位文件(review.daily/fupan/ltgd) 全部 fetched@01:20。

本脚本 = 写侧等价根治: 扫描 captures, 对“带内容日期字段”的数据集, 若文件所在
日期目录 != 内容交易日, 则把文件搬到正确的 <内容交易日>/<dataset>/ 目录。
- 覆盖全部带日期表(含 home.ztpool), 一并满足 ztpool 兜底。
- 默认 dry-run; --apply 才真正移动。
- 幂等: 搬完后 目录==内容日, 再跑 no-op。
- 只搬“能可靠解析内容日期”的文件; 其余(竞价/资金流/榜单等无内容日期表)原样不动。
- 只读 JSON + 改名, 不改 fetcher.py/batch.py。

用法:
  python3 scripts/duanxianxia_capture_datestamp_selfheal.py                 # dry-run, 最近5个日期目录
  python3 scripts/duanxianxia_capture_datestamp_selfheal.py --all           # dry-run 全量
  python3 scripts/duanxianxia_capture_datestamp_selfheal.py --all --apply   # 全量归位
  python3 scripts/duanxianxia_capture_datestamp_selfheal.py --recent 6 --apply  # 供 runner 每10分钟自愈
"""
from __future__ import annotations
import json
import re
import sys
import tempfile
from pathlib import Path

WORKSPACE = Path("/home/investmentofficehku/.openclaw/workspace")
CAPTURE_ROOT = WORKSPACE / "projects" / "duanxianxia" / "captures"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# 带内容日期的数据集: 优先取 meta 的哪个字段
META_DATE_KEY = {
    "review.ltgd.range": "latest_date",
}
DEFAULT_META_KEYS = ("date",)
TOP_DATE_KEYS = ("日期", "date")
ROW_DATE_KEYS = ("日期", "date")


def _isdate(v):
    return isinstance(v, str) and bool(DATE_RE.match(v.strip()))


def resolve_content_date(dataset_id, payload):
    """返回文件内容的真实交易日(YYYY-MM-DD)或 None(无法可靠解析则不搬)。"""
    if not isinstance(payload, dict):
        return None
    meta = payload.get("meta")
    if isinstance(meta, dict):
        pref = META_DATE_KEY.get(dataset_id)
        if pref and _isdate(meta.get(pref)):
            return meta[pref].strip()
        for k in DEFAULT_META_KEYS:
            if _isdate(meta.get(k)):
                return meta[k].strip()
    for k in TOP_DATE_KEYS:
        if _isdate(payload.get(k)):
            return payload[k].strip()
    rows = payload.get("rows")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                for k in ROW_DATE_KEYS:
                    if _isdate(row.get(k)):
                        return row[k].strip()
                break
    return None


def _date_folders(root):
    if not root.is_dir():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir() and DATE_RE.match(p.name))


def scan(root, folders, apply=False):
    moves, collisions, bad_json = [], [], []
    for folder in folders:
        fdir = root / folder
        if not fdir.is_dir():
            continue
        for ds_dir in sorted(fdir.iterdir()):
            if not ds_dir.is_dir():
                continue
            ds = ds_dir.name
            for f in sorted(ds_dir.glob("*.json")):
                try:
                    payload = json.loads(f.read_text(encoding="utf-8"))
                except Exception as e:  # noqa: BLE001
                    bad_json.append({"file": str(f.relative_to(root)), "err": f"{type(e).__name__}: {e}"})
                    continue
                cdate = resolve_content_date(ds, payload)
                if not cdate or cdate == folder:
                    continue
                target_dir = root / cdate / ds
                target = target_dir / f.name
                rec = {"dataset": ds, "file": f.name, "from": folder, "to": cdate,
                       "src": str(f.relative_to(root))}
                if target.exists():
                    collisions.append(rec)
                    continue
                if apply:
                    target_dir.mkdir(parents=True, exist_ok=True)
                    f.rename(target)
                moves.append(rec)
    return {"moves": moves, "collisions": collisions, "bad_json": bad_json}


def _self_test():
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        ds = "review.daily.top_metrics"
        f = root / "2026-07-02" / ds / "012029.json"
        f.parent.mkdir(parents=True)
        f.write_text(json.dumps({"meta": {"date": "2026-07-01"}, "rows": []}), encoding="utf-8")
        g = root / "2026-06-01" / ds / "172000.json"
        g.parent.mkdir(parents=True)
        g.write_text(json.dumps({"meta": {"date": "2026-06-01"}, "rows": []}), encoding="utf-8")
        lt = root / "2026-07-03" / "review.ltgd.range" / "012032.json"
        lt.parent.mkdir(parents=True)
        lt.write_text(json.dumps({"meta": {"latest_date": "2026-07-02"}, "rows": []}), encoding="utf-8")
        folders = _date_folders(root)
        dry = scan(root, folders, apply=False)
        assert len(dry["moves"]) == 2, dry
        assert not (root / "2026-07-01" / ds / "012029.json").exists()
        res = scan(root, folders, apply=True)
        assert len(res["moves"]) == 2, res
        assert (root / "2026-07-01" / ds / "012029.json").exists()
        assert (root / "2026-07-02" / "review.ltgd.range" / "012032.json").exists()
        assert not (root / "2026-07-02" / ds / "012029.json").exists()
        assert (root / "2026-06-01" / ds / "172000.json").exists()
        again = scan(root, _date_folders(root), apply=True)
        assert len(again["moves"]) == 0, again
    return True


_self_test()


def main(argv):
    apply = "--apply" in argv
    all_folders = "--all" in argv
    recent = None
    if "--recent" in argv:
        i = argv.index("--recent")
        if i + 1 < len(argv):
            try:
                recent = int(argv[i + 1])
            except ValueError:
                recent = None
    pos = [a for a in argv if not a.startswith("--") and not a.isdigit()]
    root = Path(pos[0]).resolve() if pos else CAPTURE_ROOT
    folders = _date_folders(root)
    if not all_folders:
        n = recent if recent is not None else 5
        folders = folders[-n:]
    res = scan(root, folders, apply=apply)
    out = {
        "task": "capture_datestamp_selfheal",
        "captures_root": str(root),
        "mode": "apply" if apply else "dry-run",
        "scanned_folders": folders,
        "moved_count": len(res["moves"]),
        "collision_count": len(res["collisions"]),
        "bad_json_count": len(res["bad_json"]),
        "moves": res["moves"],
        "collisions": res["collisions"],
        "bad_json": res["bad_json"][:20],
    }
    print(json.dumps(out, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
