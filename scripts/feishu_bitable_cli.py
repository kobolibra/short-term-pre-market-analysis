#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

WORKSPACE_ROOT = Path("/home/investmentofficehku/.openclaw/workspace")
DEFAULT_META_DIR = WORKSPACE_ROOT / "memory" / "feishu_bitable"
BASE_URL = "https://open.feishu.cn/open-apis"
TZ_SHANGHAI = ZoneInfo("Asia/Shanghai")
_FIELD_CACHE: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}


def load_workspace_env(env_path: Path) -> None:
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            value = value.strip()
            if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            os.environ.setdefault(key, value)
    except Exception:
        return


load_workspace_env(WORKSPACE_ROOT / ".env")


def load_openclaw_config_env(config_path: Path) -> None:
    if not config_path.exists():
        return
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return
    feishu = ((payload.get("channels") or {}).get("feishu") or {})
    app_id = str(feishu.get("appId") or "").strip()
    app_secret = str(feishu.get("appSecret") or "").strip()
    if app_id:
        os.environ.setdefault("FEISHU_APP_ID", app_id)
    if app_secret:
        os.environ.setdefault("FEISHU_APP_SECRET", app_secret)


load_openclaw_config_env(Path("/home/investmentofficehku/.openclaw/openclaw.json"))


def mint_tenant_access_token() -> str:
    app_id = (os.environ.get("FEISHU_APP_ID") or "").strip()
    app_secret = (os.environ.get("FEISHU_APP_SECRET") or "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("Missing FEISHU_APP_ID or FEISHU_APP_SECRET in environment.")

    url = BASE_URL + "/auth/v3/tenant_access_token/internal"
    body = json.dumps({"app_id": app_id, "app_secret": app_secret}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "openclaw-feishu-bitable-cli/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} when minting tenant token: {raw}") from exc
    payload = json.loads(raw)
    code = payload.get("code", 0)
    if code != 0:
        msg = payload.get("msg") or payload.get("message") or "unknown error"
        raise RuntimeError(f"Failed to mint tenant token, code={code}, msg={msg}")
    token = (payload.get("tenant_access_token") or "").strip()
    if not token:
        raise RuntimeError("Tenant token response missing tenant_access_token")
    return token


def get_access_token() -> str:
    app_id = (os.environ.get("FEISHU_APP_ID") or "").strip()
    app_secret = (os.environ.get("FEISHU_APP_SECRET") or "").strip()
    if app_id and app_secret:
        return mint_tenant_access_token()

    token = (
        os.environ.get("FEISHU_TENANT_ACCESS_TOKEN")
        or os.environ.get("TENANT_ACCESS_TOKEN")
        or os.environ.get("ACCESS_TOKEN")
        or os.environ.get("FEISHU_ACCESS_TOKEN")
        or ""
    ).strip()
    if token:
        return token
    raise RuntimeError("Missing Feishu token, and no FEISHU_APP_ID/FEISHU_APP_SECRET available to mint one.")


def feishu_request(method: str, path: str, *, query: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    token = get_access_token()
    url = BASE_URL + path
    if query:
        query_clean = {k: v for k, v in query.items() if v is not None and v != ""}
        if query_clean:
            url += "?" + urllib.parse.urlencode(query_clean)
    body = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "openclaw-feishu-bitable-cli/1.0",
    }
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        if exc.code in (400, 401):
            try:
                token = mint_tenant_access_token()
                req = urllib.request.Request(
                    url,
                    data=body,
                    headers={**headers, "Authorization": f"Bearer {token}"},
                    method=method.upper(),
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
            except Exception:
                raise RuntimeError(f"HTTP {exc.code}: {raw}") from exc
        else:
            raise RuntimeError(f"HTTP {exc.code}: {raw}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Non-JSON response: {raw[:500]}") from exc

    code = payload.get("code", 0)
    if code != 0:
        msg = payload.get("msg") or payload.get("message") or "unknown error"
        raise RuntimeError(f"Feishu API error code={code}, msg={msg}, payload={json.dumps(payload, ensure_ascii=False)[:800]}")
    return payload


def create_app(name: str, folder_token: str = "") -> Dict[str, Any]:
    payload = {"name": name}
    if folder_token:
        payload["folder_token"] = folder_token
    res = feishu_request("POST", "/bitable/v1/apps", data=payload)
    app = (res.get("data") or {}).get("app") or {}
    return {
        "app_token": app.get("app_token", ""),
        "name": app.get("name", name),
        "url": app.get("url", ""),
    }


def list_tables(app_token: str) -> List[Dict[str, Any]]:
    res = feishu_request("GET", f"/bitable/v1/apps/{app_token}/tables")
    return ((res.get("data") or {}).get("items") or [])


def list_fields(app_token: str, table_id: str) -> List[Dict[str, Any]]:
    res = feishu_request("GET", f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields")
    return ((res.get("data") or {}).get("items") or [])


def create_field(app_token: str, table_id: str, field_name: str, field_type: int, property_obj: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "field_name": field_name,
        "type": int(field_type),
    }
    if property_obj:
        payload["property"] = property_obj
    res = feishu_request("POST", f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields", data=payload)
    return ((res.get("data") or {}).get("field") or {})


def _to_feishu_date_timestamp(value: Any) -> Any:
    if value is None:
        return value
    if isinstance(value, (int, float)):
        ivalue = int(value)
        return ivalue if ivalue > 10_000_000_000 else ivalue * 1000
    text = str(value).strip()
    if not text:
        return text
    if re.fullmatch(r"\d{10,13}", text):
        ivalue = int(text)
        return ivalue if len(text) == 13 else ivalue * 1000
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=TZ_SHANGHAI)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return value


def _normalize_record_fields(app_token: str, table_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    cache_key = (app_token, table_id)
    field_map = _FIELD_CACHE.get(cache_key)
    if field_map is None:
        field_map = {str(item.get("field_name") or ""): item for item in list_fields(app_token, table_id)}
        _FIELD_CACHE[cache_key] = field_map
    normalized: Dict[str, Any] = {}
    for key, value in fields.items():
        field_meta = field_map.get(str(key)) or {}
        field_type = int(field_meta.get("type") or 0)
        if field_type == 5:
            normalized[key] = _to_feishu_date_timestamp(value)
        else:
            normalized[key] = value
    return normalized


def create_record(app_token: str, table_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    payload_fields = _normalize_record_fields(app_token, table_id, fields)
    res = feishu_request("POST", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records", data={"fields": payload_fields})
    return ((res.get("data") or {}).get("record") or {})


def update_record(app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    payload_fields = _normalize_record_fields(app_token, table_id, fields)
    res = feishu_request("PUT", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}", data={"fields": payload_fields})
    return ((res.get("data") or {}).get("record") or {})


def save_meta(name: str, data: Dict[str, Any]) -> Path:
    DEFAULT_META_DIR.mkdir(parents=True, exist_ok=True)
    path = DEFAULT_META_DIR / f"{name}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_meta(name: str) -> Dict[str, Any]:
    path = DEFAULT_META_DIR / f"{name}.json"
    if not path.exists():
        raise SystemExit(f"Meta file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def print_json(data: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def init_review_schema(name: str, folder_token: str = "", meta_name: str = "duanxianxia_review") -> Dict[str, Any]:
    app = create_app(name, folder_token)
    app_token = app["app_token"]
    tables = list_tables(app_token)
    if not tables:
        raise RuntimeError(f"Created app but found no default table, app_token={app_token}")
    table = tables[0]
    table_id = table.get("table_id", "")
    fields_before = list_fields(app_token, table_id)
    primary_field = next((f for f in fields_before if f.get("is_primary")), None)
    primary_field_name = (primary_field or {}).get("field_name") or "Name"

    schema = [
        ("日期", 1),
        ("推荐时间", 1),
        ("推荐场景", 1),
        ("股票代码", 1),
        ("股票名称", 1),
        ("推荐分级", 1),
        ("推荐理由", 1),
        ("竞价涨幅", 2),
        ("推荐时涨幅", 2),
        ("收盘涨幅", 2),
        ("收盘相对竞价变化", 2),
        ("成交额", 2),
        ("换手率", 2),
        ("结果评价", 1),
        ("反思结论", 1),
        ("来源会话", 1),
    ]

    created_fields: List[Dict[str, Any]] = []
    existing_names = {str(item.get("field_name", "")) for item in fields_before}
    for field_name, field_type in schema:
        if field_name in existing_names:
            continue
        created = create_field(app_token, table_id, field_name, field_type)
        created_fields.append({
            "field_id": created.get("field_id", ""),
            "field_name": created.get("field_name", field_name),
            "type": created.get("type", field_type),
        })

    result = {
        "name": app.get("name", name),
        "url": app.get("url", ""),
        "app_token": app_token,
        "table_id": table_id,
        "primary_field_name": primary_field_name,
        "created_fields": created_fields,
        "all_fields": list_fields(app_token, table_id),
    }
    meta_path = save_meta(meta_name, result)
    result["meta_path"] = str(meta_path)
    return result


def cmd_init_review(args: argparse.Namespace) -> None:
    result = init_review_schema(name=args.name, folder_token=args.folder_token or "", meta_name=args.meta_name)
    print_json(result)


def cmd_create_app(args: argparse.Namespace) -> None:
    result = create_app(args.name, args.folder_token or "")
    print_json(result)


def cmd_list_tables(args: argparse.Namespace) -> None:
    print_json({"items": list_tables(args.app_token)})


def cmd_list_fields(args: argparse.Namespace) -> None:
    print_json({"items": list_fields(args.app_token, args.table_id)})


def cmd_create_field(args: argparse.Namespace) -> None:
    prop = json.loads(args.property_json) if args.property_json else None
    result = create_field(args.app_token, args.table_id, args.field_name, args.field_type, prop)
    print_json(result)


def cmd_create_record(args: argparse.Namespace) -> None:
    fields = json.loads(args.fields_json)
    result = create_record(args.app_token, args.table_id, fields)
    print_json(result)


def cmd_update_record(args: argparse.Namespace) -> None:
    fields = json.loads(args.fields_json)
    result = update_record(args.app_token, args.table_id, args.record_id, fields)
    print_json(result)


def cmd_add_review_record(args: argparse.Namespace) -> None:
    meta = load_meta(args.meta_name)
    app_token = meta["app_token"]
    table_id = meta["table_id"]
    primary_field_name = meta.get("primary_field_name") or "Name"
    fields = json.loads(args.fields_json)
    if primary_field_name not in fields:
        fields[primary_field_name] = fields.get("股票代码") or fields.get("股票名称") or fields.get("日期") or "记录"
    result = create_record(app_token, table_id, fields)
    print_json({
        "meta_name": args.meta_name,
        "app_token": app_token,
        "table_id": table_id,
        "record": result,
    })


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Feishu Bitable helper for local automation")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("create-app")
    p.add_argument("--name", required=True)
    p.add_argument("--folder-token", default="")
    p.set_defaults(func=cmd_create_app)

    p = sub.add_parser("list-tables")
    p.add_argument("--app-token", required=True)
    p.set_defaults(func=cmd_list_tables)

    p = sub.add_parser("list-fields")
    p.add_argument("--app-token", required=True)
    p.add_argument("--table-id", required=True)
    p.set_defaults(func=cmd_list_fields)

    p = sub.add_parser("create-field")
    p.add_argument("--app-token", required=True)
    p.add_argument("--table-id", required=True)
    p.add_argument("--field-name", required=True)
    p.add_argument("--field-type", required=True, type=int)
    p.add_argument("--property-json", default="")
    p.set_defaults(func=cmd_create_field)

    p = sub.add_parser("create-record")
    p.add_argument("--app-token", required=True)
    p.add_argument("--table-id", required=True)
    p.add_argument("--fields-json", required=True)
    p.set_defaults(func=cmd_create_record)

    p = sub.add_parser("update-record")
    p.add_argument("--app-token", required=True)
    p.add_argument("--table-id", required=True)
    p.add_argument("--record-id", required=True)
    p.add_argument("--fields-json", required=True)
    p.set_defaults(func=cmd_update_record)

    p = sub.add_parser("init-review")
    p.add_argument("--name", default="短线侠推荐复盘")
    p.add_argument("--folder-token", default="")
    p.add_argument("--meta-name", default="duanxianxia_review")
    p.set_defaults(func=cmd_init_review)

    p = sub.add_parser("add-review-record")
    p.add_argument("--meta-name", default="duanxianxia_review")
    p.add_argument("--fields-json", required=True)
    p.set_defaults(func=cmd_add_review_record)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
