"""将角色 permissions（JSON 权限 id 列表）转为「标题1|标题2」展示名。

数据源 app/data/permission_menu.json；文件缺失时优雅降级为 '—'/原始 id。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_DATA_FILE = Path(__file__).resolve().parent / "data" / "permission_menu.json"

_id_to_title: dict[str, str] = {}
_data_mtime: float = 0.0


def _read_tree() -> list[dict[str, Any]]:
    if not _DATA_FILE.is_file():
        return []
    with open(_DATA_FILE, "r", encoding="utf-8") as f:
        raw: Any = json.load(f)
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("tree"), list):
        return raw["tree"]
    return []


def _rebuild_id_map() -> None:
    global _id_to_title, _data_mtime
    mtime = _DATA_FILE.stat().st_mtime if _DATA_FILE.is_file() else 0.0
    if _id_to_title and mtime == _data_mtime:
        return
    _data_mtime = mtime
    _id_to_title = {}

    def walk(nodes: list[dict[str, Any]] | None) -> None:
        if not nodes:
            return
        for n in nodes:
            if not isinstance(n, dict):
                continue
            nid = n.get("id")
            if nid is not None and "title" in n:
                _id_to_title[str(nid)] = str(n.get("title") or "")
            ch = n.get("children")
            if isinstance(ch, list):
                walk(ch)

    walk(_read_tree())


def permission_ids_to_piped_titles(permissions_json: str | None) -> str:
    _rebuild_id_map()
    s = (permissions_json or "").strip() or "[]"
    try:
        arr: Any = json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return "—"
    if not isinstance(arr, list) or not arr:
        return "—"
    titles: list[str] = []
    for x in arr:
        if x is None:
            continue
        key = str(x).strip()
        if not key:
            continue
        label = _id_to_title.get(key)
        if label is None and key.isdigit():
            label = _id_to_title.get(str(int(key)))
        titles.append(label if label is not None else key)
    return "|".join(titles) if titles else "—"


def remark_text_for_stored_role(permissions_json: str | None, role_code: str | None) -> str:
    if (role_code or "").strip().lower() == "admin":
        return "全部模块"
    piped = permission_ids_to_piped_titles(permissions_json)
    if piped == "—":
        return "未配置具体模块（可稍后在「按模块授权」中勾选）"
    if len(piped) > 512:
        return piped[:509] + "..."
    return piped
