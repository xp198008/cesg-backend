"""角色授权功能树。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter

router = APIRouter(prefix="/api/permission-menu", tags=["permission-menu"])

_DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "permission_menu.json"


_FORCED_DISABLED_IDS = {"1"}  # 智慧看板：全员必有，角色树不可取消


def _read_tree() -> list[dict[str, Any]]:
    if not _DATA_FILE.is_file():
        return []
    raw = json.loads(_DATA_FILE.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and isinstance(raw.get("tree"), list):
        return raw["tree"]
    return []


def _mark_forced_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for node in nodes or []:
        item = dict(node)
        node_id = str(item.get("id", "")).strip()
        if node_id in _FORCED_DISABLED_IDS:
            item["disabled"] = True
            item["checked"] = True
        children = item.get("children")
        if isinstance(children, list) and children:
            item["children"] = _mark_forced_nodes(children)
        out.append(item)
    return out


@router.get("/tree")
async def permission_menu_tree():
    return {"ok": True, "tree": _mark_forced_nodes(_read_tree())}
